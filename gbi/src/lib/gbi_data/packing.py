"""Read-only, bounded qualification of non-overlapping small-file subtrees.

Policy values are explicit: these are not measured site defaults. Partial
discovery yields lower bounds and never authorizes packing. Payload reading,
archive publication and cleanup belong to the normal supervised transfer path.
"""

from dataclasses import asdict, dataclass
import fnmatch
import os
from pathlib import Path, PurePosixPath
import stat
import time

from . import archives


@dataclass(frozen=True)
class PackingPolicy:
    small_file_bytes: int
    min_files: int
    max_archive_bytes: int
    max_entries: int
    max_seconds: float
    max_depth: int = 64

    def __post_init__(self):
        for name in ("small_file_bytes", "min_files", "max_archive_bytes", "max_entries", "max_depth"):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ValueError(f"packing {name} must be a positive integer")
        if self.min_files < 2:
            raise ValueError("packing min_files must be at least two; single-file archives save no file operations")
        if not isinstance(self.max_seconds, (int, float)) or not 0 < self.max_seconds < float("inf"):
            raise ValueError("packing max_seconds must be positive and finite")


def _fingerprint(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _within(name, directory):
    return directory == "." or name == directory or name.startswith(directory + "/")


def _stat(root_fd, name):
    if name == ".":
        return os.fstat(root_fd)
    parts = name.split("/")
    fd = os.dup(root_fd)
    try:
        for part in parts[:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        return os.stat(parts[-1], dir_fd=fd, follow_symlinks=False)
    finally:
        os.close(fd)


def plan(source, policy, includes=(), exclusions=(), reserved_names=(), allow_root=False,
         *, selection_mode="basename"):
    """Return a deterministic dry-run model; write nothing and follow no links.

    The wall-time bound is checked between filesystem calls. Invoke through the
    existing process supervisor to bound a stalled filesystem syscall as well.
    Execution must requalify this plan and let archive packing reconcile source
    observations; a dry-run is not a durable permission to delete any source.
    """
    select = archives._selection(includes, exclusions, selection_mode)
    if not isinstance(policy, PackingPolicy):
        raise TypeError("policy must be a PackingPolicy with explicit thresholds")
    source = Path(source).absolute()
    if source.resolve() != source or source.is_symlink() or not source.is_dir():
        raise ValueError("packing source must be a directory without symlink parents")
    started = time.monotonic()
    reserved = (".gbi", ".prefect-*", "transfer-locks", *reserved_names)
    observations, directories, selected, all_names = {}, {}, {}, set()
    visited, stopped = 0, None

    def budget():
        nonlocal stopped
        if stopped:
            return stopped
        if visited >= policy.max_entries:
            stopped = "entry budget exhausted"
        elif time.monotonic() - started >= policy.max_seconds:
            stopped = "time budget exhausted"
        return stopped

    def walk(fd, relative, depth):
        nonlocal visited
        initial = _fingerprint(os.fstat(fd))
        observations[relative] = initial
        record = {"complete": True, "reasons": [], "regular_files": 0,
                  "selected_entries": 0, "bytes": 0, "large_files": 0,
                  "special_files": 0, "children": [], "metadata_bytes": 0,
                  "metadata_entries": 0, "observation_bytes": 0,
                  "hardlink_entries": 0}
        directories[relative] = record
        if depth >= policy.max_depth:
            record["complete"] = False
            record["reasons"].append("directory depth budget exhausted")
            return record
        try:
            saw_child = False
            with os.scandir(fd) as children:
                for child in children:
                    saw_child = True
                    if budget():
                        record["complete"] = False
                        record["reasons"].append(stopped)
                        break
                    visited += 1
                    name = child.name if relative == "." else relative + "/" + child.name
                    all_names.add(name)
                    if any(fnmatch.fnmatchcase(child.name, pattern) for pattern in reserved):
                        continue
                    try:
                        info = child.stat(follow_symlinks=False)
                        observations[name] = _fingerprint(info)
                        record["observation_bytes"] += len(
                            archives._json([name, list(observations[name])]))
                        if stat.S_ISDIR(info.st_mode):
                            child_fd = os.open(child.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                            try:
                                if _fingerprint(os.fstat(child_fd)) != observations[name]:
                                    raise ValueError("directory changed during discovery")
                                nested = walk(child_fd, name, depth + 1)
                            finally:
                                os.close(child_fd)
                            record["children"].append(name)
                            record["metadata_bytes"] += nested["metadata_bytes"]
                            record["metadata_entries"] += nested["metadata_entries"]
                            record["observation_bytes"] += nested["observation_bytes"]
                            record["hardlink_entries"] += nested["hardlink_entries"]
                            for key in ("regular_files", "selected_entries", "bytes", "large_files", "special_files"):
                                record[key] += nested[key]
                            directory_entry = archives._metadata_entry(name, info, "directory")
                            record["metadata_bytes"] += len(archives._json(directory_entry))
                            record["metadata_entries"] += 1
                            if not nested["complete"]:
                                record["complete"] = False
                                record["reasons"].append("descendant discovery is incomplete or changed")
                        elif select(name):
                            kind = "file" if stat.S_ISREG(info.st_mode) else "symlink" if stat.S_ISLNK(info.st_mode) else "special"
                            target = None
                            if kind == "symlink":
                                target = os.readlink(child.name, dir_fd=fd)
                            selected[name] = {"type": kind, "bytes": info.st_size if kind == "file" else 0}
                            record["selected_entries"] += 1
                            if kind == "file":
                                record["regular_files"] += 1
                                record["bytes"] += info.st_size
                                record["large_files"] += int(info.st_size > policy.small_file_bytes)
                            elif kind == "special":
                                record["special_files"] += 1
                            if kind in ("file", "symlink"):
                                metadata_entry = archives._metadata_entry(name, info, kind, target)
                                record["metadata_bytes"] += len(archives._json(metadata_entry))
                                record["metadata_entries"] += 1
                                if stat.S_ISREG(info.st_mode) and info.st_nlink > 1:
                                    record["hardlink_entries"] += 1
                    except (OSError, ValueError) as error:
                        record["complete"] = False
                        record["reasons"].append(f"cannot qualify {name!r}: {type(error).__name__}")
            if not saw_child and select(source.name if relative == "." else relative):
                selected[relative] = {"type": "directory", "bytes": 0}
                record["selected_entries"] += 1
            if _fingerprint(os.fstat(fd)) != initial:
                record["complete"] = False
                record["reasons"].append("source directory changed during discovery")
        except OSError as error:
            record["complete"] = False
            record["reasons"].append(f"directory discovery failed: {type(error).__name__}")
        return record

    root_fd = os.open(source, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        walk(root_fd, ".", 0)
        # Final metadata reconciliation catches already-scanned files changing
        # while later subtrees are observed. No payload reads or hashes here.
        changed = []
        for name, observed in observations.items():
            if time.monotonic() - started >= policy.max_seconds:
                stopped = "time budget exhausted during source reconciliation"
                for record in directories.values():
                    record["complete"] = False
                    record["reasons"].append(stopped)
                break
            try:
                if _fingerprint(_stat(root_fd, name)) != observed:
                    changed.append(name)
            except OSError:
                changed.append(name)
        for name in changed:
            affected = {str(parent) for parent in PurePosixPath(name).parents} | {name, "."}
            if name in directories:
                affected.update(relative for relative in directories if _within(relative, name))
            for relative in affected:
                if relative in directories:
                    directories[relative]["complete"] = False
                    directories[relative]["reasons"].append(f"source changed: {name!r}")
        if _fingerprint(source.lstat()) != observations["."]:
            for record in directories.values():
                record["complete"] = False
                record["reasons"].append("source root changed during discovery")
    finally:
        os.close(root_fd)

    max_name = max(all_names, key=lambda name: len(archives._json(name)), default="")
    regular_probe = {"path": max_name, "type": "file", "mode": 0,
                     "mtime_ns": 0, "size": 0, "sha256": "0" * 64}
    hardlink_probe = {**regular_probe, "type": "hardlink", "target": max_name}
    hardlink_metadata_allowance = (len(archives._json(hardlink_probe))
                                   - len(archives._json(regular_probe)))
    candidates, unqualified = [], []
    packed_roots, covered_ancestors = set(), set()
    for relative in sorted(directories, key=lambda name: (-name.count("/"), name == ".", name)):
        record = directories[relative]
        reasons = list(dict.fromkeys(record["reasons"]))
        if relative == "." and not allow_root:
            reasons.append("source-root files stay loose to preserve layout; use --pack for whole-directory packing")
        if relative in covered_ancestors:
            reasons.append("deeper qualifying subtrees are packed separately")
        if record["regular_files"] < policy.min_files:
            reasons.append(f"fewer than {policy.min_files} selected regular files")
        if record["large_files"]:
            reasons.append(f"contains files larger than {policy.small_file_bytes} bytes; keep them ordinary")
        if record["bytes"] > policy.max_archive_bytes:
            reasons.append(f"selected bytes exceed the {policy.max_archive_bytes}-byte archive budget")
        if record["special_files"]:
            reasons.append("selected special files are unsupported")
        hardlink_allowance = record["hardlink_entries"] * hardlink_metadata_allowance
        manifest_bytes = archives._manifest_size(record["metadata_bytes"] + hardlink_allowance,
                                                 record["metadata_entries"])
        if record["observation_bytes"] > archives.MAX_MANIFEST:
            reasons.append("source observation metadata exceeds the archive manifest limit")
        if manifest_bytes > archives.MAX_MANIFEST:
            reasons.append("archive entry metadata exceeds the archive manifest limit")
        archive_relative = (source.name if relative == "." else relative) + ".gbi.tar"
        if archive_relative in all_names or os.path.lexists(source / archive_relative):
            reasons.append("planned archive name collides with an existing source entry")
        summary = {"source_relative": relative, "regular_files": record["regular_files"],
                   "selected_entries": record["selected_entries"], "bytes": record["bytes"],
                   "metadata_bytes": manifest_bytes, "observation_bytes": record["observation_bytes"],
                   "totals_kind": "observed" if record["complete"] else "observed_lower_bound"}
        if not record["complete"] or reasons:
            unqualified.append({**summary, "reasons": reasons or ["discovery incomplete"]})
            continue
        candidates.append({**summary, "archive_relative": archive_relative,
                           "restore_relative": relative, "staging_required": False,
                           "reason": "fully observed small-file subtree; deepest qualifying non-overlapping selection"})
        packed_roots.add(relative)
        covered_ancestors.update(str(parent) for parent in PurePosixPath(relative).parents)
        covered_ancestors.add(".")

    return {"version": 1, "policy": asdict(policy), "complete": all(r["complete"] for r in directories.values()),
            "visited_entries": visited, "budget_exhausted": stopped,
            "candidates": sorted(candidates, key=lambda item: item["source_relative"]),
            "unqualified": sorted(unqualified, key=lambda item: item["source_relative"]),
            "loose_selected": sorted(name for name in selected
                                     if name not in packed_roots
                                     and not any(str(parent) in packed_roots for parent in PurePosixPath(name).parents)),
            "staging_required": False,
            "totals_note": "Observed metadata totals, not checksum-verified payload sizes; incomplete scans are lower bounds.",
            "execution_note": "Requalify at execution; archive readback and stable source verification still precede move cleanup."}
