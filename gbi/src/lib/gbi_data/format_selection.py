"""Logical format units for the existing discovery queue and worker pool.

Explicit sources are recognized by content, including renamed archives. Nested
automatic restore checks generated .gbi.tar[.gz] / .gbi-chunks names first, then
validates their markers; ordinary small files incur no payload-sniffing reads.
"""

from collections import Counter
from dataclasses import asdict
import json
import os
from pathlib import Path
import stat
import subprocess
import sys

from . import chunks, formats, packing
from .selection import entries, matches
from .storage import Site, fingerprint, overlap

CHUNK_SUFFIX = ".gbi-chunks"
ARCHIVE_SUFFIXES = (".gbi.tar.gz", ".gbi.tar")


def _packing_preview(result):
    """Keep dry-run output bounded when qualification sees a large tree."""
    result = dict(result)
    unqualified = result.pop("unqualified", [])
    loose = result.pop("loose_selected", [])

    def reason_group(reason):
        for marker, label in (
                ("budget", "discovery budget"),
                ("fewer than", "too few selected files"),
                ("larger than", "large selected file"),
                ("exceed the", "archive size budget"),
                ("special files", "unsupported special file"),
                ("collides", "archive name collision"),
                ("changed", "source changed during discovery"),
                ("failed", "discovery failure")):
            if marker in reason:
                return label
        return reason

    reasons = Counter(group
                      for item in unqualified
                      for group in {reason_group(reason) for reason in
                                    (item.get("reasons") or ["discovery incomplete"])})
    reason_counts = dict(sorted(reasons.items(), key=lambda pair: (-pair[1], pair[0])))
    if len(reason_counts) > 20:
        kept = dict(list(reason_counts.items())[:19])
        kept["other reasons"] = sum(list(reason_counts.values())[19:])
        reason_counts = kept

    result["unqualified_summary"] = {
        "directory_count": len(unqualified),
        "reason_counts": reason_counts,
        "examples": [item.get("source_relative") for item in unqualified[:12]],
    }
    result["loose_selected_summary"] = {
        "entry_count": len(loose),
        "examples": loose[:12],
    }
    result["preview_notice"] = (
        "Discovery incomplete: displayed counts are observed lower bounds; "
        "no archive is authorized."
        if not result.get("complete") else
        "Discovery complete: metadata observations are not checksum-verified payload totals."
    )
    return result


def _unpacked_name(name):
    for suffix in ARCHIVE_SUFFIXES:
        if name.endswith(suffix):
            result = name[:-len(suffix)]
            if not result:
                raise ValueError("generated archive name has no restore directory name")
            return result
    return name


def configure(specification, site, *, pack=None, chunk_size=None, packing_policy=None):
    """Bind public options to a logical request without discovering whole trees."""
    spec = dict(specification)
    source = Path(spec["source"])
    requested_target = Path(spec.get("requested_target", spec["target"]))
    if pack not in (None, "tar", "gzip"):
        raise ValueError("--pack must be tar or gzip")
    if chunk_size is not None and (type(chunk_size) is not int or chunk_size < 1):
        raise ValueError("--chunk-size must be a positive size")
    if pack and packing_policy is not None:
        raise ValueError("choose --pack or --pack-small, not both")
    if (pack or packing_policy is not None) and (source.is_symlink() or not source.is_dir()):
        raise ValueError("packing requires a source directory")
    if packing_policy is not None and not isinstance(packing_policy, packing.PackingPolicy):
        raise ValueError("--pack-small requires an explicitly configured PackingPolicy")
    # A packing request intentionally treats existing encoded files as source
    # members; it must not transparently unpack them while making a new archive.
    kind = None if pack or packing_policy is not None else formats.source_format(source)
    if kind and (chunk_size is not None or pack):
        raise ValueError("restore the encoded source without packing/chunking options")
    spec.update(pack=pack, chunk_size=chunk_size, source_format=kind, format_units=True,
                packing_policy=asdict(packing_policy) if packing_policy is not None else None,
                reserved_names=list(site.reserved), scratch_root=str(site.roots["lustre"]))
    if pack:
        suffix = ".gbi.tar.gz" if pack == "gzip" else ".gbi.tar"
        if not requested_target.name.endswith(suffix) or requested_target.is_dir():
            raise ValueError(f"--pack {pack} needs an exact destination filename ending in {suffix}")
        spec["target"] = str(requested_target) + (CHUNK_SUFFIX if chunk_size is not None else "")
        spec["directory"] = False
    elif kind:
        if spec["target_kind"] == "alluxio":
            raise ValueError("restore GBI archives/chunks to Lustre or FSS")
        spec["target"] = str(requested_target)
        spec["directory"] = False
    elif chunk_size is not None and not spec["directory"] and not source.is_symlink():
        if requested_target.name.endswith(CHUNK_SUFFIX):
            raise ValueError("--chunk-size adds .gbi-chunks automatically; give the original destination filename without .gbi-chunks")
        # Ordinary plan already appends the source name for a directory target.
        spec["target"] += CHUNK_SUFFIX
    if overlap(source, Path(spec["target"])):
        raise ValueError("encoded source and destination must not overlap")
    return spec


def _unit(source, target, action=None, size=None, **extra):
    observed = fingerprint(source)
    unit = {"source": str(source), "path": str(source), "empty": False,
            "expected_source": observed, "fingerprint": observed, **extra}
    if action:
        unit.update(format_action=action, format_target=str(target))
    if size is not None:
        unit["format_source_size"] = size
    return unit


def _restore_slot(source, name):
    """Reject obvious mixed loose/encoded sibling collisions before yielding."""
    sibling = source.parent / name
    if sibling != source and os.path.lexists(sibling):
        raise ValueError(f"mixed restore destination collides with source sibling: {sibling}")
    for suffix in (*ARCHIVE_SUFFIXES, CHUNK_SUFFIX,
                   *(archive + CHUNK_SUFFIX for archive in ARCHIVE_SUFFIXES)):
        other = source.parent / (name + suffix)
        if other != source and os.path.lexists(other) and formats.source_format(other):
            raise ValueError(f"multiple encoded sources would restore to {name!r}")


def _chunk_unit(source, target, nested=False, includes=(), exclusions=()):
    manifest = chunks.read_manifest(source)
    # A raw chunked file has known logical size. Potential packed content has
    # unknown decoded size until its archive manifest is read in the worker.
    possible_archive = formats.chunk_archive_marker(source, manifest)
    if not possible_archive and not matches(manifest["original_name"], includes, exclusions):
        return None
    if nested:
        _restore_slot(source, _unpacked_name(manifest["original_name"]) if possible_archive else manifest["original_name"])
        target = target.parent / manifest["original_name"]
    return _unit(source, target, "restore_chunks", None if possible_archive else manifest["size"],
                 format_auto_archive_target=nested, decoded_size_unknown=possible_archive)


def iter_units(specification, site, on_error=None, visit=lambda: None):
    """Yield worker-unit dictionaries; never descend into a recognized store."""
    spec = specification
    source, target = Path(spec["source"]), Path(spec["target"])
    includes, exclusions = spec.get("include", ()), spec.get("exclude", ())
    root = Path(spec["source_root"])
    if spec.get("pack"):
        yield _unit(source, target, "pack")
        return
    if spec.get("source_format") == "archive":
        yield _unit(source, target, "restore_archive", decoded_size_unknown=True)
        return
    if spec.get("source_format") == "chunks":
        unit = _chunk_unit(source, target, includes=includes, exclusions=exclusions)
        if unit is not None:
            yield unit
        return
    candidates = {}
    if spec.get("packing_policy"):
        planned = packing.plan(source, packing.PackingPolicy(**spec["packing_policy"]),
                               includes=includes, exclusions=exclusions, reserved_names=site.reserved)
        if not planned["complete"]:
            raise ValueError("small-file packing discovery is incomplete; raise the explicit scan budget or narrow the source")
        candidates = {item["source_relative"]: item for item in planned["candidates"]}

    def inspect(path, relative, nested):
        visit()
        name = "." if relative == Path(".") else relative.as_posix()
        if name in candidates:
            candidate = candidates[name]
            archive_target = target / candidate["archive_relative"]
            if spec.get("chunk_size") is not None:
                archive_target = Path(str(archive_target) + CHUNK_SUFFIX)
            yield _unit(path, archive_target, "pack", candidate["bytes"], pack="tar")
            return
        if site.is_reserved(path, root):
            return
        info = path.lstat()
        if stat.S_ISDIR(info.st_mode):
            if nested and path.name.endswith(CHUNK_SUFFIX):
                if formats.source_format(path) == "chunks":
                    unit = _chunk_unit(path, target / relative, nested=True, includes=includes, exclusions=exclusions)
                    if unit is not None:
                        yield unit
                    return
            with os.scandir(path) as children:
                empty = True
                for child in children:
                    empty = False
                    child_path = Path(child.path)
                    try:
                        yield from inspect(child_path, child_path.relative_to(source), True)
                    except (OSError, ValueError) as error:
                        if on_error is None:
                            raise
                        on_error(child_path, error)
                if empty and not includes:
                    yield {**_unit(path, target), "empty": True}
        elif stat.S_ISREG(info.st_mode) and nested and path.name.endswith(ARCHIVE_SUFFIXES):
            if formats.source_format(path) == "archive":
                _restore_slot(path, _unpacked_name(path.name))
                yield _unit(path, (target / relative).with_name(_unpacked_name(path.name)),
                            "restore_archive", decoded_size_unknown=True)
            elif matches(path.name, includes, exclusions):
                yield _ordinary(path, relative)
        elif matches(path.name, includes, exclusions):
            yield _ordinary(path, relative)

    def _ordinary(path, relative):
        if spec.get("chunk_size") is not None and path.is_file() and not path.is_symlink():
            output = target / relative if spec["directory"] else target
            if spec["directory"]:
                output = Path(str(output) + CHUNK_SUFFIX)
            return _unit(path, output, "chunk", path.stat().st_size)
        return _unit(path, target, size=path.lstat().st_size)

    yield from inspect(source, Path("."), False)


def foreground_units(specification, site):
    """Read-only bounded worker-side probe; unknown decoded size chooses Slurm."""
    limit = int(site.values["inline_bytes"])
    entry_limit = int(site.values["inline_scan_entries"])
    units, total, count, encoded_bytes = [], 0, 0, 0
    if specification.get("pack"):
        def visit():
            nonlocal count
            count += 1
            if count > entry_limit:
                raise ValueError("selection needs a larger scan (foreground discovery budget exceeded)")
        source = Path(specification["source"])
        for path, empty in entries(source, site, Path(specification["source_root"]),
                                   specification.get("include", ()), visit=visit,
                                   exclusions=specification.get("exclude", ())):
            if not empty:
                total += path.lstat().st_size
                if total > limit:
                    raise ValueError("packing source exceeds foreground byte budget")
        unit = next(iter_units(specification, site))
        unit["format_source_size"] = total
        return [unit]
    def visit():
        nonlocal count
        count += 1
        if count > entry_limit:
            raise ValueError("selection needs a larger scan (foreground discovery budget exceeded)")

    for unit in iter_units(specification, site, visit=visit):
        if count > entry_limit or unit.get("decoded_size_unknown"):
            return None
        if not unit["empty"]:
            total += unit.get("format_source_size", unit["expected_source"][3])
        encoded_bytes += len(json.dumps(unit))
        if total > limit or encoded_bytes > 16 * 1024 * 1024:
            return None
        units.append(unit)
    return units


def probe_units(specification, site):
    """Use the same bounded subprocess pattern as ordinary source discovery."""
    process = subprocess.Popen([sys.executable, "-B", "-m", "gbi_data.format_selection", str(site.path)],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    try:
        output, _ = process.communicate(json.dumps(specification), timeout=float(site.values["inline_probe_seconds"]))
        return json.loads(output) if process.returncode == 0 else None
    except (subprocess.TimeoutExpired, ValueError):
        if process.poll() is None:
            process.kill()
            try:
                process.wait(timeout=0.1)
            except subprocess.TimeoutExpired:
                pass
        return None
    finally:
        process.stdin.close()
        process.stdout.close()


def dry_run(specification, site):
    """Show selective policy decisions without creating outputs or staging."""
    if specification.get("packing_policy"):
        result = packing.plan(Path(specification["source"]), packing.PackingPolicy(**specification["packing_policy"]),
                              includes=specification.get("include", ()), exclusions=specification.get("exclude", ()),
                              reserved_names=site.reserved)
        result = _packing_preview(result)
        if specification.get("chunk_size"):
            result["staging_required"] = bool(result["candidates"])
            result["staging_location"] = "bounded, capacity-checked Lustre scratch"
            for candidate in result["candidates"]:
                candidate["archive_relative"] += CHUNK_SUFFIX
                candidate["staging_required"] = True
        return result
    return {"source": specification["source"], "destination": specification["target"],
            "pack": specification.get("pack"), "chunk_size": specification.get("chunk_size"),
            "source_format": specification.get("source_format"),
            "staging_required": bool(specification.get("pack") and specification.get("chunk_size")),
            "staging_location": "bounded, capacity-checked Lustre scratch" if specification.get("pack") and specification.get("chunk_size") else None}


if __name__ == "__main__":
    try:
        print(json.dumps(foreground_units(json.load(sys.stdin), Site(sys.argv[1]))))
    except (OSError, ValueError, RecursionError):
        sys.exit(1)
