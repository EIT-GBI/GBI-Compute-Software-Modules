"""Persist archive partitions across ordinary CLI retries.

The destination selects one immutable intent/partition. A retry must use it,
even after successful moves have removed parts of the original source tree.
The coordinator holds the destination lock until its workers have stopped.
"""

from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import time

from . import archive_plan, archives, chunks, deadlines, formats
from .selection import matches
from .storage import fingerprint
from .transfer import check_parent, digest, write_json


def _key(value):
    return hashlib.sha256(os.fsencode(value)).hexdigest()


@contextmanager
def prepared(spec, site, home):
    """Freeze the complete mapping before allowing the first payload write."""
    for name in ("source", "target"):
        if name + "_kind" in spec:
            path, kind, storage_root = site.classify(spec[name])
            if (str(path), kind, str(storage_root)) != (spec[name], spec[name + "_kind"], spec[name + "_root"]):
                raise ValueError("storage route changed since submission; source retained")
    root = home / "archive-plans"
    check_parent(root, site.roots["lustre"])
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    check_parent(root / "placeholder", site.roots["lustre"])
    key = _key(spec["target"])
    intent = {name: spec.get(name) for name in
              ("source", "target", "include", "exclude", "archive_chunk_threshold")}
    with (root / (key + ".lock")).open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError("this archive destination already has an active transfer; inspect its status") from error
        saved = root / (key + ".json")
        if saved.exists():
            record = json.loads(saved.read_text())
            if record.get("intent") != intent:
                raise ValueError("destination already has a different saved archive plan; do not mix layouts")
        else:
            destination = Path(spec["target"])
            if destination.exists():
                if not destination.is_dir() or next(destination.iterdir(), None) is not None:
                    raise ValueError("archive destination is not empty and has no matching saved plan; inspect existing data before recovery")
            last_report = 0.0

            def progress():
                nonlocal last_report
                if time.monotonic() - last_report >= 1:
                    deadlines.event("planning archive layout", spec["source"])
                    last_report = time.monotonic()

            # This path runs on an execution node, bounded by Slurm resources,
            # the operation timeout and the maintained mount-I/O supervisor.
            # Do not reuse the five-second login-node probe budget here.
            plan = archive_plan.plan_archive(
                spec["source"], spec["target"], includes=spec.get("include", ()),
                exclusions=spec.get("exclude", ()), reserved_names=site.reserved,
                chunk_threshold_bytes=spec["archive_chunk_threshold"], max_entries=None,
                max_seconds=float(site.values["file_timeout"]), visit=progress)
            record = {"intent": intent, "plan": plan.as_dict()}
            write_json(saved, record)
        results = root / (key + ".units")
        results.mkdir(exist_ok=True, mode=0o700)
        units = []
        for planned in record["plan"]["units"]:
            path = Path(spec["source"]) / planned["source_relative"]
            destination = Path(spec["target"]) / planned["target_relative"]
            result_path = results / (_key(planned["source_relative"]) + ".json")
            observed = planned["expected_source"]
            unit = {"source": str(path), "path": str(path),
                    "expected_source": [observed[0], observed[1], stat.S_IFMT(observed[2]), *observed[3:5]],
                    "empty": planned["action"] == "directory",
                    "format_source_size": planned["bytes"],
                    "archive_result": str(result_path), "archive_destination": str(destination)}
            if planned["action"] == "pack":
                unit.update(format_action="pack", format_target=str(destination),
                            pack="tar", chunk_size=planned["chunk_size"])
            if result_path.is_file():
                completed = json.loads(result_path.read_text())
                if completed.get("deleted") or (planned["action"] != "pack" and not os.path.lexists(path)):
                    unit["archive_completed"] = completed
            units.append(unit)
        yield {**spec, "selection": units, "archive_plan": record["plan"]}


def resume_completed(task):
    """Reverify completed destinations, never count their source removal twice."""
    completed = task["archive_completed"]
    source, target = Path(task["source"]), Path(task["archive_destination"])
    check_parent(target, Path(task["target_root"]))
    if os.path.lexists(source):
        if source.is_symlink() or not source.is_dir():
            raise ValueError("a completed archive source has reappeared; source retained")
        # Packed roots remain as empty directories; excluded members may remain.
        for name, _, kind in archives._walk(source, task.get("reserved_names", ())):
            if kind != "directory" and matches(Path(name).name, task.get("include", ()), task.get("exclude", ())):
                raise ValueError("a completed archive source has new selected files; source retained")
    if task.get("format_action") == "pack":
        stored = (lambda: chunks.open_chunks(target)) if task.get("chunk_size") else target
        manifest = archives.inspect_archive(stored)
        if manifest is None or formats._manifest_digest(manifest) != completed["manifest_sha256"]:
            raise ValueError("completed archive destination no longer matches its receipt")
    elif not task["empty"]:
        if completed.get("kind") == "symlink" and task["target_kind"] == "alluxio":
            target = target.with_name(target.name + ".rclonelink")
        before = fingerprint(target)
        actual = (hashlib.sha256(os.fsencode(os.readlink(target))).hexdigest()
                  if target.is_symlink() else digest(target))
        if actual != completed["sha256"] or fingerprint(target) != before:
            raise ValueError("completed archive file no longer matches its receipt")
    elif not target.is_dir():
        raise ValueError("completed archive directory is missing")
    return {**completed, "reused": True, "deleted": False, "freed_bytes": 0}
