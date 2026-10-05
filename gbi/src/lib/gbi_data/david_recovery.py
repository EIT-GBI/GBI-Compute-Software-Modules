"""One-off recovery of an already complete archive that supersets its source.

This is deliberately separate from normal archive execution. It never packs to
or overwrites the destination. It verifies the existing archive, then resumes
source cleanup through the maintained archive cleanup API and records explicit
recovery provenance.
"""

import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import time
import uuid

from . import archive_state, archives, chunks, formats


def _key(value):
    return hashlib.sha256(os.fsencode(value)).hexdigest()


def _manifest_payload_bytes(manifest):
    return sum(entry["size"] for entry in manifest["entries"]
               if entry["type"] != "directory")


def _entry_signature(entry):
    fields = ("type", "size", "mode", "sha256", "target")
    return tuple((field, entry.get(field)) for field in fields if field in entry)


def _chunk_snapshot(target, outer_manifest):
    paths = [target, target / ".gbi", target / ".gbi" / "parts",
             target / "manifest.json", target / ".gbi" / "complete.json"]
    paths.extend(target / ".gbi" / "parts" / item["name"]
                 for item in outer_manifest["parts"])

    def signature(path):
        info = path.lstat()
        return (info.st_dev, info.st_ino, info.st_mode, info.st_size,
                info.st_mtime_ns, info.st_ctime_ns)

    return {str(path): signature(path) for path in paths}


def _write_new_json(path, value):
    """Create an immutable JSON receipt without replacing an existing file."""
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.parent.is_symlink() or path.parent.resolve() != path.parent.absolute():
        raise ValueError("recovery receipt parent contains a symlink")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        json.dump(value, stream, sort_keys=True, ensure_ascii=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    parent_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(parent_fd)
    finally:
        os.close(parent_fd)


def recover_completed_superset_unit(
        state_root, run_id, source_relative_sha256, expected_plan_sha256,
        provenance_id=None):
    """Finish one frozen chunk unit whose valid archive contains source state.

    ``source_relative_sha256`` identifies one unit in the saved plan without
    copying a potentially long source selection into a command or log. The
    saved plan, original destination, run request, filters and unit mapping are
    read in place and never changed. A new provenance record is created before
    cleanup. The original unit result is created exclusively after full
    cleanup and destination readback succeed.

    The caller must freeze all writers to both source and destination first.
    """
    state_root = Path(state_root).absolute()
    if state_root.is_symlink() or not state_root.is_dir() or state_root.resolve() != state_root:
        raise ValueError("GBI state root must be a real directory")
    if not run_id or run_id in (".", "..") or Path(run_id).name != run_id:
        raise ValueError("run id must be one path component")
    if len(source_relative_sha256) != 64 or any(c not in "0123456789abcdef" for c in source_relative_sha256):
        raise ValueError("source unit identity must be a lowercase SHA-256")

    request_path = state_root / "runs" / run_id / "request.json"
    request = json.loads(request_path.read_text())
    old_target = Path(request["target"])
    plan_key = _key(str(old_target))
    plan_root = state_root / "archive-plans"
    plan_path = plan_root / f"{plan_key}.json"
    units_root = plan_root / f"{plan_key}.units"
    plan_bytes = plan_path.read_bytes()
    plan_sha256 = hashlib.sha256(plan_bytes).hexdigest()
    if plan_sha256 != expected_plan_sha256:
        raise ValueError("saved archive plan differs from the authorized frozen plan")
    record = json.loads(plan_bytes)
    plan = record["plan"]
    if (record.get("intent", {}).get("target") != str(old_target)
            or Path(plan["destination"]) != old_target):
        raise ValueError("saved plan target does not match the run request")

    matches = [unit for unit in plan["units"]
               if _key(unit.get("source_relative", "")) == source_relative_sha256]
    if len(matches) != 1:
        raise ValueError("source identity does not select exactly one saved archive unit")
    unit = matches[0]
    if unit.get("action") != "pack" or not unit.get("chunk_size"):
        raise ValueError("recovery only supports saved chunked archive units")
    for name in (unit.get("source_relative"), unit.get("target_relative")):
        if (not isinstance(name, str) or Path(name).is_absolute()
                or ".." in Path(name).parts or "\\" in name):
            raise ValueError("saved unit path is not a confined POSIX relative path")

    source = Path(plan["source"]) / unit["source_relative"]
    target = old_target / unit["target_relative"]
    receipt_path = units_root / f"{source_relative_sha256}.json"
    if receipt_path.exists() or receipt_path.is_symlink():
        raise ValueError("archive unit already has a completion result")
    if units_root.is_symlink() or not units_root.is_dir():
        raise ValueError("saved archive unit receipt directory is unavailable")
    lock_path = plan_root / f"{plan_key}.lock"
    if lock_path.is_symlink() or not lock_path.is_file():
        raise ValueError("saved plan lock is unavailable")

    # This is the same lock used by archive_state.prepared. It prevents a
    # concurrent resume from changing the plan or source/destination mapping.
    with lock_path.open("rb") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError("archive plan is active; stop its workers before recovery") from error

        if hashlib.sha256(plan_path.read_bytes()).hexdigest() != expected_plan_sha256:
            raise ValueError("saved archive plan changed while acquiring its lock")
        if json.loads(request_path.read_text()) != request:
            raise ValueError("saved run request changed while acquiring the plan lock")

        for journal in (
                state_root / "pending" / f"{_key(str(target))}.format.json",
                state_root / "pending" / f"{_key(str(target))}.json",
                state_root / "chunks" / "pending" / f"{_key(str(target))}.json",
                state_root / "format-staging" / _key(str(target)) / "stage.json"):
            if journal.exists() or journal.is_symlink():
                raise ValueError("archive destination has an unresolved transfer journal")

        if (source.is_symlink() or not source.is_dir() or source.resolve() != source.absolute()
                or target.is_symlink() or not target.is_dir() or target.resolve() != target.absolute()):
            raise ValueError("source and chunk destination must be real directories without symlink parents")
        if not request.get("delete") or request.get("target_kind") != "alluxio":
            raise ValueError("saved run is not the approved move to the original Alluxio archive target")
        if (request.get("source") != str(plan["source"])
                or request.get("target") != str(plan["destination"])):
            raise ValueError("saved request and archive plan source/target differ")

        outer = chunks.read_manifest(target)
        if (outer.get("state") != "complete"
                or outer.get("chunk_size") != unit["chunk_size"]):
            raise ValueError("existing chunk layout is incomplete or differs from the saved plan")
        target_layout = _chunk_snapshot(target, outer)

        # Hash the live selected source to a null sink; this creates only a
        # transient manifest in memory and does not write an archive.
        with open(os.devnull, "wb") as sink:
            current = archives.pack(source, sink, compression=None,
                                    includes=request["include"],
                                    exclusions=request["exclude"],
                                    reserved_names=request["reserved_names"])
        planned_root = unit.get("expected_source")
        current_root = current["_source"]["root"]
        if (not isinstance(planned_root, list) or len(planned_root) < 3
                or current_root[:3] != planned_root[:3]):
            raise ValueError("source root identity differs from the frozen plan")
        archived = archives.inspect_archive(lambda: chunks.open_chunks(target))
        if archived is None:
            raise ValueError("existing destination is not a marked GBI archive")

        archived_by_path = {entry["path"]: entry for entry in archived["entries"]}
        current_by_path = {entry["path"]: entry for entry in current["entries"]}
        if len(archived_by_path) != len(archived["entries"]):
            raise ValueError("stored archive contains duplicate manifest paths")
        missing = set(current_by_path) - set(archived_by_path)
        if missing:
            raise ValueError("existing archive is missing a currently selected source path")
        selected = archives._selection(request["include"], request["exclude"], "basename")
        reserved = (".gbi", ".prefect-*", "transfer-locks", *request["reserved_names"])
        for entry in archived["entries"]:
            if entry["type"] != "directory" and (
                    not selected(entry["path"]) or archives._reserved(entry["path"], reserved)):
                raise ValueError("stored archive contains a path outside the saved selection")
        metadata_differences = []
        for path, entry in current_by_path.items():
            saved = archived_by_path[path]
            if _entry_signature(entry) != _entry_signature(saved):
                raise ValueError("current source content/type/mode/link differs from the stored archive")
            if entry.get("mtime_ns") != saved.get("mtime_ns"):
                if entry["type"] != "directory":
                    raise ValueError("current source file metadata differs from the stored archive")
                metadata_differences.append(path)

        payload_bytes = _manifest_payload_bytes(archived)
        current_bytes = _manifest_payload_bytes(current)
        if payload_bytes != unit["bytes"] or current_bytes > payload_bytes:
            raise ValueError("stored archive payload bytes do not match the original planned unit")
        if outer["size"] != sum(part["size"] for part in outer["parts"]):
            raise ValueError("chunk part sizes do not match the complete chunk manifest")

        # `resume_cleanup_source` normally receives the original exact source
        # observations. In this recovery, the current source is a subset, so
        # add tombstones for archive-only members. They are absent paths and
        # therefore only identify prior cleanup; no source stat is fabricated
        # for any extant file.
        combined = {key: value for key, value in archived.items() if not key.startswith("_")}
        source_evidence = json.loads(json.dumps(current["_source"]))
        observations = source_evidence["observations"]
        for index, entry in enumerate(archived["entries"], 1):
            if entry["path"] in observations:
                continue
            kind = {"directory": stat.S_IFDIR, "symlink": stat.S_IFLNK}.get(
                entry["type"], stat.S_IFREG)
            observations[entry["path"]] = [0, -index, kind, entry.get("size", 0),
                                           entry.get("mtime_ns", 0), 0]
        combined["_source"] = source_evidence

        outer_digest = hashlib.sha256(json.dumps(outer, sort_keys=True,
                                                 separators=(",", ":")).encode()).hexdigest()

        def destination_unchanged():
            if chunks.read_manifest(target) != outer or _chunk_snapshot(target, outer) != target_layout:
                raise ValueError("chunk destination changed during source cleanup")

        recovery_dir = state_root / "recovery-receipts" / run_id
        provenance_id = provenance_id or uuid.uuid4().hex
        if Path(provenance_id).name != provenance_id:
            raise ValueError("provenance id must be one path component")
        provenance_path = recovery_dir / f"{source_relative_sha256}.{provenance_id}.json"
        if provenance_path.exists() or provenance_path.is_symlink():
            raise ValueError("immutable recovery provenance already exists")
        archived_digest = formats._manifest_digest(archived)
        source_digest = formats._manifest_digest(current)
        provenance = {
            "schema": "gbi-archive-superset-recovery-v1",
            "status": "preflight-verified; cleanup follows",
            "run_id": run_id,
            "source_relative_sha256": source_relative_sha256,
            "plan_sha256": plan_sha256,
            "source_manifest_sha256": source_digest,
            "stored_manifest_sha256": archived_digest,
            "chunk_layout_sha256": outer_digest,
            "planned_payload_bytes": unit["bytes"],
            "stored_payload_bytes": payload_bytes,
            "current_source_payload_bytes": current_bytes,
            "stored_manifest_entries": len(archived["entries"]),
            "current_source_manifest_entries": len(current["entries"]),
            "archive_only_entries": len(archived_by_path) - len(current_by_path),
            "directory_mtime_differences": len(metadata_differences),
            "operation": "verified existing archive superset; resume per-file guarded source cleanup",
            "created_unix_ns": time.time_ns(),
        }
        _write_new_json(provenance_path, provenance)

        cleanup = archives.resume_cleanup_source(
            source, combined, lambda: chunks.open_chunks(target),
            destination_unchanged=destination_unchanged)
        destination_unchanged()
        after = archives.inspect_archive(lambda: chunks.open_chunks(target))
        if after is None or formats._manifest_digest(after) != archived_digest:
            raise ValueError("stored archive changed after source cleanup; no completion result written")
        with open(os.devnull, "wb") as sink:
            remaining = archives.pack(source, sink, compression=None,
                                      includes=request["include"],
                                      exclusions=request["exclude"],
                                      reserved_names=request["reserved_names"])
        if any(entry["type"] != "directory" for entry in remaining["entries"]):
            raise ValueError("selected source files remain after resumed cleanup")

        completion = {
            "bytes": unit["bytes"],
            "source_size": unit["bytes"],
            "deleted": True,
            "reused": True,
            "retained_reason": None,
            "freed_bytes": cleanup["freed_bytes"],
            "manifest_sha256": archived_digest,
            "entries": len(after["entries"]),
            "recovery_provenance": str(provenance_path),
            "recovery_kind": "verified-existing-archive-superset",
        }
        _write_new_json(receipt_path, completion)
        task = {
            "target_root": request["target_root"],
            "target_kind": request["target_kind"],
            "include": request["include"],
            "exclude": request["exclude"],
            "reserved_names": request["reserved_names"],
            "source": str(source),
            "archive_destination": str(target),
            "archive_completed": completion,
            "format_action": "pack",
            "chunk_size": unit["chunk_size"],
            "empty": False,
        }
        replay = archive_state.resume_completed(task)
        return {"provenance": str(provenance_path), "result": str(receipt_path),
                "freed_bytes": cleanup["freed_bytes"],
                "removed": cleanup["removed"], "manifest_sha256": archived_digest,
                "entries": len(after["entries"]), "replay_deleted": replay["deleted"]}
