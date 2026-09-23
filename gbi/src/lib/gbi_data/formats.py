"""Encoded logical transfers inside the existing supervised transfer worker.

The coordinator supplies exact ``format_target`` and ``format_action``. All
mutable journals and optional bounded archive staging stay in Lustre scratch.
Alluxio outputs use exclusive creation and independent readback, never rename
or truncate. Helpers do not start workers, submit jobs or publish history.
"""

import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import stat
import time

from . import archives, chunks, packing
from .selection import matches
from .storage import fingerprint
from .transfer import check_parent, digest, ensure_parent, receipt, write_json


def source_format(path):
    """Bounded content recognition; names alone never trigger extraction."""
    path = Path(path)
    if path.is_symlink():
        return None
    if path.is_dir():
        marker = path / "manifest.json"
        if marker.is_file() and not marker.is_symlink():
            # An ordinary directory may contain JSON of its own. Only a
            # versioned GBI format claim transfers ownership of interpretation.
            with marker.open("rb") as stream:
                prefix = stream.read(chunks.MAX_MANIFEST_BYTES + 1)
            if len(prefix) > chunks.MAX_MANIFEST_BYTES:
                return None
            try:
                value = json.loads(prefix)
            except (ValueError, UnicodeError):
                return None
            if isinstance(value, dict) and value.get("format") == chunks.FORMAT:
                chunks.read_manifest(path)
                return "chunks"
        return None
    if path.is_file() and archives.archive_marker(path):
        return "archive"
    return None


def chunk_archive_marker(store, manifest=None):
    """Read a bounded logical prefix for recognition, not payload acceptance.

    Deliberately bypass open_chunks' full-EOF acceptance on close. Full restore
    still uses open_chunks and verifies every part before claiming success.
    """
    store = Path(store)
    manifest = chunks.read_manifest(store) if manifest is None else manifest
    return archives.archive_marker(lambda: io.BufferedReader(chunks._ChunkReader(store, manifest), 8192))


def _identity(path):
    info = Path(path).lstat()
    return [info.st_dev, info.st_ino, stat.S_IFMT(info.st_mode)]


def _public_manifest(manifest):
    return {key: value for key, value in manifest.items() if not key.startswith("_")}


def _manifest_digest(manifest):
    return hashlib.sha256(json.dumps(_public_manifest(manifest), sort_keys=True,
                                     ensure_ascii=True, separators=(",", ":")).encode()).hexdigest()


class _Output:
    def __init__(self, stream, report, limit=None):
        self.stream, self.report, self.limit = stream, report, limit
        self.bytes = 0
        self.last = 0

    def write(self, data):
        if self.limit is not None and self.bytes + len(data) > self.limit:
            raise ValueError("archive exceeded its bounded staging reservation; source and stage kept")
        count = len(data) if self.stream is None else self.stream.write(data)
        self.bytes += count
        if time.monotonic() - self.last >= 2:
            self.report("packing", self.bytes)
            self.last = time.monotonic()
        return count


def _readback(operation, seconds):
    deadline = time.monotonic() + seconds
    while True:
        try:
            return operation()
        except (OSError, ValueError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(min(1, max(0, deadline - time.monotonic())))


def _owned_remove(path, identity, root):
    check_parent(path, root)
    info = path.lstat()
    if _identity(path) != identity or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise ValueError(f"owned temporary output changed; retained: {path}")
    path.unlink()


def _pack_options(task):
    return {"compression": task.get("pack", "tar"), "includes": list(task.get("include", ())),
            "exclusions": list(task.get("exclude", ())), "reserved_names": list(task.get("reserved_names", ())),
            "max_bytes": task.get("max_bytes")}


def _source_bytes(manifest):
    return sum(entry["size"] for entry in manifest["entries"] if entry["type"] in ("file", "hardlink"))


def _pack_direct(task, source, target, journal_path, journal, report, settle):
    options = _pack_options(task)
    if os.path.lexists(target):
        if journal.get("phase") == "writing" and journal.get("owned") == _identity(target):
            _owned_remove(target, journal["owned"], Path(task["target_root"]))
        else:
            # Rebuild only the source-side manifest, hashing while writing to a
            # null sink. No staging payload is created to compare a prior copy.
            manifest = archives.pack(source, _Output(None, report), **options)
            report("verifying archive", 0, _source_bytes(manifest))
            archives.verify_archive(target, manifest)
            return manifest, True
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as output:
        journal.update(phase="writing", owned=_identity(target))
        write_json(journal_path, journal)
        manifest = archives.pack(source, _Output(output, report), **options)
        output.flush()
        if task["target_kind"] != "alluxio":
            os.fsync(output.fileno())
        report("closing archive", _source_bytes(manifest), _source_bytes(manifest))
    journal.update(phase="written", manifest=manifest)
    write_json(journal_path, journal)
    report("verifying archive", 0, _source_bytes(manifest))
    _readback(lambda: archives.verify_archive(target, manifest), settle)
    return manifest, False


def _stage_archive(task, source, target, state, key, report):
    scratch = Path(task["scratch_root"])
    if state == scratch or scratch not in state.parents or state.resolve() != state or scratch.resolve() != scratch:
        raise ValueError("archive staging requires the configured Lustre scratch state")
    staging = state / "format-staging" / key
    staging.mkdir(parents=True, exist_ok=True, mode=0o700)
    if staging.resolve() != staging:
        raise ValueError("archive staging path contains a symlink")
    name = target.name.removesuffix(".gbi-chunks")
    suffix = ".gbi.tar.gz" if task.get("pack") == "gzip" else ".gbi.tar"
    if not name.endswith(suffix):
        raise ValueError(f"packed output must use an exact {suffix} pathname")
    payload, journal_path = staging / name, staging / "stage.json"
    options = _pack_options(task)
    intent = {"source": str(source), "options": options}
    if journal_path.exists():
        saved = json.loads(journal_path.read_text())
        if saved.get("intent") != intent:
            raise ValueError(f"retained archive stage is incomplete or belongs to another intent; inspect {staging}")
        if not payload.is_file() or payload.is_symlink() or _identity(payload) != saved["owned"]:
            raise ValueError("staged archive identity changed; stage retained")
        if saved.get("phase") == "writing":
            archives.verify_source(source, {"_source": saved.get("source_evidence")})
            # Incomplete stages have never been uploaded. Rebuild only our
            # inode, from a freshly hashed source; never reuse partial bytes.
            _owned_remove(payload, saved["owned"], state)
            journal_path.unlink()
        elif saved.get("phase") == "complete":
            archives.verify_source(source, saved["manifest"])
            # Include selected content hashes: coarse source mtimes alone cannot
            # make a stale stage reusable after an in-place source edit.
            current = archives.pack(source, _Output(None, report), **options)
            if _public_manifest(current) != _public_manifest(saved["manifest"]):
                raise ValueError("source content changed since staging; stage retained; choose a new destination")
            archives.verify_archive(payload, current)
            if digest(payload) != saved["sha256"]:
                raise ValueError("staged archive checksum changed; stage retained")
            return payload, current, saved, journal_path
        else:
            raise ValueError(f"unknown retained staging state; inspect {staging}")
    if os.path.lexists(payload):
        raise ValueError("unowned archive staging collision; existing stage retained")
    # Conservative reservation: padded payload, generous PAX headers, bounded
    # manifest, gzip overhead and end records. Capacity failure precedes writes.
    reserved = (".gbi", ".prefect-*", "transfer-locks", *options["reserved_names"])
    required = archives.MAX_MANIFEST + 1024 * 1024
    source_evidence = {"root": archives._observation(source.lstat()), "observations": {}, "reserved": list(reserved)}
    observation_bytes = 0
    for name, info, kind in archives._walk(source, reserved):
        source_evidence["observations"][name] = archives._observation(info)
        observation_bytes += len(archives._json([name, source_evidence["observations"][name]]))
        if observation_bytes > archives.MAX_MANIFEST:
            raise ValueError("source staging inventory exceeds the supported 64 MiB bound")
        if kind != "directory" and not matches(Path(name).name, options["includes"], options["exclusions"]):
            continue
        required += 4096 + (((info.st_size + 511) // 512) * 512 if kind == "file" else 0)
    required = required + required // 100 + 1024 * 1024
    if required > shutil.disk_usage(staging).free:
        raise ValueError(f"not enough Lustre scratch for bounded archive staging: need {required} bytes")
    configured_limit = task.get("staging_limit_bytes")
    if configured_limit is not None and required > configured_limit:
        raise ValueError(f"archive staging needs {required} bytes, exceeding the configured staging limit")
    fd = os.open(payload, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    saved = {"intent": intent, "phase": "writing", "owned": _identity(payload),
             "reserved_bytes": required, "source_evidence": source_evidence}
    with os.fdopen(fd, "wb") as output:
        write_json(journal_path, saved)
        manifest = archives.pack(source, _Output(output, report, required), **options)
        output.flush()
        os.fsync(output.fileno())
    report("verifying staged archive")
    archives.verify_archive(payload, manifest)
    saved.update(phase="complete", manifest=manifest, sha256=digest(payload))
    write_json(journal_path, saved)
    return payload, manifest, saved, journal_path


def _verify_chunk_payload(store):
    with chunks.open_chunks(store) as reader:
        while reader.read(archives.BLOCK):
            pass


def _verify_restored_archive(target, manifest):
    with archives._directory(target) as root_fd:
        for entry in manifest["entries"]:
            archives._verify_entry(root_fd, entry["path"], entry, preserve_directory=True)


def _chunk_snapshot(store):
    manifest = chunks.read_manifest(store)
    paths = [store / "manifest.json", store / ".gbi" / "complete.json"]
    paths += [store / ".gbi" / "parts" / part["name"] for part in manifest["parts"]]
    return manifest, {str(path): fingerprint(path) for path in paths}


def _require_chunk_snapshot(store, snapshot):
    manifest, files = snapshot
    if chunks.read_manifest(store) != manifest or any(fingerprint(Path(path)) != before for path, before in files.items()):
        raise ValueError("verified chunk destination changed; remaining sources retained")


def _path_snapshot(target, manifest=None):
    paths = [target]
    if manifest is not None:
        paths.extend(target / entry["path"] for entry in manifest["entries"])
    return {path: archives._observation(path.lstat()) for path in paths}


def _require_paths_unchanged(snapshot):
    if any(archives._observation(path.lstat()) != before for path, before in snapshot.items()):
        raise ValueError("restored destination changed during cleanup; remaining sources retained")


def _remove_chunk_source(store, snapshot, source_root, destination_unchanged):
    manifest, files = snapshot
    destination_unchanged()
    if chunks.read_manifest(store) != manifest:
        raise ValueError("source chunk manifest changed; source retained")
    _verify_chunk_payload(store)
    if any(fingerprint(Path(path)) != before for path, before in files.items()):
        raise ValueError("source chunk changed after verification; source retained")
    # Remove the completion claim first, then only the exact verified payloads.
    paths = [store / ".gbi" / "complete.json"] + [Path(path) for path in files if path != str(store / ".gbi" / "complete.json")]
    for path in paths:
        check_parent(path, source_root)
        if fingerprint(path) != files[str(path)]:
            raise ValueError("source chunk changed during cleanup; remaining entries retained")
        destination_unchanged()
        path.unlink()
    for directory in (store / ".gbi" / "parts", store / ".gbi", store):
        destination_unchanged()
        try:
            directory.rmdir()
        except OSError:
            pass  # Unknown additions are never recursively removed.


def _restore_chunk_file(task, source, target, journal_path, journal, report):
    manifest = chunks.read_manifest(source)
    source_manifest_digest = _manifest_digest(manifest)
    if os.path.lexists(target):
        if journal.get("phase") == "restoring" and journal.get("owned") == _identity(target):
            if journal.get("source_manifest_sha256") != source_manifest_digest:
                raise ValueError("chunk source manifest changed since partial restore; partial output retained; choose a new destination")
            _owned_remove(target, journal["owned"], Path(task["target_root"]))
        elif (target.is_file() and not target.is_symlink() and target.stat().st_size == manifest["size"]
              and digest(target) == manifest["sha256"]):
            _verify_chunk_payload(source)
            return True
        else:
            raise ValueError("destination differs from chunked file; existing data retained")
    # Own the exact inode before payload writes. chunks.restore_chunks creates
    # exclusively itself, so use its verified reader to retain this journal.
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as output:
        journal.update(phase="restoring", owned=_identity(target), source_manifest_sha256=source_manifest_digest)
        write_json(journal_path, journal)
        with chunks.open_chunks(source) as reader:
            total = 0
            for data in iter(lambda: reader.read(archives.BLOCK), b""):
                output.write(data)
                total += len(data)
                report("restoring chunks", total, manifest["size"])
        output.flush()
        os.fsync(output.fileno())
    report("verifying restored file", 0, manifest["size"])
    if _identity(target) != journal["owned"] or digest(target) != manifest["sha256"]:
        raise ValueError("restored file identity or checksum differs; source retained")
    return False


def transfer(task):
    """Perform one format unit; result matches the ordinary worker schema."""
    action = task["format_action"]
    if action not in ("pack", "chunk", "restore_archive", "restore_chunks"):
        raise ValueError("unknown encoded transfer action")
    source, target = Path(task["source"]), Path(task["format_target"])
    # Nested chunk stores retain the original logical filename. Only verified
    # archive content permits removing an archive suffix for reconstruction.
    if action == "restore_chunks":
        logical = chunks.read_manifest(source)
        chunk_is_archive = chunk_archive_marker(source, logical)
        if not chunk_is_archive and not matches(logical["original_name"], task.get("include", ()), task.get("exclude", ())):
            return {"bytes": 0, "source_size": 0, "deleted": False, "freed_bytes": 0,
                    "reused": False, "retained_reason": "logical chunked filename excluded by selection", "skipped": True}
        if chunk_is_archive and task.get("format_auto_archive_target"):
            for suffix in (".gbi.tar.gz", ".gbi.tar"):
                if target.name.endswith(suffix):
                    target = target.with_name(target.name[:-len(suffix)])
                    break
        elif not chunk_is_archive and target.is_dir():
            target /= logical["original_name"]
    source_root, target_root = Path(task["source_root"]), Path(task["target_root"])
    state = Path(task["state"])
    check_parent(source, source_root)
    ensure_parent(target, target_root)
    if source == target or source in target.parents or target in source.parents:
        raise ValueError("encoded source and destination overlap")
    before = fingerprint(source)
    if action == "chunk" and task.get("max_bytes") is not None and before[3] > task["max_bytes"]:
        raise ValueError("chunk source exceeds foreground byte budget; retry through Slurm")
    if task.get("expected_source") is not None and before != task["expected_source"]:
        raise ValueError("encoded source changed since selection; source retained")
    if state.resolve() != state:
        raise ValueError("format state path contains a symlink")
    key = hashlib.sha256(os.fsencode(target)).hexdigest()
    journal_path = state / "pending" / (key + ".format.json")
    intent = {"source": str(source), "target": str(target), "action": action,
              "pack": task.get("pack"), "chunk_size": task.get("chunk_size"),
              "include": task.get("include", []), "exclude": task.get("exclude", [])}
    source_size = task.get("format_source_size", before[3] if action.startswith("restore") or action == "chunk" else None)

    def report(phase, count=0, total=None):
        nonlocal source_size
        if total is not None:
            source_size = total
        detail = {"phase": phase, "bytes": count, "path": str(source)}
        if source_size is not None:
            detail["source_size"] = source_size
        write_json(Path(task["progress"]), detail)

    settle = task.get("settle_seconds", 0) if task["target_kind"] == "alluxio" else 0
    lock_fd = os.open(state / "locks" / key[:3], os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(lock_fd, "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        journal = {"intent": intent}
        if journal_path.exists():
            journal = json.loads(journal_path.read_text())
            if journal.get("intent") != intent:
                raise ValueError("unfinished encoded output belongs to a different selection; choose a new destination")
        report("preparing encoded transfer")
        staging = None
        retained_reason = None
        reused, deleted = False, False
        freed_bytes = 0
        if action == "pack":
            report("checking packing source")
            if task.get("packing_policy"):
                qualified = packing.plan(source, packing.PackingPolicy(**task["packing_policy"]),
                                         includes=task.get("include", ()), exclusions=task.get("exclude", ()),
                                         reserved_names=task.get("reserved_names", ()), allow_root=True)
                if not qualified["complete"] or [item["source_relative"] for item in qualified["candidates"]] != ["."]:
                    raise ValueError("small-file subtree changed or no longer qualifies; re-run the request to re-plan")
            suffix = ".gbi.tar.gz" if task.get("pack") == "gzip" else ".gbi.tar"
            if not target.name.removesuffix(".gbi-chunks").endswith(suffix):
                raise ValueError(f"packed output must be an exact pathname ending in {suffix}")
            if task.get("chunk_size"):
                stored = lambda: chunks.open_chunks(target)
                complete = None
                if target.is_dir():
                    try:
                        complete = chunks.read_manifest(target)
                    except ValueError:
                        pass
                if complete is not None:
                    if complete["chunk_size"] != task["chunk_size"]:
                        raise ValueError("existing archive uses a different chunk size; choose a new destination")
                    manifest = archives.pack(source, _Output(None, report), **_pack_options(task))
                    report("verifying archive")
                    archives.verify_archive(stored, manifest)
                    reused = True
                else:
                    report("checking Lustre archive staging")
                    payload, manifest, saved, stage_journal = _stage_archive(task, source, target, state, key, report)
                    staging = (payload, saved, stage_journal)
                    chunks.write_chunks(payload, target, state=state, chunk_size=task["chunk_size"], settle_seconds=settle,
                                        progress=lambda p: report(f"verified archive parts {p['parts']}/{p['total_parts']}",
                                                                  p["bytes"], p["total_bytes"]))
                    report("verifying archive")
                    archives.verify_archive(stored, manifest)
            else:
                manifest, reused = _pack_direct(task, source, target, journal_path, journal, report, settle)
                stored = target
            source_size = _source_bytes(manifest)
            evidence = {"manifest_sha256": _manifest_digest(manifest), "entries": len(manifest["entries"])}
        elif action == "chunk":
            manifest = chunks.write_chunks(source, target, state=state, chunk_size=task["chunk_size"], settle_seconds=settle,
                                           progress=lambda p: report(f"verified chunks {p['parts']}/{p['total_parts']}",
                                                                     p["bytes"], p["total_bytes"]))
            source_size = manifest["size"]
            evidence = {"sha256": manifest["sha256"], "parts": len(manifest["parts"])}
        else:
            if task["target_kind"] == "alluxio":
                raise ValueError("restore archives/chunks to Lustre or FSS, not to an Alluxio mount")
            chunk_snapshot = _chunk_snapshot(source) if action == "restore_chunks" else None
            stored = (lambda: chunks.open_chunks(source)) if chunk_snapshot else source
            is_archive = action == "restore_archive" or chunk_is_archive
            if is_archive:
                result = archives.restore(stored, target, includes=task.get("include", ()),
                                          exclusions=task.get("exclude", ()), phase=report)
                manifest = result["manifest"]
                full_size = _source_bytes(manifest)
                source_size = sum(entry["size"] for entry in manifest["entries"]
                                  if entry["type"] in ("file", "hardlink")
                                  and matches(Path(entry["path"]).name, task.get("include", ()), task.get("exclude", ())))
                retained_reason = "filtered restore retains the complete source archive" if result["partial"] else None
                evidence = {"manifest_sha256": _manifest_digest(manifest), "entries": result["selected"],
                            "partial": result["partial"], "metadata_limits": result["metadata_limits"],
                            "restored_bytes": source_size, "verified_container_bytes": full_size}
            else:
                reused = _restore_chunk_file(task, source, target, journal_path, journal, report)
                manifest = chunk_snapshot[0]
                source_size = manifest["size"]
                evidence = {"sha256": manifest["sha256"], "parts": len(manifest["parts"])}
        report("recording verified transfer", source_size, source_size)
        record = {"event": "verified", "source": str(source), "destination": str(target),
                  "kind": action, "bytes": source_size, "reused": reused, "time": time.time(), **evidence}
        journal.update(phase="verified", record=record)
        write_json(journal_path, journal)
        receipt(Path(task["receipt"]), record)
        if task.get("delete") and not retained_reason:
            report("rechecking source before cleanup", source_size, source_size)
            if action == "pack":
                snapshot = _chunk_snapshot(target) if task.get("chunk_size") else None
                guard = (lambda: _require_chunk_snapshot(target, snapshot)) if snapshot else None
                archives.cleanup_source(source, manifest, stored, destination_unchanged=guard)
            elif action == "restore_chunks":
                destination_snapshot = _path_snapshot(target, manifest if is_archive else None)
                if is_archive:
                    _verify_restored_archive(target, manifest)
                elif digest(target) != manifest["sha256"]:
                    raise ValueError("restored destination changed after verification; source retained")
                _remove_chunk_source(source, chunk_snapshot, source_root,
                                     lambda: _require_paths_unchanged(destination_snapshot))
            else:
                check_parent(source, source_root)
                if fingerprint(source) != before:
                    raise ValueError("source changed after verification; source retained")
                if action == "chunk":
                    destination_snapshot = _chunk_snapshot(target)
                    if digest(source) != manifest["sha256"]:
                        raise ValueError("source content changed after chunking; source retained")
                    _verify_chunk_payload(target)
                else:
                    destination_snapshot = _path_snapshot(target, manifest)
                    _verify_restored_archive(target, manifest)
                    archives.verify_archive(source, manifest)
                if fingerprint(source) != before:
                    raise ValueError("source changed before cleanup; source retained")
                if action == "chunk":
                    _require_chunk_snapshot(target, destination_snapshot)
                else:
                    _require_paths_unchanged(destination_snapshot)
                source.unlink()
            deleted = True
            freed_bytes = (_source_bytes(manifest) if action == "pack" else
                           chunk_snapshot[0]["size"] if action == "restore_chunks" else before[3])
            receipt(Path(task["receipt"]), {**record, "event": "deleted", "time": time.time()})
        if staging:
            payload, saved, stage_journal = staging
            if digest(payload) != saved["sha256"]:
                raise ValueError("accepted archive stage changed; stage retained for inspection")
            _owned_remove(payload, saved["owned"], state)
            stage_journal.unlink()
            try:
                payload.parent.rmdir()
            except OSError:
                pass
        journal_path.unlink()
        return {"bytes": source_size, "source_size": source_size, "deleted": deleted,
                "reused": reused, "retained_reason": retained_reason, "freed_bytes": freed_bytes,
                **{key: evidence[key] for key in ("restored_bytes", "verified_container_bytes") if key in evidence}}
