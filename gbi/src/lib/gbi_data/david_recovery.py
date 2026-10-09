"""One-off recovery of an already complete archive that supersets its source.

This is deliberately separate from normal archive execution. It never packs to
or overwrites the destination. It verifies the existing archive, then resumes
source cleanup through the maintained archive cleanup API and records explicit
recovery provenance.
"""

import argparse
from dataclasses import dataclass
import fcntl
import hashlib
import json
import os
import tempfile
from pathlib import Path
import stat
import time
import uuid

from gbi_data import archive_state, archives, chunks, formats


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


def _verified_safety_copy(state_root, run_id, unit_key, target, outer, archive_digest):
    """Persist one independently verified tar stream on the user's Lustre state."""
    root = state_root / "recovery-payloads" / run_id
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.is_symlink() or root.resolve() != root.absolute():
        raise ValueError("recovery payload directory contains a symlink")
    destination = root / f"{unit_key}.{outer['sha256']}.tar"
    if os.path.lexists(destination):
        before = _safety_copy_snapshot(destination)
        digest = hashlib.sha256()
        size = 0
        with destination.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
                size += len(block)
        if size != outer["size"] or digest.hexdigest() != outer["sha256"]:
            raise ValueError("existing recovery payload differs from the verified chunk archive")
        stored = archives.inspect_archive(destination)
        if stored is None or formats._manifest_digest(stored) != archive_digest:
            raise ValueError("existing recovery payload does not match the archived manifest")
        if _safety_copy_snapshot(destination) != before:
            raise ValueError("recovery payload changed during its readback")
        return destination, before, digest.hexdigest(), size

    fd, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.",
                                          suffix=".partial", dir=root)
    temporary = Path(temporary_name)
    digest = hashlib.sha256()
    size = 0
    try:
        with os.fdopen(fd, "wb") as stream, chunks.open_chunks(target) as reader:
            for block in iter(lambda: reader.read(1024 * 1024), b""):
                stream.write(block)
                digest.update(block)
                size += len(block)
            stream.flush()
            os.fchmod(stream.fileno(), 0o400)
            os.fsync(stream.fileno())
        actual_digest = digest.hexdigest()
        if size != outer["size"] or actual_digest != outer["sha256"]:
            raise ValueError("streamed recovery payload differs from the complete chunk manifest")
        os.link(temporary, destination, follow_symlinks=False)
        temporary.unlink()
        parent_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
        stored = archives.inspect_archive(destination)
        if stored is None or formats._manifest_digest(stored) != archive_digest:
            raise ValueError("reconstructed recovery payload does not match the archived manifest")
        snapshot = _safety_copy_snapshot(destination)
        return destination, snapshot, actual_digest, size
    except BaseException:
        if temporary.exists():
            temporary.unlink()
        raise


def _safety_copy_snapshot(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o400:
        raise ValueError("recovery payload must remain a read-only regular file")
    return (info.st_dev, info.st_ino, info.st_mode, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns)


def _read_pinned_json(root, reference):
    relative = Path(reference["path"])
    if (relative.is_absolute() or ".." in relative.parts
            or not isinstance(reference.get("sha256"), str)):
        raise ValueError("native cleanup evidence path is not confined")
    path = root / relative
    if path.is_symlink() or path.resolve() != path.absolute():
        raise ValueError("native cleanup evidence path contains a symlink")
    with path.open("rb") as stream:
        raw = stream.read(64 * 1024 * 1024 + 1)
    if len(raw) > 64 * 1024 * 1024:
        raise ValueError("native cleanup evidence exceeds the supported size")
    digest = hashlib.sha256(raw).hexdigest()
    if digest != reference["sha256"]:
        raise ValueError(f"native cleanup evidence changed: {relative}")
    return json.loads(raw), digest


def _relative_evidence_reference(path):
    value = Path(path)
    if not value.is_absolute():
        return str(value)
    parts = value.parts
    for marker in ("metadata", "jobs"):
        if marker in parts:
            return str(Path(marker, *parts[parts.index(marker) + 1:]))
    raise ValueError("native cleanup evidence reference is outside its evidence tree")


def _source_identity(path):
    path = Path(path).absolute()
    if path.is_symlink() or path.resolve() != path:
        raise ValueError("retained staged archive path contains a symlink")
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("retained staged archive must remain a regular file")
    return [info.st_dev, info.st_ino, info.st_mode, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns]


def _file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


@dataclass(frozen=True)
class NativeArchiveProof:
    """Validated native destination proof for one exact recovery unit."""

    binding: dict
    native: dict
    source_proof: dict
    verified_archive: archives.VerifiedArchiveEvidence
    identities: tuple

    @classmethod
    def load(cls, binding_path, expected_sha256, evidence_root, *, source, target,
             source_relative, current, outer, target_dir):
        evidence_root = Path(evidence_root).resolve(strict=True)
        binding_path = Path(binding_path).absolute()
        try:
            relative_binding = binding_path.relative_to(evidence_root)
        except ValueError as error:
            raise ValueError("native cleanup binding is outside its evidence root") from error
        binding, binding_sha = _read_pinned_json(
            evidence_root, {"path": str(relative_binding), "sha256": expected_sha256})
        if (binding.get("schema") != "strand2-native-archive-cleanup-binding-v1"
                or binding.get("source_cleanup_accepted") is not False
                or binding.get("whole_collection_accepted") is not False
                or binding.get("source_or_destination_mutation") is not False):
            raise ValueError("native cleanup binding is outside the destination-only cleanup scope")
        if (binding.get("source_fss") != str(source)
                or binding.get("canonical_target") != str(target)
                or binding.get("expected_source_relative") != source_relative
                or binding.get("archive_sha256") != outer.get("sha256")
                or binding.get("archive_bytes") != outer.get("size")
                or binding.get("part_count") != len(outer.get("parts", ()))
                or binding.get("outer_manifest_sha256") != hashlib.sha256(
                    (Path(target_dir) / "manifest.json").read_bytes()).hexdigest()):
            raise ValueError("native cleanup binding does not match the frozen target layout")
        if binding.get("current_identity_revalidation_required") is not True:
            raise ValueError("native cleanup binding lacks current-identity guards")

        refs = ("raw_proof", "process_terminal", "source_proof", "producer_semantic_proof",
                "frozen_semantic_worklist")
        documents = {name: _read_pinned_json(evidence_root, binding[name])[0] for name in refs}
        native = documents["raw_proof"]
        source_proof = documents["source_proof"].get("source_proof")
        terminal = documents["process_terminal"]
        producer = documents["producer_semantic_proof"]
        if not isinstance(source_proof, dict):
            raise ValueError("native proof source manifest is unavailable")
        if (terminal.get("returncode") != 0
                or terminal.get("stdout_sha256") != binding["raw_proof"]["sha256"]
                or terminal.get("stderr_bytes") != 0):
            raise ValueError("native destination evidence is not bound to successful terminal execution")
        producer_proof = (producer.get("capture", {}).get("proof")
                          or producer.get("remote", {}).get("value", {}))
        if (producer_proof.get("event") != "historical-chunk-reconstruction-verified"
                or producer_proof.get("root") != binding.get("prefix", "").rstrip("/")
                or producer_proof.get("bytes") != binding.get("archive_bytes")
                or producer_proof.get("sha256") != binding.get("archive_sha256")
                or producer_proof.get("parts") != binding.get("part_count")
                or producer_proof.get("archive_per_file_manifest_readback") is not True
                or producer_proof.get("original_manifest_unchanged") is not True
                or producer_proof.get("original_stage_unchanged") is not True
                or producer_proof.get("source_fss_mutation") is True
                or producer.get("source_fss_mutation") is True
                or producer.get("source_or_canonical_mutation") is True
                or producer.get("remote", {}).get("source_or_destination_mutation") is True):
            raise ValueError("original producer did not verify the retained per-file archive manifest")

        worklist = documents["frozen_semantic_worklist"]
        index = binding.get("frozen_worklist_row_index")
        rows = worklist.get("rows", ())
        if type(index) is not int or not 0 <= index < len(rows):
            raise ValueError("native cleanup binding selects no frozen worklist row")
        row = rows[index]
        frozen = row.get("row", {})
        retained = row.get("full_retained_tar_proof", {})
        manifest_sha = binding.get("per_file_manifest_sha256")
        if (frozen.get("source") != str(source)
                or frozen.get("canonical_target") != str(target)
                or frozen.get("root", "") + "/" != binding.get("prefix")
                or frozen.get("manifest_sha256") != binding.get("outer_manifest_sha256")
                or retained.get("source") != str(source)
                or retained.get("payload") != binding.get("retained_staged_tar")
                or retained.get("manifest_sha256") != manifest_sha
                or retained.get("sha256") != binding.get("archive_sha256")
                or retained.get("bytes") != binding.get("archive_bytes")):
            raise ValueError("native cleanup binding differs from the frozen semantic worklist")

        canonical_source_proof = hashlib.sha256(json.dumps(
            source_proof, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode()).hexdigest()
        prefix = binding.get("prefix")
        objects = native.get("objects", ())
        if (native.get("schema") not in ("gbi-row13-native-whole-proof-v1",
                                           "gbi-row14-native-whole-proof-v1")
                or native.get("source_proof_sha256") != binding["source_proof"]["sha256"]
                or native.get("source_proof_canonical_sha256") != canonical_source_proof
                or native.get("source_path") != binding.get("retained_staged_tar")
                or native.get("source_identity") != binding.get("retained_staged_tar_source_identity")
                or source_proof.get("source") != binding.get("retained_staged_tar")
                or source_proof.get("source_identity") != binding.get("retained_staged_tar_source_identity")
                or source_proof.get("archive_sha256") != binding.get("archive_sha256")
                or source_proof.get("size") != binding.get("archive_bytes")
                or source_proof.get("manifest_sha256") != binding.get("outer_manifest_sha256")
                or native.get("archive_sha256") != binding.get("archive_sha256")
                or native.get("archive_size") != binding.get("archive_bytes")
                or native.get("manifest_sha256") != binding.get("outer_manifest_sha256")
                or native.get("bucket") != binding.get("bucket")
                or native.get("namespace") != binding.get("namespace")
                or native.get("prefix") != prefix
                or len(objects) != len(outer.get("parts", ()))
                or len(native.get("controls", ())) != 2
                or native.get("payload_get_http_attempts") != 0
                or native.get("source_or_destination_mutation") is not False):
            raise ValueError("native whole proof is not bound to the retained source and target")

        source_scan, _ = _read_pinned_json(evidence_root, {
            "path": _relative_evidence_reference(native.get("source_scan_acceptance_receipt")),
            "sha256": native.get("source_scan_acceptance_receipt_sha256")})
        if (source_scan.get("full_source_scan_complete") is not True
                or source_scan.get("source_or_object_mutation") is not False
                or source_scan.get("raw_proof_sha256") != binding["source_proof"]["sha256"]
                or source_scan.get("raw_proof_path") != binding["source_proof"]["path"]):
            raise ValueError("native source proof is not bound to a completed immutable scan")
        source_part_size = source_proof.get("upload_part_size")
        if (source_proof.get("method") != "gbi_chunks_source_multipart_md5_v1"
                or type(source_part_size) is not int or source_part_size < 1
                or len(source_proof.get("parts", ()))
                != (source_proof.get("size", -1) + source_part_size - 1) // source_part_size):
            raise ValueError("source proof has incomplete multipart coverage")
        for index, part in enumerate(source_proof["parts"]):
            expected_size = min(source_part_size, source_proof["size"] - index * source_part_size)
            if (part.get("name") != f"{index:08d}.part" or part.get("size") != expected_size
                    or not part.get("sha256") or not part.get("multipart_md5")):
                raise ValueError("source proof part order, sizes or checksums are invalid")

        identities = []
        object_by_name = {item.get("object_name"): item for item in objects}
        if len(object_by_name) != len(objects):
            raise ValueError("native whole proof contains duplicate object identities")
        source_parts = source_proof.get("parts", ())
        if len(source_parts) != len(outer["parts"]):
            raise ValueError("source and destination proof part counts differ")
        for part, source_part in zip(outer["parts"], source_parts):
            name = prefix + ".gbi/parts/" + part["name"]
            item = object_by_name.get(name)
            if (source_part.get("name") != part["name"]
                    or source_part.get("size") != part["size"]
                    or item is None or item.get("size") != part["size"]
                    or item.get("namespace") != binding.get("namespace")
                    or item.get("bucket") != binding.get("bucket")
                    or item.get("verification_method") != "oci_native_multipart_md5"
                    or item.get("source_multipart_md5") != source_part.get("multipart_md5")
                    or not item.get("source_multipart_md5")
                    or item.get("source_multipart_md5") != item.get("native_multipart_md5")
                    or not item.get("etag") or not item.get("version_id")
                    or not isinstance(item.get("metadata"), dict)):
                raise ValueError("native part proof differs from the complete chunk manifest")
            identities.append({key: item.get(key) for key in
                               ("namespace", "bucket", "object_name", "size", "etag", "version_id", "metadata")})

        control_hashes = native.get("control_sha256", {})
        control_by_name = {item.get("object_name"): item for item in native["controls"]}
        for name, local in ((prefix + "manifest.json", Path(target_dir) / "manifest.json"),
                            (prefix + ".gbi/complete.json", Path(target_dir) / ".gbi" / "complete.json")):
            item = control_by_name.get(name)
            if (item is None or control_hashes.get(name) != hashlib.sha256(local.read_bytes()).hexdigest()
                    or item.get("size") != local.stat().st_size
                    or not item.get("etag") or not item.get("version_id")
                    or not isinstance(item.get("metadata"), dict)):
                raise ValueError("native control proof differs from the canonical chunk controls")
            identities.append({"namespace": binding["namespace"], "bucket": binding["bucket"],
                               "object_name": name, **{key: item.get(key) for key in
                               ("size", "etag", "version_id", "metadata")}})

        stage = Path(binding["retained_stage"])
        if (stage.is_symlink() or stage.resolve() != stage.absolute()
                or _file_sha256(stage) != binding["retained_stage_sha256"]
                or _source_identity(binding["retained_staged_tar"])
                != binding["retained_staged_tar_source_identity"]):
            raise ValueError("retained staged archive or staging journal changed")
        if formats._manifest_digest(current) != manifest_sha:
            raise ValueError("fresh FSS manifest differs from the native-verified per-file archive")
        verified = archives.VerifiedArchiveEvidence._issue(
            formats._manifest_digest(current), binding_sha)
        return cls(binding, native, source_proof, verified, tuple(identities))

    def revalidate_current(self, identity_reader):
        if not callable(identity_reader):
            raise ValueError("maintained Storage identity reader is required")
        observed = identity_reader(self.binding["namespace"], self.binding["bucket"],
                                   list(self.identities))
        keyed = {item.get("object_name"): item for item in observed}
        if len(keyed) != len(self.identities):
            raise ValueError("current OCI identity sweep has missing or duplicate objects")
        fields = ("namespace", "bucket", "object_name", "size", "etag", "version_id", "metadata")
        for expected in self.identities:
            actual = keyed.get(expected["object_name"])
            if actual is None or any(actual.get(field) != expected.get(field) for field in fields):
                raise ValueError("current OCI object identity differs from the native proof")

    def assert_manifest(self, manifest):
        self.verified_archive.assert_manifest(manifest)


def recover_completed_superset_unit(
        state_root, run_id, source_relative_sha256, expected_plan_sha256,
        provenance_id=None, retain_safety_copy=False, *,
        native_cleanup_binding=None, native_cleanup_binding_sha256=None,
        evidence_root=None, identity_reader=None):
    """Finish one frozen chunk unit whose valid archive contains source state.

    ``source_relative_sha256`` identifies one unit in the saved plan without
    copying a potentially long source selection into a command or log. The
    saved plan, original destination, run request, filters and unit mapping are
    read in place and never changed. A new provenance record is created before
    cleanup. With ``retain_safety_copy=True``, a verified tar is reconstructed
    under the user's Lustre ``.gbi/recovery-payloads`` and used for per-unlink
    stability checks. The original unit result is created exclusively after
    full cleanup and original destination readback succeed.

    The caller must freeze all writers to both source and destination first.
    The optional native-cleanup path is limited to an exact current-source
    manifest match with a SHA-pinned enriched receipt. It validates the raw
    source/native/producer evidence and rechecks every provider identity before
    and after cleanup through an injected maintained Storage reader. It reuses
    the already retained staged tar as recovery material; it never rebuilds it.
    The ordinary superset path keeps its full destination readback.
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
        native_proof = None
        if native_cleanup_binding is not None:
            if (evidence_root is None or native_cleanup_binding_sha256 is None
                    or identity_reader is None or not retain_safety_copy):
                raise ValueError("native cleanup requires a pinned evidence bundle, current Storage reader, and retained staged tar")
            native_proof = NativeArchiveProof.load(
                native_cleanup_binding, native_cleanup_binding_sha256, evidence_root,
                source=source, target=target, source_relative=unit["source_relative"],
                current=current, outer=outer, target_dir=target)
            # Equality with the producer's full per-file manifest proves the
            # complete archive semantics; no chunk payload is reread here.
            archived = {key: value for key, value in current.items()
                        if not key.startswith("_")}
        else:
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
        archived_digest = formats._manifest_digest(archived)

        def destination_unchanged():
            if chunks.read_manifest(target) != outer or _chunk_snapshot(target, outer) != target_layout:
                raise ValueError("chunk destination changed during source cleanup")

        safety_path = None
        safety_snapshot = None
        safety_digest = None
        safety_size = None
        if native_proof is not None:
            safety_path = Path(native_proof.binding["retained_staged_tar"])
            safety_snapshot = _source_identity(safety_path)
            if safety_snapshot != native_proof.binding["retained_staged_tar_source_identity"]:
                raise ValueError("retained staged archive identity changed")
            safety_digest = native_proof.binding["archive_sha256"]
            safety_size = native_proof.binding["archive_bytes"]

            def safety_copy_unchanged():
                if _source_identity(safety_path) != safety_snapshot:
                    raise ValueError("retained staged archive changed; remaining sources kept")
        elif retain_safety_copy:
            safety_path, safety_snapshot, safety_digest, safety_size = _verified_safety_copy(
                state_root, run_id, source_relative_sha256, target, outer, archived_digest)
            destination_unchanged()

            def safety_copy_unchanged():
                if _safety_copy_snapshot(safety_path) != safety_snapshot:
                    raise ValueError("verified recovery payload changed; remaining sources kept")

        recovery_dir = state_root / "recovery-receipts" / run_id
        provenance_id = provenance_id or uuid.uuid4().hex
        if Path(provenance_id).name != provenance_id:
            raise ValueError("provenance id must be one path component")
        provenance_path = recovery_dir / f"{source_relative_sha256}.{provenance_id}.json"
        if provenance_path.exists() or provenance_path.is_symlink():
            raise ValueError("immutable recovery provenance already exists")
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
            "recovery_payload": str(safety_path) if safety_path else None,
            "recovery_payload_sha256": safety_digest,
            "recovery_payload_bytes": safety_size,
            "native_binding_sha256": native_cleanup_binding_sha256 if native_proof else None,
            "created_unix_ns": time.time_ns(),
        }
        _write_new_json(provenance_path, provenance)

        current_identity_checked = False

        def cleanup_stability_guard():
            nonlocal current_identity_checked
            if safety_path:
                safety_copy_unchanged()
            else:
                destination_unchanged()
            if native_proof is not None and not current_identity_checked:
                native_proof.revalidate_current(identity_reader)
                current_identity_checked = True

        cleanup_arguments = {
            "destination_unchanged": cleanup_stability_guard,
        }
        if native_proof is not None:
            cleanup_arguments["verified_archive"] = native_proof.verified_archive
        cleanup = archives.resume_cleanup_source(
            source, combined,
            safety_path if safety_path else lambda: chunks.open_chunks(target),
            **cleanup_arguments)
        if safety_path:
            safety_copy_unchanged()
        destination_unchanged()
        if native_proof is not None:
            native_proof.revalidate_current(identity_reader)
            if _file_sha256(native_proof.binding["retained_stage"]) != native_proof.binding["retained_stage_sha256"]:
                raise ValueError("retained staging journal changed after cleanup")
            after = archived
        else:
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
        if native_proof is None:
            replay = archive_state.resume_completed(task)
            replay_deleted = replay["deleted"]
        else:
            # The fresh post-cleanup OCI identity sweep above closes the
            # destination check; resume_completed would reread every tar part.
            replay_deleted = False
        return {"provenance": str(provenance_path), "result": str(receipt_path),
                "freed_bytes": cleanup["freed_bytes"],
                "removed": cleanup["removed"], "manifest_sha256": archived_digest,
                "entries": len(after["entries"]), "replay_deleted": replay_deleted}


def main(argv=None):
    """Run explicitly selected frozen units from a bounded Slurm allocation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-root", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--unit-sha256", action="append", required=True)
    parser.add_argument("--provenance-prefix", required=True)
    args = parser.parse_args(argv)
    for unit_key in args.unit_sha256:
        result = recover_completed_superset_unit(
            args.state_root, args.run_id, unit_key, args.plan_sha256,
            provenance_id=f"{args.provenance_prefix}-{unit_key[:12]}-retry2",
            retain_safety_copy=True)
        print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
