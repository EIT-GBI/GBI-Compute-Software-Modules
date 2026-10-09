import hashlib
import io
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest import mock

from gbi_data import archive_state, archives, chunks, david_recovery, formats


def _write_chunk_store(path, data, source, chunk_size):
    path.mkdir(parents=True)
    parts = []
    for index, offset in enumerate(range(0, len(data), chunk_size)):
        part = data[offset:offset + chunk_size]
        name = f"{index:08d}.part"
        parts.append({"name": name, "size": len(part),
                      "sha256": hashlib.sha256(part).hexdigest()})
    manifest = {
        "format": chunks.FORMAT,
        "version": chunks.VERSION,
        "original_name": path.name.removesuffix(".gbi-chunks"),
        "size": len(data),
        "chunk_size": chunk_size,
        "sha256": hashlib.sha256(data).hexdigest(),
        "source": str(source),
        "source_fingerprint": [0, 0, 0, 0, 0],
        "parts": parts,
    }
    encoded = chunks._encoded(manifest)
    (path / "manifest.json").write_bytes(encoded)
    part_root = path / ".gbi" / "parts"
    part_root.mkdir(parents=True)
    for index, item in enumerate(parts):
        start = index * chunk_size
        (part_root / item["name"]).write_bytes(data[start:start + chunk_size])
    complete = path / ".gbi" / "complete.json"
    complete.parent.mkdir(exist_ok=True)
    complete.write_text(json.dumps(chunks._completion(manifest, encoded), sort_keys=True))


def _fixture(root, *, changed_source=False, partial=True):
    root = Path(root).resolve()
    state = root / ".gbi"
    run_id = "run-david-recovery-test"
    run = state / "runs" / run_id
    run.mkdir(parents=True)
    source_root = root / "source"
    source = source_root / "unit"
    shared = source / "shared"
    old = source / "old"
    shared.mkdir(parents=True)
    old.mkdir()
    keep = shared / "keep.dat"
    keep.write_bytes(b"remaining selected payload")
    (old / "already-moved.dat").write_bytes(b"historical selected payload")
    (source / "excluded.py").write_text("keep this excluded source")

    target_root = root / "storage"
    old_target = target_root / "archive"
    old_target.mkdir(parents=True)
    target = old_target / "unit.gbi.tar.gbi-chunks"
    archive_bytes = io.BytesIO()
    original = archives.pack(source, archive_bytes, compression=None,
                             exclusions=("*.py",))
    payload = archive_bytes.getvalue()
    planned_bytes = david_recovery._manifest_payload_bytes(original)
    chunk_size = 1024 * 1024
    _write_chunk_store(target, payload, source, chunk_size)

    # Reproduce a unit interrupted after verified packing and partial cleanup.
    if partial:
        (old / "already-moved.dat").unlink()
        old.rmdir()
        changed_time = shared.stat().st_mtime_ns - 20_000_000
        os.utime(shared, ns=(changed_time, changed_time))
    if changed_source:
        keep.write_bytes(b"different selected payload")

    plan_key = archive_state._key(str(old_target))
    plan_root = state / "archive-plans"
    plan_root.mkdir()
    lock_path = plan_root / f"{plan_key}.lock"
    lock_path.touch()
    (plan_root / f"{plan_key}.units").mkdir()
    unit = {
        "source_relative": "unit",
        "target_relative": "unit.gbi.tar.gbi-chunks",
        "action": "pack",
        "bytes": planned_bytes,
        "expected_source": archives._observation(source.lstat()),
        "chunk_size": chunk_size,
    }
    record = {"intent": {"source": str(source_root), "target": str(old_target)},
              "plan": {"source": str(source_root), "destination": str(old_target),
                       "units": [unit]}}
    plan_path = plan_root / f"{plan_key}.json"
    plan_path.write_text(json.dumps(record, sort_keys=True))
    request = {
        "source": str(source_root), "target": str(old_target),
        "target_root": str(target_root), "target_kind": "alluxio", "delete": True,
        "include": [], "exclude": ["*.py"], "reserved_names": [],
    }
    (run / "request.json").write_text(json.dumps(request, sort_keys=True))
    return {
        "state": state, "run_id": run_id, "source": source, "keep": keep,
        "target": target, "target_bytes": payload, "unit_key": archive_state._key("unit"),
        "plan_sha256": hashlib.sha256(plan_path.read_bytes()).hexdigest(),
        "unit": unit,
        "archived_manifest_sha256": formats._manifest_digest(original),
        "outer_manifest_sha256": hashlib.sha256((target / "manifest.json").read_bytes()).hexdigest(),
    }


def _native_binding(fixture, *, source_part_md5="source-derived-test-md5-1"):
    root = fixture["state"] / "native-evidence"
    root.mkdir()
    state_root = fixture["state"]
    unit = fixture["unit"]
    target = fixture["target"]
    source_path = state_root / "format-staging" / "retained.tar"
    source_path.parent.mkdir()
    source_path.write_bytes(fixture["target_bytes"])
    source_path.chmod(0o600)
    stage_path = source_path.with_name("stage.json")
    stage_path.write_text("{}\n")
    identity = david_recovery._source_identity(source_path)
    outer = chunks.read_manifest(target)
    prefix = "test-prefix/"
    namespace, bucket = "test-namespace", "test-bucket"
    source_part = {
        "name": "00000000.part", "size": len(fixture["target_bytes"]),
        "sha256": hashlib.sha256(fixture["target_bytes"]).hexdigest(),
        "multipart_md5": source_part_md5,
    }
    source_proof = {
        "archive_sha256": hashlib.sha256(fixture["target_bytes"]).hexdigest(),
        "manifest_sha256": fixture["outer_manifest_sha256"],
        "method": "gbi_chunks_source_multipart_md5_v1",
        "parts": [source_part], "size": len(fixture["target_bytes"]),
        "source": str(source_path), "source_identity": identity,
        "upload_part_size": len(fixture["target_bytes"]),
    }
    source_proof_bytes = json.dumps({"source_proof": source_proof}, sort_keys=True).encode()
    source_proof_sha = hashlib.sha256(source_proof_bytes).hexdigest()
    source_proof_path = root / "source.json"
    source_proof_path.write_bytes(source_proof_bytes)
    source_relative = unit["source_relative"]
    source_scan = {
        "full_source_scan_complete": True, "source_or_object_mutation": False,
        "raw_proof_path": "source.json", "raw_proof_sha256": source_proof_sha,
    }
    source_scan_path = root / "scan.json"
    source_scan_path.write_text(json.dumps(source_scan, sort_keys=True))
    source_scan_sha = hashlib.sha256(source_scan_path.read_bytes()).hexdigest()

    part = outer["parts"][0]
    part_name = prefix + ".gbi/parts/" + part["name"]
    part_identity = {
        "namespace": namespace, "bucket": bucket, "object_name": part_name,
        "size": part["size"], "etag": "part-etag", "version_id": "part-version",
        "metadata": {}, "verification_method": "oci_native_multipart_md5",
        "source_multipart_md5": "source-derived-test-md5-1",
        "native_multipart_md5": "source-derived-test-md5-1",
    }
    control_records = []
    control_hashes = {}
    for name, local in ((prefix + "manifest.json", target / "manifest.json"),
                        (prefix + ".gbi/complete.json", target / ".gbi" / "complete.json")):
        control_records.append({"object_name": name, "size": local.stat().st_size,
                                "etag": name + "-etag", "version_id": name + "-version",
                                "metadata": {}})
        control_hashes[name] = hashlib.sha256(local.read_bytes()).hexdigest()
    canonical_source = hashlib.sha256(json.dumps(
        source_proof, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()).hexdigest()
    native = {
        "schema": "gbi-row13-native-whole-proof-v1", "source_proof_sha256": source_proof_sha,
        "source_proof_canonical_sha256": canonical_source, "source_path": str(source_path),
        "source_identity": identity, "archive_sha256": outer["sha256"],
        "archive_size": outer["size"], "manifest_sha256": fixture["outer_manifest_sha256"],
        "bucket": bucket, "namespace": namespace, "prefix": prefix,
        "objects": [part_identity], "controls": control_records,
        "control_sha256": control_hashes, "source_scan_acceptance_receipt": "scan.json",
        "source_scan_acceptance_receipt_sha256": source_scan_sha, "payload_get_http_attempts": 0,
        "source_or_destination_mutation": False,
    }
    native_path = root / "native.json"
    native_path.write_text(json.dumps(native, sort_keys=True))
    native_sha = hashlib.sha256(native_path.read_bytes()).hexdigest()
    terminal_path = root / "terminal.json"
    terminal = {"returncode": 0, "stdout_sha256": native_sha, "stderr_bytes": 0}
    terminal_path.write_text(json.dumps(terminal, sort_keys=True))
    terminal_sha = hashlib.sha256(terminal_path.read_bytes()).hexdigest()
    producer_path = root / "producer.json"
    producer = {"capture": {"proof": {"event": "historical-chunk-reconstruction-verified",
                                           "root": prefix.rstrip("/"),
                                           "bytes": outer["size"], "sha256": outer["sha256"],
                                           "parts": len(outer["parts"]),
                                           "archive_per_file_manifest_readback": True,
                                           "original_manifest_unchanged": True,
                                           "original_stage_unchanged": True,
                                           "source_fss_mutation": False}},
                "source_fss_mutation": False}
    producer_path.write_text(json.dumps(producer, sort_keys=True))
    producer_sha = hashlib.sha256(producer_path.read_bytes()).hexdigest()
    review_path = root / "review.json"
    review_path.write_text(json.dumps({"checks": {
        "all_1_source_native_multipart_md5_matches": True,
        "ordered_part_names_sizes_sha_and_object_keys": True}}, sort_keys=True))
    review_sha = hashlib.sha256(review_path.read_bytes()).hexdigest()
    refs = {
        "raw_proof": {"path": "native.json", "sha256": native_sha},
        "process_terminal": {"path": "terminal.json", "sha256": terminal_sha},
        "source_proof": {"path": "source.json", "sha256": source_proof_sha},
        "producer_semantic_proof": {"path": "producer.json", "sha256": producer_sha},
        "independent_review": {"path": "review.json", "sha256": review_sha},
    }
    base_path = root / "base.json"
    base_path.write_text(json.dumps({"native_destination_content_accepted": True, **refs}, sort_keys=True))
    base_sha = hashlib.sha256(base_path.read_bytes()).hexdigest()
    worklist_path = root / "worklist.json"
    worklist = {"rows": [{"row": {"source": str(fixture["source"]),
                                     "canonical_target": str(target),
                                     "root": prefix[:-1],
                                     "manifest_sha256": fixture["outer_manifest_sha256"]},
                          "full_retained_tar_proof": {
                              "source": str(fixture["source"]), "payload": str(source_path),
                              "manifest_sha256": fixture["archived_manifest_sha256"],
                              "sha256": outer["sha256"], "bytes": outer["size"]}}]}
    worklist_path.write_text(json.dumps(worklist, sort_keys=True))
    worklist_sha = hashlib.sha256(worklist_path.read_bytes()).hexdigest()
    binding = {
        "schema": "strand2-native-archive-cleanup-binding-v1", "row": 13,
        "native_destination_content_accepted": True, "source_cleanup_accepted": False,
        "whole_collection_accepted": False, "source_or_destination_mutation": False,
        "part_count": len(outer["parts"]), "archive_bytes": outer["size"],
        "archive_sha256": outer["sha256"], "namespace": namespace, "bucket": bucket,
        "prefix": prefix, "raw_proof": refs["raw_proof"],
        "process_terminal": refs["process_terminal"], "source_proof": refs["source_proof"],
        "producer_semantic_proof": refs["producer_semantic_proof"],
        "independent_review": refs["independent_review"],
        "base_acceptance": {"path": "base.json", "sha256": base_sha},
        "frozen_semantic_worklist": {"path": "worklist.json", "sha256": worklist_sha},
        "frozen_worklist_row_index": 0,
        "per_file_manifest_sha256": fixture["archived_manifest_sha256"],
        "source_fss": str(fixture["source"]), "canonical_target": str(target),
        "retained_staged_tar": str(source_path), "retained_staged_tar_source_identity": identity,
        "outer_manifest_sha256": fixture["outer_manifest_sha256"],
        "retained_stage": str(stage_path),
        "retained_stage_sha256": hashlib.sha256(stage_path.read_bytes()).hexdigest(),
        "expected_source_relative": source_relative,
        "current_identity_revalidation_required": True,
    }
    binding_path = root / "binding.json"
    binding_path.write_text(json.dumps(binding, sort_keys=True))
    return {"root": root, "binding": binding_path,
            "binding_sha256": hashlib.sha256(binding_path.read_bytes()).hexdigest(),
            "identities": [part_identity, *[{
                "namespace": namespace, "bucket": bucket, **item
            } for item in control_records]]}


class DavidArchiveRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()

    def test_superset_resume_removes_only_matching_selected_source_and_writes_provenance(self):
        fixture = _fixture(self.root)
        result = david_recovery.recover_completed_superset_unit(
            fixture["state"], fixture["run_id"], fixture["unit_key"],
            fixture["plan_sha256"], provenance_id="attempt-one",
            retain_safety_copy=True)

        self.assertEqual(result["freed_bytes"], len(b"remaining selected payload"))
        self.assertFalse(fixture["keep"].exists())
        self.assertTrue((fixture["source"] / "excluded.py").exists())
        with chunks.open_chunks(fixture["target"]) as stored:
            self.assertEqual(fixture["target_bytes"], stored.read())
        completion = json.loads(Path(result["result"]).read_text())
        provenance = json.loads(Path(result["provenance"]).read_text())
        self.assertTrue(completion["deleted"])
        self.assertEqual(completion["manifest_sha256"], formats._manifest_digest(
            archives.inspect_archive(lambda: chunks.open_chunks(fixture["target"]))))
        self.assertEqual(completion["recovery_provenance"], result["provenance"])
        self.assertGreater(provenance["archive_only_entries"], 0)
        self.assertEqual(provenance["directory_mtime_differences"], 1)
        safety_copy = Path(provenance["recovery_payload"])
        self.assertEqual(stat.S_IMODE(safety_copy.stat().st_mode), 0o400)
        self.assertEqual(provenance["recovery_payload_sha256"],
                         hashlib.sha256(safety_copy.read_bytes()).hexdigest())
        self.assertEqual(archive_state.resume_completed({
            "target_root": str(self.root / "storage"), "source": str(fixture["source"]),
            "archive_destination": str(fixture["target"]), "archive_completed": completion,
            "format_action": "pack", "chunk_size": fixture["unit"]["chunk_size"],
            "empty": False, "include": [], "exclude": ["*.py"], "reserved_names": [],
        })["deleted"], False)

    def test_source_payload_difference_fails_before_provenance_or_cleanup(self):
        fixture = _fixture(self.root, changed_source=True)
        with self.assertRaisesRegex(ValueError, "content/type/mode/link differs"):
            david_recovery.recover_completed_superset_unit(
                fixture["state"], fixture["run_id"], fixture["unit_key"],
                fixture["plan_sha256"], provenance_id="attempt-one")
        self.assertTrue(fixture["keep"].exists())
        self.assertFalse((fixture["state"] / "recovery-receipts").exists())

    def test_safety_copy_mutation_fails_guard_before_more_source_cleanup(self):
        fixture = _fixture(self.root)
        original_cleanup = archives.resume_cleanup_source

        def mutate_copy(source, manifest, stored_archive, destination_unchanged=None):
            os.chmod(stored_archive, 0o600)
            with open(stored_archive, "ab") as stream:
                stream.write(b"changed")
            return original_cleanup(source, manifest, stored_archive,
                                    destination_unchanged=destination_unchanged)

        with mock.patch.object(archives, "resume_cleanup_source", mutate_copy):
            with self.assertRaisesRegex(ValueError, "recovery payload must remain"):
                david_recovery.recover_completed_superset_unit(
                    fixture["state"], fixture["run_id"], fixture["unit_key"],
                    fixture["plan_sha256"], provenance_id="safety-copy-change",
                    retain_safety_copy=True)

        self.assertTrue(fixture["keep"].exists())
        self.assertFalse((fixture["state"] / "archive-plans" /
                          (archive_state._key(str(self.root / "storage" / "archive")) +
                           ".units") / f"{fixture['unit_key']}.json").exists())

    def test_original_target_change_after_cleanup_leaves_verified_safety_copy(self):
        fixture = _fixture(self.root)
        original_cleanup = archives.resume_cleanup_source

        def mutate_target_after_cleanup(source, manifest, stored_archive,
                                        destination_unchanged=None):
            result = original_cleanup(source, manifest, stored_archive,
                                      destination_unchanged=destination_unchanged)
            part = fixture["target"] / ".gbi" / "parts" / "00000000.part"
            with part.open("ab") as stream:
                stream.write(b"changed")
            return result

        with mock.patch.object(archives, "resume_cleanup_source",
                               mutate_target_after_cleanup):
            with self.assertRaisesRegex(ValueError, "chunk destination changed"):
                david_recovery.recover_completed_superset_unit(
                    fixture["state"], fixture["run_id"], fixture["unit_key"],
                    fixture["plan_sha256"], provenance_id="original-target-change",
                    retain_safety_copy=True)

        copies = list((fixture["state"] / "recovery-payloads" /
                       fixture["run_id"]).glob("*.tar"))
        self.assertEqual(len(copies), 1)
        self.assertTrue(copies[0].is_file())
        saved_archive = archives.inspect_archive(copies[0])
        self.assertIsNotNone(saved_archive)
        result_files = list((fixture["state"] / "recovery-receipts" /
                             fixture["run_id"]).glob("*.json"))
        self.assertEqual(len(result_files), 1)
        provenance = json.loads(result_files[0].read_text())
        self.assertEqual(formats._manifest_digest(saved_archive),
                         provenance["stored_manifest_sha256"])
        self.assertFalse((fixture["state"] / "archive-plans" /
                          (archive_state._key(str(self.root / "storage" / "archive")) +
                           ".units") / f"{fixture['unit_key']}.json").exists())
        self.assertFalse((fixture["state"] / "archive-plans" / "unused").exists())

    def test_plan_byte_mismatch_fails_before_cleanup(self):
        fixture = _fixture(self.root)
        plan_path = fixture["state"] / "archive-plans" / (
            archive_state._key(str(self.root / "storage" / "archive")) + ".json")
        record = json.loads(plan_path.read_text())
        record["plan"]["units"][0]["bytes"] += 1
        plan_path.write_text(json.dumps(record, sort_keys=True))
        changed_plan_sha = hashlib.sha256(plan_path.read_bytes()).hexdigest()
        with self.assertRaisesRegex(ValueError, "planned unit"):
            david_recovery.recover_completed_superset_unit(
                fixture["state"], fixture["run_id"], fixture["unit_key"],
                changed_plan_sha, provenance_id="attempt-one")
        self.assertTrue(fixture["keep"].exists())
        self.assertFalse((fixture["state"] / "recovery-receipts").exists())

    def test_native_cleanup_uses_bound_proof_and_rechecks_all_current_identities(self):
        fixture = _fixture(self.root, partial=False)
        proof = _native_binding(fixture)
        calls = []

        def identity_reader(namespace, bucket, expected):
            calls.append((namespace, bucket))
            return expected

        with mock.patch.object(archives, "inspect_archive",
                               side_effect=AssertionError("archive body read")), \
             mock.patch.object(david_recovery, "_verified_safety_copy",
                               side_effect=AssertionError("duplicate tar reconstruction")):
            result = david_recovery.recover_completed_superset_unit(
                fixture["state"], fixture["run_id"], fixture["unit_key"],
                fixture["plan_sha256"], provenance_id="native-fast-path",
                retain_safety_copy=True,
                native_cleanup_binding=proof["binding"],
                native_cleanup_binding_sha256=proof["binding_sha256"],
                evidence_root=proof["root"], identity_reader=identity_reader)

        self.assertEqual(len(calls), 2)
        self.assertFalse(fixture["keep"].exists())
        self.assertTrue(Path(proof["root"] / "../format-staging/retained.tar").exists())
        completion = json.loads(Path(result["result"]).read_text())
        self.assertEqual(completion["manifest_sha256"], fixture["archived_manifest_sha256"])
        provenance = json.loads(Path(result["provenance"]).read_text())
        self.assertEqual(provenance["native_binding_sha256"], proof["binding_sha256"])

    def test_native_cleanup_fails_before_unlink_on_changed_current_identity(self):
        fixture = _fixture(self.root, partial=False)
        proof = _native_binding(fixture)

        def identity_reader(namespace, bucket, expected):
            changed = [dict(item) for item in expected]
            changed[0]["etag"] = "unexpected-current-version"
            return changed

        with self.assertRaisesRegex(ValueError, "current OCI object identity"):
            david_recovery.recover_completed_superset_unit(
                fixture["state"], fixture["run_id"], fixture["unit_key"],
                fixture["plan_sha256"], provenance_id="native-changed-current",
                retain_safety_copy=True,
                native_cleanup_binding=proof["binding"],
                native_cleanup_binding_sha256=proof["binding_sha256"],
                evidence_root=proof["root"], identity_reader=identity_reader)
        self.assertTrue(fixture["keep"].exists())

    def test_native_cleanup_rejects_destination_checksum_not_bound_to_source_part(self):
        fixture = _fixture(self.root, partial=False)
        proof = _native_binding(fixture, source_part_md5="different-source-part")
        calls = []

        def identity_reader(namespace, bucket, expected):
            calls.append((namespace, bucket))
            return expected

        with self.assertRaisesRegex(ValueError, "native part proof differs"):
            david_recovery.recover_completed_superset_unit(
                fixture["state"], fixture["run_id"], fixture["unit_key"],
                fixture["plan_sha256"], provenance_id="native-source-part-mismatch",
                retain_safety_copy=True,
                native_cleanup_binding=proof["binding"],
                native_cleanup_binding_sha256=proof["binding_sha256"],
                evidence_root=proof["root"], identity_reader=identity_reader)
        self.assertTrue(fixture["keep"].exists())
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
