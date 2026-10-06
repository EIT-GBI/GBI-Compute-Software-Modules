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


def _fixture(root, *, changed_source=False, with_symlink=False):
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
    if with_symlink:
        (shared / "link").symlink_to("keep.dat")
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
    chunk_size = 1024
    _write_chunk_store(target, payload, source, chunk_size)

    # Reproduce a unit interrupted after verified packing and partial cleanup.
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
    }


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

    def _verified_journal(self, fixture, phase="verified"):
        archived = archives.inspect_archive(lambda: chunks.open_chunks(fixture["target"]))
        key = david_recovery._key(str(fixture["target"]))
        state = fixture["state"] / "state"
        (state / "pending").mkdir(parents=True)
        (state / "locks").mkdir()
        path = state / "pending" / (key + ".format.json")
        record = {"source": str(fixture["source"]), "destination": str(fixture["target"]),
                  "kind": "pack", "bytes": formats._source_bytes(archived),
                  "manifest_sha256": formats._manifest_digest(archived)}
        journal = {"phase": phase, "manifest": archived, "record": record,
                   "intent": {"source": str(fixture["source"]), "target": str(fixture["target"]),
                              "action": "pack", "pack": "tar", "chunk_size": 1024,
                              "include": [], "exclude": ["*.py"]}}
        path.write_text(json.dumps(journal))
        return path, hashlib.sha256(path.read_bytes()).hexdigest()

    def test_pinned_verified_journal_survives_explicit_mount_binding_recovery(self):
        fixture = _fixture(self.root, with_symlink=True)
        path, digest = self._verified_journal(fixture)
        encoded = path.read_bytes()
        root_identity = archives._observation(fixture["source"].lstat())[:3]
        plan_path = next((fixture["state"] / "archive-plans").glob("*.json"))
        plan = json.loads(plan_path.read_bytes())
        plan["plan"]["units"][0]["expected_source"][0] += 1
        plan_path.write_text(json.dumps(plan))
        plan_digest = hashlib.sha256(plan_path.read_bytes()).hexdigest()
        result = david_recovery.recover_completed_superset_unit(
            fixture["state"], fixture["run_id"], fixture["unit_key"], plan_digest,
            provenance_id="verified-journal", retain_safety_copy=True,
            expected_current_root=root_identity, verified_journal_sha256=digest)
        self.assertFalse(fixture["keep"].exists())
        self.assertTrue((fixture["source"] / "excluded.py").exists())
        self.assertEqual(path.read_bytes(), encoded)
        provenance = json.loads(Path(result["provenance"]).read_bytes())
        self.assertEqual(provenance["current_source_root"], root_identity)
        self.assertNotEqual(provenance["planned_source_root"][0], root_identity[0])
        self.assertEqual(provenance["retained_journal_sha256"][str(path)], digest)

    def test_actual_transfer_state_journal_is_rejected_without_explicit_pin(self):
        fixture = _fixture(self.root)
        self._verified_journal(fixture)
        with self.assertRaisesRegex(ValueError, "unresolved transfer journal"):
            david_recovery.recover_completed_superset_unit(
                fixture["state"], fixture["run_id"], fixture["unit_key"], fixture["plan_sha256"])
        self.assertTrue(fixture["keep"].exists())

    def test_pinned_writing_journal_cannot_authorize_cleanup(self):
        fixture = _fixture(self.root)
        path, digest = self._verified_journal(fixture, phase="writing")
        with self.assertRaisesRegex(ValueError, "exact verified archive selection"):
            david_recovery.recover_completed_superset_unit(
                fixture["state"], fixture["run_id"], fixture["unit_key"], fixture["plan_sha256"],
                verified_journal_sha256=digest)
        self.assertTrue(fixture["keep"].exists())
        self.assertFalse((fixture["state"] / "recovery-receipts").exists())

    def test_mismatched_current_root_pin_fails_before_cleanup(self):
        fixture = _fixture(self.root)
        current = archives._observation(fixture["source"].lstat())[:3]
        current[1] += 1
        with self.assertRaisesRegex(ValueError, "pinned current mount identity"):
            david_recovery.recover_completed_superset_unit(
                fixture["state"], fixture["run_id"], fixture["unit_key"], fixture["plan_sha256"],
                expected_current_root=current)
        self.assertTrue(fixture["keep"].exists())


    def test_verified_journal_pin_rejects_changed_selection(self):
        fixture = _fixture(self.root)
        path, _ = self._verified_journal(fixture)
        journal = json.loads(path.read_bytes())
        journal["intent"]["exclude"] = []
        path.write_text(json.dumps(journal))
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        with self.assertRaisesRegex(ValueError, "exact verified archive selection"):
            david_recovery.recover_completed_superset_unit(
                fixture["state"], fixture["run_id"], fixture["unit_key"], fixture["plan_sha256"],
                verified_journal_sha256=digest)
        self.assertTrue(fixture["keep"].exists())

    def test_current_mount_pin_never_bypasses_original_inode_identity(self):
        fixture = _fixture(self.root)
        current = archives._observation(fixture["source"].lstat())[:3]
        plan_path = next((fixture["state"] / "archive-plans").glob("*.json"))
        plan = json.loads(plan_path.read_bytes())
        plan["plan"]["units"][0]["expected_source"][1] += 1
        plan_path.write_text(json.dumps(plan))
        with self.assertRaisesRegex(ValueError, "frozen plan"):
            david_recovery.recover_completed_superset_unit(
                fixture["state"], fixture["run_id"], fixture["unit_key"],
                hashlib.sha256(plan_path.read_bytes()).hexdigest(), expected_current_root=current)
        self.assertTrue(fixture["keep"].exists())

    def test_complete_stage_is_preserved_and_writing_stage_is_rejected(self):
        for phase in ("complete", "writing"):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as directory:
                fixture = _fixture(Path(directory).resolve())
                path, journal_digest = self._verified_journal(fixture)
                encoded = path.read_bytes()
                key = david_recovery._key(str(fixture["target"]))
                stage = fixture["state"] / "state" / "format-staging" / key
                stage.mkdir(parents=True)
                payload = stage / "unit.gbi.tar"
                payload.write_bytes(fixture["target_bytes"])
                payload.chmod(0o400)
                outer_path = fixture["target"] / "manifest.json"
                outer = json.loads(outer_path.read_bytes())
                outer["source"] = str(payload)
                outer["source_fingerprint"] = david_recovery.storage.fingerprint(payload)
                outer_encoded = chunks._encoded(outer)
                outer_path.write_bytes(outer_encoded)
                (fixture["target"] / ".gbi" / "complete.json").write_text(
                    json.dumps(chunks._completion(outer, outer_encoded)))
                stage_path = stage / "stage.json"
                stage_path.write_text(json.dumps({"phase": phase, "intent": {"source": str(fixture["source"])},
                    "manifest": json.loads(encoded)["manifest"], "sha256": outer["sha256"]}))
                before = stage_path.read_bytes()
                if phase == "writing":
                    with self.assertRaisesRegex(ValueError, "retained stage differs"):
                        david_recovery.recover_completed_superset_unit(
                            fixture["state"], fixture["run_id"], fixture["unit_key"],
                            fixture["plan_sha256"], verified_journal_sha256=journal_digest)
                    self.assertTrue(fixture["keep"].exists())
                else:
                    david_recovery.recover_completed_superset_unit(
                        fixture["state"], fixture["run_id"], fixture["unit_key"],
                        fixture["plan_sha256"], verified_journal_sha256=journal_digest,
                        retain_safety_copy=True)
                    self.assertFalse(fixture["keep"].exists())
                self.assertEqual(stage_path.read_bytes(), before)
                self.assertEqual(path.read_bytes(), encoded)


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


if __name__ == "__main__":
    unittest.main()
