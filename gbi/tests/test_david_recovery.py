import hashlib
import io
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest

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


def _fixture(root, *, changed_source=False):
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
            fixture["plan_sha256"], provenance_id="attempt-one")

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
