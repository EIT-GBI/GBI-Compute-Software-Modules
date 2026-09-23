"""Encoded logical transfers reuse worker receipts, locks and cleanup rules."""

import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from gbi_data import archives, chunks, formats, packing
from dataclasses import asdict
from gbi_data.transfer import transfer


class Formats(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name).resolve()
        self.source = self.base / "source"
        self.target_root = self.base / "target"
        self.scratch = self.base / "lustre"
        self.state = self.scratch / ".gbi" / "state"
        for path in (self.source, self.target_root, self.state / "locks", self.state / "pending"):
            path.mkdir(parents=True)
        (self.source / "a.txt").write_text("alpha")
        (self.source / "b.bin").write_text("beta")

    def task(self, action="pack", **overrides):
        return {"format_action": action, "source": str(self.source),
                "format_target": str(self.target_root / "bundle.gbi.tar"),
                "source_root": str(self.base), "target_root": str(self.target_root),
                "source_kind": "fss", "target_kind": "alluxio", "state": str(self.state),
                "scratch_root": str(self.scratch), "receipt": str(self.base / "receipts.jsonl"),
                "progress": str(self.base / "progress.json"), "settle_seconds": 0,
                "delete": False, "pack": "tar", **overrides}

    def records(self):
        return [json.loads(line) for line in (self.base / "receipts.jsonl").read_text().splitlines()]

    def test_streamed_pack_verify_receipt_then_selected_cleanup(self):
        task = self.task(delete=True, include=["*.txt"])
        result = transfer(task)
        self.assertTrue(result["deleted"])
        self.assertEqual(result["bytes"], 5)
        self.assertFalse((self.source / "a.txt").exists())
        self.assertTrue((self.source / "b.bin").exists())
        self.assertEqual([r["event"] for r in self.records()], ["verified", "deleted"])
        self.assertEqual(formats.source_format(task["format_target"]), "archive")
        self.assertFalse((self.state / "format-staging").exists())

    def test_small_pack_requalifies_member_growth_before_output(self):
        policy = packing.PackingPolicy(10, 2, 100, 100, 10)
        self.assertEqual(len(packing.plan(self.source, policy, allow_root=True)["candidates"]), 1)
        before = self.source.stat().st_mtime_ns
        (self.source / "a.txt").write_bytes(bytes(20))
        self.assertEqual(self.source.stat().st_mtime_ns, before)
        task = self.task(packing_policy=asdict(policy))
        with self.assertRaisesRegex(ValueError, "no longer qualifies"):
            transfer(task)
        self.assertFalse(Path(task["format_target"]).exists())

    def test_progress_exposes_verified_part_counts_and_restore_phases(self):
        updates = []
        original = formats.write_json

        def capture(path, value):
            if path == self.base / "progress.json":
                updates.append(value.copy())
            return original(path, value)

        with patch.object(formats, "write_json", side_effect=capture):
            raw = self.target_root / "raw.gbi-chunks"
            transfer(self.task("chunk", source=str(self.source / "a.txt"),
                               format_target=str(raw), chunk_size=2))
            self.assertTrue(any(row["phase"] == "verified chunks 3/3" and row["bytes"] == 5
                                and row["source_size"] == 5 for row in updates))
            packed = self.task(chunk_size=4096)
            transfer(packed)
            self.assertTrue(any(row["phase"].startswith("verified archive parts ") for row in updates))
            self.assertTrue(any(row["phase"] == "checking Lustre archive staging" for row in updates))
            updates.clear()
            transfer(self.task("restore_chunks", source=packed["format_target"], target_kind="fss",
                               format_target=str(self.target_root / "restored")))
            phases = [row["phase"] for row in updates]
            self.assertLess(phases.index("verifying archive"), phases.index("extracting archive"))
            self.assertLess(phases.index("extracting archive"), phases.index("verifying restored archive"))

    def test_excluded_raw_chunks_named_like_archive_are_retained(self):
        source = self.source / "ordinary.gbi.tar"
        source.write_bytes(b"ordinary unmarked file")
        store = self.target_root / "ordinary.gbi.tar.gbi-chunks"
        transfer(self.task("chunk", source=str(source), format_target=str(store), chunk_size=8))
        restored = self.target_root / "restored"
        result = transfer(self.task("restore_chunks", source=str(store), target_kind="fss",
                                    format_target=str(restored), exclude=["*.gbi.tar"], delete=True))
        self.assertTrue(result["skipped"])
        self.assertFalse(restored.exists())
        self.assertTrue((store / ".gbi" / "complete.json").exists())

    def test_pack_identical_destination_reused_conflict_kept(self):
        task = self.task()
        transfer(task)
        self.assertTrue(transfer(task)["reused"])
        (self.source / "a.txt").write_text("changed")
        old = Path(task["format_target"]).read_bytes()
        with self.assertRaises(ValueError):
            transfer(task)
        self.assertEqual(Path(task["format_target"]).read_bytes(), old)

    def test_failed_readback_retains_source(self):
        task = self.task(delete=True)
        with patch.object(archives, "verify_archive", side_effect=ValueError("readback failed")):
            with self.assertRaisesRegex(ValueError, "readback failed"):
                transfer(task)
        self.assertTrue((self.source / "a.txt").exists())
        self.assertFalse((self.base / "receipts.jsonl").exists())

    def test_foreground_budget_covers_packed_source_growth(self):
        with self.assertRaisesRegex(ValueError, "foreground byte budget"):
            transfer(self.task(max_bytes=1, delete=True))
        self.assertTrue((self.source / "a.txt").exists())
        self.assertFalse((self.base / "receipts.jsonl").exists())

    def test_partial_archive_restore_retains_even_explicit_move_source(self):
        packed = self.task()
        transfer(packed)
        restore = self.task("restore_archive", source=packed["format_target"], source_kind="alluxio",
                            target_kind="lustre", format_target=str(self.target_root / "restored"),
                            delete=True, include=["*.txt"])
        result = transfer(restore)
        self.assertFalse(result["deleted"])
        self.assertIn("filtered", result["retained_reason"])
        self.assertEqual(result["restored_bytes"], 5)
        self.assertEqual(result["verified_container_bytes"], 9)
        self.assertEqual(result["freed_bytes"], 0)
        self.assertTrue(Path(packed["format_target"]).exists())
        self.assertEqual((self.target_root / "restored" / "a.txt").read_text(), "alpha")
        self.assertFalse((self.target_root / "restored" / "b.bin").exists())

    def test_complete_explicit_archive_move_removes_only_archive(self):
        packed = self.task()
        transfer(packed)
        archive_size = Path(packed["format_target"]).stat().st_size
        result = transfer(self.task("restore_archive", source=packed["format_target"],
                                    format_target=str(self.target_root / "restored"), target_kind="fss", delete=True))
        self.assertTrue(result["deleted"])
        self.assertEqual(result["freed_bytes"], archive_size)
        self.assertFalse(Path(packed["format_target"]).exists())

    def test_chunk_file_restore_reuse_and_foreign_conflict(self):
        store = self.target_root / "large-file"
        transfer(self.task("chunk", source=str(self.source / "a.txt"), format_target=str(store), chunk_size=2))
        self.assertEqual(formats.source_format(store), "chunks")
        restore = self.task("restore_chunks", source=str(store), target_kind="lustre",
                            format_target=str(self.target_root / "restored.txt"))
        self.assertFalse(transfer(restore)["reused"])
        self.assertTrue(transfer(restore)["reused"])
        Path(restore["format_target"]).write_text("foreign")
        with self.assertRaisesRegex(ValueError, "differs"):
            transfer(restore)
        self.assertEqual(Path(restore["format_target"]).read_text(), "foreign")

    def test_pack_and_chunk_stage_only_in_lustre_and_restore(self):
        packed = self.task(chunk_size=4096)
        transfer(packed)
        store = Path(packed["format_target"])
        self.assertEqual(formats.source_format(store), "chunks")
        self.assertEqual(chunks.read_manifest(store)["original_name"], "bundle.gbi.tar")
        self.assertTrue(transfer(packed)["reused"])
        result = transfer(self.task("restore_chunks", source=str(store), target_kind="fss",
                                    format_target=str(self.target_root / "restored")))
        self.assertFalse(result["deleted"])
        self.assertEqual((self.target_root / "restored" / "a.txt").read_text(), "alpha")
        self.assertEqual(list((self.state / "format-staging").iterdir()), [])

    def test_stage_reused_after_upload_failure_and_changed_source_refused(self):
        packed = self.task(chunk_size=4096)
        with patch.object(chunks, "write_chunks", side_effect=ValueError("upload interrupted")):
            with self.assertRaisesRegex(ValueError, "interrupted"):
                transfer(packed)
        stages = list((self.state / "format-staging").glob("*/bundle.gbi.tar"))
        self.assertEqual(len(stages), 1)
        inode = stages[0].stat().st_ino
        (self.source / "a.txt").write_text("changed")
        with self.assertRaises(ValueError):
            transfer(packed)
        self.assertEqual(stages[0].stat().st_ino, inode)
        self.assertFalse(Path(packed["format_target"]).exists())

    def test_unchanged_stage_resumes_without_repacking_to_disk(self):
        packed = self.task(chunk_size=4096)
        with patch.object(chunks, "write_chunks", side_effect=ValueError("upload interrupted")):
            with self.assertRaises(ValueError):
                transfer(packed)
        stage = next((self.state / "format-staging").glob("*/bundle.gbi.tar"))
        inode = stage.stat().st_ino
        original = chunks.write_chunks

        def resumed(source, *args, **kwargs):
            self.assertEqual(Path(source).stat().st_ino, inode)
            return original(source, *args, **kwargs)

        with patch.object(chunks, "write_chunks", side_effect=resumed):
            transfer(packed)
        self.assertFalse(stage.exists())

    def test_receipt_failure_keeps_source(self):
        with patch.object(formats, "receipt", side_effect=OSError("receipt failed")):
            with self.assertRaisesRegex(OSError, "receipt failed"):
                transfer(self.task(delete=True))
        self.assertTrue((self.source / "a.txt").exists())

    def test_owned_interrupted_pack_can_retry_without_overwriting_foreign_data(self):
        original = archives.pack

        def interrupted(source, output, **kwargs):
            output.write(b"owned incomplete")
            raise ValueError("interrupted")

        packed = self.task()
        with patch.object(archives, "pack", side_effect=interrupted):
            with self.assertRaisesRegex(ValueError, "interrupted"):
                transfer(packed)
        self.assertEqual(Path(packed["format_target"]).read_bytes(), b"owned incomplete")
        transfer(packed)
        self.assertTrue(archives.archive_marker(packed["format_target"]))

    def test_destination_mutation_after_receipt_keeps_archive_source(self):
        packed = self.task()
        transfer(packed)
        restore = self.task("restore_archive", source=packed["format_target"],
                            format_target=str(self.target_root / "restored"), target_kind="fss", delete=True)
        original = formats.receipt

        def receipt_then_change(path, record):
            original(path, record)
            if record["event"] == "verified":
                (self.target_root / "restored" / "a.txt").write_text("changed")

        with patch.object(formats, "receipt", side_effect=receipt_then_change):
            with self.assertRaises(ValueError):
                transfer(restore)
        self.assertTrue(Path(packed["format_target"]).exists())

    def test_staging_capacity_refusal_retains_sources(self):
        with patch.object(formats.shutil, "disk_usage", return_value=type("Space", (), {"free": 1})()):
            with self.assertRaisesRegex(ValueError, "not enough Lustre"):
                transfer(self.task(chunk_size=1024))
        self.assertTrue((self.source / "a.txt").exists())
        self.assertFalse((self.target_root / "bundle.gbi.tar").exists())

    def test_chunk_cleanup_retains_unknown_store_additions(self):
        store = self.target_root / "chunks"
        transfer(self.task("chunk", source=str(self.source / "a.txt"), format_target=str(store), chunk_size=2))
        (store / "unselected").write_text("keep")
        result = transfer(self.task("restore_chunks", source=str(store), target_kind="fss", delete=True,
                                    format_target=str(self.target_root / "restored")))
        self.assertTrue(result["deleted"])
        self.assertEqual((store / "unselected").read_text(), "keep")

    def test_chunk_cleanup_rechecks_destination_before_each_unlink(self):
        store = self.target_root / "chunks"
        transfer(self.task("chunk", source=str(self.source / "a.txt"), format_target=str(store), chunk_size=2))
        restored = self.target_root / "restored"
        original = Path.unlink
        changed = False

        def unlink_then_change(path, *args, **kwargs):
            nonlocal changed
            result = original(path, *args, **kwargs)
            if path == store / ".gbi" / "complete.json" and not changed:
                restored.write_text("changed")
                changed = True
            return result

        with patch.object(Path, "unlink", unlink_then_change):
            with self.assertRaisesRegex(ValueError, "destination changed"):
                transfer(self.task("restore_chunks", source=str(store), target_kind="fss", delete=True,
                                   format_target=str(restored)))
        self.assertTrue((store / ".gbi" / "parts" / "00000000.part").exists())

    def test_chunk_cleanup_checks_destination_before_container_removal(self):
        store = self.target_root / "chunks"
        transfer(self.task("chunk", source=str(self.source / "a.txt"), format_target=str(store), chunk_size=2))
        restored = self.target_root / "restored"
        parts = store / ".gbi" / "parts"
        original = Path.unlink

        def unlink_then_change(path, *args, **kwargs):
            result = original(path, *args, **kwargs)
            if path.parent == parts and not list(parts.iterdir()):
                restored.write_text("changed after final part removal")
            return result

        with patch.object(Path, "unlink", unlink_then_change):
            with self.assertRaisesRegex(ValueError, "destination changed"):
                transfer(self.task("restore_chunks", source=str(store), target_kind="fss", delete=True,
                                   format_target=str(restored)))
        self.assertTrue(parts.is_dir())
        self.assertNotIn("deleted", [record["event"] for record in self.records()])

    def test_incomplete_staging_rebuilt_only_for_unchanged_owned_source(self):
        packed = self.task(chunk_size=4096)

        def interrupted(source, output, **kwargs):
            output.write(b"incomplete")
            raise ValueError("interrupted staging")

        with patch.object(archives, "pack", side_effect=interrupted):
            with self.assertRaises(ValueError):
                transfer(packed)
        transfer(packed)
        self.assertEqual(formats.source_format(packed["format_target"]), "chunks")

    def test_changed_manifest_does_not_replace_owned_partial_chunk_restore(self):
        store = self.target_root / "chunks"
        transfer(self.task("chunk", source=str(self.source / "a.txt"), format_target=str(store), chunk_size=2))
        restore = self.task("restore_chunks", source=str(store), target_kind="fss",
                            format_target=str(self.target_root / "restored"))
        original = formats.write_json

        def interrupted(path, record):
            original(path, record)
            if record.get("phase") == "restoring":
                raise ValueError("interrupted restore")

        with patch.object(formats, "write_json", side_effect=interrupted):
            with self.assertRaisesRegex(ValueError, "interrupted restore"):
                transfer(restore)
        partial = Path(restore["format_target"])
        inode = partial.stat().st_ino
        manifest = json.loads((store / "manifest.json").read_text())
        manifest["original_name"] = "renamed.txt"
        encoded = chunks._encoded(manifest)
        (store / "manifest.json").write_bytes(encoded)
        (store / ".gbi" / "complete.json").write_bytes(chunks._encoded(chunks._completion(manifest, encoded)))
        with self.assertRaisesRegex(ValueError, "manifest changed"):
            transfer(restore)
        self.assertEqual(partial.stat().st_ino, inode)

    def test_content_recognition_does_not_interpret_ordinary_manifest(self):
        (self.source / "manifest.json").write_text('{"ordinary": true}')
        self.assertIsNone(formats.source_format(self.source))


if __name__ == "__main__":
    unittest.main()
