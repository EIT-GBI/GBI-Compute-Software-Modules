"""Focused replay tests for archive move cleanup."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from gbi_data import archive_state, archives, formats


class ArchiveCleanupResume(unittest.TestCase):
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
        (self.source / "b.txt").write_text("beta")
        (self.source / "keep.bin").write_text("keep")

    def task(self, **overrides):
        return {"format_action": "pack", "source": str(self.source),
                "format_target": str(self.target_root / "bundle.gbi.tar"),
                "source_root": str(self.base), "target_root": str(self.target_root),
                "source_kind": "fss", "target_kind": "alluxio", "state": str(self.state),
                "scratch_root": str(self.scratch), "receipt": str(self.base / "receipts.jsonl"),
                "progress": str(self.base / "progress.json"), "settle_seconds": 0,
                "delete": True, "pack": "tar", "include": ["*.txt"], **overrides}

    def records(self):
        return [json.loads(line) for line in (self.base / "receipts.jsonl").read_text().splitlines()]

    def fail_after_first_unlink(self):
        original = archives.cleanup_source
        self.first_removed = None
        failed = False

        def injected(source, manifest, stored, destination_unchanged=None):
            nonlocal failed

            def after_unlink(entry):
                nonlocal failed
                if not failed:
                    self.first_removed = entry["path"]
                    failed = True
                    raise ValueError("injected cleanup interruption")

            return original(source, manifest, stored, destination_unchanged=destination_unchanged,
                            on_remove=after_unlink)

        return patch.object(formats.archives, "cleanup_source", side_effect=injected)

    def remaining_root_files(self):
        selected = {"a.txt": 5, "b.txt": 4}
        self.assertIn(self.first_removed, selected)
        remaining = {path: size for path, size in selected.items() if path != self.first_removed}
        for path in selected:
            self.assertEqual((self.source / path).exists(), path in remaining)
        return remaining

    def journal(self, task):
        key = hashlib.sha256(task["format_target"].encode()).hexdigest()
        return json.loads((self.state / "pending" / f"{key}.format.json").read_text())

    def test_interrupted_cleanup_reuses_manifest_and_counts_only_newly_removed_bytes(self):
        task = self.task()
        with self.fail_after_first_unlink(), self.assertRaisesRegex(ValueError, "interruption"):
            formats.transfer(task)
        journal = self.journal(task)
        self.assertEqual(journal["phase"], "verified")
        self.assertIn("_source", journal["manifest"])
        remaining = self.remaining_root_files()

        result = formats.transfer(task)
        self.assertEqual(result["bytes"], 9)
        self.assertEqual(result["freed_bytes"], sum(remaining.values()))
        self.assertTrue((self.source / "keep.bin").exists())
        self.assertEqual([record["event"] for record in self.records()], ["verified", "deleted"])
        self.assertEqual(self.records()[-1]["freed_bytes"], sum(remaining.values()))

    def test_corrupt_destination_keeps_remaining_source(self):
        task = self.task()
        with self.fail_after_first_unlink(), self.assertRaises(ValueError):
            formats.transfer(task)
        target = Path(task["format_target"])
        target.write_bytes(target.read_bytes() + b"corrupt")
        with self.assertRaisesRegex(ValueError, "archive|tar|manifest"):
            formats.transfer(task)
        self.remaining_root_files()
        self.assertTrue((self.source / "keep.bin").exists())

    def test_added_source_path_keeps_remaining_source(self):
        task = self.task()
        with self.fail_after_first_unlink(), self.assertRaises(ValueError):
            formats.transfer(task)
        (self.source / "new.txt").write_text("new")
        with self.assertRaisesRegex(ValueError, "added|source"):
            formats.transfer(task)
        self.remaining_root_files()

    def test_changed_source_path_keeps_remaining_source(self):
        task = self.task()
        with self.fail_after_first_unlink(), self.assertRaises(ValueError):
            formats.transfer(task)
        (self.source / "a.txt").write_text("changed")
        with self.assertRaisesRegex(ValueError, "changed|checksum|source"):
            formats.transfer(task)
        self.remaining_root_files()

    def test_cleanup_complete_before_ack_is_idempotent(self):
        task = self.task(include=["a.txt"])
        with self.fail_after_first_unlink(), self.assertRaises(ValueError):
            formats.transfer(task)
        result = formats.transfer(task)
        self.assertEqual(result["bytes"], 5)
        self.assertEqual(result["freed_bytes"], 0)
        self.assertFalse((self.source / "a.txt").exists())
        self.assertTrue((self.source / "b.txt").exists())
        self.assertEqual([record["event"] for record in self.records()], ["verified", "deleted"])

    def test_resume_allows_directory_size_and_hardlink_ctime_changes(self):
        nested = self.source / "nested"
        nested.mkdir()
        (nested / "one.txt").write_text("same")
        (nested / "two.txt").hardlink_to(nested / "one.txt")
        task = self.task(include=["*.txt"])
        with self.fail_after_first_unlink(), self.assertRaises(ValueError):
            formats.transfer(task)
        result = formats.transfer(task)
        self.assertEqual(result["bytes"], 17)
        self.assertEqual(result["freed_bytes"], 17 - {"a.txt": 5, "b.txt": 4,
                                                       "nested/one.txt": 4, "nested/two.txt": 4}[self.first_removed])
        self.assertTrue((self.source / "keep.bin").exists())
        self.assertFalse(nested.exists())

    def test_receipt_write_replay_does_not_double_credit_cleanup(self):
        task = self.task(include=["a.txt"])
        original = formats.receipt

        def fail_after_receipt(path, record):
            original(path, record)
            if record["event"] == "deleted":
                raise ValueError("ack interrupted")

        with patch.object(formats, "receipt", side_effect=fail_after_receipt), self.assertRaisesRegex(ValueError, "ack"):
            formats.transfer(task)
        self.assertEqual(self.journal(task)["phase"], "cleanup_complete")
        result = formats.transfer(task)
        self.assertTrue(result["deleted"])
        self.assertEqual(result["freed_bytes"], 0)
        self.assertEqual([record["event"] for record in self.records()], ["verified", "deleted"])

    def test_resumed_copy_reverifies_corrupt_destination(self):
        task = self.task(delete=False)
        original = formats.receipt

        def fail_after_verified(path, record):
            original(path, record)
            if record["event"] == "verified":
                raise ValueError("copy acknowledgement interrupted")

        with patch.object(formats, "receipt", side_effect=fail_after_verified), self.assertRaisesRegex(ValueError, "acknowledgement"):
            formats.transfer(task)
        Path(task["format_target"]).write_bytes(Path(task["format_target"]).read_bytes() + b"corrupt")
        with self.assertRaisesRegex(ValueError, "archive|tar|manifest"):
            formats.transfer(task)
        self.assertTrue((self.source / "a.txt").exists())
        self.assertTrue((self.source / "b.txt").exists())

    def test_partial_cleanup_rejects_copy_retry_and_keeps_manifest(self):
        move = self.task()
        with self.fail_after_first_unlink(), self.assertRaises(ValueError):
            formats.transfer(move)
        copy_retry = {**move, "delete": False}
        with self.assertRaisesRegex(ValueError, "incomplete archive cleanup"):
            formats.transfer(copy_retry)
        self.assertEqual(self.journal(copy_retry)["phase"], "verified")
        self.remaining_root_files()

    def test_cleanup_complete_replay_reverifies_corrupt_destination(self):
        task = self.task(include=["a.txt"])
        original = formats.receipt

        def fail_after_deleted(path, record):
            original(path, record)
            if record["event"] == "deleted":
                raise ValueError("cleanup acknowledgement interrupted")

        with patch.object(formats, "receipt", side_effect=fail_after_deleted), self.assertRaisesRegex(ValueError, "acknowledgement"):
            formats.transfer(task)
        Path(task["format_target"]).write_bytes(Path(task["format_target"]).read_bytes() + b"corrupt")
        with self.assertRaisesRegex(ValueError, "archive|tar|manifest"):
            formats.transfer(task)
        self.assertFalse((self.source / "a.txt").exists())

    def test_replay_writes_deleted_receipt_to_new_run_path(self):
        task = self.task(include=["a.txt"])
        original = formats.receipt

        def fail_after_deleted(path, record):
            original(path, record)
            if record["event"] == "deleted":
                raise ValueError("cleanup acknowledgement interrupted")

        with patch.object(formats, "receipt", side_effect=fail_after_deleted), self.assertRaises(ValueError):
            formats.transfer(task)
        new_receipt = self.base / "new-run-receipts.jsonl"
        replay = {**task, "receipt": str(new_receipt)}
        result = formats.transfer(replay)
        self.assertTrue(result["deleted"])
        self.assertEqual([record["event"] for record in self.records()], ["verified", "deleted"])
        new_records = [json.loads(line) for line in new_receipt.read_text().splitlines()]
        self.assertEqual([record["event"] for record in new_records], ["verified", "deleted"])
        self.assertEqual(new_records[-1]["freed_bytes"], 0)
        self.assertIn("replay_of", new_records[-1])

    def test_archive_result_is_checkpointed_before_journal_close(self):
        task = self.task(include=["a.txt"], archive_result=str(self.scratch / "archive-result.json"))
        original = formats._owned_remove

        def fail_after_journal_close(path, identity, state):
            original(path, identity, state)
            if Path(path).name.endswith(".format.json"):
                raise ValueError("worker acknowledgement interrupted")

        with patch.object(formats, "_owned_remove", side_effect=fail_after_journal_close), self.assertRaisesRegex(ValueError, "acknowledgement"):
            formats.transfer(task)
        completed = json.loads(Path(task["archive_result"]).read_text())
        self.assertTrue(completed["deleted"])
        replay = {**task, "archive_destination": task["format_target"],
                  "archive_completed": completed, "empty": False}
        replay_result = archive_state.resume_completed(replay)
        self.assertTrue(replay_result["reused"])
        self.assertEqual(replay_result["freed_bytes"], 0)


if __name__ == "__main__":
    unittest.main()
