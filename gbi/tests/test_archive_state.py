"""Retries reuse the original partition, not a partially deleted source scan."""

import json
import os
from pathlib import Path
import pwd
import tempfile
import unittest
from unittest.mock import patch

from gbi_data import archive_plan, archive_state, archives, formats, transfer
from gbi_data.storage import Site


class ArchiveState(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name).resolve()
        user = pwd.getpwuid(os.getuid()).pw_name
        for kind in ("lustre", "bucket"):
            (self.base / kind / user).mkdir(parents=True)
        config = self.base / "site.conf"
        config.write_text(f"lustre_root={self.base / 'lustre'}\nbucket_root={self.base / 'bucket'}\n")
        self.site = Site(config)
        self.source = self.site.roots["lustre"] / "source"
        self.source.mkdir()
        (self.source / "child").mkdir()
        (self.source / "child/a").write_text("payload")
        self.target = self.site.roots["alluxio"] / "destination"
        self.home = self.site.roots["lustre"] / ".gbi"
        self.spec = {"source": str(self.source), "target": str(self.target),
                     "include": [], "exclude": [], "archive_chunk_threshold": 1024**3}

    def test_same_destination_replays_plan_after_source_cleanup(self):
        with archive_state.prepared(self.spec, self.site, self.home) as first:
            original = first["archive_plan"]
        (self.source / "child/a").unlink()
        with patch.object(archive_plan, "plan_archive", side_effect=AssertionError("must not replan")):
            with archive_state.prepared(self.spec, self.site, self.home) as replay:
                self.assertEqual(replay["archive_plan"], original)

    def test_changed_selection_refuses_destination_reuse(self):
        with archive_state.prepared(self.spec, self.site, self.home):
            pass
        with self.assertRaisesRegex(ValueError, "different saved archive plan"):
            with archive_state.prepared({**self.spec, "exclude": ["*.tmp"]}, self.site, self.home):
                pass

    def test_unplanned_existing_destination_is_not_mixed_with_new_layout(self):
        self.target.mkdir()
        (self.target / "existing").write_text("keep")
        with self.assertRaisesRegex(ValueError, "no matching saved plan"):
            with archive_state.prepared(self.spec, self.site, self.home):
                pass
        self.assertEqual((self.target / "existing").read_text(), "keep")

    def test_overlapping_coordinator_is_refused(self):
        with archive_state.prepared(self.spec, self.site, self.home):
            with self.assertRaisesRegex(ValueError, "active transfer"):
                with archive_state.prepared(self.spec, self.site, self.home):
                    pass

    def completed(self):
        self.target.mkdir()
        archive = self.target / "child.gbi.tar"
        with archive.open("xb") as output:
            manifest = archives.pack(self.source / "child", output)
        (self.source / "child/a").unlink()
        return {"source": str(self.source / "child"), "archive_destination": str(archive),
                "target_root": str(self.site.roots["alluxio"]), "target_kind": "alluxio",
                "format_action": "pack", "empty": False,
                "archive_completed": {"manifest_sha256": formats._manifest_digest(manifest),
                                      "deleted": True, "bytes": 7, "freed_bytes": 7}}

    def test_completed_move_reverifies_destination_without_double_counting(self):
        task = self.completed()
        result = archive_state.resume_completed(task)
        self.assertTrue(result["reused"])
        self.assertFalse(result["deleted"])
        self.assertEqual(result["freed_bytes"], 0)
        Path(task["archive_destination"]).write_bytes(b"broken")
        with self.assertRaises(ValueError):
            archive_state.resume_completed(task)

    def test_recreated_source_is_not_silently_skipped_or_deleted(self):
        task = self.completed()
        (self.source / "child/new").write_text("keep")
        with self.assertRaisesRegex(ValueError, "new selected files"):
            archive_state.resume_completed(task)
        self.assertEqual((self.source / "child/new").read_text(), "keep")

    def test_failed_planning_publishes_no_partial_plan(self):
        with patch.object(archive_plan, "plan_archive", side_effect=ValueError("incomplete")):
            with self.assertRaisesRegex(ValueError, "incomplete"):
                with archive_state.prepared(self.spec, self.site, self.home):
                    pass
        self.assertEqual(list((self.home / "archive-plans").glob("*.json")), [])

    def test_lost_loose_file_cleanup_ack_reuses_verified_copy(self):
        loose = self.source / "manifest.tsv"
        loose.write_text("inventory")
        state = self.home / "state"
        for path in (state / "locks", state / "pending"):
            path.mkdir(parents=True)
        original = transfer.receipt

        def lost_ack(path, record):
            original(path, record)
            if record["event"] == "deleted":
                raise ValueError("lost cleanup acknowledgement")

        with archive_state.prepared(self.spec, self.site, self.home) as planned:
            unit = next(item for item in planned["selection"] if item["source"] == str(loose))
            task = {**unit, "source_root": str(self.site.roots["lustre"]),
                    "target_root": str(self.site.roots["alluxio"]), "target_kind": "alluxio",
                    "target": str(self.target), "directory": True, "target_base": True,
                    "selection_root": str(self.source), "state": str(state),
                    "scratch_root": str(self.site.roots["lustre"]), "delete": True,
                    "receipt": str(self.home / "receipts.jsonl"), "progress": str(self.home / "progress.json"),
                    "rclone": None, "settle_seconds": 0}
            with patch.object(transfer, "receipt", side_effect=lost_ack), self.assertRaisesRegex(ValueError, "lost cleanup"):
                transfer.transfer(task)
        self.assertFalse(loose.exists())
        with archive_state.prepared(self.spec, self.site, self.home) as planned:
            replay = next(item for item in planned["selection"] if item["source"] == str(loose))
            self.assertIn("archive_completed", replay)
            result = archive_state.resume_completed({**task, **replay})
            self.assertEqual(result["freed_bytes"], 0)
            self.assertTrue(result["reused"])
