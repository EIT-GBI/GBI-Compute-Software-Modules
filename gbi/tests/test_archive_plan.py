"""Deterministic archive-set planning without payload or destination writes."""

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from gbi_data import archives, archive_plan, chunks


class ArchivePlan(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.source = Path(self.temporary.name).resolve() / "source"
        self.source.mkdir()
        self.destination = Path(self.temporary.name).resolve() / "destination"

    def files(self, relative, count=2, size=8):
        path = self.source / relative
        path.mkdir(parents=True, exist_ok=True)
        for index in range(count):
            (path / f"file-{index}.txt").write_bytes(b"x" * size)

    def test_recursive_split_has_complete_disjoint_mapping(self):
        self.files("large/first", count=2)
        self.files("large/second", count=2)
        (self.source / "large/root.txt").write_bytes(b"root")
        with patch.object(archives, "MAX_MANIFEST", 500):
            plan = archive_plan.plan_archive(self.source, self.destination)
        units = {item["source_relative"]: item for item in plan.units}
        self.assertEqual(units["large/first"]["target_relative"], "large/first.gbi.tar")
        self.assertEqual(units["large/second"]["target_relative"], "large/second.gbi.tar")
        self.assertEqual(units["large/root.txt"]["action"], "file")
        self.assertEqual(len(plan.units), 3)
        self.assertEqual(plan.as_dict()["units"], list(plan.units))

    def test_large_packed_unit_chooses_chunks_and_scales_parts(self):
        self.files("payload", count=2, size=4096)
        plan = archive_plan.plan_archive(self.source, self.destination,
                                         chunk_threshold_bytes=1, chunk_size_bytes=1)
        unit = plan.units[0]
        self.assertEqual(unit["target_relative"], "payload.gbi.tar.gbi-chunks")
        self.assertGreaterEqual(unit["chunk_size"], 2)
        self.assertLessEqual((unit["estimated_bytes"] + unit["chunk_size"] - 1) // unit["chunk_size"], 4096)

    def test_empty_directories_use_portable_archives_and_links_remain_plain(self):
        (self.source / "empty").mkdir()
        (self.source / "link").symlink_to("missing")
        plan = archive_plan.plan_archive(self.source, self.destination)
        units = {item["source_relative"]: item for item in plan.units}
        self.assertEqual(units["empty"]["action"], "pack")
        self.assertEqual(units["empty"]["target_relative"], "empty.gbi.tar")
        self.assertEqual(units["empty"]["bytes"], 0)
        self.assertEqual(units["link"]["action"], "file")
        self.assertEqual(units["link"]["bytes"], len("missing"))
        self.assertNotIn("missing", units)

    def test_physical_encoded_name_collision_is_rejected(self):
        self.files("data")
        (self.source / "data.gbi.tar").write_text("ordinary")
        with self.assertRaisesRegex(ValueError, "explicit restore"):
            archive_plan.plan_archive(self.source, self.destination)

    def test_symlink_physical_encoding_collision_is_rejected(self):
        (self.source / "link").symlink_to("missing")
        (self.source / "link.rclonelink").write_text("ordinary")
        with self.assertRaisesRegex(ValueError, "collides with source path"):
            archive_plan.plan_archive(self.source, self.destination)

    def test_filters_apply_to_files_and_empty_directories(self):
        folder = self.source / "folder"
        folder.mkdir()
        (folder / "keep.txt").write_text("keep")
        (folder / "skip.tmp").write_text("skip")
        (self.source / "skip.tmp").write_text("skip")
        (self.source / "empty.tmp").mkdir()
        plan = archive_plan.plan_archive(self.source, self.destination, exclusions=("*.tmp",))
        self.assertEqual([unit["source_relative"] for unit in plan.units], ["folder"])
        self.assertEqual(plan.units[0]["bytes"], 4)

    def test_reserved_names_are_not_discovered(self):
        self.files("visible")
        self.files("private")
        self.files(".gbi/state")
        plan = archive_plan.plan_archive(self.source, self.destination, reserved_names=("private",))
        self.assertEqual([unit["source_relative"] for unit in plan.units], ["visible"])

    def test_discovery_entry_budget_fails_before_returning_plan(self):
        self.files("many", count=2)
        with self.assertRaisesRegex(archives.ArchiveError, "budget exhausted"):
            archive_plan.plan_archive(self.source, self.destination, max_entries=1)

    def test_child_metadata_scan_reports_progress_and_obeys_deadline(self):
        self.files("many", count=2)
        elapsed = 0

        def scan(path, **kwargs):
            nonlocal elapsed
            self.assertIn("visit", kwargs)
            elapsed = 6
            kwargs["visit"]()

        with patch.object(archives, "check_metadata_budget", side_effect=scan):
            with patch.object(archive_plan.time, "monotonic", side_effect=lambda: elapsed):
                with self.assertRaisesRegex(archives.ArchiveError, "budget exhausted"):
                    archive_plan.plan_archive(self.source, self.destination, max_seconds=5)

    def test_source_mutation_during_qualification_is_rejected(self):
        self.files("mutable")
        original = archives.check_metadata_budget

        def mutate(path, **kwargs):
            (self.source / "mutable/file-0.txt").write_bytes(b"changed")
            return original(path, **kwargs)

        with patch.object(archives, "check_metadata_budget", side_effect=mutate):
            with self.assertRaisesRegex(archives.ArchiveError, "source changed"):
                archive_plan.plan_archive(self.source, self.destination)

    def test_restore_name_collision_is_rejected(self):
        units = [
            {"source_relative": "a", "target_relative": "a.gbi.tar", "action": "pack",
             "bytes": 1, "expected_source": [], "chunk_size": None},
            {"source_relative": "b", "target_relative": "a", "action": "directory",
             "bytes": 0, "expected_source": [], "chunk_size": None},
        ]
        with self.assertRaisesRegex(ValueError, "restore path collides"):
            archive_plan._validate_units(units, {})

    def test_non_oversize_scan_error_returns_no_plan(self):
        self.files("broken")
        with patch.object(archives, "check_metadata_budget",
                          side_effect=archives.ArchiveError("source changed during scan")):
            with self.assertRaisesRegex(archives.ArchiveError, "source changed"):
                archive_plan.plan_archive(self.source, self.destination)


if __name__ == "__main__":
    unittest.main()
