"""Read-only packing plans, budgets, filters and source-change refusal."""

from dataclasses import replace
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from gbi_data import packing


class Packing(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.source = Path(self.temporary.name).resolve()
        self.policy = packing.PackingPolicy(small_file_bytes=128, min_files=3,
                                            max_archive_bytes=4096, max_entries=1000,
                                            max_seconds=10)

    def files(self, directory, count=3, size=32, suffix=".txt"):
        path = self.source / directory
        path.mkdir(parents=True, exist_ok=True)
        for index in range(count):
            (path / f"file-{index}{suffix}").write_bytes(b"a" * size)

    def test_mixed_small_large_and_explicit_layout(self):
        self.files("node_modules")
        self.files("checkpoints", count=1, size=256)
        before = sorted(str(p) for p in self.source.rglob("*"))
        result = packing.plan(self.source, self.policy)
        self.assertEqual([c["source_relative"] for c in result["candidates"]], ["node_modules"])
        candidate = result["candidates"][0]
        self.assertEqual(candidate["archive_relative"], "node_modules.gbi.tar")
        self.assertEqual(candidate["restore_relative"], "node_modules")
        self.assertEqual(candidate["bytes"], 96)
        self.assertEqual(result["loose_selected"], ["checkpoints/file-0.txt"])
        self.assertFalse(result["staging_required"])
        self.assertEqual(before, sorted(str(p) for p in self.source.rglob("*")))

    def test_deepest_nonoverlapping_candidates(self):
        self.files("dependencies/first")
        self.files("dependencies/second")
        result = packing.plan(self.source, self.policy)
        self.assertEqual([c["source_relative"] for c in result["candidates"]],
                         ["dependencies/first", "dependencies/second"])
        self.assertEqual(result["loose_selected"], [])

    def test_one_file_leaves_are_not_packed_individually(self):
        for name in ("a", "b", "c"):
            self.files("dependencies/" + name, count=1)
        result = packing.plan(self.source, self.policy)
        self.assertEqual([c["source_relative"] for c in result["candidates"]], ["dependencies"])

    def test_adjacent_empty_directory_remains_loose(self):
        self.files("small")
        (self.source / "empty").mkdir()
        result = packing.plan(self.source, self.policy)
        self.assertEqual(result["loose_selected"], ["empty"])

    def test_filters_apply_inside_dependency_names(self):
        self.files(".git", suffix=".txt")
        self.files(".git", count=2, size=2048, suffix=".bin")
        result = packing.plan(self.source, self.policy, includes=["*.txt"], exclusions=["file-0*"])
        self.assertEqual(result["candidates"], [])
        self.assertEqual(result["loose_selected"], [".git/file-1.txt", ".git/file-2.txt"])
        result = packing.plan(self.source, self.policy, includes=["*.txt"])
        self.assertEqual([c["source_relative"] for c in result["candidates"]], [".git"])

    def test_links_not_followed_and_reserved_subtrees_omitted(self):
        self.files("safe")
        self.files(".gbi", count=5)
        self.files("private", count=5)
        (self.source / "safe" / "outside-link").symlink_to("/does/not/exist")
        result = packing.plan(self.source, self.policy, reserved_names=["private"])
        self.assertEqual([c["source_relative"] for c in result["candidates"]], ["safe"])
        self.assertEqual(result["candidates"][0]["selected_entries"], 4)

    def test_budget_reports_lower_bounds_not_guessed_totals(self):
        self.files("many", count=20)
        result = packing.plan(self.source, replace(self.policy, max_entries=5))
        self.assertFalse(result["complete"])
        self.assertEqual(result["visited_entries"], 5)
        self.assertEqual(result["candidates"], [])
        self.assertEqual(result["budget_exhausted"], "entry budget exhausted")
        self.assertTrue(any(c["totals_kind"] == "observed_lower_bound" for c in result["unqualified"]))

    def test_time_and_depth_bounds(self):
        self.files("deep/subtree")
        result = packing.plan(self.source, replace(self.policy, max_depth=1))
        self.assertFalse(result["complete"])
        self.assertEqual(result["candidates"], [])
        clock = iter(range(10000))
        with patch.object(packing.time, "monotonic", side_effect=lambda: next(clock)):
            result = packing.plan(self.source, replace(self.policy, max_seconds=0.5))
        self.assertFalse(result["complete"])
        self.assertEqual(result["candidates"], [])

    def test_changed_source_never_qualified(self):
        self.files("small")
        original = packing._stat
        changed = False

        def mutate(root_fd, name):
            nonlocal changed
            if not changed:
                (self.source / "small" / "file-0.txt").write_bytes(b"changed")
                changed = True
            return original(root_fd, name)

        with patch.object(packing, "_stat", side_effect=mutate):
            result = packing.plan(self.source, self.policy)
        self.assertFalse(result["complete"])
        self.assertEqual(result["candidates"], [])
        self.assertTrue(any("changed" in reason for item in result["unqualified"] for reason in item["reasons"]))

    def test_archive_name_collision_even_if_excluded(self):
        self.files("small")
        (self.source / "small.gbi.tar").write_text("existing")
        result = packing.plan(self.source, self.policy, includes=["*.txt"])
        self.assertFalse(any(c["source_relative"] == "small" for c in result["candidates"]))
        rejected = next(c for c in result["unqualified"] if c["source_relative"] == "small")
        self.assertTrue(any("collides" in reason for reason in rejected["reasons"]))

    def test_special_selected_blocks_but_excluded_does_not(self):
        self.files("small")
        os.mkfifo(self.source / "small" / "pipe")
        result = packing.plan(self.source, self.policy)
        self.assertEqual(result["candidates"], [])
        result = packing.plan(self.source, self.policy, includes=["*.txt"])
        self.assertEqual([c["source_relative"] for c in result["candidates"]], ["small"])

    def test_metadata_budget_rejects_wide_subtree_but_keeps_safe_sibling(self):
        wide = self.source / ("wide-" + "x" * 100)
        wide.mkdir()
        for index in range(3):
            (wide / f"file-{index}.txt").write_bytes(b"x")
        self.files("safe")

        with patch.object(packing.archives, "MAX_MANIFEST", 700):
            result = packing.plan(self.source, self.policy)

        self.assertEqual([item["source_relative"] for item in result["candidates"]], ["safe"])
        rejected = next(item for item in result["unqualified"]
                        if item["source_relative"] == wide.name)
        self.assertGreater(rejected["metadata_bytes"], 700)
        self.assertTrue(any("entry metadata" in reason for reason in rejected["reasons"]))
        self.assertEqual(result["loose_selected"], [f"{wide.name}/file-{i}.txt" for i in range(3)])

    def test_hardlinks_keep_file_totals_and_use_conservative_metadata_allowance(self):
        hard = self.source / "hard"
        hard.mkdir()
        (hard / "file-0.txt").write_bytes(b"x")
        (hard / "file-1.txt").hardlink_to(hard / "file-0.txt")
        (hard / "file-2.txt").write_bytes(b"x")
        self.files("safe")

        with patch.object(packing.archives, "MAX_MANIFEST", 570):
            result = packing.plan(self.source, self.policy)

        self.assertEqual([item["source_relative"] for item in result["candidates"]], ["safe"])
        rejected = next(item for item in result["unqualified"]
                        if item["source_relative"] == "hard")
        self.assertEqual(rejected["regular_files"], 3)
        self.assertEqual(rejected["selected_entries"], 3)
        self.assertEqual(rejected["bytes"], 3)
        self.assertGreater(rejected["metadata_bytes"], 570)

    def test_policy_and_plan_are_explicit_and_deterministic(self):
        self.files("small")
        with self.assertRaises(ValueError):
            replace(self.policy, min_files=1)
        with self.assertRaises(ValueError):
            replace(self.policy, max_seconds=float("nan"))
        self.assertEqual(packing.plan(self.source, self.policy), packing.plan(self.source, self.policy))


if __name__ == "__main__":
    unittest.main()
