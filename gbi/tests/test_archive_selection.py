"""Explicit relative-path codec selection without changing CLI basenames."""

import io
import os
from pathlib import Path
import tempfile
import unittest

from gbi_data import archives, packing


class ArchiveSelection(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name).resolve()
        self.source = self.base / "source"
        self.source.mkdir()
        for name in ("root.pt", "nested/a.pt", "nested/b.pt", "nested/deep/a.pt",
                     "other/a.pt", "nested/a.bin"):
            path = self.source / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(name)
        (self.source / "nested/empty").mkdir()
        (self.source / "other/empty").mkdir()

    def pack(self, **kwargs):
        output = io.BytesIO()
        budget = archives.check_metadata_budget(self.source, **kwargs)
        manifest = archives.pack(self.source, output, **kwargs)
        self.assertEqual(budget["entries"], len(manifest["entries"]))
        self.assertEqual(budget["selected_bytes"], sum(e["size"] for e in manifest["entries"]))
        self.assertEqual(budget["manifest_bytes"], len(archives._json(archives._public(manifest))))
        archives.verify_archive(output, manifest)
        return output, manifest

    def files(self, manifest):
        return {e["path"] for e in manifest["entries"] if e["type"] != "directory"}

    def test_default_basename_and_explicit_relative_globs(self):
        _, default = self.pack(includes=["*.pt"])
        self.assertEqual(self.files(default), {"root.pt", "nested/a.pt", "nested/b.pt",
                                              "nested/deep/a.pt", "other/a.pt"})
        _, root = self.pack(includes=["*.pt"], selection_mode="relative")
        self.assertEqual(self.files(root), {"root.pt"})
        for compression in (None, "gzip"):
            with self.subTest(compression=compression):
                stream, manifest = self.pack(compression=compression,
                    includes=["nested/**/*.pt", "root.pt"], exclusions=["nested/deep/**", "**/b.pt"],
                    selection_mode="relative")
                self.assertEqual(self.files(manifest), {"root.pt", "nested/a.pt"})
                result = archives.restore(stream, self.base / str(compression))
                self.assertTrue(result["verified"])
                self.assertFalse(result["partial"])

    def test_segment_patterns_and_exclusion_only(self):
        _, one_level = self.pack(includes=["nested/?.pt"], selection_mode="relative")
        self.assertEqual(self.files(one_level), {"nested/a.pt", "nested/b.pt"})
        _, recursive = self.pack(includes=["**/a.pt"], selection_mode="relative")
        self.assertEqual(self.files(recursive), {"nested/a.pt", "nested/deep/a.pt", "other/a.pt"})
        _, excluded = self.pack(exclusions=["nested/**"], selection_mode="relative")
        self.assertEqual(self.files(excluded), {"root.pt", "other/a.pt"})

    def test_subtree_prefix_preserves_original_selection_but_not_member_names(self):
        source = self.source / "nested"
        options = {"selection_mode": "relative", "selection_prefix": "nested",
                   "includes": ["nested/**/*.pt", "nested/empty"],
                   "exclusions": ["nested/deep/**", "**/b.pt"]}
        budget = archives.check_metadata_budget(source, **options)
        stream = io.BytesIO()
        manifest = archives.pack(source, stream, **options)
        self.assertEqual(self.files(manifest), {"a.pt"})
        self.assertEqual({entry["path"] for entry in manifest["entries"]}, {"a.pt", "empty"})
        self.assertEqual(budget["entries"], len(manifest["entries"]))
        self.assertEqual(budget["selected_bytes"], sum(entry["size"] for entry in manifest["entries"]))
        archives.verify_archive(stream, manifest)
        target = self.base / "subtree-restored"
        archives.restore(stream, target)
        self.assertEqual((target / "a.pt").read_bytes(), (source / "a.pt").read_bytes())
        self.assertTrue((target / "empty").is_dir())
        self.assertFalse((target / "nested").exists())

    def test_invalid_selection_prefix_fails_before_output(self):
        for prefix in (None, 1, "/root", "..", "a/../b", "a//b", "a/", "a\\b", "."):
            with self.subTest(prefix=prefix):
                output = io.BytesIO()
                with self.assertRaises(archives.ArchiveError):
                    archives.pack(self.source, output, selection_mode="relative", selection_prefix=prefix)
                self.assertEqual(output.getvalue(), b"")
                with self.assertRaises(archives.ArchiveError):
                    archives.check_metadata_budget(self.source, selection_mode="relative", selection_prefix=prefix)
        with self.assertRaisesRegex(archives.ArchiveError, "relative mode"):
            archives.pack(self.source, io.BytesIO(), selection_prefix="nested")

    def test_relative_empty_directories_keep_required_ancestors(self):
        stream, manifest = self.pack(includes=["nested/empty"], selection_mode="relative")
        self.assertEqual({e["path"] for e in manifest["entries"]}, {"nested", "nested/empty"})
        target = self.base / "empty-restored"
        archives.restore(stream, target, includes=["nested/empty"], selection_mode="relative")
        self.assertTrue((target / "nested/empty").is_dir())
        self.assertFalse((target / "other").exists())
        _, excluded = self.pack(exclusions=["**/empty"], selection_mode="relative")
        self.assertFalse(any(e["path"].endswith("/empty") for e in excluded["entries"]))

    def test_relative_cleanup_removes_only_verified_selected_paths(self):
        stream, manifest = self.pack(includes=["nested/**/*.pt"], exclusions=["nested/deep/**"],
                                     selection_mode="relative")
        self.assertEqual(archives.cleanup_source(self.source, manifest, stream), 2)
        self.assertFalse((self.source / "nested/a.pt").exists())
        self.assertFalse((self.source / "nested/b.pt").exists())
        self.assertEqual((self.source / "nested/deep/a.pt").read_text(), "nested/deep/a.pt")
        self.assertEqual((self.source / "nested/a.bin").read_text(), "nested/a.bin")
        self.assertTrue((self.source / "nested/empty").is_dir())
        self.assertTrue((self.source / "other/a.pt").is_file())

    def test_cleanup_callback_accounts_success_before_a_late_failure(self):
        stream, manifest = self.pack(includes=["nested/*.pt"], selection_mode="relative")
        removed = []

        def on_remove(entry):
            self.assertFalse((self.source / entry["path"]).exists())
            removed.append(entry)
            other = next(e for e in manifest["entries"] if e["type"] != "directory"
                         and e["path"] != entry["path"])
            (self.source / other["path"]).write_text("changed after first unlink")

        with self.assertRaisesRegex(archives.ArchiveError, "changed"):
            archives.cleanup_source(self.source, manifest, stream, on_remove=on_remove)
        self.assertEqual(len(removed), 1)
        self.assertEqual(removed[0]["size"], len(removed[0]["path"]))

    def test_relative_restore_keeps_hardlink_content_without_unselected_names(self):
        os.link(self.source / "root.pt", self.source / "nested/alias.pt")
        stream, manifest = self.pack()
        alias = next(e for e in manifest["entries"] if e["type"] == "hardlink")
        target = self.base / "alias-restored"
        result = archives.restore(stream, target, includes=[alias["path"]],
                                  exclusions=[alias["target"]], selection_mode="relative")
        self.assertTrue(result["verified"])
        self.assertTrue(result["partial"])
        self.assertEqual((target / alias["path"]).read_bytes(), (self.source / alias["path"]).read_bytes())
        self.assertFalse((target / alias["target"]).exists())
        full_target = self.base / "links-restored"
        archives.restore(stream, full_target, includes=["root.pt", "nested/alias.pt"], selection_mode="relative")
        self.assertEqual((full_target / "root.pt").stat().st_ino,
                         (full_target / "nested/alias.pt").stat().st_ino)

    def test_restore_union_then_exclusions_and_required_directories(self):
        stream, _ = self.pack()
        target = self.base / "filtered"
        archives.restore(stream, target, includes=["nested/**/*.pt", "other/*.pt"],
                         exclusions=["nested/deep/**", "**/b.pt"], selection_mode="relative")
        self.assertEqual({p.relative_to(target).as_posix() for p in target.rglob("*") if p.is_file()},
                         {"nested/a.pt", "other/a.pt"})
        self.assertFalse((target / "nested/empty").exists())

    def test_invalid_selection_rejected_before_output_or_destination_writes(self):
        stream, _ = self.pack()
        for options in ({"selection_mode": "unknown"}, *(
                {"selection_mode": "relative", "includes": [pattern]}
                for pattern in ("/absolute", "../parent", "a//b", "a/**b", "{a,b}", ""))):
            with self.subTest(options=options):
                output = io.BytesIO()
                with self.assertRaises(archives.ArchiveError):
                    archives.pack(self.source, output, **options)
                self.assertEqual(output.getvalue(), b"")
                with self.assertRaises(archives.ArchiveError):
                    archives.check_metadata_budget(self.source, **options)
                with self.assertRaises(archives.ArchiveError):
                    archives.restore(stream, self.base / "invalid", **options)
                self.assertFalse((self.base / "invalid").exists())

    def test_packing_plan_relative_filters_match_codec(self):
        policy = packing.PackingPolicy(small_file_bytes=128, min_files=2,
            max_archive_bytes=4096, max_entries=1000, max_seconds=10)
        result = packing.plan(self.source, policy, includes=["nested/*.pt", "other/empty"],
                              selection_mode="relative")
        self.assertTrue(result["complete"])
        self.assertEqual([c["source_relative"] for c in result["candidates"]], ["nested"])
        self.assertEqual(result["candidates"][0]["regular_files"], 2)
        self.assertEqual(result["loose_selected"], ["other/empty"])
        excluded = packing.plan(self.source, policy, includes=["nested/*.pt"],
                                exclusions=["**/b.pt"], selection_mode="relative")
        self.assertEqual(excluded["candidates"], [])
        self.assertEqual(excluded["loose_selected"], ["nested/a.pt"])


if __name__ == "__main__":
    unittest.main()
