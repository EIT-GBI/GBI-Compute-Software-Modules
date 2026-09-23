"""Logical units preserve layouts, filters, discovery bounds and ordinary files."""

from pathlib import Path
import pwd
import os
import tempfile
import unittest
from unittest.mock import patch

from gbi_data import archives, chunks, format_selection as selection, packing
from gbi_data.storage import Site


class FormatSelection(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name).resolve()
        username = pwd.getpwuid(os.getuid()).pw_name
        for kind in ("lustre", "fss", "bucket"):
            (self.base / kind / username).mkdir(parents=True)
        config = self.base / "site.conf"
        config.write_text("\n".join(f"{key}={self.base / kind}" for key, kind in
                                   (("lustre_root", "lustre"), ("fss_root", "fss"), ("bucket_root", "bucket"))))
        self.site = Site(config)
        self.source = self.site.roots["lustre"] / "source"
        self.source.mkdir()
        self.target = self.site.roots["alluxio"] / "destination"
        self.spec = {"source": str(self.source), "target": str(self.target), "requested_target": str(self.target),
                     "source_kind": "lustre", "source_root": str(self.site.roots["lustre"]),
                     "target_kind": "alluxio", "target_root": str(self.site.roots["alluxio"]),
                     "directory": True, "include": [], "exclude": [], "delete": False}
        self.state = self.site.roots["lustre"] / ".gbi" / "state"
        self.state.mkdir(parents=True)

    def archive(self, name="nested.gbi.tar"):
        content = self.base / "content"
        content.mkdir(exist_ok=True)
        (content / "selected.txt").write_text("payload")
        output = self.source / name
        with output.open("xb") as stream:
            archives.pack(content, stream)
        return output

    def configured(self, **kwargs):
        return selection.configure(self.spec, self.site, **kwargs)

    def test_whole_pack_is_one_unit_with_source_size_probe(self):
        (self.source / "a").write_bytes(b"abc")
        self.spec["requested_target"] = str(self.target.with_suffix(".gbi.tar"))
        configured = self.configured(pack="tar")
        self.assertFalse(configured["directory"])
        units = selection.foreground_units(configured, self.site)
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]["format_action"], "pack")
        self.assertEqual(units[0]["format_source_size"], 3)

    def test_nested_marked_archive_ignores_container_basename_filter(self):
        self.archive()
        self.spec.update(include=["*.txt"], target_kind="fss")
        configured = self.configured()
        unit = next(selection.iter_units(configured, self.site))
        self.assertEqual(unit["format_action"], "restore_archive")
        self.assertEqual(unit["format_target"], str(self.target / "nested"))
        self.assertIsNone(selection.foreground_units(configured, self.site))

    def test_explicit_renamed_archive_is_recognized(self):
        path = self.archive("renamed.bin")
        self.spec.update(source=str(path), directory=False, target_kind="fss")
        configured = self.configured()
        self.assertEqual(configured["source_format"], "archive")
        self.assertEqual(next(selection.iter_units(configured, self.site))["format_target"], str(self.target))

    def test_nested_ordinary_files_are_not_sniffed(self):
        for index in range(20):
            (self.source / f"file-{index}.npz").write_bytes(b"\xff\xfe")
        configured = self.configured()
        with patch.object(selection.formats, "source_format", side_effect=AssertionError("ordinary payload sniffed")):
            self.assertEqual(len(list(selection.iter_units(configured, self.site))), 20)

    def test_chunk_unit_never_descends_internal_parts_and_filters_logical_name(self):
        plain = self.base / "original.txt"
        plain.write_text("contents")
        store = self.source / "original.txt.gbi-chunks"
        chunks.write_chunks(plain, store, state=self.state, chunk_size=2)
        self.spec["target_kind"] = "fss"
        configured = self.configured()
        units = list(selection.iter_units(configured, self.site))
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]["format_action"], "restore_chunks")
        self.assertEqual(units[0]["format_target"], str(self.target / "original.txt"))
        configured["include"] = ["*.bin"]
        self.assertEqual(list(selection.iter_units(configured, self.site)), [])

    def test_explicit_and_nested_raw_chunk_exclusion_uses_content(self):
        plain = self.base / "ordinary.gbi.tar"
        plain.write_bytes(b"not a marked archive")
        store = self.source / "ordinary.gbi.tar.gbi-chunks"
        chunks.write_chunks(plain, store, state=self.state, chunk_size=8)
        self.spec.update(target_kind="fss", exclude=["*.gbi.tar"])
        self.assertEqual(list(selection.iter_units(self.configured(), self.site)), [])
        self.spec.update(source=str(store), directory=False)
        self.assertEqual(list(selection.iter_units(self.configured(), self.site)), [])
        self.assertTrue((store / ".gbi" / "complete.json").exists())

    def test_chunk_creation_names_and_symlinks(self):
        (self.source / "file").write_text("payload")
        (self.source / "link").symlink_to("file")
        units = list(selection.iter_units(self.configured(chunk_size=1024), self.site))
        regular = next(u for u in units if Path(u["source"]).name == "file")
        link = next(u for u in units if Path(u["source"]).name == "link")
        self.assertTrue(regular["format_target"].endswith("file.gbi-chunks"))
        self.assertNotIn("format_action", link)

    def test_explicit_chunk_suffix_has_actionable_error(self):
        source = self.source / "file"
        source.write_text("payload")
        self.spec.update(source=str(source), directory=False,
                         requested_target=str(self.target) + ".gbi-chunks")
        with self.assertRaisesRegex(ValueError, "adds .gbi-chunks automatically"):
            self.configured(chunk_size=1024)

    def test_selective_packing_has_disjoint_units_and_keeps_root_layout(self):
        (self.source / "dependency").mkdir()
        for index in range(4):
            (self.source / "dependency" / str(index)).write_text("a")
        (self.source / "large").write_bytes(bytes(1000))
        policy = packing.PackingPolicy(10, 3, 100, 100, 10)
        configured = self.configured(packing_policy=policy)
        units = list(selection.iter_units(configured, self.site))
        self.assertEqual(len(units), 2)
        candidate = next(u for u in units if u.get("format_action") == "pack")
        self.assertTrue(candidate["format_target"].endswith("dependency.gbi.tar"))
        self.assertEqual(selection.dry_run(configured, self.site)["candidates"][0]["restore_relative"], "dependency")

    def test_packing_dry_run_summarizes_incomplete_large_preview(self):
        policy = packing.PackingPolicy(10, 3, 100, 100, 10)
        configured = self.configured(packing_policy=policy)
        planned = {
            "version": 1, "complete": False, "visited_entries": 100,
            "budget_exhausted": "entry budget exhausted", "candidates": [{
                "source_relative": "candidate", "archive_relative": "candidate.gbi.tar",
                "restore_relative": "candidate", "bytes": 30,
            }],
            "unqualified": [{
                # A root row includes its nested directory rollups; these rows
                # must not be summed into a purported total.
                "source_relative": "." if index == 0 else ("nested" if index == 1 else f"directory-{index}"),
                "regular_files": 2, "bytes": 20,
                "reasons": (["entry budget exhausted", "discovery budget exhausted during reconciliation"]
                            if index == 0 else ["entry budget exhausted"]),
            } for index in range(1000)],
            "loose_selected": [f"loose-{index}" for index in range(10000)],
            "totals_note": "lower bounds", "staging_required": False,
        }
        with patch.object(selection.packing, "plan", return_value=planned):
            preview = selection.dry_run(configured, self.site)
        self.assertEqual(len(preview["candidates"]), 1)
        self.assertEqual(preview["unqualified_summary"]["directory_count"], 1000)
        self.assertEqual(preview["unqualified_summary"]["reason_counts"]["discovery budget"], 1000)
        self.assertNotIn("selected_entries_lower_bound", preview["unqualified_summary"])
        self.assertEqual(preview["loose_selected_summary"]["entry_count"], 10000)
        self.assertLess(len(str(preview)), 10000)
        self.assertIn("lower bounds", preview["preview_notice"])
        self.assertNotIn("unqualified", preview.keys())
        self.assertNotIn("loose_selected", preview.keys())

    def test_excluded_entries_count_toward_probe_budget(self):
        for index in range(10):
            (self.source / f"file-{index}.bin").write_text("a")
        self.site.values["inline_scan_entries"] = "2"
        configured = self.configured()
        configured["include"] = ["*.txt"]
        with self.assertRaisesRegex(ValueError, "discovery budget"):
            selection.foreground_units(configured, self.site)

    def test_mixed_restore_collision_refused(self):
        self.archive()
        (self.source / "nested").mkdir()
        self.spec["target_kind"] = "fss"
        with self.assertRaisesRegex(ValueError, "collides"):
            list(selection.iter_units(self.configured(), self.site))


if __name__ == "__main__":
    unittest.main()
