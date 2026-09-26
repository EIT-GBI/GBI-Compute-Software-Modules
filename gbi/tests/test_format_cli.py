"""Public copy/move commands exercise format selection, workers and history."""

import contextlib
import io
import json
import os
from pathlib import Path
import pwd
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

from gbi_data import cli, jobs


class FormatCLI(unittest.TestCase):
    def setUp(self):
        if not shutil.which("rclone"):
            self.skipTest("format CLI integration requires the maintained rclone dependency")
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name).resolve()
        user = pwd.getpwuid(os.getuid()).pw_name
        self.roots = {kind: self.base / kind / user for kind in ("lustre", "fss", "bucket")}
        for path in self.roots.values():
            path.mkdir(parents=True)
        self.config = self.base / "site.conf"
        self.config.write_text("".join(f"{kind}_root = {path.parent}\n" for kind, path in self.roots.items())
                               + "pack_min_files = 2\nverify_settle_seconds = 1\n")

    def run_cli(self, *arguments):
        output = io.StringIO()
        # Encoded archive sizes are unknown before verification; an existing
        # allocation must be reused rather than submitting a nested job.
        with patch.dict(os.environ, {"GBI_DATA_SITE_CONF": str(self.config), "SLURM_JOB_ID": "12345"}), \
             patch.object(sys, "argv", ["gbi", "data", *map(str, arguments)]), \
             contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            result = cli.main()
        self.assertEqual(result, 0, output.getvalue())
        return output.getvalue()

    def fixture(self, kind):
        source = self.roots[kind] / "experiment"
        source.mkdir()
        (source / "small").mkdir()
        (source / "small/a.txt").write_bytes(b"alpha")
        (source / "small/b.txt").write_bytes(b"beta")
        (source / "empty").mkdir()
        (source / "dangling").symlink_to("missing")
        return source

    def test_tar_and_gzip_restore_across_native_filesystems(self):
        for origin, destination, compression in (("lustre", "fss", "tar"), ("fss", "lustre", "gzip")):
            with self.subTest(compression=compression):
                source = self.fixture(origin)
                suffix = ".gbi.tar.gz" if compression == "gzip" else ".gbi.tar"
                archive = self.roots["bucket"] / (origin + suffix)
                target = self.roots[destination] / (origin + "-restored")
                target.mkdir()  # Extraction goes here, without appending the archive's filename.
                self.run_cli("copy", source, archive, "--pack", compression)
                self.run_cli("move", archive, target)
                self.assertEqual((target / "small/a.txt").read_bytes(), b"alpha")
                self.assertEqual(os.readlink(target / "dangling"), "missing")
                self.assertTrue((target / "empty").is_dir())
                self.assertTrue(archive.exists(), "object-storage originals are retained by default")

    def test_chunked_archive_restores_without_assembling_parts(self):
        source = self.fixture("lustre")
        archive = self.roots["bucket"] / "bundle.gbi.tar"
        self.run_cli("copy", source, archive, "--pack", "tar", "--chunk-size", "4KiB")
        store = Path(str(archive) + ".gbi-chunks")
        self.assertTrue((store / "manifest.json").is_file())
        restored = self.roots["fss"] / "restored"
        before = {path.relative_to(store): path.read_bytes() for path in store.rglob("*") if path.is_file()}
        self.run_cli("move", store, restored)
        self.assertEqual((restored / "small/b.txt").read_bytes(), b"beta")
        self.assertEqual(before, {path.relative_to(store): path.read_bytes() for path in store.rglob("*") if path.is_file()})
        self.assertFalse(list((self.roots["lustre"] / ".gbi/state/format-staging").glob("*/payload*")))

    def test_chunked_file_restores_to_named_file(self):
        source = self.roots["lustre"] / "checkpoint.bin"
        source.write_bytes(b"checkpoint" * 1000)
        destination = self.roots["bucket"] / "checkpoint.bin"
        self.run_cli("copy", source, destination, "--chunk-size", "4KiB")
        target = self.roots["fss"] / "checkpoint-restored.bin"
        store = Path(str(destination) + ".gbi-chunks")
        before = {path.relative_to(store): path.read_bytes() for path in store.rglob("*") if path.is_file()}
        self.run_cli("move", store, target)
        self.assertEqual(target.read_bytes(), source.read_bytes())
        self.assertEqual(before, {path.relative_to(store): path.read_bytes() for path in store.rglob("*") if path.is_file()})

    def test_selective_move_dry_run_and_mixed_restore_preserve_excluded_source(self):
        source = self.fixture("lustre")
        (source / "small/keep.tmp").write_bytes(b"do not move")
        (source / "checkpoint.bin").write_bytes(b"large" * 20000)
        target = self.roots["bucket"] / "mixed"
        before = sorted(path.relative_to(self.base) for path in self.base.rglob("*"))
        output = self.run_cli("move", source, target, "--pack-small", "--exclude", "*.tmp", "--dry-run")
        self.assertIn("small.gbi.tar", output)
        self.assertIn("fully observed", output)
        self.assertEqual(before, sorted(path.relative_to(self.base) for path in self.base.rglob("*")))
        self.run_cli("move", source, target, "--pack-small", "--exclude", "*.tmp")
        self.assertEqual((source / "small/keep.tmp").read_bytes(), b"do not move")
        self.assertFalse((source / "small/a.txt").exists())
        self.assertTrue((target / "small.gbi.tar").is_file())
        self.assertTrue((target / "checkpoint.bin").is_file())
        restored = self.roots["fss"] / "restored-mixed"
        self.run_cli("copy", target, restored)
        self.assertEqual((restored / "small/a.txt").read_bytes(), b"alpha")
        self.assertFalse((restored / "small/keep.tmp").exists())
        self.assertEqual((restored / "checkpoint.bin").read_bytes(), b"large" * 20000)

    def test_partial_archive_move_retains_container_and_counts_selected_bytes(self):
        source = self.fixture("lustre")
        archive = self.roots["bucket"] / "partial.gbi.tar"
        self.run_cli("copy", source, archive, "--pack", "tar")
        target = self.roots["fss"] / "partial"
        self.run_cli("move", archive, target, "--include", "a.txt")
        self.assertTrue(archive.exists())
        self.assertTrue((target / "small/a.txt").is_file())
        self.assertFalse((target / "small/b.txt").exists())
        requests = list((self.roots["bucket"] / ".gbi/transfers/12345").glob("*/request.json"))
        request = next(path for path in requests if json.loads(path.read_text())["target"] == str(target))
        progress = json.loads(request.with_name("progress.json").read_text())
        self.assertEqual(progress["bytes"], 5)
        self.assertEqual(progress["discovered_bytes"], 5)
        self.assertTrue(progress["totals_known"])
        self.assertEqual(progress["freed"], 0)


class SizeFlags(unittest.TestCase):
    def test_human_readable_binary_sizes(self):
        for value, expected in (("64MiB", 64 * 1024 ** 2), ("1G", 1024 ** 3), ("4096", 4096)):
            self.assertEqual(cli.byte_size(value), expected)

    def test_unknown_archive_size_never_claims_a_percentage(self):
        progress = {"phase": "running", "discovery_complete": True, "totals_known": False,
                    "discovered_bytes": 1024, "bytes": 1000, "files": 1, "freed": 0, "failed": 0}
        for render in (jobs.display, jobs.terminal_display):
            self.assertNotIn("%", render(progress))
            self.assertIn("archive sizes", render(progress))
