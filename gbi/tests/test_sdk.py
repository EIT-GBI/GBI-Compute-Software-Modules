"""The public SDK crosses a real process boundary without changing CLI policy."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from gbi import data
from gbi_data.cli import parser


class SDK(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.log = self.root / "child.json"
        executable = self.root / "gbi test executable"
        executable.write_text(
            f"#!{sys.executable}\n"
            "import json, os, sys\n"
            f"with open({str(self.log)!r}, 'w') as log:\n"
            "    json.dump({'argv': sys.argv[1:], 'job': os.getenv('SLURM_JOB_ID')}, log)\n"
            "sys.exit(23 if os.getenv('GBI_SDK_TEST_FAIL') else 0)\n"
        )
        executable.chmod(0o700)
        self.cli = patch.object(data, "_CLI", executable)
        self.cli.start()
        self.addCleanup(self.cli.stop)

    def parsed(self):
        return parser().parse_args(json.loads(self.log.read_text())["argv"])

    def test_archive_filters_paths_and_existing_allocation(self):
        sentinel = self.root / "shell-must-not-run"
        source = f"--source with spaces\n$(touch {sentinel})"
        target = self.root / "checkpoints.gbi.tar"
        with patch.dict(os.environ, {"SLURM_JOB_ID": "12345"}):
            result = data.move(source, target, include=["*.pt", "*.pt.*"],
                               exclude="--literal-pattern", pack="tar", chunk_size="64MiB")
        options = self.parsed()
        self.assertEqual(options.source, source)
        self.assertEqual(options.destination, str(target))
        self.assertEqual(options.include, ["*.pt", "*.pt.*"])
        self.assertEqual(options.exclude, ["--literal-pattern"])
        self.assertEqual(options.pack, "tar")
        self.assertEqual(options.chunk_size, 64 * 1024 * 1024)
        self.assertTrue(options.wait)
        self.assertFalse(options.detach)
        self.assertFalse(options.prefect)
        self.assertEqual(json.loads(self.log.read_text())["job"], "12345")
        self.assertEqual(result.returncode, 0)
        self.assertIsNone(result.stdout)
        self.assertFalse(sentinel.exists())

    def test_restore_retention_and_explicit_deletion_are_cli_options(self):
        data.copy("saved.gbi.tar.gz", self.root / "restored")
        self.assertEqual(self.parsed().verb, "copy")
        self.assertFalse(self.parsed().delete_source)
        data.move("saved.gbi.tar.gz", self.root / "restored", delete_source=True)
        self.assertTrue(self.parsed().delete_source)

    def test_preview_and_prefect_still_wait_for_cli(self):
        data.copy("source", "target", pack_small=True, dry_run=True)
        self.assertTrue(self.parsed().pack_small)
        self.assertTrue(self.parsed().dry_run)
        data.move("source", "target", prefect=True)
        self.assertTrue(self.parsed().prefect)
        self.assertTrue(self.parsed().wait)

    def test_failure_raises_original_exit_code(self):
        with patch.dict(os.environ, {"GBI_SDK_TEST_FAIL": "1"}):
            with self.assertRaises(subprocess.CalledProcessError) as raised:
                data.move("source", "target")
        self.assertEqual(raised.exception.returncode, 23)
        self.assertEqual(raised.exception.cmd[2], "move")

    def test_ambiguous_deletion_value_never_launches_a_process(self):
        with self.assertRaisesRegex(TypeError, "delete_source must be a boolean"):
            data.move("source", "target", delete_source="false")
        self.assertFalse(self.log.exists())

    def test_bad_filters_and_unknown_options_never_launch(self):
        with self.assertRaisesRegex(TypeError, "patterns must be strings"):
            data.copy("source", "target", include=[42])
        with self.assertRaises(TypeError):
            data.copy("source", "target", detach=True)
        self.assertFalse(self.log.exists())
