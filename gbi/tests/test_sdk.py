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

    def test_restore_retention_and_explicit_deletion_rejected_before_cli(self):
        data.copy("saved.gbi.tar.gz", self.root / "restored")
        self.assertEqual(self.parsed().verb, "copy")
        self.assertFalse(self.parsed().delete_source)
        self.log.unlink()
        for prefect in (False, True):
            with self.subTest(prefect=prefect), self.assertRaisesRegex(ValueError, "durable and always retained"):
                data.move("saved.gbi.tar.gz", self.root / "restored", delete_source=True, prefect=prefect)
        self.assertFalse(self.log.exists())
        data.move("saved.gbi.tar.gz", self.root / "restored", delete_source=False)
        self.assertFalse(self.parsed().delete_source)

    def test_preview_and_prefect_still_wait_for_cli(self):
        data.copy("source", "target", pack_small=True, dry_run=True)
        self.assertTrue(self.parsed().pack_small)
        self.assertTrue(self.parsed().dry_run)
        data.move("source", "target", prefect=True, include=["*.pt", "*.pt.*"],
                  exclude="unfinished*")
        self.assertTrue(self.parsed().prefect)
        self.assertTrue(self.parsed().wait)
        self.assertEqual(self.parsed().include, ["*.pt", "*.pt.*"])
        self.assertEqual(self.parsed().exclude, ["unfinished*"])

    def test_prefect_portable_options_and_managed_preview_use_existing_flags(self):
        for transfer in (data.copy, data.move):
            transfer("source", "target.gbi.tar.gz", prefect=True, pack="gzip", dry_run=True)
            options = self.parsed()
            self.assertEqual(options.pack, "gzip")
            self.assertTrue(options.prefect and options.dry_run and options.wait)
            transfer("source", "target", prefect=True, pack_small=True)
            options = self.parsed()
            self.assertTrue(options.pack_small and options.prefect and options.wait)
            self.assertFalse(options.dry_run)
            transfer("source", "target.gbi.tar.gz", prefect=True, pack="gzip", chunk_size=67108864)
            options = self.parsed()
            self.assertEqual(options.chunk_size, 67108864)
            self.assertTrue(options.prefect and options.wait)
            self.assertEqual(options.destination, "target.gbi.tar.gz")

    def test_failure_raises_original_exit_code(self):
        with patch.dict(os.environ, {"GBI_SDK_TEST_FAIL": "1"}):
            with self.assertRaises(subprocess.CalledProcessError) as raised:
                data.move("source", "target")
        self.assertEqual(raised.exception.returncode, 23)
        self.assertEqual(raised.exception.cmd[2], "move")

    def test_prefect_restore_job_size_is_forwarded_only_when_explicit(self):
        data.copy("source", "target", prefect=True)
        self.assertIsNone(self.parsed().job_size)
        for transfer in (data.copy, data.move):
            for size in ("small", "large"):
                transfer("source", "target", prefect=True, job_size=size)
                self.assertEqual(self.parsed().job_size, size)
                self.assertTrue(self.parsed().wait)

    def test_invalid_job_size_never_launches_a_process(self):
        for size in ("", "medium", True, 2, []):
            with self.subTest(size=size), self.assertRaisesRegex(ValueError, "job_size"):
                data.copy("source", "target", prefect=True, job_size=size)
        with self.assertRaisesRegex(ValueError, "prefect=True"):
            data.copy("source", "target", job_size="small")
        self.assertFalse(self.log.exists())

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
