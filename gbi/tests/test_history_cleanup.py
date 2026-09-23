"""Verified history survives scratch cleanup failure and read-only installs."""

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


class HistoryCleanup(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name).resolve()
        user = pwd.getpwuid(os.getuid()).pw_name
        self.roots = {kind: self.base / kind / user for kind in ("lustre", "fss", "bucket")}
        for root in self.roots.values():
            root.mkdir(parents=True)
        self.config = self.base / "site.conf"
        self.config.write_text("".join(f"{kind}_root = {root.parent}\n" for kind, root in self.roots.items()))
        self.source = self.roots["lustre"] / "source"
        self.source.write_bytes(b"verified source remains")
        self.target = self.roots["fss"] / "target"
        self.arguments = ["gbi", "data", "copy", str(self.source), str(self.target)]

    def execute(self):
        with patch.dict(os.environ, {"GBI_DATA_SITE_CONF": str(self.config)}), \
             patch.object(sys, "argv", self.arguments):
            return cli.main()

    def test_cleanup_failure_keeps_successful_history_and_terminal_status(self):
        output = io.StringIO()
        with patch("gbi_data.cli.shutil.rmtree", side_effect=PermissionError("read-only runtime")), \
             contextlib.redirect_stdout(output):
            self.assertEqual(self.execute(), 0)
        self.assertEqual(self.target.read_bytes(), self.source.read_bytes())
        run, = (self.roots["lustre"] / ".gbi/runs").iterdir()
        progress = jobs.read_progress(run)
        self.assertEqual(progress["phase"], "complete")
        archived = json.loads((Path(progress["history"]) / "history.json").read_text())
        self.assertEqual(archived["progress"]["bytes"], self.source.stat().st_size)
        self.assertIn("temporary records could not be removed", output.getvalue())
        self.assertNotIn("Could not save history", output.getvalue())

    def test_read_only_install_produces_removable_private_runtime(self):
        installed = self.base / "installed"
        nested = installed / "nested"
        nested.mkdir(parents=True)
        (installed / "cli.py").write_text("# immutable installed fixture\n")
        (nested / "helper.py").write_text("# nested source\n")
        installed.chmod(0o555)
        nested.chmod(0o555)
        try:
            with patch("gbi_data.cli.__file__", str(installed / "cli.py")), \
                 patch("gbi_data.cli.run", return_value=0), \
                 contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(self.execute(), 0)
            run, = (self.roots["lustre"] / ".gbi/runs").iterdir()
            runtime = run / "runtime/gbi_data"
            self.assertEqual(runtime.stat().st_mode & 0o777, 0o700)
            self.assertEqual((runtime / "nested").stat().st_mode & 0o777, 0o700)
            self.assertEqual(installed.stat().st_mode & 0o777, 0o555)
            shutil.rmtree(run)
        finally:
            installed.chmod(0o700)
            nested.chmod(0o700)
