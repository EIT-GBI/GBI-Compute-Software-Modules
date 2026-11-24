"""Ordinary transfer output distinguishes verified reuse from copying."""

import json
import os
from pathlib import Path
import pwd
import subprocess
import sys
import tempfile
import unittest

from gbi_data.storage import Site


class Reporting(unittest.TestCase):
    def test_mixed_copy_and_move_report_reuse_without_changing_results(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary).resolve()
            user = pwd.getpwuid(os.getuid()).pw_name
            config = base / "site.conf"
            for name in ("lustre", "fss", "bucket"):
                (base / name / user).mkdir(parents = True)
            config.write_text("".join(
                f"{name}_root = {base / name}\n"
                for name in ("lustre", "fss", "bucket")
            ))
            site = Site(config)
            environment = {
                **os.environ, "GBI_DATA_SITE_CONF": str(config), "PATH": "",
            }
            environment.pop("SLURM_JOB_ID", None)
            for verb in ("copy", "move"):
                with self.subTest(verb = verb):
                    source = site.roots["lustre"] / verb
                    target = site.roots["fss"] / verb
                    source.mkdir()
                    target.mkdir()
                    contents = {"fresh": b"fresh bytes", "reused": b"same bytes", "conflict": b"source bytes"}
                    for name, content in contents.items():
                        (source / name).write_bytes(content)
                    (source / "empty").mkdir()
                    (target / "empty").mkdir()
                    (target / "reused").write_bytes(contents["reused"])
                    (target / "conflict").write_bytes(b"different bytes")
                    result = subprocess.run(
                        [sys.executable, "-B", "-m", "gbi_data.cli", "data", verb,
                         str(source), str(target)],
                        env = environment, capture_output = True, text = True,
                        timeout = 20,
                    )
                    self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                    fresh_action = "COPIED" if verb == "copy" else "MOVED"
                    reuse_action = "REUSED" if verb == "copy" else "MOVED"
                    self.assertIn(f"VERIFIED {fresh_action} {source / 'fresh'}", result.stdout)
                    self.assertIn(f"VERIFIED {reuse_action} {source / 'reused'}", result.stdout)
                    self.assertIn(f"FAILED {source / 'conflict'}", result.stdout)
                    self.assertIn("2 verified (1 reused)", result.stdout)
                    if verb == "move":
                        self.assertIn("(reused destination)", result.stdout)
                    else:
                        self.assertNotIn("(reused destination)", result.stdout)
                    records = [
                        json.loads(path.read_text())
                        for path in (site.roots["alluxio"] / ".gbi/transfers").glob("*/*/history.json")
                    ]
                    history, = [record for record in records if record["request"]["source"] == str(source)]
                    progress = history["progress"]
                    self.assertEqual((progress["files"], progress["reused"], progress["failed"]), (2, 1, 1))
                    verified_bytes = len(contents["fresh"]) + len(contents["reused"])
                    self.assertEqual(progress["bytes"], verified_bytes)
                    self.assertEqual(progress["freed"], verified_bytes if verb == "move" else 0)
                    self.assertEqual((source / "conflict").read_bytes(), contents["conflict"])
                    self.assertEqual((target / "conflict").read_bytes(), b"different bytes")
                    for name in ("fresh", "reused"):
                        self.assertEqual((target / name).read_bytes(), contents[name])
                        self.assertEqual((source / name).exists(), verb == "copy")
