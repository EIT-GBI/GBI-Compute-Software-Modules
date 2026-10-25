"""Installer tests for optional, validated site configuration."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT         = Path(__file__).resolve().parents[1]
INSTALLER    = ROOT / "sm-config" / "install.sh"
SOURCE       = ROOT / "src"
VERSION_FILE = ROOT / "VERSION"


class Install(unittest.TestCase):
    def run_installer(self, directory, **environment):
        env = {
            **{
                key: value
                for key, value in os.environ.items()
                if not key.startswith("GBI_SITE_")
            }, "GBI_SOURCE_DIR": str(SOURCE),
            "GBI_VERSION_FILE": str(VERSION_FILE),
            "VERSION": VERSION_FILE.read_text().strip(),
            **environment,
        }
        return subprocess.run(
            ["bash", str(INSTALLER)], cwd = directory, env = env, text = True,
            capture_output = True, check = False,
        )

    def read_config(self, directory):
        result = {}
        for line in (
            (Path(directory) / "downloaded/etc/site.conf")
            .read_text()
            .splitlines()
        ):
            key, value = line.split("=", 1)
            result[key.strip()] = value.strip()
        return result

    def test_optional_config_is_retained_and_environment_overrides_it(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            config    = directory / "site.conf"
            config.write_text(
                "partition = batch\n"
                "prefect_url = https://broker.example/api\n"
                "lfs_bin = /site/tools/lfs\n"
            )
            result = self.run_installer(
                directory, GBI_SITE_CONFIG_FILE = str(config),
                GBI_SITE_PARTITION = "interactive",
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            installed = self.read_config(directory)
            self.assertEqual(installed["partition"], "interactive")
            self.assertEqual(
                installed["prefect_url"], "https://broker.example/api"
            )
            self.assertEqual(installed["lfs_bin"], "/site/tools/lfs")

    def test_unset_optional_config_keeps_generic_defaults_and_env_behavior(
                self,
            ):
        with tempfile.TemporaryDirectory() as temporary:
            result = self.run_installer(
                Path(temporary), GBI_SITE_PARTITION = "cpu"
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            installed = self.read_config(temporary)
            self.assertEqual(installed["partition"], "cpu")
            self.assertEqual(installed["prefect_url"], "")

    def test_configured_missing_or_invalid_file_fails(self):
        cases = ("missing", "unknown = value\n")
        for case in cases:
            with (
                self.subTest(case = case),
                tempfile.TemporaryDirectory() as temporary,
            ):
                directory = Path(temporary)
                config    = directory / "site.conf"
                if case != "missing":
                    config.write_text(case)
                result = self.run_installer(
                    directory, GBI_SITE_CONFIG_FILE = str(config)
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(
                    (directory / "downloaded/etc/site.conf").exists()
                )


if __name__ == "__main__":
    unittest.main()
