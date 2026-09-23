"""Real temporary Git fixtures exercise privacy findings and safe examples."""

import contextlib
import io
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import public_hygiene


class PublicHygiene(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        subprocess.run(["git", "init", "-q", str(self.root)], check=True)
        self.add("gbi/src/lib/example.py", "print('public example')\n")

    def add(self, name, value):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value)
        subprocess.run(["git", "add", "--", name], cwd=self.root, check=True)

    def test_safe_documentation_placeholders_and_local_fixtures(self):
        self.add("gbi/README.md", "\n".join((
            "/mnt/user-data/$USER/run", "/mnt/lustre/users/<username>/run",
            "/your/lustre/run", "https://github.com/example/project", "https://rclone.org",
            "127.0.0.1", "192.0.2.4", "198.51.100.8", "203.0.113.9", "rclone 1.75.1")))
        self.add("gbi/tests/example.py", "root = temporary_directory / 'alice'\n")
        count, findings = public_hygiene.scan_repository(self.root)
        self.assertEqual(count, 3)
        self.assertEqual(findings, [])

    def test_python_help_recipes_and_docs_are_scanned_without_disclosing_values(self):
        samples = {
            "gbi/src/lib/private.py": ("endpoint = '" + ".".join(("10", "23", "45", "67")) + "'", "private IPv4 address"),
            "gbi/sm-help": ("/mnt/user-data/" + "actual-person/run", "literal personal path"),
            "rclone/sm-config/settings.toml": ("id = '" + "ocid1." + "bucket.oc1.region.secret" + "'", "cloud resource identifier"),
            "gbi/src/lib/url.py": ("/n/" + "actualnamespace/b/actualbucket", "literal Object Storage namespace/bucket"),
            "gbi/README.md": ("-----BEGIN " + "RSA PRIVATE KEY-----", "private key"),
        }
        for name, (value, _) in samples.items():
            self.add(name, value)
        _, findings = public_hygiene.scan_repository(self.root)
        self.assertEqual({(name, rule) for name, _, rule in findings},
                         {(name, rule) for name, (_, rule) in samples.items()})
        with patch.object(public_hygiene, "scan_repository", return_value=(6, findings)), contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(public_hygiene.main(), 1)
        for value, _ in samples.values():
            self.assertNotIn(value, output.getvalue())

    def test_generated_configuration_is_rejected_but_template_is_allowed(self):
        self.add("gbi/site.conf", "bucket_root=/configured/location\n")
        self.add("gbi/site.conf.example", "bucket_root=<site-root>\n")
        _, findings = public_hygiene.scan_repository(self.root)
        self.assertEqual(findings, [("gbi/site.conf", 0, "generated site configuration must not be committed")])

    def test_empty_checkout_cannot_pass(self):
        subprocess.run(["git", "rm", "-q", "--cached", "gbi/src/lib/example.py"], cwd=self.root, check=True)
        with self.assertRaisesRegex(ValueError, "no tracked gbi/src"):
            public_hygiene.scan_repository(self.root)

    def test_unreadable_text_and_symlink_do_not_silently_pass(self):
        self.add("gbi/binary", "placeholder")
        (self.root / "gbi/binary").write_bytes(b"\xff\x00")
        (self.root / "gbi/link").symlink_to("../outside-private-file")
        subprocess.run(["git", "add", "gbi/link"], cwd=self.root, check=True)
        _, findings = public_hygiene.scan_repository(self.root)
        self.assertEqual(len(findings), 2)


if __name__ == "__main__":
    unittest.main()
