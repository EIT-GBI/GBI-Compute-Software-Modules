import contextlib
import os
import io
import sqlite3
import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from gbi_data import cli, usage


class Usage(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve() / "lustre" / "alice"
        self.root.mkdir(parents=True)
        (self.root / "project" / "nested").mkdir(parents=True)
        self.site = SimpleNamespace(
            user="alice", values={"usage_db": "", "lfs_bin": "lfs"},
            roots={"lustre": self.root},
            path=Path(self.temp.name) / "site.conf",
        )
        database = self.root / ".gbi" / "usage.sqlite3"
        database.parent.mkdir()
        connection = sqlite3.connect(database)
        connection.executescript("""
            CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE directories (
              path TEXT PRIMARY KEY, parent_path TEXT NOT NULL,
              apparent_bytes INTEGER NOT NULL, entries INTEGER NOT NULL
            );
            CREATE INDEX directories_parent ON directories(parent_path);
        """)
        connection.executemany("INSERT INTO metadata VALUES (?, ?)", [
            ("schema_version", "1"), ("owner", "alice"),
            ("owner_uid", str(os.getuid())), ("root", str(self.root)),
            ("status", "partial"), ("complete_input", "false"),
            ("snapshot_at", "2026-09-23T12:00:00Z"),
            ("published_at", "2026-09-23T12:01:00Z"),
        ])
        connection.executemany("INSERT INTO directories VALUES (?, ?, ?, ?)", [
            (".", "", 3000, 30), ("project", ".", 2000, 20),
            ("project/nested", "project", 1000, 10),
            ("deleted", ".", 500, 5),
        ])
        connection.commit()
        connection.close()

    def tearDown(self):
        self.temp.cleanup()

    def test_report_reads_owner_snapshot_read_only_and_labels_partial(self):
        with patch("gbi_data.usage._quota", return_value={
            "status": "available", "filesystem": "/mnt/lustre", "used": "3G",
            "quota": "0", "limit": "0", "grace": "-", "files": "30",
            "file_quota": "0", "file_limit": "0", "file_grace": "-",
        }):
            result = usage.report(self.site, None, 1, 20)
        self.assertEqual(result["summary"], (3000, 30))
        self.assertEqual(result["rows"], [("project", 2000, 20), ("deleted", 500, 5)])
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            usage.print_report(result, 1, 20)
        self.assertIn("partial", output.getvalue())
        self.assertIn("Warning", output.getvalue())

    def test_shared_path_is_rejected(self):
        shared = Path(self.temp.name) / "shared"
        shared.mkdir()
        with self.assertRaisesRegex(ValueError, "only for your own Lustre root"):
            usage.report(self.site, str(shared), 1, 20)
        with self.assertRaisesRegex(ValueError, "only for your own Lustre root"):
            usage.report(self.site, "../alice-sibling", 1, 20)

    def test_snapshot_can_report_a_path_removed_since_scan(self):
        result = usage.report(self.site, "deleted", 1, 20)
        self.assertEqual(result["summary"], (500, 5))

    def test_wrong_identity_or_root_is_rejected(self):
        database = self.root / ".gbi" / "usage.sqlite3"
        connection = sqlite3.connect(database)
        connection.execute("UPDATE metadata SET value = ? WHERE key = 'owner_uid'", ("999999",))
        connection.commit()
        connection.close()
        with self.assertRaisesRegex(ValueError, "another user"):
            usage.report(self.site, None, 1, 20)
        connection = sqlite3.connect(database)
        connection.execute("UPDATE metadata SET value = ? WHERE key = 'owner_uid'", (str(os.getuid()),))
        connection.execute("UPDATE metadata SET value = ? WHERE key = 'root'", (str(self.root.parent),))
        connection.commit()
        connection.close()
        with self.assertRaisesRegex(ValueError, "different Lustre root"):
            usage.report(self.site, None, 1, 20)

    def test_depth_two_aggregates_nested_rows(self):
        result = usage.report(self.site, "project", 2, 20)
        self.assertEqual(result["rows"], [("project/nested", 1000, 10)])

    @patch("gbi_data.usage.shutil.which", return_value="/usr/bin/lfs")
    @patch("gbi_data.usage.subprocess.run")
    def test_quota_reports_the_whole_filesystem_uid_row(self, run, _which):
        run.return_value = SimpleNamespace(
            returncode=0,
            stdout=("Disk quotas for usr 150038 (uid 150038):\n"
                    "Filesystem used quota limit grace files quota limit grace\n"
                    "/mnt/lustre\n"
                    "85.2G 0 0 - 2888 0 0 -\n"),
        )
        quota = usage._quota(self.site)
        self.assertEqual((quota["filesystem"], quota["used"], quota["files"]),
                         ("/mnt/lustre", "85.2G", "2888"))
        self.assertEqual(run.call_args.args[0][4], str(os.getuid()))

    def test_malformed_snapshot_time_is_rejected_and_future_is_not_fresh(self):
        database = self.root / ".gbi" / "usage.sqlite3"
        connection = sqlite3.connect(database)
        connection.execute("UPDATE metadata SET value = ? WHERE key = 'snapshot_at'", ("later",))
        connection.commit()
        connection.close()
        with self.assertRaisesRegex(ValueError, "timestamps"):
            usage.report(self.site, None, 1, 20)
        self.assertEqual(usage._age(usage._timestamp("2999-01-01T00:00:00Z")), "future timestamp")
        self.assertIsNone(usage._timestamp("2026-09-23T12:00:00"))

    def test_control_characters_are_escaped_for_terminal_output(self):
        self.assertEqual(usage._display_path("folder\tname\nnext"), "folder\\tname\\nnext")

    def test_live_quota_survives_missing_cache(self):
        self.root.joinpath(".gbi", "usage.sqlite3").unlink()
        with patch("gbi_data.usage._quota", return_value={
            "status": "available", "filesystem": "/mnt/lustre", "used": "1G",
            "quota": "0", "limit": "0", "grace": "-", "files": "2",
            "file_quota": "0", "file_limit": "0", "file_grace": "-",
        }):
            result = usage.report(self.site, None, 1, 20)
        self.assertIsNone(result["summary"])
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            usage.print_report(result, 1, 20)
        self.assertIn("live UID quota", output.getvalue())
        self.assertIn("No published Lustre folder inventory is available", output.getvalue())

    def test_report_formats_lustre_and_fss_as_aligned_storage_summary(self):
        database = self.root / ".gbi" / "usage.sqlite3"
        connection = sqlite3.connect(database)
        connection.executemany("INSERT INTO metadata VALUES (?, ?)", [
            ("fss_used_bytes", str(12 * 1024**3)),
            ("fss_limit_bytes", str(1024**5)),
            ("fss_files", "12345"),
            ("fss_observed_at", "2026-09-23T12:00:00Z"),
            ("fss_source", "OCI per-UID usage"),
        ])
        connection.commit()
        connection.close()
        quota = {
            "status": "available", "filesystem": "/mnt/lustre", "used": "85.2G",
            "quota": "0", "limit": "0", "grace": "-", "files": "2888",
            "file_quota": "0", "file_limit": "0", "file_grace": "-",
        }
        with patch("gbi_data.usage._quota", return_value=quota):
            result = usage.report(self.site, None, 1, 20)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            usage.print_report(result, 1, 20)
        rendered = output.getvalue()
        self.assertIn("Storage summary", rendered)
        self.assertRegex(rendered, r"Lustre\s+85\.2G")
        self.assertIn("FSS", rendered)
        self.assertIn("12.0 GiB", rendered)
        self.assertIn("1.0 PiB", rendered)
        self.assertIn("12,345", rendered)
        self.assertIn("Folder", rendered)
        self.assertNotIn("filesystem=/mnt/lustre", rendered)

    def test_missing_fss_measurement_is_explicit(self):
        quota = {"status": "unavailable", "detail": "lfs is not installed"}
        with patch("gbi_data.usage._quota", return_value=quota):
            result = usage.report(self.site, None, 1, 20)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            usage.print_report(result, 1, 20)
        self.assertIn("not published", output.getvalue())

    def test_long_folder_names_are_clipped_without_terminal_controls(self):
        value = "x" * 80 + "\nnext"
        clipped = usage._clip(value)
        self.assertEqual(len(clipped), 56)
        self.assertTrue(clipped.endswith("..."))
        self.assertNotIn("\n", clipped)

    def test_open_failure_is_not_reported_as_an_unpublished_scan(self):
        with patch("gbi_data.usage.sqlite3.connect", side_effect=sqlite3.OperationalError("denied")):
            with self.assertRaisesRegex(ValueError, "cannot be read"):
                usage.report(self.site, None, 1, 20)

    def test_unreadable_cache_is_actionable(self):
        database = self.root / ".gbi" / "usage.sqlite3"
        database.write_bytes(b"not sqlite")
        with self.assertRaisesRegex(ValueError, "cannot be read"):
            usage.report(self.site, None, 1, 20)

    def test_parser_preserves_path_and_limits(self):
        options = cli.parser().parse_args(["data", "usage", "project", "--depth", "2", "--limit", "7"])
        self.assertEqual((options.verb, options.path, options.depth, options.limit), ("usage", "project", 2, 7))


if __name__ == "__main__":
    unittest.main()
