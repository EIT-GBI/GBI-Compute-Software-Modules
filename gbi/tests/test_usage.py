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
        with patch("gbi_data.usage._quota", return_value="/mnt/lustre 3G 0 0 - 30 0 0 -"):
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
        self.assertIn("filesystem=/mnt/lustre used=85.2G", usage._quota(self.site))
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
        with patch("gbi_data.usage._quota", return_value="whole-filesystem UID row: /mnt/lustre 1G"):
            result = usage.report(self.site, None, 1, 20)
        self.assertIsNone(result["summary"])
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            usage.print_report(result, 1, 20)
        self.assertIn("whole-filesystem UID row", output.getvalue())
        self.assertIn("No cached folder report is available", output.getvalue())

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
