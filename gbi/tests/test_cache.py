"""gbi data cache reads every file once through an Object Storage mount."""

import io
import json
import os
from pathlib import Path
import pwd
import tempfile
import time
import unittest
from unittest.mock import patch
from contextlib import redirect_stdout, redirect_stderr

from gbi_data import cache, cli
from gbi_data.storage import Site


class Cache(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name).resolve()
        user      = pwd.getpwuid(os.getuid()).pw_name
        for name in ("lustre", "fss", "bucket"):
            (self.base / name / user).mkdir(parents = True)
        config = self.base / "site.conf"
        config.write_text(
            "".join(
                f"{name}_root = {self.base / name}\n"
                for name in ("lustre", "fss", "bucket")
            )
            + "jobs = 3\n"
        )
        self.site    = Site(config)
        self.alluxio = self.site.roots["alluxio"]
        self.lustre  = self.site.roots["lustre"]

    def tree(self):
        data = self.alluxio / "dataset"
        (data / "a" / "deep").mkdir(parents = True)
        (data / ".gbi").mkdir()
        files = {
            data / "a" / "one.bin": b"1" * 3000,
            data / "a" / "deep" / "two.bin": b"2" * 70000,
            data / "three.bin": b"3" * 11,
            data / "empty.bin": b"",
        }
        for path, content in files.items():
            path.write_bytes(content)
        (data / ".gbi" / "state.json").write_bytes(b"{}")
        (data / "link").symlink_to(data / "three.bin")
        os.mkfifo(data / "fifo")
        return data, files

    def options(self, *arguments):
        return cli.parser().parse_args(["data", "cache", *arguments])

    def test_reads_every_regular_file_once_and_skips_links_reserved_and_special(self):
        data, files = self.tree()
        seen = []
        original = cache.read_file

        def recording(path):
            seen.append(Path(path))
            return original(path)

        with patch.object(cache, "read_file", recording):
            summary = cache.warm(self.site, [str(data)], jobs = 3)
        self.assertEqual(sorted(seen), sorted(files))
        self.assertEqual(len(seen), len(set(seen)))
        self.assertEqual(summary["files"], 4)
        self.assertEqual(summary["read_files"], 4)
        self.assertEqual(summary["read_bytes"], sum(map(len, files.values())))
        self.assertEqual(summary["bytes"], summary["read_bytes"])
        self.assertEqual(summary["failed"], [])
        self.assertFalse(summary["timed_out"])
        self.assertEqual(summary["skipped"]["symlinks"], 1)
        self.assertEqual(summary["skipped"]["special"], 1)
        self.assertEqual(summary["skipped"]["reserved"], 1)
        self.assertEqual(summary["paths"], [str(data)])

    def test_one_file_and_several_paths(self):
        data, files = self.tree()
        single = cache.warm(self.site, [str(data / "three.bin")], jobs = 1)
        self.assertEqual((single["files"], single["read_bytes"]), (1, 11))
        both = cache.warm(
            self.site, [str(data / "a"), str(data / "three.bin")], jobs = 2
        )
        self.assertEqual(both["files"], 3)
        self.assertEqual(both["read_bytes"], 3000 + 70000 + 11)

    def test_refuses_paths_outside_object_storage_missing_symlinked_or_reserved(self):
        data, _files = self.tree()
        (self.lustre / "scratch").mkdir()
        for raw, message in (
            (self.lustre / "scratch", "not inside an Object Storage mount"),
            (data / "absent", "does not exist"),
            (data / "link", "symbolic link"),
            (data / ".gbi", "reserved"),
            (self.base / "elsewhere", "not inside an Object Storage mount"),
        ):
            with self.subTest(raw = raw):
                with self.assertRaisesRegex(cache.CacheError, message):
                    cache.warm(self.site, [str(raw)], jobs = 1)
        # One bad path refuses the whole request before anything is read.
        with patch.object(cache, "read_file", side_effect = AssertionError("read")):
            with self.assertRaises(cache.CacheError):
                cache.warm(self.site, [str(data), str(data / "absent")], jobs = 1)

    def test_dry_run_reads_nothing_and_bounds_are_checked(self):
        data, files = self.tree()
        with patch.object(cache, "read_file", side_effect = AssertionError("read")):
            summary = cache.warm(self.site, [str(data)], jobs = 2, dry_run = True)
        self.assertTrue(summary["dry_run"])
        self.assertEqual(summary["files"], 4)
        self.assertEqual(summary["bytes"], sum(map(len, files.values())))
        self.assertEqual(summary["read_files"], 0)
        for jobs in (0, cache.MAX_JOBS + 1, "2", 1.5):
            with self.assertRaisesRegex(cache.CacheError, "--jobs"):
                cache.warm(self.site, [str(data)], jobs = jobs, dry_run = True)
        with self.assertRaisesRegex(cache.CacheError, "--timeout"):
            cache.warm(self.site, [str(data)], jobs = 1, timeout = -1, dry_run = True)

    def test_timeout_stops_starting_files_and_reports_partial(self):
        data, _files = self.tree()
        original = cache.read_file

        def slow(path):
            time.sleep(0.15)
            return original(path)

        with patch.object(cache, "read_file", slow):
            summary = cache.warm(self.site, [str(data)], jobs = 1, timeout = 1)
            self.assertFalse(summary["timed_out"])
            self.assertEqual(summary["read_files"], 4)
            ticks = iter([0.0, 0.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0])
            summary = cache.warm(
                self.site, [str(data)], jobs = 1, timeout = 5,
                clock = lambda: next(ticks),
            )
        self.assertTrue(summary["timed_out"])
        self.assertLess(summary["read_files"], 4)
        self.assertGreaterEqual(summary["read_files"], 1)

    def test_read_failures_are_reported_and_do_not_stop_the_rest(self):
        data, files = self.tree()
        original = cache.read_file

        def failing(path):
            if Path(path).name == "one.bin":
                raise OSError(5, "simulated input/output error")
            return original(path)

        with patch.object(cache, "read_file", failing):
            summary = cache.warm(self.site, [str(data)], jobs = 2)
        self.assertEqual(summary["read_files"], 3)
        self.assertEqual(len(summary["failed"]), 1)
        self.assertTrue(summary["failed"][0]["path"].endswith("one.bin"))
        self.assertIn("input/output", summary["failed"][0]["error"])

    def test_cli_wiring_progress_json_and_exit_codes(self):
        data, files = self.tree()
        options = self.options(str(data), "--jobs", "2", "--json")
        self.assertEqual(options.verb, "cache")
        out = io.StringIO()
        with redirect_stdout(out):
            code = cache.main(options, self.site)
        self.assertEqual(code, cache.EXIT_OK)
        summary = json.loads(out.getvalue().strip().splitlines()[-1])
        self.assertEqual(summary["read_files"], 4)
        self.assertEqual(summary["jobs"], 2)
        # Default jobs come from the site; human output names the totals.
        out = io.StringIO()
        with redirect_stdout(out):
            code = cache.main(self.options(str(data)), self.site)
        self.assertEqual(code, cache.EXIT_OK)
        self.assertIn("Warmed 4/4 files", out.getvalue())
        with redirect_stdout(io.StringIO()):
            self.assertEqual(
                cache.main(self.options(str(data), "--dry-run"), self.site),
                cache.EXIT_OK,
            )
        err = io.StringIO()
        with redirect_stdout(io.StringIO()), redirect_stderr(err):
            code = cache.main(self.options(str(self.lustre)), self.site)
        self.assertEqual(code, cache.EXIT_REFUSED)
        self.assertIn("not inside an Object Storage mount", err.getvalue())
        with redirect_stdout(io.StringIO()), redirect_stderr(err):
            code = cache.main(self.options(str(data), "--jobs", "0"), self.site)
        self.assertEqual(code, cache.EXIT_REFUSED)
        with patch.object(cache, "read_file", side_effect = OSError(5, "boom")):
            with redirect_stdout(io.StringIO()):
                code = cache.main(self.options(str(data)), self.site)
        self.assertEqual(code, cache.EXIT_FAILED)
        ticks = iter([0.0] * 2 + [10.0] * 20)
        with patch.object(cache.time, "monotonic", lambda: next(ticks)):
            with redirect_stdout(io.StringIO()):
                code = cache.main(
                    self.options(str(data), "--jobs", "1", "--timeout", "5"),
                    self.site,
                )
        self.assertEqual(code, cache.EXIT_PARTIAL)

    def test_help_lists_the_verb(self):
        out = io.StringIO()
        with redirect_stdout(out), self.assertRaises(SystemExit):
            cli.parser().parse_args(["data", "cache", "--help"])
        text = out.getvalue().lower().replace("\n", " ")
        self.assertIn("object storage mount", text)
        self.assertIn("--timeout", text)
        self.assertIn("exit codes: 0 warmed", text)


if __name__ == "__main__":
    unittest.main()
