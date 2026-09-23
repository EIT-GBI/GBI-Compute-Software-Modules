"""The same filters must govern planning, background discovery and safe moves."""

import json
import os
from pathlib import Path
import pwd
import subprocess
import sys
import tempfile
import time
import unittest

from gbi_data import cli, selection
from gbi_data.storage import Site


class Selection(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name).resolve()
        user = pwd.getpwuid(os.getuid()).pw_name
        self.conf = self.base / "site.conf"
        roots = {name: self.base / name for name in ("lustre", "fss", "bucket")}
        for root in roots.values():
            (root / user).mkdir(parents=True)
        self.conf.write_text("".join(f"{name}_root={root}\n" for name, root in roots.items()))
        self.site = Site(self.conf)
        self.source = self.site.roots["lustre"] / "tree"
        self.source.mkdir()
        self.target = self.site.roots["fss"] / "tree"
        for name in ("model.pt", "model.pt.ready.json", "trace.log", "UPPER.LOG", "nested/trace.log",
                     "nested/model.pt", "named.log/keep.pt", ".gbi/hidden.pt"):
            path = self.source / name
            path.parent.mkdir(exist_ok=True)
            path.write_text(name)
        (self.source / "skip.log").symlink_to("model.pt")
        (self.source / "link.pt").symlink_to("nested")

    def specification(self, *flags):
        options = cli.parser().parse_args(["data", "move", str(self.source), str(self.target), *flags])
        return cli.plan(options, self.site)

    def names(self, specification, streamed=False):
        if not streamed:
            return {str(Path(row["path"]).relative_to(self.source))
                    for row in selection.foreground_selection(specification, self.site)}
        stream = selection.stream_entries(self.source, self.site, self.source, specification["include"],
                                          exclusions=specification["exclude"])
        self.addCleanup(stream.stop)
        names = set()
        while True:
            event = stream.get(2)
            self.assertIsNotNone(event, "discovery did not return a terminal event")
            kind, item, error = event
            if kind == "done":
                return names
            self.assertEqual(kind, "entry", error)
            names.add(str(item[0].relative_to(self.source)))

    def test_excludes_win_regardless_of_order_in_both_discovery_paths(self):
        for flags in (("--include", "*.pt*", "--exclude", "*.ready.json", "--exclude", "*.ready.json"),
                      ("--exclude", "*.ready.json", "--include", "*.pt*")):
            specification = self.specification(*flags)
            for streamed in (False, True):
                self.assertEqual(self.names(specification, streamed),
                                 {"model.pt", "nested/model.pt", "named.log/keep.pt", "link.pt"})

    def test_exclude_only_matches_case_sensitive_basenames_not_directory_names(self):
        expected = {"model.pt", "model.pt.ready.json", "UPPER.LOG", "nested/model.pt",
                    "named.log/keep.pt", "link.pt"}
        for streamed in (False, True):
            self.assertEqual(self.names(self.specification("--exclude", "*.log"), streamed), expected)

    def test_no_matches_and_empty_directories(self):
        for streamed in (False, True):
            self.assertEqual(self.names(self.specification("--include", "*.absent"), streamed), set())
        (self.source / "empty").mkdir()
        self.assertEqual(self.names(self.specification("--exclude", "*")), {"empty"})

    def test_stream_progress_counts_nonmatching_entries(self):
        for number in range(250):
            (self.source / f"scan-{number}.log").write_text("ignored")
        stream = selection.stream_entries(self.source, self.site, self.source, ["*.absent"])
        self.addCleanup(stream.stop)
        self.assertEqual(stream.get(2)[0], "done")
        visited, _ = stream.progress()
        self.assertGreaterEqual(visited, 250)

    def test_stream_progress_stops_advancing_when_iterator_stalls(self):
        def stalled(_on_error, visit):
            visit()
            time.sleep(0.2)
            yield self.source / "never-reached", False

        stream = selection.stream_entries(self.source, self.site, self.source, [], iterator_factory=stalled)
        self.addCleanup(stream.stop)
        self.assertIsNone(stream.get(0.05))
        visited, last_progress = stream.progress()
        self.assertEqual(visited, 1)
        self.assertLess(last_progress, time.monotonic())

    def test_explicit_file_and_symlink_use_the_shared_rule(self):
        for name in ("model.pt", "link.pt", "skip.log"):
            source = self.source / name
            self.assertEqual(list(selection.entries(source, self.site, self.source, [], exclusions=[name])), [])
            self.assertEqual(list(selection.entries(source, self.site, self.source, ["*.absent"])), [])
            self.assertEqual(list(selection.entries(source, self.site, self.source, [name])), [(source, False)])

    def test_excluded_bytes_do_not_force_background_execution(self):
        self.site.values["inline_bytes"] = "1"
        large = self.source / "large.tmp"
        large.write_bytes(b"large")
        small = self.source / "small.dat"
        small.write_bytes(b"x")
        specification = self.specification("--include", "large.tmp", "--include", "small.dat",
                                           "--exclude", "*.tmp")
        self.assertEqual(self.names(specification), {"small.dat"})

    def test_move_keeps_excluded_files_links_and_reserved_state(self):
        environment = {**os.environ, "GBI_DATA_SITE_CONF": str(self.conf)}
        environment.pop("SLURM_JOB_ID", None)
        result = subprocess.run(
            [sys.executable, "-B", "-m", "gbi_data.cli", "data", "move", str(self.source), str(self.target),
             "--include", "*.pt", "--include", "*.pt.*", "--exclude", "*.ready.json"],
            env=environment, capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual((self.target / "nested/model.pt").read_text(), "nested/model.pt")
        self.assertEqual(os.readlink(self.target / "link.pt"), "nested")
        self.assertFalse((self.source / "model.pt").exists())
        self.assertFalse((self.target / "model.pt.ready.json").exists())
        for name in ("model.pt.ready.json", "trace.log", "nested/trace.log", ".gbi/hidden.pt"):
            self.assertTrue((self.source / name).is_file(), name)
        self.assertTrue((self.source / "skip.log").is_symlink())
        history, = (self.site.roots["alluxio"] / ".gbi/transfers").glob("*/*/history.json")
        self.assertEqual(json.loads(history.read_text())["request"]["exclude"], ["*.ready.json"])


if __name__ == "__main__":
    unittest.main()
