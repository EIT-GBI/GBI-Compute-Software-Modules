"""Prefect submission preserves user paths and retries one saved request."""

import contextlib
import io
import json
import os
from pathlib import Path
import pwd
import tempfile
import unittest
from unittest.mock import patch

from gbi_data import cli, prefect
from gbi_data.storage import Site


class Prefect(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        user = pwd.getpwuid(os.getuid()).pw_name
        for name in ("lustre", "fss", "bucket"):
            (self.base / name / user).mkdir(parents=True)
        config = self.base / "site.conf"
        config.write_text("".join(f"{name}_root = {self.base / name}\n" for name in ("lustre", "fss", "bucket")))
        self.site = Site(config)
        self.home = self.site.roots["lustre"] / ".gbi"

    def options(self, source="lustre", target="alluxio", *flags):
        return cli.parser().parse_args(["data", "copy", str(self.site.roots[source] / "experiment"),
                                        str(self.site.roots[target] / "experiment"), "--prefect", *flags])

    def test_four_personal_routes_and_retention(self):
        for source, target, operation in (("lustre", "alluxio", "archive-lustre"),
                                           ("fss", "alluxio", "archive-fss"),
                                           ("alluxio", "lustre", "stage-lustre"),
                                           ("alluxio", "fss", "stage-fss")):
            with self.subTest(operation=operation):
                request = prefect.prepare(self.options(source, target), self.site)
                self.assertEqual(request["operation"], operation)
                self.assertEqual(request["source"], "experiment")
                self.assertEqual(request["destination"], "experiment")
                self.assertEqual(request["verb"], "copy")

    def test_explicit_runner_excludes_other_execution_flags(self):
        for flag in ("--detach", "--local"):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                self.options("lustre", "alluxio", flag)
        self.assertFalse(cli.parser().parse_args(["data", "copy", "a", "b"]).prefect)

    def test_unsupported_filters_deletion_and_paths_refused(self):
        for flags in (("--exclude", "*.tmp"), ("--delete-source",)):
            with self.assertRaises(ValueError):
                prefect.prepare(self.options("lustre", "alluxio", *flags), self.site)
        for path in (self.base / "shared", self.site.roots["lustre"] / ".." / "other",
                     self.site.roots["lustre"] / ".gbi" / "state"):
            options = self.options()
            options.source = str(path)
            with self.assertRaises(ValueError):
                prefect.prepare(options, self.site)

    def test_fixed_layout_and_include_preserved(self):
        options = self.options("fss", "alluxio", "--include", "*.pt", "--include", "*.txt")
        self.assertEqual(prefect.prepare(options, self.site)["include"], ["*.pt", "*.txt"])
        options.destination = str(self.site.roots["alluxio"] / "renamed")
        with self.assertRaisesRegex(ValueError, "matching relative"):
            prefect.prepare(options, self.site)

    def test_dry_run_creates_no_state_or_submission(self):
        with patch("gbi_data.prefect.exchange") as exchange:
            self.assertEqual(prefect.start(self.options("lustre", "alluxio", "--dry-run"), self.site, self.home), 0)
        exchange.assert_not_called()
        self.assertFalse(self.home.exists())

    def test_socket_exchange_uses_saved_request_and_validates_reply(self):
        request = prefect.prepare(self.options(), self.site)
        run_id = "74ec9296-0446-4a46-a428-f004e556f066"
        response = {"version": 1, "run_id": run_id, "state": "COMPLETED", "terminal": False}
        with patch("gbi_data.prefect.socket.socket") as factory:
            client = factory.return_value.__enter__.return_value
            client.makefile.return_value = io.BytesIO(json.dumps(response).encode() + b"\n")
            result = prefect.exchange(self.site, request)
            client.connect.assert_called_once_with(self.site.values["prefect_socket"])
            self.assertEqual(json.loads(client.sendall.call_args.args[0]), request)
            self.assertTrue(result["terminal"])
        for raw in (b"broken\n", b"{}", b'{"version":1,"state":"COMPLETED","run_id":null}\n'):
            with self.subTest(raw=raw), patch("gbi_data.prefect.socket.socket") as factory:
                factory.return_value.__enter__.return_value.makefile.return_value = io.BytesIO(raw)
                with self.assertRaisesRegex(ValueError, "same transfer ID"):
                    prefect.exchange(self.site, request)

    def test_lost_reply_retry_resends_identical_saved_request(self):
        sent = []

        def lost_reply(_site, request):
            sent.append(request)
            self.assertEqual(len(list((self.home / "runs").glob("*/request.json"))), 1)
            raise ValueError("lost reply")

        with patch("gbi_data.prefect.exchange", side_effect=lost_reply), \
             patch("sys.stdout.isatty", return_value=False):
            with self.assertRaisesRegex(ValueError, "lost reply"):
                prefect.start(self.options(), self.site, self.home)
        run_dir = next((self.home / "runs").iterdir())
        persisted = (run_dir / "request.json").read_bytes()
        response = {"version": 1, "state": "RUNNING", "terminal": False}
        with patch("gbi_data.prefect.exchange", return_value=response) as exchange:
            self.assertEqual(prefect.submit(run_dir, self.site), 0)
        self.assertEqual(exchange.call_args.args[1], sent[0])
        self.assertEqual((run_dir / "request.json").read_bytes(), persisted)

    def test_status_recovers_by_request_id_without_resubmission(self):
        with patch("gbi_data.prefect.exchange", side_effect=ValueError("lost")), \
             patch("sys.stdout.isatty", return_value=False):
            with self.assertRaises(ValueError):
                prefect.start(self.options(), self.site, self.home)
        run_dir = next((self.home / "runs").iterdir())
        request = prefect.stored_request(run_dir)
        with patch("gbi_data.prefect.exchange", return_value={"version": 1, "state": "COMPLETED", "terminal": True}) as exchange:
            self.assertEqual(prefect.follow(run_dir, self.site, True), 0)
        self.assertEqual(exchange.call_args.args[1], {"version": 1, "action": "status", "request_id": request["request_id"]})
        self.assertTrue((run_dir / "request.json").exists())

    def test_ordinary_request_cannot_be_retried_through_broker(self):
        run_dir = self.base / "ordinary"
        run_dir.mkdir()
        (run_dir / "request.json").write_text(json.dumps({"execution": "slurm", "uid": os.getuid()}))
        with patch("gbi_data.prefect.exchange") as exchange, self.assertRaisesRegex(ValueError, "re-running"):
            prefect.submit(run_dir, self.site)
        exchange.assert_not_called()

    def test_terminal_failure_and_missing_submission_are_not_success(self):
        run_dir = self.base / "record"
        run_dir.mkdir()
        (run_dir / "request.json").write_text(json.dumps({"execution": "prefect", "uid": os.getuid(),
                                                         "broker_request": prefect.prepare(self.options(), self.site)}))
        for state, terminal in (("FAILED", True), ("NOT_SUBMITTED", False)):
            with self.subTest(state=state), patch("gbi_data.prefect.exchange", return_value={
                    "version": 1, "state": state, "terminal": terminal}):
                self.assertEqual(prefect.follow(run_dir, self.site, True), 1)
