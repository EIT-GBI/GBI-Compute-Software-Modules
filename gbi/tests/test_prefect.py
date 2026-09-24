"""Prefect submission preserves user paths and retries one saved request."""

import contextlib
import errno
import io
import json
import os
from pathlib import Path
import pwd
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from urllib import error as urlerror

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
        prefect._TOKEN = None
        prefect._TOKEN_EXPIRES = 0

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

    def test_https_fallback_only_handles_unavailable_socket(self):
        request = prefect.prepare(self.options(), self.site)
        response = {"version": 1, "state": "SCHEDULED", "run_id": "74ec9296-0446-4a46-a428-f004e556f066"}
        with patch("gbi_data.prefect._exchange_socket", side_effect=prefect._BrokerUnavailable("missing")), \
             patch("gbi_data.prefect._exchange_https") as https:
            with self.assertRaisesRegex(ValueError, "login node.*No request was sent"):
                prefect.exchange(self.site, request)
        https.assert_not_called()
        self.site.values["prefect_url"] = "https://broker.example/prefect"
        with patch("gbi_data.prefect._exchange_socket", side_effect=prefect._BrokerUnavailable("missing")), \
             patch("gbi_data.prefect._exchange_https", return_value=response) as https:
            self.assertEqual(prefect.exchange(self.site, request), response)
        https.assert_called_once_with(self.site.values["prefect_url"], request)
        with patch("gbi_data.prefect._exchange_socket", side_effect=ValueError("partial reply")), \
             patch("gbi_data.prefect._exchange_https") as https:
            with self.assertRaisesRegex(ValueError, "partial reply"):
                prefect.exchange(self.site, request)
        https.assert_not_called()
        with patch("gbi_data.prefect._exchange_socket", side_effect=PermissionError("denied")), \
             patch("gbi_data.prefect._exchange_https") as https:
            with self.assertRaisesRegex(ValueError, "denied"):
                prefect.exchange(self.site, request)
        https.assert_not_called()

    def test_https_url_rejects_unsafe_forms(self):
        for value in ("http://broker.example", "https://user:pass@broker.example", "https://broker.example/?x=1",
                      "https://broker.example/#fragment"):
            with self.subTest(value=value):
                self.site.values["prefect_url"] = value
                with self.assertRaises(ValueError):
                    prefect._https_url(self.site)

    def test_slurm_token_is_cached_without_leaking_command_output(self):
        completed = subprocess.CompletedProcess(("scontrol", "token"), 0,
                                                 stdout="SLURM_JWT=secret-token\n", stderr="")
        with patch("gbi_data.prefect.subprocess.run", return_value=completed) as run, \
             patch("gbi_data.prefect.time.monotonic", side_effect=[100, 140, 146]), \
             patch.dict(os.environ, {"SLURM_JWT": "inherited-secret", "PATH": "/bin"}, clear=True):
            self.assertEqual(prefect._token(), "secret-token")
            self.assertEqual(prefect._token(), "secret-token")
            self.assertEqual(run.call_count, 1)
            self.assertEqual(prefect._token(), "secret-token")
            self.assertEqual(run.call_count, 2)
        self.assertEqual(run.call_args.args, (("scontrol", "token", "lifespan=60"),))
        self.assertEqual(run.call_args.kwargs["env"], {"PATH": "/bin"})

    def test_token_failures_do_not_expose_process_output(self):
        for error in (FileNotFoundError("private command details"),
                      subprocess.CalledProcessError(1, "scontrol", output="private token", stderr="private error"),
                      subprocess.TimeoutExpired("scontrol", 10, output="private token")):
            with self.subTest(error=type(error).__name__), \
                 patch("gbi_data.prefect.subprocess.run", side_effect=error), \
                 self.assertRaisesRegex(ValueError, "cannot obtain Slurm user token") as raised:
                prefect._token()
            self.assertNotIn("private", str(raised.exception))
            self.assertTrue(raised.exception.__suppress_context__)

    def test_https_preserves_request_and_public_errors_but_bounds_untrusted_responses(self):
        request = prefect.prepare(self.options(), self.site)
        response = {"version": 1, "state": "COMPLETED", "run_id": "74ec9296-0446-4a46-a428-f004e556f066"}
        with patch("gbi_data.prefect._token", return_value="private-token"), \
             patch("gbi_data.prefect.urlrequest.build_opener") as factory:
            opener = factory.return_value
            opener.open.return_value.__enter__.return_value.read.return_value = json.dumps(response).encode()
            self.assertTrue(prefect._exchange_https("https://broker.example/migrations", request)["terminal"])
            submitted = opener.open.call_args.args[0]
            self.assertEqual(json.loads(submitted.data), request)
            self.assertEqual(submitted.get_header("Authorization"), "Bearer private-token")
            for raw, message in ((b'{"version":1,"error":"your route is unavailable"}', "your route is unavailable"),
                                 (b"private broken body", "response was invalid"),
                                 (b"x" * (prefect._MAX_RESPONSE + 1), "response was invalid")):
                opener.open.return_value.__enter__.return_value.read.return_value = raw
                with self.assertRaisesRegex(ValueError, message) as raised:
                    prefect._exchange_https("https://broker.example/migrations", request)
                self.assertNotIn("private", str(raised.exception))

    def test_https_transport_failures_and_redirects_do_not_expose_credentials(self):
        errors = (urlerror.HTTPError("https://private.example", 401, "private-token", {}, io.BytesIO(b"private body")),
                  urlerror.URLError("private-token"), TimeoutError("private-token"))
        for error in errors:
            with self.subTest(error=type(error).__name__), \
                 patch("gbi_data.prefect._token", return_value="private-token"), \
                 patch("gbi_data.prefect.urlrequest.build_opener") as factory:
                factory.return_value.open.side_effect = error
                with self.assertRaisesRegex(ValueError, "HTTPS request failed") as raised:
                    prefect._exchange_https("https://broker.example/migrations", {})
                self.assertNotIn("private", str(raised.exception))
                self.assertTrue(raised.exception.__suppress_context__)
        with self.assertRaisesRegex(ValueError, "redirected"):
            prefect._NoRedirect().redirect_request(None, None, 307, None, {}, "https://other.example")

    def test_socket_write_failure_never_switches_transport(self):
        self.site.values["prefect_url"] = "https://broker.example/migrations"
        for error in (BrokenPipeError(errno.EPIPE, "lost write"), TimeoutError("lost reply")):
            with self.subTest(error=type(error).__name__), \
                 patch("gbi_data.prefect.socket.socket") as factory, \
                 patch("gbi_data.prefect._exchange_https") as remote:
                client = factory.return_value.__enter__.return_value
                client.sendall.side_effect = error
                with self.assertRaisesRegex(ValueError, "same transfer ID"):
                    prefect.exchange(self.site, prefect.prepare(self.options(), self.site))
                remote.assert_not_called()

    def test_missing_socket_falls_back_without_changing_request(self):
        self.site.values["prefect_url"] = "https://broker.example/migrations"
        request = prefect.prepare(self.options(), self.site)
        for code in (errno.ENOENT, errno.ECONNREFUSED):
            with self.subTest(code=code), patch("gbi_data.prefect.socket.socket") as factory, \
                 patch("gbi_data.prefect._exchange_https", return_value={"state": "RUNNING"}) as remote:
                client = factory.return_value.__enter__.return_value
                client.connect.side_effect = OSError(code, "unavailable")
                prefect.exchange(self.site, request)
                client.sendall.assert_not_called()
                remote.assert_called_once_with(self.site.values["prefect_url"], request)

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


class NativeSync(unittest.TestCase):
    setUp = Prefect.setUp
    options = Prefect.options
    def native_options(self, source="lustre", target="alluxio", *flags):
        return cli.parser().parse_args([
            "data", "copy", str(self.site.roots[source] / "experiment"),
            str(self.site.roots[target] / "experiment"), "--native-sync", *flags,
        ])

    def test_native_selects_managed_export_without_extra_prefect_flag(self):
        options = self.native_options()
        request = prefect.prepare(options, self.site)
        self.assertEqual(request["operation"], "archive-lustre")
        self.assertIs(request["native_sync"], True)
        self.assertEqual(request["include"], [])
        self.assertFalse(options.prefect)
        self.assertNotIn("native_sync", prefect.prepare(self.options(), self.site))

    def test_native_rejects_any_filter_or_format(self):
        for flags in (("--include", "*.pt"), ("--exclude", "*.tmp"),
                      ("--pack", "gzip"), ("--pack-small",),
                      ("--chunk-size", "64MiB"), ("--delete-source",)):
            with self.subTest(flags=flags), self.assertRaisesRegex(ValueError, "--native-sync"):
                prefect.prepare(self.native_options("lustre", "alluxio", *flags), self.site)

    def test_native_rejects_fss_and_restore_routes(self):
        for source, target in (("fss", "alluxio"), ("alluxio", "lustre"), ("alluxio", "fss")):
            with self.subTest(source=source, target=target), self.assertRaisesRegex(ValueError, "Lustre to Object Storage"):
                prefect.prepare(self.native_options(source, target), self.site)

    def test_native_execution_choice_is_exclusive(self):
        for flag in ("--prefect", "--detach", "--local"):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                self.native_options("lustre", "alluxio", flag)

    def test_native_dry_run_explains_unchecked_link_without_submission(self):
        output = io.StringIO()
        with patch("gbi_data.prefect.exchange") as exchange, contextlib.redirect_stdout(output):
            self.assertEqual(prefect.start(
                self.native_options("lustre", "alluxio", "--dry-run"), self.site, self.home,
            ), 0)
        exchange.assert_not_called()
        self.assertFalse(self.home.exists())
        self.assertIn("Native OCI sync", output.getvalue())
        self.assertIn("dry run checks local options only", output.getvalue())

    def test_native_cli_dispatches_to_managed_route(self):
        args = ["gbi", "data", "copy", str(self.site.roots["lustre"] / "experiment"),
                str(self.site.roots["alluxio"] / "experiment"), "--native-sync", "--dry-run"]
        with patch("sys.argv", args), patch.dict(os.environ, {"GBI_DATA_SITE_CONF": str(self.site.path)}), \
             patch("gbi_data.prefect.start", return_value=0) as start, \
             patch("gbi_data.cli.plan") as ordinary:
            self.assertEqual(cli.main(), 0)
        self.assertTrue(start.call_args.args[0].native_sync)
        ordinary.assert_not_called()

    def test_native_lost_reply_keeps_request_identity(self):
        with patch("gbi_data.prefect.exchange", side_effect=ValueError("lost")), \
             patch("sys.stdout.isatty", return_value=False):
            with self.assertRaises(ValueError):
                prefect.start(self.native_options(), self.site, self.home)
        run_dir = next((self.home / "runs").iterdir())
        request = prefect.stored_request(run_dir)
        self.assertIs(request["native_sync"], True)
        with patch("gbi_data.prefect.exchange", return_value={
            "version": 1, "state": "RUNNING", "terminal": False,
        }) as exchange:
            prefect.submit(run_dir, self.site)
        self.assertEqual(exchange.call_args.args[1], request)
