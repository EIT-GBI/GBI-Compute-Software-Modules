"""gbi data cache submits native loads through the broker and waits for them."""

import io
import json
import os
from pathlib import Path
import pwd
import tempfile
import time as real_time
import unittest
from unittest.mock import patch
from contextlib import redirect_stdout, redirect_stderr

from gbi_data import cache, cli
from gbi_data.storage import Site


class FakeTime:
    """A clock that only advances when the code sleeps."""

    def __init__(self):
        self.now = 1000.0
        self.slept = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds

    def strftime(self, fmt, when = None):
        return real_time.strftime(fmt, when if when is not None else self.gmtime())

    def gmtime(self, *args):
        return real_time.gmtime(1700000000 + int(self.now))


class Broker:
    """A scripted broker: records requests, answers submit and status."""

    def __init__(
                self, states = ("RUNNING", "COMPLETED"), message = None,
                refuse = None, submit_state = "SCHEDULED", execution = "cache",
            ):
        self.requests = []
        self.states = list(states)
        self.message = message
        self.refuse = refuse
        self.submit_state = submit_state
        self.execution = execution
        self.polls = {}

    def __call__(self, site, request, retry_hint = None):
        self.requests.append((request, retry_hint))
        if self.refuse is not None:
            raise ValueError(self.refuse)
        if request["action"] == "submit":
            state = self.submit_state
        else:
            key = request.get("request_id") or (
                request["path"], tuple(request["include"])
            )
            count = self.polls.get(key, 0)
            self.polls[key] = count + 1
            state = self.states[min(count, len(self.states) - 1)]
        result = {
            "version": 1, "state": state,
            "terminal": state in {"COMPLETED", "FAILED", "CANCELLED", "CRASHED"},
            "run_id": "11111111-2222-3333-4444-555555555555",
        }
        if result["terminal"] and self.message:
            result["message"] = self.message
        if self.execution is not None:
            result["execution"] = self.execution
        return result


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
        )
        self.site    = Site(config)
        self.alluxio = self.site.roots["alluxio"]
        self.lustre  = self.site.roots["lustre"]
        self.data    = self.alluxio / "dataset"
        (self.data / "a").mkdir(parents = True)
        (self.data / "a" / "one.bin").write_bytes(b"1")
        self.time = FakeTime()

    def options(self, *arguments):
        return cli.parser().parse_args(["data", "cache", *arguments])

    def run_cache(self, broker, *arguments):
        out, err = io.StringIO(), io.StringIO()
        with (
            patch("gbi_data.prefect.exchange", broker),
            patch("gbi_data.cache.time", self.time),
            redirect_stdout(out), redirect_stderr(err),
        ):
            code = cache.main(self.options(*arguments), self.site)
        return code, out.getvalue(), err.getvalue()

    def runs(self):
        return sorted((self.lustre / ".gbi" / "runs").glob("*"))

    def run_status(self, broker, target, *arguments):
        out, err = io.StringIO(), io.StringIO()
        with (
            patch("gbi_data.prefect.exchange", broker),
            patch("gbi_data.cache.time", self.time),
            patch("sys.argv", ["gbi", "data", "status", str(target), *arguments]),
            patch.dict(os.environ, {"GBI_DATA_SITE_CONF": str(self.site.path)}),
            redirect_stdout(out), redirect_stderr(err),
        ):
            code = cli.main()
        return code, out.getvalue(), err.getvalue()

    def test_resolves_a_symlinked_path_once_and_refuses_lustre_and_fss(self):
        link = self.base / "instrument-data"
        link.symlink_to(self.data)
        located, root = cache.resolve(self.site, str(link / "a"))
        self.assertEqual(located, self.data / "a")
        self.assertEqual(root, self.alluxio)
        for raw in (self.lustre, self.site.roots["fss"], self.base / "missing"):
            with self.assertRaises(cache.CacheError):
                cache.resolve(self.site, str(raw))

    def test_submits_one_request_per_path_with_the_resolved_path_and_patterns(self):
        broker = Broker(message = "cached 1 files, 0.00 GiB, 100% resident after 4 s")
        code, out, err = self.run_cache(
            broker, str(self.data), str(self.data / "a"),
            "--include", "*.fastq.gz,*.bam", "--include", "*.bin",
        )
        self.assertEqual((code, err), (0, ""))
        submits = [request for request, _hint in broker.requests if request["action"] == "submit"]
        self.assertEqual([request["path"] for request in submits], [str(self.data), str(self.data / "a")])
        self.assertEqual(submits[0]["include"], ["*.fastq.gz", "*.bam", "*.bin"])
        self.assertEqual(submits[0]["operation"], "cache")
        self.assertEqual({hint for _request, hint in broker.requests}, {cache.RETRY_HINT})
        self.assertEqual(len(self.runs()), 2)
        saved = {
            json.loads((run / "request.json").read_text())["broker_request"]["path"]: run
            for run in self.runs()
        }
        record = json.loads((saved[str(self.data)] / "request.json").read_text())
        self.assertEqual(record["broker_request"], submits[0])
        self.assertEqual(record["execution"], "cache")
        self.assertIn("Cache COMPLETED", out)
        self.assertNotIn("Prefect", out)
        self.assertIn("  cached 1 files, 0.00 GiB, 100% resident after 4 s", out)
        self.assertIn("Reconnect: gbi data status", out)
        self.assertEqual(
            json.loads((saved[str(self.data)] / "cache.json").read_text())["state"], "COMPLETED"
        )
        for query, _hint in broker.requests:
            if query["action"] == "status":
                self.assertEqual(query, {
                    "version": 1, "action": "status", "operation": "cache",
                    "path": query["path"], "include": submits[0]["include"],
                })

    def test_a_failed_load_exits_1_with_the_service_message(self):
        broker = Broker(states = ("RUNNING", "FAILED"), message = "refused: /x is not readable by me")
        code, out, _err = self.run_cache(broker, str(self.data))
        self.assertEqual(code, 1)
        self.assertIn("Cache FAILED", out)
        self.assertIn("refused: /x is not readable by me", out)

    def test_immediate_native_results_keep_exit_status_with_detach(self):
        for state, expected in (("COMPLETED", 0), ("FAILED", 1)):
            for arguments in ((), ("--detach",)):
                with self.subTest(state = state, arguments = arguments):
                    broker = Broker(submit_state = state)
                    code, out, err = self.run_cache(
                        broker, str(self.data), *arguments
                    )
                    self.assertEqual((code, err), (expected, ""))
                    self.assertIn("Cache " + state, out)
                    self.assertEqual(len(broker.requests), 1)

    def test_timeout_stops_waiting_and_exits_3_with_a_reconnect_hint(self):
        broker = Broker(states = ("RUNNING",))
        code, out, _err = self.run_cache(broker, str(self.data), "--timeout", "10")
        self.assertEqual(code, 3)
        self.assertIn("Stopped waiting at --timeout", out)
        self.assertIn("Reconnect: gbi data status", out)
        self.assertLessEqual(len(self.time.slept), 5)

    def test_a_refusal_exits_2_and_waits_for_nothing(self):
        broker = Broker(refuse = "Cache request not confirmed: path is not inside an Object Storage mount that can be cached. Use status or run the same command again")
        code, _out, err = self.run_cache(broker, str(self.data))
        self.assertEqual(code, 2)
        self.assertIn("not inside an Object Storage mount that can be cached", err)
        self.assertEqual(len(broker.requests), 1)

    def test_detach_returns_after_submission_and_status_reconnects(self):
        broker = Broker(message = "cached 1 files, 0.00 GiB, 100% resident after 2 s")
        code, out, _err = self.run_cache(
            broker, str(self.data), "--include", "*.bin", "--detach"
        )
        self.assertEqual(code, 0)
        self.assertIn("Cache SCHEDULED", out)
        self.assertEqual(len(broker.requests), 1)
        run_dir, = self.runs()
        code, out, err = self.run_status(broker, run_dir.name, "--watch")
        self.assertEqual((code, err), (0, ""))
        self.assertIn("Cache COMPLETED · " + run_dir.name, out)
        self.assertIn("100% resident", out)
        self.assertTrue(all(r["action"] == "status" for r, _ in broker.requests[1:]))
        self.assertTrue(all(r["include"] == ["*.bin"] for r, _ in broker.requests[1:]))

    def test_status_path_checks_current_state_without_submitting_or_saving(self):
        broker = Broker()
        link = self.base / "instrument-data"
        link.symlink_to(self.data)
        code, out, err = self.run_status(broker, link, "--watch")
        self.assertEqual((code, err), (0, ""))
        self.assertIn(f"Cache COMPLETED · {self.data}", out)
        self.assertEqual(self.runs(), [])
        self.assertEqual([r for r, _ in broker.requests], [{
            "version": 1, "action": "status", "operation": "cache",
            "path": str(self.data), "include": [],
        }] * 2)

    def test_status_without_a_load_returns_without_waiting_or_submitting(self):
        broker = Broker(states = ("NOT_SUBMITTED",))
        code, out, err = self.run_status(broker, self.data, "--watch")
        self.assertEqual((code, err), (1, ""))
        self.assertIn("no submitted load found", out)
        self.assertEqual(len(broker.requests), 1)
        self.assertEqual(self.time.slept, [])

    def test_status_refuses_paths_outside_object_storage(self):
        broker = Broker()
        for path in (self.lustre, self.base / "missing"):
            code, _out, err = self.run_status(broker, path)
            self.assertNotEqual(code, 0)
            self.assertTrue(err)
        self.assertEqual(broker.requests, [])

    def test_historical_saved_cache_id_keeps_its_prefect_lookup(self):
        _path, body = cache.request(self.site, str(self.data), ["*.bin"])
        run_dir = cache.record(self.site, body)
        (run_dir / "request.json").write_text(json.dumps({
            "execution": "prefect", "uid": os.getuid(), "broker_request": body,
        }))
        broker = Broker(states = ("COMPLETED",))
        code, out, err = self.run_status(broker, run_dir.name)
        self.assertEqual((code, err), (0, ""))
        self.assertIn("Prefect COMPLETED · cache " + run_dir.name, out)
        self.assertEqual(broker.requests[0][0], {
            "version": 1, "action": "status", "request_id": body["request_id"],
        })
        self.assertEqual(
            json.loads((run_dir / "prefect.json").read_text())["state"], "COMPLETED"
        )

    def test_new_cli_keeps_legacy_submission_and_reconnect_on_old_broker(self):
        broker = Broker(execution = None)
        code, out, err = self.run_cache(broker, str(self.data), "--detach")
        self.assertEqual((code, err), (0, ""))
        self.assertIn("Prefect SCHEDULED · cache", out)
        run_dir, = self.runs()
        record = json.loads((run_dir / "request.json").read_text())
        self.assertEqual(record["execution"], "prefect")
        code, out, err = self.run_status(broker, run_dir.name, "--watch")
        self.assertEqual((code, err), (0, ""))
        self.assertIn("Prefect COMPLETED · cache", out)
        self.assertEqual([r for r, _ in broker.requests[1:]], [{
            "version": 1, "action": "status",
            "request_id": record["broker_request"]["request_id"],
        }] * 2)

    def test_detached_json_keeps_the_initial_service_state(self):
        broker = Broker()
        code, out, err = self.run_cache(
            broker, str(self.data), "--detach", "--json"
        )
        self.assertEqual((code, err), (0, ""))
        summary = json.loads(out.strip().splitlines()[-1])
        self.assertEqual(summary["paths"][0]["state"], "SCHEDULED")
        self.assertEqual(len(broker.requests), 1)

    def test_saved_cache_status_preserves_user_ownership(self):
        _path, body = cache.request(self.site, str(self.data), [])
        run_dir = cache.record(self.site, body)
        (run_dir / "request.json").write_text(json.dumps({
            "execution": "cache", "uid": os.getuid() + 1, "broker_request": body,
        }))
        broker = Broker()
        code, _out, err = self.run_status(broker, run_dir.name)
        self.assertNotEqual(code, 0)
        self.assertIn("another user", err)
        self.assertEqual(broker.requests, [])

    def test_dry_run_and_invalid_arguments_submit_nothing(self):
        broker = Broker()
        code, out, _err = self.run_cache(broker, str(self.data), "--include", "*.bam", "--dry-run")
        self.assertEqual(code, 0)
        self.assertIn(f"Would load {self.data} (files matching *.bam)", out)
        self.assertEqual(broker.requests, [])
        self.assertEqual(self.runs(), [])
        for arguments in (
            (str(self.data), "--include", "a/b"),
            (str(self.data), "--timeout", "-1"),
            (str(self.lustre),),
        ):
            code, _out, err = self.run_cache(broker, *arguments)
            self.assertEqual(code, 2, arguments)
            self.assertTrue(err.startswith("gbi data cache: "), err)
        self.assertEqual(broker.requests, [])

    def test_json_summary_lists_every_path(self):
        broker = Broker(message = "cached 1 files, 0.00 GiB, 100% resident after 1 s")
        code, out, _err = self.run_cache(broker, str(self.data), "--json")
        self.assertEqual(code, 0)
        summary = json.loads(out.strip().splitlines()[-1])
        self.assertFalse(summary["timed_out"])
        entry, = summary["paths"]
        self.assertEqual(entry["path"], str(self.data))
        self.assertEqual(entry["state"], "COMPLETED")
        self.assertEqual(entry["message"], "cached 1 files, 0.00 GiB, 100% resident after 1 s")
        self.assertEqual(entry["transfer_id"], self.runs()[0].name)

    def test_waiting_reports_to_the_supervisor_and_heartbeats(self):
        broker = Broker(states = ("RUNNING",) * 30 + ("COMPLETED",))
        with patch("gbi_data.cache.deadlines.event") as event:
            code, out, _err = self.run_cache(broker, str(self.data))
        self.assertEqual(code, 0)
        self.assertGreaterEqual(event.call_count, 30)
        self.assertEqual(event.call_args[0][0], "waiting for the cache load")
        self.assertIn("still loading: 1 path(s)", out)


if __name__ == "__main__":
    unittest.main()
