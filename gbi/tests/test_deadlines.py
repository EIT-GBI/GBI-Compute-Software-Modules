"""Real subprocess stalls and signal propagation at the mount watchdog boundary."""

import contextlib
import io
import json
import os
from pathlib import Path
import pwd
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from gbi_data import cli, deadlines, jobs
from gbi_data.storage import Site


class Deadlines(unittest.TestCase):
    def supervise(self, script, timeout=0.2):
        output = io.StringIO()
        started = time.monotonic()
        with contextlib.redirect_stderr(output):
            result = deadlines.watch([sys.executable, "-B", "-c", script], startup_timeout=timeout)
        self.assertLess(time.monotonic() - started, 4)
        return result, output.getvalue()

    def test_initial_site_resolution_is_bounded(self):
        result, output = self.supervise(
            "from pathlib import Path; import time; from gbi_data.storage import Site; "
            "Path.resolve=lambda *args, **kwargs: time.sleep(30); Site('/unavailable/site.conf')")
        self.assertEqual(result, 1)
        self.assertIn("FAILED startup", output)
        self.assertIn("does not prove underlying filesystem I/O stopped", output)

    def test_history_deadline_reports_last_counters_and_preserves_records(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            username = pwd.getpwuid(os.getuid()).pw_name
            for name in ("lustre", "fss", "bucket"):
                (base / name / username).mkdir(parents=True)
            config = base / "site.conf"
            config.write_text("".join(f"{key}_root = {base / key}\n" for key in ("lustre", "fss", "bucket"))
                              + "mount_timeout = 5\nhistory_timeout = 0.2\n")
            source = base / "lustre" / username / "source"
            target = base / "fss" / username / "copy"
            source.write_bytes(b"retained original")
            script = (
                "import os, sys, time; from gbi_data import cli; "
                f"os.environ['GBI_DATA_SITE_CONF']={str(config)!r}; "
                f"sys.argv=['gbi','data','copy',{str(source)!r},{str(target)!r}]; "
                "cli.publish_history=lambda *args: time.sleep(30); sys.exit(cli.main())")
            result, output = self.supervise(script, timeout=5)
            self.assertEqual(result, 1)
            self.assertIn("FAILED saving history", output)
            self.assertIn("verified bytes=17", output)
            self.assertIn("source bytes removed=0", output)
            self.assertEqual(source.read_bytes(), target.read_bytes())
            self.assertTrue(list((base / "lustre" / username / ".gbi/runs").glob("*/receipts.jsonl")))

    def test_stalled_discovery_fails_without_claiming_completion(self):
        # The discovery timeout is distinct from the outer filesystem timeout.
        script = (
            "import time; from gbi_data import deadlines; "
            "deadlines.event('discovering', '/stalled/source'); time.sleep(30)")
        result, output = self.supervise(script)
        self.assertEqual(result, 1)
        self.assertIn("FAILED discovering", output)
        self.assertIn("/stalled/source", output)
        self.assertIn("inspect receipts before retrying", output)

    def test_coordinator_discovery_deadline_keeps_unverified_source(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            user = pwd.getpwuid(os.getuid()).pw_name
            for name in ("lustre", "fss", "bucket"):
                (base / name / user).mkdir(parents=True)
            config = base / "site.conf"
            config.write_text("".join(f"{key}_root = {base / key}\n" for key in ("lustre", "fss", "bucket"))
                              + "discovery_timeout = 0.02\n")
            site = Site(config)
            source = site.roots["lustre"] / "source"
            source.write_bytes(b"unverified")
            target = site.roots["fss"] / "target"
            options = cli.parser().parse_args(["data", "move", str(source), str(target)])
            request = {**cli.plan(options, site), "reader": "native", "execution": "allocation"}
            run_dir = base / "run"
            run_dir.mkdir()
            (run_dir / "request.json").write_text(json.dumps(request))
            from unittest.mock import MagicMock
            discovery = MagicMock()
            discovery.get.side_effect = lambda _timeout: time.sleep(0.03)
            captured = {}

            def keep_history(_run, _site, _state, _rclone, progress, *_args):
                captured.update(progress)
                return "test history"

            with patch("gbi_data.cli.stream_entries", return_value=discovery), \
                 patch("gbi_data.cli.publish_history", side_effect=keep_history):
                self.assertEqual(cli.run(run_dir, site, base / "home"), 1)
            self.assertEqual(captured["phase"], "failed")
            self.assertFalse(captured["discovery_complete"])
            self.assertEqual(captured["freed"], 0)
            self.assertEqual(source.read_bytes(), b"unverified")
            self.assertFalse(target.exists())

    def test_coordinator_distinguishes_scan_progress_from_stall_and_busy_workers(self):
        from gbi_data import selection

        for scenario in ("scanning", "stalled", "busy"):
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as directory:
                base = Path(directory).resolve()
                user = pwd.getpwuid(os.getuid()).pw_name
                for name in ("lustre", "fss", "bucket"):
                    (base / name / user).mkdir(parents=True)
                config = base / "site.conf"
                config.write_text("".join(f"{key}_root = {base / key}\n" for key in ("lustre", "fss", "bucket"))
                                  + "discovery_timeout = 0.1\njobs = 1\n")
                site = Site(config)
                source = site.roots["lustre"] / "source"
                source.write_bytes(b"retained")
                target = site.roots["fss"] / "target"
                options = cli.parser().parse_args(["data", "copy", str(source), str(target)])
                request = {**cli.plan(options, site), "reader": "native", "execution": "allocation"}
                run_dir = base / "run"
                run_dir.mkdir()
                (run_dir / "request.json").write_text(json.dumps(request))

                def iterator(_on_error, visit):
                    if scenario == "scanning":
                        for _ in range(60):
                            visit()  # Traversal advances through excluded/nonmatching entries.
                            time.sleep(0.02)
                    else:
                        time.sleep(1)
                    yield source, False

                if scenario == "busy":
                    from unittest.mock import Mock
                    discovery = Mock()
                    discovery.progress.return_value = (1, time.monotonic())
                    events = iter([("entry", (source, False), None), None, ("done", None, None)])

                    def get(_timeout):
                        time.sleep(0.02)
                        return next(events)

                    discovery.get.side_effect = get
                else:
                    discovery = selection.stream_entries(source, site, source.parent, [], iterator_factory=iterator)
                captured = {}

                def keep_history(_run, _site, _state, _rclone, progress, *_args):
                    captured.update(progress)
                    return "test history"

                def slow_worker(*_args):
                    time.sleep(0.3)  # Discovery cannot time out while its worker capacity is full.
                    return {"bytes": 8, "deleted": False}

                with patch("gbi_data.cli.stream_entries", return_value=discovery), \
                     patch("gbi_data.cli.supervise", side_effect=slow_worker), \
                     patch("gbi_data.cli.publish_history", side_effect=keep_history), \
                     contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    self.assertEqual(cli.run(run_dir, site, base / "home"), 1 if scenario == "stalled" else 0)
                self.assertEqual(captured["discovery_complete"], scenario != "stalled")
                self.assertEqual(captured["freed"], 0)
                self.assertEqual(source.read_bytes(), b"retained")

    def test_finished_worker_is_not_signalled_again(self):
        script = ("import time; from gbi_data import deadlines; "
                  "deadlines.event(worker_started=99999999); "
                  "deadlines.event(worker_finished=99999999); time.sleep(30)")
        with patch("gbi_data.deadlines._signal_group", wraps=deadlines._signal_group) as send:
            self.assertEqual(self.supervise(script)[0], 1)
        self.assertNotIn(99999999, [call.args[0] for call in send.call_args_list])

    def test_stalled_readback_keeps_source_without_verified_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            source_root, target_root, state = base / "source", base / "target", base / "state"
            for path in (source_root, target_root, state / "locks", state / "pending"):
                path.mkdir(parents=True)
            source, target = source_root / "file", target_root / "file"
            source.write_bytes(b"do not delete before readback")
            task = {"source": str(source), "target": str(target), "source_root": str(source_root),
                    "target_root": str(target_root), "selection_root": str(source_root),
                    "state": str(state), "receipt": str(base / "receipt.jsonl"),
                    "progress": str(base / "progress.json"), "rclone": None, "settle_seconds": 1,
                    "target_kind": "fss", "delete": True, "encode_link": False, "decode_link": False}
            script = ("import time; from gbi_data import transfer, deadlines\n"
                      "def blocked_readback(path):\n"
                      "    deadlines.event('destination readback', path)\n"
                      "    time.sleep(30)\n"
                      "transfer.digest=blocked_readback\n"
                      f"transfer.transfer({task!r})")
            result, output = self.supervise(script, timeout=0.5)
            self.assertEqual(result, 1)
            self.assertIn("FAILED destination readback", output)
            self.assertEqual(source.read_bytes(), target.read_bytes())
            self.assertFalse((base / "receipt.jsonl").exists())

    def test_status_reports_dead_local_coordinator(self):
        progress = {"phase": "saving history", "files": 1, "bytes": 10, "freed": 0,
                    "failed": 0, "hostname": socket.gethostname(), "coordinator_pid": 12345}
        with tempfile.TemporaryDirectory() as directory, \
             patch("gbi_data.jobs.read_progress", return_value=progress), \
             patch("gbi_data.jobs.os.kill", side_effect=ProcessLookupError), \
             contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(jobs.watch(Path(directory), "20260923T000000Z-abcdef123456"), 1)
        self.assertIn("stopped without a final history record", output.getvalue())

    def test_status_does_not_treat_reused_pid_as_the_coordinator(self):
        progress = {"phase": "saving history", "files": 1, "bytes": 10, "freed": 0,
                    "failed": 0, "hostname": socket.gethostname(), "coordinator_pid": 12345,
                    "coordinator_identity": "111"}
        with tempfile.TemporaryDirectory() as directory, \
             patch("gbi_data.jobs.read_progress", return_value=progress), \
             patch("gbi_data.jobs.os.kill"), \
             patch("gbi_data.jobs.process_identity", return_value="222"), \
             contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(jobs.watch(Path(directory), "20260923T000000Z-abcdef123456"), 1)
        self.assertIn("stopped without a final history record", output.getvalue())

    def test_child_exit_code_is_preserved(self):
        self.assertEqual(self.supervise("raise SystemExit(7)")[0], 7)

    def test_exported_stale_event_environment_does_not_skip_watchdog(self):
        with patch.dict(os.environ, {deadlines.EVENT_FD: "999999", deadlines.SUPERVISOR_PID: "1"}), \
             patch("gbi_data.deadlines.watch", return_value=7) as watch:
            self.assertEqual(deadlines.entry(lambda: 99), 7)
            watch.assert_called_once()

    def test_reused_descriptor_cannot_write_events_into_an_ordinary_file(self):
        with tempfile.TemporaryFile() as stream, patch.dict(os.environ, {
                deadlines.EVENT_FD: str(stream.fileno()), deadlines.SUPERVISOR_PID: str(os.getppid())}):
            self.assertFalse(deadlines.supervised())
            deadlines.event("must not write")
            self.assertEqual(stream.tell(), 0)

    def test_malformed_pipe_events_do_not_abandon_the_child(self):
        script = ("import os,time; from gbi_data import deadlines; "
                  "fd=int(os.environ[deadlines.EVENT_FD]); "
                  "os.write(fd,b'not-json\\n{\"timeouts\":1}\\n{\"phase\":'); time.sleep(30)")
        self.assertEqual(self.supervise(script)[0], 1)

    def test_concurrent_large_pipe_events_remain_readable(self):
        script = ("import threading; from gbi_data import deadlines\n"
                  "def send():\n"
                  "    for _ in range(5): deadlines.event('startup', 'x'*10000)\n"
                  "threads=[threading.Thread(target=send) for _ in range(4)]\n"
                  "for thread in threads: thread.start()\n"
                  "for thread in threads: thread.join()\n")
        self.assertEqual(self.supervise(script, timeout=2)[0], 0)

    def test_history_can_continue_while_file_workers_finish(self):
        script = ("import time; from gbi_data import deadlines\n"
                  "deadlines.event('saving history', '/history')\n"
                  "for _ in range(4):\n"
                  "    deadlines.event(worker_started=99999999)\n"
                  "    time.sleep(0.2)\n"
                  "    deadlines.event(worker_finished=99999999)\n")
        self.assertEqual(self.supervise(script, timeout=0.5)[0], 0)

    def test_counter_heartbeats_do_not_hide_stalled_history(self):
        script = ("import time; from gbi_data import deadlines\n"
                  "deadlines.event('saving history', '/history')\n"
                  "for _ in range(30):\n"
                  "    deadlines.event(counters={'bytes': 10})\n"
                  "    time.sleep(0.05)\n")
        self.assertEqual(self.supervise(script)[0], 1)

    def test_sigint_is_forwarded_for_child_detach_behavior(self):
        script = (
            "import os, signal, sys, time; "
            "signal.signal(signal.SIGINT, lambda *_: sys.exit(0)); "
            "os.kill(os.getppid(), signal.SIGINT); time.sleep(30)")
        self.assertEqual(self.supervise(script, timeout=2)[0], 0)

    def test_nonterminal_progress_has_timestamps_and_periodic_newlines(self):
        progress = {"phase": "saving history", "files": 1, "bytes": 10, "freed": 0,
                    "failed": 0, "elapsed": 1}
        output = io.StringIO()
        with contextlib.redirect_stdout(output), patch("sys.stdout.isatty", return_value=False), \
             patch("gbi_data.jobs.time.monotonic", side_effect=[1, 17, 17]):
            display = jobs.ProgressDisplay()
            display.update(progress)
            display.update(progress)
        lines = output.getvalue().splitlines()
        self.assertEqual(len(lines), 2)
        self.assertRegex(lines[0], r"^\d{4}-\d{2}-\d{2}T.*Z saving history:")


if __name__ == "__main__":
    unittest.main()
