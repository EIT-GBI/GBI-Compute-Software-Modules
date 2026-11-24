"""Saved ordinary retries reconcile evidence without rewriting old history."""

from contextlib import redirect_stdout
import copy
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import pwd
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import ANY, MagicMock, patch

from gbi_data import cli, retry
from gbi_data import transfer as engine
from gbi_data.storage import Site, fingerprint, lock_stripe


class WorkerLiveness(unittest.TestCase):
    def test_only_absent_process_group_proves_workers_stopped(self):
        for error, expected in (
            (ProcessLookupError(), True), (PermissionError(), False),
            (None, False),
        ):
            with self.subTest(error = error), patch.object(
                cli.os, "killpg", side_effect = error,
            ) as probe:
                self.assertEqual(cli.worker_group_stopped(12345), expected)
                probe.assert_called_once_with(12345, 0)

    def test_dead_leader_with_surviving_group_is_unresolved_before_stdout_read(self):
        process = MagicMock()
        process.pid = 12345
        process.poll.return_value = 0
        with (
            patch.object(cli.subprocess, "Popen", return_value = process),
            patch.object(cli, "worker_group_stopped", return_value = False),
            patch.object(cli.deadlines, "event"),
        ):
            outcome = cli.supervise({"source": "fixture"}, 1, MagicMock())
        self.assertFalse(outcome["workers_stopped"])
        self.assertIn("group remains active", outcome["error"])
        process.stdout.read.assert_not_called()
        process.stdout.close.assert_called_once()


class RetryFixture(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name).resolve()
        self.config = self.base / "site.conf"
        user = pwd.getpwuid(os.getuid()).pw_name
        for name in ("lustre", "fss", "bucket"):
            (self.base / name / user).mkdir(parents = True)
        self.config.write_text(
            "".join(
                f"{name}_root = {self.base / name}\n"
                for name in ("lustre", "fss", "bucket")
            ) + "partition = test\nverify_settle_seconds = 1\n"
        )
        self.site = Site(self.config)
        self.home = self.site.roots["lustre"] / ".gbi"
        self.source = self.site.roots["lustre"] / "input"
        self.target = self.site.roots["fss"] / "output"
        self.source.mkdir()
        self.environment = {
            **os.environ, "GBI_DATA_SITE_CONF": str(self.config),
            "PATH": "", "PYTHONDONTWRITEBYTECODE": "1",
        }
        self.environment.pop("SLURM_JOB_ID", None)

    def command(self, *arguments, expected = 0):
        result = subprocess.run(
            [sys.executable, "-B", "-m", "gbi_data.cli", "data", *arguments],
            env = self.environment, capture_output = True, text = True,
            timeout = 25,
        )
        self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
        return result

    def histories(self):
        return {
            path.parent: json.loads(path.read_text())
            for path in (self.site.roots["alluxio"] / ".gbi/transfers").glob(
                "*/*/history.json"
            )
        }

    def first_run(self, verb = "move", *arguments, expected = 0):
        self.command(verb, str(self.source), str(self.target), *arguments,
                     expected = expected)
        run, = self.histories()
        return run

    def retry_run(self, original, expected = 0):
        before = (original / "history.json").read_bytes()
        previous = set(self.histories())
        result = self.command("retry", original.name, expected = expected)
        self.assertEqual((original / "history.json").read_bytes(), before)
        histories = self.histories()
        new, = set(histories) - previous
        self.assertNotEqual(new.name, original.name)
        self.assertEqual(histories[new]["request"]["retry_from"], str(original))
        return histories[new], result

    def save(self, run, history):
        (run / "history.json").write_text(json.dumps(history))

    def task(self, path):
        state = self.home / "state"
        for directory in (state / "locks", state / "pending"):
            directory.mkdir(parents = True, exist_ok = True)
        return {
            "source": str(path), "target": str(self.target / path.name),
            "source_root": str(self.site.roots["lustre"]),
            "target_root": str(self.site.roots["fss"]),
            "selection_root": str(self.source), "state": str(state),
            "receipt": str(self.base / "fault-receipts.jsonl"),
            "progress": str(self.base / "fault-progress.json"),
            "rclone": None, "native_bytes": 1024 * 1024,
            "settle_seconds": 1, "target_kind": "fss", "delete": True,
            "encode_link": False, "decode_link": False,
        }

    def saved_task(self, original, path):
        request = retry.prepare(original, self.site)
        selected, = request["selection"]
        return {
            **self.task(path), **request, "source": str(path),
            "target": str(self.target / path.name), "directory": False,
            "expected_source": selected["fingerprint"],
            "retry_receipt": request["retry_receipts"].get(str(path)),
        }


class OrdinaryRetry(RetryFixture):
    def test_public_copy_retry_creates_linked_history_and_reuses_destination(self):
        path = self.source / "sample.dat"
        path.write_bytes(b"retained original data")
        original = self.first_run("copy")
        history, result = self.retry_run(original)
        self.assertEqual(path.read_bytes(), (self.target / path.name).read_bytes())
        self.assertEqual(history["progress"]["files"], 1)
        self.assertEqual(history["progress"]["reused"], 1)
        self.assertEqual(history["progress"]["freed"], 0)
        self.assertIn("VERIFIED REUSED", result.stdout)

    def test_public_move_retry_reverifies_removed_root_without_newly_freed_bytes(self):
        content = b"moved once, verified twice"
        (self.source / "sample.dat").write_bytes(content)
        original = self.first_run()
        self.assertFalse(self.source.exists())
        history, result = self.retry_run(original)
        self.assertEqual((self.target / "sample.dat").read_bytes(), content)
        self.assertEqual(history["progress"]["already_moved"], 1)
        self.assertEqual(history["progress"]["bytes"], len(content))
        self.assertEqual(history["progress"]["freed"], 0)
        self.assertEqual([row["event"] for row in history["receipts"]], ["already_moved"])
        self.assertIn("ALREADY MOVED", result.stdout)

    def test_mixed_deleted_partial_and_pending_move_preserves_owned_recovery(self):
        contents = {
            "deleted.dat": b"already moved",
            "partial.dat": b"recover owned partial bytes",
            "pending.dat": b"not started",
        }
        self.target.mkdir()
        for name, content in contents.items():
            (self.source / name).write_bytes(content)
            if name != "deleted.dat":
                (self.target / name).write_bytes(b"foreign conflict")
        original = self.first_run(expected = 1)
        self.assertFalse((self.source / "deleted.dat").exists())
        (self.target / "partial.dat").unlink()

        def interrupted_copy(source, target, fd, *arguments):
            with os.fdopen(fd, "wb") as output:
                output.write(b"partial")
            raise OSError("injected interrupted copy")

        with patch.object(engine, "copy_stream", side_effect = interrupted_copy):
            with self.assertRaisesRegex(OSError, "injected interrupted"):
                engine.transfer(self.task(self.source / "partial.dat"))
        self.assertEqual((self.target / "partial.dat").read_bytes(), b"partial")
        (self.target / "pending.dat").unlink()
        history, _ = self.retry_run(original)
        self.assertEqual(history["progress"]["files"], 3)
        self.assertEqual(history["progress"]["already_moved"], 1)
        self.assertEqual(history["progress"]["failed"], 0)
        self.assertEqual(
            history["progress"]["freed"],
            len(contents["partial.dat"]) + len(contents["pending.dat"]),
        )
        for name, content in contents.items():
            self.assertEqual((self.target / name).read_bytes(), content)
            self.assertFalse((self.source / name).exists())
        self.assertEqual(list((self.home / "state/pending").glob("*.json")), [])

    def test_missing_previously_moved_destination_fails_without_claiming_success(self):
        (self.source / "sample.dat").write_bytes(b"only destination remains")
        original = self.first_run()
        (self.target / "sample.dat").unlink()
        history, _ = self.retry_run(original, expected = 1)
        self.assertEqual(history["progress"]["files"], 0)
        self.assertEqual(history["progress"]["already_moved"], 0)
        self.assertEqual(history["progress"]["freed"], 0)
        self.assertEqual([row["event"] for row in history["receipts"]], ["failed"])

    def test_corrupt_previously_moved_destination_is_preserved_and_rejected(self):
        (self.source / "sample.dat").write_bytes(b"original")
        original = self.first_run()
        destination = self.target / "sample.dat"
        destination.write_bytes(b"tampered")  # Same size: checksum must matter.
        history, result = self.retry_run(original, expected = 1)
        self.assertEqual(destination.read_bytes(), b"tampered")
        self.assertEqual(history["progress"]["files"], 0)
        self.assertEqual(history["progress"]["freed"], 0)
        self.assertIn("checksum", result.stdout)

    def test_verified_receipt_without_deleted_receipt_cannot_excuse_missing_source(self):
        (self.source / "sample.dat").write_bytes(b"unresolved deletion")
        original = self.first_run()
        history = self.histories()[original]
        history["receipts"] = [
            row for row in history["receipts"] if row["event"] == "verified"
        ]
        self.save(original, history)
        retried, result = self.retry_run(original, expected = 1)
        self.assertEqual(retried["progress"]["files"], 0)
        self.assertIn("without an acknowledged deletion receipt", result.stdout)

    def test_changed_existing_source_retains_both_versions(self):
        source = self.source / "sample.dat"
        source.write_bytes(b"original")
        original = self.first_run("copy")
        source.write_bytes(b"a new version")
        history, result = self.retry_run(original, expected = 1)
        self.assertEqual(source.read_bytes(), b"a new version")
        self.assertEqual((self.target / source.name).read_bytes(), b"original")
        self.assertEqual(history["progress"]["files"], 0)
        self.assertIn("source changed", result.stdout)

    def test_finite_retry_keeps_saved_filters_and_does_not_add_new_files(self):
        for name in ("selected.txt", "skip.txt", "other.bin"):
            (self.source / name).write_bytes(name.encode())
        original = self.first_run("copy", "--include", "*.txt", "--exclude", "skip*")
        (self.source / "new.txt").write_bytes(b"after original snapshot")
        history, _ = self.retry_run(original)
        self.assertEqual(history["request"]["include"], ["*.txt"])
        self.assertEqual(history["request"]["exclude"], ["skip*"])
        self.assertEqual({path.name for path in self.target.iterdir()}, {"selected.txt"})
        self.assertTrue(all((self.source / name).exists() for name in (
            "selected.txt", "skip.txt", "other.bin", "new.txt",
        )))

    def test_object_move_retry_preserves_saved_decision_to_retain_source(self):
        self.source = self.site.roots["alluxio"] / "input"
        self.source.mkdir()
        source = self.source / "sample.dat"
        source.write_bytes(b"retained object source")
        original = self.first_run()
        self.assertTrue(source.exists())
        history, _ = self.retry_run(original)
        self.assertFalse(history["request"]["delete"])
        self.assertTrue(source.exists())
        self.assertEqual(history["progress"]["freed"], 0)
        self.assertNotIn("deleted", [row["event"] for row in history["receipts"]])

    def test_streaming_retry_rediscovers_pending_scope_and_reports_unknown_remainder(self):
        (self.source / "deleted.dat").write_bytes(b"already moved")
        (self.source / "skip.dat").write_bytes(b"excluded")
        original = self.first_run("move", "--include", "*.dat", "--exclude", "skip*")
        history = self.histories()[original]
        history["request"]["selection"] = None
        history["progress"]["discovery_complete"] = False
        self.save(original, history)
        (self.source / "pending.dat").write_bytes(b"discovered on retry")
        (self.source / "other.bin").write_bytes(b"outside filters")
        retried, result = self.retry_run(original)
        self.assertIn("Undiscovered original remainder is unknown", result.stdout)
        self.assertEqual(retried["progress"]["files"], 2)
        self.assertEqual(retried["progress"]["already_moved"], 1)
        self.assertEqual(retried["progress"]["freed"], len(b"discovered on retry"))
        self.assertEqual({path.name for path in self.target.iterdir()}, {"deleted.dat", "pending.dat"})
        self.assertEqual({path.name for path in self.source.iterdir()}, {"skip.dat", "other.bin"})

    def test_streaming_retry_accepts_removed_root_only_with_complete_inventory(self):
        (self.source / "sample.dat").write_bytes(b"complete inventory")
        original = self.first_run()
        history = self.histories()[original]
        history["request"]["selection"] = None
        self.save(original, history)
        retried, _ = self.retry_run(original)
        self.assertEqual(retried["progress"]["already_moved"], 1)
        self.assertEqual(retried["progress"]["freed"], 0)
        history["progress"]["discovery_complete"] = False
        self.save(original, history)
        before = (original / "history.json").read_bytes()
        count = len(self.histories())
        result = self.command("retry", original.name, expected = 2)
        self.assertIn("inventory is incomplete", result.stderr)
        self.assertEqual(len(self.histories()), count)
        self.assertEqual((original / "history.json").read_bytes(), before)

    def test_symlink_move_retries_preserve_link_representation_on_each_route(self):
        for source_kind, target_kind in (
            ("lustre", "fss"), ("lustre", "alluxio"), ("alluxio", "fss"),
        ):
            with self.subTest(source_kind = source_kind, target_kind = target_kind):
                label = f"{source_kind}-{target_kind}"
                source = self.site.roots[source_kind] / label
                target = self.site.roots[target_kind] / label
                source.mkdir()
                if source_kind == "alluxio":
                    (source / "link.rclonelink").write_bytes(b"../referent")
                else:
                    (source / "link").symlink_to("../referent")
                before = set(self.histories())
                self.command("move", str(source), str(target), "--delete-source")
                original, = set(self.histories()) - before
                history, _ = self.retry_run(original)
                self.assertEqual(history["progress"]["already_moved"], 1)
                self.assertEqual(history["progress"]["freed"], 0)
                if target_kind == "alluxio":
                    self.assertEqual((target / "link.rclonelink").read_bytes(), b"../referent")
                else:
                    self.assertEqual(os.readlink(target / "link"), "../referent")

    def test_absent_source_destination_readback_holds_existing_lock_stripe(self):
        (self.source / "sample.dat").write_bytes(b"verify inside lock")
        original = self.first_run()
        request = retry.prepare(original, self.site)
        prior = request["retry_receipts"][str(self.source / "sample.dat")]
        task = {**self.task(self.source / "sample.dat"), "retry_receipt": prior}
        destination = self.target / "sample.dat"
        key = hashlib.sha256(os.fsencode(destination)).hexdigest()
        lock_path = self.home / "state/locks" / lock_stripe(key)
        actual_digest = engine.digest
        checked = []

        def inspect_lock(path):
            with lock_path.open("a+") as competing:
                with self.assertRaises(BlockingIOError):
                    fcntl.flock(competing, fcntl.LOCK_EX | fcntl.LOCK_NB)
            checked.append(path)
            return actual_digest(path)

        with patch.object(engine, "digest", side_effect = inspect_lock):
            outcome = engine.transfer(task)
        self.assertEqual(checked, [destination])
        self.assertTrue(outcome["already_moved"])
        self.assertEqual(outcome["freed_bytes"], 0)

    def test_retry_of_retry_after_device_change_preserves_both_histories(self):
        (self.source / "sample.dat").write_bytes(b"deleted only once")
        original = self.first_run()
        history = self.histories()[original]
        history["request"]["selection"][0]["fingerprint"][0] += 100
        for row in history["receipts"]:
            row["fingerprint"][0] += 200
        self.save(original, history)
        original_bytes = (original / "history.json").read_bytes()
        first, _ = self.retry_run(original)
        parent, = [
            path for path, saved in self.histories().items()
            if saved["request"].get("retry_from") == str(original)
        ]
        second, _ = self.retry_run(parent)
        for saved in (first, second):
            self.assertEqual(saved["progress"]["already_moved"], 1)
            self.assertEqual(saved["progress"]["freed"], 0)
        self.assertEqual((original / "history.json").read_bytes(), original_bytes)

    def test_recreated_deleted_source_is_refused_even_with_identical_content(self):
        content = b"same bytes, new source"
        source = self.source / "sample.dat"
        source.write_bytes(content)
        original = self.first_run()
        self.source.mkdir()
        source.write_bytes(content)
        history, result = self.retry_run(original, expected = 1)
        self.assertEqual(source.read_bytes(), content)
        self.assertEqual((self.target / source.name).read_bytes(), content)
        self.assertEqual(history["progress"]["files"], 0)
        self.assertIn("source changed or reappeared", result.stdout)

    def test_streaming_failed_file_disappearance_is_not_silently_omitted(self):
        source = self.source / "sample.dat"
        source.write_bytes(b"unresolved source")
        self.target.mkdir()
        (self.target / source.name).write_bytes(b"foreign conflict")
        original = self.first_run(expected = 1)
        history = self.histories()[original]
        history["request"]["selection"] = None
        self.save(original, history)
        source.unlink()
        history, result = self.retry_run(original, expected = 1)
        self.assertEqual(history["progress"]["files"], 0)
        self.assertEqual(history["progress"]["failed"], 1)
        self.assertIn("source disposition is unresolved", result.stdout)
        self.assertEqual((self.target / source.name).read_bytes(), b"foreign conflict")

    def test_missing_empty_directories_verify_destination_without_recreation(self):
        for nested, streaming in ((True, False), (False, False), (False, True)):
            with self.subTest(nested = nested, streaming = streaming):
                name = f"empty-{nested}-{streaming}"
                self.source = self.site.roots["lustre"] / name
                self.target = self.site.roots["fss"] / name
                self.source.mkdir()
                if nested:
                    (self.source / "empty-child").mkdir()
                before = set(self.histories())
                self.command("move", str(self.source), str(self.target))
                original, = set(self.histories()) - before
                if streaming:
                    history = self.histories()[original]
                    history["request"]["selection"] = None
                    self.save(original, history)
                self.assertFalse(self.source.exists())
                history, _ = self.retry_run(original)
                self.assertEqual(history["progress"]["failed"], 0)
                self.assertEqual(history["progress"]["freed"], 0)
                self.assertFalse(self.source.exists())
                self.assertTrue(self.target.is_dir())
                if nested:
                    self.assertTrue((self.target / "empty-child").is_dir())

    def test_device_only_change_reuses_current_source_without_copy_stream(self):
        source = self.source / "sample.dat"
        source.write_bytes(b"same shared inode on another mount")
        original = self.first_run("copy")
        history = self.histories()[original]
        history["request"]["selection"][0]["fingerprint"][0] += 100
        history["receipts"][0]["fingerprint"][0] += 200
        self.save(original, history)
        task = self.saved_task(original, source)
        with patch.object(engine, "copy_stream", side_effect = AssertionError("must reuse")):
            outcome = engine.transfer(task)
        self.assertTrue(outcome["reused"])
        self.assertFalse(outcome["deleted"])
        self.assertEqual(source.read_bytes(), (self.target / source.name).read_bytes())
        history, _ = self.retry_run(original)
        self.assertEqual(history["progress"]["reused"], 1)

    def test_same_content_replacement_inode_cannot_reuse_saved_verification(self):
        source = self.source / "sample.dat"
        source.write_bytes(b"matching contents")
        original = self.first_run("copy")
        source.rename(self.source / "original-inode")
        source.write_bytes(b"matching contents")
        history, result = self.retry_run(original, expected = 1)
        self.assertEqual(history["progress"]["files"], 0)
        self.assertIn("source changed", result.stdout)
        self.assertEqual(source.read_bytes(), b"matching contents")

    def test_changed_source_hash_with_unchanged_reported_identity_is_refused(self):
        source = self.source / "sample.dat"
        source.write_bytes(b"original")
        original = self.first_run("copy")
        task = self.saved_task(original, source)
        saved_identity = fingerprint(source)
        source.write_bytes(b"tampered")
        actual_fingerprint = engine.fingerprint

        def saved_source_identity(path):
            return saved_identity if Path(path) == source else actual_fingerprint(path)

        with (
            patch.object(engine, "fingerprint", side_effect = saved_source_identity),
            patch.object(engine, "copy_stream", side_effect = AssertionError("must refuse")),
        ):
            with self.assertRaisesRegex(ValueError, "saved verification"):
                engine.transfer(task)
        self.assertEqual(source.read_bytes(), b"tampered")
        self.assertEqual((self.target / source.name).read_bytes(), b"original")

    def test_owned_partial_journal_accepts_only_device_identity_changes(self):
        source = self.source / "sample.dat"
        source.write_bytes(b"finish interrupted shared file")
        self.target.mkdir()
        destination = self.target / source.name
        destination.write_bytes(b"foreign conflict")
        original = self.first_run(expected = 1)
        destination.unlink()

        def interrupted_copy(source, target, fd, *arguments):
            with os.fdopen(fd, "wb") as output:
                output.write(b"partial")
            raise OSError("injected interrupted copy")

        with patch.object(engine, "copy_stream", side_effect = interrupted_copy):
            with self.assertRaises(OSError):
                engine.transfer(self.task(source))
        journal, = (self.home / "state/pending").glob("*.json")
        saved = json.loads(journal.read_text())
        saved["fingerprint"][0] += 100
        saved["target_identity"][0] += 200
        journal.write_text(json.dumps(saved))
        history, _ = self.retry_run(original)
        self.assertEqual(destination.read_bytes(), b"finish interrupted shared file")
        self.assertFalse(source.exists())
        self.assertFalse(journal.exists())
        self.assertEqual(history["progress"]["files"], 1)

    def test_destination_mutation_during_readback_never_acknowledges_old_move(self):
        source = self.source / "sample.dat"
        source.write_bytes(b"original")
        original = self.first_run()
        task = self.saved_task(original, source)
        destination = self.target / source.name
        actual_digest = engine.digest

        def mutate_after_readback(path):
            value = actual_digest(path)
            path.write_bytes(b"changed during readback")
            return value

        with patch.object(engine, "digest", side_effect = mutate_after_readback):
            with self.assertRaisesRegex(ValueError, "changed or failed checksum"):
                engine.transfer(task)
        self.assertFalse(Path(task["receipt"]).exists())
        self.assertEqual(destination.read_bytes(), b"changed during readback")
        self.assertFalse(source.exists())


class RetryValidation(RetryFixture):
    def setUp(self):
        super().setUp()
        self.file = self.source / "sample.dat"
        self.file.write_bytes(b"saved source")
        self.target.mkdir()
        options = cli.parser().parse_args([
            "data", "move", str(self.source), str(self.target),
        ])
        request = cli.plan(options, self.site)
        identity = fingerprint(self.file)
        request.update(
            execution = "inline", reader = "native",
            selection = [{"path": str(self.file), "empty": False, "fingerprint": identity}],
        )
        self.row = {
            "event": "verified", "source": str(self.file),
            "destination": str(self.target / self.file.name),
            "fingerprint": identity, "bytes": self.file.stat().st_size,
            "sha256": hashlib.sha256(self.file.read_bytes()).hexdigest(),
            "kind": "file",
        }
        self.history = {
            "request": request,
            "progress": {
                "phase": "interrupted", "failed": 0,
                "workers_stopped": True, "hostname": "previous-other-host",
                "discovery_complete": True, "discovered_files": 1,
            },
            "receipts": [self.row],
        }
        self.run_dir = self.home / "runs/20261105T120000Z-123456789abc"
        self.run_dir.mkdir(parents = True)
        self.save(self.run_dir, self.history)

    def prepare(self, history):
        self.save(self.run_dir, history)
        return retry.prepare(self.run_dir, self.site)

    def test_active_or_unresolved_previous_workers_are_refused(self):
        changes = (
            {"phase": "running"}, {"phase": "saving history"},
            {"phase": "queued"}, {"workers_stopped": False}, {"failed": 1},
        )
        for change in changes:
            with self.subTest(change = change):
                history = copy.deepcopy(self.history)
                history["progress"].update(change)
                with self.assertRaisesRegex(ValueError, "active|uncertain|unresolved"):
                    self.prepare(history)
        history = copy.deepcopy(self.history)
        history["receipts"].append({
            "event": "failed", "error": "kernel I/O may still be active",
        })
        with self.assertRaisesRegex(ValueError, "unresolved"):
            self.prepare(history)

    def test_local_live_coordinator_is_refused_even_with_terminal_progress(self):
        self.history["progress"].update(
            hostname = socket.gethostname(), coordinator_pid = os.getpid(),
            coordinator_identity = cli.jobs.process_identity(os.getpid()),
        )
        with self.assertRaisesRegex(ValueError, "still active or uncertain"):
            self.prepare(self.history)

    def test_live_coordinator_without_saved_identity_is_uncertain(self):
        self.history["progress"].update(
            hostname = socket.gethostname(), coordinator_pid = os.getpid(),
        )
        with patch.object(cli.jobs, "process_identity", return_value = "known-current"):
            with self.assertRaisesRegex(ValueError, "still active or uncertain"):
                self.prepare(self.history)

    def test_legacy_worker_failures_require_explicit_stop_evidence(self):
        errors = (
            "transfer worker exited with code 1",
            "transfer worker returned no valid result",
            "interrupted or timed out: waiting for copy",
        )
        for error in errors:
            with self.subTest(error = error):
                history = copy.deepcopy(self.history)
                history["progress"]["failed"] = 1
                history["receipts"].append({
                    "event": "failed", "source": str(self.file), "error": error,
                })
                with self.assertRaisesRegex(ValueError, "unresolved"):
                    self.prepare(history)
                history["receipts"][-1]["workers_stopped"] = True
                prepared = self.prepare(history)
                self.assertEqual(prepared["source"], str(self.source))

    def test_streaming_discovery_directory_errors_respect_file_filters(self):
        nested = self.source / "nested"
        nested.mkdir()
        for path in (self.source, nested):
            with self.subTest(path = path):
                history = copy.deepcopy(self.history)
                history["request"].update(selection = None, include = ["*.dat"])
                history["progress"]["failed"] = 1
                history["receipts"].append({
                    "event": "failed", "source": str(path),
                    "error": "discovery failed: temporary directory failure",
                })
                prepared = self.prepare(history)
                discovered = list(retry.entries(prepared, self.site, None, lambda: None))
                paths = {
                    str(item.get("source", item.get("path")))
                    if isinstance(item, dict) else str(item[0])
                    for item in discovered
                }
                self.assertIn(str(self.file), paths)
                self.assertNotIn(str(path), prepared["retry_missing"])

    def test_absent_streaming_root_after_discovery_failure_is_not_empty_success(self):
        self.file.unlink()
        self.source.rmdir()
        self.history["request"].update(selection = None, include = ["*.dat"])
        self.history["progress"].update(
            failed = 1, discovery_complete = True, discovered_files = 0,
        )
        self.history["receipts"] = [{
            "event": "failed", "source": str(self.source),
            "error": "discovery failed: mount unavailable",
        }]
        self.save(self.run_dir, self.history)
        before = (self.run_dir / "history.json").read_bytes()
        result = self.command("retry", self.run_dir.name, expected = 2)
        self.assertIn("inventory is incomplete", result.stderr)
        self.assertEqual(list((self.home / "runs").iterdir()), [self.run_dir])
        self.assertEqual((self.run_dir / "history.json").read_bytes(), before)

    def test_split_receipts_are_read_incrementally(self):
        (self.run_dir / "history.json").unlink()
        for name in ("request", "progress"):
            (self.run_dir / f"{name}.json").write_text(json.dumps(self.history[name]))
        path = self.run_dir / "receipts.jsonl"
        path.write_text(json.dumps(self.row) + "\n")
        actual_read_text = Path.read_text

        def forbid_bulk_receipts(path, *arguments, **keywords):
            if path.name == "receipts.jsonl":
                raise AssertionError("receipt JSONL must be streamed")
            return actual_read_text(path, *arguments, **keywords)

        with patch.object(Path, "read_text", new = forbid_bulk_receipts):
            request, progress, rows = retry.load(self.run_dir)
            self.assertIs(iter(rows), rows)
            self.assertEqual(list(rows), [self.row])
            self.assertEqual(retry.prepare(self.run_dir, self.site)["source"], str(self.source))

    def test_malformed_and_mismatched_receipts_are_refused(self):
        changes = (
            {"fingerprint": [1, 2]}, {"bytes": True}, {"bytes": -1},
            {"bytes": self.row["bytes"] + 1}, {"sha256": "invalid"},
            {"kind": "symlink"}, {"destination": str(self.target / "other")},
            {"source": str(self.source.parent / "outside")},
        )
        for change in changes:
            with self.subTest(change = change):
                history = copy.deepcopy(self.history)
                history["receipts"][0].update(change)
                with self.assertRaises(ValueError):
                    self.prepare(history)
        for rows in ([None], [{"event": "verified"}]):
            with self.subTest(rows = rows):
                history = copy.deepcopy(self.history)
                history["receipts"] = rows
                with self.assertRaisesRegex(ValueError, "malformed"):
                    self.prepare(history)

    def test_conflicting_receipts_and_out_of_scope_selection_are_refused(self):
        history = copy.deepcopy(self.history)
        history["receipts"].append({**self.row, "sha256": "0" * 64})
        with self.assertRaisesRegex(ValueError, "conflicting source identities"):
            self.prepare(history)
        mutations = (
            {"include": ["*.txt"]}, {"exclude": ["*.dat"]},
            {"selection": []},
            {"selection": self.history["request"]["selection"] * 2},
            {"selection": [{"path": str(self.source.parent / "outside"), "empty": False}]},
        )
        for mutation in mutations:
            with self.subTest(mutation = mutation):
                history = copy.deepcopy(self.history)
                history["request"].update(mutation)
                with self.assertRaises(ValueError):
                    self.prepare(history)

    def test_saved_owner_route_delete_consent_and_formats_are_validated(self):
        mutations = (
            {"uid": os.getuid() + 1}, {"source_kind": "fss"},
            {"target_root": str(self.base)}, {"delete": "yes"},
            {"verb": "copy", "delete": True}, {"include": "*.dat"},
            {"archive_auto": True}, {"pack": "tar"}, {"chunk_size": 1024},
        )
        for mutation in mutations:
            with self.subTest(mutation = mutation):
                history = copy.deepcopy(self.history)
                history["request"].update(mutation)
                with self.assertRaises(ValueError):
                    self.prepare(history)

    def test_split_slurm_history_public_retry_submits_fresh_saved_request(self):
        self.history["request"]["execution"] = "slurm"
        (self.run_dir / "history.json").unlink()
        for name in ("request", "progress"):
            (self.run_dir / f"{name}.json").write_text(json.dumps(self.history[name]))
        (self.run_dir / "receipts.jsonl").write_text(json.dumps(self.row) + "\n")
        (self.run_dir / "slurm.json").write_text('{"job_id": "12345"}')
        before = {path.name: path.read_bytes() for path in self.run_dir.iterdir()}
        output = io.StringIO()
        with (
            patch.dict(os.environ, self.environment, clear = True),
            patch.object(sys, "argv", ["gbi", "data", "retry", "12345"]),
            patch.object(cli.jobs, "slurm_state", return_value = "FAILED") as state,
            patch.object(cli.jobs, "submit", return_value = "67890") as submit,
            redirect_stdout(output),
        ):
            self.assertEqual(cli.main(), 0)
        state.assert_called_once_with("12345")
        new_run, command, _site = submit.call_args.args
        self.assertNotEqual(new_run, self.run_dir)
        self.assertIn("_run", command)
        request = json.loads((new_run / "request.json").read_text())
        self.assertEqual(request["retry_from"], str(self.run_dir))
        self.assertEqual(request["execution"], "slurm")
        self.assertEqual(request["source"], str(self.source))
        self.assertEqual(before, {path.name: path.read_bytes() for path in self.run_dir.iterdir()})
        self.assertIn("Submitted transfer 67890", output.getvalue())

    def test_nonterminal_or_unknown_slurm_job_is_refused(self):
        self.history["request"]["execution"] = "slurm"
        with self.assertRaisesRegex(ValueError, "unconfirmed"):
            self.prepare(self.history)
        (self.run_dir / "slurm.json").write_text('{"job_id": "12345"}')
        for state in ("PENDING", "RUNNING", "COMPLETING", "UNKNOWN"):
            with self.subTest(state = state):
                with patch.object(cli.jobs, "slurm_state", return_value = state):
                    with self.assertRaisesRegex(ValueError, "confirmed terminal"):
                        self.prepare(self.history)

    def test_public_managed_retry_keeps_existing_submit_dispatch(self):
        for mode in ("prefect", "cache"):
            with self.subTest(mode = mode):
                request = {"execution": mode, "saved_request": "unchanged"}
                path = self.run_dir / "request.json"
                path.write_text(json.dumps(request))
                before = path.read_bytes()
                with (
                    patch.dict(os.environ, self.environment, clear = True),
                    patch.object(sys, "argv", ["gbi", "data", "retry", self.run_dir.name]),
                    patch.object(cli.prefect, "submit", return_value = 0) as submit,
                    patch.object(retry, "prepare") as prepare,
                ):
                    self.assertEqual(cli.main(), 0)
                submit.assert_called_once()
                self.assertEqual(submit.call_args.args[:2], (self.run_dir, ANY))
                prepare.assert_not_called()
                self.assertEqual(path.read_bytes(), before)
