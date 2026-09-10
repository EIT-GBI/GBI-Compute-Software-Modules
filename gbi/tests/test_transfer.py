"""Real local transfers plus fault injection at the source-deletion boundary."""

import hashlib
import json
import os
from pathlib import Path
import pwd
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from gbi_data import cli, jobs
from gbi_data import selection
from gbi_data.storage import Site, fingerprint
from gbi_data.transfer import copy_stream, transfer


class Transfers(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name).resolve()
        self.source_root = self.base / "source"
        self.target_root = self.base / "destination"
        self.state = self.base / "state"
        for path in (self.source_root, self.target_root, self.state / "locks", self.state / "pending"):
            path.mkdir(parents=True)
        self.source = self.source_root / "file.dat"
        self.target = self.target_root / "file.dat"
        self.source.write_bytes(os.urandom(1024 * 1024))
        self.task = {"source": str(self.source), "target": str(self.target),
                     "source_root": str(self.source_root), "target_root": str(self.target_root),
                     "selection_root": str(self.source_root), "state": str(self.state),
                     "receipt": str(self.base / "receipt.jsonl"), "progress": str(self.base / "progress.json"),
                     "rclone": shutil.which("rclone"), "settle_seconds": 1,
                     "target_kind": "fss", "delete": True, "encode_link": False, "decode_link": False}
        self.assertIsNotNone(self.task["rclone"], "tests require real rclone on PATH")

    def test_move_checksums_then_receipts_then_deletes(self):
        expected = hashlib.sha256(self.source.read_bytes()).hexdigest()
        outcome = transfer(self.task)
        self.assertTrue(outcome["deleted"])
        self.assertFalse(self.source.exists())
        self.assertEqual(hashlib.sha256(self.target.read_bytes()).hexdigest(), expected)
        rows = [json.loads(line) for line in Path(self.task["receipt"]).read_text().splitlines()]
        self.assertEqual([row["event"] for row in rows], ["verified", "deleted"])
        self.assertEqual(rows[0]["sha256"], expected)
        self.assertEqual(list((self.state / "pending").iterdir()), [])

    def test_copy_retains_source(self):
        transfer({**self.task, "delete": False})
        self.assertEqual(self.source.read_bytes(), self.target.read_bytes())

    def test_different_destination_is_never_overwritten(self):
        self.target.write_bytes(b"existing research")
        with self.assertRaisesRegex(ValueError, "both copies kept"):
            transfer(self.task)
        self.assertTrue(self.source.exists())
        self.assertEqual(self.target.read_bytes(), b"existing research")

    def test_identical_destination_reused_and_verified(self):
        shutil.copyfile(self.source, self.target)
        with patch("gbi_data.transfer.copy_stream", side_effect=AssertionError("must not recopy")):
            outcome = transfer(self.task)
        self.assertTrue(outcome["reused"])
        self.assertFalse(self.source.exists())

    def test_corrupt_write_retains_source(self):
        def corrupt(*args):
            value = copy_stream(*args)
            self.target.write_bytes(b"corrupt")
            return value
        with patch("gbi_data.transfer.copy_stream", side_effect=corrupt):
            with self.assertRaisesRegex(ValueError, "checksum"):
                transfer(self.task)
        self.assertTrue(self.source.exists())

    def test_successful_short_stream_cannot_delete_source(self):
        def short(source, target, fd, *args):
            os.close(fd)
            return hashlib.sha256(b"").hexdigest(), 0
        with patch("gbi_data.transfer.copy_stream", side_effect=short):
            with self.assertRaisesRegex(ValueError, "stream was short"):
                transfer(self.task)
        self.assertTrue(self.source.exists())
        self.assertFalse(Path(self.task["receipt"]).exists())

    def test_blocked_reader_times_out_without_source_deletion(self):
        blocked = self.base / "blocked-reader"
        blocked.write_text("#!/bin/sh\nexec sleep 60\n")
        blocked.chmod(0o700)
        result = cli.supervise({**self.task, "rclone": str(blocked)}, 0.2, threading.Event())
        self.assertIn("timed out", result["error"])
        self.assertTrue(self.source.exists())
        self.assertFalse(Path(self.task["receipt"]).exists())

    def test_same_size_source_change_retains_source(self):
        def change(*args):
            value = copy_stream(*args)
            self.source.write_bytes(b"x" * self.source.stat().st_size)
            return value
        with patch("gbi_data.transfer.copy_stream", side_effect=change):
            with self.assertRaisesRegex(ValueError, "source changed"):
                transfer(self.task)
        self.assertTrue(self.source.exists())

    def test_permission_repair_does_not_count_as_content_change(self):
        before = fingerprint(self.source)
        self.source.chmod(0o600)
        self.assertEqual(fingerprint(self.source), before)

    def test_same_size_same_timestamp_source_edit_retains_source(self):
        def change(*args):
            original = self.source.stat()
            value = copy_stream(*args)
            self.source.write_bytes(b"x" * original.st_size)
            os.utime(self.source, ns=(original.st_atime_ns, original.st_mtime_ns))
            return value
        with patch("gbi_data.transfer.copy_stream", side_effect=change):
            with self.assertRaisesRegex(ValueError, "source content changed"):
                transfer(self.task)
        self.assertTrue(self.source.exists())

    def test_no_receipt_means_no_deletion(self):
        with patch("gbi_data.transfer.receipt", side_effect=OSError("scratch unavailable")):
            with self.assertRaises(OSError):
                transfer(self.task)
        self.assertTrue(self.source.exists())
        self.assertEqual(self.source.read_bytes(), self.target.read_bytes())

    def test_native_copy_keeps_source_when_receipt_fails(self):
        with patch("gbi_data.transfer.receipt", side_effect=OSError("scratch unavailable")):
            with self.assertRaises(OSError):
                transfer({**self.task, "rclone": None})
        self.assertEqual(self.source.read_bytes(), self.target.read_bytes())

    def test_changed_small_selection_cannot_start_copying(self):
        original = fingerprint(self.source)
        self.source.write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "changed since the foreground probe"):
            transfer({**self.task, "rclone": None, "expected_source": original})
        self.assertFalse(self.target.exists())
        self.assertEqual(self.source.read_bytes(), b"changed")

    def test_growing_native_stream_stops_at_the_small_transfer_budget(self):
        def grow(*args):
            self.source.write_bytes(b"x" * 2 * 1024 * 1024)
            return copy_stream(*args)
        with patch("gbi_data.transfer.copy_stream", side_effect=grow):
            with self.assertRaisesRegex(ValueError, "grew beyond"):
                transfer({**self.task, "rclone": None, "max_bytes": 1024 * 1024})
        self.assertTrue(self.source.exists())
        self.assertLessEqual(self.target.stat().st_size, 1024 * 1024)

    def test_partial_retry_replaces_only_owned_output(self):
        def partial(source, target, fd, *args):
            with os.fdopen(fd, "wb") as output:
                output.write(b"partial")
            raise OSError("interrupted")
        with patch("gbi_data.transfer.copy_stream", side_effect=partial):
            with self.assertRaises(OSError):
                transfer(self.task)
        self.assertTrue(self.source.exists())
        transfer(self.task)
        self.assertFalse(self.source.exists())
        self.assertEqual(self.target.stat().st_size, 1024 * 1024)

    def test_changed_source_cannot_authorize_retry_overwrite(self):
        def partial(source, target, fd, *args):
            os.close(fd)
            raise OSError("interrupted")
        with patch("gbi_data.transfer.copy_stream", side_effect=partial):
            with self.assertRaises(OSError):
                transfer(self.task)
        self.source.write_bytes(b"new version")
        with self.assertRaisesRegex(ValueError, "both copies kept"):
            transfer(self.task)
        self.assertEqual(self.target.read_bytes(), b"")

    def test_destination_symlink_is_preserved(self):
        other = self.base / "other"
        other.write_bytes(b"do not touch")
        self.target.symlink_to(other)
        with self.assertRaises(ValueError):
            transfer(self.task)
        self.assertEqual(other.read_bytes(), b"do not touch")

    def test_parent_symlink_does_not_create_outside_directories(self):
        other = self.base / "other"
        other.mkdir()
        (self.target_root / "link").symlink_to(other)
        with self.assertRaises(ValueError):
            transfer({**self.task, "target": str(self.target_root / "link" / "new" / "file")})
        self.assertEqual(list(other.iterdir()), [])

    def test_native_symlink_is_not_followed(self):
        self.source.unlink()
        self.source.symlink_to("/an/absent/target")
        transfer(self.task)
        self.assertEqual(os.readlink(self.target), "/an/absent/target")
        self.assertFalse(os.path.lexists(self.source))

    def test_symlink_roundtrip_through_object_representation(self):
        self.source.unlink()
        self.source.symlink_to("../original")
        stored = self.target.with_name("file.dat.rclonelink")
        transfer({**self.task, "target": str(stored), "target_kind": "alluxio", "encode_link": True})
        self.assertEqual(stored.read_text(), "../original")
        restored = self.source_root / "restored"
        transfer({**self.task, "source": str(stored), "source_root": str(self.target_root),
                  "target": str(restored), "target_root": str(self.source_root),
                  "selection_root": str(self.target_root), "decode_link": True, "delete": False})
        self.assertTrue(stored.exists())
        self.assertEqual(os.readlink(restored), "../original")


class Interface(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name).resolve()
        self.user = pwd.getpwuid(os.getuid()).pw_name
        self.conf = self.base / "site.conf"
        roots = {name: self.base / name for name in ("lustre", "fss", "bucket")}
        for root in roots.values():
            (root / self.user).mkdir(parents=True)
        self.conf.write_text("".join(f"{name}_root = {root}\n" for name, root in roots.items()) + "partition = test\n")
        self.site = Site(self.conf)

    def test_small_file_all_routes_needs_neither_slurm_nor_rclone(self):
        environment = {**os.environ, "GBI_DATA_SITE_CONF": str(self.conf), "PATH": ""}
        environment.pop("SLURM_JOB_ID", None)
        for source_kind, source_root in self.site.roots.items():
            for target_kind, target_root in self.site.roots.items():
                if source_kind == target_kind:
                    continue
                source = source_root / f"small-to-{target_kind}"
                source.write_bytes(os.urandom(5000))
                expected = source.read_bytes()
                target = target_root / f"small-from-{source_kind}"
                result = subprocess.run([sys.executable, "-B", "-m", "gbi_data.cli", "data", "move",
                                         str(source), str(target)], env=environment,
                                        capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertIn("Foreground transfer", result.stdout)
                self.assertNotIn("Submitted transfer", result.stdout)
                self.assertEqual(target.read_bytes(), expected)
                self.assertEqual(source.exists(), source_kind == "alluxio")
        histories = list((self.site.roots["alluxio"] / ".gbi/transfers").glob("*/*/history.json"))
        self.assertEqual(len(histories), 6)
        for path in histories:
            record = json.loads(path.read_text())
            self.assertEqual(record["request"]["execution"], "inline")
            self.assertEqual(record["progress"]["phase"], "complete")
            self.assertEqual(record["receipts"][0]["reader"], "native")
            status = subprocess.run([sys.executable, "-B", "-m", "gbi_data.cli", "data", "status",
                                     path.parent.name], env=environment, capture_output=True, text=True, timeout=10)
            self.assertEqual(status.returncode, 0, status.stdout + status.stderr)
            self.assertIn("Complete", status.stdout)
        self.assertEqual(list((self.site.roots["lustre"] / ".gbi/runs").iterdir()), [])

    def test_large_and_explicit_background_requests_submit_without_moving(self):
        binary = self.base / "bin"
        binary.mkdir()
        sbatch = binary / "sbatch"
        sbatch.write_text("#!/bin/sh\nprintf '314159\\n'\n")
        sbatch.chmod(0o700)
        environment = {**os.environ, "GBI_DATA_SITE_CONF": str(self.conf), "PATH": str(binary)}
        environment.pop("SLURM_JOB_ID", None)
        for name, size, flags in (("large", 8589934593, []), ("background", 5000, ["--detach"])):
            source = self.site.roots["lustre"] / name
            with source.open("wb") as stream:
                stream.truncate(size)
            result = subprocess.run([sys.executable, "-B", "-m", "gbi_data.cli", "data", "move",
                                     str(source), str(self.site.roots["alluxio"] / name), *flags],
                                    env=environment, capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("Submitted transfer 314159", result.stdout)
            self.assertEqual(source.stat().st_size, size)
        for path in (self.site.roots["lustre"] / ".gbi/runs").glob("*/request.json"):
            record = json.loads(path.read_text())
            self.assertEqual(record["execution"], "slurm")
            self.assertIsNone(record["selection"])

    def test_probe_bounds_file_count_and_excluded_entry_scan(self):
        self.conf.write_text(self.conf.read_text() + "inline_scan_entries = 32\n")
        self.site = Site(self.conf)
        source = self.site.roots["lustre"] / "tree"
        source.mkdir()
        for number in range(33):
            (source / str(number)).touch()
        options = cli.parser().parse_args(["data", "move", str(source), str(self.site.roots["alluxio"] / "tree")])
        specification = cli.plan(options, self.site)
        self.assertIsNone(selection.probe(specification, self.site))
        self.site.values["inline_scan_entries"] = "20"
        with self.assertRaisesRegex(ValueError, "larger scan"):
            selection.foreground_selection({**specification, "include": ["*.pt"]}, self.site)

    def test_foreground_engine_and_slurm_threshold_are_independent(self):
        source = self.site.roots["lustre"] / "file"
        for destination_kind in ("fss", "alluxio"):
            for size, mode, reader in ((5000, "inline", "native"),
                                       (9 * 1024**2, "inline", "rclone"),
                                       (2 * 1024**3, "inline", "rclone"),
                                       (8 * 1024**3, "inline", "rclone"),
                                       (8 * 1024**3 + 1, "slurm", "rclone")):
                with source.open("wb") as stream:
                    stream.truncate(size)
                options = cli.parser().parse_args(["data", "move", str(source),
                                                  str(self.site.roots[destination_kind] / "destination")])
                with patch.dict(os.environ, {key: value for key, value in os.environ.items()
                                            if key != "SLURM_JOB_ID"}, clear=True):
                    planned = cli.execution_plan(cli.plan(options, self.site), self.site, options)
                self.assertEqual((planned["execution"], planned["reader"]), (mode, reader))

    def test_everyday_directory_uses_parallel_rclone_without_slurm(self):
        source = self.site.roots["lustre"] / "everyday"
        source.mkdir()
        for number in range(40):
            (source / str(number)).write_bytes(os.urandom(1000))
        target = self.site.roots["alluxio"] / "everyday"
        environment = {**os.environ, "GBI_DATA_SITE_CONF": str(self.conf)}
        environment.pop("SLURM_JOB_ID", None)
        result = subprocess.run([sys.executable, "-B", "-m", "gbi_data.cli", "data", "move",
                                 str(source), str(target)], env=environment,
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Foreground transfer (rclone", result.stdout)
        self.assertNotIn("Submitted transfer", result.stdout)
        history = next((self.site.roots["alluxio"] / ".gbi/transfers").glob("*/*/history.json"))
        bundle = json.loads(history.read_text())
        self.assertEqual(bundle["progress"]["files"], 40)
        self.assertTrue(all(record["reader"] == "rclone" for record in bundle["receipts"]
                            if record["event"] == "verified"))
        self.assertEqual(len(list(target.iterdir())), 40)

    def test_probe_timeout_falls_back_without_writing(self):
        reader = self.base / "slow-probe"
        reader.write_text("#!/bin/sh\nexec sleep 60\n")
        reader.chmod(0o700)
        self.site.values["inline_probe_seconds"] = "0.05"
        started = time.monotonic()
        with patch("gbi_data.selection.sys.executable", str(reader)):
            self.assertIsNone(selection.probe({}, self.site))
        self.assertLess(time.monotonic() - started, 2)
        self.assertFalse((self.site.roots["lustre"] / ".gbi").exists())

    def test_existing_allocation_does_not_submit_another_job(self):
        options = cli.parser().parse_args(["data", "copy", "source", "destination"])
        with patch.dict(os.environ, {"SLURM_JOB_ID": "42"}), patch("gbi_data.cli.probe", return_value=None):
            self.assertEqual(cli.execution_plan({}, self.site, options)["execution"], "allocation")

    def test_all_six_directions_and_object_deletion_policy(self):
        for source_kind, source_root in self.site.roots.items():
            source = source_root / "data"
            source.mkdir(exist_ok=True)
            for target_kind, target_root in self.site.roots.items():
                if source_kind == target_kind:
                    continue
                options = cli.parser().parse_args(["data", "move", str(source), str(target_root / "dest")])
                planned = cli.plan(options, self.site)
                self.assertEqual(planned["delete"], source_kind != "alluxio")
                options.delete_source = True
                self.assertTrue(cli.plan(options, self.site)["delete"])

    def test_source_overlap_refused(self):
        source = self.site.roots["lustre"] / "data"
        source.mkdir()
        options = cli.parser().parse_args(["data", "move", str(source), str(source / "child")])
        with self.assertRaisesRegex(ValueError, "overlap"):
            cli.plan(options, self.site)

    def test_individual_user_root_alias_is_resolved(self):
        aliases = self.site.roots.copy()
        for kind, alias in aliases.items():
            physical = self.base / f"physical-{kind}-root"
            alias.rename(physical)
            alias.symlink_to(physical)
        site = Site(self.conf)
        result = subprocess.run([sys.executable, "-B", "-m", "gbi_data.cli", "data", "roots"],
                                env={**os.environ, "GBI_DATA_SITE_CONF": str(self.conf)},
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "".join(f"{kind:8} {alias}\n" for kind, alias in aliases.items()))
        for kind, alias in aliases.items():
            physical = self.base / f"physical-{kind}-root"
            self.assertEqual(site.classify(alias), (physical, kind, physical))
            self.assertEqual(site.classify(alias / "data"), (physical / "data", kind, physical))
            # A final symlink remains data; traversing it cannot escape the root.
            (alias / "link").symlink_to(self.base)
            self.assertEqual(site.classify(alias / "link"), (physical / "link", kind, physical))
            with self.assertRaisesRegex(ValueError, "outside your configured storage roots"):
                site.classify(alias / "link" / "outside")

    def test_reserved_paths_pruned_and_filters_do_not_match_ptx(self):
        root = self.site.roots["lustre"]
        for name in ("file.pt", "file.pt.ready.json", "file.ptx"):
            (root / name).write_text("x")
        (root / ".gbi").mkdir()
        (root / ".gbi" / "file.pt").write_text("x")
        found = list(cli.entries(root, self.site, root, ["*.pt", "*.pt.*"]))
        self.assertEqual({p.name for p, _ in found}, {"file.pt", "file.pt.ready.json"})

    def test_progress_has_no_percentage_until_discovery_complete(self):
        progress = {"files": 2, "bytes": 20, "freed": 10, "failed": 0,
                    "elapsed": 2, "discovered_bytes": 40, "phase": "running", "discovery_complete": False}
        self.assertNotIn("%", jobs.display(progress))
        progress["discovery_complete"] = True
        self.assertIn("50.0%", jobs.display(progress))
        progress["phase"] = "saving history"
        self.assertIn("Saving history", jobs.terminal_display(progress))

    def test_full_cli_all_routes_archives_history_and_cleans_scratch(self):
        environment = {**os.environ, "GBI_DATA_SITE_CONF": str(self.conf)}
        number = 100
        for source_kind, source_root in self.site.roots.items():
            for target_kind, target_root in self.site.roots.items():
                if source_kind == target_kind:
                    continue
                number += 1
                environment["SLURM_JOB_ID"] = str(number)
                source = source_root / f"source-{number}"
                source.mkdir()
                (source / "a file\nwith whitespace").write_bytes(b"preserve these bytes")
                target = target_root / f"target-{number}"
                command = [sys.executable, "-B", "-m", "gbi_data.cli", "data", "move",
                           str(source), str(target), "--local"]
                result = subprocess.run(command, env=environment, capture_output=True, text=True, timeout=30)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertEqual((target / "a file\nwith whitespace").read_bytes(), b"preserve these bytes")
                self.assertEqual((source / "a file\nwith whitespace").exists(), source_kind == "alluxio")
                history = list((self.site.roots["alluxio"] / ".gbi" / "transfers" / str(number)).iterdir())
                self.assertEqual(len(history), 1)
                progress = json.loads((history[0] / "progress.json").read_text())
                self.assertEqual(progress["phase"], "complete")
                self.assertEqual(progress["files"], 1)
                scratch = self.site.roots["lustre"] / ".gbi" / "runs"
                self.assertEqual(list(scratch.iterdir()), [])
                self.assertFalse((self.site.roots["fss"] / ".gbi").exists())
                status = subprocess.run([sys.executable, "-B", "-m", "gbi_data.cli", "data", "status", str(number)],
                                        env=environment, capture_output=True, text=True, timeout=10)
                self.assertEqual(status.returncode, 0, status.stdout + status.stderr)
                self.assertIn("Complete", status.stdout)


if __name__ == "__main__":
    unittest.main()
