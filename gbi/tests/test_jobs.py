"""Transfer lookup stays unambiguous inside reused Slurm allocations."""

import json
import io
import os
from pathlib import Path
import shutil
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from gbi_data import jobs


class JobLookup(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.home = self.base / "home"
        self.runs = self.home / "runs"
        self.runs.mkdir(parents = True)
        self.archive = self.base / "archive"

    def active(self, name, job_id):
        run = self.runs / name
        run.mkdir()
        (run / "request.json").write_text(
            json.dumps({"execution": "allocation", "allocation_job_id": job_id})
        )
        return run

    def archived(self, job_id, name):
        run = self.archive / ".gbi" / "transfers" / job_id / name
        run.mkdir(parents = True)
        (run / "history.json").write_text("{}")
        return run

    def test_active_allocation_wins_over_older_archived_transfer(self):
        old     = self.archived("314159", "20260925T000000Z-aaaaaaaaaaaa")
        current = self.active("20260925T010000Z-bbbbbbbbbbbb", "314159")
        self.assertEqual(
            jobs.locate(self.home, "314159", self.archive), current
        )
        self.assertNotEqual(current, old)

    def test_multiple_active_transfers_require_transfer_id(self):
        self.active("20260925T010000Z-aaaaaaaaaaaa", "314159")
        self.active("20260925T010001Z-bbbbbbbbbbbb", "314159")
        with self.assertRaisesRegex(ValueError, "multiple active transfers"):
            jobs.locate(self.home, "314159", self.archive)

    def test_multiple_archived_transfers_require_transfer_id(self):
        self.archived("314159", "20260925T000000Z-aaaaaaaaaaaa")
        self.archived("314159", "20260925T000001Z-bbbbbbbbbbbb")
        with self.assertRaisesRegex(ValueError, "multiple transfers"):
            jobs.locate(self.home, "314159", self.archive)

    def test_archived_allocation_transfer_id_resolves_after_local_cleanup(self):
        transfer_id = "20260925T000000Z-aaaaaaaaaaaa"
        published   = self.archived("314159", transfer_id)
        self.assertEqual(
            jobs.locate(self.home, transfer_id, self.archive), published
        )

    def test_malformed_request_ids_are_ignored(self):
        run = self.runs / "malformed"
        run.mkdir()
        (run / "request.json").write_text(json.dumps({"allocation_job_id": []}))
        (run / "slurm.json").write_text(json.dumps({"job_id": {}}))
        with self.assertRaisesRegex(ValueError, "no transfer record"):
            jobs.locate(self.home, "314159", self.archive)

    def test_exact_transfer_watch_survives_publication_from_local_allocation(
                self,
            ):
        for with_allocation_id in (True, False):
            with self.subTest(with_allocation_id = with_allocation_id):
                transfer_id = "20260925T020000Z-aaaaaaaaaaaa"
                run         = self.runs / transfer_id
                run.mkdir()
                request = {"execution": "allocation"}
                if with_allocation_id:
                    request["allocation_job_id"] = "314159"
                (run / "request.json").write_text(json.dumps(request))
                published = (
                    self.archive / ".gbi" / "transfers" / "314159" / transfer_id
                )
                progress = {
                    "phase": "complete", "files": 1, "bytes": 4, "freed": 0,
                    "failed": 0, "elapsed": 1,
                }

                observed_paths = []

                def publish(current):
                    observed_paths.append(current)
                    if current == run:
                        published.mkdir(parents = True, exist_ok = True)
                        (published / "progress.json").write_text(
                            json.dumps(progress)
                        )
                        shutil.rmtree(run)
                        return dict(progress, phase = "saving history")
                    self.assertEqual(current, published)
                    return progress

                output = io.StringIO()
                with (
                    patch("gbi_data.jobs.read_progress", side_effect = publish),
                    patch("gbi_data.jobs.time.sleep"),
                    patch(
                        "gbi_data.jobs.slurm_state", return_value = "RUNNING"
                    ) as state, patch("sys.stdout", output),
                ):
                    result = jobs.watch(
                        run, transfer_id, follow = True, archive_root = self.archive
                    )
                self.assertEqual(result, 0)
                self.assertEqual(observed_paths, [run, published])
                if with_allocation_id:
                    state.assert_called_once_with("314159")
                else:
                    state.assert_not_called()
                self.assertTrue((published / "progress.json").exists())

    def test_exact_transfer_watch_polls_recorded_allocation_job_for_terminal_failure(
                self,
            ):
        transfer_id = "20260925T020001Z-bbbbbbbbbbbb"
        run         = self.active(transfer_id, "314159")
        progress = {
            "phase": "running", "files": 1, "bytes": 4, "freed": 0, "failed": 0,
            "elapsed": 1,
        }
        output = io.StringIO()
        with (
            patch("gbi_data.jobs.read_progress", return_value = progress),
            patch("gbi_data.jobs.slurm_state", return_value = "FAILED") as state,
            patch("gbi_data.jobs.time.sleep"), patch("sys.stdout", output),
        ):
            result = jobs.watch(
                run, transfer_id, follow = True, archive_root = self.archive
            )
        self.assertEqual(result, 1)
        state.assert_called_once_with("314159")

    def test_submitted_job_is_not_its_parent_allocation(self):
        run = self.active("20260925T010000Z-bbbbbbbbbbbb", "314159")
        (run / "slurm.json").write_text(json.dumps({"job_id": "271828"}))
        self.assertEqual(jobs.locate(self.home, "271828", self.archive), run)
        with self.assertRaisesRegex(ValueError, "no transfer record"):
            jobs.locate(self.home, "314159", self.archive)

    def test_progress_reports_reuse_without_inventing_older_history_counts(self):
        progress = {
            "phase": "failed", "files": 2, "bytes": 20, "freed": 10,
            "failed": 1, "elapsed": 2,
        }
        for display in (jobs.display, jobs.terminal_display):
            with self.subTest(display = display.__name__):
                self.assertNotIn("reused", display(progress))
                self.assertIn("(1 reused)", display({**progress, "reused": 1}))
                self.assertIn("failed 1", display({**progress, "reused": 1}))


class Submission(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.run_dir = Path(temporary.name)
        self.site = SimpleNamespace(values = {
            "partition": "cpu", "time_limit": "01:00:00", "cpus": "1", "mem": "1G",
        })

    def test_missing_sbatch_gives_guidance_without_attempting_submission(self):
        with (
            patch.dict(os.environ, {"PATH": ""}),
            patch("gbi_data.jobs.subprocess.run") as run,
            self.assertRaisesRegex(ValueError, "Slurm login node.*omit --detach.*No job was submitted"),
        ):
            jobs.submit(self.run_dir, ["gbi", "data", "copy"], self.site)
        run.assert_not_called()
        self.assertFalse((self.run_dir / "job.sh").exists())
        self.assertFalse((self.run_dir / "slurm.json").exists())

    def test_existing_sbatch_path_submits_and_preserves_other_os_errors(self):
        executable = self.run_dir / "sbatch"
        executable.write_text("#!/bin/sh\nprintf '314159;cluster\\n'\n")
        executable.chmod(0o700)
        with patch.dict(os.environ, {"PATH": str(self.run_dir)}):
            self.assertEqual(jobs.submit(self.run_dir, ["gbi", "data", "copy"], self.site), "314159")
            self.assertEqual(json.loads((self.run_dir / "slurm.json").read_text()), {"job_id": "314159"})
            error = PermissionError("submission permission denied")
            with (
                patch("gbi_data.jobs.subprocess.run", side_effect = error),
                self.assertRaises(PermissionError) as raised,
            ):
                jobs.submit(self.run_dir, ["gbi", "data", "copy"], self.site)
            self.assertIs(raised.exception, error)
