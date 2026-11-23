import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from gbi_data import cli, dataset

ADDRESS = ("bento://us-ashburn-1/psto-iad-drplab-allthebacteria-bucket@lr40ytmfucrr/"
           "allthebacteria/allthebacteria_base@2026-08-28a")
RUN = "11111111-2222-3333-4444-555555555555"


class Site:
    def __init__(self, root):
        self.values = {"bento_root": str(root)}


def options(verb = "get", address = ADDRESS, json_output = False):
    return SimpleNamespace(dataset_verb = verb, address = address, json = json_output)


def run(site, opts, broker = None):
    out, err = io.StringIO(), io.StringIO()
    calls = []

    def exchange(site, request, retry_hint = None):
        calls.append(request)
        return broker(request) if broker else {"version": 1, "state": "SCHEDULED", "run_id": RUN}

    with patch.object(dataset.prefect, "exchange", exchange), redirect_stdout(out), redirect_stderr(err):
        code = dataset.main(opts, site)
    return code, out.getvalue(), err.getvalue(), calls


class DatasetTest(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.site = Site(self.root)
        self.path = self.root / "allthebacteria" / "allthebacteria_base" / "2026-08-28a"

    def test_held_version_prints_its_path_without_asking_the_broker(self):
        (self.path / "_bento").mkdir(parents = True)
        (self.path / "_bento" / "ready.json").write_text(json.dumps({"files": 1}))
        code, out, _, calls = run(self.site, options())
        self.assertEqual((code, out.strip(), calls), (0, str(self.path), []))

    def test_missing_version_starts_the_shared_copy_and_prints_run_and_path(self):
        code, out, _, calls = run(self.site, options())
        self.assertEqual(code, 3)
        self.assertEqual(calls, [{"version": 1, "action": "submit",
                                  "operation": "bento-ingest", "address": ADDRESS}])
        self.assertIn(RUN, out)
        self.assertIn(str(self.path), out)

    def test_status_never_submits(self):
        code, out, _, calls = run(self.site, options("status"),
                                  lambda request: {"version": 1, "state": "NOT_SUBMITTED"})
        self.assertEqual((code, calls[0]["action"]), (3, "status"))
        self.assertIn("gbi dataset get", out)

    def test_a_failed_copy_exits_one_and_says_how_to_resume(self):
        code, _, err, _ = run(self.site, options(), lambda request: {
            "version": 1, "state": "FAILED", "run_id": RUN, "message": "3 files failed"})
        self.assertEqual(code, 1)
        self.assertIn("3 files failed", err)

    def bilbo(self, status, **changes):
        row = {"status": status, "flow_run_id": RUN, "files_copied": 7577078,
               "bytes_copied": 19_600_000_000_000, "file_count": 23237657,
               "size_bytes": 61089243350360, "mount_available": False,
               "mount_path": str(self.path) + "/", "requested_by": "gbi:christiaan-viljoen",
               "last_error": None}
        row.update(changes)
        return lambda request: {"version": 1, "state": "RUNNING", "run_id": RUN, "bilbo": row}

    def test_status_reports_bilbo_progress_owner_and_requester(self):
        code, out, _, calls = run(self.site, options("status"), self.bilbo("copying"))
        self.assertEqual((code, calls[0]["action"]), (3, "status"))
        self.assertIn("32% done (7.6 M of 23.2 M files, 19.6 of 61.1 TB)", out)
        self.assertIn(RUN, out)
        self.assertIn("requested by gbi:christiaan-viljoen", out)

    def test_ready_prints_bilbos_path_only_once_the_path_is_verified(self):
        code, out, _, _ = run(self.site, options("status"), self.bilbo("ready"))
        self.assertEqual(code, 3)
        self.assertIn("still being verified", out)
        self.assertNotEqual(out.strip(), str(self.path))
        code, out, _, _ = run(self.site, options("status"), self.bilbo("ready", mount_available = True))
        self.assertEqual((code, out.strip()), (0, str(self.path)))

    def test_a_permanent_failure_never_suggests_running_get_again(self):
        code, _, err, _ = run(self.site, options("status"), self.bilbo(
            "failed", last_error = "[permanent] ManifestError: duplicate source paths"))
        self.assertEqual(code, 1)
        self.assertIn("duplicate source paths", err)
        self.assertNotIn("gbi dataset get", err)
        self.assertIn("send the run ID to Scientific Computing", err)
        self.assertIn("cannot be copied as published: duplicate source paths", err)

    def test_a_transient_failure_says_it_is_retried_automatically(self):
        _, _, err, _ = run(self.site, options("status"), self.bilbo(
            "failed", last_error = "[transient] after 3 attempts: OSError"))
        self.assertIn("retried automatically", err)

    def test_a_manifest_fault_from_prefect_alone_never_suggests_resuming(self):
        _, _, err, _ = run(self.site, options("status"), lambda request: {
            "version": 1, "state": "FAILED", "run_id": RUN,
            "message": "Flow run encountered an exception: ManifestError: path listed twice"})
        self.assertNotIn("to resume", err)

    def test_an_unreadable_manifest_points_at_the_address(self):
        code, _, err, _ = run(self.site, options("status"), lambda request: {
            "version": 1, "state": "FAILED", "run_id": RUN,
            "message": "Flow run encountered an exception: ManifestError: Bento has no readable "
                       "manifest for bento://x: the address may be wrong"})
        self.assertEqual(code, 1)
        self.assertIn("Check the address", err)
        self.assertIn("Artefacts tab", err)
        self.assertNotIn("Flow run encountered", err)

    def test_get_explains_when_the_broker_starts_no_new_run(self):
        code, _, err, calls = run(self.site, options(), lambda request: {
            "version": 1, "state": "FAILED", "run_id": RUN, "resubmit": "refused",
            "message": "Flow run encountered an exception: ManifestError: manifest lists 'a' twice"})
        self.assertEqual((code, calls[0]["action"]), (1, "submit"))
        self.assertIn("No new copy was started.", err)
        self.assertIn("manifest lists 'a' twice", err)

    def test_a_crash_without_a_bilbo_row_says_how_to_resume(self):
        _, _, err, _ = run(self.site, options("status"), lambda request: {
            "version": 1, "state": "CRASHED", "run_id": RUN, "message": "pod evicted"})
        self.assertIn("Files already copied are kept. Resume it with: gbi dataset get", err)

    def test_queued_and_running_before_registration_say_what_is_happening(self):
        _, out, _, _ = run(self.site, options("status"), lambda request: {
            "version": 1, "state": "PENDING", "run_id": RUN})
        self.assertIn("Starting the copy", out)
        _, out, _, _ = run(self.site, options("status"), lambda request: {
            "version": 1, "state": "RUNNING", "run_id": RUN})
        self.assertIn("Checking Bento's manifest", out)

    def test_the_cli_never_reads_bilbo_itself(self):
        source = Path(dataset.__file__).read_text()
        self.assertNotIn("urlopen", source)
        self.assertNotIn("dataset_register_url", source)

    def test_json_output_carries_state_path_and_address(self):
        _, out, _, _ = run(self.site, options(json_output = True))
        result = json.loads(out)
        self.assertEqual((result["state"], result["path"], result["address"]),
                         ("SCHEDULED", str(self.path), ADDRESS))

    def test_names_and_partial_addresses_are_refused_before_the_broker(self):
        for text in ("allthebacteria", ADDRESS.replace("@2026-08-28a", ""), ADDRESS + "/x",
                     ADDRESS.replace("allthebacteria/", "../", 1)):
            code, _, err, calls = run(self.site, options(address = text))
            self.assertEqual((code, calls), (2, []))
            self.assertIn("located Bento address", err)

    def test_broker_refusal_is_reported(self):
        def refuse(request):
            raise ValueError("Bento ingestion is paused")
        code, _, err, _ = run(self.site, options(), refuse)
        self.assertEqual(code, 2)
        self.assertIn("paused", err)

    def test_parser_accepts_dataset_get_and_status(self):
        parsed = cli.parser().parse_args(["dataset", "get", ADDRESS, "--json"])
        self.assertEqual((parsed.command, parsed.dataset_verb, parsed.address, parsed.json),
                         ("dataset", "get", ADDRESS, True))
        self.assertEqual(cli.parser().parse_args(["dataset", "status", ADDRESS]).dataset_verb, "status")


if __name__ == "__main__":
    unittest.main()
