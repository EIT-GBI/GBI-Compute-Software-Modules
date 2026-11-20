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
        self.assertIn("7,577,078/23,237,657 files", out)
        self.assertIn("19.60 TB of 61.09 TB (32.1%)", out)
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
        self.assertIn("contact Scientific Computing", err)

    def test_a_transient_failure_says_it_is_retried_automatically(self):
        _, _, err, _ = run(self.site, options("status"), self.bilbo(
            "failed", last_error = "[transient] after 3 attempts: OSError"))
        self.assertIn("retried automatically", err)

    def test_a_manifest_fault_from_prefect_alone_never_suggests_resuming(self):
        _, _, err, _ = run(self.site, options("status"), lambda request: {
            "version": 1, "state": "FAILED", "run_id": RUN,
            "message": "Flow run encountered an exception: ManifestError: path listed twice"})
        self.assertNotIn("to resume", err)

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
