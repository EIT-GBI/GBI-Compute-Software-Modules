"""Exercise SDK archive/restore in one Slurm allocation using fresh own fixtures."""

import argparse
import json
import os
import subprocess
import uuid

from gbi import data
from gbi_data.storage import Site
from cluster_selftest import digest


def main():
    job_id = os.environ.get("SLURM_JOB_ID")
    if not job_id:
        raise SystemExit("run inside a Slurm job")
    site = Site(os.environ["GBI_DATA_SITE_CONF"])
    token = "gbi-sdk-test-" + uuid.uuid4().hex
    source = site.roots["lustre"] / token
    target = site.roots["alluxio"] / token
    source.mkdir(mode=0o700)
    target.mkdir(mode=0o700)
    checkpoints = source / "checkpoints"
    checkpoints.mkdir()
    expected = {}
    for name, contents in (("model.pt", b"checkpoint fixture" * 2048),
                           ("model.pt.ready.json", b'{"ready": true}\n')):
        (checkpoints / name).write_bytes(contents)
        expected[name] = digest(checkpoints / name)
    (checkpoints / "keep.txt").write_text("unselected source remains")
    archive = target / "checkpoints.gbi.tar.gz"
    data.move(checkpoints, archive, pack="gzip", include=["*.pt", "*.pt.*"])
    assert sorted(path.name for path in checkpoints.iterdir()) == ["keep.txt"]
    restored = source / "restored"
    data.copy(archive, restored)
    assert archive.is_file()
    assert {path.name: digest(path) for path in restored.iterdir()} == expected
    histories = site.roots["alluxio"] / ".gbi" / "transfers" / job_id
    requests, receipts, run_ids = [], [], []
    for path in histories.glob("*/request.json"):
        request = json.loads(path.read_text())
        if token not in request["source"]:
            continue
        requests.append(request)
        run_ids.append(path.parent.name)
        receipts.extend(json.loads(line) for line in
                        path.with_name("receipts.jsonl").read_text().splitlines())
    assert len(requests) == 2
    assert all(request["execution"] == "allocation" for request in requests)
    assert all(request["allocation_job_id"] == job_id for request in requests)
    assert not any(row["event"] == "failed" for row in receipts)
    assert any(row["event"] == "deleted" for row in receipts)
    run_ids.sort()
    status_checks = {}
    for run_id in run_ids:
        result = subprocess.run(
            ["gbi", "data", "status", run_id],
            capture_output=True, text=True, check=False, timeout=120,
        )
        assert result.returncode == 0, result.stderr
        assert "Complete." in result.stdout
        status_checks[run_id] = {"returncode": result.returncode, "complete": True}
    numeric_status = subprocess.run(
        ["gbi", "data", "status", job_id],
        capture_output=True, text=True, check=False, timeout=120,
    )
    numeric_status_output = numeric_status.stdout + numeric_status.stderr
    assert numeric_status.returncode != 0
    assert all(run_id in numeric_status_output for run_id in run_ids)
    result = {"status": "PASS", "slurm_job": job_id, "same_allocation_transfers": 2,
              "allocation_job_ids": [request["allocation_job_id"] for request in requests],
              "transfer_run_ids": run_ids, "status_checks": status_checks,
              "numeric_status_ambiguous": True,
              "selected_files": len(expected), "verified_events": sum(
                  row["event"] == "verified" for row in receipts),
              "source": str(source), "archive": str(archive),
              "archive_retained": True, "unselected_source_retained": True}
    (source / "acceptance.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result), flush=True)


def prefect_main(job_size=None):
    """Submit through the installed HTTPS broker from a real user allocation."""
    job_id = os.environ.get("SLURM_JOB_ID")
    if not job_id:
        raise SystemExit("run inside a Slurm job")
    site = Site(os.environ["GBI_DATA_SITE_CONF"])
    token = "gbi-sdk-prefect-test-" + uuid.uuid4().hex
    source = site.roots["lustre"] / token
    target = site.roots["alluxio"] / token
    source.mkdir(mode=0o700)
    expected = {}
    for name, contents in (("model.pt", b"checkpoint fixture" * 2048),
                           ("model.pt.ready.json", b'{"ready": true}\n')):
        (source / name).write_bytes(contents)
        expected[name] = digest(source / name)
    (source / "keep.txt").write_text("unselected source remains")
    include = ["*.pt", "*.pt.*"]
    print(json.dumps({"phase": "submit", "slurm_job": job_id,
                      "source": str(source), "destination": str(target)}), flush=True)
    data.move(source, target, include=include, prefect=True)
    assert sorted(path.name for path in source.iterdir()) == ["keep.txt"]
    data.copy(target, source, include=include, prefect=True, job_size=job_size)
    assert {name: digest(source / name) for name in expected} == expected
    assert (source / "keep.txt").read_text() == "unselected source remains"
    result = {"status": "PASS", "slurm_job": job_id,
              "prefect_transfers": 2, "selected_files": len(expected),
              "source": str(source), "destination": str(target),
              "restored_sha256": expected, "unselected_source_retained": True}
    (source / "acceptance.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result), flush=True)


def cli(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefect", action="store_true",
                        help="test managed moves and restores through the compute HTTPS broker")
    parser.add_argument("--job-size", choices=("small", "large"),
                        help="Prefect restore reservation only; omitted uses the deployment default")
    args = parser.parse_args(argv)
    if args.job_size is not None and not args.prefect:
        parser.error("--job-size requires --prefect")
    prefect_main(job_size=args.job_size) if args.prefect else main()


if __name__ == "__main__":
    cli()
