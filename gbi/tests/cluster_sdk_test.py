"""Exercise SDK archive/restore in one Slurm allocation using fresh own fixtures."""

import json
import os
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
    requests, receipts = [], []
    for path in histories.glob("*/request.json"):
        request = json.loads(path.read_text())
        if token not in request["source"]:
            continue
        requests.append(request)
        receipts.extend(json.loads(line) for line in
                        path.with_name("receipts.jsonl").read_text().splitlines())
    assert len(requests) == 2
    assert all(request["execution"] == "allocation" for request in requests)
    assert not any(row["event"] == "failed" for row in receipts)
    assert any(row["event"] == "deleted" for row in receipts)
    result = {"status": "PASS", "slurm_job": job_id, "same_allocation_transfers": 2,
              "selected_files": len(expected), "verified_events": sum(
                  row["event"] == "verified" for row in receipts),
              "source": str(source), "archive": str(archive),
              "archive_retained": True, "unselected_source_retained": True}
    (source / "acceptance.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
