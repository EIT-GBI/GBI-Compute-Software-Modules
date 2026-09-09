"""Exercise owned-partial recovery on real Alluxio using fresh Slurm fixtures."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import uuid
from unittest.mock import patch

from gbi_data.storage import Site
from gbi_data.transfer import transfer
from cluster_selftest import await_fixture


assert os.environ.get("SLURM_JOB_ID"), "run inside Slurm"
site = Site(os.environ["GBI_DATA_SITE_CONF"])
name = "gbi-retry-test-" + uuid.uuid4().hex
scratch = site.roots["lustre"] / name
archive = site.roots["alluxio"] / name
for directory in (scratch / "state" / "locks", scratch / "state" / "pending", archive):
    directory.mkdir(parents=True, mode=0o700)
source, target = scratch / "source.dat", archive / "target.dat"
payload = os.urandom(4 * 1024 * 1024)
source.write_bytes(payload)
task = {"source": str(source), "target": str(target),
        "source_root": str(site.roots["lustre"]), "target_root": str(site.roots["alluxio"]),
        "state": str(scratch / "state"), "receipt": str(scratch / "receipt.jsonl"),
        "progress": str(scratch / "progress.json"), "selection_root": str(scratch),
        "rclone": shutil.which("rclone"), "settle_seconds": 120,
        "target_kind": "alluxio", "delete": True, "encode_link": False, "decode_link": False}


def interrupted(source, target, fd, *args):
    with os.fdopen(fd, "wb") as stream:
        stream.write(b"partial")
    raise OSError("injected interruption after partial write")


with patch("gbi_data.transfer.copy_stream", side_effect=interrupted):
    try:
        transfer(task)
    except OSError as error:
        assert "injected interruption" in str(error)
    else:
        raise AssertionError("interruption did not fail")
assert source.read_bytes() == payload
assert not Path(task["receipt"]).exists()
await_fixture(target, hashlib.sha256(b"partial").hexdigest())
transfer(task)
assert target.read_bytes() == payload
assert not source.exists()
rows = [json.loads(line) for line in Path(task["receipt"]).read_text().splitlines()]
assert [row["event"] for row in rows] == ["verified", "deleted"]
print(f"PASS real Alluxio interrupted-write recovery; receipt: {task['receipt']}", flush=True)
