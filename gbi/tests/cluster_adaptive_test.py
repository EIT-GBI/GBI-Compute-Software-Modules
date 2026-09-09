"""Validate automatic execution on fresh fixtures; run as an ordinary user.

From a login shell: native and parallel foreground routes plus explicit background.
Inside Slurm: one 5 KB transfer that must reuse the existing allocation.
"""

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import uuid

from gbi_data.storage import Site
from cluster_selftest import await_fixture, digest


def main():
    site = Site(os.environ["GBI_DATA_SITE_CONF"])
    allocated = bool(os.environ.get("SLURM_JOB_ID"))
    token = "gbi-adaptive-test-" + uuid.uuid4().hex
    roots = {kind: root / token for kind, root in site.roots.items()}
    for root in roots.values():
        root.mkdir(mode=0o700)
    results = []

    def execute(source_kind, target_kind, size, name, count=1, background=False):
        source, target = roots[source_kind] / name, roots[target_kind] / name
        expected = {}
        if count > 1:
            source.mkdir()
        for number in range(count):
            path = source / str(number) if count > 1 else source
            checksum = hashlib.sha256()
            block = os.urandom(min(size, 1024 * 1024))
            remaining = size
            with path.open("wb") as stream:
                while remaining:
                    part = block[:min(remaining, len(block))]
                    stream.write(part)
                    checksum.update(part)
                    remaining -= len(part)
            expected[path.name] = checksum.hexdigest()
            if source_kind == "alluxio":
                await_fixture(path, checksum.hexdigest())
        started = time.monotonic()
        result = subprocess.run(["gbi", "data", "move", str(source), str(target),
                                 "--detach" if background else "--wait"],
                                capture_output=True, text=True, timeout=1200)
        elapsed = time.monotonic() - started
        print(result.stdout, end="", flush=True)
        assert result.returncode == 0, result.stderr + result.stdout
        mode = "slurm" if background else "allocation" if allocated else "inline"
        if mode == "inline":
            assert "Submitted transfer" not in result.stdout
            transfer_id = re.search(r"Transfer ([0-9]{8}T[0-9]{6}Z-[0-9a-f]{12})", result.stdout).group(1)
        elif mode == "allocation":
            assert "Submitted transfer" not in result.stdout
            transfer_id = os.environ["SLURM_JOB_ID"]
        else:
            transfer_id = re.search(r"Submitted transfer ([0-9]+)", result.stdout).group(1)
        if background:
            watched = subprocess.run(["gbi", "data", "status", transfer_id, "--watch"],
                                     capture_output=True, text=True, timeout=1200)
            print(watched.stdout, end="", flush=True)
            assert watched.returncode == 0, watched.stderr + watched.stdout
            elapsed = time.monotonic() - started
        history_root = site.roots["alluxio"] / ".gbi" / "transfers" / transfer_id
        history = next(history_root.iterdir())
        if mode == "inline":
            bundle = json.loads((history / "history.json").read_text())
            request, receipts = bundle["request"], bundle["receipts"]
        else:
            request = json.loads((history / "request.json").read_text())
            receipts = [json.loads(line) for line in (history / "receipts.jsonl").read_text().splitlines()]
        assert request["execution"] == mode
        assert receipts[0]["reader"] == ("native" if size == 5000 and not background else "rclone")
        for filename, checksum in expected.items():
            assert digest(target / filename if count > 1 else target) == checksum
            assert (source / filename if count > 1 else source).exists() == (source_kind == "alluxio")
        status = subprocess.run(["gbi", "data", "status", transfer_id], capture_output=True, text=True, timeout=20)
        assert status.returncode == 0 and "Complete" in status.stdout, status.stdout + status.stderr
        results.append({"route": f"{source_kind}->{target_kind}", "bytes": size * count, "files": count,
                        "execution": mode, "seconds": elapsed, "id": transfer_id})
        print("PASS " + json.dumps(results[-1]), flush=True)

    if allocated:
        execute("lustre", "fss", 5000, "reuse-allocation")
    else:
        for source_kind in roots:
            for target_kind in roots:
                if source_kind != target_kind:
                    execute(source_kind, target_kind, 5000, f"{source_kind}-to-{target_kind}")
        execute("lustre", "alluxio", 128 * 1024**2, "parallel-foreground", count=4)
        execute("lustre", "alluxio", 2 * 1024**3, "multigib-foreground")
        execute("lustre", "alluxio", 16 * 1024**2, "explicit-background", background=True)
        bulk = roots["lustre"] / "bulk-plan-only"
        with bulk.open("wb") as stream:
            stream.truncate(8 * 1024**3 + 1)
        planned = subprocess.run(["gbi", "data", "move", str(bulk),
                                  str(roots["alluxio"] / bulk.name), "--dry-run"],
                                 capture_output=True, text=True, timeout=30)
        assert planned.returncode == 0 and "Submitting background work to Slurm" in planned.stdout
        assert not (roots["alluxio"] / bulk.name).exists()
        print("PASS automatic bulk plan above 8 GiB; dry run only", flush=True)
    report = roots["alluxio"] / "acceptance.json"
    contents = json.dumps(results, indent=2).encode()
    report.write_bytes(contents)
    await_fixture(report, hashlib.sha256(contents).hexdigest())
    print(f"PASS all checks; report: {report}", flush=True)


if __name__ == "__main__":
    main()
