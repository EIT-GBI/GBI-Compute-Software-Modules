#!/usr/bin/env python3
"""Run inside Slurm as an ordinary user; touch only newly created test roots.

Keeps Alluxio fixtures and an acceptance report. Moves delete only fixtures
created by this invocation. No username argument, existing-data cleanup or IAM.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import uuid

from gbi_data.storage import Site


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def await_fixture(path, checksum):
    deadline = time.monotonic() + 120
    while True:
        try:
            if digest(path) == checksum:
                return
        except OSError:
            pass
        if time.monotonic() >= deadline:
            raise AssertionError(f"fixture did not become readable: {path}")
        time.sleep(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--large-bytes", type=int, default=0, help="optional bytes per file for four parallel files")
    options = parser.parse_args()
    if not os.environ.get("SLURM_JOB_ID"):
        parser.error("run inside a Slurm job")
    site = Site(os.environ["GBI_DATA_SITE_CONF"])
    token = "gbi-selftest-" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:8]
    roots = {name: root / token for name, root in site.roots.items()}
    for root in roots.values():
        root.mkdir(mode=0o700)
    results = []

    def execute(source, destination, *flags):
        started = time.monotonic()
        result = subprocess.run(["gbi", "data", "move", str(source), str(destination), "--local", *flags],
                                text=True, capture_output=True)
        print(result.stdout, end="", flush=True)
        print(result.stderr, end="", flush=True)
        if result.returncode:
            raise AssertionError(f"transfer failed: {source} -> {destination}")
        return time.monotonic() - started

    for source_kind, source_root in roots.items():
        for target_kind, target_root in roots.items():
            if source_kind == target_kind:
                continue
            source = source_root / ("to-" + target_kind)
            source.mkdir()
            expected = {}
            for index in range(16):
                name = f"file {index:02}.dat"
                data = os.urandom(1024 * 1024)
                (source / name).write_bytes(data)
                expected[name] = hashlib.sha256(data).hexdigest()
            if source_kind == "alluxio":
                for name, checksum in expected.items():
                    await_fixture(source / name, checksum)
            # Keep path encoding tests on native filesystems; the mount itself
            # may reject unusual names independently of the CLI.
            if source_kind != "alluxio" and target_kind != "alluxio":
                (source / "line\nbreak").write_bytes(b"newline filename")
                expected["line\nbreak"] = hashlib.sha256(b"newline filename").hexdigest()
                (source / "link").symlink_to("file 00.dat")
            destination = target_root / ("from-" + source_kind)
            elapsed = execute(source, destination)
            for name, checksum in expected.items():
                assert digest(destination / name) == checksum, name
                assert (source / name).exists() == (source_kind == "alluxio"), name
            results.append({"route": f"{source_kind}->{target_kind}", "files": len(expected), "seconds": elapsed})
            print("PASS " + json.dumps(results[-1]), flush=True)

    # Explicit Alluxio deletion is tested only on a file made by this test.
    explicit = roots["alluxio"] / "explicit-delete.dat"
    explicit.write_bytes(b"purpose-made disposable fixture")
    await_fixture(explicit, hashlib.sha256(b"purpose-made disposable fixture").hexdigest())
    destination = roots["lustre"] / "explicit-delete.dat"
    execute(explicit, destination, "--delete-source")
    assert not explicit.exists() and destination.read_bytes() == b"purpose-made disposable fixture"
    results.append({"explicit_object_source_deletion": "PASS"})

    if options.large_bytes:
        source = roots["lustre"] / "large"
        source.mkdir()
        block = os.urandom(1024 * 1024)
        expected = {}
        for index in range(4):
            path = source / f"checkpoint-{index}.pt"
            remaining = options.large_bytes
            with path.open("wb") as stream:
                while remaining:
                    chunk = block[:min(remaining, len(block))]
                    stream.write(chunk)
                    remaining -= len(chunk)
            expected[path.name] = digest(path)
        destination = roots["alluxio"] / "large"
        elapsed = execute(source, destination)
        for name, checksum in expected.items():
            assert digest(destination / name) == checksum
            assert not (source / name).exists()
        results.append({"parallel_large_files": 4, "bytes_each": options.large_bytes,
                        "seconds": elapsed, "MiB_per_second": options.large_bytes * 4 / elapsed / 2**20})
        print("PASS " + json.dumps(results[-1]), flush=True)

    report = roots["alluxio"] / "acceptance.json"
    with report.open("x") as stream:
        json.dump({"user": site.user, "job": os.environ["SLURM_JOB_ID"], "results": results}, stream, indent=2)
    print(f"PASS all checks; retained fixtures and report: {report}", flush=True)


if __name__ == "__main__":
    main()
