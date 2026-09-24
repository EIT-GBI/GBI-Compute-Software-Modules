#!/usr/bin/env python3
"""Bounded ordinary-user benchmark; creates and retains only new token roots.

Runs the maintained CLI end-to-end (including receipt/history publication),
then profiles one real transfer per reader in-process to count metadata calls.
The native comparison uses test-only site thresholds; it changes no installed
configuration or concurrency defaults. Outside Slurm results are explicitly
local synthetic evidence, not shared-storage runtime qualification.
"""

import argparse
from collections import Counter
import cProfile
import hashlib
import json
import os
from pathlib import Path
import platform
import pstats
import pwd
import re
import resource
import shutil
import subprocess
import sys
import time
import uuid

from gbi_data import __version__, cli, selection, transfer as transfer_module
from gbi_data.transfer import transfer
from gbi_data.storage import Site


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def fixture(root, sizes):
    expected = {}
    block = bytes(range(256)) * 4096
    for index, size in enumerate(sizes):
        path = root / f"file-{index:04d}.bin"
        with path.open("xb") as output:
            remaining = size
            while remaining:
                data = block[:min(remaining, len(block))]
                output.write(data)
                remaining -= len(data)
        expected[path.name] = digest(path)
    return expected


def usage():
    values = [resource.getrusage(kind) for kind in (resource.RUSAGE_SELF, resource.RUSAGE_CHILDREN)]
    return {"cpu_seconds": sum(row.ru_utime + row.ru_stime for row in values),
            "input_blocks": sum(row.ru_inblock for row in values),
            "output_blocks": sum(row.ru_oublock for row in values),
            # getrusage reports process-lifetime high watermarks, not interval
            # deltas; label this honestly instead of subtracting peak values.
            "process_lifetime_peak_rss": max(row.ru_maxrss for row in values)}


def delta(before, after):
    return {key: value if key.endswith("rss") else value - before[key] for key, value in after.items()}


def engine_profile(source, destination, state, rclone, target_kind="alluxio"):
    """Count real Python-visible filesystem calls; not kernel/NFS RPC counts."""
    for directory in (state, state / "locks", state / "pending"):
        directory.mkdir(mode=0o700, exist_ok=True)
    task = {"source": str(source), "target": str(destination),
            "source_root": str(source.parent), "target_root": str(destination.parent),
            "selection_root": str(source.parent), "state": str(state),
            "receipt": str(state / "receipts.jsonl"), "progress": str(state / "progress.json"),
            "rclone": rclone, "settle_seconds": 1200, "target_kind": target_kind,
            "delete": False, "encode_link": False, "decode_link": False}
    profiler = cProfile.Profile()
    before, started = usage(), time.monotonic()
    result = profiler.runcall(transfer, task)
    elapsed, after = time.monotonic() - started, usage()
    counters = Counter()
    for (_, _, function), (_, calls, _, _, _) in pstats.Stats(profiler).stats.items():
        for name in ("stat", "lstat", "mkdir", "open", "rename", "unlink", "scandir", "fsync"):
            if function in (f"<built-in method posix.{name}>", f"<built-in method io.{name}>"):
                counters[name] += calls
    receipt = json.loads((state / "receipts.jsonl").read_text().splitlines()[0])
    assert receipt["sha256"] == digest(destination)
    return {"seconds": elapsed, **delta(before, after), "python_filesystem_calls": dict(counters),
            "timings_seconds": receipt["timings_seconds"],
            "note": "single maintained transfer() call; excludes CLI/discovery/worker process startup"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site-conf", type=Path, help="real installed site configuration; requires an existing Slurm allocation")
    parser.add_argument("--source-kind", choices=("lustre", "fss", "alluxio"), default="lustre")
    parser.add_argument("--target-kind", choices=("lustre", "fss", "alluxio"), default="alluxio")
    parser.add_argument("--source-root", type=Path, help="local synthetic fixture parent; omit with --site-conf")
    parser.add_argument("--target-root", type=Path, help="local synthetic destination parent; omit with --site-conf")
    parser.add_argument("--state-root", type=Path, help="local synthetic scratch/report parent; omit with --site-conf")
    parser.add_argument("--rclone", required=True, help="maintained pinned rclone executable")
    parser.add_argument("--repetitions", type=int, default=3, choices=range(1, 6))
    parser.add_argument("--readers", nargs="+", choices=("default", "rclone", "native"),
                        default=["default", "native"] if sys.platform == "darwin" else ["rclone", "native"],
                        help="default tests unchanged site policy; other values force comparison readers only in fixture config")
    parser.add_argument("--large-bytes", type=int, default=16 * 1024**2, help="bytes per large fixture (max 256 MiB)")
    options = parser.parse_args()
    if sys.platform == "darwin" and "rclone" in options.readers:
        parser.error("macOS uses native regular-file reads; choose --readers default or native")
    if not 1 <= options.large_bytes <= 256 * 1024**2:
        parser.error("--large-bytes must be between 1 and 268435456")
    rclone = shutil.which(options.rclone)
    if not rclone:
        parser.error("maintained rclone executable is unavailable")
    site = Site(options.site_conf) if options.site_conf else None
    if site:
        if not os.environ.get("SLURM_JOB_ID"):
            parser.error("real-route benchmark requires an existing Slurm allocation")
        if any((options.source_root, options.target_root, options.state_root)):
            parser.error("--site-conf uses the real site roots; do not pass synthetic root overrides")
        if options.source_kind == options.target_kind:
            parser.error("choose two different storage routes")
        if int(site.values["jobs"]) > 4:
            parser.error("this bounded benchmark requires a site limit of at most four file workers")
        parents = {"source": site.roots[options.source_kind], "target": site.roots[options.target_kind],
                   "state": site.roots["lustre"]}
    else:
        if not all((options.source_root, options.target_root, options.state_root)):
            parser.error("local synthetic mode needs all three root paths")
        if os.environ.get("SLURM_JOB_ID"):
            parser.error("inside Slurm use --site-conf; simulated filesystem types do not qualify real routes")
        parents = {key: getattr(options, key + "_root").resolve() for key in ("source", "target", "state")}
    token = "gbi-benchmark-" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:8]
    roots = {}
    for key, parent in parents.items():
        if not parent.is_dir():
            parser.error(f"{key} root is not an existing directory")
        roots[key] = parent / (token + "-" + key)
        roots[key].mkdir(mode=0o700)
    user = pwd.getpwuid(os.getuid()).pw_name
    code_hashes = {module.__file__: digest(Path(module.__file__)) for module in (cli, selection, transfer_module)}
    report = {"version": __version__, "code_sha256": code_hashes,
              "rclone_version": subprocess.check_output([rclone, "version"], text=True).splitlines()[0],
              "platform": platform.platform(), "slurm_job": os.environ.get("SLURM_JOB_ID"),
              "scope": "real site route" if site else "local synthetic only",
              "route": [options.source_kind, options.target_kind] if site else ["synthetic-fss", "synthetic-alluxio"],
              "rss_units": "bytes" if sys.platform == "darwin" else "KiB",
              "concurrency": int(site.values["jobs"]) if site else 4,
              "roots": {key: str(path) for key, path in roots.items()}, "results": []}
    report_path = roots["state"] / "report.json"
    cases = {"small": [4096] * 40, "mixed": [4096] * 36 + [options.large_bytes] * 4,
             "large": [options.large_bytes] * 4}
    for case, sizes in cases.items():
        source_parent = roots["source"] / case
        source = source_parent / user / "payload"
        source.mkdir(parents=True)
        expected = fixture(source, sizes)
        for repetition in range(options.repetitions):
            for reader in options.readers:
                name = f"{case}-{repetition}-{reader}"
                target_parent = roots["target"] / name
                (target_parent / user).mkdir(parents=True)
                destination = target_parent / user / "payload"
                scratch = roots["state"] / name
                (scratch / user).mkdir(parents=True)
                conf = scratch / "site.conf"
                values = site.values.copy() if site else {"lustre_root": str(scratch),
                    "fss_root": str(source_parent), "bucket_root": str(target_parent)}
                values["rclone_bin"] = rclone
                if reader != "default":
                    values.update(native_files=str(len(sizes) + 1 if reader == "native" else 1),
                                  native_bytes=str(sum(sizes) + 1 if reader == "native" else 1))
                conf.write_text("".join(f"{key} = {value}\n" for key, value in values.items()))
                environment = {**os.environ, "GBI_DATA_SITE_CONF": str(conf)}
                before, started = usage(), time.monotonic()
                completed = subprocess.run([sys.executable, "-B", "-m", "gbi_data.cli", "data", "copy",
                                            str(source), str(destination)], env=environment,
                                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=300)
                elapsed, after = time.monotonic() - started, usage()
                (scratch / "cli.log").write_text(completed.stdout)
                if completed.returncode:
                    report["results"].append({"case": case, "reader": reader, "repetition": repetition,
                                              "seconds": elapsed, "exit_code": completed.returncode,
                                              "error": "CLI failed; stopped comparison", "log": str(scratch / "cli.log")})
                    report_path.write_text(json.dumps(report, indent=2) + "\n")
                    raise RuntimeError(f"benchmark failed; inspect {scratch / 'cli.log'}")
                history_paths = list(dict.fromkeys(re.findall(r"History: (/[^\n]+)", completed.stdout)))
                if len(history_paths) != 1:
                    raise AssertionError("expected one exact published history path")
                history = Path(history_paths[0])
                bundle = (json.loads((history / "history.json").read_text()) if (history / "history.json").exists()
                          else {"progress": json.loads((history / "progress.json").read_text()),
                                "receipts": [json.loads(line) for line in (history / "receipts.jsonl").read_text().splitlines()]})
                receipts = [row for row in bundle["receipts"] if row["event"] == "verified"]
                assert len(receipts) == len(expected) and bundle["progress"]["failed"] == 0
                if reader != "default":
                    assert all(row["reader"] == reader for row in receipts)
                assert {path.name: digest(path) for path in destination.iterdir()} == expected
                assert all((source / name).exists() for name in expected)
                row = {"case": case, "repetition": repetition, "reader": reader, "seconds": elapsed,
                       "files": len(sizes), "bytes": sum(sizes), "verified_receipts": len(receipts),
                       "failed": 0, **delta(before, after),
                       "reader_counts": dict(Counter(row["reader"] for row in receipts)),
                       "history": str(history),
                       "receipt_phase_seconds": {phase: sum(receipt["timings_seconds"].get(phase, 0) for receipt in receipts)
                                                 for phase in ("copy", "close", "destination_readback", "source_recheck")}}
                report["results"].append(row)
                print(json.dumps(row), flush=True)
                report_path.write_text(json.dumps(report, indent=2) + "\n")
        report.setdefault("engine_profiles", {})[case] = {}
        for reader in (("native",) if sys.platform == "darwin" else ("rclone", "native")):
            destination = roots["target"] / f"profile-{case}-{reader}"
            state = roots["state"] / f"profile-{case}-{reader}"
            report["engine_profiles"][case][reader] = engine_profile(
                source / "file-0000.bin", destination, state, rclone if reader == "rclone" else None,
                target_kind=options.target_kind if site else "alluxio")
        report_path.write_text(json.dumps(report, indent=2) + "\n")
    report["code_unchanged_during_run"] = all(digest(Path(path)) == value for path, value in code_hashes.items())
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Retained owned fixtures and report: {report_path}", flush=True)


if __name__ == "__main__":
    main()
