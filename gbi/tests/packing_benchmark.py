#!/usr/bin/env python3
"""Compare loose files and tar archives on real configured routes inside Slurm.

Creates only unique owned fixture directories, retains all sources and outputs,
and runs the public CLI through verification and history publication.
"""

import argparse
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import time
import uuid

from cluster_benchmark import delta, digest, fixture, usage
from gbi_data.storage import Site


def run_cli(arguments, config, log, expected_receipts, *, operation="copy"):
    if operation not in ("copy", "move"):
        raise ValueError("benchmark operation must be copy or move")
    before, started = usage(), time.monotonic()
    result = subprocess.run([sys.executable, "-B", "-m", "gbi_data.cli", "data", operation, *map(str, arguments)],
                            env={**os.environ, "GBI_DATA_SITE_CONF": str(config)},
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=300)
    elapsed, after = time.monotonic() - started, usage()
    log.write_text(result.stdout)
    if result.returncode:
        raise RuntimeError(f"CLI failed ({result.returncode}); see {log}")
    histories = list(dict.fromkeys(re.findall(r"History: (/[^\n]+)", result.stdout)))
    if len(histories) != 1:
        raise AssertionError("expected one published history")
    history = Path(histories[0])
    if (history / "history.json").exists():
        bundle = json.loads((history / "history.json").read_text())
    else:
        bundle = {"progress": json.loads((history / "progress.json").read_text()),
                  "receipts": [json.loads(line) for line in (history / "receipts.jsonl").read_text().splitlines()]}
    verified = [row for row in bundle["receipts"] if row["event"] == "verified"]
    if len(verified) != expected_receipts or bundle["progress"]["failed"]:
        raise AssertionError("missing verified receipts or failed transfers")
    return {"seconds": elapsed, **delta(before, after), "history": str(history),
            "verified_receipts": len(verified), "failed": 0}


def verify_directory(directory, expected):
    if {path.name: digest(path) for path in directory.iterdir()} != expected:
        raise AssertionError(f"independent fixture comparison failed: {directory}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site-conf", type=Path, required=True)
    parser.add_argument("--source-kind", choices=("lustre", "fss"), default="lustre")
    options = parser.parse_args()
    if not os.environ.get("SLURM_JOB_ID"):
        parser.error("requires an existing Slurm allocation and real site configuration")
    site = Site(options.site_conf)
    if int(site.values["jobs"]) > 4:
        parser.error("requires at most four file workers")
    token = "gbi-pack-benchmark-" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:8]
    roots = {kind: root / token for kind, root in site.roots.items()}
    for root in roots.values():
        root.mkdir(mode=0o700)
    report_path = roots["lustre"] / "report.json"
    values = site.values.copy()
    # Explicit experimental policy, not a proposed production threshold.
    values.update(pack_small_bytes=str(1024**2), pack_min_files="8", pack_max_bytes=str(64 * 1024**2))
    config = roots["lustre"] / "site.conf"
    config.write_text("".join(f"{key} = {value}\n" for key, value in values.items()))
    code = {str(path): digest(path) for path in Path(__file__).resolve().parents[1].joinpath("src/lib/gbi_data").glob("*.py")}
    report = {"scope": "real site routes", "source_kind": options.source_kind,
              "slurm_job": os.environ["SLURM_JOB_ID"], "platform": platform.platform(),
              "concurrency": int(values["jobs"]), "code_sha256": code,
              "rss_units": "KiB" if sys.platform != "darwin" else "bytes",
              "roots": {key: str(value) for key, value in roots.items()}, "results": []}
    try:
        for size in (4096, 65536, 1048576):
            for count in (8, 40):
                case = f"{count}x{size}"
                source = roots[options.source_kind] / (case + "-source")
                source.mkdir()
                expected = fixture(source, [size] * count)
                loose = roots["alluxio"] / (case + "-loose")
                archive = roots["alluxio"] / (case + ".gbi.tar")
                row = {"case": case, "files": count, "bytes_per_file": size, "bytes": count * size}
                for mode, arguments, receipts in (("loose", [source, loose], count),
                                                 ("tar", [source, archive, "--pack", "tar"], 1)):
                    row[mode] = run_cli(arguments, config, roots["lustre"] / f"{case}-{mode}.log", receipts)
                verify_directory(loose, expected)
                row["archive_bytes"] = archive.stat().st_size
                for kind in ("lustre", "fss"):
                    restored = roots[kind] / (case + "-restored")
                    restored.mkdir()
                    row["restore_" + kind] = run_cli([archive, restored], config,
                        roots["lustre"] / f"{case}-restore-{kind}.log", 1)
                    verify_directory(restored, expected)
                verify_directory(source, expected)
                report["results"].append(row)
                report_path.write_text(json.dumps(report, indent=2) + "\n")
                print(json.dumps(row), flush=True)
        report["code_unchanged_during_run"] = all(digest(Path(path)) == value for path, value in code.items())
        if not report["code_unchanged_during_run"]:
            raise RuntimeError("candidate source changed during benchmark")
    except Exception as error:
        report["error"] = str(error)
        raise
    finally:
        report_path.write_text(json.dumps(report, indent=2) + "\n")
        print(f"Retained owned fixtures and report: {report_path}", flush=True)


if __name__ == "__main__":
    main()
