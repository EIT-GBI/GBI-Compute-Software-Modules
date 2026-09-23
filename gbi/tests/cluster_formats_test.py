#!/usr/bin/env python3
"""Bounded portable-format acceptance inside an existing ordinary-user Slurm job.

Uses real configured roots and the public CLI. Retains fixtures, CLI histories
and a Lustre report. Only the explicitly created disposable move fixture loses
source entries. Run sequentially with packing_benchmark.py in the same job.
"""

import argparse
import json
import os
from pathlib import Path
import stat
import time
import uuid

from cluster_benchmark import digest
from packing_benchmark import run_cli
from gbi_data.storage import Site


def fixture(root):
    root.mkdir()
    (root / "empty directory").mkdir()
    (root / "nested").mkdir()
    (root / "keep.txt").write_bytes(b"selected payload\n" * 31)
    (root / "nested" / "line\nbreak λ.dat").write_bytes(bytes(range(256)) * 17)
    (root / "skip.bin").write_bytes(b"excluded payload")
    (root / "relative link").symlink_to("nested/line\nbreak λ.dat")
    (root / "dangling link").symlink_to("absent-file")
    os.link(root / "keep.txt", root / "alias.txt")
    os.chmod(root / "keep.txt", 0o640)
    for path in sorted(root.rglob("*"), key=lambda item: len(item.parts), reverse=True):
        if not path.is_symlink():
            os.utime(path, ns=(1700000000123456789, 1700000000123456789))
    return inventory(root)


def inventory(root):
    """Independent native-tree inventory; do not trust archive manifests."""
    entries, links = {}, {}
    for path in sorted(root.rglob("*")):
        value = path.lstat()
        name = path.relative_to(root).as_posix()
        if stat.S_ISLNK(value.st_mode):
            entries[name] = {"kind": "symlink", "target": os.readlink(path)}
        elif stat.S_ISDIR(value.st_mode):
            entries[name] = {"kind": "directory"}
        elif stat.S_ISREG(value.st_mode):
            identity = (value.st_dev, value.st_ino)
            entries[name] = {"kind": "file", "bytes": value.st_size, "sha256": digest(path),
                             "hardlink_group": links.setdefault(identity, name)}
        else:
            raise AssertionError(f"unexpected special fixture entry: {path}")
        if not stat.S_ISLNK(value.st_mode):
            entries[name].update(mode=stat.S_IMODE(value.st_mode), mtime_ns=value.st_mtime_ns)
    return entries


def compare(root, expected):
    actual = inventory(root)
    if actual.keys() != expected.keys():
        raise AssertionError(f"names differ at {root}: {set(actual) ^ set(expected)}")
    for name, value in expected.items():
        observed = actual[name].copy()
        wanted = value.copy()
        if "mtime_ns" in wanted:
            # Native destination precision may truncate nanoseconds.
            difference = wanted.pop("mtime_ns") - observed.pop("mtime_ns")
            if not 0 <= difference < 1_000_000_000:
                raise AssertionError(f"mtime differs: {root / name}")
        if observed != wanted:
            raise AssertionError(f"content/type/mode/link parity failed: {root / name}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site-conf", type=Path, required=True)
    options = parser.parse_args()
    if not os.environ.get("SLURM_JOB_ID"):
        parser.error("requires an existing Slurm allocation")
    site = Site(options.site_conf)
    if int(site.values["jobs"]) > 4:
        parser.error("requires at most four file workers")
    token = "gbi-formats-test-" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:8]
    roots = {kind: root / token for kind, root in site.roots.items()}
    for root in roots.values():
        root.mkdir(mode=0o700)
    report_path = roots["lustre"] / "report.json"
    report = {"slurm_job": os.environ["SLURM_JOB_ID"], "uid": os.getuid(),
              "roots": {key: str(value) for key, value in roots.items()}, "results": []}
    code = {str(path): digest(path) for path in Path(__file__).resolve().parents[1].joinpath("src/lib/gbi_data").glob("*.py")}
    report["code_sha256"] = code

    def execute(name, arguments, operation="copy"):
        row = {"case": name, **run_cli(arguments, options.site_conf, roots["lustre"] / (name + ".log"),
                                       1, operation=operation)}
        report["results"].append(row)
        report_path.write_text(json.dumps(report, indent=2) + "\n")
        return row

    try:
        for origin in ("lustre", "fss"):
            source = roots[origin] / "source"
            expected = fixture(source)
            report[origin + "_inventory"] = expected
            for compression in ("tar", "gzip"):
                suffix = ".gbi.tar" + (".gz" if compression == "gzip" else "")
                archive = roots["alluxio"] / (origin + suffix)
                execute(origin + "-" + compression, [source, archive, "--pack", compression])
                for native in ("lustre", "fss"):
                    restored = roots[native] / (origin + "-" + compression + "-restored")
                    execute(origin + "-" + compression + "-to-" + native, [archive, restored])
                    compare(restored, expected)
                compare(source, expected)
            filtered = roots["fss"] / (origin + "-filtered")
            archive = roots["alluxio"] / (origin + ".gbi.tar")
            execute(origin + "-filter", [archive, filtered, "--include", "alias.txt"])
            alias = expected["alias.txt"].copy()
            alias["hardlink_group"] = "alias.txt"
            compare(filtered, {"alias.txt": alias})
            if not archive.is_file():
                raise AssertionError("filtered restore removed source archive")

        source = roots["lustre"] / "source"
        for packed in (False, True):
            name = "archive-chunks" if packed else "raw-chunks"
            original = source if packed else source / "keep.txt"
            logical = roots["alluxio"] / (name + (".gbi.tar" if packed else ".txt"))
            flags = ["--chunk-size", "4KiB"] + (["--pack", "tar"] if packed else [])
            execute(name, [original, logical, *flags])
            # Identical retry exercises completed output/stage reuse, without
            # synthetic remote corruption or killing a live transfer worker.
            execute(name + "-retry", [original, logical, *flags])
            store = Path(str(logical) + ".gbi-chunks")
            restored = roots["fss"] / (name + "-restored")
            execute(name + "-restore", [store, restored])
            if packed:
                compare(restored, report["lustre_inventory"])
            elif digest(restored) != digest(original):
                raise AssertionError("raw chunk independent checksum mismatch")
            execute(name + "-restore-retry", [store, restored])
        compare(source, report["lustre_inventory"])

        disposable = roots["lustre"] / "disposable-move"
        disposable.mkdir()
        (disposable / "payload.txt").write_bytes(b"only this new fixture may be deleted")
        expected = inventory(disposable)
        archive = roots["alluxio"] / "disposable.gbi.tar"
        execute("disposable-move", [disposable, archive, "--pack", "tar"], operation="move")
        if list(disposable.iterdir()):
            raise AssertionError("disposable move failed or retained selected source entries")
        restored = roots["fss"] / "disposable-restored"
        execute("disposable-restore", [archive, restored])
        compare(restored, expected)
        report["disposable_move"] = "verified by independent restore; source entries removed"
        if any(digest(Path(path)) != value for path, value in code.items()):
            raise AssertionError("candidate code changed during acceptance")
        report["passed"] = True
    except Exception as error:
        report["error"] = str(error)
        raise
    finally:
        report_path.write_text(json.dumps(report, indent=2) + "\n")
        print(f"Retained owned fixtures, logs and report: {report_path}", flush=True)


if __name__ == "__main__":
    main()
