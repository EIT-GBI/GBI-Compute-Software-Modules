"""Path-based data movement with direct Slurm execution and terminal progress."""

import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import fnmatch
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid

from . import __version__
from . import jobs
from .storage import Site, overlap
from .transfer import ensure_parent, receipt, write_json


def parser():
    result = argparse.ArgumentParser(prog="gbi", description="Move data between your HPC filesystems.")
    result.add_argument("--version", action="version", version=__version__)
    data = result.add_subparsers(dest="command", required=True).add_parser("data")
    verbs = data.add_subparsers(dest="verb", required=True, metavar="{copy,move,status,roots}")
    for verb in ("copy", "move"):
        command = verbs.add_parser(verb, help=f"{verb} a file or directory to a destination")
        command.add_argument("source")
        command.add_argument("destination")
        command.add_argument("--include", action="append", default=[], metavar="GLOB",
                             help="select file names matching this pattern; repeat for alternatives")
        command.add_argument("--delete-source", action="store_true",
                             help="explicitly allow source deletion when moving OUT of Alluxio/Object Storage")
        command.add_argument("--dry-run", action="store_true", help="show route and policy without writing")
        command.add_argument("--detach", action="store_true", help="submit and return without following progress")
        command.add_argument("--wait", action="store_true", help="follow progress even when output is not a terminal")
        command.add_argument("--local", action="store_true", help="execute inside an existing Slurm allocation")
    status = verbs.add_parser("status", help="show a transfer's progress")
    status.add_argument("job_id")
    status.add_argument("--watch", action="store_true", help="follow until the transfer finishes")
    verbs.add_parser("roots", help="show your available storage paths")
    # Internal entrypoint used by the generated batch script.
    run = verbs.add_parser("_run")
    run.add_argument("run_dir", type=Path)
    return result


def plan(options, site):
    source, source_kind, source_root = site.classify(options.source)
    target, target_kind, target_root = site.classify(options.destination)
    if not os.path.lexists(source):
        raise ValueError(f"source does not exist: {source}")
    if source.is_symlink():
        directory = False
    else:
        directory = source.is_dir()
    if not directory and target.is_dir():
        target /= source.name
    if overlap(source, target):
        raise ValueError("source and destination must not overlap")
    if options.delete_source and options.verb != "move":
        raise ValueError("--delete-source is available with move only")
    delete = options.verb == "move" and (source_kind != "alluxio" or options.delete_source)
    return {"source": str(source), "target": str(target), "source_kind": source_kind,
            "target_kind": target_kind, "source_root": str(source_root), "target_root": str(target_root),
            "directory": directory, "delete": delete, "include": options.include,
            "verb": options.verb, "uid": os.getuid(), "site": str(site.path)}


def entries(source, site, root, patterns):
    """Depth-first scandir iterator: no whole-tree inventory or checksum prepass."""
    if source.is_symlink() or not source.is_dir():
        yield source, False
        return
    with os.scandir(source) as children:
        empty = True
        for child in children:
            empty = False
            path = Path(child.path)
            if site.is_reserved(path, root):
                continue
            if child.is_dir(follow_symlinks=False):
                yield from entries(path, site, root, patterns)
            elif not patterns or any(fnmatch.fnmatchcase(child.name, pattern) for pattern in patterns):
                yield path, False
        if empty and not patterns:
            yield source, True


def supervise(task, timeout, stopped):
    """Bound all per-file I/O, including close and verification, in a process."""
    process = subprocess.Popen(
        [sys.executable, "-B", "-m", "gbi_data.transfer"], stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, text=True, start_new_session=True,
    )
    process.stdin.write(json.dumps(task))
    process.stdin.close()
    deadline = time.monotonic() + timeout
    while process.poll() is None:
        if stopped.is_set() or time.monotonic() >= deadline:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                try:
                    process.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    pass
            process.stdout.close()
            return {"error": "interrupted or timed out; inspect receipts before retrying"}
        try:
            process.wait(timeout=0.2)
        except subprocess.TimeoutExpired:
            pass
    output = process.stdout.read()
    process.stdout.close()
    if process.returncode:
        return {"error": f"transfer worker exited {process.returncode}; inspect receipts"}
    try:
        return json.loads(output)
    except ValueError:
        return {"error": "transfer worker returned no valid result; inspect receipts"}


class TransferPool(ThreadPoolExecutor):
    """Stop in-flight workers before waiting if discovery or state writing fails."""
    def __init__(self, stopped, **kwargs):
        super().__init__(**kwargs)
        self.stopped = stopped

    def __exit__(self, exception_type, exception, traceback):
        if exception_type is not None:
            self.stopped.set()
        return super().__exit__(exception_type, exception, traceback)


def run(run_dir, site, home):
    specification = json.loads((run_dir / "request.json").read_text())
    if specification["uid"] != os.getuid():
        raise ValueError("this request belongs to another user")
    # Recheck both storage roots on the execution node. A missing mount never
    # becomes a new local directory masquerading as the intended filesystem.
    for prefix in ("source", "target"):
        path, kind, root = site.classify(specification[prefix])
        if (str(path), kind, str(root)) != (specification[prefix], specification[prefix + "_kind"],
                                         specification[prefix + "_root"]):
            raise ValueError("storage route changed since submission; submit again")
    rclone = shutil.which(site.values["rclone_bin"])
    if not rclone:
        raise ValueError("rclone is unavailable; the installed gbi module must load its dependency")
    source, target = Path(specification["source"]), Path(specification["target"])
    source_root, target_root = Path(specification["source_root"]), Path(specification["target_root"])
    state = home / "state"
    for path in (state / "locks", state / "pending", run_dir / "active"):
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if specification["directory"]:
        ensure_parent(target / ".placeholder", target_root)
    stopped = threading.Event()
    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(signum, lambda *_: stopped.set())
    started, last_publish = time.monotonic(), 0.0
    progress = {"phase": "running", "files": 0, "bytes": 0, "freed": 0, "failed": 0,
                "discovered_files": 0, "discovered_bytes": 0, "discovery_complete": False,
                "elapsed": 0, "active": 0, "active_bytes": 0}
    iterator = iter(entries(source, site, source_root, specification["include"]))
    pending = {}
    width = int(site.values["jobs"])
    try:
        with TransferPool(stopped, max_workers=width) as pool:
            while pending or not progress["discovery_complete"]:
                while not stopped.is_set() and not progress["discovery_complete"] and len(pending) < width:
                    entry = next(iterator, None)
                    if entry is None:
                        progress["discovery_complete"] = True
                        break
                    path, empty = entry
                    destination = target / path.relative_to(source) if specification["directory"] else target
                    if empty:
                        ensure_parent(destination / ".placeholder", target_root)
                        if specification["delete"] and path != source:
                            path.rmdir()
                        continue
                    encode = path.is_symlink() and specification["target_kind"] == "alluxio"
                    decode = specification["source_kind"] == "alluxio" and path.name.endswith(".rclonelink")
                    if path.name.endswith(".rclonelink") and not decode:
                        raise ValueError(f"reserved symlink representation suffix: {path}")
                    if encode:
                        destination = destination.with_name(destination.name + ".rclonelink")
                    elif decode and specification["target_kind"] != "alluxio":
                        destination = destination.with_name(destination.name[:-len(".rclonelink")])
                    active = run_dir / "active" / (uuid.uuid4().hex + ".json")
                    task = {**specification, "source": str(path), "target": str(destination),
                            "state": str(state), "progress": str(active), "rclone": rclone,
                            "receipt": str(run_dir / "receipts.jsonl"), "encode_link": encode or
                            (decode and specification["target_kind"] == "alluxio"), "decode_link": decode,
                            "selection_root": str(source if specification["directory"] else source.parent),
                            "settle_seconds": int(site.values["verify_settle_seconds"])}
                    progress["discovered_files"] += 1
                    progress["discovered_bytes"] += path.lstat().st_size
                    future = pool.submit(supervise, task, int(site.values["file_timeout"]), stopped)
                    pending[future] = (path, active)
                if stopped.is_set():
                    progress["discovery_complete"] = True
                done, _ = wait(pending, timeout=0.5, return_when=FIRST_COMPLETED)
                for future in done:
                    path, active = pending.pop(future)
                    outcome = future.result()
                    active.unlink(missing_ok=True)
                    if "error" in outcome:
                        progress["failed"] += 1
                        record = {"event": "failed", "source": str(path), "error": outcome["error"], "time": time.time()}
                        receipt(run_dir / "receipts.jsonl", record)
                        print(f"FAILED {path}: {outcome['error']}", flush=True)
                    else:
                        progress["files"] += 1
                        progress["bytes"] += outcome["bytes"]
                        progress["freed"] += outcome["bytes"] if outcome["deleted"] else 0
                        print(f"VERIFIED {'MOVED' if outcome['deleted'] else 'COPIED'} {path} bytes={outcome['bytes']}", flush=True)
                if time.monotonic() - last_publish >= 2:
                    progress["elapsed"] = time.monotonic() - started
                    progress["active"] = len(pending)
                    progress["active_bytes"] = 0
                    progress["phases"] = {}
                    for _, active in pending.values():
                        try:
                            detail = json.loads(active.read_text())
                            progress["active_bytes"] += detail["bytes"]
                            phase = detail["phase"]
                            progress["phases"][phase] = progress["phases"].get(phase, 0) + 1
                        except (OSError, ValueError, KeyError):
                            pass
                    write_json(run_dir / "progress.json", progress)
                    print(jobs.display(progress), flush=True)
                    last_publish = time.monotonic()
    except (OSError, ValueError) as error:
        stopped.set()
        progress["failed"] += 1
        print(f"FAILED discovery: {error}", flush=True)
    progress["phase"] = "interrupted" if stopped.is_set() else "failed" if progress["failed"] else "complete"
    progress["elapsed"] = time.monotonic() - started
    progress["active"] = 0
    progress["active_bytes"] = 0
    progress["phases"] = {}
    print(jobs.display(progress), flush=True)
    print(f"{progress['phase'].capitalize()}. Receipts: {run_dir / 'receipts.jsonl'}", flush=True)
    write_json(run_dir / "progress.json", {**progress, "phase": "saving history"})
    try:
        history = publish_history(run_dir, site, state, rclone, progress, stopped)
        print(f"History: {history}", flush=True)
        # All history files passed independent readback. The open Slurm log
        # descriptor can now disappear with this temporary run directory.
        shutil.rmtree(run_dir)
    except (OSError, ValueError) as error:
        print(f"Could not save history to Alluxio: {error}. Temporary records retained: {run_dir}", flush=True)
        write_json(run_dir / "progress.json", {**progress, "phase": "failed", "history_error": str(error)})
        return 1
    return 0 if progress["phase"] == "complete" else 1


def publish_history(run_dir, site, state, rclone, progress, stopped):
    """Publish closed immutable files; never append logs through Alluxio FUSE."""
    archive_root = site.roots["alluxio"]
    site.check_root(archive_root)
    job_id = str(os.environ.get("SLURM_JOB_ID", run_dir.name))
    destination = archive_root / ".gbi" / "transfers" / job_id / run_dir.name
    records = run_dir / "records"
    records.mkdir(exist_ok=True)
    sys.stdout.flush()
    sys.stderr.flush()
    for name in ("request.json", "receipts.jsonl", "slurm.out", "slurm.json"):
        original = run_dir / name
        if original.exists():
            shutil.copyfile(original, records / name)
    write_json(records / "progress.json", progress)
    # The publication receipt remains temporary on scratch. The immutable
    # destination history records describe the data transfer, not themselves.
    paths = sorted(records.iterdir(), key=lambda path: path.name == "progress.json")
    for path in paths:
        task = {"source": str(path), "target": str(destination / path.name),
                "source_root": str(site.roots["lustre"]), "target_root": str(archive_root),
                "selection_root": str(records), "state": str(state),
                "receipt": str(run_dir / "publication.jsonl"), "progress": str(run_dir / "publication-progress.json"),
                "rclone": rclone, "settle_seconds": int(site.values["verify_settle_seconds"]),
                "target_kind": "alluxio", "delete": False, "encode_link": False, "decode_link": False}
        outcome = supervise(task, int(site.values["file_timeout"]), stopped)
        if "error" in outcome:
            raise ValueError(outcome["error"])
    return destination


def main():
    options = parser().parse_args()
    os.umask(0o077)
    try:
        configuration = os.environ.get("GBI_DATA_SITE_CONF")
        if not configuration:
            raise ValueError("load the configured gbi module first (module load gbi)")
        site = Site(configuration)
        # Scratch holds only active work. Completed records are immutable files
        # in Alluxio; FSS is reserved for code and configuration.
        if "lustre" not in site.roots or "alluxio" not in site.roots:
            raise ValueError("the gbi module needs Lustre scratch and Alluxio archive roots")
        home = site.roots["lustre"] / ".gbi"
        if options.verb == "roots":
            for name, root in site.roots.items():
                print(f"{name:8} {root}")
            return 0
        if options.verb == "status":
            return jobs.watch(jobs.locate(home, options.job_id, site.roots["alluxio"]), options.job_id,
                              options.watch, site.roots["alluxio"])
        if options.verb == "_run":
            return run(options.run_dir.resolve(), site, home)
        specification = plan(options, site)
        if not options.local and not site.values["partition"]:
            raise ValueError("the gbi module has no Slurm partition configured")
        print(f"{specification['source_kind']} → {specification['target_kind']}: "
              f"{specification['source']} → {specification['target']}")
        print("Sources: delete each verified file." if specification["delete"] else "Sources: keep originals.")
        if options.verb == "move" and specification["source_kind"] == "alluxio" and not specification["delete"]:
            print("Alluxio/Object Storage originals stay. Use --delete-source to explicitly remove them.")
        if options.dry_run:
            print("Dry run: no job submitted and no files written.")
            return 0
        if options.local and not os.environ.get("SLURM_JOB_ID"):
            raise ValueError("--local runs inside an existing Slurm job; omit it to submit automatically")
        site.check_root(site.roots["lustre"])
        site.check_root(site.roots["alluxio"])
        run_dir = home / "runs" / (time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:12])
        run_dir.mkdir(parents=True, mode=0o700)
        # A queued job uses the exact code and settings that were submitted,
        # even if the module's default version changes before it starts.
        runtime = run_dir / "runtime" / "gbi_data"
        shutil.copytree(Path(__file__).parent, runtime, ignore=shutil.ignore_patterns("__pycache__"))
        shutil.copyfile(site.path, run_dir / "site.conf")
        source_hash = hashlib.sha256()
        for path in sorted(runtime.glob("*.py")):
            source_hash.update(path.name.encode() + b"\0" + path.read_bytes())
        specification["code_sha256"] = source_hash.hexdigest()
        specification["version"] = __version__
        write_json(run_dir / "request.json", specification)
        if options.local:
            return run(run_dir, site, home)
        command = [sys.executable, "-B", "-m", "gbi_data.cli", "data", "_run", str(run_dir)]
        job_id = jobs.submit(run_dir, command, site)
        print(f"Submitted transfer {job_id}. Reconnect: gbi data status {job_id} --watch")
        if not options.detach and (options.wait or sys.stdout.isatty()):
            return jobs.watch(run_dir, job_id, archive_root=site.roots["alluxio"])
        return 0
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"gbi data: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
