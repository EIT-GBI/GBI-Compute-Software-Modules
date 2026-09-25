"""Path-based data movement with direct Slurm execution and terminal progress."""

import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid

from . import __version__
from . import jobs
from . import deadlines
from . import prefect
from . import format_selection, packing, usage
from .selection import entries, probe, stream_entries
from .storage import Site, overlap
from .transfer import ensure_parent, receipt, write_json


def byte_size(value):
    match = re.fullmatch(r"([1-9][0-9]*)([KMGT]?)(?:i?B)?", value, re.IGNORECASE)
    if not match:
        raise argparse.ArgumentTypeError("use a positive size such as 64MiB, 1GiB or a byte count")
    return int(match[1]) * 1024 ** " KMGT".index(match[2].upper() or " ")


def parser():
    result = argparse.ArgumentParser(prog="gbi", description="Move data between your HPC filesystems.")
    result.add_argument("--version", action="version", version=__version__)
    data = result.add_subparsers(dest="command", required=True).add_parser("data")
    verbs = data.add_subparsers(dest="verb", required=True, metavar="{copy,move,status,retry,roots,usage}")
    for verb in ("copy", "move"):
        command = verbs.add_parser(
            verb, help=f"{verb} a file or directory to a destination",
            description=(
                "Copy and independently verify each file." if verb == "copy" else
                "Copy and independently verify each file before removing its source.\n"
                "Alluxio/Object Storage originals stay unless --delete-source is given."
            ),
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog=(
                "A directory's contents go into the exact destination directory you name.\n"
                "Identical destinations are verified and reused; different files are never\n"
                "overwritten. Re-run the same command after an interruption.\n\n"
                "By default, up to 8 GiB runs in your shell; larger or slow-to-scan trees\n"
                "use Slurm (site settings may differ). Existing allocations are reused.\n"
                "Ctrl-C stops foreground work; when following a submitted Slurm job, it\n"
                "only detaches the display. Reconnect with: gbi data status ID --watch\n\n"
                "--prefect submits individual files; it does not create tar/gzip archives.\n"
                "FSS archives\n"
                "and restores require matching relative paths. Exclusions, packing, chunks\n"
                "and deleting Object Storage originals are unavailable on this route.\n"
                "After a lost submission reply: gbi data retry ID (same saved request).\n\n"
                f"Examples:\n  gbi data {verb} SOURCE DESTINATION --dry-run\n"
                f"  gbi data {verb} SOURCE DESTINATION --detach\n"
                f"  gbi data {verb} SOURCE DESTINATION --detach --wait\n"
                f"  gbi data {verb} SOURCE DESTINATION --prefect --wait\n"
                f"  gbi data {verb} SOURCE DESTINATION --include '*.pt' --exclude 'temp*'"
            ),
        )
        command.add_argument("source")
        command.add_argument("destination")
        command.add_argument("--include", action="append", default=[], metavar="GLOB",
                             help="select matching file names; case-sensitive, repeat for alternatives")
        command.add_argument("--exclude", action="append", default=[], metavar="GLOB",
                             help="skip matching file names; case-sensitive, repeatable, overrides --include")
        archive = command.add_mutually_exclusive_group()
        archive.add_argument("--pack", choices=("tar", "gzip"),
                             help="pack one directory into .gbi.tar or .gbi.tar.gz; very large file lists need smaller folders")
        archive.add_argument("--pack-small", action="store_true",
                             help="pack small-file subdirectories that fit archive limits; preview with --dry-run")
        command.add_argument("--chunk-size", type=byte_size, metavar="SIZE",
                             help="store verified, resumable parts of this size, e.g. 64MiB (adds .gbi-chunks)")
        command.add_argument("--delete-source", action="store_true",
                             help="explicitly allow source deletion when moving OUT of Alluxio/Object Storage")
        command.add_argument("--dry-run", action="store_true", help="show route and policy without writing")
        execution = command.add_mutually_exclusive_group()
        execution.add_argument("--detach", action="store_true", help="submit to Slurm and return immediately, unless --wait is given")
        command.add_argument("--wait", action="store_true", help="wait for completion, including with --detach or in scripts")
        execution.add_argument("--local", action="store_true", help="execute inside an existing Slurm allocation")
        execution.add_argument("--prefect", action="store_true",
                               help="submit a personal Object Storage migration through Prefect; no UI or credentials needed")
    status = verbs.add_parser("status", help="show a transfer's progress")
    status.add_argument("job_id", metavar="JOB_OR_TRANSFER_ID",
                        help="Slurm job ID, or a printed transfer ID when the job has several transfers")
    status.add_argument("--watch", action="store_true", help="follow until the transfer finishes")
    retry = verbs.add_parser("retry", help="resend an unchanged saved Prefect request after a lost reply")
    retry.add_argument("job_id", metavar="TRANSFER_ID")
    retry.add_argument("--wait", action="store_true", help="wait for the existing migration's result")
    verbs.add_parser("roots", help="show your available storage paths")
    usage_command = verbs.add_parser(
        "usage", help="show live quota and your cached Lustre folder usage",
        description="Show your live Lustre quota and the latest owner-scoped inventory snapshot.",
    )
    usage_command.add_argument("path", nargs="?", help="directory relative to your own Lustre root")
    usage_command.add_argument("--depth", type=int, default=1, help="folder levels below the selected root (default: 1)")
    usage_command.add_argument("--limit", type=int, default=20, help="top folders returned at each level (default: 20)")
    # Internal entrypoint used by the generated batch script.
    run = verbs.add_parser("_run")
    run.add_argument("run_dir", type=Path)
    return result


def plan(options, site):
    deadlines.event("resolving source", options.source)
    source, source_kind, source_root = site.classify(options.source)
    deadlines.event("resolving destination", options.destination)
    target, target_kind, target_root = site.classify(options.destination)
    requested_target = str(target)
    deadlines.event("checking source", source)
    if not os.path.lexists(source):
        raise ValueError(f"source does not exist: {source}")
    if source.is_symlink():
        directory = False
    else:
        directory = source.is_dir()
    deadlines.event("checking destination", target)
    if not directory and target.is_dir():
        target /= source.name
    if overlap(source, target):
        raise ValueError("source and destination must not overlap")
    if options.delete_source and options.verb != "move":
        raise ValueError("--delete-source is available with move only")
    delete = options.verb == "move" and (source_kind != "alluxio" or options.delete_source)
    specification = {"source": str(source), "target": str(target), "source_kind": source_kind,
            "target_kind": target_kind, "source_root": str(source_root), "target_root": str(target_root),
            "directory": directory, "delete": delete, "include": options.include, "exclude": options.exclude,
            "verb": options.verb, "uid": os.getuid(), "site": str(site.path),
            "requested_target": requested_target}
    policy = packing.PackingPolicy(
        small_file_bytes=int(site.values["pack_small_bytes"]),
        min_files=int(site.values["pack_min_files"]),
        max_archive_bytes=int(site.values["pack_max_bytes"]),
        max_entries=int(site.values["inline_scan_entries"]),
        max_seconds=float(site.values["inline_probe_seconds"]),
    ) if getattr(options, "pack_small", False) else None
    return format_selection.configure(specification, site, pack=getattr(options, "pack", None),
                                      chunk_size=getattr(options, "chunk_size", None), packing_policy=policy)


def execution_plan(specification, site, options):
    allocated = bool(os.environ.get("SLURM_JOB_ID"))
    if options.local and not allocated:
        raise ValueError("--local requires an existing Slurm allocation")
    selected = None if options.detach else probe(specification, site)
    mode = "slurm" if options.detach else "allocation" if allocated else "inline" if selected is not None else "slurm"
    native = selected is not None and len(selected) <= int(site.values["native_files"]) and sum(
        item.get("format_source_size", (item.get("expected_source") or item.get("fingerprint"))[3])
        for item in selected if not item["empty"]
    ) <= int(site.values["native_bytes"])
    return {**specification, "execution": mode, "selection": selected,
            "reader": "native" if native else "rclone"}


def supervise(task, timeout, stopped):
    """Bound all per-file I/O, including close and verification, in a process."""
    process = subprocess.Popen(
        [sys.executable, "-B", "-m", "gbi_data.transfer"], stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, text=True, start_new_session=True,
    )
    deadlines.event(worker_started=process.pid)
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
            unresolved = process.poll() is None
            deadlines.event(worker_finished=process.pid, **({"unresolved": process.pid} if unresolved else {}))
            return {"error": f"interrupted or timed out: worker PID {process.pid}; "
                    + ("kernel I/O may still be active; confirm terminal state before retrying; " if unresolved else "")
                    + "inspect receipts before retrying"}
        try:
            process.wait(timeout=0.2)
        except subprocess.TimeoutExpired:
            pass
    output = process.stdout.read()
    process.stdout.close()
    deadlines.event(worker_finished=process.pid)
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
    deadlines.event("reading request", run_dir, records=str(run_dir))
    specification = json.loads((run_dir / "request.json").read_text())
    if specification["uid"] != os.getuid():
        raise ValueError("this request belongs to another user")
    # Recheck both storage roots on the execution node. A missing mount never
    # becomes a new local directory masquerading as the intended filesystem.
    for prefix in ("source", "target"):
        deadlines.event("checking storage route", specification[prefix])
        path, kind, root = site.classify(specification[prefix])
        if (str(path), kind, str(root)) != (specification[prefix], specification[prefix + "_kind"],
                                         specification[prefix + "_root"]):
            raise ValueError("storage route changed since submission; submit again")
    selected = specification.get("selection")
    inline = specification.get("execution") == "inline"
    native = specification.get("reader") == "native"
    rclone = None if native else shutil.which(site.values["rclone_bin"])
    if not native and not rclone:
        raise ValueError("rclone is unavailable; the installed gbi module must load its dependency")
    source, target = Path(specification["source"]), Path(specification["target"])
    source_root, target_root = Path(specification["source_root"]), Path(specification["target_root"])
    state = home / "state"
    deadlines.event("preparing transfer state", state)
    for path in (state / "locks", state / "pending", run_dir / "active"):
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if specification["directory"]:
        deadlines.event("preparing destination", target)
        ensure_parent(target / ".placeholder", target_root)
    stopped = threading.Event()
    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(signum, lambda *_: stopped.set())
    started, last_publish, discovery_wait = time.monotonic(), 0.0, 0.0
    discovery_progress = None
    progress = {"phase": "running", "files": 0, "bytes": 0, "freed": 0, "failed": 0,
                "discovered_files": 0, "discovered_bytes": 0, "discovery_complete": False,
                "elapsed": 0, "active": 0, "active_bytes": 0,
                "coordinator_pid": os.getpid(), "hostname": socket.gethostname(),
                "coordinator_identity": jobs.process_identity(os.getpid())}
    iterator = iter(selected) if selected is not None else None
    discovery = None if selected is not None else stream_entries(
        source, site, source_root, specification["include"], exclusions=specification.get("exclude", []),
        **({"iterator_factory": lambda on_error, visit: format_selection.iter_units(specification, site, on_error, visit)}
           if specification.get("format_units") else {}))
    file_specification = {key: value for key, value in specification.items() if key != "selection"}
    pending = {}
    counted_bytes = {}
    unknown_sizes = set()
    width = int(site.values["jobs"])
    output = jobs.ProgressDisplay()

    def record_failure(path, error):
        progress["failed"] += 1
        record = {"event": "failed", "source": str(path), "error": str(error), "time": time.time()}
        deadlines.event("writing failure receipt", run_dir / "receipts.jsonl", counters=progress)
        receipt(run_dir / "receipts.jsonl", record)
        output.finish()
        print(f"{jobs.timestamp()} FAILED {path}: {error}", flush=True)

    def schedule(entry):
        if isinstance(entry, dict):
            unit = {**entry, "source": entry.get("source", entry.get("path")),
                    "expected_source": entry.get("expected_source", entry.get("fingerprint"))}
            path, empty = Path(unit["source"]), unit["empty"]
        else:
            path, empty = entry
            unit = {}
        active = run_dir / "active" / (uuid.uuid4().hex + ".json")
        task = {**file_specification, "source": str(path), "target": str(target),
                "state": str(state), "progress": str(active), "rclone": rclone,
                "receipt": str(run_dir / "receipts.jsonl"),
                "selection_root": str(source if specification["directory"] else source.parent),
                "settle_seconds": int(site.values["verify_settle_seconds"]),
                "expected_source": None,
                "max_bytes": int(site.values["inline_bytes"]) if selected is not None else None,
                "native_bytes": int(site.values["native_bytes"]),
                "target_base": True,
                "empty": empty, **unit}
        if not empty:
            progress["discovered_files"] += 1
        if unit.get("decoded_size_unknown") or (unit.get("format_action") == "pack" and
                                                "format_source_size" not in unit):
            unknown_sizes.add(active)
        future = pool.submit(supervise, task, int(site.values["file_timeout"]), stopped)
        pending[future] = (path, active, empty)

    def account_source_size(active, source_size=None):
        if active in unknown_sizes:
            return
        if source_size is None:
            try:
                deadlines.event("reading worker progress", active, counters=progress)
                source_size = json.loads(active.read_text()).get("source_size")
            except (OSError, ValueError, TypeError):
                return
        if isinstance(source_size, int) and source_size >= 0:
            progress["discovered_bytes"] += source_size - counted_bytes.get(active, 0)
            counted_bytes[active] = source_size

    try:
        with TransferPool(stopped, max_workers=width) as pool:
            while pending or (not progress["discovery_complete"] and not stopped.is_set()):
                event = None
                if not stopped.is_set() and not progress["discovery_complete"] and (
                    len(pending) < width):
                    if selected is not None:
                        entry = next(iterator, None)
                        event = ("done", None, None) if entry is None else ("entry", entry, None)
                    else:
                        waiting_since = time.monotonic()
                        event = discovery.get(0.1 if pending else 0.5)
                        if event is None:
                            progress_info = getattr(discovery, "progress", None)
                            last_progress = progress_info()[1] if callable(progress_info) else None
                            now = time.monotonic()
                            discovery_wait += now - waiting_since
                            if isinstance(last_progress, (int, float)) and last_progress != discovery_progress:
                                # Only time spent waiting with worker capacity counts.
                                discovery_wait = max(0, now - max(waiting_since, last_progress))
                            discovery_progress = last_progress
                        else:
                            discovery_wait = 0
                        if discovery_wait >= float(site.values["discovery_timeout"]):
                            raise ValueError(f"discovery deadline exceeded at {source}; "
                                             "inspect receipts before retrying; mount I/O may still be unresolved")
                    if event is not None:
                        kind, value, error = event
                        if kind == "entry":
                            schedule(value)
                        elif kind == "error":
                            record_failure(value, f"source lookup failed: {error}")
                        else:
                            progress["discovery_complete"] = True
                            if kind == "fatal":
                                record_failure(source, f"discovery failed: {error}")
                            if discovery is not None:
                                discovery.stop()
                if stopped.is_set():
                    if discovery is not None:
                        discovery.stop()
                done, _ = wait(pending, timeout=0 if event is not None else 0.5,
                               return_when=FIRST_COMPLETED)
                for future in done:
                    path, active, empty = pending.pop(future)
                    outcome = future.result()
                    if "error" not in outcome:
                        unknown_sizes.discard(active)
                    if not empty:
                        account_source_size(active, outcome.get("source_size"))
                    deadlines.event("retiring worker progress", active, counters=progress)
                    active.unlink(missing_ok=True)
                    if "error" in outcome:
                        record_failure(path, outcome["error"])
                    elif not empty:
                        progress["files"] += 1
                        progress["bytes"] += outcome["bytes"]
                        progress["freed"] += outcome.get("freed_bytes", outcome["bytes"] if outcome["deleted"] else 0)
                        if not output.terminal:
                            print(f"{jobs.timestamp()} VERIFIED {'MOVED' if outcome['deleted'] else 'COPIED'} {path} bytes={outcome['bytes']}", flush=True)
                if time.monotonic() - last_publish >= 2:
                    progress["elapsed"] = time.monotonic() - started
                    progress["active"] = len(pending)
                    progress["totals_known"] = not unknown_sizes
                    progress["active_bytes"] = 0
                    progress["phases"] = {}
                    for _, active, empty in pending.values():
                        if empty:
                            continue
                        try:
                            deadlines.event("reading worker progress", active, counters=progress)
                            detail = json.loads(active.read_text())
                            account_source_size(active, detail.get("source_size"))
                            progress["active_bytes"] += detail["bytes"]
                            phase = detail["phase"]
                            progress["phases"][phase] = progress["phases"].get(phase, 0) + 1
                        except (OSError, ValueError, KeyError):
                            pass
                    deadlines.event("publishing progress", run_dir, counters=progress)
                    write_json(run_dir / "progress.json", progress)
                    output.update(progress)
                    deadlines.event("transferring" if pending else "discovering", source, counters=progress)
                    last_publish = time.monotonic()
    except (OSError, ValueError) as error:
        stopped.set()
        progress["failed"] += 1
        output.finish()
        print(f"FAILED discovery: {error}", flush=True)
    finally:
        if discovery is not None:
            discovery.stop()
    progress["phase"] = "failed" if progress["failed"] else "interrupted" if stopped.is_set() else "complete"
    progress["elapsed"] = time.monotonic() - started
    progress["active"] = 0
    progress["totals_known"] = not unknown_sizes
    progress["active_bytes"] = 0
    progress["phases"] = {}
    output.finish()
    print(f"Data phase: {progress['phase']}. Receipts: {run_dir / 'receipts.jsonl'}", flush=True)
    deadlines.event("publishing final progress", run_dir, counters=progress)
    write_json(run_dir / "progress.json", {**progress, "phase": "saving history"})
    output.update({**progress, "phase": "saving history"})
    output.finish()
    try:
        deadlines.event("saving history", site.roots["alluxio"], counters=progress)
        history = publish_history(run_dir, site, state, rclone, progress, stopped, inline)
        print(f"History: {history}", flush=True)
    except (OSError, ValueError) as error:
        print(f"Could not save history to Alluxio: {error}. Temporary records retained: {run_dir}", flush=True)
        deadlines.event("recording history failure", run_dir, counters=progress)
        write_json(run_dir / "progress.json", {**progress, "phase": "failed", "history_error": str(error)})
        return 1
    # History is already verified and durable. A cleanup failure must not turn
    # successful publication into a transfer failure or an endless status wait.
    deadlines.event("removing archived scratch records", run_dir, counters=progress)
    try:
        write_json(run_dir / "progress.json", {**progress, "history": str(history)})
        shutil.rmtree(run_dir)
    except OSError as error:
        print(f"History saved; temporary records could not be removed: {run_dir}: {error}", flush=True)
    print(f"{progress['phase'].capitalize()}. History: {history}", flush=True)
    return 0 if progress["phase"] == "complete" else 1


def publish_history(run_dir, site, state, rclone, progress, stopped, inline=False):
    """Publish closed immutable files; never append logs through Alluxio FUSE."""
    archive_root = site.roots["alluxio"]
    site.check_root(archive_root)
    job_id = run_dir.name if inline else str(os.environ.get("SLURM_JOB_ID", run_dir.name))
    destination = archive_root / ".gbi" / "transfers" / job_id / run_dir.name
    records = run_dir / "records"
    records.mkdir(exist_ok=True)
    sys.stdout.flush()
    sys.stderr.flush()
    if inline:
        receipts = run_dir / "receipts.jsonl"
        write_json(records / "history.json", {
            "request": json.loads((run_dir / "request.json").read_text()), "progress": progress,
            "receipts": [json.loads(line) for line in receipts.read_text().splitlines()] if receipts.exists() else [],
        })
    else:
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
        deadlines.event("loading site and storage roots", configuration)
        site = Site(configuration, configured=deadlines.configure)
        deadlines.event("checking request paths", getattr(options, "source", configuration))
        # Scratch holds only active work. Completed records are immutable files
        # in Alluxio; FSS is reserved for code and configuration.
        if options.verb == "usage":
            if "lustre" not in site.roots:
                raise ValueError("the gbi module needs a Lustre scratch root for usage")
            if options.depth < 1 or options.limit < 1:
                raise ValueError("usage depth and limit must be positive")
            usage.print_report(usage.report(site, options.path, options.depth, options.limit),
                               options.depth, options.limit)
            return 0
        if "lustre" not in site.roots or "alluxio" not in site.roots:
            raise ValueError("the gbi module needs Lustre scratch and Alluxio archive roots")
        home = site.roots["lustre"] / ".gbi"
        if options.verb == "roots":
            for name, root in site.root_aliases.items():
                print(f"{name:8} {root}")
            return 0
        if options.verb in ("status", "retry"):
            run_dir = jobs.locate(home, options.job_id, site.roots["alluxio"])
            if options.verb == "retry":
                return prefect.submit(run_dir, site, options.wait or sys.stdout.isatty())
            request = run_dir / "request.json"
            if request.is_file() and json.loads(request.read_text()).get("execution") == "prefect":
                return prefect.follow(run_dir, site, options.watch)
            return jobs.watch(run_dir, options.job_id, options.watch, site.roots["alluxio"])
        if options.verb == "_run":
            return run(options.run_dir.resolve(), site, home)
        if options.prefect:
            return prefect.start(options, site, home)
        specification = execution_plan(plan(options, site), site, options)
        if specification["execution"] == "slurm" and not site.values["partition"]:
            raise ValueError("the gbi module has no Slurm partition configured")
        print(f"{specification['source_kind']} → {specification['target_kind']}: "
              f"{specification['source']} → {specification['target']}")
        print("Sources: delete each verified file." if specification["delete"] else "Sources: keep originals.")
        if options.verb == "move" and specification["source_kind"] == "alluxio" and not specification["delete"]:
            print("Alluxio/Object Storage originals stay. Use --delete-source to explicitly remove them.")
        mode = specification["execution"]
        print({"inline": "Foreground transfer. Ctrl-C stops the transfer.",
               "allocation": "Using your existing Slurm allocation.",
               "slurm": "Submitting background work to Slurm."}[mode])
        if options.dry_run:
            if specification.get("pack") or specification.get("packing_policy") or specification.get("chunk_size") or specification.get("source_format"):
                deadlines.event("planning archive layout", specification["source"])
                print(json.dumps(format_selection.dry_run(specification, site), indent=2))
            print("Dry run: no job submitted and no files written.")
            return 0
        deadlines.event("checking scratch root", site.roots["lustre"])
        site.check_root(site.roots["lustre"])
        deadlines.event("checking archive root", site.roots["alluxio"])
        site.check_root(site.roots["alluxio"])
        run_dir = home / "runs" / (time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:12])
        deadlines.event("preparing request snapshot", run_dir, records=str(run_dir))
        run_dir.mkdir(parents=True, mode=0o700)
        # A queued job uses the exact code and settings that were submitted,
        # even if the module's default version changes before it starts.
        runtime = run_dir / "runtime" / "gbi_data"
        shutil.copytree(Path(__file__).parent, runtime, ignore=shutil.ignore_patterns("__pycache__"))
        # Installed source may be read-only. This private, caller-owned copy
        # must remain removable after its verified history has been published.
        for directory in (runtime, *(path for path in runtime.rglob("*") if path.is_dir())):
            directory.chmod(0o700)
        shutil.copyfile(site.path, run_dir / "site.conf")
        source_hash = hashlib.sha256()
        for path in sorted(runtime.glob("*.py")):
            source_hash.update(path.name.encode() + b"\0" + path.read_bytes())
        specification["code_sha256"] = source_hash.hexdigest()
        specification["version"] = __version__
        if mode == "allocation":
            specification["allocation_job_id"] = os.environ["SLURM_JOB_ID"]
        write_json(run_dir / "request.json", specification)
        if mode != "slurm":
            if mode == "allocation":
                print(f"Transfer {run_dir.name}. Reconnect: gbi data status {run_dir.name} --watch", flush=True)
            else:
                print(f"Transfer {run_dir.name}. History: gbi data status {run_dir.name}")
            return run(run_dir, site, home)
        command = [sys.executable, "-B", "-m", "gbi_data.cli", "data", "_run", str(run_dir)]
        job_id = jobs.submit(run_dir, command, site)
        print(f"Submitted transfer {job_id}. Reconnect: gbi data status {job_id} --watch")
        if options.wait or (not options.detach and sys.stdout.isatty()):
            return jobs.watch(run_dir, job_id, archive_root=site.roots["alluxio"])
        return 0
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"gbi data: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(deadlines.entry(main))
