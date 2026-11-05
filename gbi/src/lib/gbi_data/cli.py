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
from . import cache, moves, prefect, retry
from . import format_selection, packing, roots, usage
from .selection import entries, probe, stream_entries
from .storage import Site, fingerprint, overlap
from .transfer import (
    can_remove_source_root, ensure_parent, receipt, remove_empty_source_root,
    validate_source_retention, write_json,
)


class _ArgumentParser(argparse.ArgumentParser):
    """Keep argparse's usage output, but make common mode conflicts actionable."""

    def error(self, message):
        conflict = re.search(
            r"argument (--\S+): not allowed with argument (--\S+)", message
        )
        if conflict:
            arguments = conflict.groups()
            if any(
                argument in {"--detach", "--local", "--prefect"}
                for argument in arguments
            ):
                alternative = "choose one execution mode (omit all three for automatic selection)"
            else:
                alternative = "choose one archive format or omit both for an ordinary file transfer"
            message = (
                f"unsupported option combination: {arguments[0]} and {arguments[1]}; "
                + alternative
            )
        super().error(message)


def byte_size(value):
    match = re.fullmatch(
        r"([1-9][0-9]*)([KMGT]?)(?:i?B)?", value, re.IGNORECASE
    )
    if not match:
        raise argparse.ArgumentTypeError(
            "use a positive size such as 64MiB, 1GiB or a byte count"
        )
    return int(match[1]) * 1024 ** " KMGT".index(match[2].upper() or " ")


def command_reference(verbs):
    """Keep overview help complete using the actual command argument definitions."""
    formatter = argparse.RawDescriptionHelpFormatter(prog = "gbi")
    for name, command in verbs.choices.items():
        if name == "copy" or name.startswith("_"):
            continue
        title = (
            "gbi data copy / move SOURCE DESTINATION"
            if name == "move"
            else command.prog
        )
        formatter.start_section(title)
        if name == "move":
            formatter.add_text(
                "copy keeps sources. move removes the source after verification.\n"
                "Moving Object Storage data to Lustre or FSS asks first; without a\n"
                "terminal the source is kept unless you add --delete-source."
            )
        else:
            formatter.add_text(command.description)
        formatter.add_arguments(
            action for action in command._actions if action.dest != "help"
        )
        if name == "move":
            formatter.add_text("\n" + command.epilog)
        formatter.end_section()
    return formatter.format_help()


class _FullHelp(argparse.Action):
    """Expose compatible expert overrides without crowding ordinary help."""

    def __call__(self, command, namespace, values, option_string = None):
        parser(advanced = True).parse_args([*command.prog.split()[1:], "--help"])


def parser(*, advanced = False):
    def expert(description):
        return description if advanced else argparse.SUPPRESS

    result = _ArgumentParser(
        prog = "gbi",
        description = "Move, archive, restore, and inspect your own HPC data.",
        epilog = (
            "Examples:\n"
            "  gbi data copy SOURCE DESTINATION --dry-run\n"
            "  gbi data move SOURCE DESTINATION --detach --wait\n"
            "  gbi data status TRANSFER_ID --watch\n"
            "  gbi data usage projects --depth 2 --limit 10\n\n"
            "Every everyday option is explained below; --help-all includes expert overrides.\n"
            "For a command's usage and examples,\n"
            "use e.g. `gbi data copy --help` or `gbi data usage --help`.\n"
            "Ordinary and --prefect transfers both run with your normal Unix\n"
            "permissions; --prefect needs an Object Storage mount on one side."
        ),
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )
    result.add_argument("--version", action = "version", version = __version__)
    result.add_argument(
        "--help-all", action = _FullHelp, nargs = 0,
        help = "show all options, including expert and compatibility overrides",
    )
    data = result.add_subparsers(dest = "command", required = True).add_parser(
        "data",
        help = "move data, inspect transfer state, or report usage",
        description = (
            "Public data commands. `copy` keeps the source; `move` removes only sources\n"
            "whose verified deletion policy allows it. `status` accepts a saved transfer\n"
            "ID or a cache path; `retry` uses a saved transfer ID. `usage` reads live\n"
            "quota and the owner-scoped Lustre snapshot."
        ),
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )
    data.add_argument(
        "--help-all", action = _FullHelp, nargs = 0,
        help = "show all options, including expert and compatibility overrides",
    )
    verbs = data.add_subparsers(
        dest = "verb", required = True,
        metavar = "{copy,move,status,retry,roots,usage,cache}",
    )
    for verb in ("copy", "move"):
        command = verbs.add_parser(
            verb,
            help = f"{verb} a file or directory to a destination",
            description = (
                (
                    "Copy SOURCE to DESTINATION and independently verify every file.\n"
                    "SOURCE and DESTINATION may be files or directories; the source remains."
                    if verb == "copy"
                    else "Copy SOURCE to DESTINATION and independently verify every "
                    "file, then remove\n"
                    "the source. Moving Object Storage data to Lustre or FSS asks "
                    "first; without a\n"
                    "terminal the source is kept unless you add --delete-source."
                )
            ),
            formatter_class = argparse.RawDescriptionHelpFormatter,
            epilog = (
                "A directory's contents go into the exact destination directory you name.\n"
                "Identical destinations are verified and reused; different files are never\n"
                "overwritten. Re-run the same command after an interruption.\n\n"
                "By default, up to 8 GiB runs in your shell; larger or slow-to-scan trees\n"
                "use Slurm (site settings may differ). Existing allocations are reused.\n"
                "Ctrl-C stops foreground work; when following a submitted Slurm job, it\n"
                "only detaches the display. Reconnect with: gbi data status ID --watch\n"
                "Ordinary transfers keep directly readable file paths. --archive packages a\n"
                "directory under one destination root; GBI chooses archive boundaries and\n"
                "resumable part sizes. Restore that root to Lustre or FSS with "
                "an ordinary copy.\n"
                "No packing or chunk-size choices are needed for restore.\n\n"
                "--prefect submits the transfer through the site's broker: any path you can\n"
                "read to any path you can write on Lustre, FSS and Object Storage mounts,\n"
                "with Object Storage on at least one side. The transfer job checks\n"
                "permissions as you before any data moves.\n"
                "Archive previews may create managed tracking records, but never change data.\n"
                "After a lost submission reply: gbi data retry ID (same saved request).\n\n"
                "Use --help-all for expert overrides and compatibility options.\n\n"
                f"Examples:\n  gbi data {verb} SOURCE DESTINATION --dry-run\n"
                f"  gbi data {verb} SOURCE DESTINATION --archive\n"
                f"  gbi data {verb} SOURCE DESTINATION --detach\n"
                f"  gbi data {verb} SOURCE DESTINATION --detach --wait\n"
                f"  gbi data {verb} SOURCE DESTINATION --prefect --wait\n"
                f"  gbi data {verb} SOURCE DESTINATION --include '*.pt' --exclude 'temp*'"
            ),
        )
        command.add_argument("source", help = "source file or directory path")
        command.add_argument(
            "destination", help = "destination file or directory path"
        )
        command.add_argument(
            "--help-all", action = _FullHelp, nargs = 0,
            help = "show expert and compatibility options as well",
        )
        selection = command.add_argument_group("selection")
        selection.add_argument(
            "--include", action = "append", default = [], metavar = "GLOB",
            help = (
                "select basenames matching any supplied pattern; quote globs "
                "such as '*.pt' (case-sensitive; repeatable; default: all "
                "files)"
            ),
        )
        selection.add_argument(
            "--exclude", action = "append", default = [], metavar = "GLOB",
            help = "skip matching basenames (repeatable; wins over --include)",
        )
        archive = command.add_mutually_exclusive_group()
        archive.add_argument(
            "--archive", action = "store_true",
            help = (
                "archive a directory under one destination root; choose "
                "packing and resumable parts automatically"
            ),
        )
        archive.add_argument(
            "--pack", choices = ("tar", "gzip"),
            help = expert(
                "pack one directory: tar is uncompressed, gzip compresses; "
                "exact destination must end in .gbi.tar or .gbi.tar.gz "
                "(requires matching Prefect broker/flow)"
            ),
        )
        archive.add_argument(
            "--pack-small", action = "store_true",
            help = expert(
                "legacy selective packing: pack eligible small-file "
                "subdirectories, transfer the rest individually; prefer "
                "--archive for a saved whole-tree layout"
            ),
        )
        command.add_argument(
            "--archive-unit", action = "append", default = [],
            metavar = "SOURCE_RELATIVE",
            help = expert(
                "run an exact unit from an existing saved --archive plan; "
                "repeat for multiple units"
            ),
        )
        command.add_argument(
            "--chunk-size", type = byte_size, metavar = "SIZE",
            help = expert(
                "override verified resumable part size, e.g. 64MiB; not "
                "combined with --archive; omit on restore"
            ),
        )
        command.add_argument(
            "--delete-source", action = "store_true",
            help = (
                "remove an Object Storage source after verification without "
                "asking"
                if verb == "move"
                else argparse.SUPPRESS
            ),
        )
        command.add_argument(
            "--dry-run", action = "store_true",
            help = (
                "preview without data writes; Prefect packing/chunking submits "
                "a metadata-only plan and creates tracking records"
            ),
        )
        execution = command.add_mutually_exclusive_group()
        execution.add_argument(
            "--detach", action = "store_true",
            help = "submit to Slurm; return after submission unless --wait",
        )
        command.add_argument(
            "--wait", action = "store_true",
            help = "wait for completion (also valid with --detach or --prefect)",
        )
        execution.add_argument(
            "--local", action = "store_true",
            help = expert(
                "require the current Slurm allocation (normally detected automatically)"
            ),
        )
        execution.add_argument(
            "--prefect", action = "store_true",
            help = (
                "submit through the site's Prefect service; Object Storage on "
                "at least one side"
            ),
        )
        command.add_argument(
            "--job-size", choices = ("small", "large"),
            help = expert(
                "Prefect restore reservation override: small reserves 2 "
                "CPUs/16 GiB; large reserves 8 CPUs/48 GiB; omit to use the "
                "deployment setting"
            ),
        )
    status = verbs.add_parser(
        "status", help = "show transfer progress or a path's cache state",
        description = (
            "Show progress for a Slurm job ID, printed transfer ID, or an Object\n"
            "Storage cache path. A path shows current state for the whole path;\n"
            "a cache transfer ID keeps its original --include selection."
        ),
        epilog = (
            "Examples:\n  gbi data status 123456\n"
            "  gbi data status TRANSFER_ID --watch\n"
            "  gbi data status /mnt/team/my-lab/shared/reference --watch"
        ),
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )
    status.add_argument(
        "job_id", metavar = "JOB_OR_TRANSFER_ID_OR_PATH",
        help = "Slurm job ID, printed transfer ID, or Object Storage path",
    )
    status.add_argument(
        "--watch", action = "store_true",
        help = "follow until the transfer finishes",
    )
    retry = verbs.add_parser(
        "retry",
        help = "resume a saved ordinary transfer or resend a managed request",
        description = (
            "Retry a stopped ordinary transfer in a new run using its saved "
            "paths, filters and receipts. Already moved files are reverified. "
            "A managed request is resent unchanged after a lost reply."
        ),
        epilog = "Example:\n  gbi data retry TRANSFER_ID --wait",
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )
    retry.add_argument(
        "job_id", metavar = "TRANSFER_ID",
        help = "printed transfer ID or unambiguous Slurm job ID",
    )
    retry.add_argument(
        "--wait", action = "store_true",
        help = "wait for the transfer result",
    )
    verbs.add_parser(
        "roots",
        help = "show your storage roots and what each one is for",
        description = (
            "Print your Lustre scratch, FSS home and Object Storage roots, the\n"
            "team folders you belong to and the read-only collections, with a\n"
            "line on what belongs where."
        ),
        epilog = "Example:\n  gbi data roots",
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )
    usage_command = verbs.add_parser(
        "usage",
        help = "show your Lustre and FSS usage and your largest Lustre folders",
        description = (
            "Show a Lustre/FSS storage summary, then your largest Lustre folders.\n"
            "Lustre uses your live UID quota; FSS and the folder report come from the\n"
            "owner-scoped snapshot the site publishes weekly (Sunday 02:00 UTC).\n"
            "PATH is relative to your own Lustre root; omit it for the root report.\n"
            "The folder report is apparent bytes from the dated snapshot, not live\n"
            "recursive usage."
        ),
        epilog = (
            "Examples:\n  gbi data usage\n  gbi data usage projects --depth 2 --limit 10"
        ),
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )
    usage_command.add_argument(
        "path", nargs = "?", metavar = "PATH",
        help = "directory below your own Lustre root (default: root)",
    )
    usage_command.add_argument(
        "--depth", type = int, default = 1,
        help = "levels below PATH to show (default: 1)",
    )
    usage_command.add_argument(
        "--limit", type = int, default = 20,
        help = "top N folders at each level (default: 20)",
    )
    cache_command = verbs.add_parser(
        "cache",
        help = "load paths into the Object Storage cache before a job reads them",
        description = (
            "Load every file below PATH into the Object Storage cache so a job\n"
            "reads warm blocks. The site's broker submits one native load and every\n"
            "storage worker serves it in parallel, so the data lands in the cache\n"
            "faster than any single reader could pull it; nothing is read on this\n"
            "node. Returns when the blocks are cached; repeating it is cheap. Use it\n"
            "as the first step of your job script. Paths must be inside\n"
            "an Object Storage (Alluxio) mount you can read: your member folder, a\n"
            "team or shared mount, or sequencing data under /mnt/instrument-data.\n"
            "--include keeps only files whose name matches a pattern. Exit codes:\n"
            "0 cached, 1 the load failed, 2 refused, 3 stopped waiting at --timeout\n"
            "(the load itself continues)."
        ),
        epilog = (
            "Examples:\n  gbi data cache /mnt/team/my-lab/members/$USER/dataset\n"
            "  gbi data cache /mnt/team/my-lab/shared/reference --timeout 900\n"
            "  gbi data cache /mnt/instrument-data/illumina_nextseq_1/RUN --include '*.fastq.gz,*.bam'\n"
            "  gbi data cache PATH --detach   # then: gbi data status TRANSFER_ID --watch"
        ),
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )
    cache_command.add_argument(
        "paths", nargs = "+", metavar = "PATH",
        help = "file or directory inside an Object Storage mount",
    )
    cache_command.add_argument(
        "--timeout", type = int, default = 0, metavar = "SECONDS",
        help = "stop waiting after this long and exit 3; the load continues (default: wait)",
    )
    cache_command.add_argument(
        "--include", action = "append", default = None, metavar = "PATTERN",
        help = (
            "only files whose name matches this glob; repeatable, and a "
            "comma-separated list is accepted, e.g. --include '*.fastq.gz,*.bam'"
        ),
    )
    cache_command.add_argument(
        "--dry-run", action = "store_true",
        help = "show what would be loaded without submitting anything",
    )
    cache_command.add_argument(
        "--detach", action = "store_true",
        help = "return after submission; reconnect with gbi data status TRANSFER_ID --watch",
    )
    cache_command.add_argument(
        "--json", action = "store_true",
        help = "print one JSON summary line instead of progress",
    )
    # Internal entrypoint used by the generated batch script.  The public
    # subparser metavar above intentionally leaves this choice undocumented.
    run = verbs.add_parser("_run")
    run.add_argument("run_dir", type = Path)
    data.epilog   = command_reference(verbs)
    result.epilog += "\n\n" + data.epilog
    return result


def plan(options, site):
    moves.refuse_copy_deletion(options)
    if getattr(options, "job_size", None) is not None:
        raise ValueError(
            "--job-size is supported only for --prefect restores to Lustre or FSS"
        )
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
    delete = moves.deletes(options, source_kind, target_kind, source)
    if delete:
        moves.check_removable(
            source, int(site.values["inline_scan_entries"]),
            float(site.values["inline_probe_seconds"]),
        )
    specification = {
        "source": str(source), "target": str(target),
        "source_kind": source_kind, "target_kind": target_kind,
        "source_root": str(source_root), "target_root": str(target_root),
        "directory": directory, "delete": delete, "include": options.include,
        "exclude": options.exclude, "verb": options.verb, "uid": os.getuid(),
        "site": str(site.path), "requested_target": requested_target,
    }
    archive_units = getattr(options, "archive_unit", [])
    if archive_units:
        if (
            not options.archive
            or options.verb != "move"
            or options.prefect
            or options.dry_run
        ):
            raise ValueError(
                "--archive-unit requires an executing local/Slurm `data move --archive` request"
            )
        specification["archive_units"] = archive_units
    validate_source_retention(specification, site.storage_roots)
    policy = (
        packing.PackingPolicy(
            small_file_bytes = int(site.values["pack_small_bytes"]),
            min_files = int(site.values["pack_min_files"]),
            max_archive_bytes = int(site.values["pack_max_bytes"]),
            max_entries = int(site.values["inline_scan_entries"]),
            max_seconds = float(site.values["inline_probe_seconds"]),
        )
        if getattr(options, "pack_small", False)
        else None
    )
    specification = format_selection.configure(
        specification, site, pack = getattr(options, "pack", None),
        chunk_size = getattr(options, "chunk_size", None),
        packing_policy = policy,
        archive = getattr(options, "archive", False),
    )
    specification["remove_empty_source_root"] = can_remove_source_root(specification)
    return specification


def execution_plan(specification, site, options):
    allocated = bool(os.environ.get("SLURM_JOB_ID"))
    if options.local and not allocated:
        raise ValueError("--local requires an existing Slurm allocation")
    selected = (
        None
        if options.detach or specification.get("archive_auto")
        else probe(specification, site)
    )
    mode = (
        "slurm"
        if options.detach
        else (
            "allocation"
            if allocated
            else "inline" if selected is not None else "slurm"
        )
    )
    native = (
        selected is not None
        and len(selected) <= int(site.values["native_files"])
        and sum(
            item.get(
                "format_source_size",
                (item.get("expected_source") or item.get("fingerprint"))[3],
            )
            for item in selected
            if not item["empty"]
        )
        <= int(site.values["native_bytes"])
    )
    return {
        **specification, "execution": mode, "selection": selected,
        "reader": "native" if native else "rclone",
    }


def worker_group_stopped(pid):
    try:
        os.killpg(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        pass
    return False


def supervise(task, timeout, stopped):
    """Bound all per-file I/O, including close and verification, in a process."""
    process = subprocess.Popen(
        [sys.executable, "-B", "-m", "gbi_data.transfer"],
        stdin = subprocess.PIPE, stdout = subprocess.PIPE, text = True,
        start_new_session = True,
    )
    deadlines.event(worker_started = process.pid)
    process.stdin.write(json.dumps(task))
    process.stdin.close()
    deadline = time.monotonic() + timeout
    while process.poll() is None:
        if stopped.is_set() or time.monotonic() >= deadline:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout = 5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                try:
                    process.wait(timeout = 1)
                except subprocess.TimeoutExpired:
                    pass
            process.stdout.close()
            unresolved = process.poll() is None or not worker_group_stopped(process.pid)
            deadlines.event(
                worker_finished = process.pid,
                **({"unresolved": process.pid} if unresolved else {}),
            )
            return {
                "workers_stopped": not unresolved,
                "error": f"interrupted or timed out: worker PID {process.pid}; "
                + (
                    "kernel I/O may still be active; confirm terminal state before retrying; "
                    if unresolved
                    else ""
                )
                + "inspect receipts before retrying"
            }
        try:
            process.wait(timeout = 0.2)
        except subprocess.TimeoutExpired:
            pass
    if not worker_group_stopped(process.pid):
        process.stdout.close()
        deadlines.event(worker_finished = process.pid, unresolved = process.pid)
        return {
            "error": "transfer worker group remains active; inspect worker PIDs before retrying",
            "workers_stopped": False,
        }
    output = process.stdout.read()
    process.stdout.close()
    deadlines.event(worker_finished = process.pid)
    if process.returncode:
        return {
            "error": f"transfer worker exited {process.returncode}; inspect receipts",
            "workers_stopped": True,
        }
    try:
        result = json.loads(output)
        if not isinstance(result, dict):
            raise ValueError("worker result must be an object")
        return {**result, "workers_stopped": True}
    except ValueError:
        return {
            "error": "transfer worker returned no valid result; inspect receipts",
            "workers_stopped": True,
        }


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
    if specification.get("archive_auto"):
        from .archive_state import prepared

        if specification["uid"] != os.getuid():
            raise ValueError("this request belongs to another user")
        with prepared(specification, site, home) as planned:
            write_json(run_dir / "request.json", planned)
            return _run(run_dir, site, home)
    return _run(run_dir, site, home)


def _run(run_dir, site, home):
    deadlines.event("reading request", run_dir, records = str(run_dir))
    specification = json.loads((run_dir / "request.json").read_text())
    if specification["uid"] != os.getuid():
        raise ValueError("this request belongs to another user")
    # Recheck both storage roots on the execution node. A missing mount never
    # becomes a new local directory masquerading as the intended filesystem.
    for prefix in ("source", "target"):
        deadlines.event("checking storage route", specification[prefix])
        path, kind, root = site.classify(specification[prefix])
        if (str(path), kind, str(root)) != (
            specification[prefix], specification[prefix + "_kind"],
            specification[prefix + "_root"],
        ):
            raise ValueError(
                "storage route changed since submission; submit again"
            )
    validate_source_retention(specification, site.storage_roots)
    selected = specification.get("selection")
    inline   = specification.get("execution") == "inline"
    native   = specification.get("reader") == "native"
    rclone   = None if native else shutil.which(site.values["rclone_bin"])
    if not native and not rclone:
        raise ValueError(
            "rclone is unavailable; the installed gbi module must load its dependency"
        )
    source, target = Path(specification["source"]), Path(
        specification["target"]
    )
    source_root, target_root = Path(specification["source_root"]), Path(
        specification["target_root"]
    )
    if specification["directory"] and os.path.lexists(source) and (
        source.is_symlink() or not source.is_dir()
    ):
        raise ValueError("saved source directory changed; source kept")
    state = home / "state"
    deadlines.event("preparing transfer state", state)
    for path in (state / "locks", state / "pending", run_dir / "active"):
        path.mkdir(parents = True, exist_ok = True, mode = 0o700)
    if specification["directory"] and not (
        specification.get("retry_from") and not os.path.lexists(source)
    ):
        deadlines.event("preparing destination", target)
        ensure_parent(target / ".placeholder", target_root)
    source_cleanup = None
    if (
        specification.get("remove_empty_source_root")
        and can_remove_source_root(specification) and source.exists()
    ):
        source_cleanup = (fingerprint(source)[:3], fingerprint(target)[:3])
    stopped = threading.Event()
    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(signum, lambda *_: stopped.set())
    started, last_publish, discovery_wait = time.monotonic(), 0.0, 0.0
    discovery_progress = None
    progress = {
        "phase": "running", "files": 0, "reused": 0, "already_moved": 0,
        "workers_stopped": True,
        "bytes": 0, "freed": 0, "failed": 0,
        "discovered_files": 0, "discovered_bytes": 0,
        "discovery_complete": False, "elapsed": 0, "active": 0,
        "active_bytes": 0, "coordinator_pid": os.getpid(),
        "hostname": socket.gethostname(),
        "coordinator_identity": jobs.process_identity(os.getpid()),
    }
    if specification.get("retry_from"):
        progress["copied"] = 0
        progress["original_inventory_known"] = selected is not None
    iterator = iter(selected) if selected is not None else None
    unit_iterator = (
        retry.entries if specification.get("retry_from")
        else format_selection.iter_units
    )
    discovery = (
        None
        if selected is not None
        else stream_entries(
            source, site, source_root, specification["include"],
            exclusions = specification.get("exclude", []),
            **(
                {
                    "iterator_factory": lambda on_error, visit: unit_iterator(
                        specification, site, on_error, visit
                    )
                }
                if specification.get("format_units") or specification.get("retry_from")
                else {}
            ),
        )
    )
    file_specification = {
        key: value for key, value in specification.items()
        if key not in ("selection", "retry_receipts", "retry_missing")
    }
    pending       = {}
    counted_bytes = {}
    unknown_sizes = set()
    width         = int(site.values["jobs"])
    output        = jobs.ProgressDisplay()

    def record_failure(path, error, workers_stopped = True):
        progress["failed"] += 1
        record = {
            "event": "failed", "source": str(path), "error": str(error),
            "time": time.time(), "workers_stopped": workers_stopped,
        }
        deadlines.event(
            "writing failure receipt", run_dir / "receipts.jsonl",
            counters = progress,
        )
        receipt(run_dir / "receipts.jsonl", record)
        output.finish()
        print(f"{jobs.timestamp()} FAILED {path}: {error}", flush = True)

    def schedule(entry):
        nonlocal source_cleanup
        if isinstance(entry, dict):
            unit = {
                **entry,
                "source": entry.get("source", entry.get("path")),
                "expected_source": entry.get(
                    "expected_source", entry.get("fingerprint")
                ),
            }
            path, empty = Path(unit["source"]), unit["empty"]
        else:
            path, empty = entry
            unit = {}
        if unit.get("format_action"):
            if specification.get("retry_from"):
                raise ValueError(
                    "retry encountered an archive/packed unit; repeat its "
                    "original command with recovery state"
                )
            source_cleanup = None
        active = run_dir / "active" / (uuid.uuid4().hex + ".json")
        task = {
            **file_specification, "source": str(path), "target": str(target),
            "state": str(state), "progress": str(active), "rclone": rclone,
            "receipt": str(run_dir / "receipts.jsonl"),
            "selection_root": str(
                source if specification["directory"] else source.parent
            ),
            "settle_seconds": int(site.values["verify_settle_seconds"]),
            "expected_source": None,
            "max_bytes": (
                int(site.values["inline_bytes"])
                if selected is not None
                and not specification.get("archive_auto")
                else None
            ),
            "native_bytes": int(site.values["native_bytes"]),
            "target_base": True, "empty": empty, **unit,
            "retry_receipt": specification.get("retry_receipts", {}).get(str(path)),
        }
        if not empty:
            progress["discovered_files"] += 1
        if unit.get("decoded_size_unknown") or (
            unit.get("format_action") == "pack"
            and "format_source_size" not in unit
        ):
            unknown_sizes.add(active)
        future = pool.submit(
            supervise, task, int(site.values["file_timeout"]), stopped
        )
        pending[future] = (path, active, empty, unit.get("archive_result"))

    def account_source_size(active, source_size = None):
        if active in unknown_sizes:
            return
        if source_size is None:
            try:
                deadlines.event(
                    "reading worker progress", active, counters = progress
                )
                source_size = json.loads(active.read_text()).get("source_size")
            except (OSError, ValueError, TypeError):
                return
        if isinstance(source_size, int) and source_size >= 0:
            progress["discovered_bytes"] += source_size - counted_bytes.get(
                active, 0
            )
            counted_bytes[active] = source_size

    try:
        with TransferPool(stopped, max_workers = width) as pool:
            while pending or (
                not progress["discovery_complete"] and not stopped.is_set()
            ):
                event = None
                if (
                    not stopped.is_set()
                    and not progress["discovery_complete"]
                    and (len(pending) < width)
                ):
                    if selected is not None:
                        entry = next(iterator, None)
                        event = (
                            ("done", None, None)
                            if entry is None
                            else ("entry", entry, None)
                        )
                    else:
                        waiting_since = time.monotonic()
                        event         = discovery.get(0.1 if pending else 0.5)
                        if event is None:
                            progress_info = getattr(discovery, "progress", None)
                            last_progress = (
                                progress_info()[1]
                                if callable(progress_info)
                                else None
                            )
                            now            = time.monotonic()
                            discovery_wait += now - waiting_since
                            if (
                                isinstance(last_progress, (int, float))
                                and last_progress != discovery_progress
                            ):
                                # Only time spent waiting with worker capacity counts.
                                discovery_wait = max(
                                    0, now - max(waiting_since, last_progress)
                                )
                            discovery_progress = last_progress
                        else:
                            discovery_wait = 0
                        if discovery_wait >= float(
                            site.values["discovery_timeout"]
                        ):
                            raise ValueError(
                                f"discovery deadline exceeded at {source}; "
                                "inspect receipts before retrying; mount I/O "
                                "may still be unresolved"
                            )
                    if event is not None:
                        kind, value, error = event
                        if kind == "entry":
                            schedule(value)
                        elif kind == "error":
                            record_failure(
                                value, f"source lookup failed: {error}"
                            )
                        else:
                            progress["discovery_complete"] = True
                            if kind == "fatal":
                                record_failure(
                                    source, f"discovery failed: {error}"
                                )
                            if discovery is not None:
                                discovery.stop()
                if stopped.is_set():
                    if discovery is not None:
                        discovery.stop()
                done, _ = wait(
                    pending, timeout = 0 if event is not None else 0.5,
                    return_when = FIRST_COMPLETED,
                )
                for future in done:
                    path, active, empty, archive_result = pending.pop(future)
                    outcome = future.result()
                    if "error" not in outcome:
                        unknown_sizes.discard(active)
                        if archive_result and (
                            outcome.get("deleted")
                            or not Path(archive_result).exists()
                        ):
                            write_json(Path(archive_result), outcome)
                    if not empty:
                        account_source_size(active, outcome.get("source_size"))
                    deadlines.event(
                        "retiring worker progress", active, counters = progress
                    )
                    active.unlink(missing_ok = True)
                    if "error" in outcome:
                        workers_stopped = not retry.uncertain_worker(outcome)
                        if not workers_stopped:
                            progress["workers_stopped"] = False
                        record_failure(path, outcome["error"], workers_stopped)
                    elif not empty:
                        progress["files"] += 1
                        reused = outcome.get("reused", False)
                        progress["reused"] += int(reused)
                        progress["already_moved"] += int(
                            outcome.get("already_moved", False)
                        )
                        if "copied" in progress:
                            progress["copied"] += int(not reused)
                        progress["bytes"] += outcome["bytes"]
                        progress["freed"] += outcome.get(
                            "freed_bytes",
                            outcome["bytes"] if outcome["deleted"] else 0,
                        )
                        if not output.terminal:
                            action = (
                                "ALREADY MOVED" if outcome.get("already_moved")
                                else "MOVED" if outcome["deleted"]
                                else "REUSED" if reused else "COPIED"
                            )
                            detail = " (reused destination)" if reused and outcome["deleted"] else ""
                            print(
                                f"{jobs.timestamp()} VERIFIED {action} "
                                f"{path} bytes={outcome['bytes']}{detail}",
                                flush = True,
                            )
                if time.monotonic() - last_publish >= 2:
                    progress["elapsed"] = time.monotonic() - started
                    progress["active"] = len(pending)
                    progress["totals_known"] = not unknown_sizes
                    progress["active_bytes"] = 0
                    progress["phases"] = {}
                    for _, active, empty, _ in pending.values():
                        if empty:
                            continue
                        try:
                            deadlines.event(
                                "reading worker progress", active,
                                counters = progress,
                            )
                            detail = json.loads(active.read_text())
                            account_source_size(
                                active, detail.get("source_size")
                            )
                            progress["active_bytes"] += detail["bytes"]
                            phase = detail["phase"]
                            progress["phases"][phase] = (
                                progress["phases"].get(phase, 0) + 1
                            )
                        except (OSError, ValueError, KeyError):
                            pass
                    deadlines.event(
                        "publishing progress", run_dir, counters = progress
                    )
                    write_json(run_dir / "progress.json", progress)
                    output.update(progress)
                    deadlines.event(
                        "transferring" if pending else "discovering",
                        source, counters = progress,
                    )
                    last_publish = time.monotonic()
    except (OSError, ValueError) as error:
        stopped.set()
        progress["workers_stopped"] = progress["workers_stopped"] and not pending
        progress["failed"] += 1
        output.finish()
        print(f"FAILED discovery: {error}", flush = True)
    finally:
        if discovery is not None:
            discovery.stop()
    if (
        source_cleanup is not None and progress["discovery_complete"]
        and not pending and not progress["failed"] and not stopped.is_set()
    ):
        try:
            deadlines.event("removing empty source directory", source, counters = progress)
            remove_empty_source_root(specification, *source_cleanup)
        except (OSError, ValueError) as error:
            record_failure(source, error)
    progress["phase"] = (
        "failed"
        if progress["failed"]
        else "interrupted" if stopped.is_set() else "complete"
    )
    progress["elapsed"] = time.monotonic() - started
    progress["active"] = 0
    progress["totals_known"] = not unknown_sizes
    progress["active_bytes"] = 0
    progress["phases"] = {}
    output.finish()
    print(
        f"Data phase: {progress['phase']}. Receipts: {run_dir / 'receipts.jsonl'}",
        flush = True,
    )
    deadlines.event("publishing final progress", run_dir, counters = progress)
    write_json(
        run_dir / "progress.json", {**progress, "phase": "saving history"}
    )
    output.update({**progress, "phase": "saving history"})
    output.finish()
    try:
        deadlines.event(
            "saving history", site.roots["alluxio"], counters = progress
        )
        history = publish_history(
            run_dir, site, state, rclone, progress, stopped, inline
        )
        print(f"History: {history}", flush = True)
    except (OSError, ValueError) as error:
        print(
            f"Could not save history to Alluxio: {error}. Temporary records retained: {run_dir}",
            flush = True,
        )
        deadlines.event("recording history failure", run_dir, counters = progress)
        write_json(
            run_dir / "progress.json",
            {**progress, "phase": "failed", "history_error": str(error)},
        )
        return 1
    # History is already verified and durable. A cleanup failure must not turn
    # successful publication into a transfer failure or an endless status wait.
    deadlines.event(
        "removing archived scratch records", run_dir, counters = progress
    )
    try:
        write_json(
            run_dir / "progress.json", {**progress, "history": str(history)}
        )
        shutil.rmtree(run_dir)
    except OSError as error:
        print(
            f"History saved; temporary records could not be removed: {run_dir}: {error}",
            flush = True,
        )
    print(f"{progress['phase'].capitalize()}. History: {history}", flush = True)
    return 0 if progress["phase"] == "complete" else 1


def publish_history(
            run_dir, site, state, rclone, progress, stopped, inline = False,
        ):
    """Publish closed immutable files; never append logs through Alluxio FUSE."""
    archive_root = site.roots["alluxio"]
    site.check_root(archive_root)
    job_id = (
        run_dir.name
        if inline
        else str(os.environ.get("SLURM_JOB_ID", run_dir.name))
    )
    destination = archive_root / ".gbi" / "transfers" / job_id / run_dir.name
    records     = run_dir / "records"
    records.mkdir(exist_ok = True)
    sys.stdout.flush()
    sys.stderr.flush()
    if inline:
        receipts = run_dir / "receipts.jsonl"
        write_json(
            records / "history.json",
            {
                "request": json.loads((run_dir / "request.json").read_text()),
                "progress": progress,
                "receipts": (
                    [
                        json.loads(line)
                        for line in receipts.read_text().splitlines()
                    ]
                    if receipts.exists()
                    else []
                ),
            },
        )
    else:
        for name in (
            "request.json", "receipts.jsonl", "slurm.out", "slurm.json",
        ):
            original = run_dir / name
            if original.exists():
                # Network filesystems may reject kernel fast-copy operations.
                with (
                    original.open("rb") as reader,
                    (records / name).open("wb") as writer,
                ):
                    shutil.copyfileobj(reader, writer)
        write_json(records / "progress.json", progress)
    # The publication receipt remains temporary on scratch. The immutable
    # destination history records describe the data transfer, not themselves.
    paths = sorted(
        records.iterdir(), key = lambda path: path.name == "progress.json"
    )
    for path in paths:
        task = {
            "source": str(path), "target": str(destination / path.name),
            "source_root": str(site.roots["lustre"]),
            "target_root": str(archive_root), "selection_root": str(records),
            "state": str(state),
            "receipt": str(run_dir / "publication.jsonl"),
            "progress": str(run_dir / "publication-progress.json"),
            "rclone": rclone,
            "settle_seconds": int(site.values["verify_settle_seconds"]),
            "target_kind": "alluxio", "delete": False, "encode_link": False,
            "decode_link": False,
        }
        outcome = supervise(task, int(site.values["file_timeout"]), stopped)
        if "error" in outcome:
            raise ValueError(outcome["error"])
    return destination


def start(specification, site, home, wait = False, detach = False):
    """Snapshot and execute a new ordinary request, including saved retries."""
    mode = specification["execution"]
    if mode == "slurm" and not site.values["partition"]:
        raise ValueError("the gbi module has no Slurm partition configured")
    deadlines.event("checking scratch root", site.roots["lustre"])
    site.check_root(site.roots["lustre"])
    deadlines.event("checking archive root", site.roots["alluxio"])
    site.check_root(site.roots["alluxio"])
    run_dir = (
        home
        / "runs"
        / (
            time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
            + "-"
            + uuid.uuid4().hex[:12]
        )
    )
    deadlines.event(
        "preparing request snapshot", run_dir, records = str(run_dir)
    )
    run_dir.mkdir(parents = True, mode = 0o700)
    # A queued job uses the exact code and settings that were submitted,
    # even if the module's default version changes before it starts.
    runtime = run_dir / "runtime" / "gbi_data"
    shutil.copytree(
        Path(__file__).parent, runtime,
        ignore = shutil.ignore_patterns("__pycache__"),
    )
    # Installed source may be read-only. This private, caller-owned copy
    # must remain removable after its verified history has been published.
    for directory in (
        runtime,
        *(path for path in runtime.rglob("*") if path.is_dir()),
    ):
        directory.chmod(0o700)
    shutil.copyfile(site.path, run_dir / "site.conf")
    source_hash = hashlib.sha256()
    for path in sorted(runtime.glob("*.py")):
        source_hash.update(path.name.encode() + b"\0" + path.read_bytes())
    specification["code_sha256"] = source_hash.hexdigest()
    specification["version"] = __version__
    specification.pop("allocation_job_id", None)
    if mode == "allocation":
        specification["allocation_job_id"] = os.environ["SLURM_JOB_ID"]
    write_json(run_dir / "request.json", specification)
    if mode != "slurm":
        if mode == "allocation":
            print(
                f"Transfer {run_dir.name}. Reconnect: gbi data status {run_dir.name} --watch",
                flush = True,
            )
        else:
            print(
                f"Transfer {run_dir.name}. History: gbi data status {run_dir.name}"
            )
        return run(run_dir, site, home)
    command = [
        sys.executable, "-B", "-m", "gbi_data.cli", "data", "_run",
        str(run_dir),
    ]
    job_id = jobs.submit(run_dir, command, site)
    print(
        f"Submitted transfer {job_id}. Reconnect: gbi data status {job_id} --watch"
    )
    if wait or (not detach and sys.stdout.isatty()):
        return jobs.watch(
            run_dir, job_id, archive_root = site.roots["alluxio"]
        )
    return 0


def main():
    options = parser().parse_args()
    os.umask(0o077)
    try:
        configuration = os.environ.get("GBI_DATA_SITE_CONF")
        if not configuration:
            raise ValueError(
                "load the configured gbi module first (module load gbi)"
            )
        deadlines.event("loading site and storage roots", configuration)
        site = Site(configuration, configured = deadlines.configure)
        deadlines.event(
            "checking request paths", getattr(options, "source", configuration)
        )
        # Scratch holds only active work. Completed records are immutable files
        # in Alluxio; FSS is reserved for code and configuration.
        if options.verb == "usage":
            if "lustre" not in site.roots:
                raise ValueError(
                    "the gbi module needs a Lustre scratch root for usage"
                )
            if options.depth < 1 or options.limit < 1:
                raise ValueError("usage depth and limit must be positive")
            usage.print_report(
                usage.report(site, options.path, options.depth, options.limit),
                options.depth, options.limit,
            )
            return 0
        if options.verb == "cache":
            return cache.main(options, site)
        if options.verb == "status" and not re.fullmatch(
            r"(?:[0-9]+|[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12})", options.job_id
        ):
            return cache.status_path(options.job_id, site, options.watch)
        if "lustre" not in site.roots or "alluxio" not in site.roots:
            raise ValueError(
                "the gbi module needs Lustre scratch and Alluxio archive roots"
            )
        home = site.roots["lustre"] / ".gbi"
        if options.verb == "roots":
            return roots.print_report(site)
        if options.verb in ("status", "retry"):
            run_dir = jobs.locate(home, options.job_id, site.roots["alluxio"])
            if options.verb == "retry":
                request = run_dir / "request.json"
                record = json.loads(request.read_text()) if request.is_file() else {}
                if record.get("execution") in ("prefect", "cache"):
                    return prefect.submit(
                        run_dir, site, options.wait or sys.stdout.isatty()
                    )
                specification = retry.prepare(run_dir, site)
                if specification["execution"] == "allocation":
                    specification["execution"] = (
                        "allocation" if os.environ.get("SLURM_JOB_ID") else "slurm"
                    )
                print(
                    f"Retry of {run_dir.name}; original history retained at {run_dir}"
                )
                if specification["selection"] is None:
                    print(
                        "Original file inventory was not saved; rediscovering "
                        "the saved root and filters. Undiscovered original "
                        "remainder is unknown."
                    )
                return start(specification, site, home, options.wait, True)
            request = run_dir / "request.json"
            record = json.loads(request.read_text()) if request.is_file() else {}
            if record.get("execution") in ("prefect", "cache"):
                if record.get("broker_request", {}).get("operation") == "cache":
                    return cache.status(
                        run_dir, site, options.watch, record["execution"]
                    )
                return prefect.follow(run_dir, site, options.watch)
            return jobs.watch(
                run_dir, options.job_id, options.watch, site.roots["alluxio"]
            )
        if options.verb == "_run":
            return run(options.run_dir.resolve(), site, home)
        if options.archive_unit and (
            not options.archive
            or options.verb != "move"
            or options.prefect
            or options.dry_run
        ):
            raise ValueError(
                "--archive-unit requires an executing local/Slurm `data move --archive` request"
            )
        if options.prefect:
            return prefect.start(options, site, home)
        specification = execution_plan(plan(options, site), site, options)
        if (
            specification["execution"] == "slurm"
            and not site.values["partition"]
        ):
            raise ValueError("the gbi module has no Slurm partition configured")
        print(
            f"{specification['source_kind']} → {specification['target_kind']}: "
            f"{specification['source']} → {specification['target']}"
        )
        print(
            "Sources: delete each verified file."
            if specification["delete"]
            else "Sources: keep originals."
        )
        mode = specification["execution"]
        print(
            {
                "inline": "Foreground transfer. Ctrl-C stops the transfer.",
                "allocation": "Using your existing Slurm allocation.",
                "slurm": "Submitting background work to Slurm.",
            }[mode]
        )
        if options.dry_run:
            if (
                specification.get("archive_auto")
                or specification.get("pack")
                or specification.get("packing_policy")
                or specification.get("chunk_size")
                or specification.get("source_format")
            ):
                deadlines.event(
                    "planning archive layout", specification["source"]
                )
                print(
                    json.dumps(
                        format_selection.dry_run(specification, site), indent = 2
                    )
                )
            print("Dry run: no job submitted and no files written.")
            return 0
        return start(specification, site, home, options.wait, options.detach)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"gbi data: {error}", file = sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(deadlines.entry(main))
