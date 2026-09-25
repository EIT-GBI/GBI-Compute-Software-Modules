"""Slurm submission and terminal progress. No Prefect client or credentials."""

import json
from itertools import islice
import os
from pathlib import Path
import re
import shlex
import shutil
import socket
import subprocess
import sys
import time

from .transfer import write_json
from . import deadlines


def timestamp():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def process_identity(pid):
    """Linux process start ticks distinguish a reused PID from this transfer."""
    try:
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19]
    except (OSError, IndexError):
        return None


def human_bytes(value):
    value = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if abs(value) < 1024 or unit == "PiB":
            return f"{value:.1f} {unit}"
        value /= 1024


def slurm_state(job_id):
    result = subprocess.run(
        ["sacct", "-j", job_id, "--noheader", "--parsable2", "--format=JobIDRaw,State,ExitCode"],
        capture_output=True, text=True, timeout=15,
    )
    if result.returncode:
        return "UNKNOWN"
    for line in result.stdout.splitlines():
        fields = line.split("|")
        if fields[0] == job_id:
            return fields[1].split()[0].rstrip("+")
    return "PENDING"


def submit(run_dir, command, site):
    script = run_dir / "job.sh"
    environment = ["env", f"PYTHONPATH={run_dir / 'runtime'}",
                   f"GBI_DATA_SITE_CONF={run_dir / 'site.conf'}"]
    script.write_text("#!/usr/bin/env bash\nset -euo pipefail\nexec " + shlex.join(environment + command) + "\n")
    result = subprocess.run(
        ["sbatch", "--parsable", f"--partition={site.values['partition']}",
         f"--time={site.values['time_limit']}", f"--cpus-per-task={site.values['cpus']}",
         f"--mem={site.values['mem']}", "--job-name=gbi-data",
         f"--output={run_dir / 'slurm.out'}", f"--error={run_dir / 'slurm.out'}", str(script)],
        capture_output=True, text=True, timeout=30,
        env={key: value for key, value in os.environ.items()
             if key not in (deadlines.EVENT_FD, deadlines.SUPERVISOR_PID)},
    )
    if result.returncode:
        raise ValueError(result.stderr.strip() or "Slurm rejected the transfer")
    job_id = result.stdout.strip().split(";")[0]
    if not job_id.isdigit():
        raise ValueError(f"unexpected Slurm response; inspect submission before retrying: {result.stdout!r}")
    write_json(run_dir / "slurm.json", {"job_id": job_id})
    return job_id


def _slurm_job_id(run_dir):
    for filename, field in (("slurm.json", "job_id"), ("request.json", "allocation_job_id")):
        try:
            record = json.loads((run_dir / filename).read_text())
            value = record.get(field)
        except (OSError, ValueError, AttributeError):
            continue
        if isinstance(value, str) and value.isdigit():
            return value
    return None


def locate(home, job_id, archive_root):
    if not re.fullmatch(r"(?:[0-9]+|[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12})", job_id):
        raise ValueError("use the Slurm job ID or the foreground transfer ID printed by gbi")
    runs = list((home / "runs").glob("*"))
    if not job_id.isdigit():
        for run in runs:
            if run.name == job_id:
                return run
        archived_root = archive_root / ".gbi" / "transfers"
        exact = list(islice((parent / job_id for parent in archived_root.glob("*")
                            if (parent / job_id / "progress.json").is_file()
                            or (parent / job_id / "history.json").is_file()), 2))
        if len(exact) > 1:
            raise ValueError(f"transfer ID {job_id} is published more than once; inspect its histories")
        if exact:
            return exact[0]

    active = list(islice((run for run in runs if _slurm_job_id(run) == job_id), 2))
    if active:
        if len(active) > 1:
            ids = ", ".join(sorted(run.name for run in active))
            raise ValueError(f"Slurm job ID {job_id} maps to multiple active transfers; "
                             f"use a transfer ID, for example {ids}")
        return active[0]

    archived = archive_root / ".gbi" / "transfers" / job_id
    completed = list(islice((run for run in archived.glob("*")
                            if (run / "progress.json").is_file() or (run / "history.json").is_file()), 2))
    if len(completed) > 1:
        ids = ", ".join(sorted(run.name for run in completed))
        raise ValueError(f"Slurm job ID {job_id} maps to multiple transfers; "
                         f"use a transfer ID, for example {ids}")
    if completed:
        return completed[0]
    raise ValueError(f"no transfer record for job {job_id} in your GBI state")


def read_progress(run_dir):
    try:
        if (run_dir / "history.json").is_file():
            return json.loads((run_dir / "history.json").read_text())["progress"]
        return json.loads((run_dir / "progress.json").read_text())
    except (OSError, ValueError, KeyError):
        return {"phase": "queued", "files": 0, "bytes": 0, "freed": 0, "failed": 0,
                "elapsed": 0, "discovered_bytes": 0, "discovery_complete": False}


def display(progress, width=24):
    completed = progress.get("bytes", 0)
    total = progress.get("discovered_bytes", 0)
    elapsed = progress.get("elapsed", 0)
    speed = completed / elapsed if elapsed else 0
    if progress.get("discovery_complete") and progress.get("totals_known", True) and total:
        proportion = min(completed / total, 1)
        filled = int(width * proportion)
        bar = "[" + "=" * filled + " " * (width - filled) + f"] {proportion:6.1%}"
    else:
        bar = ("[ verifying archive sizes ]" if not progress.get("totals_known", True) else
               "[ discovering files ]" if progress.get("phase") == "running" else "[ awaiting job ]")
    phases = ", ".join(f"{count} {phase}" for phase, count in sorted(progress.get("phases", {}).items()))
    return (f"{bar}  {progress['files']:,} verified  {human_bytes(completed)}  "
            f"{human_bytes(speed)}/s  freed {human_bytes(progress['freed'])}  "
            f"failed {progress['failed']}  "
            f"active {progress.get('active', 0)} ({human_bytes(progress.get('active_bytes', 0))})" +
            (f" · {phases}" if phases else ""))


def terminal_display(progress):
    """A compact display with explicit close/readback phases during long waits."""
    total = progress.get("discovered_bytes", 0)
    done = progress.get("bytes", 0)
    width = max(10, min(36, shutil.get_terminal_size().columns - 36))
    if progress.get("discovery_complete") and progress.get("totals_known", True) and total:
        fraction = min(done / total, 1)
        filled = int(width * fraction)
        bar = "[" + "=" * filled + " " * (width - filled) + f"] {fraction:.1%}"
    else:
        bar = ("Verifying archive sizes…" if not progress.get("totals_known", True) else
               "Discovering files…" if progress["phase"] == "running" else progress["phase"].capitalize())
    if progress["phase"] == "saving history" and progress.get("discovery_complete") and total:
        bar += " · Saving history"
    speed = done / max(progress.get("elapsed", 0), 1)
    phases = ", ".join(f"{count} {phase}" for phase, count in sorted(progress.get("phases", {}).items()))
    return (f"{bar}\n"
            f"Verified {progress['files']:,} files · {human_bytes(done)} · {human_bytes(speed)}/s\n"
            f"Source removed {human_bytes(progress['freed'])} · failed {progress['failed']}\n"
            f"Active {progress.get('active', 0)}" + (f": {phases}" if phases else ""))


class ProgressDisplay:
    """Use the same compact progress display in the foreground and job viewer."""
    def __init__(self):
        self.terminal = sys.stdout.isatty()
        self.last_line = ""
        self.last_print = 0.0

    def update(self, progress):
        line = terminal_display(progress) if self.terminal else display(progress)
        if self.terminal:
            print(("\033[3A\r\033[J" if self.last_line else "") + line, end="", flush=True)
        elif line != self.last_line or time.monotonic() - self.last_print >= 15:
            print(f"{timestamp()} {progress['phase']}: {line}", flush=True)
            self.last_print = time.monotonic()
        self.last_line = line

    def finish(self):
        if self.terminal and self.last_line:
            print(flush=True)
        self.last_line = ""


def watch(run_dir, job_id, follow=True, archive_root=None):
    output = ProgressDisplay()
    last_check, state = 0.0, "PENDING"
    home = run_dir.parent.parent
    native_job_id = _slurm_job_id(run_dir) or (job_id if job_id.isdigit() else None)
    print(f"Transfer {job_id}   Records: {run_dir}")
    if follow:
        print("Ctrl-C detaches this display; the transfer continues.")
    try:
        while True:
            deadlines.event("reading transfer status", run_dir)
            if not run_dir.exists() and archive_root is not None:
                run_dir = locate(home, run_dir.name, archive_root)
            progress = read_progress(run_dir)
            output.update(progress)
            deadlines.event("following transfer status", run_dir, counters=progress)
            if progress["phase"] in ("complete", "failed", "interrupted"):
                receipt = run_dir / ("history.json" if (run_dir / "history.json").is_file() else "receipts.jsonl")
                print(f"\n{progress['phase'].capitalize()}. Receipts: {receipt}")
                return 0 if progress["phase"] == "complete" else 1
            if not job_id.isdigit() and progress.get("hostname") == socket.gethostname():
                try:
                    os.kill(progress["coordinator_pid"], 0)
                    identity = process_identity(progress["coordinator_pid"])
                    if identity is not None and progress.get("coordinator_identity") is not None and (
                            identity != progress["coordinator_identity"]):
                        raise ProcessLookupError
                except ProcessLookupError:
                    output.finish()
                    print("Transfer process stopped without a final history record. "
                          f"Inspect receipts and the original command's output: {run_dir}. "
                          "Check any reported worker PIDs before retrying.")
                    return 1
                except (KeyError, PermissionError):
                    pass
            if native_job_id and time.monotonic() - last_check > 15:
                try:
                    state = slurm_state(native_job_id)
                except (OSError, subprocess.SubprocessError):
                    state = "UNKNOWN"
                last_check = time.monotonic()
            if state in ("FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL", "OUT_OF_MEMORY", "COMPLETED"):
                print(f"\nSlurm: {state}. No completed transfer summary; inspect {run_dir / 'slurm.out'}.")
                return 1
            if not follow:
                print(f"\nSlurm: {state}" if native_job_id else f"\nLocal transfer: {progress['phase']}")
                return 0
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\nDetached. Reconnect: gbi data status {job_id} --watch")
        return 0
