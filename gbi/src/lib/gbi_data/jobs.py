"""Slurm submission and terminal progress. No Prefect client or credentials."""

import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import time

from .transfer import write_json


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
    )
    if result.returncode:
        raise ValueError(result.stderr.strip() or "Slurm rejected the transfer")
    job_id = result.stdout.strip().split(";")[0]
    if not job_id.isdigit():
        raise ValueError(f"unexpected Slurm response; inspect submission before retrying: {result.stdout!r}")
    write_json(run_dir / "slurm.json", {"job_id": job_id})
    return job_id


def locate(home, job_id, archive_root):
    if not re.fullmatch(r"(?:[0-9]+|[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12})", job_id):
        raise ValueError("use the Slurm job ID or the foreground transfer ID printed by gbi")
    for run in (home / "runs").glob("*"):
        if run.name == job_id:
            return run
        try:
            if json.loads((run / "slurm.json").read_text())["job_id"] == job_id:
                return run
        except (OSError, ValueError, KeyError):
            continue
    archived = archive_root / ".gbi" / "transfers" / job_id
    for run in archived.glob("*"):
        if (run / "progress.json").is_file() or (run / "history.json").is_file():
            return run
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
    if progress.get("discovery_complete") and total:
        proportion = min(completed / total, 1)
        filled = int(width * proportion)
        bar = "[" + "=" * filled + " " * (width - filled) + f"] {proportion:6.1%}"
    else:
        bar = "[ discovering files ]" if progress.get("phase") == "running" else "[ awaiting job ]"
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
    if progress.get("discovery_complete") and total:
        fraction = min(done / total, 1)
        filled = int(width * fraction)
        bar = "[" + "=" * filled + " " * (width - filled) + f"] {fraction:.1%}"
    else:
        bar = "Discovering files…" if progress["phase"] == "running" else progress["phase"].capitalize()
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

    def update(self, progress):
        line = terminal_display(progress) if self.terminal else display(progress)
        if self.terminal:
            print(("\033[3A\r\033[J" if self.last_line else "") + line, end="", flush=True)
        elif line != self.last_line:
            print(line, flush=True)
        self.last_line = line

    def finish(self):
        if self.terminal and self.last_line:
            print(flush=True)
        self.last_line = ""


def watch(run_dir, job_id, follow=True, archive_root=None):
    output = ProgressDisplay()
    last_check, state = 0.0, "PENDING"
    print(f"Transfer {job_id}   Records: {run_dir}")
    if follow:
        print("Ctrl-C detaches this display; the transfer continues.")
    try:
        while True:
            if not run_dir.exists() and archive_root is not None:
                run_dir = archive_root / ".gbi" / "transfers" / job_id / run_dir.name
            progress = read_progress(run_dir)
            output.update(progress)
            if progress["phase"] in ("complete", "failed", "interrupted"):
                receipt = run_dir / ("history.json" if (run_dir / "history.json").is_file() else "receipts.jsonl")
                print(f"\n{progress['phase'].capitalize()}. Receipts: {receipt}")
                return 0 if progress["phase"] == "complete" else 1
            if job_id.isdigit() and time.monotonic() - last_check > 15:
                try:
                    state = slurm_state(job_id)
                except (OSError, subprocess.SubprocessError):
                    state = "UNKNOWN"
                last_check = time.monotonic()
            if state in ("FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL", "OUT_OF_MEMORY", "COMPLETED"):
                print(f"\nSlurm: {state}. No completed transfer summary; inspect {run_dir / 'slurm.out'}.")
                return 1
            if not follow:
                print(f"\nSlurm: {state}" if job_id.isdigit() else f"\nLocal transfer: {progress['phase']}")
                return 0
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\nDetached. Reconnect: gbi data status {job_id} --watch")
        return 0
