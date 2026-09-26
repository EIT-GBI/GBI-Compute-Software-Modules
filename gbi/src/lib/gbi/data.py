"""Blocking CLI calls with progress in your terminal or Slurm log.

Load the gbi module before starting Python. Transfers use the CLI installed
beside this SDK. Ordinary transfers inherit your identity and Slurm allocation;
prefect=True submits a managed migration through the site's broker. Submission
from a compute job requires the site's authenticated HTTPS endpoint.
Failures raise subprocess.CalledProcessError; no retry is submitted implicitly.
"""

import os
from pathlib import Path
import subprocess


_CLI = Path(__file__).resolve().parents[2] / "bin" / "gbi"


def _patterns(value):
    values = [value] if isinstance(value, str) else list(value)
    if any(not isinstance(pattern, str) for pattern in values):
        raise TypeError("include and exclude patterns must be strings")
    return values


def _transfer(verb, source, destination, *, include, exclude, pack, pack_small,
              chunk_size, dry_run, prefect, delete_source=False, job_size=None):
    if not isinstance(delete_source, bool):
        raise TypeError("delete_source must be a boolean")
    if delete_source:
        raise ValueError("delete_source=True is not supported; Object Storage originals are durable and always retained")
    if job_size is not None:
        if job_size not in ("small", "large"):
            raise ValueError("job_size must be small or large")
        if not prefect:
            raise ValueError("job_size requires prefect=True and a restore to Lustre or FSS")
    command = [str(_CLI), "data", verb, "--wait"]
    for flag, patterns in (("include", include), ("exclude", exclude)):
        command.extend(f"--{flag}={pattern}" for pattern in _patterns(patterns))
    for flag, value in (("pack", pack), ("chunk-size", chunk_size), ("job-size", job_size)):
        if value is not None:
            command.append(f"--{flag}={value}")
    for flag, enabled in (("pack-small", pack_small), ("dry-run", dry_run),
                          ("prefect", prefect)):
        if not isinstance(enabled, bool):
            raise TypeError(f"{flag.replace('-', '_')} must be a boolean")
        if enabled:
            command.append(f"--{flag}")
    command.extend(["--", os.fspath(source), os.fspath(destination)])
    # Inherit output: progress stays live and large jobs do not buffer logs in RAM.
    return subprocess.run(command, check=True)


def copy(source, destination, *, include=(), exclude=(), pack=None,
         pack_small=False, chunk_size=None, dry_run=False, prefect=False, job_size=None):
    """Copy and verify, retaining originals; return only when the CLI finishes.

    Paths accept strings or pathlib.Path. include/exclude accept one glob or an
    iterable of globs. pack="tar" or "gzip" creates a .gbi.tar or .gbi.tar.gz;
    copying a GBI archive restores it. pack_small and chunk_size have the same
    meaning as CLI flags. dry_run previews without data writes. prefect=True uses
    the site's broker (local socket or authenticated HTTPS). Prefect archives
    support pack/pack_small and chunk_size with matching broker and flow updates.
    Chunking adds .gbi-chunks to a direct file or whole-pack target; restore
    encoded stores without these flags. A Prefect packing/chunking dry_run submits a managed metadata-only plan
    and creates tracking records, but no data/receipt/destination writes or
    deletions; without packing/chunking it remains a local preview. Exclusions require
    the matching broker and flow update. It
    starts a separate managed transfer instead of reusing a Slurm allocation.
    On Prefect restores only, job_size="small" requests 2 CPUs/16 GiB and
    "large" requests 8 CPUs/48 GiB; None preserves the deployment's default.

    Return subprocess.CompletedProcess (output streams to the current log).
    A nonzero exit raises subprocess.CalledProcessError. CLI validation and all
    verification/receipt rules apply unchanged. Ordinary transfers reuse an
    existing Slurm allocation; queued transfers are followed until they finish.
    """
    return _transfer("copy", source, destination, include=include, exclude=exclude,
                     pack=pack, pack_small=pack_small, chunk_size=chunk_size,
                     dry_run=dry_run, prefect=prefect, job_size=job_size)


def move(source, destination, *, include=(), exclude=(), pack=None,
         pack_small=False, chunk_size=None, dry_run=False, prefect=False,
         delete_source=False, job_size=None):
    """Copy and verify, then remove unchanged selected filesystem originals.

    Options and return/exception behavior match copy(). Object Storage originals
    are durable and always retained; delete_source=True is rejected. A partial
    archive restore always keeps its container. This calls the same maintained
    `gbi data move` operation; deletion is never a separate SDK pass.
    """
    return _transfer("move", source, destination, include=include, exclude=exclude,
                     pack=pack, pack_small=pack_small, chunk_size=chunk_size,
                     dry_run=dry_run, prefect=prefect, delete_source=delete_source, job_size=job_size)
