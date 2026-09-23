"""Blocking CLI calls with progress in your terminal or Slurm log.

Load the gbi module before starting Python. Transfers use the CLI installed
beside this SDK. Ordinary transfers inherit your identity and Slurm allocation;
prefect=True needs the login-node broker and submits a managed migration.
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
              chunk_size, dry_run, prefect, delete_source=False):
    command = [str(_CLI), "data", verb, "--wait"]
    for flag, patterns in (("include", include), ("exclude", exclude)):
        command.extend(f"--{flag}={pattern}" for pattern in _patterns(patterns))
    for flag, value in (("pack", pack), ("chunk-size", chunk_size)):
        if value is not None:
            command.append(f"--{flag}={value}")
    for flag, enabled in (("pack-small", pack_small), ("dry-run", dry_run),
                          ("prefect", prefect), ("delete-source", delete_source)):
        if not isinstance(enabled, bool):
            raise TypeError(f"{flag.replace('-', '_')} must be a boolean")
        if enabled:
            command.append(f"--{flag}")
    command.extend(["--", os.fspath(source), os.fspath(destination)])
    # Inherit output: progress stays live and large jobs do not buffer logs in RAM.
    return subprocess.run(command, check=True)


def copy(source, destination, *, include=(), exclude=(), pack=None,
         pack_small=False, chunk_size=None, dry_run=False, prefect=False):
    """Copy and verify, retaining originals; return only when the CLI finishes.

    Paths accept strings or pathlib.Path. include/exclude accept one glob or an
    iterable of globs. pack="tar" or "gzip" creates a .gbi.tar or .gbi.tar.gz;
    copying a GBI archive restores it. pack_small and chunk_size have the same
    meaning as CLI flags. dry_run previews without writing. prefect=True needs
    the login-node broker, submits individual objects, and cannot be combined
    with packing, chunks or exclusions. It does not reuse a Slurm allocation.

    Return subprocess.CompletedProcess (output streams to the current log).
    A nonzero exit raises subprocess.CalledProcessError. CLI validation and all
    verification/receipt rules apply unchanged. Ordinary transfers reuse an
    existing Slurm allocation; queued transfers are followed until they finish.
    """
    return _transfer("copy", source, destination, include=include, exclude=exclude,
                     pack=pack, pack_small=pack_small, chunk_size=chunk_size,
                     dry_run=dry_run, prefect=prefect)


def move(source, destination, *, include=(), exclude=(), pack=None,
         pack_small=False, chunk_size=None, dry_run=False, prefect=False,
         delete_source=False):
    """Copy and verify, then remove unchanged selected filesystem originals.

    Options and return/exception behavior match copy(). Object Storage originals
    remain unless delete_source=True, and a partial archive restore always keeps
    its container. This calls the same maintained `gbi data move` operation;
    deletion is never a separate SDK pass.
    """
    return _transfer("move", source, destination, include=include, exclude=exclude,
                     pack=pack, pack_small=pack_small, chunk_size=chunk_size,
                     dry_run=dry_run, prefect=prefect, delete_source=delete_source)
