"""Which sources a move removes, and the caller's courtesy access checks.

A move removes a Lustre or FSS source after verification, and an Object
Storage source when the destination is Object Storage too. Moving Object
Storage data to Lustre or FSS asks first on a terminal; --delete-source
answers yes, and without a terminal or the flag the source is kept. Both the
ordinary and the --prefect route use this table.

The checks here refuse obvious mistakes before anything is submitted. They
are bounded so a login node never walks a whole tree; the transfer itself
runs with the caller's permissions and is the check that counts.
"""

import os
from pathlib import Path
import sys
import time

from . import deadlines

REPORT_DIRECTORIES = 500


def terminal():
    return sys.stdin.isatty()


def refuse_copy_deletion(options):
    if getattr(options, "delete_source", False) and options.verb != "move":
        raise ValueError("--delete-source applies only to move")


def deletes(options, source_kind, target_kind, source):
    """Whether this request removes its source after verification."""
    if options.verb != "move":
        return False
    if source_kind != "alluxio" or target_kind == "alluxio":
        return True
    if getattr(options, "delete_source", False):
        return True
    if options.dry_run:
        print(
            "Dry run: a real move asks before removing the Object Storage "
            "source; --delete-source removes it without asking."
        )
        return False
    if not terminal():
        print(
            "No terminal and no --delete-source: copying and keeping the "
            f"Object Storage source {source}."
        )
        return False
    try:
        answer = input(
            f"Remove the Object Storage source {source} after the copy is "
            "verified? [y/N] "
        )
    except EOFError:
        answer = ""
    if answer.strip().lower() in ("y", "yes"):
        return True
    print(f"Keeping the Object Storage source {source}; copying only.")
    return False


def check_readable(path):
    path = Path(path)
    mode = os.R_OK | (os.X_OK if path.is_dir() else 0)
    if not os.access(path, mode):
        raise ValueError(f"cannot read {path}: permission denied")


def check_writable(path):
    """The destination itself, or the deepest folder it will be created in."""
    path     = Path(path)
    existing = path
    while not os.path.lexists(existing):
        existing = existing.parent
    if existing == path:
        mode = os.W_OK | (os.X_OK if path.is_dir() else 0)
    elif existing.is_dir():
        mode = os.W_OK | os.X_OK
    else:
        raise ValueError(f"cannot write {path}: {existing} is not a folder")
    if not os.access(existing, mode):
        raise ValueError(f"cannot write {existing}: permission denied")


def check_removable(path, entries, seconds):
    """A move needs write access to the source's folder and every folder in it.

    The walk stops quietly after ``entries`` folders or ``seconds``; the
    transfer refuses or keeps whatever this did not reach.
    """
    path     = Path(path)
    folders  = [path.parent]
    if path.is_dir() and not path.is_symlink():
        folders.append(path)
    deadline = time.monotonic() + seconds
    checked  = 0
    while folders and checked < entries and time.monotonic() < deadline:
        folder = folders.pop()
        if not os.access(folder, os.W_OK | os.X_OK):
            raise ValueError(f"cannot remove from {folder}: permission denied")
        checked += 1
        if checked % REPORT_DIRECTORIES == 0:
            deadlines.event("checking the source can be removed", folder)
        if folder == path.parent:
            continue
        try:
            with os.scandir(folder) as listing:
                folders.extend(
                    Path(entry.path)
                    for entry in listing
                    if entry.is_dir(follow_symlinks = False)
                )
        except PermissionError:
            raise ValueError(
                f"cannot read {folder}: permission denied"
            ) from None
