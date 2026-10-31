"""Warm the Object Storage cache for paths a job is about to read.

Every Alluxio-backed mount on the cluster caches a block on its workers the
first time a client reads it (``alluxio.user.file.readtype.default=CACHE``).
``gbi data cache`` therefore needs no credential and no service: it reads
every regular file below the paths you name, once, through your own mount,
with a bounded number of parallel readers, and returns when the last read has
finished. A second run re-reads warm blocks at cache speed, so the command is
safe to repeat, for example as the first line of a job script or a prolog.
"""

import json
import os
import stat
import sys
import threading
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path

BLOCK            = 1024 * 1024
MAX_JOBS         = 32
PROGRESS_SECONDS = 2.0

EXIT_OK      = 0
EXIT_FAILED  = 1
EXIT_REFUSED = 2
EXIT_PARTIAL = 3


class CacheError(ValueError):
    """A refused request: nothing was read."""


def resolve(site, raw):
    """One named path: inside an Object Storage mount, present, not a symlink."""
    path = Path(raw).expanduser().absolute()
    try:
        located, kind, root = site.classify(path)
    except ValueError as error:
        raise CacheError(f"{raw}: {error}") from None
    if kind != "alluxio":
        raise CacheError(
            f"{raw}: not inside an Object Storage mount; the cache applies to "
            "Alluxio-backed paths only"
        )
    if not os.path.lexists(located):
        raise CacheError(f"{raw}: does not exist")
    if located.is_symlink():
        raise CacheError(f"{raw}: is a symbolic link; name the real path")
    return located, root


def walk(site, located, root, skipped):
    """Regular files below ``located`` in a stable order, never through a symlink."""
    info = located.lstat()
    if stat.S_ISREG(info.st_mode):
        yield located, info.st_size
        return
    if not stat.S_ISDIR(info.st_mode):
        skipped["special"] += 1
        return
    stack = [located]
    while stack:
        directory = stack.pop()
        try:
            with os.scandir(directory) as entries:
                children = sorted(entries, key = lambda entry: entry.name)
        except OSError as error:
            skipped["unreadable_directories"].append(
                {"path": str(directory), "error": str(error)}
            )
            continue
        for entry in children:
            path = Path(entry.path)
            if site.is_reserved(path, root):
                skipped["reserved"] += 1
                continue
            try:
                entry_info = entry.stat(follow_symlinks = False)
            except OSError as error:
                skipped["unreadable_directories"].append(
                    {"path": str(path), "error": str(error)}
                )
                continue
            mode = entry_info.st_mode
            if stat.S_ISLNK(mode):
                skipped["symlinks"] += 1
            elif stat.S_ISDIR(mode):
                stack.append(path)
            elif stat.S_ISREG(mode):
                yield path, entry_info.st_size
            else:
                skipped["special"] += 1


def read_file(path):
    """Read the whole file through the mount; return the bytes read."""
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0))
    total = 0
    try:
        while True:
            block = os.read(fd, BLOCK)
            if not block:
                return total
            total += len(block)
    finally:
        os.close(fd)


def warm(
            site, paths, *, jobs, timeout = 0, dry_run = False,
            progress = None, clock = None,
        ):
    """Read every file below ``paths`` once; return the summary dictionary."""
    clock = clock if clock is not None else time.monotonic
    if type(jobs) is not int or not 1 <= jobs <= MAX_JOBS:
        raise CacheError(f"--jobs must be between 1 and {MAX_JOBS}")
    if type(timeout) is not int or timeout < 0:
        raise CacheError("--timeout must be a non-negative number of seconds")
    skipped = {
        "symlinks": 0, "special": 0, "reserved": 0,
        "unreadable_directories": [],
    }
    located = [resolve(site, raw) for raw in paths]
    files = []
    for path, root in located:
        files.extend(walk(site, path, root, skipped))
    summary = {
        "paths": [str(path) for path, _root in located],
        "files": len(files), "bytes": sum(size for _path, size in files),
        "skipped": skipped, "read_files": 0, "read_bytes": 0,
        "failed": [], "seconds": 0.0, "timed_out": False, "dry_run": dry_run,
        "jobs": jobs,
    }
    if dry_run:
        return summary
    started  = clock()
    deadline = started + timeout if timeout else None
    lock     = threading.Lock()
    reported = [started]

    def report(force = False):
        if progress is None:
            return
        now = clock()
        if not force and now - reported[0] < PROGRESS_SECONDS:
            return
        reported[0] = now
        elapsed = max(now - started, 1e-9)
        progress(
            {
                "read_files": summary["read_files"], "files": summary["files"],
                "read_bytes": summary["read_bytes"], "bytes": summary["bytes"],
                "seconds": round(elapsed, 1),
                "bytes_per_second": int(summary["read_bytes"] / elapsed),
            }
        )

    def one(path, size):
        try:
            count = read_file(path)
        except OSError as error:
            return path, size, None, str(error)
        return path, size, count, None

    pending = deque(files)
    with ThreadPoolExecutor(max_workers = jobs) as pool:
        running = set()
        while pending or running:
            while pending and len(running) < jobs * 2:
                if deadline is not None and clock() >= deadline:
                    summary["timed_out"] = True
                    pending.clear()
                    break
                running.add(pool.submit(one, *pending.popleft()))
            if not running:
                break
            done, running = wait(running, return_when = FIRST_COMPLETED)
            for future in done:
                path, size, count, error = future.result()
                with lock:
                    if error is not None:
                        summary["failed"].append({"path": str(path), "error": error})
                    else:
                        summary["read_files"] += 1
                        summary["read_bytes"] += count
                report()
    summary["seconds"] = round(clock() - started, 2)
    report(force = True)
    return summary


def _mib(value):
    return f"{value / (1024 * 1024):.1f} MiB"


def print_progress(detail):
    print(
        f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} warming: "
        f"{detail['read_files']}/{detail['files']} files  "
        f"{_mib(detail['read_bytes'])} of {_mib(detail['bytes'])}  "
        f"{_mib(detail['bytes_per_second'])}/s",
        flush = True,
    )


def main(options, site):
    jobs    = options.jobs if options.jobs is not None else int(site.values["jobs"])
    timeout = options.timeout if options.timeout is not None else 0
    try:
        summary = warm(
            site, options.paths, jobs = jobs, timeout = timeout,
            dry_run = options.dry_run,
            progress = None if options.json else print_progress,
        )
    except CacheError as error:
        print(f"gbi data cache: {error}", file = sys.stderr)
        return EXIT_REFUSED
    if options.json:
        print(json.dumps(summary, sort_keys = True), flush = True)
    elif summary["dry_run"]:
        print(
            f"Dry run: {summary['files']} files, {_mib(summary['bytes'])} below "
            f"{len(summary['paths'])} path(s); nothing read."
        )
    else:
        rate = summary["read_bytes"] / max(summary["seconds"], 1e-9)
        print(
            f"Warmed {summary['read_files']}/{summary['files']} files, "
            f"{_mib(summary['read_bytes'])} in {summary['seconds']} s "
            f"({_mib(rate)}/s); failed {len(summary['failed'])}; "
            f"skipped {summary['skipped']['symlinks']} symlinks, "
            f"{summary['skipped']['special']} special, "
            f"{summary['skipped']['reserved']} reserved."
        )
        for failure in summary["failed"][:20]:
            print(f"  failed: {failure['path']}: {failure['error']}")
        if summary["timed_out"]:
            print("Stopped at --timeout before every file was read.")
    if summary["failed"]:
        return EXIT_FAILED
    if summary["timed_out"]:
        return EXIT_PARTIAL
    return EXIT_OK
