"""Streaming selection and a bounded, read-only foreground probe."""

import fnmatch
import json
import os
from pathlib import Path
from queue import Empty, Full, Queue
import subprocess
import sys
import threading
import time

from .storage import Site, fingerprint


def matches(name, includes=(), exclusions=()):
    """Select case-sensitive basenames; any exclusion wins over all includes."""
    return (not includes or any(fnmatch.fnmatchcase(name, pattern) for pattern in includes)) and not any(
        fnmatch.fnmatchcase(name, pattern) for pattern in exclusions
    )


def entries(source, site, root, patterns, visit=lambda: None, on_error=None, exclusions=()):
    if source.is_symlink() or not source.is_dir():
        visit()
        if matches(source.name, patterns, exclusions):
            yield source, False
        return
    with os.scandir(source) as children:
        empty = True
        for child in children:
            visit()  # Count excluded names and directories, not just matches.
            empty = False
            path = Path(child.path)
            try:
                if site.is_reserved(path, root):
                    continue
                is_dir = child.is_dir(follow_symlinks=False)
            except OSError as error:
                if on_error is None:
                    raise
                on_error(path, error)
                continue
            if is_dir:
                yield from entries(path, site, root, patterns, visit, on_error, exclusions)
            elif matches(child.name, patterns, exclusions):
                yield path, False
        if empty and not patterns:
            yield source, True


class EntryStream:
    """Run blocking directory discovery without stopping transfer receipts."""

    def __init__(self, source, site, root, patterns, exclusions=(), iterator_factory=None):
        self._events = Queue(maxsize=1)
        self._stopped = threading.Event()
        self._visited = 0
        self._last_progress = time.monotonic()
        self._iterator = (iterator_factory(self._report_error, self._visit) if iterator_factory is not None else
                          entries(source, site, root, patterns, visit=self._visit,
                                  on_error=self._report_error, exclusions=exclusions))
        self._thread = threading.Thread(target=self._produce, daemon=True)
        self._thread.start()

    def _put(self, event):
        while not self._stopped.is_set():
            try:
                self._events.put(event, timeout=0.2)
                return
            except Full:
                pass

    def _report_error(self, path, error):
        self._put(("error", path, error))

    def _visit(self):
        self._visited += 1
        self._last_progress = time.monotonic()

    def progress(self):
        """Return entries visited and the last time discovery advanced."""
        return self._visited, self._last_progress

    def _produce(self):
        try:
            for item in self._iterator:
                if self._stopped.is_set():
                    break
                self._put(("entry", item, None))
        except (OSError, ValueError, RecursionError) as error:
            self._put(("fatal", None, error))
        finally:
            self._put(("done", None, None))

    def get(self, timeout):
        try:
            return self._events.get(timeout=timeout)
        except Empty:
            return None

    def stop(self):
        self._stopped.set()


def stream_entries(source, site, root, patterns, exclusions=(), iterator_factory=None):
    return EntryStream(source, site, root, patterns, exclusions, iterator_factory)


def foreground_selection(specification, site):
    if specification.get("format_units"):
        from .format_selection import foreground_units
        return foreground_units(specification, site)
    visited, total = 0, 0
    selected = []
    byte_limit = int(site.values["inline_bytes"])

    def visit():
        nonlocal visited
        visited += 1
        if visited > int(site.values["inline_scan_entries"]):
            raise ValueError("selection needs a larger scan")

    for path, empty in entries(Path(specification["source"]), site,
                               Path(specification["source_root"]), specification["include"], visit,
                               exclusions=specification.get("exclude", ())):
        identity = None if empty else fingerprint(path)
        total += 0 if empty else identity[3]
        selected.append({"path": str(path), "empty": empty, "fingerprint": identity})
        if total > byte_limit:
            raise ValueError("selection exceeds the foreground byte budget")
    return selected


def probe(specification, site):
    # A slow mount or huge filtered tree must not trap discovery on a login
    # node. The read-only child can be abandoned safely if kernel I/O blocks.
    process = subprocess.Popen([sys.executable, "-B", "-m", "gbi_data.selection", str(site.path)],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                               text=True)
    try:
        output, _ = process.communicate(json.dumps(specification),
                                        timeout=float(site.values["inline_probe_seconds"]))
        return json.loads(output) if process.returncode == 0 else None
    except (subprocess.TimeoutExpired, ValueError):
        if process.poll() is None:
            process.kill()
            try:
                process.wait(timeout=0.1)
            except subprocess.TimeoutExpired:
                pass
        return None
    finally:
        process.stdin.close()
        process.stdout.close()


if __name__ == "__main__":
    try:
        print(json.dumps(foreground_selection(json.load(sys.stdin), Site(sys.argv[1]))))
    except (OSError, ValueError, RecursionError):
        sys.exit(1)
