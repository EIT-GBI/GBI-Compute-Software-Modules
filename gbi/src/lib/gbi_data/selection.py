"""Streaming selection and a bounded, read-only foreground probe."""

import fnmatch
import json
import os
from pathlib import Path
import subprocess
import sys

from .storage import Site, fingerprint


def entries(source, site, root, patterns, visit=lambda: None):
    if source.is_symlink() or not source.is_dir():
        visit()
        yield source, False
        return
    with os.scandir(source) as children:
        empty = True
        for child in children:
            visit()  # Count excluded names and directories, not just matches.
            empty = False
            path = Path(child.path)
            if site.is_reserved(path, root):
                continue
            if child.is_dir(follow_symlinks=False):
                yield from entries(path, site, root, patterns, visit)
            elif not patterns or any(fnmatch.fnmatchcase(child.name, pattern) for pattern in patterns):
                yield path, False
        if empty and not patterns:
            yield source, True


def foreground_selection(specification, site):
    visited, total = 0, 0
    selected = []
    byte_limit = int(site.values["inline_bytes"])

    def visit():
        nonlocal visited
        visited += 1
        if visited > int(site.values["inline_scan_entries"]):
            raise ValueError("selection needs a larger scan")

    for path, empty in entries(Path(specification["source"]), site,
                               Path(specification["source_root"]), specification["include"], visit):
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
