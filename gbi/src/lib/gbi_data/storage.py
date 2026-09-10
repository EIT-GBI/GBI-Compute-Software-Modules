"""Site configuration and path rules. Configuration never executes shell code."""

import fnmatch
import os
from pathlib import Path
import pwd
import re


DEFAULTS = {
    "lustre_root": "", "fss_root": "", "bucket_root": "",
    "partition": "", "time_limit": "1-00:00:00", "cpus": "2", "mem": "4G",
    "jobs": "4", "file_timeout": "86400", "verify_settle_seconds": "1200",
    "rclone_bin": "rclone", "reserved_names": "",
    "inline_bytes": "8589934592",
    "native_bytes": "8388608", "native_files": "32",
    "inline_scan_entries": "100000", "inline_probe_seconds": "5",
}


def mounted_roots(path=Path("/proc/self/mountinfo")):
    """Identify storage behavior from mounts, not an access allowlist."""
    if not path.exists():
        return []
    roots = []
    for line in path.read_text().splitlines():
        fields, filesystem = line.split(" - ", 1)
        mount = re.sub(r"\\([0-7]{3})", lambda match: chr(int(match[1], 8)), fields.split()[4])
        kind, source, *_ = filesystem.split()
        if "alluxio" in kind.lower() or "alluxio" in source.lower():
            kind = "alluxio"
        elif kind.startswith("nfs"):
            kind = "fss"
        elif kind != "lustre":
            kind = "posix"
        roots.append((kind, Path(mount)))
    return roots


class Site:
    def __init__(self, path):
        self.path = Path(path).resolve()
        self.values = DEFAULTS.copy()
        for number, line in enumerate(self.path.read_text().splitlines(), 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, separator, value = line.partition("=")
            key, value = key.strip(), value.strip()
            if not separator or key not in DEFAULTS:
                raise ValueError(f"{self.path}:{number}: unknown setting {key!r}")
            self.values[key] = value
        for key in ("jobs", "cpus", "file_timeout", "verify_settle_seconds", "inline_bytes",
                    "native_bytes", "native_files", "inline_scan_entries"):
            if int(self.values[key]) < 1:
                raise ValueError(f"{key} must be positive")
        if float(self.values["inline_probe_seconds"]) <= 0:
            raise ValueError("inline_probe_seconds must be positive")
        self.user = pwd.getpwuid(os.getuid()).pw_name
        self.root_aliases = {
            name: Path(self.values[key]).expanduser().absolute() / self.user
            for name, key in (("lustre", "lustre_root"), ("fss", "fss_root"),
                              ("alluxio", "bucket_root")) if self.values[key]
        }
        self.roots = {name: path.resolve() for name, path in self.root_aliases.items()}
        self.storage_roots = sorted([*self.roots.items(), *mounted_roots()],
                                    key=lambda item: len(item[1].parts), reverse=True)
        self.reserved = [".gbi", ".prefect-*", *self.values["reserved_names"].split()]

    def classify(self, path):
        # Resolve the parent, but keep a final symlink as data, not a traversal.
        path = Path(path).expanduser().absolute()
        if path.parent in {root.parent for root in self.root_aliases.values()}:
            path = path.resolve()
        path = path.parent.resolve() / path.name
        for kind, root in [*self.storage_roots, ("posix", Path(path.anchor))]:
            if path == root or root in path.parents:
                self.check_root(root)
                if self.is_reserved(path, root):
                    raise ValueError(f"reserved transfer state: {path}")
                return path, kind, root

    def is_reserved(self, path, root):
        parts = path.relative_to(root).parts
        return (bool(parts) and parts[0] == "transfer-locks") or any(
            fnmatch.fnmatchcase(part, pattern)
            for part in parts for pattern in self.reserved
        )

    @staticmethod
    def check_root(root):
        if not root.is_dir():
            raise ValueError(f"storage root is unavailable on this node: {root}")
        if root.resolve() != root:
            raise ValueError(f"storage root changed: {root}")


def overlap(first, second):
    return first == second or first in second.parents or second in first.parents


def fingerprint(path):
    info = path.lstat()
    # Permission repairs change ctime without changing the data. The source's
    # inode, type, size and nanosecond mtime must remain unchanged instead.
    return [info.st_dev, info.st_ino, info.st_mode & 0o170000,
            info.st_size, info.st_mtime_ns]
