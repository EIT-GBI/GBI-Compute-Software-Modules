"""Site configuration and path rules. Configuration never executes shell code."""

import fnmatch
import os
from pathlib import Path
import pwd


DEFAULTS = {
    "lustre_root": "", "fss_root": "", "bucket_root": "",
    "partition": "", "time_limit": "1-00:00:00", "cpus": "2", "mem": "4G",
    "jobs": "4", "file_timeout": "86400", "verify_settle_seconds": "1200",
    "rclone_bin": "rclone", "reserved_names": "",
}


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
        for key in ("jobs", "cpus", "file_timeout", "verify_settle_seconds"):
            if int(self.values[key]) < 1:
                raise ValueError(f"{key} must be positive")
        self.user = pwd.getpwuid(os.getuid()).pw_name
        self.aliases = {Path(self.values[key]).expanduser().absolute() / self.user
                        for key in ("lustre_root", "fss_root", "bucket_root") if self.values[key]}
        self.roots = {
            name: (Path(self.values[key]) / self.user).resolve()
            for name, key in (("lustre", "lustre_root"), ("fss", "fss_root"),
                              ("alluxio", "bucket_root")) if self.values[key]
        }
        self.reserved = [".gbi", ".prefect-*", *self.values["reserved_names"].split()]

    def classify(self, path):
        # Resolve the parent, but keep a final symlink as data, not a traversal.
        path = Path(path).expanduser().absolute()
        if path in self.aliases:
            path = path.resolve()
        path = path.parent.resolve() / path.name
        for kind, root in self.roots.items():
            if path == root or root in path.parents:
                self.check_root(root)
                if self.is_reserved(path, root):
                    raise ValueError(f"reserved transfer state: {path}")
                return path, kind, root
        raise ValueError(f"outside your configured storage roots: {path}")

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
