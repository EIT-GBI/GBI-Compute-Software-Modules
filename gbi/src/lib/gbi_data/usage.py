"""Read an owner-scoped, published Lustre usage snapshot."""

from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
from urllib.parse import quote


SCHEMA_VERSION = "1"


def database_path(site):
    configured = site.values.get("usage_db", "").strip()
    default_root = site.roots.get("fss", site.roots["lustre"])
    path = Path(configured).expanduser() if configured else default_root / ".gbi" / "usage.sqlite3"
    if not path.is_absolute():
        path = site.path.parent / path
    path = path.resolve()
    allowed = [root for root in (site.roots.get("lustre"), site.roots.get("fss")) if root]
    if not any(path == root or root in path.parents for root in allowed):
        raise ValueError("usage snapshot is outside your configured storage roots")
    return path


def _metadata(connection, user):
    try:
        rows = connection.execute("SELECT key, value FROM metadata").fetchall()
    except sqlite3.Error as error:
        raise ValueError("usage snapshot cannot be read") from error
    metadata = dict(rows)
    required = {"schema_version", "owner_uid", "root", "complete_input", "status",
                "snapshot_at", "published_at"}
    if not required.issubset(metadata):
        raise ValueError("usage snapshot is missing publication metadata")
    if metadata.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("usage snapshot schema is unsupported")
    if metadata["owner_uid"] != str(os.getuid()):
        raise ValueError("usage snapshot belongs to another user")
    complete_input = metadata.get("complete_input")
    if complete_input is not None and complete_input not in {"true", "false"}:
        raise ValueError("usage snapshot has an invalid completeness marker")
    if complete_input == "false":
        metadata["status"] = "partial"
    if _timestamp(metadata["snapshot_at"]) is None or _timestamp(metadata["published_at"]) is None:
        raise ValueError("usage snapshot has invalid publication timestamps")
    if metadata.get("owner") and metadata["owner"] != user:
        raise ValueError("usage snapshot belongs to another user")
    if metadata["status"] not in {"complete", "partial"}:
        raise ValueError("usage snapshot has an invalid publication status")
    return metadata


def _open(path):
    try:
        connection = sqlite3.connect(f"file:{quote(str(path))}?mode=ro&immutable=1", uri=True)
    except sqlite3.Error as error:
        raise ValueError("published usage snapshot cannot be read") from error
    return connection


def _timestamp(value):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed


def _age(timestamp):
    if timestamp is None:
        return "unknown age"
    seconds = int((datetime.now(timezone.utc) - timestamp).total_seconds())
    if seconds < 0:
        return "future timestamp"
    if seconds < 3600:
        return f"{seconds // 60}m old"
    if seconds < 86400:
        return f"{seconds // 3600}h old"
    return f"{seconds // 86400}d old"


def _quota(site):
    command = shutil.which(site.values.get("lfs_bin", "lfs"))
    if not command:
        return "unavailable (lfs is not installed)"
    try:
        result = subprocess.run(
            [command, "quota", "-h", "-u", str(os.getuid()), str(site.roots["lustre"])],
            capture_output=True, text=True, timeout=5, check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return "unavailable (lfs quota did not respond)"
    if result.returncode:
        return "unavailable (lfs quota failed)"
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    for index, line in enumerate(lines):
        if not line.startswith("/"):
            continue
        tokens = line.split()
        if len(tokens) < 9 and index + 1 < len(lines):
            tokens.extend(lines[index + 1].split())
        if len(tokens) >= 9:
            labels = ("filesystem", "used", "quota", "limit", "grace",
                      "files", "file_quota", "file_limit", "file_grace")
            return " ".join(f"{label}={value}" for label, value in zip(labels, tokens[:9]))
        return f"whole-filesystem UID quota: {' '.join(tokens)}"
    return "unavailable (no quota row)"


def _canonical_path(site, requested):
    root = site.roots["lustre"]
    if not requested:
        path = root
    else:
        candidate = Path(requested).expanduser()
        path = candidate if candidate.is_absolute() else root / candidate
    try:
        path = path.resolve(strict=False)
    except OSError as error:
        raise ValueError(f"usage path is not accessible: {path}") from error
    if path != root and root not in path.parents:
        raise ValueError("usage is available only for your own Lustre root")
    if path.exists() and not path.is_dir():
        raise ValueError("usage path must be a directory")
    return root, path


def report(site, requested, depth, limit):
    root, path = _canonical_path(site, requested)
    quota = _quota(site)
    database = database_path(site)
    if not database.exists():
        return {"path": path, "summary": None, "rows": [], "metadata": None, "quota": quota}
    connection = _open(database)
    try:
        metadata = _metadata(connection, site.user)
        catalog_root = metadata["root"]
        if not Path(catalog_root).is_absolute() or Path(catalog_root).resolve() != root:
            raise ValueError("usage snapshot is for a different Lustre root")
        relative = path.relative_to(root).as_posix() or "."
        try:
            summary = connection.execute(
                "SELECT apparent_bytes, entries FROM directories WHERE path = ?", (relative,)
            ).fetchone()
        except sqlite3.Error as error:
            raise ValueError("usage snapshot is missing its directory table") from error
        if summary is None:
            raise ValueError(f"no cached usage result is available for {path}")
        rows = []
        parents = [relative]
        for _ in range(depth):
            placeholders = ",".join("?" for _ in parents)
            try:
                level_rows = connection.execute(
                    f"SELECT path, apparent_bytes, entries FROM directories "
                    f"WHERE parent_path IN ({placeholders}) ORDER BY apparent_bytes DESC, path LIMIT ?",
                    [*parents, limit],
                ).fetchall()
            except sqlite3.Error as error:
                raise ValueError("usage snapshot is missing its directory table") from error
            rows.extend(level_rows)
            parents = [row[0] for row in level_rows]
            if not parents:
                break
        return {
            "path": path, "summary": summary, "rows": rows,
            "metadata": metadata, "quota": quota,
        }
    finally:
        connection.close()


def _size(value):
    value = float(value)
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    index = 0
    while abs(value) >= 1024 and index < len(units) - 1:
        value /= 1024
        index += 1
    return f"{value:.1f} {units[index]}"


def _display_path(path):
    return str(path).encode("unicode_escape").decode("ascii")


def print_report(result, depth, limit):
    metadata = result["metadata"]
    print(f"Selected Lustre root: {_display_path(result['path'])}")
    print(f"Lustre quota (live, whole-filesystem UID): {result['quota']}")
    if result["summary"] is None:
        print("No cached folder report is available yet. No filesystem scan was started.")
        return
    status = metadata.get("status", "complete")
    stamp = metadata["snapshot_at"]
    parsed_stamp = _timestamp(stamp)
    print(f"Inventory apparent bytes ({status}, snapshot {stamp}, {_age(parsed_stamp)}): "
          f"{_size(result['summary'][0])}; entries: {result['summary'][1]}")
    if status == "partial":
        print("Warning: this publication is partial; missing paths are not included.")
    print("Folder\tApparent size\tEntries")
    for path, apparent_bytes, entries in result["rows"]:
        print(f"{_display_path(path)}\t{_size(apparent_bytes)}\t{entries} entries")
    print("Snapshot totals include directory and symlink sizes; they are not allocated disk space.")
