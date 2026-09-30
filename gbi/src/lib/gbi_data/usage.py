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
        return {"status": "unavailable", "detail": "lfs is not installed"}
    try:
        result = subprocess.run(
            [command, "quota", "-h", "-u", str(os.getuid()), str(site.roots["lustre"])],
            capture_output=True, text=True, timeout=5, check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return {"status": "unavailable", "detail": "lfs quota did not respond"}
    if result.returncode:
        return {"status": "unavailable", "detail": "lfs quota failed"}
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
            return {"status": "available", **dict(zip(labels, tokens[:9]))}
        return {"status": "unavailable", "detail": "malformed Lustre quota row"}
    return {"status": "unavailable", "detail": "no Lustre quota row"}


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
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    index = 0
    while abs(value) >= 1024 and index < len(units) - 1:
        value /= 1024
        index += 1
    return f"{value:.1f} {units[index]}"


def _display_path(path):
    return str(path).encode("unicode_escape").decode("ascii")


def _lfs_size(value):
    """Render an ``lfs quota -h`` value (``85.2G``) in the same units as FSS."""
    value = str(value or "").rstrip("*")
    scale = {"k": 1, "K": 1, "M": 2, "G": 3, "T": 4, "P": 5}
    try:
        if value[-1:] in scale:
            return _size(float(value[:-1]) * 1024 ** scale[value[-1]])
        return _size(float(value) * 1024)  # lfs reports bare numbers in KiB
    except ValueError:
        return "--"


def _limit(value):
    return "--" if not value or value in {"0", "0k", "0K", "-"} else _lfs_size(value)


def _count(value):
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return "--"


def _clip(value, width=56):
    value = _display_path(value)
    return value if len(value) <= width else value[:width - 3] + "..."


def _fss_row(metadata):
    if not metadata or "fss_used_bytes" not in metadata:
        return ["FSS", "--", "--", "--", "not published"]
    observed = _timestamp(metadata.get("fss_observed_at"))
    source = metadata.get("fss_source", "OCI per-UID usage")
    if observed:
        source = f"{_display_path(source)}, {_age(observed)}"
    return [
        "FSS", _size(metadata["fss_used_bytes"]),
        _size(metadata["fss_limit_bytes"]) if metadata.get("fss_limit_bytes") else "--",
        _count(metadata.get("fss_files")), source,
    ]


def _table(headers, rows, *, right=()):
    widths = [max(len(str(value)) for value in [header, *(row[index] for row in rows)])
              for index, header in enumerate(headers)]
    for row in (headers, ["-" * width for width in widths], *rows):
        print("  ".join(str(value).rjust(widths[index]) if index in right
                        else str(value).ljust(widths[index])
                        for index, value in enumerate(row)).rstrip())


def print_report(result, depth, limit):
    metadata = result["metadata"]
    print(f"Usage for {_display_path(result['path'])}")
    print("\nStorage summary")
    quota = result["quota"]
    if quota["status"] == "available":
        lustre = ["Lustre", _lfs_size(quota["used"]), _limit(quota["limit"]),
                  _count(quota["files"]), "live UID quota"]
    else:
        lustre = ["Lustre", "--", "--", "--", quota["detail"]]
    _table(("Tier", "Used", "Limit", "Files", "Measured by"),
           [lustre, _fss_row(metadata)], right={1, 2, 3})
    if result["summary"] is None:
        print("\nFolder breakdown\nNo published Lustre folder inventory is available."
              " No filesystem scan was started.")
        return
    status = metadata.get("status", "complete")
    stamp = metadata["snapshot_at"]
    parsed_stamp = _timestamp(stamp)
    print(f"\nLustre folder breakdown ({status} snapshot {stamp}, {_age(parsed_stamp)})")
    print(f"Apparent total: {_size(result['summary'][0])} across "
          f"{result['summary'][1]:,} inventory entries")
    if status == "partial":
        print("Warning: this publication is partial; missing paths are not included.")
    rows = [[_clip(path), _size(apparent_bytes), f"{entries:,}"]
            for path, apparent_bytes, entries in result["rows"]]
    _table(("Folder", "Apparent size", "Entries"), rows, right={1, 2})
    print("\nApparent sizes include directory and symlink entries; they are not allocated space.")
