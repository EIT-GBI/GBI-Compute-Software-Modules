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
    configured   = site.values.get("usage_db", "").strip()
    default_root = site.roots.get("fss", site.roots["lustre"])
    path = (
        Path(configured).expanduser()
        if configured
        else default_root / ".gbi" / "usage.sqlite3"
    )
    if not path.is_absolute():
        path = site.path.parent / path
    path = path.resolve()
    allowed = [
        root
        for root in (site.roots.get("lustre"), site.roots.get("fss"))
        if root
    ]
    if not any(path == root or root in path.parents for root in allowed):
        raise ValueError(
            "usage snapshot is outside your configured storage roots"
        )
    return path


def _metadata(connection, user):
    try:
        rows = connection.execute("SELECT key, value FROM metadata").fetchall()
    except sqlite3.Error as error:
        raise ValueError("usage snapshot cannot be read") from error
    metadata = dict(rows)
    required = {
        "schema_version", "owner_uid", "root", "complete_input", "status",
        "snapshot_at", "published_at",
    }
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
    if (
        _timestamp(metadata["snapshot_at"]) is None
        or _timestamp(metadata["published_at"]) is None
    ):
        raise ValueError("usage snapshot has invalid publication timestamps")
    if metadata.get("owner") and metadata["owner"] != user:
        raise ValueError("usage snapshot belongs to another user")
    if metadata["status"] not in {"complete", "partial"}:
        raise ValueError("usage snapshot has an invalid publication status")
    return metadata


def _open(path):
    try:
        connection = sqlite3.connect(
            f"file:{quote(str(path))}?mode=ro&immutable=1", uri = True
        )
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
            [
                command, "quota", "-h", "-u", str(os.getuid()),
                str(site.roots["lustre"]),
            ], capture_output = True, text = True, timeout = 5, check = False,
        )
    except (OSError, subprocess.SubprocessError):
        return {"status": "unavailable", "detail": "lfs quota did not respond"}
    if result.returncode:
        return {"status": "unavailable", "detail": "lfs quota failed"}
    lines = [
        line.strip() for line in result.stdout.splitlines() if line.strip()
    ]
    for index, line in enumerate(lines):
        if not line.startswith("/"):
            continue
        tokens = line.split()
        if len(tokens) < 9 and index + 1 < len(lines):
            tokens.extend(lines[index + 1].split())
        if len(tokens) >= 9:
            labels = (
                "filesystem", "used", "quota", "limit", "grace", "files",
                "file_quota", "file_limit", "file_grace",
            )
            return {"status": "available", **dict(zip(labels, tokens[:9]))}
        return {"status": "unavailable", "detail": "malformed Lustre quota row"}
    return {"status": "unavailable", "detail": "no Lustre quota row"}


def _canonical_path(site, requested):
    root = site.roots["lustre"]
    if not requested:
        path = root
    else:
        candidate = Path(requested).expanduser()
        path      = candidate if candidate.is_absolute() else root / candidate
    try:
        path = path.resolve(strict = False)
    except OSError as error:
        raise ValueError(f"usage path is not accessible: {path}") from error
    if path != root and root not in path.parents:
        raise ValueError("usage is available only for your own Lustre root")
    if path.exists() and not path.is_dir():
        raise ValueError("usage path must be a directory")
    return root, path


def report(site, requested, depth, limit):
    root, path = _canonical_path(site, requested)
    quota    = _quota(site)
    database = database_path(site)
    if not database.exists():
        return {
            "path": path, "summary": None, "rows": [], "metadata": None,
            "quota": quota,
        }
    connection = _open(database)
    try:
        metadata     = _metadata(connection, site.user)
        catalog_root = metadata["root"]
        if (
            not Path(catalog_root).is_absolute()
            or Path(catalog_root).resolve() != root
        ):
            raise ValueError("usage snapshot is for a different Lustre root")
        relative = path.relative_to(root).as_posix() or "."
        try:
            summary = connection.execute(
                "SELECT apparent_bytes, entries FROM directories WHERE path = ?",
                (relative,),
            ).fetchone()
        except sqlite3.Error as error:
            raise ValueError(
                "usage snapshot is missing its directory table"
            ) from error
        if summary is None:
            raise ValueError(f"no cached usage result is available for {path}")
        rows    = []
        parents = [relative]
        for _ in range(depth):
            placeholders = ",".join("?" for _ in parents)
            try:
                level_rows = connection.execute(
                    f"SELECT path, apparent_bytes, entries FROM directories "
                    f"WHERE parent_path IN ({placeholders}) "
                    "ORDER BY apparent_bytes DESC, path LIMIT ?",
                    [*parents, limit],
                ).fetchall()
            except sqlite3.Error as error:
                raise ValueError(
                    "usage snapshot is missing its directory table"
                ) from error
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
    return (
        "--"
        if not value or value in {"0", "0k", "0K", "-"}
        else _lfs_size(value)
    )


def _count(value):
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return "--"


def _clip(value, width = 56):
    value = _display_path(value)
    return value if len(value) <= width else value[: width - 3] + "..."


def _fss_row(metadata):
    # The weekly publisher records fss_lookup_failed_at when it could not read
    # OCI; it then keeps the previous value, whose own observation time is kept.
    failed = bool(metadata) and "fss_lookup_failed_at" in metadata
    if not metadata or "fss_used_bytes" not in metadata:
        detail = (
            "lookup failed, no earlier value" if failed else "not published"
        )
        return ["FSS", "--", "--", "--", detail]
    observed = _timestamp(metadata.get("fss_observed_at"))
    source   = metadata.get("fss_source", "OCI per-UID usage")
    if observed:
        source = f"{_display_path(source)}, {_age(observed)}"
    if failed:
        source = f"{source} (latest lookup failed)"
    return [
        "FSS", _size(metadata["fss_used_bytes"]),
        (
            _size(metadata["fss_limit_bytes"])
            if metadata.get("fss_limit_bytes")
            else "--"
        ), _count(metadata.get("fss_files")), source,
    ]


def _table(headers, rows, *, right = ()):
    widths = [
        max(
            len(str(value)) for value in [header, *(row[index] for row in rows)]
        )
        for index, header in enumerate(headers)
    ]
    for row in (headers, ["-" * width for width in widths], *rows):
        print(
            "  ".join(
                (
                    str(value).rjust(widths[index])
                    if index in right
                    else str(value).ljust(widths[index])
                )
                for index, value in enumerate(row)
            ).rstrip()
        )

def print_report(result, depth, limit):
    import sys

    def quota_bytes(value):
        """Convert lfs quota -h output to bytes; bare values are KiB."""
        text = str(value or "").strip().rstrip("*")
        if text in {"", "-", "--"}:
            return None

        powers = {"k": 1, "K": 1, "M": 2, "G": 3, "T": 4, "P": 5}
        suffix = text[-1:]
        power = powers.get(suffix, 1)
        number = text[:-1] if suffix in powers else text

        try:
            value = float(number) * 1024 ** power
            return int(value) if value >= 0 else None
        except (ValueError, OverflowError):
            return None

    def stored_bytes(value):
        """Read a nonnegative byte count from snapshot metadata."""
        try:
            value = int(value)
        except (TypeError, ValueError):
            return None
        return value if value >= 0 else None

    def percentage_colour(percent):
        if percent > 80:
            return "\033[31m"
        if percent >= 60:
            return "\033[33m"
        return "\033[32m"

    use_colour = (
            sys.stdout.isatty()
            and "NO_COLOR" not in os.environ
    )

    metadata = result.get("metadata") or {}
    quota = result["quota"]
    notes = []

    # Lustre: live usage. Zero means no configured hard quota, so
    # its Hard quota and Used % cells remain blank.
    lustre_used = None
    lustre_limit = None

    if quota["status"] == "available":
        lustre_used = quota_bytes(quota.get("used"))
        lustre_limit = quota_bytes(quota.get("limit"))
        notes.append("Lustre quota accounting: live")
    else:
        notes.append(
            f"Lustre: {quota.get('detail', 'usage unavailable')}"
        )

    # FSS: published OCI accounting, with its own observation time.
    fss_used = stored_bytes(metadata.get("fss_used_bytes"))
    fss_limit = stored_bytes(metadata.get("fss_limit_bytes"))
    fss_failed = bool(metadata.get("fss_lookup_failed_at"))

    if fss_used is None:
        detail = (
            "latest lookup failed; no earlier value"
            if fss_failed
            else "usage not published"
        )
        notes.append(f"FSS: {detail}")
    else:
        observed = _timestamp(metadata.get("fss_observed_at"))
        detail = (
            f"observed {_age(observed)}"
            if observed is not None
            else "observation time unavailable"
        )
        if fss_failed:
            detail += "; latest lookup failed, showing last known value"
        notes.append(f"FSS: {detail}")

        if fss_limit is None:
            notes.append("FSS hard quota: unavailable")

    measurements = [
        ("Lustre", lustre_used, lustre_limit),
        ("FSS", fss_used, fss_limit),
    ]
    measurements.sort(
        key=lambda row: (
            row[1] is None,
            -(row[1] if row[1] is not None else 0),
            row[0],
        )
    )

    rows = []
    percentages = []

    for tier, used, hard_limit in measurements:
        used_text = _size(used) if used is not None else "—"
        limit_text = ""
        percent_text = ""
        percent = None

        if hard_limit is not None and hard_limit > 0:
            limit_text = _size(hard_limit)

            if used is not None:
                percent = 100 * used / hard_limit
                percent_text = f"{percent:.1f}%"

                # Avoid displaying a rounded boundary value with the
                # colour belonging to the other side of that boundary.
                rounded = float(percent_text[:-1])
                if (
                    percentage_colour(rounded)
                    != percentage_colour(percent)
                ):
                    percent_text = f"{percent:.3f}%"
        elif tier == "FSS" and hard_limit is None:
            limit_text = "—"
            percent_text = "—"

        rows.append([tier, used_text, limit_text, percent_text])
        percentages.append(percent)

    print("Storage usage")

    # Pad plain text first so ANSI sequences do not affect alignment.
    headers = ("Tier", "Used", "Hard quota", "Used %")
    widths = [
        max(len(header), *(len(row[index]) for row in rows))
        for index, header in enumerate(headers)
    ]

    def table_line(values, percent=None):
        cells = []

        for index, value in enumerate(values):
            cell = (
                value.ljust(widths[index])
                if index == 0
                else value.rjust(widths[index])
            )

            if index == 3 and percent is not None and use_colour:
                cell = (
                    percentage_colour(percent)
                    + cell
                    + "\033[0m"
                )

            cells.append(cell)

        return "  ".join(cells).rstrip()

    print(table_line(headers))
    for row, percent in zip(rows, percentages):
        print(table_line(row, percent))

    print()
    for note in notes:
        print(note)

    if result["summary"] is None:
        print(
            "\nLustre folders\n"
            "No published Lustre folder inventory is available. "
            "No filesystem scan was started."
        )
        return

    status = metadata.get("status", "complete")
    stamp = metadata["snapshot_at"]
    age = _age(_timestamp(stamp))

    print(
        f"\nLustre folders · {status} snapshot {stamp} "
        f"({age}) · apparent sizes"
    )

    if status == "partial":
        unreadable = _count(metadata.get("unreadable_directories"))
        detail = (
            f"{unreadable} folders could not be opened"
            if unreadable not in {"--", "0"}
            else "some paths are missing"
        )
        print(f"Warning: partial inventory; {detail}.")

    # report() currently returns:
    # (path relative to Lustre root, apparent_bytes, entries).
    # Derive parent relationships without changing that interface.
    selected = Path(result["path"])
    inventory_root = Path(metadata["root"])
    relative_root = selected.relative_to(inventory_root).as_posix()

    children = {}
    for path, apparent_bytes, entries in result["rows"]:
        parent = Path(path).parent.as_posix()
        children.setdefault(parent, []).append(
            (path, apparent_bytes, entries)
        )

    for siblings in children.values():
        siblings.sort(key=lambda row: (-row[1], row[0]))

    total_bytes, total_entries = result["summary"]
    print(
        f"{_display_path(selected)}/"
        f"  {_size(total_bytes)}"
        f"  ({total_entries:,} inventory entries)"
    )

    def print_children(parent, prefix=""):
        siblings = children.get(parent, [])

        for index, (path, apparent_bytes, entries) in enumerate(siblings):
            last = index == len(siblings) - 1
            connector = "└── " if last else "├── "
            name = _display_path(Path(path).name)

            print(
                f"{prefix}{connector}{name}/"
                f"  {_size(apparent_bytes)}"
            )

            child_prefix = prefix + ("    " if last else "│   ")
            print_children(path, child_prefix)

    print_children(relative_root)

    print(
        f"\nShowing up to {limit} folders at each of "
        f"{depth} selected depth levels."
        "\nFolders are sorted largest first within each parent."
        "\nParent totals include their descendants; do not add tree rows."
        "\nApparent sizes can differ from quota-accounted usage."
    )
