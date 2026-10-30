"""Deterministic metadata-only plans for an explicit archive operation.

The planner chooses a stable set of archive units without reading payloads or
creating outputs.  Archive qualification remains owned by ``archives``; this
module only partitions the source tree and records the mapping needed to
replay that partition after a partial transfer.
"""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import math
import os
from pathlib import Path, PurePosixPath
import time

from . import archives, chunks

_OVERSIZE_MARKERS = (
    "archive manifest exceeds", "source observation inventory exceeds",
)
_MAX_PLANNED_PARTS = 4096
# Threads re-observing the inventory after planning; each is one lstat.
_VERIFY_WORKERS    = 16
_VERIFY_WINDOW     = 4096
_ARCHIVE_SUFFIX    = ".gbi.tar"
_CHUNK_SUFFIX      = ".gbi-chunks"
_LINK_SUFFIX       = ".rclonelink"
_RESERVED_NAMES    = (".gbi", ".prefect-*", "transfer-locks")


@dataclass(frozen = True)
class ArchivePlan:
    """A serializable archive partition and its source observations."""

    source: str
    destination: str
    units: tuple

    def as_dict(self):
        return {
            "version": 1, "source": self.source,
            "destination": self.destination,
            "units": [dict(unit) for unit in self.units],
        }


def _observation(info):
    return archives._observation(info)


def _is_oversized(error):
    return any(marker in str(error) for marker in _OVERSIZE_MARKERS)


def _subtree_budget(
            entries, relative, *, includes, exclusions, selection_mode, visit,
        ):
    """The metadata budget of one inventory subtree, without a second walk.

    ``entries`` are the ``(name, item)`` pairs below ``relative`` in walk
    order; they are replayed through the codec's own budget accounting with
    names re-rooted at the subtree, exactly as a live scan of that directory
    would see them.
    """
    selected = archives._selection(
        includes, exclusions, selection_mode,
        relative if selection_mode == "relative" else "",
    )
    skip  = len(relative) + 1
    items = {name[skip:]: item for name, item in entries}

    def stream():
        for name, item in items.items():
            yield name, item["info"], item["kind"]

    return archives._metadata_budget(
        stream(), selected, visit = visit,
        read_target = lambda name: items[name]["target"],
    )


def _subtree_bounds(inventory):
    """Start and end positions of every directory's subtree in walk order."""
    order  = list(inventory)
    bounds = {}
    stack  = []
    for index, name in enumerate(order):
        while stack and not name.startswith(stack[-1][0] + "/"):
            top, start = stack.pop()
            bounds[top] = (start, index)
        if inventory[name]["kind"] == "directory":
            stack.append((name, index + 1))
    while stack:
        top, start = stack.pop()
        bounds[top] = (start, len(order))
    return order, bounds


def _archive_estimate(result):
    """Conservatively estimate tar bytes including metadata and padding."""
    entries = int(result["entries"])
    return (
        int(result["selected_bytes"])
        + int(result["manifest_bytes"])
        + int(result["observation_bytes"])
        + (entries + 4) * 4096
    )


def _chunk_size(estimated_bytes, requested):
    return max(requested, math.ceil(estimated_bytes / _MAX_PLANNED_PARTS))


def _restore_name(target):
    name = (
        target[: -len(_CHUNK_SUFFIX)]
        if target.endswith(_CHUNK_SUFFIX)
        else target
    )
    if name.endswith(_ARCHIVE_SUFFIX):
        return name[: -len(_ARCHIVE_SUFFIX)]
    return None


def _verify_inventory(source, inventory, progress):
    """Re-observe every entry after planning, several at a time, in order."""

    def observe(relative):
        try:
            return _observation((source / PurePosixPath(relative)).lstat())
        except OSError:
            return None

    names = list(inventory)
    with ThreadPoolExecutor(
        max_workers = _VERIFY_WORKERS, thread_name_prefix = "gbi-plan-verify"
    ) as pool:
        for start in range(0, len(names), _VERIFY_WINDOW):
            window = names[start:start + _VERIFY_WINDOW]
            for relative, current in zip(window, pool.map(observe, window)):
                progress()
                if current != inventory[relative]["observation"]:
                    raise archives.ArchiveError(
                        f"source changed during archive discovery: {relative}"
                    )


def _validate_units(units, source_entries):
    targets      = [unit["target_relative"] for unit in units]
    source_names = set(source_entries)
    generated_targets = {
        unit["target_relative"] for unit in units if unit["action"] == "pack"
    }
    physical_targets = set(generated_targets)
    for unit in units:
        if (
            unit["action"] == "file"
            and source_entries[unit["source_relative"]]["kind"] == "symlink"
        ):
            physical_targets.add(unit["target_relative"] + _LINK_SUFFIX)
    collisions = sorted(physical_targets & source_names)
    if collisions:
        raise ValueError(
            f"archive target collides with source path: {collisions[0]}"
        )
    if len(targets) != len(set(targets)):
        raise ValueError("archive plan has duplicate destination paths")

    restores   = {}
    target_set = set(targets)
    for target in targets:
        restored = _restore_name(target)
        if restored is None:
            continue
        if restored in target_set:
            raise ValueError(
                f"archive restore path collides with destination path: {restored}"
            )
        prior = restores.setdefault(restored, target)
        if prior != target:
            raise ValueError(f"archive restore path collision: {restored}")


def plan_archive(
            source, destination, *, includes = (), exclusions = (),
            selection_mode = "basename", chunk_threshold_bytes = None,
            chunk_size_bytes = chunks.DEFAULT_CHUNK_SIZE, reserved_names = (),
            max_entries = 100000, max_seconds = 5.0, visit = lambda: None,
        ):
    """Plan one stable archive set from a directory.

    Child directories that pass ``archives.check_metadata_budget`` become
    archive units.  A directory that genuinely exceeds the existing archive
    metadata limit is split into its children; all other qualification errors
    abort planning.  ``chunk_threshold_bytes`` is the site's automatic
    chunking threshold for an estimated packed payload.
    """
    source      = Path(source).absolute()
    destination = Path(destination).absolute()
    if source.is_symlink() or not source.is_dir() or source.resolve() != source:
        raise ValueError(
            "archive source must be a directory without symlink parents"
        )
    if max_entries is not None and (
        type(max_entries) is not int or max_entries < 1
    ):
        raise ValueError("maximum archive entries must be positive")
    if max_seconds is not None and (
        not isinstance(max_seconds, (int, float))
        or not math.isfinite(max_seconds)
        or max_seconds <= 0
    ):
        raise ValueError(
            "maximum archive discovery time must be positive and finite"
        )
    if chunk_threshold_bytes is not None and (
        type(chunk_threshold_bytes) is not int or chunk_threshold_bytes < 1
    ):
        raise ValueError("chunk threshold must be positive")
    if type(chunk_size_bytes) is not int or chunk_size_bytes < 1:
        raise ValueError("chunk size must be positive")

    selected         = archives._selection(includes, exclusions, selection_mode)
    started          = time.monotonic()
    root_observation = _observation(source.lstat())
    inventory        = {}
    reserved         = (*_RESERVED_NAMES, *reserved_names)

    def progress():
        visit()
        if (
            max_seconds is not None
            and time.monotonic() - started >= max_seconds
        ):
            raise archives.ArchiveError(
                "archive discovery budget exhausted; no plan created"
            )

    for relative, info, kind in archives._walk(source, reserved):
        progress()
        if (max_entries is not None and len(inventory) >= max_entries) or (
            max_seconds is not None
            and time.monotonic() - started >= max_seconds
        ):
            raise archives.ArchiveError(
                "archive discovery budget exhausted; no plan created"
            )
        if kind == "special":
            raise archives.ArchiveError(
                f"unsupported special file: {relative!r}"
            )
        target = None
        if kind == "symlink":
            try:
                target = os.readlink(source / PurePosixPath(relative))
            except OSError as error:
                raise archives.ArchiveError(
                    f"source changed during archive discovery: {relative}"
                ) from error
        inventory[relative] = {
            "kind": kind, "observation": _observation(info), "info": info,
            "bytes": info.st_size if kind in ("file", "symlink") else 0,
            "selected": selected(relative), "target": target,
        }
    order, bounds = _subtree_bounds(inventory)
    children = {}
    for relative in sorted(inventory):
        parent = str(PurePosixPath(relative).parent)
        children.setdefault("." if parent == "." else parent, []).append(
            relative
        )
    for values in children.values():
        values.sort()

    units = []

    def add_plain(relative):
        item = inventory[relative]
        if any(
            relative.endswith(suffix)
            for suffix in (_ARCHIVE_SUFFIX, ".gbi.tar.gz", _CHUNK_SUFFIX)
        ):
            raise archives.ArchiveError(
                f"encoded source requires explicit restore: {relative}"
            )
        units.append(
            {
                "source_relative": relative, "target_relative": relative,
                "action": (
                    "directory" if item["kind"] == "directory" else "file"
                ), "bytes": item["bytes"],
                "expected_source": item["observation"], "chunk_size": None,
            }
        )

    def add_archive(relative, result):
        payload_bytes   = int(result["selected_bytes"])
        estimated_bytes = _archive_estimate(result)
        chunk_size      = None
        target          = relative + _ARCHIVE_SUFFIX
        if (
            chunk_threshold_bytes is not None
            and estimated_bytes > chunk_threshold_bytes
        ):
            chunk_size = _chunk_size(estimated_bytes, chunk_size_bytes)
            target     += _CHUNK_SUFFIX
        units.append(
            {
                "source_relative": relative, "target_relative": target,
                "action": "pack", "bytes": payload_bytes,
                "estimated_bytes": estimated_bytes,
                "expected_source": inventory[relative]["observation"],
                "chunk_size": chunk_size,
            }
        )

    def visit_directory(relative):
        progress()
        if relative.endswith(_CHUNK_SUFFIX):
            raise archives.ArchiveError(
                f"encoded source requires explicit restore: {relative}"
            )
        child_paths = children.get(relative, [])
        start, end  = bounds[relative]
        entries     = [(name, inventory[name]) for name in order[start:end]]
        if not child_paths:
            if inventory[relative]["selected"]:
                # The existing marked empty archive restores its named root.
                # This also represents empty directories on native Object
                # Storage, which has no POSIX directory entry to copy.
                add_archive(
                    relative,
                    _subtree_budget(
                        entries, relative, includes = (), exclusions = (),
                        selection_mode = "basename", visit = progress,
                    ),
                )
            return
        try:
            result = _subtree_budget(
                entries, relative, includes = includes,
                exclusions = exclusions, selection_mode = selection_mode,
                visit = progress,
            )
        except archives.ArchiveError as error:
            if not _is_oversized(error):
                raise
            for child in child_paths:
                item = inventory[child]
                if item["kind"] == "directory":
                    visit_directory(child)
                elif item["selected"]:
                    add_plain(child)
            return
        if result["entries"] == 0:
            for child in child_paths:
                if inventory[child]["kind"] == "directory":
                    visit_directory(child)
                elif inventory[child]["selected"]:
                    add_plain(child)
            return
        add_archive(relative, result)
        progress()

    for relative in children.get(".", []):
        if inventory[relative]["kind"] == "directory":
            visit_directory(relative)
        elif inventory[relative]["selected"]:
            add_plain(relative)

    if _observation(source.lstat()) != root_observation:
        raise archives.ArchiveError(
            "source changed during archive discovery; no plan created"
        )
    _verify_inventory(source, inventory, progress)
    _validate_units(units, inventory)
    progress()
    units.sort(key = lambda unit: unit["target_relative"])
    return ArchivePlan(str(source), str(destination), tuple(units))
