"""Reconcile one saved ordinary request without changing its history."""

from itertools import chain
import json
import os
from pathlib import Path
import re
import socket
import stat
import subprocess

from . import jobs
from .selection import matches
from .storage import overlap
from .transfer import target_path, validate_source_retention


def load(run_dir):
    if (run_dir / "history.json").is_file():
        saved = json.loads((run_dir / "history.json").read_text())
        if not isinstance(saved["receipts"], list):
            raise ValueError("malformed saved receipts")
        return saved["request"], saved["progress"], saved["receipts"]
    request = json.loads((run_dir / "request.json").read_text())
    progress = json.loads((run_dir / "progress.json").read_text())
    return request, progress, _rows(run_dir / "receipts.jsonl")


def _rows(path):
    if path.exists():
        with path.open() as stream:
            for line in stream:
                yield json.loads(line)


def uncertain_worker(record):
    if "workers_stopped" in record:
        return record["workers_stopped"] is not True
    # Older records did not prove process-group disappearance. A reaped
    # Python worker can still have a blocked rclone descendant holding I/O.
    return any(text in record.get("error", "") for text in (
        "kernel I/O may still be active", "interrupted or timed out:",
        "transfer worker exited", "transfer worker returned no valid result",
    ))


def stopped(run_dir, request, progress, failures, unresolved):
    if progress.get("phase") not in ("complete", "failed", "interrupted"):
        raise ValueError(
            "previous transfer is active or uncertain; inspect its status "
            "before retrying"
        )
    if (
        progress.get("workers_stopped") is False
        or unresolved or progress.get("failed", 0) > failures
    ):
        raise ValueError(
            "previous workers are unresolved; inspect the original output "
            "and worker PIDs before retrying"
        )
    if progress.get("hostname") == socket.gethostname():
        pid = progress.get("coordinator_pid")
        if type(pid) is not int or pid <= 0:
            raise ValueError(
                "previous coordinator identity is missing; cannot confirm "
                "it stopped"
            )
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            pass
        else:
            identity = jobs.process_identity(pid)
            if (
                identity is None or progress.get("coordinator_identity") is None
                or identity == progress["coordinator_identity"]
            ):
                raise ValueError("previous transfer coordinator is still active or uncertain")
    # An allocation can host successive transfers; its state is not this
    # coordinator's state. A detached job, however, belongs to this request.
    if request["execution"] == "slurm":
        path = run_dir / "slurm.json"
        record = json.loads(path.read_text()) if path.is_file() else {}
        job_id = record.get("job_id")
        if not isinstance(job_id, str) or not job_id.isdigit():
            raise ValueError(
                "previous Slurm submission is unconfirmed; inspect it "
                "before retrying"
            )
        try:
            state = jobs.slurm_state(job_id)
        except (OSError, subprocess.SubprocessError):
            state = "UNKNOWN"
        if state not in (
            "COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL",
            "OUT_OF_MEMORY",
        ):
            raise ValueError(
                f"previous Slurm job {job_id} is {state}; "
                "retry requires confirmed terminal state"
            )


def _path(request, value, empty = False):
    path, source = Path(value), Path(request["source"])
    if not path.is_absolute() or str(path) != value or ".." in path.parts:
        raise ValueError("saved selection contains a noncanonical source path")
    if path != source and (not request["directory"] or source not in path.parents):
        raise ValueError("saved selection escapes the original source")
    if not empty and not matches(path.name, request["include"], request["exclude"]):
        raise ValueError("saved receipt or selection falls outside the original filters")
    return path


def _identity(value):
    return (
        isinstance(value, list) and len(value) == 5
        and all(type(item) is int for item in value)
        and value[2] in (stat.S_IFREG, stat.S_IFLNK) and value[3] >= 0
    )


def prepare(run_dir, site):
    try:
        request, progress, rows = load(run_dir)
        if (
            not isinstance(request, dict) or not isinstance(progress, dict)
        ):
            raise ValueError("malformed saved transfer history")
        if request["uid"] != os.getuid():
            raise ValueError("this request belongs to another user")
        if request["execution"] not in ("inline", "allocation", "slurm"):
            raise ValueError("this is not an ordinary transfer request")
        for key in ("delete", "directory"):
            if type(request[key]) is not bool:
                raise ValueError(f"saved {key} must be a boolean")
        if request["verb"] not in ("copy", "move") or (
            request["verb"] == "copy" and request["delete"]
        ):
            raise ValueError("saved request has inconsistent deletion consent")
        request = {**request, "exclude": request.get("exclude", [])}
        for key in ("include", "exclude"):
            if not isinstance(request[key], list) or not all(
                isinstance(item, str) for item in request[key]
            ):
                raise ValueError("saved selection filters are malformed")
        for prefix in ("source", "target"):
            path, kind, root = site.classify(request[prefix])
            if (str(path), kind, str(root)) != (
                request[prefix], request[prefix + "_kind"],
                request[prefix + "_root"],
            ):
                raise ValueError("storage route changed since the saved transfer")
        if overlap(Path(request["source"]), Path(request["target"])):
            raise ValueError("saved source and destination overlap")
        validate_source_retention(request, site.storage_roots)
        if any(request.get(key) for key in (
            "archive_auto", "pack", "packing_policy", "chunk_size",
            "source_format", "format_action", "archive_completed",
        )):
            raise ValueError(
                "retry supports ordinary files and links; repeat the original "
                "archive/packed command with its recovery state"
            )
        selected = request.get("selection")
        if selected is not None:
            if not isinstance(selected, list):
                raise ValueError("malformed saved selection")
            normalized = []
            for item in selected:
                if item.get("format_action"):
                    raise ValueError(
                        "retry cannot reinterpret archive/packed units; "
                        "repeat their original command"
                    )
                path = _path(
                    request, item.get("source", item.get("path")), item["empty"]
                )
                identity = item.get("expected_source", item.get("fingerprint"))
                if type(item["empty"]) is not bool or (
                    not item["empty"] and not _identity(identity)
                ):
                    raise ValueError("malformed saved source identity")
                normalized.append({
                    "path": str(path), "empty": item["empty"],
                    "fingerprint": identity,
                })
            selected = normalized
            if len({item["path"] for item in selected}) != len(selected):
                raise ValueError("saved selection contains duplicate sources")
        evidence = {}
        missing = set(request.get("retry_missing", []))
        failures, unresolved = 0, False
        incomplete_discovery = False
        inherited = request.get("retry_receipts", {})
        for row in chain(inherited.values(), rows):
            if not isinstance(row, dict):
                raise ValueError("malformed saved receipts")
            if row.get("event") == "failed":
                failures += 1
                unresolved = unresolved or uncertain_worker(row)
                incomplete_discovery = incomplete_discovery or any(
                    text in row.get("error", "")
                    for text in ("discovery failed:", "source lookup failed:")
                )
                if row.get("source"):
                    missing.add(row["source"])
            if row.get("event") not in ("verified", "deleted", "already_moved"):
                continue
            path = _path(request, row["source"])
            identity = row["fingerprint"]
            if (
                not _identity(identity)
                or type(row["bytes"]) is not int or row["bytes"] < 0
                or row["bytes"] != identity[3]
                or row["kind"] not in ("file", "symlink")
                or not re.fullmatch("[0-9a-f]{64}", row["sha256"])
            ):
                raise ValueError("receipt is not an ordinary file/link verification")
            task = {
                **request, "source": str(path), "target_base": True,
                "selection_root": (
                    request["source"] if request["directory"] else str(path.parent)
                ),
            }
            destination, decode, _ = target_path(task, identity)
            link = stat.S_ISLNK(identity[2]) or decode
            if row["destination"] != str(destination) or (
                row["kind"] == "symlink"
            ) != link:
                raise ValueError("receipt destination or kind differs from the saved request")
            previous = evidence.get(str(path))
            if previous and (
                previous["fingerprint"][1:] != identity[1:]
                or any(previous[key] != row[key] for key in (
                    "sha256", "destination", "kind",
                ))
            ):
                raise ValueError(
                    "conflicting source identities in saved receipts; "
                    "inspect them before retrying"
                )
            if not previous or previous["event"] == "verified":
                evidence[str(path)] = row
        if selected is not None and set(evidence) - {
            item["path"] for item in selected if not item["empty"]
        }:
            raise ValueError("receipt falls outside the saved finite selection")
        if selected is not None and any(
            item["path"] in evidence
            and item["fingerprint"][1:] != evidence[item["path"]]["fingerprint"][1:]
            for item in selected
        ):
            raise ValueError("receipt identity differs from the saved selection")
        stopped(run_dir, request, progress, failures, unresolved)
        source = Path(request["source"])
        for path in list(missing):
            candidate = Path(path)
            directory = (
                candidate == source and request["directory"]
            ) or (candidate.is_dir() and not candidate.is_symlink())
            _path(request, path, empty = directory)
            if directory:
                # Discovery errors name directories, whose basenames need
                # not match the file filters. Rediscover them from the root.
                incomplete_discovery = True
                missing.remove(path)
        if selected is None and not os.path.lexists(source):
            if (
                not progress.get("discovery_complete")
                or incomplete_discovery
                or len(evidence) != progress.get("discovered_files")
                or not all(
                    row["event"] in ("deleted", "already_moved")
                    for row in evidence.values()
                )
            ):
                raise ValueError(
                    "source root is absent and the original inventory is "
                    "incomplete; inspect receipts"
                )
        return {
            **request, "selection": selected, "retry_from": str(run_dir),
            "retry_receipts": evidence, "retry_missing": sorted(missing),
        }
    except (KeyError, TypeError, AttributeError) as error:
        raise ValueError("malformed saved ordinary transfer history") from error


def entries(request, site, on_error, visit):
    """Reconcile missing recorded files, then rediscover the saved scope."""
    from .format_selection import iter_units

    for path in chain(request["retry_receipts"], (
        path for path in request["retry_missing"]
        if path not in request["retry_receipts"]
    )):
        if not os.path.lexists(path):
            visit()
            record = request["retry_receipts"].get(path, {})
            yield {
                "path": path, "empty": False,
                "fingerprint": record.get("fingerprint"),
            }
    if os.path.lexists(request["source"]):
        yield from iter_units(request, site, on_error, visit)
    elif not request["retry_receipts"] and not request["retry_missing"]:
        yield {"path": request["source"], "empty": True}
