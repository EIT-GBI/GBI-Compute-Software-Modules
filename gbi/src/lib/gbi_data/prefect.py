"""Explicit personal migrations through the identity-binding local broker."""

import json
import os
from pathlib import Path
import socket
import sys
import time
import uuid

from . import deadlines, jobs
from .transfer import write_json


def prepare(options, site):
    if options.exclude or any(getattr(options, key, None) for key in ("pack", "pack_small", "chunk_size")):
        raise ValueError("--prefect does not support exclusions, packing or chunks; use an ordinary transfer")
    if options.delete_source:
        raise ValueError("--prefect retains Object Storage originals; --delete-source is not supported")
    if len(options.include) > 100 or any(
            not pattern or len(pattern.encode()) > 255 or not pattern.isprintable()
            or any(character in pattern for character in "/\\{}") for pattern in options.include):
        raise ValueError("Prefect include patterns must be quoted file-name globs (at most 100 patterns)")

    def personal(path):
        path = Path(path).expanduser().absolute()
        for name in ("lustre", "fss", "alluxio"):
            for root in (site.root_aliases.get(name), site.roots.get(name)):
                if root is not None and root in path.parents:
                    relative = path.relative_to(root).as_posix()
                    if any(part in ("", ".", "..") or part != part.strip() for part in relative.split("/")):
                        raise ValueError("--prefect needs a plain path inside your personal storage root")
                    if site.is_reserved(path, root):
                        raise ValueError("transfer state and development prefixes are reserved")
                    return name, relative
        raise ValueError("--prefect supports paths inside your personal Lustre, FSS and Object Storage roots only")

    source_kind, source = personal(options.source)
    target_kind, target = personal(options.destination)
    if target_kind == "alluxio" and source_kind in ("lustre", "fss"):
        operation = "archive-" + source_kind
    elif source_kind == "alluxio" and target_kind in ("lustre", "fss"):
        operation = "stage-" + target_kind
    else:
        raise ValueError("--prefect needs one personal Object Storage path and one Lustre or FSS path")
    if operation != "archive-lustre" and source != target:
        raise ValueError("Prefect FSS archives and restores require matching relative source and destination paths")
    for value in (source, target):
        if len(value.encode()) > 4096 or not value.isprintable() or any(character in value for character in "\\*?[]{}"):
            raise ValueError("--prefect requires plain paths without wildcard characters")
        if any(part.startswith((".gbi", ".prefect", "prefect-transfer-dev")) for part in value.split("/")):
            raise ValueError("transfer state and development prefixes are reserved")
    return {"version": 1, "action": "submit", "request_id": str(uuid.uuid4()),
            "operation": operation, "verb": options.verb, "source": source,
            "destination": target, "include": options.include}


def exchange(site, request):
    path = site.values["prefect_socket"]
    deadlines.event("contacting Prefect migration broker", path)
    try:
        with socket.socket(socket.AF_UNIX) as client:
            client.settimeout(30)
            client.connect(path)
            client.sendall(json.dumps(request).encode() + b"\n")
            with client.makefile("rb") as stream:
                raw = stream.readline(65537)
        if len(raw) > 65536 or not raw.endswith(b"\n"):
            raise ValueError("incomplete broker response")
        result = json.loads(raw)
        if not isinstance(result, dict) or result.get("version") != 1:
            raise ValueError("unsupported broker response")
        if "error" in result:
            raise ValueError(str(result["error"]))
        if result.get("state") not in {"NOT_SUBMITTED", "SCHEDULED", "PENDING", "RUNNING", "COMPLETED",
                                       "FAILED", "CANCELLED", "CANCELLING", "CRASHED", "PAUSED", "UNKNOWN"}:
            raise ValueError("unsupported Prefect state")
        if result["state"] != "NOT_SUBMITTED":
            result["run_id"] = str(uuid.UUID(result.get("run_id", "")))
        result["terminal"] = result["state"] in {"COMPLETED", "FAILED", "CANCELLED", "CRASHED"}
        return result
    except (OSError, ValueError, TypeError, AttributeError) as error:
        raise ValueError(f"Prefect request not confirmed: {error}. Use status or retry with the same transfer ID") from error


def stored_request(run_dir):
    record = json.loads((run_dir / "request.json").read_text())
    if record.get("execution") != "prefect":
        raise ValueError("ordinary transfers are retried by re-running their original copy or move command")
    if record.get("uid") != os.getuid():
        raise ValueError("this request belongs to another user")
    return record["broker_request"]


def show(result, transfer_id):
    print(f"{jobs.timestamp()} Prefect {result['state']} · transfer {transfer_id}"
          + (f" · run {result['run_id']}" if result.get("run_id") else ""), flush=True)


def follow(run_dir, site, wait=False, initial=None):
    request = stored_request(run_dir)
    result = initial
    try:
        while True:
            if result is None:
                result = exchange(site, {"version": 1, "action": "status", "request_id": request["request_id"]})
            show(result, run_dir.name)
            write_json(run_dir / "prefect.json", result)
            if result["state"] == "NOT_SUBMITTED":
                print(f"No submitted run found. Retry the saved request: gbi data retry {run_dir.name}")
                return 1
            if result.get("terminal"):
                print("This is the Prefect flow state; inspect its maintained migration receipts for file verification.")
                return 0 if result["state"] == "COMPLETED" else 1
            if not wait:
                return 0
            time.sleep(2)
            result = None
    except KeyboardInterrupt:
        print(f"\nDetached. Migration continues. Reconnect: gbi data status {run_dir.name} --watch")
        return 0


def submit(run_dir, site, wait=False):
    request = stored_request(run_dir)
    print(f"Transfer {run_dir.name}. Saved request: {run_dir / 'request.json'}", flush=True)
    print(f"Reconnect: gbi data status {run_dir.name} --watch; retry a lost reply: gbi data retry {run_dir.name}", flush=True)
    result = exchange(site, request)
    return follow(run_dir, site, wait, result)


def start(options, site, home):
    request = prepare(options, site)
    print(f"Prefect {request['operation']}: {request['source']} → {request['destination']}")
    print("Sources: delete verified filesystem originals." if options.verb == "move" and request["operation"].startswith("archive-")
          else "Sources: keep originals.")
    if options.dry_run:
        print("Dry run: no migration submitted and no files written.")
        return 0
    site.check_root(site.roots["lustre"])
    run_dir = home / "runs" / (time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:12])
    deadlines.event("saving Prefect request", run_dir, records=str(run_dir))
    run_dir.mkdir(parents=True, mode=0o700)
    write_json(run_dir / "request.json", {"execution": "prefect", "uid": os.getuid(), "broker_request": request})
    return submit(run_dir, site, options.wait or sys.stdout.isatty())
