"""Explicit personal migrations through the identity-binding local broker."""

import errno
import json
import os
from pathlib import Path
import socket
import ssl
import subprocess
import sys
import time
from urllib import error as urlerror
from urllib import request as urlrequest
from urllib.parse import urlsplit
import uuid

from . import deadlines, jobs
from .transfer import write_json


_TOKEN = None
_TOKEN_EXPIRES = 0.0
_MAX_RESPONSE = 65536


class _BrokerUnavailable(OSError):
    """The local socket was unavailable before a request was sent."""


class _BrokerError(ValueError):
    """A deliberate public error from the authenticated broker."""


class _NoRedirect(urlrequest.HTTPRedirectHandler):
    def redirect_request(self, request, file, code, msg, headers, newurl):
        raise ValueError("Prefect HTTPS endpoint redirected")


def prepare(options, site):
    unsupported = []
    unsupported.extend(flag for flag, key in (("--pack", "pack"), ("--pack-small", "pack_small"),
                                               ("--chunk-size", "chunk_size"))
                      if getattr(options, key, None))
    if unsupported:
        flags = ", ".join(unsupported)
        raise ValueError(f"--prefect cannot be combined with {flags}; omit --prefect for an ordinary transfer")
    if options.delete_source:
        raise ValueError("--prefect cannot be combined with --delete-source; use an ordinary move to request source deletion")
    for name in ("include", "exclude"):
        patterns = getattr(options, name)
        if len(patterns) > 100 or any(
                not pattern or len(pattern.encode()) > 255 or not pattern.isprintable()
                or any(character in pattern for character in "/\\{}") for pattern in patterns):
            raise ValueError(f"Prefect --{name} requires quoted file-name globs (at most 100 patterns)")

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
        raise ValueError("--prefect needs one personal Object Storage path and one personal Lustre or FSS path")
    if operation != "archive-lustre" and source != target:
        raise ValueError("--prefect FSS archives and all restores require matching relative source and destination paths; omit --prefect to rename")
    for value in (source, target):
        if len(value.encode()) > 4096 or not value.isprintable() or any(character in value for character in "\\*?[]{}"):
            raise ValueError("--prefect requires plain paths without wildcard characters")
        if any(part.startswith((".gbi", ".prefect", "prefect-transfer-dev")) for part in value.split("/")):
            raise ValueError("transfer state and development prefixes are reserved")
    request = {"version": 1, "action": "submit", "request_id": str(uuid.uuid4()),
               "operation": operation, "verb": options.verb, "source": source,
               "destination": target, "include": options.include}
    # Keep existing requests unchanged; older brokers must reject an unknown
    # selection field rather than silently submit a broader transfer.
    if options.exclude:
        request["exclude"] = options.exclude
    return request


def _validate_result(raw):
    if len(raw) > _MAX_RESPONSE or not raw.endswith(b"\n"):
        raise ValueError("incomplete broker response")
    result = json.loads(raw)
    if not isinstance(result, dict) or result.get("version") != 1:
        raise ValueError("unsupported broker response")
    if "error" in result:
        message = result["error"]
        if not isinstance(message, str) or not message.isprintable() or len(message) > 1024:
            raise ValueError("invalid broker error")
        raise _BrokerError(message)
    if result.get("state") not in {"NOT_SUBMITTED", "SCHEDULED", "PENDING", "RUNNING", "COMPLETED",
                                   "FAILED", "CANCELLED", "CANCELLING", "CRASHED", "PAUSED", "UNKNOWN"}:
        raise ValueError("unsupported Prefect state")
    if result["state"] != "NOT_SUBMITTED":
        result["run_id"] = str(uuid.UUID(result.get("run_id", "")))
    result["terminal"] = result["state"] in {"COMPLETED", "FAILED", "CANCELLED", "CRASHED"}
    return result


def _exchange_socket(path, request):
    with socket.socket(socket.AF_UNIX) as client:
        client.settimeout(30)
        try:
            client.connect(path)
        except OSError as error:
            if error.errno not in (errno.ENOENT, errno.ECONNREFUSED):
                raise
            raise _BrokerUnavailable(str(error)) from error
        client.sendall(json.dumps(request).encode() + b"\n")
        with client.makefile("rb") as stream:
            raw = stream.readline(65537)
    return _validate_result(raw)


def _https_url(site):
    value = site.values.get("prefect_url", "").strip()
    if not value:
        return None
    parsed = urlsplit(value)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("prefect_url must be an HTTPS URL without credentials")
    if parsed.query or parsed.fragment:
        raise ValueError("prefect_url must not contain a query or fragment")
    return value


def _token():
    global _TOKEN, _TOKEN_EXPIRES
    now = time.monotonic()
    if _TOKEN and now < _TOKEN_EXPIRES:
        return _TOKEN
    try:
        # Use the process's native Slurm identity, not a token inherited from
        # another tool or an earlier job. Never write the returned token to disk.
        environment = {key: value for key, value in os.environ.items() if key != "SLURM_JWT"}
        completed = subprocess.run(("scontrol", "token", "lifespan=60"), check=True,
                                   capture_output=True, text=True, timeout=10, env=environment)
    except (OSError, subprocess.SubprocessError) as error:
        raise ValueError(f"cannot obtain Slurm user token: {type(error).__name__}; contact cluster support") from None
    token = next((line.split("=", 1)[1].strip() for line in completed.stdout.splitlines()
                  if line.startswith("SLURM_JWT=") and line.split("=", 1)[1].strip()), None)
    if not token or len(token) > 8192 or not token.isascii() or not token.isprintable():
        raise ValueError("scontrol did not return a Slurm user token")
    _TOKEN, _TOKEN_EXPIRES = token, now + 45
    return token


def _exchange_https(url, request):
    body = json.dumps(request).encode() + b"\n"
    req = urlrequest.Request(url, data=body, method="POST", headers={
        "Authorization": f"Bearer {_token()}", "Content-Type": "application/json"})
    opener = urlrequest.build_opener(_NoRedirect, urlrequest.HTTPSHandler(context=ssl.create_default_context()))
    try:
        with opener.open(req, timeout=30) as response:
            raw = response.read(_MAX_RESPONSE + 1)
    except urlerror.HTTPError as error:
        raise ValueError(f"Prefect HTTPS request failed: HTTP {error.code}") from None
    except (urlerror.URLError, OSError, ValueError) as error:
        raise ValueError(f"Prefect HTTPS request failed: {type(error).__name__}") from None
    if raw and not raw.endswith(b"\n"):
        raw += b"\n"
    try:
        return _validate_result(raw)
    except _BrokerError:
        raise
    except (ValueError, TypeError, AttributeError):
        raise ValueError("Prefect HTTPS response was invalid") from None


def exchange(site, request):
    path = site.values["prefect_socket"]
    deadlines.event("contacting Prefect migration broker", path)
    try:
        return _exchange_socket(path, request)
    except _BrokerUnavailable as error:
        url = _https_url(site)
        if not url:
            raise ValueError("Prefect broker unavailable on this node. Run this command on the login node "
                             "or contact cluster support. No request was sent; retry with the same transfer ID") from error
        deadlines.event("local Prefect broker unavailable; using HTTPS transport", url)
        try:
            return _exchange_https(url, request)
        except (OSError, ValueError, TypeError, AttributeError) as https_error:
            raise ValueError(f"Prefect request not confirmed: {https_error}. Use status or retry with the same transfer ID") from https_error
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
    for selection in ("include", "exclude"):
        if request.get(selection):
            print(f"{selection.capitalize()} basenames: {json.dumps(request[selection])}")
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
