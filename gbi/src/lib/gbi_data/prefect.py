"""Managed transfers through the identity-binding local broker."""

import errno
from dataclasses import asdict
import json
import os
from pathlib import Path
import socket
import ssl
import stat
import subprocess
import sys
import time
from urllib import error as urlerror
from urllib import request as urlrequest
from urllib.parse import urlsplit
import uuid

from . import archive_plan, deadlines, jobs, moves, packing
from .storage import overlap
from .transfer import write_json

_TOKEN         = None
_TOKEN_EXPIRES = 0.0
_MAX_RESPONSE  = 65536


class _BrokerUnavailable(OSError):
    """The local socket was unavailable before a request was sent."""


class _BrokerError(ValueError):
    """A deliberate public error from the authenticated broker."""


class _NoRedirect(urlrequest.HTTPRedirectHandler):
    def redirect_request(self, request, file, code, msg, headers, newurl):
        raise ValueError("broker HTTPS endpoint redirected")


def _resolve_source(value):
    path = Path(value).expanduser().absolute()
    if not os.path.exists(path):
        raise ValueError(f"source does not exist: {path}")
    return Path(os.path.realpath(path))


def _resolve_destination(value):
    """Resolve what exists; the rest is created by the transfer."""
    path    = Path(value).expanduser().absolute()
    missing = []
    while not os.path.exists(path):
        if os.path.lexists(path):
            raise ValueError(f"destination is a broken symlink: {path}")
        if path.parent == path:
            break
        missing.append(path.name)
        path = path.parent
    if any(part in ("", ".", "..") for part in missing):
        raise ValueError("--prefect needs a plain destination path")
    return Path(os.path.realpath(path)).joinpath(*reversed(missing))


def _object_roots(site):
    values = [
        site.values["team_root"], *site.values["readonly_roots"].split(),
        *site.values["object_roots"].split(),
    ]
    return [Path(os.path.realpath(value)) for value in values if value]


def _kind(site, path):
    """Lustre, FSS or Object Storage, as the broker will map the path."""
    _, kind, _ = site.classify(path)
    if kind != "alluxio" and any(
        path == root or root in path.parents for root in _object_roots(site)
    ):
        kind = "alluxio"
    if kind not in ("lustre", "fss", "alluxio"):
        raise ValueError(
            f"--prefect supports Lustre, FSS and Object Storage paths: {path}"
        )
    return kind


def prepare(options, site):
    moves.refuse_copy_deletion(options)
    for name in ("include", "exclude"):
        patterns = getattr(options, name)
        if len(patterns) > 100 or any(
            not pattern
            or len(pattern.encode()) > 255
            or not pattern.isprintable()
            or any(character in pattern for character in "/\\{}")
            for pattern in patterns
        ):
            raise ValueError(
                f"Prefect --{name} requires quoted file-name globs (at most 100 patterns)"
            )

    # Any path you can read to any path you can write: the broker maps the
    # mounts and the transfer job checks permissions as you. The checks here
    # only refuse obvious mistakes before anything is submitted.
    source_path = _resolve_source(options.source)
    target_path = _resolve_destination(options.destination)
    for path in (source_path, target_path):
        _check_plain_path(str(path))
    source_kind = _kind(site, source_path)
    target_kind = _kind(site, target_path)
    if source_kind == target_kind == "alluxio":
        operation = "copy-objects"
    elif target_kind == "alluxio":
        operation = "archive-" + source_kind
    elif source_kind == "alluxio":
        operation = "stage-" + target_kind
    else:
        raise ValueError(
            "both paths are on filesystems; run the command without --prefect"
        )
    if overlap(source_path, target_path):
        raise ValueError("source and destination must not overlap")
    moves.check_readable(source_path)
    moves.check_writable(target_path)
    source, target = str(source_path), str(target_path)
    pack, pack_small = getattr(options, "pack", None), getattr(
        options, "pack_small", False
    )
    archive_auto = getattr(options, "archive", False)
    if operation == "copy-objects" and (
        archive_auto or pack or pack_small
        or getattr(options, "chunk_size", None) is not None
        or getattr(options, "job_size", None) is not None
    ):
        raise ValueError(
            "--archive, --pack, --pack-small, --chunk-size and --job-size do "
            "not apply to a copy between Object Storage mounts"
        )
    if archive_auto and (
        pack or pack_small or getattr(options, "chunk_size", None) is not None
    ):
        raise ValueError(
            "--archive chooses packing and part sizes; omit --pack, "
            "--pack-small and --chunk-size"
        )
    if pack not in (None, "tar", "gzip") or type(pack_small) is not bool:
        raise ValueError("packing requires --pack tar/gzip or --pack-small")
    if pack and pack_small:
        raise ValueError("choose --pack or --pack-small, not both")
    if (archive_auto or pack or pack_small) and not operation.startswith(
        "archive-"
    ):
        raise ValueError(
            "packing is supported only for Prefect archives from Lustre or FSS"
        )
    chunk_size = getattr(options, "chunk_size", None)
    if chunk_size is not None and (
        type(chunk_size) is not int
        or chunk_size < 1
        or not operation.startswith("archive-")
    ):
        raise ValueError(
            "--chunk-size requires positive integer bytes and is supported "
            "only for Prefect archives"
        )
    if pack:
        suffix = ".gbi.tar.gz" if pack == "gzip" else ".gbi.tar"
        if not target.endswith(suffix):
            raise ValueError(
                f"--pack {pack} needs an exact destination filename ending in {suffix}"
            )
    job_size = getattr(options, "job_size", None)
    if job_size is not None and (
        job_size not in ("small", "large") or not operation.startswith("stage-")
    ):
        raise ValueError(
            "--job-size small or large is supported only for --prefect "
            "restores to Lustre or FSS"
        )
    request = {
        "version": 1, "action": "submit", "request_id": str(uuid.uuid4()),
        "operation": operation, "verb": options.verb, "source": source,
        "destination": target, "include": options.include,
    }
    # Keep existing requests unchanged; older brokers must reject an unknown
    # selection field rather than silently submit a broader transfer.
    if options.exclude:
        request["exclude"] = options.exclude
    if archive_auto:
        request["archive_auto"] = True
    if job_size is not None:
        request["job_size"] = job_size
    if pack:
        request["archive_format"] = pack
    if chunk_size is not None:
        request["chunk_size"] = chunk_size
    if operation.startswith("archive-") and not (pack or pack_small):
        # A plain or automatic archive of one regular file takes the chunk
        # route the backend already supports for files: the exact store grant
        # a direct file needs, never a broader parent grant. The source is
        # already resolved, so no symlink is traversed. Directories keep their
        # request unchanged.
        info = source_path.lstat()
        mode, size = info.st_mode, info.st_size
        if stat.S_ISREG(mode):
            if options.include or options.exclude:
                raise ValueError(
                    "a single-file source takes no --include or --exclude"
                )
            if str(options.destination).endswith("/"):
                # A destination written as a directory receives the file by name.
                target = target + "/" + source.rsplit("/", 1)[-1]
                _check_plain_path(target)
            if target.endswith(".gbi-chunks"):
                raise ValueError(
                    "omit the automatic .gbi-chunks suffix from a direct file destination"
                )
            request["destination"] = target
            request["source_is_file"] = True
            request.pop("archive_auto", None)
            if chunk_size is None:
                chunk_size = archive_plan.auto_chunk_size(size)
                request["chunk_size"] = chunk_size
        elif not stat.S_ISDIR(mode):
            raise ValueError(
                "Prefect source must be a regular file or directory"
            )
    if pack_small:
        request["pack_small"] = True
        request["packing_policy"] = asdict(
            packing.PackingPolicy(
                small_file_bytes = int(site.values["pack_small_bytes"]),
                min_files = int(site.values["pack_min_files"]),
                max_archive_bytes = int(site.values["pack_max_bytes"]),
                max_entries = int(site.values["inline_scan_entries"]),
                max_seconds = float(site.values["inline_probe_seconds"]),
            )
        )
    if options.dry_run and (
        archive_auto or pack or pack_small or chunk_size is not None
    ):
        request["dry_run"] = True
    # The broker treats "move" as "remove the source after verification", so
    # a move that keeps an Object Storage source is sent as a copy.
    delete = moves.deletes(options, source_kind, target_kind, source)
    if delete:
        moves.check_removable(
            source_path, int(site.values["inline_scan_entries"]),
            float(site.values["inline_probe_seconds"]),
        )
    request["verb"] = "move" if delete else "copy"
    return request


def _check_plain_path(value):
    if (
        len(value.encode()) > 4096
        or not value.isprintable()
        or any(character in value for character in "\\*?[]{}")
    ):
        raise ValueError(
            "--prefect requires plain paths without wildcard characters"
        )
    if any(
        part == "transfer-locks"
        or part.startswith((".gbi", ".prefect", "prefect-transfer-dev"))
        for part in value.split("/")
    ):
        raise ValueError(
            "transfer state and development prefixes are reserved"
        )


def _validate_result(raw):
    if len(raw) > _MAX_RESPONSE or not raw.endswith(b"\n"):
        raise ValueError("incomplete broker response")
    result = json.loads(raw)
    if not isinstance(result, dict) or result.get("version") != 1:
        raise ValueError("unsupported broker response")
    if "error" in result:
        message = result["error"]
        if (
            not isinstance(message, str)
            or not message.isprintable()
            or len(message) > 1024
        ):
            raise ValueError("invalid broker error")
        raise _BrokerError(message)
    if result.get("state") not in {
        "NOT_SUBMITTED", "SCHEDULED", "PENDING", "RUNNING", "COMPLETED",
        "FAILED", "CANCELLED", "CANCELLING", "CRASHED", "PAUSED", "UNKNOWN",
    }:
        raise ValueError("unsupported broker state")
    if result["state"] != "NOT_SUBMITTED":
        result["run_id"] = str(uuid.UUID(result.get("run_id", "")))
    result["terminal"] = result["state"] in {
        "COMPLETED", "FAILED", "CANCELLED", "CRASHED",
    }
    message = result.get("message")
    if message is not None and (
        not isinstance(message, str)
        or not message.isprintable()
        or len(message) > 1024
    ):
        del result["message"]
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
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
    ):
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
        environment = {
            key: value
            for key, value in os.environ.items()
            if key != "SLURM_JWT"
        }
        completed = subprocess.run(
            ("scontrol", "token", "lifespan=60"), check = True,
            capture_output = True, text = True, timeout = 10, env = environment,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise ValueError(
            f"cannot obtain Slurm user token: {type(error).__name__}; contact cluster support"
        ) from None
    token = next(
        (
            line.split("=", 1)[1].strip()
            for line in completed.stdout.splitlines()
            if line.startswith("SLURM_JWT=") and line.split("=", 1)[1].strip()
        ), None,
    )
    if (
        not token
        or len(token) > 8192
        or not token.isascii()
        or not token.isprintable()
    ):
        raise ValueError("scontrol did not return a Slurm user token")
    _TOKEN, _TOKEN_EXPIRES = token, now + 45
    return token


def _exchange_https(url, request):
    body = json.dumps(request).encode() + b"\n"
    req = urlrequest.Request(
        url, data = body, method = "POST",
        headers = {
            "Authorization": f"Bearer {_token()}",
            "Content-Type": "application/json",
        },
    )
    opener = urlrequest.build_opener(
        _NoRedirect,
        urlrequest.HTTPSHandler(context = ssl.create_default_context()),
    )
    try:
        with opener.open(req, timeout = 30) as response:
            raw = response.read(_MAX_RESPONSE + 1)
    except urlerror.HTTPError as error:
        raise ValueError(
            f"broker HTTPS request failed: HTTP {error.code}"
        ) from None
    except (urlerror.URLError, OSError, ValueError) as error:
        raise ValueError(
            f"broker HTTPS request failed: {type(error).__name__}"
        ) from None
    if raw and not raw.endswith(b"\n"):
        raw += b"\n"
    try:
        return _validate_result(raw)
    except _BrokerError:
        raise
    except (ValueError, TypeError, AttributeError):
        raise ValueError("broker HTTPS response was invalid") from None


def exchange(site, request, retry_hint = "retry with the same transfer ID"):
    path    = site.values["prefect_socket"]
    service = "Cache" if request.get("operation") == "cache" else "Prefect"
    deadlines.event(f"contacting {service} broker", path)
    try:
        return _exchange_socket(path, request)
    except _BrokerUnavailable as error:
        url = _https_url(site)
        if not url:
            raise ValueError(
                f"{service} broker unavailable on this node. "
                "Run this command on the login node "
                f"or contact cluster support. No request was sent; {retry_hint}"
            ) from error
        deadlines.event(
            f"local {service} broker unavailable; using HTTPS transport", url
        )
        try:
            return _exchange_https(url, request)
        except (OSError, ValueError, TypeError, AttributeError) as https_error:
            raise ValueError(
                f"{service} request not confirmed: {https_error}. "
                f"Use status or {retry_hint}"
            ) from https_error
    except (OSError, ValueError, TypeError, AttributeError) as error:
        raise ValueError(
            f"{service} request not confirmed: {error}. Use status or {retry_hint}"
        ) from error


def stored_request(run_dir, *, execution = "prefect"):
    record = json.loads((run_dir / "request.json").read_text())
    if record.get("execution") != execution:
        if record.get("execution") == "cache":
            raise ValueError(
                "cache loads are retried by re-running their original "
                "gbi data cache command"
            )
        raise ValueError(
            "ordinary transfers are retried by re-running their original copy or move command"
        )
    if record.get("uid") != os.getuid():
        raise ValueError("this request belongs to another user")
    return record["broker_request"]


def reason(message):
    """The flow's own words, without Prefect's exception framing."""
    text = message.removeprefix("Flow run encountered an exception: ")
    name, separator, rest = text.partition(": ")
    if separator and rest and name.isidentifier() and name[:1].isupper():
        return rest
    return text


def show(result, transfer_id):
    print(
        f"{jobs.timestamp()} Prefect {result['state']} · transfer {transfer_id}"
        + (f" · run {result['run_id']}" if result.get("run_id") else ""),
        flush = True,
    )


def follow(run_dir, site, wait = False, initial = None):
    request = stored_request(run_dir)
    result  = initial
    try:
        while True:
            if result is None:
                result = exchange(
                    site,
                    {
                        "version": 1, "action": "status",
                        "request_id": request["request_id"],
                    },
                )
            show(result, run_dir.name)
            write_json(run_dir / "prefect.json", result)
            if result["state"] == "NOT_SUBMITTED":
                print(
            "No submitted run found. Retry the saved request: "
            f"gbi data retry {run_dir.name}"
                )
                return 1
            if result.get("terminal"):
                if result.get("message"):
                    print(f"Prefect: {reason(result['message'])}")
                print(
                    "This is the Prefect flow state; inspect its maintained "
                    "migration receipts for file verification."
                )
                return 0 if result["state"] == "COMPLETED" else 1
            if not wait:
                return 0
            time.sleep(2)
            result = None
    except KeyboardInterrupt:
        print(
            f"\nDetached. Migration continues. Reconnect: gbi data status {run_dir.name} --watch"
        )
        return 0


def submit(run_dir, site, wait = False):
    request = stored_request(run_dir)
    print(
        f"Transfer {run_dir.name}. Saved request: {run_dir / 'request.json'}",
        flush = True,
    )
    print(
        f"Reconnect: gbi data status {run_dir.name} --watch; "
        f"retry a lost reply: gbi data retry {run_dir.name}",
        flush = True,
    )
    result = exchange(site, request)
    return follow(run_dir, site, wait, result)


def start(options, site, home):
    request = prepare(options, site)
    print(
        f"Prefect {request['operation']}: {request['source']} → {request['destination']}"
    )
    if request.get("archive_auto"):
        print(
            "Archive directory: automatic packing and part sizes; one saved destination layout."
        )
    if "job_size" in request:
        print(f"Restore job size: {request['job_size']}")
    if "archive_format" in request:
        destination_kind = (
            "chunk store" if request.get("chunk_size") else "object"
        )
        print(
            f"Archive format: {request['archive_format']}; exact destination {destination_kind}."
        )
    if request.get("pack_small"):
        print(
            f"Small-file packing policy: {json.dumps(request['packing_policy'], sort_keys = True)}"
        )
    if request.get("source_is_file"):
        print(
            f"Single file: uploaded as one resumable chunk store at {request['destination']}.gbi-chunks/"
        )
    if "chunk_size" in request:
        print(
            f"Chunk size: {request['chunk_size']} bytes; encoded stores use the .gbi-chunks suffix."
        )
    for selection in ("include", "exclude"):
        if request.get(selection):
            print(
                f"{selection.capitalize()} basenames: {json.dumps(request[selection])}"
            )
    print(
        "Sources: delete verified originals."
        if not options.dry_run and request["verb"] == "move"
        else "Sources: keep originals."
    )
    if options.dry_run and not request.get("dry_run"):
        print("Dry run: no migration submitted and no files written.")
        return 0
    if request.get("dry_run"):
        print(
            "Managed dry run: creates Prefect/run tracking records only; no "
            "data, receipt, destination writes or source deletions."
        )
    site.check_root(site.roots["lustre"])
    run_dir = (
        home
        / "runs"
        / (
            time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
            + "-"
            + uuid.uuid4().hex[:12]
        )
    )
    deadlines.event("saving Prefect request", run_dir, records = str(run_dir))
    run_dir.mkdir(parents = True, mode = 0o700)
    write_json(
        run_dir / "request.json",
        {"execution": "prefect", "uid": os.getuid(), "broker_request": request},
    )
    return submit(run_dir, site, options.wait or sys.stdout.isatty())
