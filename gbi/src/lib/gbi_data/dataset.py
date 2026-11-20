"""`gbi dataset get|status`: whole Tier 0 Bento artefact versions on /mnt/shared/bento.

A located address names one published version in one Bento bucket:

    bento://us-ashburn-1/psto-iad-drplab-allthebacteria-bucket@lr40ytmfucrr/allthebacteria/allthebacteria_base@2026-08-28a

If GBI already holds that version, `get` prints its path. Otherwise the site
broker starts (or reports) the one Prefect copy of it, and `get` prints the
run ID and the path where it will appear. Anyone may ask; the copy is shared.

Status comes from BILBO's register of the version, read by the broker with its
own server-held reader credential: owning run, copied files and bytes against
the manifest totals, and whether the cluster path has been verified. The path
is printed as usable only when BILBO says ready and mount_available, or when
this node can read the copy's own ready record under the mount.
"""

import json
import re
import sys
from pathlib import Path

from . import prefect

ADDRESS = re.compile(
    r"bento://(?P<region>[a-z]+-[a-z]+-[0-9]+)"
    r"/(?P<bucket>[A-Za-z0-9][A-Za-z0-9._-]{0,254})@(?P<namespace>[a-z0-9]{1,64})"
    r"/(?P<dataset>[a-z0-9][a-z0-9_-]{0,127})"
    r"/(?P<artefact>[a-z0-9][a-z0-9_-]{0,127})@(?P<version>[0-9]{4}-[0-9]{2}-[0-9]{2}[a-z]{0,2})"
)
READY, COPYING = 0, 3
ACTIVE = {"SCHEDULED", "PENDING", "RUNNING", "PAUSED", "CANCELLING"}
RETRY_HINT = "run the same gbi dataset get again"


class DatasetError(ValueError):
    pass


def parse(text):
    match = ADDRESS.fullmatch(text.strip().rstrip("/"))
    if not match:
        raise DatasetError(
            "expected a located Bento address, "
            "bento://<region>/<bucket>@<namespace>/<dataset>/<artefact>@<version> "
            "(copy it from the Bento GUI's version page)")
    return match.group(0), match.groupdict()


def mount_path(site, parts):
    return Path(site.values["bento_root"]) / parts["dataset"] / parts["artefact"] / parts["version"]


def ready_record(path):
    """The copy's own completion record, if this node can see it."""
    try:
        return json.loads((path / "_bento" / "ready.json").read_text())
    except (OSError, ValueError):
        return None


def request(action, address):
    return {"version": 1, "action": action, "operation": "bento-ingest", "address": address}


def scaled(done, total, units, unit_on_both):
    """'7.6 M of 23.2 M' or '19.7 of 61.1 TB', in decimal units chosen by the total."""
    scale, name = next(((scale, name) for scale, name in units if total >= scale), units[-1])

    def figure(value):
        return f"{value:,}" if scale == 1 else f"{value / scale:.1f}"

    suffix = f" {name}" if name else ""
    return f"{figure(done)}{suffix if unit_on_both else ''} of {figure(total)}{suffix}"


def progress(bilbo):
    """'32% done (7.6 M of 23.2 M files, 19.7 of 61.1 TB)', as on the BILBO page."""
    files, total_files = bilbo.get("files_copied") or 0, bilbo.get("file_count") or 0
    done, total = bilbo.get("bytes_copied") or 0, bilbo.get("size_bytes") or 0
    percent = 100 * done // total if total else 100
    return (f"{percent}% done ("
            f"{scaled(files, total_files, ((10**6, 'M'), (10**3, 'k'), (1, '')), True)} files, "
            f"{scaled(done, total, ((10**12, 'TB'), (10**9, 'GB'), (10**6, 'MB'), (1, 'B')), False)})")


def bilbo_state(bilbo):
    """The CLI state for BILBO's row: READY only when the path is verified."""
    status = bilbo.get("status")
    if status == "ready":
        return "READY" if bilbo.get("mount_available") is True else "VERIFYING_PATH"
    return {"requested": "COPYING", "copying": "COPYING", "failed": "FAILED"}.get(status, "UNKNOWN")


def failure_hint(error, address):
    """What the user should do about a failed copy, from the producer's error class."""
    if error.startswith(("[transient]", "[crashed]")):
        return ("It is retried automatically; check again later with: "
                f"gbi dataset status {address}")
    if error.startswith("[cancelled]"):
        return f"Start it again with: gbi dataset get {address}"
    return ("Running get again would fail the same way; "
            "contact Scientific Computing support with the run ID.")


def report(result, address, path, as_json):
    if as_json:
        print(json.dumps(result, sort_keys = True))
        return
    state = result["state"]
    bilbo = result.get("bilbo") or {}
    run = bilbo.get("flow_run_id") or result.get("run_id")
    requested = f", requested by {bilbo['requested_by']}" if bilbo.get("requested_by") else ""
    if state == "READY":
        print(result.get("path") or path)
    elif state == "VERIFYING_PATH":
        print(f"Copied and verified (Prefect run {run}{requested}); "
              f"the cluster path {path} is still being verified.\n"
              f"Check with: gbi dataset status {address}")
    elif state == "COPYING":
        stopped = result.get("prefect_state") in ("FAILED", "CRASHED", "CANCELLED")
        print(f"Copying, {progress(bilbo)}. Prefect run {run}{requested}.\n"
              + (f"Its Prefect run ended {result['prefect_state'].lower()} without reporting; "
                 "Scientific Computing will reconcile it.\n" if stopped else "")
              + f"Files will appear at {path}\nCheck with: gbi dataset status {address}")
    elif state == "FAILED" and bilbo:
        error = bilbo.get("last_error") or "no error recorded"
        print(f"The copy failed (Prefect run {run}): {error}\n{failure_hint(error, address)}",
              file = sys.stderr)
    elif state == "NOT_SUBMITTED":
        print(f"Not held and no copy requested. Run: gbi dataset get {address}")
    elif state == "COMPLETED":
        print(f"Copy finished (run {result['run_id']}). It appears at {path} within a few minutes.")
    elif state in ACTIVE:
        print(f"Copying (Prefect run {result['run_id']}, {state.lower()}).\n"
              f"Files will appear at {path}\n"
              f"Check with: gbi dataset status {address}")
    else:
        message = result.get("message") or ""
        permanent = "ManifestError" in message or "[permanent]" in message
        print(f"The last copy ended {state.lower()} (run {result.get('run_id')})"
              + (f": {message}" if message else "") + ". "
              + (failure_hint("[permanent]", address) if permanent
                 else f"Run gbi dataset get {address} to resume it."), file = sys.stderr)


def lookup(options, site):
    address, parts = parse(options.address)
    path = mount_path(site, parts)
    if ready_record(path):
        return address, path, {"state": "READY", "path": str(path), "address": address}
    return address, path, None


def with_bilbo(result, path):
    """Let BILBO's row, when the broker supplies one, decide the state."""
    bilbo = result.get("bilbo")
    if not isinstance(bilbo, dict) or not bilbo.get("status"):
        return result
    state = bilbo_state(bilbo)
    result = {**result, "prefect_state": result.get("state"), "state": state}
    if state == "READY":
        result["path"] = str(bilbo.get("mount_path") or path).rstrip("/")
    return result


def main(options, site):
    try:
        address, path, held = lookup(options, site)
        if held:
            result = held
        else:
            action = "submit" if options.dataset_verb == "get" else "status"
            result = prefect.exchange(site, request(action, address), retry_hint = RETRY_HINT)
            if not isinstance(result, dict) or "error" in result:
                raise DatasetError((result or {}).get("error", "unexpected broker response"))
            result = with_bilbo({**result, "path": str(path), "address": address}, path)
    except ValueError as error:
        print(f"gbi dataset: {error}", file = sys.stderr)
        return 2
    report(result, address, path, options.json)
    state = result["state"]
    if state == "READY":
        return READY
    if state in ACTIVE or state in ("COMPLETED", "NOT_SUBMITTED", "COPYING", "VERIFYING_PATH"):
        return COPYING
    return 1
