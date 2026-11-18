"""`gbi dataset get|status`: whole Tier 0 Bento artefact versions on /mnt/shared/bento.

A located address names one published version in one Bento bucket:

    bento://us-ashburn-1/psto-iad-drplab-allthebacteria-bucket@lr40ytmfucrr/allthebacteria/allthebacteria_base@2026-08-28a

If GBI already holds that version, `get` prints its path. Otherwise the site
broker starts (or reports) the one Prefect copy of it, and `get` prints the
run ID and the path where it will appear. Anyone may ask; the copy is shared.
"""

import json
import re
import sys
import urllib.parse
import urllib.request
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


def register_entry(site, address):
    """BILBO's record for the address, when the site configures its read route.

    Optional and advisory: the mount's ready record decides. Any failure here
    falls back to the mount and the broker.
    """
    url = site.values.get("dataset_register_url", "").strip()
    if not url.startswith("https://"):
        return None
    query = url + ("&" if "?" in url else "?") + urllib.parse.urlencode({"source_address": address})
    try:
        with urllib.request.urlopen(query, timeout = 10) as response:
            body = json.loads(response.read(1024 * 1024))
    except (OSError, ValueError):
        return None
    return body if isinstance(body, dict) else None


def request(action, address):
    return {"version": 1, "action": action, "operation": "bento-ingest", "address": address}


def report(result, address, path, as_json):
    if as_json:
        print(json.dumps(result, sort_keys = True))
        return
    state = result["state"]
    if state == "READY":
        print(path)
    elif state == "NOT_SUBMITTED":
        print(f"Not held and no copy requested. Run: gbi dataset get {address}")
    elif state == "COMPLETED":
        print(f"Copy finished (run {result['run_id']}). It appears at {path} within a few minutes.")
    elif state in ACTIVE:
        print(f"Copying (Prefect run {result['run_id']}, {state.lower()}).\n"
              f"Files will appear at {path}\n"
              f"Check with: gbi dataset status {address}")
    else:
        message = result.get("message")
        print(f"The last copy ended {state.lower()} (run {result.get('run_id')})"
              + (f": {message}" if message else "")
              + f". Run gbi dataset get {address} to resume it.", file = sys.stderr)


def lookup(options, site):
    address, parts = parse(options.address)
    path = mount_path(site, parts)
    entry = register_entry(site, address)
    if (entry and entry.get("status") == "ready") or ready_record(path):
        return address, path, {"state": "READY", "path": str(path), "address": address}
    return address, path, None


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
            result = {**result, "path": str(path), "address": address}
    except ValueError as error:
        print(f"gbi dataset: {error}", file = sys.stderr)
        return 2
    report(result, address, path, options.json)
    state = result["state"]
    if state == "READY":
        return READY
    if state in ACTIVE or state in ("COMPLETED", "NOT_SUBMITTED"):
        return COPYING
    return 1
