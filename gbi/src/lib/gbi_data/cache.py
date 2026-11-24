"""Load paths into the Object Storage cache with the native Alluxio load.

``gbi data cache PATH`` resolves each path on this node and hands it to the
site's identity-binding broker. The broker maps the mount to an Alluxio
cluster and namespace path and submits one native load, which every storage
worker serves in parallel, so the data lands in the cache faster than any
single reader could pull it. The command waits until the blocks are cached and
returns one line per path. Nothing is read through this node and nothing is
written. Repeating the command is cheap: resident blocks are not loaded again.
"""

import json
import os
import shlex
import sys
import time
import uuid
from pathlib import Path

from . import deadlines, jobs, prefect
from .transfer import write_json

POLL_SECONDS      = 3
HEARTBEAT_SECONDS = 60
RETRY_HINT        = "run the same command again"

EXIT_OK      = 0
EXIT_FAILED  = 1
EXIT_REFUSED = 2
EXIT_PARTIAL = 3

FAILED_STATES = {"FAILED", "CRASHED", "CANCELLED", "NOT_SUBMITTED"}


class CacheError(ValueError):
    """A refused request: nothing was submitted."""


def resolve(site, raw):
    """One named path: present, and inside an Object Storage mount.

    The path you name may itself be a symbolic link, or lie below one, as the
    ``/mnt/instrument-data`` entries do (each is a link to a sequencing
    mount): it is followed here, once, and must land inside an Object Storage
    mount. The broker checks the resolved path against the site's mount map.
    """
    named = Path(raw).expanduser().absolute()
    if not os.path.lexists(named):
        raise CacheError(f"{raw}: does not exist")
    try:
        path = named.resolve(strict = True)
    except (OSError, RuntimeError) as error:
        raise CacheError(f"{raw}: cannot be resolved: {error}") from None
    try:
        located, kind, root = site.classify(path)
    except ValueError as error:
        raise CacheError(f"{raw}: {error}") from None
    if kind != "alluxio":
        raise CacheError(
            f"{raw}: not inside an Object Storage mount; the cache applies to "
            "Alluxio-backed paths only"
        )
    return located, root


def _patterns(values):
    """``--include`` values, each possibly a comma-separated list."""
    patterns = []
    for value in values or ():
        for pattern in value.split(","):
            pattern = pattern.strip()
            if pattern:
                patterns.append(pattern)
    if len(patterns) > 100 or any(
        len(pattern.encode()) > 255
        or not pattern.isprintable()
        or any(character in pattern for character in "/\\{}")
        for pattern in patterns
    ):
        raise CacheError(
            "--include takes quoted file-name globs without '/' (at most 100)"
        )
    return patterns


def request(site, raw, include):
    """The broker request for one named path."""
    located, _root = resolve(site, raw)
    return located, {
        "version": 1, "action": "submit", "request_id": str(uuid.uuid4()),
        "operation": "cache", "path": str(located), "include": list(include),
    }


def record(site, body):
    """Save the request so ``gbi data status ID --watch`` can reconnect."""
    root = site.roots.get("lustre")
    if root is None:
        return None
    try:
        site.check_root(root)
    except ValueError:
        return None
    run_dir = root / ".gbi" / "runs" / (
        time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:12]
    )
    run_dir.mkdir(parents = True, mode = 0o700)
    write_json(
        run_dir / "request.json",
        {"execution": "prefect", "uid": os.getuid(), "broker_request": body},
    )
    return run_dir


def show(run, result):
    label = run["run_dir"].name if run["run_dir"] else run["path"]
    legacy = run["execution"] == "prefect"
    engine = "Prefect" if legacy else "Cache"
    label  = "cache " + label if legacy else label
    print(
        f"{jobs.timestamp()} {engine} {result['state']} · {label}"
        + (f" · run {result['run_id']}" if result.get("run_id") else ""),
        flush = True,
    )


def follow(
            site, runs, *, wait = True, timeout = 0, clock = None, sleep = None,
            as_json = False,
        ):
    """Poll every submission until it ends; return the exit code.

    Each run carries the path, selection, execution route, request ID, local
    record directory and last result (None when it still has to be fetched).
    """
    clock = clock if clock is not None else time.monotonic
    sleep = sleep if sleep is not None else time.sleep
    started   = clock()
    shown     = {}
    heartbeat = started
    timed_out = False
    try:
        while True:
            now = clock()
            for run in runs:
                if run.get("done") or run.get("error"):
                    continue
                result = run.get("result")
                if result is None:
                    if timeout and clock() >= started + timeout:
                        timed_out = True
                        break
                    query = {"version": 1, "action": "status"}
                    if run["execution"] == "cache":
                        query.update(
                            operation = "cache", path = run["path"],
                            include = run["include"],
                        )
                    else:
                        query = prefect.status_query(
                            run["request_id"], run.get("last_result")
                        )
                    try:
                        result = prefect.status(
                            site, query, retry_hint = RETRY_HINT,
                            sleep = sleep, clock = clock,
                            until = started + timeout if timeout else None,
                        )
                    except ValueError as error:
                        run["error"] = str(error)
                        run["monitoring_lost"] = isinstance(error, prefect.TransientError)
                        last = run.get("last_result")
                        observed = f" Last observed state: {last['state']}." if last else ""
                        print(
                            f"gbi data cache: {run['path']}: monitoring stopped; "
                            f"current load state is unknown.{observed} {error}."
                            + _reconnect([run]), file = sys.stderr,
                        )
                        continue
                    run["result"] = result
                run["last_result"] = result
                if run["run_dir"] is not None:
                    write_json(
                        run["run_dir"] / (run["execution"] + ".json"), result
                    )
                if shown.get(run["request_id"]) != result["state"] and not as_json:
                    show(run, result)
                    shown[run["request_id"]] = result["state"]
                if result.get("terminal") or result["state"] == "NOT_SUBMITTED":
                    run["done"] = True
                    if not as_json:
                        if result.get("message"):
                            print(f"  {result['message']}", flush = True)
                        if result["state"] == "NOT_SUBMITTED":
                            print(f"  no submitted load found; {RETRY_HINT}", flush = True)
            if timed_out or all(
                run.get("done") or run.get("error") for run in runs
            ) or not wait:
                break
            now = clock()
            deadlines.event(
                "waiting for the cache load", runs[0]["path"],
                counters = {"seconds": int(now - started)},
            )
            if timeout and now - started >= timeout:
                timed_out = True
                break
            if now - heartbeat >= HEARTBEAT_SECONDS and not as_json:
                heartbeat = now
                active = sum(
                    1 for run in runs if not run.get("done") and not run.get("error")
                )
                print(
                    f"{jobs.timestamp()} still loading: {active} path(s), "
                    f"{int(now - started)} s elapsed",
                    flush = True,
                )
            for run in runs:
                if not run.get("done") and not run.get("error"):
                    run["result"] = None
            delay = min(POLL_SECONDS, started + timeout - clock()) if timeout else POLL_SECONDS
            sleep(max(0, delay))
    except KeyboardInterrupt:
        print("\nDetached. The load continues on the storage workers." + _reconnect(runs))
        return EXIT_OK
    if as_json:
        print(json.dumps(_summary(runs, timed_out), sort_keys = True), flush = True)
    elif timed_out:
        print(
            f"Stopped waiting at --timeout; the load continues on the storage workers."
            + _reconnect(runs)
        )
    if timed_out:
        return EXIT_PARTIAL
    if any(
        (run.get("error") and not run.get("monitoring_lost"))
        or (run.get("done") and run["last_result"]["state"] in FAILED_STATES)
        for run in runs
    ):
        return EXIT_FAILED
    if any(run.get("monitoring_lost") for run in runs):
        return EXIT_PARTIAL
    return EXIT_OK


def _reconnect(runs):
    ids = [
        run["run_dir"].name if run["run_dir"] else shlex.quote(run["path"])
        for run in runs if not run.get("done")
    ]
    if not ids:
        return ""
    return " Reconnect: " + "; ".join(f"gbi data status {name} --watch" for name in ids)


def _summary(runs, timed_out):
    return {
        "timed_out": timed_out,
        "paths": [
            {
                "path": run["path"],
                "request_id": run["request_id"],
                "transfer_id": run["run_dir"].name if run["run_dir"] else None,
                "run_id": (run.get("last_result") or {}).get("run_id"),
                "state": (run.get("last_result") or {}).get("state"),
                "message": (run.get("last_result") or {}).get("message"),
                "error": run.get("error"),
            }
            for run in runs
        ],
    }


def status(run_dir, site, watch, execution = "cache"):
    """``gbi data status ID [--watch]`` for a saved cache request."""
    body = prefect.stored_request(run_dir, execution = execution)
    runs = [
        {
            "path": body["path"], "request_id": body["request_id"],
            "run_dir": run_dir, "result": None,
            "execution": execution, "include": body.get("include", []),
            "last_result": prefect.saved_result(run_dir, execution),
        }
    ]
    return follow(site, runs, wait = watch)


def status_path(raw, site, watch):
    """Inspect current cache state without a saved request or a new load."""
    located, _root = resolve(site, raw)
    return follow(
        site,
        [{
            "path": str(located), "request_id": str(uuid.uuid4()),
            "run_dir": None, "result": None, "execution": "cache", "include": [],
        }],
        wait = watch,
    )


def main(options, site):
    try:
        include = _patterns(getattr(options, "include", None))
        targets = [request(site, raw, include) for raw in options.paths]
        if type(options.timeout) is not int or options.timeout < 0:
            raise CacheError("--timeout must be a non-negative number of seconds")
    except CacheError as error:
        print(f"gbi data cache: {error}", file = sys.stderr)
        return EXIT_REFUSED
    selection = f" (files matching {', '.join(include)})" if include else ""
    if options.dry_run:
        for located, _body in targets:
            print(f"Would load {located}{selection} into the cache.")
        print("Dry run: nothing submitted.")
        return EXIT_OK
    runs = []
    for located, body in targets:
        print(f"Loading {located}{selection} into the cache.", flush = True)
        run_dir = record(site, body)
        try:
            result = prefect.exchange(site, body, retry_hint = RETRY_HINT)
        except ValueError as error:
            print(f"gbi data cache: {located}: {error}", file = sys.stderr)
            return EXIT_REFUSED if not runs else EXIT_FAILED
        execution = "cache" if result.get("execution") == "cache" else "prefect"
        if run_dir is not None:
            if execution == "cache":
                write_json(run_dir / "request.json", {
                    "execution": execution, "uid": os.getuid(),
                    "broker_request": body,
                })
            print(
                f"Transfer {run_dir.name}. Reconnect: gbi data status {run_dir.name} --watch",
                flush = True,
            )
        runs.append(
            {
                "path": str(located), "request_id": body["request_id"],
                "run_dir": run_dir, "result": result,
                "execution": execution, "include": include,
            }
        )
    return follow(
        site, runs, wait = not options.detach,
        timeout = options.timeout, as_json = options.json,
    )
