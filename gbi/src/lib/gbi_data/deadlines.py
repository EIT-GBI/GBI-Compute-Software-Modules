"""Keep mount waits out of the CLI supervisor; report uncertainty honestly."""

import json
import os
import selectors
import signal
import stat
import subprocess
import sys
import threading
import time


BOOTSTRAP_SECONDS = 120
EVENT_FD = "GBI_DATA_EVENT_FD"
SUPERVISOR_PID = "GBI_DATA_SUPERVISOR_PID"
_WRITE_LOCK = threading.Lock()


def supervised():
    """An exported Slurm environment is not an inherited supervision pipe."""
    try:
        return (int(os.environ[SUPERVISOR_PID]) == os.getppid()
                and stat.S_ISFIFO(os.fstat(int(os.environ[EVENT_FD])).st_mode))
    except (KeyError, ValueError, OSError):
        return False


def event(phase=None, path=None, **details):
    descriptor = os.environ.get(EVENT_FD)
    if descriptor is None or not supervised():
        return
    message = {**details}
    if phase is not None:
        message["phase"] = phase
    if path is not None:
        message["path"] = str(path)
    try:
        # Coordinator and supervision threads share this pipe. Serialize even
        # large path/counter events so writes exceeding PIPE_BUF cannot interleave.
        payload = (json.dumps(message) + "\n").encode()
        with _WRITE_LOCK:
            while payload:
                payload = payload[os.write(int(descriptor), payload):]
    except (OSError, ValueError):
        pass


def configure(values):
    event("resolving storage roots", ", ".join(values[key] for key in
          ("lustre_root", "fss_root", "bucket_root") if values[key]),
          timeouts={key: float(values[key]) for key in
                    ("mount_timeout", "discovery_timeout", "history_timeout")})


def _signal_group(pid, signum):
    try:
        os.killpg(pid, signum)
    except ProcessLookupError:
        pass


def watch(command, environment=None, startup_timeout=BOOTSTRAP_SECONDS):
    """Supervise one CLI process using only a pipe, clock and process handles."""
    reader, writer = os.pipe()
    environment = {**(os.environ if environment is None else environment), EVENT_FD: str(writer),
                   SUPERVISOR_PID: str(os.getpid())}
    process = subprocess.Popen(command, env=environment, pass_fds=(writer,), start_new_session=True)
    os.close(writer)
    os.set_blocking(reader, False)
    selector = selectors.DefaultSelector()
    selector.register(reader, selectors.EVENT_READ)
    phase, path = "startup", environment.get("GBI_DATA_SITE_CONF", "site configuration")
    timeouts = {"mount_timeout": startup_timeout, "history_timeout": startup_timeout}
    deadline = time.monotonic() + startup_timeout
    workers, unresolved = set(), set()
    counters, records = {}, None
    buffer = b""
    previous = {}

    def forward(signum, _frame):
        # The existing child handlers distinguish stopping data work from
        # detaching a Slurm viewer. Do not kill workers on a viewer's Ctrl-C.
        _signal_group(process.pid, signum)

    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        previous[signum] = signal.signal(signum, forward)
    try:
        while process.poll() is None:
            for _key, _mask in selector.select(timeout=0.1):
                chunk = os.read(reader, 65536)
                if not chunk:
                    selector.unregister(reader)
                    continue
                buffer += chunk
                if len(buffer) > 1024 * 1024:
                    buffer = b""
                    continue
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    try:
                        message = json.loads(line)
                    except (ValueError, UnicodeError):
                        continue
                    if not isinstance(message, dict):
                        continue
                    limits = message.get("timeouts", {})
                    if not isinstance(limits, dict) or any(
                            not isinstance(value, (int, float)) or not 0 < value < float("inf")
                            for value in limits.values()):
                        continue
                    if any(not isinstance(message[key], int) or message[key] <= 0
                           for key in ("worker_started", "worker_finished", "unresolved") if key in message):
                        continue
                    if "phase" in message and not isinstance(message["phase"], str):
                        continue
                    if "counters" in message and not isinstance(message["counters"], dict):
                        continue
                    timeouts.update(limits)
                    if "worker_started" in message:
                        workers.add(message["worker_started"])
                    if "worker_finished" in message:
                        workers.discard(message["worker_finished"])
                        if phase == "saving history" and "unresolved" not in message:
                            deadline = time.monotonic() + timeouts["history_timeout"]
                    if "unresolved" in message:
                        unresolved.add(message["unresolved"])
                    if "records" in message:
                        records = message["records"]
                    if "counters" in message:
                        counters = message["counters"]
                    if "phase" in message:
                        phase, path = message["phase"], message.get("path", path)
                        limit = timeouts["history_timeout" if phase == "saving history" else "mount_timeout"]
                        deadline = time.monotonic() + limit
            if time.monotonic() < deadline:
                continue
            print(f"\nFAILED {phase}: deadline exceeded at {path}. "
                  f"Last reported verified bytes={counters.get('bytes', 0)}, "
                  f"source bytes removed={counters.get('freed', 0)}. "
                  "These are last observations, not a final inventory.", file=sys.stderr, flush=True)
            for pid in workers | {process.pid}:
                _signal_group(pid, signal.SIGTERM)
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                pass
            for pid in workers | ({process.pid} if process.poll() is None else set()):
                _signal_group(pid, signal.SIGKILL)
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                unresolved.add(process.pid)
            # Workers are grandchildren: the supervisor cannot reap them or
            # prove their kernel I/O stopped merely by sending a signal.
            unresolved.update(workers)
            print(f"Records retained: {records or 'startup did not report a record directory'}. "
                  f"Worker PIDs requiring terminal-state checks: {sorted(unresolved)}. "
                  "Do not remove locks or partial files. Confirm the job and these workers "
                  "have stopped, then inspect receipts before retrying. A deadline does not "
                  "prove underlying filesystem I/O stopped.", file=sys.stderr, flush=True)
            return 1
        return process.returncode if process.returncode >= 0 else 128 - process.returncode
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
        selector.close()
        os.close(reader)


def entry(main):
    if supervised():
        return main()
    return watch([sys.executable, "-B", "-m", "gbi_data.cli", *sys.argv[1:]])
