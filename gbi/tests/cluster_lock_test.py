"""Run with srun --nodes=2 --ntasks=2 on a fresh shared scratch directory."""

import fcntl
import os
from pathlib import Path
import socket
import sys
import time


root = Path(sys.argv[1])
rank = int(os.environ["SLURM_PROCID"])
deadline = time.monotonic() + 60


def await_file(name):
    while not (root / name).exists():
        if time.monotonic() > deadline:
            raise TimeoutError(name)
        time.sleep(0.1)


with (root / "lock").open("a") as lock:
    if rank == 0:
        fcntl.flock(lock, fcntl.LOCK_EX)
        (root / "held").write_text(socket.gethostname())
        await_file("checked")
    else:
        await_file("held")
        assert (root / "held").read_text() != socket.gethostname()
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            (root / "checked").write_text("PASS: second node could not acquire held lock")
        else:
            raise AssertionError("cross-node exclusion failed")
print(f"PASS rank={rank} node={socket.gethostname()}", flush=True)
