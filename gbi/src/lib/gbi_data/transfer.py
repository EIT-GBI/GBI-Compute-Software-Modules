"""One-file copy, independent checksum, receipt, then optional source deletion.

Each invocation is a child process. Its parent bounds the entire operation,
including filesystem calls; a blocked child keeps its cross-node lock.
"""

import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import time

from .storage import fingerprint


def write_json(path, value):
    temporary = path.with_name(path.name + f".{os.getpid()}.tmp")
    with temporary.open("x") as stream:
        json.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def receipt(path, record):
    with path.open("a") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        stream.write(json.dumps(record, ensure_ascii=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def check_parent(path, root):
    if path.parent.resolve() != path.parent or not (path.parent == root or root in path.parent.parents):
        raise ValueError(f"parent path changed or contains a symlink: {path}")


def ensure_parent(path, root):
    # Check BEFORE mkdir: do not create directories through an existing link.
    check_parent(path, root)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    check_parent(path, root)


def copy_stream(source, target, fd, rclone, lock, progress, target_kind="fss", timings=None, max_bytes=None):
    timings = timings if timings is not None else {}
    started = time.monotonic()
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith("RCLONE_")}
    result = hashlib.sha256()
    # Pin the regular source inode and pass its descriptor. Rclone's path
    # namespace encodes control characters; passing an ordinary pathname can
    # silently select no file. A descriptor also avoids reopening a replaced
    # source path. --copy-links follows only our /dev/fd handle, not user links.
    source_fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        child = subprocess.Popen(
            [rclone, "cat", "/dev/fd", "--files-from-raw", "-", "--no-traverse",
             "--copy-links", "--config", "/dev/null",
             "--retries", "1", "--low-level-retries", "1", "--multi-thread-streams", "0"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=environment, pass_fds=(lock.fileno(), source_fd),
        ) if rclone else None
        if child:
            # Select the pinned handle directly, without listing transient Go
            # runtime descriptors in /dev/fd (which can disappear mid-listing).
            child.stdin.write(f"{source_fd}\n".encode())
            child.stdin.close()
    except BaseException:
        os.close(source_fd)
        os.close(fd)
        raise
    # The rclone child shares this process group and inherits the lock. Killing
    # the supervising job cannot let a retry write over surviving kernel I/O.
    reader = child.stdout if child else os.fdopen(os.dup(source_fd), "rb")
    try:
        total, last = 0, 0.0
        with os.fdopen(fd, "wb") as output:
            for block in iter(lambda: reader.read(1024 * 1024), b""):
                if max_bytes is not None and total + len(block) > max_bytes:
                    raise ValueError("source grew beyond the foreground byte budget; source kept")
                output.write(block)
                result.update(block)
                total += len(block)
                if time.monotonic() - last >= 2:
                    write_json(progress, {"bytes": total, "phase": "copying", "path": str(source)})
                    last = time.monotonic()
            output.flush()
            if target_kind != "alluxio":
                os.fsync(output.fileno())
            timings["copy"] = time.monotonic() - started
            write_json(progress, {"bytes": total, "phase": "closing", "path": str(source)})
            closing = time.monotonic()
        timings["close"] = time.monotonic() - closing
        if child and child.wait():
            raise OSError("rclone could not read the complete source")
    finally:
        os.close(source_fd)
        reader.close()
        if child and child.poll() is None:
            child.terminate()
            child.wait()
    write_json(progress, {"bytes": total, "phase": "verifying", "path": str(source)})
    return result.hexdigest(), total


def transfer(task):
    timings = {}
    source, target = Path(task["source"]), Path(task["target"])
    source_root, target_root = Path(task["source_root"]), Path(task["target_root"])
    state = Path(task["state"])
    key = hashlib.sha256(os.fsencode(target)).hexdigest()
    journal = state / "pending" / (key + ".json")
    # Fixed lock stripes avoid leaving millions of lock files after a move.
    # Unfinished state is separate and removed only while the stripe is held.
    with (state / "locks" / key[:3]).open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        check_parent(source, source_root)
        ensure_parent(target, target_root)
        before = fingerprint(source)
        if task.get("expected_source") is not None and before != task["expected_source"]:
            raise ValueError("source changed since the foreground probe; source kept; retry the command")
        if task.get("max_bytes") is not None and before[3] > task["max_bytes"]:
            raise ValueError("source exceeds the foreground byte budget; source kept")
        link = stat.S_ISLNK(before[2]) or task.get("decode_link", False)
        if not link and not stat.S_ISREG(before[2]):
            raise ValueError("only regular files and symlinks can be transferred")
        link_text = (os.fsdecode(source.read_bytes()) if task.get("decode_link") else os.readlink(source)) if link else None
        expected = hashlib.sha256(os.fsencode(link_text)).hexdigest() if link else None
        saved = json.loads(journal.read_text()) if journal.exists() else {}
        exists = os.path.lexists(target)
        reused = False
        if exists:
            target_before = fingerprint(target)
            if link and not task["encode_link"]:
                reused = target.is_symlink() and os.readlink(target) == link_text
            elif stat.S_ISREG(target_before[2]) and target_before[3] == before[3]:
                expected = expected or digest(source)
                reused = digest(target) == expected and fingerprint(target) == target_before
            if not reused:
                ours = (saved.get("source") == str(source) and saved.get("fingerprint") == before
                        and saved.get("target_identity") == target_before[:3]
                        and saved.get("phase") == "writing")
                if not ours:
                    raise ValueError(f"destination differs; both copies kept: {target}")
                target.unlink()  # Never truncate an incomplete Alluxio inode.
        if not reused:
            saved = {"source": str(source), "fingerprint": before, "phase": "writing"}
            if link and not task["encode_link"]:
                os.symlink(link_text, target)
                saved["target_identity"] = fingerprint(target)[:3]
                write_json(journal, saved)
            else:
                fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                try:
                    info = os.fstat(fd)
                    saved["target_identity"] = [info.st_dev, info.st_ino, stat.S_IFMT(info.st_mode)]
                    write_json(journal, saved)
                except BaseException:
                    os.close(fd)
                    raise
                if link:
                    with os.fdopen(fd, "wb") as stream:
                        stream.write(os.fsencode(link_text))
                else:
                    expected, copied_bytes = copy_stream(source, target, fd, task["rclone"], lock,
                                                        Path(task["progress"]), task["target_kind"], timings,
                                                        task.get("max_bytes"))
                    if copied_bytes != before[3]:
                        raise ValueError("source stream was short or grew; source kept")
        # Alluxio closes asynchronously. Only our own fresh write gets a retry
        # window; conflicting pre-existing entries are never overwritten.
        deadline = time.monotonic() + task["settle_seconds"]
        verifying = time.monotonic()
        while True:
            try:
                target_before = fingerprint(target)
                if link and not task["encode_link"]:
                    matches = target.is_symlink() and os.readlink(target) == link_text
                else:
                    matches = stat.S_ISREG(target_before[2]) and digest(target) == expected
                if matches and fingerprint(target) == target_before:
                    break
            except OSError:
                pass
            if reused or task["target_kind"] != "alluxio" or time.monotonic() >= deadline:
                raise ValueError("destination checksum did not match; source kept")
            time.sleep(1)
        timings["destination_readback"] = time.monotonic() - verifying
        check_parent(source, source_root)
        check_parent(target, target_root)
        if fingerprint(source) != before:
            raise ValueError("source changed during transfer; source kept")
        if not link and task["target_kind"] != "alluxio" and not reused:
            if fingerprint(target) != target_before:
                raise ValueError("destination changed after verification; source kept")
            metadata = source.stat()
            os.chmod(target, stat.S_IMODE(metadata.st_mode) & 0o777)
            os.utime(target, ns=(metadata.st_atime_ns, metadata.st_mtime_ns))
            after_metadata = fingerprint(target)
            # Some Lustre mounts store whole-second mtimes even when FSS
            # supplies nanoseconds. Preserve the representable timestamp.
            if (after_metadata[:4] != target_before[:4]
                    or not 0 <= metadata.st_mtime_ns - after_metadata[4] < 1_000_000_000):
                raise ValueError("destination changed while preserving metadata; source kept")
            target_before = after_metadata
        # Coarse filesystem timestamps can hide a same-size, same-second edit.
        # A move therefore rechecks source content before writing its receipt.
        if task["delete"] and not link:
            write_json(Path(task["progress"]), {"bytes": before[3], "phase": "source recheck", "path": str(source)})
            rechecking = time.monotonic()
            if digest(source) != expected:
                raise ValueError("source content changed during transfer; source kept")
            timings["source_recheck"] = time.monotonic() - rechecking
        record = {"source": str(source), "destination": str(target), "bytes": before[3],
                  "sha256": expected, "fingerprint": before, "kind": "symlink" if link else "file",
                  "reused": reused, "event": "verified", "time": time.time(), "timings_seconds": timings,
                  "reader": "rclone" if task["rclone"] else "native"}
        # An append-only, fsynced verification receipt is required before unlink.
        saved["phase"] = "verified"
        write_json(journal, saved)
        receipt(Path(task["receipt"]), record)
        if task["delete"]:
            if fingerprint(source) != before or fingerprint(target) != target_before:
                raise ValueError("file changed after verification; source kept")
            source.unlink()
            receipt(Path(task["receipt"]), {**record, "event": "deleted", "time": time.time()})
            parent = source.parent
            boundary = Path(task["selection_root"])
            while parent != boundary and boundary in parent.parents:
                try:
                    parent.rmdir()
                except OSError:
                    break
                parent = parent.parent
        journal.unlink()
        return {"bytes": before[3], "deleted": task["delete"], "reused": reused}


def main():
    os.umask(0o077)
    task = json.load(sys.stdin)
    try:
        result = transfer(task)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        result = {"error": str(error), "source": task["source"]}
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
