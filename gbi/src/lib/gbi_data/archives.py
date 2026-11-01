"""Portable, marked tar archives. Payloads stream; extraction never follows links.

Streams may be seekable binary objects or zero-argument factories returning a
fresh binary reader. Factories allow chunked storage without staging a tar file.
The manifest is bounded to 64 MiB; payload size is not bounded by memory.
"""

from collections import deque
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor
from concurrent.futures import wait as _wait_futures
from contextlib import contextmanager
import gzip
import inspect
import fnmatch
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import stat
import queue
import tarfile
import threading
import time
import uuid

from .selection import matches

FORMAT       = "gbi-portable-archive"
VERSION      = 1
MARKER       = ".gbi-archive/format.json"
MANIFEST     = ".gbi-archive/manifest.json"
MAX_MANIFEST = 64 * 1024 * 1024
BLOCK        = 1024 * 1024


class ArchiveError(ValueError):
    """An archive or source cannot be verified safely."""


class VerifiedArchiveEvidence:
    """Capability issued by a maintained verifier for one public manifest."""

    __slots__ = ("manifest_sha256", "evidence_sha256")

    def __init__(self, manifest_sha256, evidence_sha256):
        raise ArchiveError(
            "verified archive evidence can only be issued by a maintained verifier"
        )

    @classmethod
    def _issue(cls, manifest_sha256, evidence_sha256):
        value = object.__new__(cls)
        object.__setattr__(value, "manifest_sha256", manifest_sha256)
        object.__setattr__(value, "evidence_sha256", evidence_sha256)
        return value

    def __setattr__(self, name, value):
        raise AttributeError("verified archive evidence is immutable")

    def assert_manifest(self, manifest):
        from .formats import _manifest_digest

        if _manifest_digest(manifest) != self.manifest_sha256:
            raise ArchiveError(
                "verified archive evidence does not match the cleanup manifest"
            )


def _json(value):
    result = json.dumps(
        value, ensure_ascii = True, sort_keys = True, separators = (",", ":")
    ).encode()
    if len(result) > MAX_MANIFEST:
        raise ArchiveError(
            "archive manifest exceeds the 64 MiB supported limit"
        )
    return result


def _relative(name):
    if (
        not isinstance(name, str)
        or not name
        or "\0" in name
        or name.startswith("/")
        or any(p in ("", ".", "..") for p in name.split("/"))
        or PurePosixPath(name).as_posix() != name
    ):
        raise ArchiveError(f"unsafe archive path: {name!r}")
    return name


def _observation(info):
    return [
        info.st_dev, info.st_ino, info.st_mode, info.st_size, info.st_mtime_ns,
        info.st_ctime_ns,
    ]


def _rebased_evidence(evidence, root_info):
    """Return the journal's root and observations numbered for this client.

    NFS gives every client its own ``st_dev`` for the same export, so a cleanup
    resumed on another Slurm node sees a different device number for an
    unchanged tree.  The device number identifies a filesystem only within one
    client; the inode, mode, size, mtime and ctime fields and the content
    hashes keep their meaning.  Observations recorded on the root's device are
    renumbered to this client's device only when the root is still the same
    inode and mode; entries recorded on another device (a mount below the
    source) keep their recorded number and so fail closed as before.
    """
    recorded = evidence["root"]
    current  = _observation(root_info)
    if current[0] == recorded[0] or current[1:3] != recorded[1:3]:
        return evidence["root"], evidence["observations"]

    def rebase(observation):
        if observation[0] == recorded[0]:
            return [current[0], *observation[1:]]
        return observation

    return rebase(recorded), {
        name: rebase(observation)
        for name, observation in evidence["observations"].items()
    }


def _kind(info):
    for test, name in (
        (stat.S_ISREG, "file"), (stat.S_ISDIR, "directory"),
        (stat.S_ISLNK, "symlink"),
    ):
        if test(info.st_mode):
            return name
    return "special"


def _reserved(name, patterns):
    return any(
        fnmatch.fnmatchcase(part, pattern)
        for part in name.split("/")
        for pattern in patterns
    )


def _selection(includes, exclusions, mode, selection_prefix = ""):
    if not isinstance(selection_prefix, str):
        raise ArchiveError("selection_prefix must be a relative path or empty")
    if selection_prefix:
        _relative(selection_prefix)
        if mode != "relative" or "\\" in selection_prefix:
            raise ArchiveError(
                "selection_prefix requires relative mode and a POSIX path"
            )
    if mode == "basename":
        return lambda name: matches(
            PurePosixPath(name).name, includes, exclusions
        )
    if mode != "relative":
        raise ArchiveError("selection_mode must be basename or relative")
    patterns = []
    for group in (includes, exclusions):
        compiled = []
        for pattern in group:
            parts = _relative(pattern).split("/")
            if (
                "{" in pattern
                or "}" in pattern
                or any("**" in part and part != "**" for part in parts)
            ):
                raise ArchiveError(
                    "relative selection requires plain globs with whole-segment **"
                )
            compiled.append(parts)
        patterns.append(compiled)

    def match(parts, pattern):
        # Segment matching agrees with transfer_core.operations.matches_parts.
        # Keep reachable positions instead of recursively revisiting ** paths.
        positions = {0}
        for segment in pattern:
            if segment == "**":
                positions = (
                    set(range(min(positions), len(parts) + 1))
                    if positions
                    else set()
                )
            else:
                positions = {
                    i + 1
                    for i in positions
                    if i < len(parts) and fnmatch.fnmatchcase(parts[i], segment)
                }
        return len(parts) in positions

    def selected(name):
        parts = (
            (selection_prefix + "/" if selection_prefix else "") + name
        ).split("/")
        return (
            not patterns[0] or any(match(parts, p) for p in patterns[0])
        ) and not any(match(parts, p) for p in patterns[1])

    return selected


def _walk(root, reserved = ()):
    # No scandir recursion through a symlink. Directory identity is checked
    # before and after walking, and once more after the complete archive closes.
    def visit(directory, prefix = ""):
        before = _observation(directory.lstat())
        if not stat.S_ISDIR(before[2]):
            raise ArchiveError("source directory changed into a non-directory")
        with os.scandir(directory) as children:
            for child in children:
                name = f"{prefix}/{child.name}" if prefix else child.name
                if _reserved(name, reserved):
                    continue
                info = child.stat(follow_symlinks = False)
                kind = _kind(info)
                yield name, info, kind
                if kind == "directory":
                    yield from visit(Path(child.path), name)
        if _observation(directory.lstat()) != before:
            raise ArchiveError(
                "source directory changed during packing; source kept"
            )

    yield from visit(root)


class _HashReader:
    def __init__(self, stream):
        self.stream = stream
        self.hash   = hashlib.sha256()

    def read(self, size = -1):
        data = self.stream.read(size)
        self.hash.update(data)
        return data


@contextmanager
def _compressed(output, compression):
    """The stream the tar writer targets: ``output`` or a gzip layer over it.

    The gzip header carries no name and a zero timestamp, so packing an
    unchanged source again yields byte-identical output and an interrupted
    chunked upload can reuse the parts it already stored.
    """
    if compression not in ("gzip", "tar.gz"):
        yield output
        return
    layer = gzip.GzipFile(
        filename = "", fileobj = output, mode = "wb", mtime = 0
    )
    try:
        yield layer
    finally:
        layer.close()


READAHEAD_BLOCK = 8 * BLOCK
READAHEAD_DEPTH = 8


class _ReadAhead:
    """Ordered block read-ahead for one large file on a helper thread.

    The helper reads consecutive ``READAHEAD_BLOCK`` pieces into a bounded
    queue while the caller hashes and frames the previous ones, so the tar
    writer no longer waits for each network round trip of a file too large
    to prefetch whole. Exactly the file's bytes are delivered, in order.
    """

    def __init__(self, stream, size):
        self.stream   = stream
        self.size     = size
        self._queue   = queue.Queue(READAHEAD_DEPTH)
        self._buffer  = b""
        self._done    = False
        self._failure = None
        self._stop    = threading.Event()
        self._thread  = threading.Thread(
            target = self._fill, name = "gbi-readahead", daemon = True
        )
        self._thread.start()

    def _fill(self):
        try:
            remaining = self.size
            while remaining > 0 and not self._stop.is_set():
                block = self.stream.read(min(READAHEAD_BLOCK, remaining))
                if not block:
                    break
                remaining -= len(block)
                self._queue.put(block)
        except BaseException as error:
            self._failure = error
        finally:
            self._queue.put(None)

    def read(self, size = -1):
        parts = []
        wanted = size
        while wanted != 0:
            if not self._buffer:
                if self._done:
                    break
                block = self._queue.get()
                if block is None:
                    self._done = True
                    if self._failure is not None:
                        raise self._failure
                    break
                self._buffer = block
            take = len(self._buffer) if wanted < 0 else min(wanted, len(self._buffer))
            parts.append(self._buffer[:take])
            self._buffer = self._buffer[take:]
            if wanted > 0:
                wanted -= take
        return b"".join(parts)

    def close(self):
        """Stop the helper and wait for it, so the stream may be closed safely."""
        self._stop.set()
        while not self._done:
            if self._queue.get() is None:
                self._done = True
        self._thread.join()


def _add_json(archive, name, value):
    data   = _json(value)
    member = tarfile.TarInfo(name)
    member.size = len(data)
    member.mode = 0o600
    archive.addfile(member, io.BytesIO(data))


def _metadata_entry(name, info, kind, target = None):
    """Describe an entry without reading payload; SHA256 has a fixed width."""
    entry = {
        "path": name, "type": kind,
        "mode": stat.S_IMODE(info.st_mode) & 0o777,
        "mtime_ns": info.st_mtime_ns,
        "size": 0 if kind == "directory" else info.st_size,
    }
    if kind != "directory":
        entry["sha256"] = "0" * 64
    if target is not None:
        entry["target"] = target
    return entry


def _manifest_size(entries_bytes, count):
    envelope = len(_json({"format": FORMAT, "version": VERSION, "entries": []}))
    return envelope + entries_bytes + max(0, count - 1)


def check_metadata_budget(
            source, compression = None, includes = (), exclusions = (),
            reserved_names = (), max_bytes = None, *,
            selection_mode = "basename", selection_prefix = "",
            visit = lambda: None,
        ):
    """Reject oversized metadata before a caller opens or replaces an output.

    Read names, file metadata and symlink targets only. Packing still rechecks
    the live source and enforces both bounds; this estimate never authorizes
    source deletion or destination reuse.
    """
    selected = _selection(
        includes, exclusions, selection_mode, selection_prefix
    )
    if compression not in (None, "tar", "gzip", "tar.gz"):
        raise ArchiveError("compression must be tar or gzip")
    source = Path(source)
    if (
        source.is_symlink()
        or not source.is_dir()
        or source.resolve() != source.absolute()
    ):
        raise ArchiveError(
            "archive source must be a directory without symlink parents"
        )
    reserved = (".gbi", ".prefect-*", "transfer-locks", *reserved_names)
    with _directory(source) as root_fd:
        root_before = _observation(os.fstat(root_fd))

        def read_target(name):
            with _parent(root_fd, name, create = False) as (parent, leaf):
                return os.readlink(leaf, dir_fd = parent)

        result = _metadata_budget(
            _walk(source, reserved), selected, read_target = read_target,
            visit = visit, max_bytes = max_bytes,
        )
        if (
            _observation(os.fstat(root_fd)) != root_before
            or _observation(source.lstat()) != root_before
        ):
            raise ArchiveError(
                "source directory changed during archive preflight; source kept"
            )
    return result


def _metadata_budget(
            entries, selected, *, read_target, visit = lambda: None,
            max_bytes = None,
        ):
    """Account the manifest and observation sizes of an entry stream.

    ``entries`` yields ``(name, info, kind)`` in walk order, as ``_walk``
    does; ``read_target`` returns a symlink's target. The same accounting
    serves a live directory scan and the planner's replay of an inventory it
    already holds, so both agree to the byte.
    """
    directories, hardlinks = {}, {}
    required, nonempty = set(), set()
    entries_bytes     = 0
    count             = 0
    observation_bytes = 0
    selected_bytes    = 0

    def account(entry):
        nonlocal entries_bytes, count
        entries_bytes += len(_json(entry))
        count         += 1
        if _manifest_size(entries_bytes, count) > MAX_MANIFEST:
            raise ArchiveError(
                "archive manifest exceeds the 64 MiB supported limit; "
                "choose smaller source folders or use --pack-small"
            )

    for name, info, kind in entries:
        visit()
        _relative(name)
        observation_bytes += len(_json([name, _observation(info)]))
        if observation_bytes > MAX_MANIFEST:
            raise ArchiveError(
                "source observation inventory exceeds the 64 MiB supported limit; "
                "choose smaller source folders or use --pack-small"
            )
        nonempty.add(str(PurePosixPath(name).parent))
        if kind == "directory":
            directories[name] = info
            continue
        if not selected(name):
            continue
        if kind == "special":
            raise ArchiveError(
                f"selected source is an unsupported special file: {name!r}"
            )
        selected_bytes += info.st_size
        if max_bytes is not None and selected_bytes > max_bytes:
            raise ArchiveError(
                "selected source exceeds the foreground byte budget; "
                "source kept; retry through Slurm"
            )
        target = None
        if kind == "symlink":
            target = read_target(name)
        elif (info.st_dev, info.st_ino) in hardlinks:
            kind, target = "hardlink", hardlinks[(info.st_dev, info.st_ino)]
        else:
            hardlinks[(info.st_dev, info.st_ino)] = name
        account(_metadata_entry(name, info, kind, target))
        required.update(
            str(parent)
            for parent in PurePosixPath(name).parents
            if str(parent) != "."
        )
    required.update(
        name
        for name in directories
        if name not in nonempty and selected(name)
    )
    for name in list(required):
        required.update(
            str(parent)
            for parent in PurePosixPath(name).parents
            if str(parent) != "."
        )
    for name, info in directories.items():
        visit()
        if name in required:
            account(_metadata_entry(name, info, "directory"))
    return {
        "entries": count,
        "manifest_bytes": _manifest_size(entries_bytes, count),
        "observation_bytes": observation_bytes,
        "selected_bytes": selected_bytes,
    }


PACK_READERS        = 16
PACK_PREFETCH_BYTES = 256 * 1024 * 1024
PACK_PREFETCH_ITEMS = 2048
PACK_INLINE_BYTES   = 32 * 1024 * 1024


def _pack_readers(readers):
    if readers is None:
        value   = os.environ.get("GBI_PACK_READERS", "").strip()
        readers = int(value) if value.isdigit() and int(value) > 0 else PACK_READERS
    if type(readers) is not int or readers < 1:
        raise ArchiveError("pack readers must be a positive integer")
    return readers


def _read_entry(root_fd, name, observation):
    """Read one selected regular file completely on a reader thread.

    The file is opened relative to the pinned source root and its identity is
    compared with the walk observation before and after the read, so a path
    component swapped underneath the walk is detected as a different inode.
    """
    fd = os.open(
        name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd = root_fd
    )
    try:
        if _observation(os.fstat(fd)) != observation:
            raise ArchiveError("source changed before packing; source kept")
        expected = observation[3]
        digest   = hashlib.sha256()
        blocks   = []
        total    = 0
        while True:
            block = os.read(fd, BLOCK)
            if not block:
                break
            total += len(block)
            if total > expected:
                raise ArchiveError("source changed during packing; source kept")
            digest.update(block)
            blocks.append(block)
        if total != expected or _observation(os.fstat(fd)) != observation:
            raise ArchiveError("source changed during packing; source kept")
    finally:
        os.close(fd)
    return b"".join(blocks), digest.hexdigest()


def pack(
            source, output, compression = None, includes = (), exclusions = (),
            reserved_names = (), max_bytes = None, *,
            selection_mode = "basename", selection_prefix = "",
            readers = None, prefetch_bytes = PACK_PREFETCH_BYTES,
            inline_bytes = PACK_INLINE_BYTES,
        ):
    """Stream a directory into standard tar/tar+gzip; return cleanup observations.

    Output is caller-owned. Publish it only after success and the caller's
    destination proof. This function never deletes sources.
    Filters match recursive basenames by default; ``selection_mode="relative"``
    matches source-relative POSIX segments, with ** spanning zero or more levels.

    Selected regular files up to ``inline_bytes`` are read ahead of the tar
    writer by ``readers`` threads (``GBI_PACK_READERS`` overrides the default),
    holding at most ``prefetch_bytes`` of content, so one network round trip per
    file no longer serialises the whole archive. Larger files stream inline.
    The tar bytes are identical for any reader count: members are written in
    walk order and every file is read exactly once.
    """
    selected = _selection(
        includes, exclusions, selection_mode, selection_prefix
    )
    if compression not in (None, "tar", "gzip", "tar.gz"):
        raise ArchiveError("compression must be tar or gzip")
    readers = _pack_readers(readers)
    if (
        type(prefetch_bytes) is not int or prefetch_bytes < 0
        or type(inline_bytes) is not int or inline_bytes < 0
    ):
        raise ArchiveError("pack prefetch bounds must be non-negative integers")
    source = Path(source)
    if (
        source.is_symlink()
        or not source.is_dir()
        or source.resolve() != source.absolute()
    ):
        raise ArchiveError(
            "archive source must be a directory without symlink parents"
        )
    root_before = _observation(source.lstat())
    reserved    = (".gbi", ".prefect-*", "transfer-locks", *reserved_names)
    entries, observations, hardlinks = [], {}, {}
    directories = {}
    manifest_bytes, observation_bytes, selected_bytes = 0, 0, 0
    pool = ThreadPoolExecutor(
        max_workers = readers, thread_name_prefix = "gbi-pack"
    )
    try:
        with (
            _directory(source) as root_fd,
            _compressed(output, compression) as sink,
            tarfile.open(
                fileobj = sink, mode = "w|", format = tarfile.PAX_FORMAT,
                bufsize = BLOCK, copybufsize = BLOCK,
            ) as archive,
        ):
            _add_json(archive, MARKER, {"format": FORMAT, "version": VERSION})
            walk      = _walk(source, reserved)
            pending   = deque()
            queued    = set()
            state     = {"inflight": 0, "exhausted": False}

            def admit():
                nonlocal observation_bytes, selected_bytes
                while (
                    not state["exhausted"]
                    and len(pending) < PACK_PREFETCH_ITEMS
                    and state["inflight"] < prefetch_bytes
                ):
                    try:
                        name, info, kind = next(walk)
                    except StopIteration:
                        state["exhausted"] = True
                        return
                    _relative(name)
                    observations[name] = _observation(info)
                    observation_bytes += len(_json([name, observations[name]]))
                    if observation_bytes > MAX_MANIFEST:
                        raise ArchiveError(
                            "source observation inventory exceeds the 64 MiB supported limit"
                        )
                    if kind == "directory":
                        directories[name] = info
                        continue
                    if not selected(name):
                        continue
                    if kind == "special":
                        raise ArchiveError(
                            f"selected source is an unsupported special file: {name!r}"
                        )
                    selected_bytes += info.st_size
                    if max_bytes is not None and selected_bytes > max_bytes:
                        raise ArchiveError(
                            "selected source exceeds the foreground byte budget; "
                            "source kept; retry through Slurm"
                        )
                    future   = None
                    identity = (info.st_dev, info.st_ino)
                    if kind == "file" and identity not in queued:
                        queued.add(identity)
                        if info.st_size <= inline_bytes:
                            future = pool.submit(
                                _read_entry, root_fd, name, observations[name]
                            )
                            state["inflight"] += info.st_size
                    pending.append((name, info, kind, future))

            admit()
            while pending:
                name, info, kind, future = pending.popleft()
                path  = source / name
                entry = {
                    "path": name, "type": kind,
                    "mode": stat.S_IMODE(info.st_mode) & 0o777,
                    "mtime_ns": info.st_mtime_ns, "size": info.st_size,
                }
                member = tarfile.TarInfo("data/" + name)
                member.mode  = entry["mode"]
                member.mtime = info.st_mtime_ns / 1_000_000_000
                member.pax_headers = {
                    "mtime": str(info.st_mtime_ns // 1_000_000_000)
                    + "."
                    + f"{info.st_mtime_ns % 1_000_000_000:09d}"
                }
                if kind == "symlink":
                    with _parent(root_fd, name, create = False) as (parent, leaf):
                        target = os.readlink(leaf, dir_fd = parent)
                    entry.update(
                        target = target,
                        sha256 = hashlib.sha256(os.fsencode(target)).hexdigest(),
                    )
                    member.type     = tarfile.SYMTYPE
                    member.linkname = target
                    archive.addfile(member)
                    if _observation(path.lstat()) != observations[name]:
                        raise ArchiveError(
                            "source changed during packing; source kept"
                        )
                elif (info.st_dev, info.st_ino) in hardlinks:
                    target = hardlinks[(info.st_dev, info.st_ino)]
                    entry.update(
                        type = "hardlink", target = target["path"],
                        sha256 = target["sha256"],
                    )
                    member.type     = tarfile.LNKTYPE
                    member.linkname = "data/" + target["path"]
                    archive.addfile(member)
                    if _observation(path.lstat()) != observations[name]:
                        raise ArchiveError(
                            "source changed during packing; source kept"
                        )
                elif future is not None:
                    content, digest = future.result()
                    state["inflight"] -= info.st_size
                    member.size = len(content)
                    archive.addfile(member, io.BytesIO(content))
                    del content
                    entry["sha256"] = digest
                    hardlinks[(info.st_dev, info.st_ino)] = entry
                else:
                    with _parent(root_fd, name, create = False) as (parent, leaf):
                        fd = os.open(
                            leaf, os.O_RDONLY | os.O_NOFOLLOW, dir_fd = parent
                        )
                    with os.fdopen(fd, "rb") as stream:
                        if (
                            _observation(os.fstat(stream.fileno()))
                            != observations[name]
                        ):
                            raise ArchiveError(
                                "source changed before packing; source kept"
                            )
                        ahead       = _ReadAhead(stream, info.st_size)
                        reader      = _HashReader(ahead)
                        member.size = info.st_size
                        try:
                            archive.addfile(member, reader)
                            if ahead.read(1):
                                raise ArchiveError(
                                    "source changed during packing; source kept"
                                )
                        finally:
                            ahead.close()
                        if (
                            stream.read(1)
                            or _observation(os.fstat(stream.fileno()))
                            != observations[name]
                        ):
                            raise ArchiveError(
                                "source changed during packing; source kept"
                            )
                        entry["sha256"] = reader.hash.hexdigest()
                    hardlinks[(info.st_dev, info.st_ino)] = entry
                    if _observation(path.lstat()) != observations[name]:
                        raise ArchiveError(
                            "source changed during packing; source kept"
                        )
                entries.append(entry)
                manifest_bytes += len(_json(entry)) + 1
                if manifest_bytes > MAX_MANIFEST:
                    raise ArchiveError(
                        "archive manifest exceeds the 64 MiB supported limit"
                    )
                admit()
            # Retain ancestors of selections and genuinely empty directories. Do
            # not add unrelated nonempty directories when filters select nothing.
            required = {
                str(parent)
                for e in entries
                for parent in PurePosixPath(e["path"]).parents
                if str(parent) != "."
            }
            nonempty = {str(PurePosixPath(name).parent) for name in observations}
            required.update(
                name
                for name in directories
                if name not in nonempty and selected(name)
            )
            for name in list(required):
                required.update(
                    str(parent)
                    for parent in PurePosixPath(name).parents
                    if str(parent) != "."
                )
            for name, info in directories.items():
                if name not in required:
                    continue
                entry = {
                    "path": name, "type": "directory",
                    "mode": stat.S_IMODE(info.st_mode) & 0o777,
                    "mtime_ns": info.st_mtime_ns, "size": 0,
                }
                member = tarfile.TarInfo("data/" + name)
                member.type  = tarfile.DIRTYPE
                member.mode  = entry["mode"]
                member.mtime = info.st_mtime
                member.pax_headers = {
                    "mtime": str(info.st_mtime_ns // 1_000_000_000)
                    + "."
                    + f"{info.st_mtime_ns % 1_000_000_000:09d}"
                }
                archive.addfile(member)
                entries.append(entry)
            manifest = {"format": FORMAT, "version": VERSION, "entries": entries}
            _add_json(archive, MANIFEST, manifest)
    except BaseException:
        pool.shutdown(wait = False, cancel_futures = True)
        raise
    finally:
        pool.shutdown(wait = True)
    manifest["_source"] = {
        "root": root_before, "observations": observations,
        "reserved": list(reserved),
    }
    verify_source(source, manifest)
    return manifest

@contextmanager
def _reader(source):
    if callable(source):
        stream = source()
        if hasattr(stream, "__enter__"):
            with stream as reader:
                yield reader
            return
        try:
            yield stream
        finally:
            stream.close()
    elif isinstance(source, (str, os.PathLike)):
        with open(source, "rb") as stream:
            yield stream
    else:
        source.seek(0)
        yield source


class _PrefixReader:
    def __init__(self, prefix, stream):
        self.prefix = prefix
        self.stream = stream

    def read(self, size = -1):
        if size < 0:
            value       = self.prefix
            self.prefix = b""
            return value + self.stream.read()
        value       = self.prefix[:size]
        self.prefix = self.prefix[size:]
        return value + self.stream.read(size - len(value))


@contextmanager
def _tar_reader(source):
    with _reader(source) as stream:
        prefix  = stream.read(2)
        wrapped = _PrefixReader(prefix, stream)
        decoded = (
            gzip.GzipFile(fileobj = wrapped, mode = "rb")
            if prefix == b"\x1f\x8b"
            else wrapped
        )
        try:
            header = decoded.read(512)
            try:
                with tarfile.open(
                    fileobj = _PrefixReader(header, decoded), mode = "r|",
                    bufsize = BLOCK, copybufsize = BLOCK,
                ) as archive:
                    yield archive
            except tarfile.ReadError as error:
                if header[:100].split(b"\0", 1)[0] == MARKER.encode():
                    raise ArchiveError(
                        "GBI archive header is corrupt or truncated"
                    ) from error
                raise
        finally:
            if decoded is not wrapped:
                decoded.close()


def _read_json(archive, member):
    if not member.isfile() or member.size > MAX_MANIFEST:
        raise ArchiveError("invalid or oversized GBI archive manifest")
    try:
        value = json.loads(archive.extractfile(member).read())
    except (ValueError, UnicodeError) as error:
        raise ArchiveError("invalid GBI archive JSON") from error
    if not isinstance(value, dict):
        raise ArchiveError("invalid GBI archive manifest")
    return value


def _public(manifest):
    return {
        key: value for key, value in manifest.items() if not key.startswith("_")
    }


def _scan(source, expected = None, consume = None):
    """Read and hash every member, including members excluded from restoration."""
    marked = False
    expected_entries = (
        {entry["path"]: entry for entry in expected["entries"]}
        if expected
        else None
    )

    def check_expected(item):
        if expected_entries is not None:
            saved = expected_entries.get(item["path"])
            if saved is None or any(
                saved.get(key) != value for key, value in item.items()
            ):
                raise ArchiveError(
                    "archive member metadata or checksum changed since verification/packing"
                )

    try:
        with _tar_reader(source) as archive:
            first = archive.next()
            if first is None or first.name != MARKER:
                return None
            marked = True
            if _read_json(archive, first) != {
                "format": FORMAT, "version": VERSION,
            }:
                raise ArchiveError("unsupported GBI archive format or version")
            observed, manifest, manifest_bytes = {}, None, 0
            for member in archive:
                if member is first:
                    continue
                if manifest is not None:
                    raise ArchiveError(
                        "members follow the final archive manifest"
                    )
                if member.name == MANIFEST:
                    manifest = _read_json(archive, member)
                    continue
                if not member.name.startswith("data/"):
                    raise ArchiveError("unexpected archive member")
                name = _relative(member.name[5:])
                if _reserved(name, (".gbi", ".prefect-*", "transfer-locks")):
                    raise ArchiveError(
                        "archive member enters reserved transfer state"
                    )
                if name in observed or member.mode & ~0o777:
                    raise ArchiveError(
                        "duplicate member or privileged mode in archive"
                    )
                if member.sparse is not None or any(
                    k.startswith("GNU.sparse") for k in member.pax_headers
                ):
                    raise ArchiveError("sparse archive members are unsupported")
                if member.isfile():
                    kind = "file"
                elif member.isdir():
                    kind = "directory"
                elif member.issym():
                    kind = "symlink"
                elif member.islnk():
                    kind = "hardlink"
                else:
                    raise ArchiveError("unsupported special archive member")
                if kind != "file" and member.size != 0:
                    raise ArchiveError(
                        "non-file member contains unexpected payload"
                    )
                item = {
                    "path": name, "type": kind, "mode": member.mode,
                    "mtime_ns": _mtime_ns(member), "size": member.size,
                }
                if kind == "file":
                    reader = _HashReader(archive.extractfile(member))
                    check_expected(item)
                    if consume:
                        consume(item, reader)
                    while reader.read(BLOCK):
                        pass
                    item["sha256"] = reader.hash.hexdigest()
                elif kind == "symlink":
                    item.update(
                        target = member.linkname,
                        size = len(os.fsencode(member.linkname)),
                        sha256 = hashlib.sha256(
                            os.fsencode(member.linkname)
                        ).hexdigest(),
                    )
                elif kind == "hardlink":
                    if not member.linkname.startswith("data/"):
                        raise ArchiveError("unsafe hardlink target")
                    target = _relative(member.linkname[5:])
                    prior  = observed.get(target)
                    if prior is None or prior["type"] != "file":
                        raise ArchiveError(
                            "hardlink must refer to an earlier regular member"
                        )
                    item.update(
                        target = target, size = prior["size"],
                        sha256 = prior["sha256"],
                    )
                elif member.size != 0:
                    raise ArchiveError("directory contains unexpected payload")
                check_expected(item)
                if kind != "file" and consume:
                    consume(item, None)
                observed[name] = item
                manifest_bytes += len(_json(item)) + 1
                if manifest_bytes > MAX_MANIFEST:
                    raise ArchiveError(
                        "archive manifest exceeds the 64 MiB supported limit"
                    )
            if manifest is None:
                raise ArchiveError("truncated archive: final manifest missing")
            if (
                set(manifest) != {"format", "version", "entries"}
                or manifest.get("format") != FORMAT
                or manifest.get("version") != VERSION
            ):
                raise ArchiveError(
                    "unsupported archive manifest format or version"
                )
            entries = manifest.get("entries")
            if not isinstance(entries, list) or len(entries) != len(observed):
                raise ArchiveError("manifest does not match archive members")
            seen = set()
            for entry in entries:
                if (
                    not isinstance(entry, dict)
                    or not isinstance(entry.get("path"), str)
                    or entry["path"] in seen
                    or observed.get(entry["path"]) != entry
                ):
                    raise ArchiveError(
                        "archive member checksum or metadata differs from manifest"
                    )
                seen.add(entry["path"])
                for parent in PurePosixPath(entry["path"]).parents:
                    if str(parent) != "." and (
                        str(parent) not in observed
                        or observed[str(parent)]["type"] != "directory"
                    ):
                        raise ArchiveError(
                            "archive parent is absent or is not a directory"
                        )
            # tarfile stops after the first zero end record. Require the second
            # and drain all padding, which also checks the gzip trailer/CRC.
            if archive.fileobj.read(512) != bytes(512):
                raise ArchiveError("truncated archive end records")
            while True:
                padding = archive.fileobj.read(BLOCK)
                if not padding:
                    break
                if padding.strip(b"\0"):
                    raise ArchiveError("unexpected data after archive end")
            if expected is not None and manifest != _public(expected):
                raise ArchiveError(
                    "stored archive does not match the packed source manifest"
                )
            return manifest
    except (
        tarfile.TarError, EOFError, gzip.BadGzipFile, UnicodeError,
    ) as error:
        if not marked and isinstance(error, tarfile.ReadError):
            return None
        raise ArchiveError(
            "archive is corrupt or truncated; source kept"
        ) from error


def _mtime_ns(member):
    from decimal import Decimal

    return int(
        Decimal(str(member.pax_headers.get("mtime", member.mtime)))
        * 1_000_000_000
    )


def inspect_archive(source):
    """Return a fully verified manifest, or None for an ordinary unmarked file."""
    return _scan(source)


def archive_marker(source):
    """Recognize the bounded first marker without hashing ordinary payloads.

    Recognition is not validation. Call inspect_archive/restore before using
    members. Arbitrary binary and ordinary tar return False; claimed GBI
    markers with corruption or unsupported versions fail clearly.
    """
    claimed = False
    try:
        with _reader(source) as stream:
            prefix  = stream.read(2)
            wrapped = _PrefixReader(prefix, stream)
            decoded = (
                gzip.GzipFile(fileobj = wrapped, mode = "rb")
                if prefix == b"\x1f\x8b"
                else wrapped
            )
            try:
                header  = decoded.read(512)
                claimed = header[:100].split(b"\0", 1)[0] == MARKER.encode()
                if not claimed:
                    return False
                member = tarfile.TarInfo.frombuf(
                    header, "utf-8", "surrogateescape"
                )
                if not member.isfile() or not 0 < member.size <= 4096:
                    raise ArchiveError("invalid GBI format marker")
                marker = json.loads(decoded.read(member.size))
                if marker != {"format": FORMAT, "version": VERSION}:
                    raise ArchiveError(
                        "unsupported GBI archive format or version"
                    )
                return True
            finally:
                if decoded is not wrapped:
                    decoded.close()
    except (
        tarfile.TarError, EOFError, gzip.BadGzipFile, UnicodeError,
        json.JSONDecodeError,
    ) as error:
        if claimed:
            raise ArchiveError(
                "GBI format marker is corrupt or truncated"
            ) from error
        return False


def verify_archive(source, expected):
    """Independently read the complete stored archive against the pack result."""
    before = (
        _observation(Path(source).lstat())
        if isinstance(source, (str, os.PathLike))
        else None
    )
    if _scan(source, expected) is None:
        raise ArchiveError("stored file is not a marked GBI archive")
    if before is not None and _observation(Path(source).lstat()) != before:
        raise ArchiveError("stored archive changed during independent readback")
    return _public(expected)


def verify_source(source, manifest):
    """Reconcile the complete observed source tree, without reading file payloads."""
    source   = Path(source)
    evidence = manifest.get("_source")
    if not evidence:
        raise ArchiveError("source changed since packing; source kept")
    root_expected, observations = _rebased_evidence(evidence, source.lstat())
    if _observation(source.lstat()) != root_expected:
        raise ArchiveError("source changed since packing; source kept")
    current = {
        name: _observation(info)
        for name, info, _ in _walk(source, evidence.get("reserved", ()))
    }
    if current != observations:
        raise ArchiveError(
            "source inventory changed since packing; source kept"
        )


def _fresh_source_entries(manifest):
    """Return hashes from a caller-provided, freshly packed source manifest.

    Cleanup callers must pass the in-memory result of their current
    ``pack()`` call.  A persisted journal manifest is deliberately not
    supplied here; omitting this argument keeps the full source hash path.
    The caller's six-field observation remains the validity check at every
    cleanup read.
    """
    if manifest is None:
        return None
    evidence = manifest.get("_source")
    if not isinstance(evidence, dict) or not isinstance(
        evidence.get("observations"), dict
    ):
        raise ArchiveError("fresh source manifest has no source observations")
    entries = {}
    for entry in manifest.get("entries", ()):
        if isinstance(entry, dict) and entry.get("type") != "directory":
            entries[entry["path"]] = (
                entry,
                evidence["observations"].get(entry["path"]),
            )
    return entries


def _verify_source_content(
            root_fd, entry, before, allow_ctime_change = False,
            fresh_entries = None, verified_entries = None,
        ):
    with _parent(root_fd, entry["path"], create = False) as (parent, leaf):
        return _verify_source_content_at(
            parent, leaf, entry, before, allow_ctime_change,
            fresh_entries, verified_entries,
        )


def _verify_source_content_at(
            parent, leaf, entry, before, allow_ctime_change = False,
            fresh_entries = None, verified_entries = None,
        ):
    """``_verify_source_content`` for a leaf under an already-open parent."""
    if True:
        info        = os.stat(leaf, dir_fd = parent, follow_symlinks = False)
        observation = _observation(info)
        if (
            observation[:5] != before[:5]
            if allow_ctime_change
            else observation != before
        ):
            raise ArchiveError(
                "selected source changed before removal; source kept"
            )
        verified = (
            verified_entries.get(entry["path"])
            if verified_entries is not None
            else None
        )
        if (
            observation == before
            and verified is not None
            and verified[0] == entry
            and verified[1] == before
        ):
            return observation
        fresh = (
            fresh_entries.get(entry["path"])
            if fresh_entries is not None
            else None
        )
        if (
            observation == before
            and fresh is not None
            and fresh[1] == before
            and fresh[0].get("type") == entry.get("type")
            and fresh[0].get("size") == entry.get("size")
            and fresh[0].get("mode") == entry.get("mode")
            and fresh[0].get("mtime_ns") == entry.get("mtime_ns")
            and fresh[0].get("sha256") == entry.get("sha256")
            and fresh[0].get("target") == entry.get("target")
        ):
            return observation
        if entry["type"] == "symlink":
            digest = hashlib.sha256(
                os.fsencode(os.readlink(leaf, dir_fd = parent))
            ).hexdigest()
        else:
            fd = os.open(leaf, os.O_RDONLY | os.O_NOFOLLOW, dir_fd = parent)
            try:
                if _observation(os.fstat(fd)) != observation:
                    raise ArchiveError(
                        "source replaced before cleanup read; source kept"
                    )
                digest = _hash_fd(fd)
                if _observation(os.fstat(fd)) != observation:
                    raise ArchiveError(
                        "source changed during cleanup read; source kept"
                    )
            finally:
                os.close(fd)
        if digest != entry["sha256"]:
            raise ArchiveError(
                "source checksum changed since packing; source kept"
            )
        return observation


CLEANUP_WORKERS           = 1
DESTINATION_CHECK_SECONDS = 30.0


def _cleanup_workers(workers):
    if type(workers) is not int or workers < 1:
        raise ArchiveError("cleanup workers must be a positive integer")
    return workers


def _run_windowed(pool, function, items, *, window, on_tick = None, interval = None):
    """Run ``function(item)`` for every item with bounded submission; sum results.

    ``on_tick`` runs on the calling thread at least every ``interval`` seconds
    while work is outstanding. Any failure stops further submission, lets the
    calls already running finish, and is raised.
    """
    items   = iter(items)
    pending = set()
    total   = 0
    last    = time.monotonic()

    def submit_more():
        while len(pending) < window:
            try:
                item = next(items)
            except StopIteration:
                return False
            pending.add(pool.submit(function, item))
        return True

    more = submit_more()
    try:
        while pending:
            timeout = (
                None if interval is None
                else max(0.0, interval - (time.monotonic() - last))
            )
            done, _ = _wait_futures(
                pending, timeout = timeout, return_when = FIRST_EXCEPTION
            )
            for future in done:
                pending.discard(future)
                total += future.result()
            if on_tick is not None and (
                not done or time.monotonic() - last >= interval
            ):
                on_tick()
                last = time.monotonic()
            if more:
                more = submit_more()
    except BaseException:
        for future in pending:
            future.cancel()
        for future in pending:
            if not future.cancelled():
                try:
                    future.result()
                except BaseException:
                    pass
        raise
    return total


def _remove_selected_directories(
            root_fd, entries, evidence, check_destination, missing = (),
        ):
    """Remove emptied selected directories deepest first; keep nonempty ones."""
    import errno

    check_destination()
    for entry in sorted(
        (e for e in entries if e["type"] == "directory"),
        key = lambda e: e["path"].count("/"), reverse = True,
    ):
        if entry["path"] in missing:
            continue
        with _parent(root_fd, entry["path"], create = False) as (parent, name):
            now = os.stat(name, dir_fd = parent, follow_symlinks = False)
            if [now.st_dev, now.st_ino, now.st_mode] != evidence[
                entry["path"]
            ][:3]:
                raise ArchiveError(
                    "selected source directory changed; directory kept"
                )
            try:
                os.rmdir(name, dir_fd = parent)
            except OSError as error:
                if error.errno not in (errno.ENOTEMPTY, errno.EEXIST):
                    raise


def _unlink_groups(
            root_fd, groups, evidence, *, fresh_entries, verified_entries,
            removed_inodes, check_destination, on_remove, workers, interval,
            pool,
        ):
    """Unlink selected entries on ``workers`` threads, one group per call.

    A group is either every name of one hardlinked inode, unlinked in manifest
    order by a single call so the ctime change that unlinking a hardlink's
    other name causes is tolerated exactly as the sequential path tolerates
    it, or the single-name entries of one parent directory, whose descriptor
    is opened once for all of them. The destination check runs on the calling
    thread every ``interval`` seconds instead of before each unlink.
    Returns ``(count, freed_bytes)``.
    """
    lock  = threading.Lock()
    freed = [0]

    def record(entry):
        with lock:
            if entry["type"] in ("file", "hardlink"):
                freed[0] += entry["size"]
            if on_remove is not None:
                on_remove(entry)

    def unlink_at(parent, name, entry, before):
        now = os.stat(name, dir_fd = parent, follow_symlinks = False)
        if stat.S_ISDIR(now.st_mode) or _observation(now)[:5] != before[:5]:
            raise ArchiveError(
                "selected source changed before removal; remaining sources kept"
            )
        os.unlink(name, dir_fd = parent)

    def unlink_group(item):
        kind, group = item
        count = 0
        if kind == "directory":
            with _parent(root_fd, group[0]["path"], create = False) as (parent, _):
                for entry in group:
                    before = evidence[entry["path"]]
                    leaf   = entry["path"].rsplit("/", 1)[-1]
                    _verify_source_content_at(
                        parent, leaf, entry, before, fresh_entries = fresh_entries,
                        verified_entries = verified_entries,
                    )
                    unlink_at(parent, leaf, entry, before)
                    count += 1
                    record(entry)
            return count
        key      = tuple(evidence[group[0]["path"]][:2])
        unlinked = key in removed_inodes
        for entry in group:
            before = evidence[entry["path"]]
            # Later names of one inode share the payload just verified for its
            # first name; only their identity, size, mode and mtime are checked
            # below, since unlinking the first name changed their ctime.
            if not unlinked:
                _verify_source_content(
                    root_fd, entry, before, fresh_entries = fresh_entries,
                    verified_entries = verified_entries,
                )
            with _parent(root_fd, entry["path"], create = False) as (parent, name):
                unlink_at(parent, name, entry, before)
            unlinked = True
            count   += 1
            record(entry)
        return count

    count = _run_windowed(
        pool, unlink_group, groups, window = workers * 32,
        on_tick = check_destination, interval = interval,
    )
    return count, freed[0]


def _parent_of(path):
    return path.rsplit("/", 1)[0] if "/" in path else ""


def _cleanup_groups(entries, evidence):
    """Group entries for parallel cleanup: hardlinked inodes together, the
    rest per parent directory; order within a group is manifest order."""
    names = {}
    for entry in entries:
        names.setdefault(tuple(evidence[entry["path"]][:2]), []).append(entry)
    inode_groups = [("inode", group) for group in names.values() if len(group) > 1]
    directories  = {}
    for entry in entries:
        if len(names[tuple(evidence[entry["path"]][:2])]) == 1:
            directories.setdefault(_parent_of(entry["path"]), []).append(entry)
    return inode_groups + [("directory", group) for group in directories.values()]


def _parent_groups(items, path_of):
    """Group items by the parent directory of ``path_of(item)``, keeping order."""
    groups = {}
    for item in items:
        groups.setdefault(_parent_of(path_of(item)), []).append(item)
    return list(groups.values())


def _inode_groups(entries, evidence):
    return [group for _kind, group in _cleanup_groups(entries, evidence)]


def cleanup_source(
            source, manifest, stored_archive, destination_unchanged = None, *,
            on_remove = None, on_verified = None, verify_stored_archive = None,
            fresh_source_manifest = None, verified_archive = None,
            workers = CLEANUP_WORKERS,
            destination_check_interval = DESTINATION_CHECK_SECONDS,
        ):
    """Explicit move cleanup: full archive readback, stable tree, exact unlinks.

    ``workers`` above one verifies and unlinks on that many threads; the
    destination check then runs once before the first unlink, every
    ``destination_check_interval`` seconds while unlinking, and once after.

    Caller must retain its normal source/destination lock and immutable receipt.
    No recursive deletion; new or unselected entries are never removed.
    ``on_remove(entry)`` records each successful non-directory unlink immediately,
    so callers can account for a partial cleanup if a later check fails.
    ``on_verified()`` publishes the caller's receipt after full destination and
    source verification, before any unlink. A failed publication keeps sources.
    ``verify_stored_archive()`` is the caller's destination proof: either the
    complete ``verify_archive`` readback under its transport retry policy, or
    an equivalent proof the caller maintains (for example server-verified
    upload checksums plus a pinned-identity listing of every stored object).
    ``fresh_source_manifest`` is only the current in-memory ``pack`` result;
    journal resumes must leave it unset. With it, entries whose six-field
    observation still matches are not re-read.
    """
    workers = _cleanup_workers(workers)
    if destination_unchanged is None:
        if isinstance(stored_archive, (str, os.PathLike)):
            destination_before = _observation(Path(stored_archive).lstat())

            def destination_unchanged():
                if (
                    _observation(Path(stored_archive).lstat())
                    != destination_before
                ):
                    raise ArchiveError(
                        "stored archive changed before source removal; remaining sources kept"
                    )

        elif hasattr(stored_archive, "getvalue"):
            destination_digest = hashlib.sha256(
                stored_archive.getvalue()
            ).hexdigest()

            def destination_unchanged():
                if (
                    hashlib.sha256(stored_archive.getvalue()).hexdigest()
                    != destination_digest
                ):
                    raise ArchiveError(
                        "archive stream changed before source removal; sources kept"
                    )

        else:
            raise ArchiveError(
                "opaque archive cleanup requires a destination stability callback"
            )
    fresh_entries = _fresh_source_entries(fresh_source_manifest)
    force_supported = (
        "force" in inspect.signature(destination_unchanged).parameters
    )

    def check_destination(force = False):
        if force and force_supported:
            destination_unchanged(force = True)
        else:
            destination_unchanged()

    check_destination()
    if verified_archive is not None:
        if verify_stored_archive is not None or not isinstance(
            verified_archive, VerifiedArchiveEvidence
        ):
            raise ArchiveError(
                "archive verification evidence is ambiguous or invalid"
            )
        verified_archive.assert_manifest(manifest)
    elif verify_stored_archive is None:
        verify_archive(stored_archive, manifest)
    else:
        verify_stored_archive()
    check_destination()
    verify_source(source, manifest)
    source          = Path(source)
    removed         = 0
    evidence        = manifest["_source"]["observations"]
    unlinked_inodes = set()
    if workers > 1:
        files = [e for e in manifest["entries"] if e["type"] != "directory"]
        with (
            _directory(source) as root_fd,
            ThreadPoolExecutor(
                max_workers = workers, thread_name_prefix = "gbi-cleanup"
            ) as pool,
        ):
            # Verify EVERY selected entry before the first unlink, in parallel,
            # opening each parent directory once for all of its entries.
            def verify(group):
                with _parent(root_fd, group[0]["path"], create = False) as (parent, _):
                    for entry in group:
                        _verify_source_content_at(
                            parent, entry["path"].rsplit("/", 1)[-1], entry,
                            evidence[entry["path"]], fresh_entries = fresh_entries,
                        )
                return 0

            _run_windowed(
                pool, verify, _parent_groups(files, lambda e: e["path"]),
                window = workers * 32,
            )
            verify_source(source, manifest)
            check_destination()
            if on_verified is not None:
                on_verified()
            check_destination(force = True)
            removed, _freed = _unlink_groups(
                root_fd, _cleanup_groups(files, evidence), evidence,
                fresh_entries = fresh_entries, verified_entries = None,
                removed_inodes = set(), check_destination = check_destination,
                on_remove = on_remove, workers = workers,
                interval = destination_check_interval, pool = pool,
            )
            _remove_selected_directories(
                root_fd, manifest["entries"], evidence, check_destination
            )
            check_destination(force = True)
        return removed
    with _directory(source) as root_fd:
        # Read EVERY selected payload before the first unlink. A late corrupt
        # or changed entry cannot turn a failed preflight into a partial move.
        for entry in manifest["entries"]:
            if entry["type"] != "directory":
                _verify_source_content(
                    root_fd, entry, evidence[entry["path"]],
                    fresh_entries = fresh_entries,
                )
        verify_source(source, manifest)
        check_destination()
        if on_verified is not None:
            on_verified()
        check_destination(force = True)
        for entry in manifest["entries"]:
            if entry["type"] == "directory":
                continue
            before   = evidence[entry["path"]]
            identity = tuple(before[:2])
            _verify_source_content(
                root_fd, entry, before,
                allow_ctime_change = identity in unlinked_inodes,
                fresh_entries = fresh_entries,
            )
            with _parent(root_fd, entry["path"], create = False) as (
                parent, name,
            ):
                now    = os.stat(name, dir_fd = parent, follow_symlinks = False)
                before = evidence[entry["path"]]
                # Unlinking another name of a hardlinked inode changes ctime;
                # content, identity, size, mode and mtime must still match.
                if _observation(now)[:5] != before[:5]:
                    raise ArchiveError(
                        "selected source changed before removal; remaining sources kept"
                    )
                check_destination()
                os.unlink(name, dir_fd = parent)
                unlinked_inodes.add(identity)
                removed += 1
                if on_remove is not None:
                    on_remove(entry)
        for entry in sorted(
            manifest["entries"], key = lambda e: e["path"].count("/"),
            reverse = True,
        ):
            if entry["type"] != "directory":
                continue
            with _parent(root_fd, entry["path"], create = False) as (
                parent, name,
            ):
                now = os.stat(name, dir_fd = parent, follow_symlinks = False)
                if [now.st_dev, now.st_ino, now.st_mode] != evidence[
                    entry["path"]
                ][:3]:
                    raise ArchiveError(
                        "selected source directory changed; directory kept"
                    )
                check_destination()
                try:
                    os.rmdir(name, dir_fd = parent)
                except OSError as error:
                    import errno

                    if error.errno not in (errno.ENOTEMPTY, errno.EEXIST):
                        raise
        check_destination(force = True)
    return removed


def resume_cleanup_source(
            source, manifest, stored_archive, destination_unchanged = None, *,
            on_verified = None, verify_stored_archive = None,
            verified_archive = None, fresh_source_manifest = None,
            on_remove = None, workers = CLEANUP_WORKERS,
            destination_check_interval = DESTINATION_CHECK_SECONDS,
        ):
    """Resume exact cleanup from a verified archive journal.

    ``workers`` above one verifies and unlinks on that many threads with the
    destination check once before, periodically during, and once after the
    unlinks; ``on_remove(entry)`` records each successful unlink.

    Missing entries that were selected by the original manifest are treated as
    already removed.  Every remaining source entry is revalidated; additions,
    excluded-entry changes and replacements fail closed.  The optional
    ``fresh_source_manifest`` is the current in-memory ``pack`` result, never
    the persisted journal manifest. The return value counts only bytes
    unlinked by this invocation.
    """
    if destination_unchanged is None:
        if isinstance(stored_archive, (str, os.PathLike)):
            destination_before = _observation(Path(stored_archive).lstat())

            def destination_unchanged():
                if (
                    _observation(Path(stored_archive).lstat())
                    != destination_before
                ):
                    raise ArchiveError(
                        "stored archive changed before source removal; remaining sources kept"
                    )

        else:
            raise ArchiveError(
                "resumed archive cleanup requires a destination stability callback"
            )

    force_supported = (
        "force" in inspect.signature(destination_unchanged).parameters
    )

    def check_destination(force = False):
        if force and force_supported:
            destination_unchanged(force = True)
        else:
            destination_unchanged()

    source   = Path(source)
    evidence = manifest.get("_source")
    if not evidence or "observations" not in evidence:
        raise ArchiveError("archive has no resumable source observations")
    root_expected, expected = _rebased_evidence(evidence, source.lstat())
    selected_entries = {
        entry["path"]: entry
        for entry in manifest["entries"]
        if entry["type"] != "directory"
    }
    selected_directories = {
        entry["path"]
        for entry in manifest["entries"]
        if entry["type"] == "directory"
    }
    reserved = evidence.get("reserved", ())

    def inventory():
        current = {
            name: _observation(info)
            for name, info, _ in _walk(source, reserved)
        }
        if _observation(source.lstat())[:3] != root_expected[:3]:
            raise ArchiveError(
                "source root changed during resumed cleanup; remaining sources kept"
            )
        added = sorted(set(current) - set(expected))
        if added:
            raise ArchiveError(
                f"source path added during resumed cleanup: {added[0]}"
            )
        missing         = set(expected) - set(current)
        invalid_missing = missing - set(selected_entries) - selected_directories
        if invalid_missing:
            raise ArchiveError(
                f"unselected source path disappeared: {sorted(invalid_missing)[0]}"
            )
        return current, missing

    workers = _cleanup_workers(workers)
    fresh_entries = _fresh_source_entries(fresh_source_manifest) or {}
    current, missing = inventory()
    removed_inodes = {
        tuple(expected[name][:2])
        for name in missing
        if name in selected_entries
    }
    verified_entries = {}
    verified_lock    = threading.Lock()
    pool = (
        ThreadPoolExecutor(max_workers = workers, thread_name_prefix = "gbi-cleanup")
        if workers > 1 else None
    )

    def verify_selected(root_fd, name, observation):
        with _parent(root_fd, name, create = False) as (parent, leaf):
            return verify_selected_at(parent, leaf, name, observation)

    def verify_selected_at(parent, leaf, name, observation):
        entry  = selected_entries[name]
        before = expected[name]
        verified_observation = _verify_source_content_at(
            parent, leaf, entry, before,
            allow_ctime_change = tuple(observation[:2]) in removed_inodes,
            fresh_entries = fresh_entries,
        )
        if verified_observation == before:
            with verified_lock:
                verified_entries[name] = (entry, before)
        return 0

    with _directory(source) as root_fd:
        if pool is not None:
            def verify_group(group):
                with _parent(root_fd, group[0][0], create = False) as (parent, _):
                    for name, observation in group:
                        verify_selected_at(parent, name.rsplit("/", 1)[-1], name, observation)
                return 0

            _run_windowed(
                pool, verify_group,
                _parent_groups(
                    [
                        (name, observation)
                        for name, observation in current.items()
                        if name in selected_entries
                        and not stat.S_ISDIR(expected[name][2])
                    ],
                    lambda item: item[0],
                ),
                window = workers * 32,
            )
        for name, observation in current.items():
            before = expected[name]
            if name in selected_entries:
                if stat.S_ISDIR(before[2]):
                    if observation[:3] != before[:3]:
                        raise ArchiveError(
                            f"source directory changed during resumed cleanup: {name}"
                        )
                elif pool is None:
                    verify_selected(root_fd, name, observation)
            elif stat.S_ISDIR(before[2]):
                if observation[:3] != before[:3]:
                    raise ArchiveError(
                        f"source directory changed during resumed cleanup: {name}"
                    )
            elif (
                tuple(observation[:2]) in removed_inodes
                and observation[:5] == before[:5]
            ):
                pass  # unlinking a selected hardlink legitimately changes ctime
            elif observation != before:
                raise ArchiveError(
                    f"unselected source path changed during resumed cleanup: {name}"
                )

    check_destination()
    if verified_archive is not None:
        if verify_stored_archive is not None:
            raise ArchiveError("archive verification evidence is ambiguous")
        # The narrowly scoped recovery caller may supply a typed proof object
        # whose validator joins a fresh source manifest to an independently
        # verified native destination. Ordinary archive execution still uses
        # the full body readback above.
        if not isinstance(verified_archive, VerifiedArchiveEvidence):
            raise ArchiveError("invalid verified archive evidence")
        verified_archive.assert_manifest(manifest)
    elif verify_stored_archive is not None:
        verify_stored_archive()
    else:
        verify_archive(stored_archive, manifest)
    check_destination()
    if on_verified is not None:
        on_verified()
    check_destination(force = True)
    freed_bytes     = 0
    removed_count   = 0
    unlinked_inodes = set(removed_inodes)
    if pool is not None:
        remaining = [
            entry for entry in manifest["entries"]
            if entry["type"] != "directory" and entry["path"] not in missing
        ]
        with _directory(source) as root_fd, pool:
            removed_count, freed_bytes = _unlink_groups(
                root_fd, _cleanup_groups(remaining, expected), expected,
                fresh_entries = fresh_entries, verified_entries = verified_entries,
                removed_inodes = removed_inodes,
                check_destination = check_destination, on_remove = on_remove,
                workers = workers, interval = destination_check_interval,
                pool = pool,
            )
            _remove_selected_directories(
                root_fd, manifest["entries"], expected, check_destination,
                missing = missing,
            )
        inventory()
        check_destination(force = True)
        return {"removed": removed_count, "freed_bytes": freed_bytes}
    with _directory(source) as root_fd:
        for entry in manifest["entries"]:
            if entry["type"] == "directory" or entry["path"] in missing:
                continue
            before = expected[entry["path"]]
            _verify_source_content(
                root_fd, entry, before,
                allow_ctime_change = tuple(before[:2]) in unlinked_inodes,
                fresh_entries = fresh_entries,
                verified_entries = verified_entries,
            )
            with _parent(root_fd, entry["path"], create = False) as (
                parent, name,
            ):
                now = os.stat(name, dir_fd = parent, follow_symlinks = False)
                if (
                    stat.S_ISDIR(now.st_mode)
                    or _observation(now)[:5] != before[:5]
                ):
                    raise ArchiveError(
                        "selected source changed before resumed cleanup; source kept"
                    )
                check_destination()
                os.unlink(name, dir_fd = parent)
                unlinked_inodes.add(tuple(before[:2]))
                removed_count += 1
                if entry["type"] in ("file", "hardlink"):
                    freed_bytes += entry["size"]
                if on_remove is not None:
                    on_remove(entry)
        for entry in sorted(
            (e for e in manifest["entries"] if e["type"] == "directory"),
            key = lambda e: e["path"].count("/"), reverse = True,
        ):
            if entry["path"] in missing:
                continue
            before = expected[entry["path"]]
            with _parent(root_fd, entry["path"], create = False) as (
                parent, name,
            ):
                now = os.stat(name, dir_fd = parent, follow_symlinks = False)
                if [now.st_dev, now.st_ino, now.st_mode] != before[:3]:
                    raise ArchiveError(
                        "selected source directory changed during resumed cleanup; source kept"
                    )
                check_destination()
                try:
                    os.rmdir(name, dir_fd = parent)
                except OSError as error:
                    import errno

                    if error.errno not in (errno.ENOTEMPTY, errno.EEXIST):
                        raise
    inventory()
    check_destination(force = True)
    return {"removed": removed_count, "freed_bytes": freed_bytes}


@contextmanager
def _directory(path):
    path = Path(path)
    if path.resolve() != path.absolute():
        raise ArchiveError("destination/source directory has symlink parents")
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        yield fd
    finally:
        os.close(fd)


@contextmanager
def _parent(root_fd, relative, create = True):
    parts   = _relative(relative).split("/")
    current = os.dup(root_fd)
    try:
        for part in parts[:-1]:
            if create:
                try:
                    os.mkdir(part, 0o700, dir_fd = current)
                except FileExistsError:
                    pass
            child = os.open(
                part,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd = current,
            )
            os.close(current)
            current = child
        yield current, parts[-1]
    finally:
        os.close(current)


def _hash_fd(fd):
    result = hashlib.sha256()
    with os.fdopen(os.dup(fd), "rb") as stream:
        stream.seek(0)
        for block in iter(lambda: stream.read(BLOCK), b""):
            result.update(block)
    return result.hexdigest()


def restore(
            source, destination, includes = (), exclusions = (), phase = None,
            *, selection_mode = "basename",
        ):
    """Verify all members, restore selected entries, then independently verify.

    Native POSIX destinations only (Lustre/FSS), not Alluxio extraction. Existing
    files are reused only when content and representable metadata agree.
    Existing directory permissions are kept. No overwrite or archive deletion.
    A partial/interrupted
    restore keeps its archive and completed entries for an identical retry.
    """
    select = _selection(includes, exclusions, selection_mode)
    if phase:
        phase("verifying archive")
    manifest = inspect_archive(source)
    if manifest is None:
        raise ArchiveError(
            "ordinary unmarked archives are not automatically extracted"
        )
    all_entries = {entry["path"]: entry for entry in manifest["entries"]}
    selected    = {name for name, entry in all_entries.items() if select(name)}
    for name in list(selected):
        selected.update(
            str(p) for p in PurePosixPath(name).parents if str(p) != "."
        )
    # A selected hardlink needs its content even when its original name was
    # filtered out. Restore it as a regular file in that case, without creating
    # an unselected destination name.
    aliases = {}
    for name in selected:
        entry = all_entries[name]
        if entry["type"] == "hardlink":
            aliases.setdefault(entry["target"], []).append(name)
    destination = Path(destination)
    if not destination.exists():
        if destination.parent.resolve() != destination.parent.absolute():
            raise ArchiveError("destination has symlink parents")
        destination.mkdir(mode = 0o700)
    with _directory(destination) as root_fd:
        reused_directories = set()
        for name in selected:
            if all_entries[name]["type"] != "directory":
                continue
            try:
                with _parent(root_fd, name, create = False) as (parent, leaf):
                    info = os.stat(leaf, dir_fd = parent, follow_symlinks = False)
                    if not stat.S_ISDIR(info.st_mode):
                        raise ArchiveError(
                            "destination directory conflicts with existing entry"
                        )
                    reused_directories.add(name)
            except FileNotFoundError:
                pass

        def regular(name, reader, entry):
            with _parent(root_fd, name) as (parent, leaf):
                temporary = ".gbi-restore-" + uuid.uuid4().hex
                fd = os.open(
                    temporary,
                    os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW,
                    0o600, dir_fd = parent,
                )
                try:
                    with os.fdopen(os.dup(fd), "wb") as out:
                        while True:
                            data = reader.read(BLOCK)
                            if not data:
                                break
                            out.write(data)
                        out.flush()
                        os.fsync(out.fileno())
                    if _hash_fd(fd) != entry["sha256"]:
                        raise ArchiveError(
                            "restored content checksum mismatch; archive kept"
                        )
                    os.fchmod(fd, entry["mode"])
                    os.utime(fd, ns = (entry["mtime_ns"], entry["mtime_ns"]))
                    try:
                        os.link(
                            temporary, leaf, src_dir_fd = parent,
                            dst_dir_fd = parent, follow_symlinks = False,
                        )
                    except FileExistsError:
                        _verify_entry(root_fd, name, entry)
                finally:
                    os.close(fd)
                    os.unlink(temporary, dir_fd = parent)

        def consume(item, reader):
            name   = item["path"]
            wanted = name in selected
            entry  = all_entries.get(name)
            if entry is None:
                raise ArchiveError(
                    "archive changed between verification and restoration"
                )
            if item["type"] == "file":
                targets = ([name] if wanted else []) + aliases.get(name, [])
                if not targets:
                    return
                # Write one selected regular name; make remaining names links.
                regular(targets[0], reader, all_entries[targets[0]])
                for alias in targets[1:]:
                    _link(root_fd, targets[0], alias, all_entries[alias])
            elif wanted and item["type"] == "directory":
                with _parent(root_fd, name) as (parent, leaf):
                    try:
                        os.mkdir(leaf, 0o700, dir_fd = parent)
                    except FileExistsError:
                        if not stat.S_ISDIR(
                            os.stat(
                                leaf, dir_fd = parent, follow_symlinks = False
                            ).st_mode
                        ):
                            raise ArchiveError(
                                "destination directory conflicts with existing entry"
                            )
            elif wanted and item["type"] == "symlink":
                with _parent(root_fd, name) as (parent, leaf):
                    try:
                        os.symlink(entry["target"], leaf, dir_fd = parent)
                        os.utime(
                            leaf,
                            ns = (entry["mtime_ns"], entry["mtime_ns"]),
                            dir_fd = parent, follow_symlinks = False,
                        )
                    except FileExistsError:
                        _verify_entry(root_fd, name, entry)

        if phase:
            phase("extracting archive")
        _scan(source, manifest, consume)
        if phase:
            phase("verifying restored archive")
        for name in sorted(selected, key = lambda p: p.count("/"), reverse = True):
            entry = all_entries[name]
            if entry["type"] == "directory" and name not in reused_directories:
                with _parent(root_fd, name) as (parent, leaf):
                    fd = os.open(
                        leaf,
                        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                        dir_fd = parent,
                    )
                    try:
                        os.fchmod(fd, entry["mode"])
                        os.utime(fd, ns = (entry["mtime_ns"], entry["mtime_ns"]))
                    finally:
                        os.close(fd)
            _verify_entry(
                root_fd, name, entry,
                preserve_directory = name in reused_directories,
            )
    return {
        "selected": len(selected), "total": len(all_entries), "verified": True,
        "partial": len(selected) != len(all_entries),
        "manifest": manifest,
        "reused_directories": len(reused_directories),
        "metadata_limits": [
            "Ownership, ACLs, xattrs and privileged bits are not restored.",
            "Modification times may be truncated to destination precision "
            "(less than one second).",
            "Existing directory permissions are kept; additions can change "
            "their modification time.",
            "Symlink targets and modification times are restored; symlink "
            "modes are filesystem-defined.",
        ],
    }


def _link(root_fd, target, alias, entry):
    with _parent(root_fd, target) as (source_parent, source_leaf):
        with _parent(root_fd, alias) as (parent, leaf):
            try:
                os.link(
                    source_leaf, leaf, src_dir_fd = source_parent,
                    dst_dir_fd = parent, follow_symlinks = False,
                )
            except FileExistsError:
                _verify_entry(root_fd, alias, entry)
                original = os.stat(
                    source_leaf, dir_fd = source_parent, follow_symlinks = False
                )
                existing = os.stat(leaf, dir_fd = parent, follow_symlinks = False)
                if (original.st_dev, original.st_ino) != (
                    existing.st_dev, existing.st_ino,
                ):
                    raise ArchiveError(
                        "existing destination does not preserve the recorded "
                        "hardlink relationship"
                    )


def _verify_entry(root_fd, name, entry, preserve_directory = False):
    with _parent(root_fd, name, create = False) as (parent, leaf):
        info = os.stat(leaf, dir_fd = parent, follow_symlinks = False)
        kind = entry["type"]
        if kind == "symlink":
            okay = (
                stat.S_ISLNK(info.st_mode)
                and os.readlink(leaf, dir_fd = parent) == entry["target"]
            )
        elif kind == "directory":
            okay = stat.S_ISDIR(info.st_mode)
            if preserve_directory and okay:
                return
        else:
            okay = stat.S_ISREG(info.st_mode) and info.st_size == entry["size"]
            if okay:
                fd = os.open(leaf, os.O_RDONLY | os.O_NOFOLLOW, dir_fd = parent)
                try:
                    okay = _hash_fd(fd) == entry["sha256"] and _observation(
                        os.fstat(fd)
                    ) == _observation(info)
                finally:
                    os.close(fd)
        if kind != "symlink":
            okay = okay and stat.S_IMODE(info.st_mode) == entry["mode"]
        if (
            not okay
            or not 0 <= entry["mtime_ns"] - info.st_mtime_ns < 1_000_000_000
        ):
            raise ArchiveError(
                f"destination content or metadata conflict: {name!r}; archive kept"
            )
