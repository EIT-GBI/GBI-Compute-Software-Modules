"""Sequential, verified file chunks over ordinary filesystem paths.

A store represents one logical file. Only its versioned manifest identifies it
as GBI data; similarly named directories are never inferred to be chunk stores.
Helpers never delete the source or stage a payload on another filesystem.
"""

from contextlib import contextmanager
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import re
import stat
import time
import uuid

from .storage import fingerprint


FORMAT = "gbi-chunks"
VERSION = 1
BLOCK_SIZE = 1024 * 1024
DEFAULT_CHUNK_SIZE = 64 * BLOCK_SIZE
MAX_PARTS = 100000
MAX_MANIFEST_BYTES = 16 * BLOCK_SIZE


def _identity(info):
    return [info.st_dev, info.st_ino, stat.S_IFMT(info.st_mode)]


def _regular(path, flags=os.O_RDONLY):
    fd = os.open(path, flags | os.O_NOFOLLOW)
    info = os.fstat(fd)
    if not stat.S_ISREG(info.st_mode) or (flags != os.O_RDONLY and info.st_nlink != 1):
        os.close(fd)
        raise ValueError(f"expected a regular file with safe ownership: {path}")
    return fd


def _directory(path):
    if path.is_symlink() or not path.is_dir() or path.resolve() != path:
        raise ValueError(f"chunk directory is unavailable or contains a symlink: {path}")


def _save(path, value):
    """Atomic mutable state lives ONLY in caller-provided Lustre scratch."""
    temporary = path.with_name(uuid.uuid4().hex + ".json")
    with temporary.open("x") as output:
        json.dump(value, output, sort_keys=True)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _completion(manifest, encoded):
    return {"format": "gbi-chunks-complete", "version": VERSION,
            "manifest_sha256": hashlib.sha256(encoded).hexdigest(),
            "sha256": manifest["sha256"], "size": manifest["size"],
            "parts": len(manifest["parts"])}


def read_manifest(store, *, complete=True):
    """Recognize and validate a manifest without interpreting arbitrary files."""
    store = Path(store).absolute()
    _directory(store)
    try:
        with os.fdopen(_regular(store / "manifest.json"), "rb") as source:
            encoded = source.read(MAX_MANIFEST_BYTES + 1)
            if len(encoded) > MAX_MANIFEST_BYTES:
                raise ValueError("chunk manifest exceeds the supported 16 MiB limit")
            manifest = json.loads(encoded)
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"not a readable GBI chunk manifest: {store}") from error
    try:
        if manifest["format"] != FORMAT or manifest["version"] != VERSION:
            raise ValueError("unsupported chunk format/version")
        name = manifest["original_name"]
        if not isinstance(name, str) or name in ("", ".", "..") or Path(name).name != name:
            raise ValueError("invalid original filename")
        size, chunk_size = manifest["size"], manifest["chunk_size"]
        if type(size) is not int or size < 0 or type(chunk_size) is not int or chunk_size < 1:
            raise ValueError("invalid chunk sizes")
        if not re.fullmatch("[0-9a-f]{64}", manifest["sha256"]):
            raise ValueError("invalid full checksum")
        if (not isinstance(manifest["source"], str) or
                len(manifest["source_fingerprint"]) != 5):
            raise ValueError("invalid source identity")
        if len(manifest["parts"]) > MAX_PARTS:
            raise ValueError("too many chunks; use a larger chunk size")
        if len(manifest["parts"]) != (size + chunk_size - 1) // chunk_size:
            raise ValueError("invalid part count")
        for index, part in enumerate(manifest["parts"]):
            if (part["name"] != f"{index:08d}.part" or
                    part["size"] != min(chunk_size, size - index * chunk_size) or
                    not re.fullmatch("[0-9a-f]{64}", part["sha256"])):
                raise ValueError("invalid chunk order, size or checksum")
        _directory(store / ".gbi")
        _directory(store / ".gbi" / "parts")
    except (KeyError, TypeError, AttributeError) as error:
        raise ValueError("not a valid GBI chunk manifest") from error
    manifest["state"] = "incomplete"
    marker = store / ".gbi" / "complete.json"
    if marker.exists() or marker.is_symlink():
        try:
            with os.fdopen(_regular(marker), "rb") as stream:
                content = stream.read(4097)
                if len(content) > 4096 or json.loads(content) != _completion(manifest, encoded):
                    raise ValueError("chunk completion marker does not match manifest")
            manifest["state"] = "complete"
        except (OSError, json.JSONDecodeError) as error:
            if complete:
                raise ValueError("chunk completion marker is unreadable or incomplete") from error
    if complete and manifest["state"] != "complete":
        raise ValueError("chunk transfer is incomplete; resume it before restoring")
    return manifest


@contextmanager
def _locked(state, store):
    _directory(state)
    if state == store or store in state.parents:
        raise ValueError("chunk state must be on Lustre scratch, outside the destination")
    root = state / "chunks"
    for path in (root, root / "locks", root / "pending"):
        path.mkdir(mode=0o700, exist_ok=True)
        _directory(path)
    key = hashlib.sha256(os.fsencode(store)).hexdigest()
    fd = os.open(root / "locks" / key[:3], os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode) or os.fstat(fd).st_nlink != 1:
            raise ValueError("chunk scratch lock is not a private regular file")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError("chunk transfer is already in use; retry after it finishes") from error
        yield root / "pending" / f"{key}.json"
    finally:
        os.close(fd)


def _hash(source, limit=None, combined=None):
    result = hashlib.sha256()
    size = 0
    while limit is None or size < limit:
        block = source.read(BLOCK_SIZE if limit is None else min(BLOCK_SIZE, limit - size))
        if not block:
            break
        result.update(block)
        if combined is not None:
            combined.update(block)
        size += len(block)
    return result.hexdigest(), size


def _plan(source, chunk_size):
    source.seek(0)
    total = os.fstat(source.fileno()).st_size
    if (total + chunk_size - 1) // chunk_size > MAX_PARTS:
        raise ValueError("too many chunks; use a larger chunk size (at most 100000 parts)")
    combined = hashlib.sha256()
    parts = []
    for index, offset in enumerate(range(0, total, chunk_size)):
        digest, size = _hash(source, min(chunk_size, total - offset), combined)
        if size != min(chunk_size, total - offset):
            raise ValueError("source changed while planning chunks")
        parts.append({"name": f"{index:08d}.part", "size": size, "sha256": digest})
    return parts, combined.hexdigest()


def _remove_owned(path, journal, journal_path):
    _directory(path.parent)
    if path.exists() or path.is_symlink():
        info = path.lstat()
        if (_identity(info) != journal["owned"].get(str(path)) or
                not stat.S_ISREG(info.st_mode) or info.st_nlink != 1):
            raise ValueError(f"unowned chunk collision; existing file retained: {path}")
        path.unlink()
        journal["owned"].pop(str(path), None)
        _save(journal_path, journal)


def _write_part(source, path, part, journal, journal_path):
    # Alluxio supports exclusive create/unlink; never truncate or rename a
    # payload. Only the inode retained in Lustre's ownership journal is ours.
    _remove_owned(path, journal, journal_path)
    _directory(path.parent)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as output:
        journal["owned"][str(path)] = _identity(os.fstat(output.fileno()))
        # Persist ownership BEFORE the first payload write so interruption can
        # safely retry this inode. A crash before this record fails closed.
        _save(journal_path, journal)
        remaining = part["size"]
        digest = hashlib.sha256()
        while remaining:
            block = source.read(min(BLOCK_SIZE, remaining))
            if not block:
                raise ValueError("source changed while writing chunks")
            output.write(block)
            digest.update(block)
            remaining -= len(block)
        output.flush()
        # Closing the exclusively-created inode publishes it on Alluxio;
        # independent readback below is the acceptance condition there.
        if digest.hexdigest() != part["sha256"]:
            raise ValueError("source changed while writing chunks")


def _verify(path, part, *, settle_seconds=0, combined=None):
    deadline = time.monotonic() + settle_seconds
    while True:
        candidate = combined.copy() if combined is not None else None
        try:
            _part_digest(path, part, candidate)
            return candidate
        except (OSError, ValueError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(min(1, max(0, deadline - time.monotonic())))


def _remote_json(path, value, journal, journal_path, settle_seconds):
    content = _encoded(value)
    expected = {"size": len(content), "sha256": hashlib.sha256(content).hexdigest()}
    if path.exists() or path.is_symlink():
        try:
            _verify(path, expected)
            return
        except (OSError, ValueError):
            pass
    _write_part(io.BytesIO(content), path, expected, journal, journal_path)
    _verify(path, expected, settle_seconds=settle_seconds)


def _part_digest(path, part, combined=None):
    with os.fdopen(_regular(path), "rb") as source:
        before = os.fstat(source.fileno())
        if before.st_size != part["size"]:
            raise ValueError(f"corrupt chunk size: {path}")
        digest, size = _hash(source, combined=combined)
        if (size != part["size"] or digest != part["sha256"] or
                _identity(before) != _identity(path.lstat()) or
                before.st_mtime_ns != path.lstat().st_mtime_ns):
            raise ValueError(f"missing, changed or corrupt chunk: {path}")
    return digest


def write_chunks(source, store, *, state, chunk_size=DEFAULT_CHUNK_SIZE,
                 progress=None, settle_seconds=0):
    """Write/resume one unchanged regular file; report verified part progress.

    ``progress`` receives ``parts``, ``bytes``, ``total_parts``, ``total_bytes``
    and ``reused`` after each independently read-back part. One part at a time.
    ``state`` is an existing caller-owned Lustre directory for locks/journals;
    no mutable state, rename, flock or truncate is used on the destination.
    """
    source, store, state = Path(source).absolute(), Path(store).absolute(), Path(state).absolute()
    if type(chunk_size) is not int or chunk_size < 1:
        raise ValueError("chunk size must be positive")
    if settle_seconds < 0:
        raise ValueError("verification wait must not be negative")
    _directory(source.parent)
    _directory(store.parent)
    before = fingerprint(source)
    with _locked(state, store) as journal_path, os.fdopen(_regular(source), "rb") as input_file:
        if _identity(os.fstat(input_file.fileno())) != before[:3]:
            raise ValueError("source changed before chunk planning")
        parts, digest = _plan(input_file, chunk_size)
        if fingerprint(source) != before:
            raise ValueError("source changed while planning chunks")
        manifest = {"format": FORMAT, "version": VERSION,
                    "original_name": source.name, "source": str(source),
                    "source_fingerprint": before, "size": before[3],
                    "chunk_size": chunk_size, "sha256": digest, "parts": parts}
        encoded = _encoded(manifest)
        if len(encoded) > MAX_MANIFEST_BYTES:
            raise ValueError("chunk manifest exceeds 16 MiB; use a larger chunk size")
        journal = {"store": str(store), "manifest_sha256": hashlib.sha256(encoded).hexdigest(),
                   "owned": {}, "store_identity": None}
        if journal_path.exists():
            with os.fdopen(_regular(journal_path), "r") as saved:
                journal = json.load(saved)
            if (journal.get("store") != str(store) or
                    journal.get("manifest_sha256") != hashlib.sha256(encoded).hexdigest()):
                raise ValueError("source or chunk format changed; existing chunks retained")
        if not store.exists() and not store.is_symlink():
            store.mkdir(mode=0o700)
            journal["store_identity"] = _identity(store.lstat())
            _save(journal_path, journal)
        else:
            _directory(store)
            if _identity(store.lstat()) != journal.get("store_identity"):
                saved = read_manifest(store, complete=False)
                saved.pop("state")
                if saved != manifest:
                    raise ValueError("source or chunk format changed; existing chunks retained")
        if _identity(store.lstat()) == journal.get("store_identity"):
            # Only our freshly-created or journal-owned namespace may be
            # completed after interruption; unrelated directories are refused.
            for directory in (store / ".gbi", store / ".gbi" / "parts"):
                directory.mkdir(mode=0o700, exist_ok=True)
                _directory(directory)
        marker = store / ".gbi" / "complete.json"
        _remote_json(store / "manifest.json", manifest, journal, journal_path, settle_seconds)
        combined = hashlib.sha256()
        verified_bytes = 0
        verified_parts = []
        for index, part in enumerate(manifest["parts"]):
            path = store / ".gbi" / "parts" / part["name"]
            reused = False
            if path.exists() or path.is_symlink():
                try:
                    combined = _verify(path, part, combined=combined)
                    reused = True
                except (OSError, ValueError):
                    pass
            if not reused:
                # Completion is invalidated before any owned part repair.
                # A foreign marker cannot be removed even if its parts fail.
                _remove_owned(marker, journal, journal_path)
                input_file.seek(index * chunk_size)
                _write_part(input_file, path, part, journal, journal_path)
                combined = _verify(path, part, settle_seconds=settle_seconds, combined=combined)
            verified_parts.append((path, fingerprint(path)))
            verified_bytes += part["size"]
            if progress:
                progress({"parts": index + 1, "bytes": verified_bytes,
                          "total_parts": len(manifest["parts"]),
                          "total_bytes": manifest["size"], "reused": reused})
        input_file.seek(0)
        final_digest, final_size = _hash(input_file)
        if (fingerprint(source) != before or final_digest != digest or
                final_size != before[3] or combined.hexdigest() != digest):
            raise ValueError("source changed or full chunk checksum differs; source retained")
        if any(fingerprint(path) != identity for path, identity in verified_parts):
            raise ValueError("verified chunk changed before completion; source retained")
        _remote_json(marker, _completion(manifest, encoded), journal, journal_path, settle_seconds)
        return {**manifest, "state": "complete"}


class _ChunkReader(io.RawIOBase):
    def __init__(self, store, manifest):
        super().__init__()
        self.store, self.manifest = store, manifest
        self.index = 0
        self.part = None
        self.part_hash = hashlib.sha256()
        self.part_size = 0
        self.full_hash = hashlib.sha256()
        self.verified = False

    def readable(self):
        return True

    def readinto(self, buffer):
        self._checkClosed()
        if not buffer:
            return 0
        while True:
            if self.part is None:
                if self.index == len(self.manifest["parts"]):
                    if self.full_hash.hexdigest() != self.manifest["sha256"]:
                        raise ValueError("full reconstructed checksum differs")
                    self.verified = True
                    return 0
                path = self.store / ".gbi" / "parts" / self.manifest["parts"][self.index]["name"]
                try:
                    self.part = os.fdopen(_regular(path), "rb")
                except OSError as error:
                    raise ValueError(f"missing or unreadable chunk: {path}") from error
                self.part_hash = hashlib.sha256()
                self.part_size = 0
            size = self.part.readinto(buffer)
            if size:
                block = memoryview(buffer)[:size]
                self.part_hash.update(block)
                self.full_hash.update(block)
                self.part_size += size
                if self.part_size > self.manifest["parts"][self.index]["size"]:
                    raise ValueError(f"corrupt chunk size at part {self.index}")
                return size
            expected = self.manifest["parts"][self.index]
            self.part.close()
            self.part = None
            if self.part_size != expected["size"] or self.part_hash.hexdigest() != expected["sha256"]:
                raise ValueError(f"corrupt chunk at part {self.index}")
            self.index += 1

    def close(self):
        if self.part is not None:
            self.part.close()
        super().close()


@contextmanager
def open_chunks(store):
    """Yield a binary reader, authenticating every part and the entire stream.

    On normal exit, drain unread bytes so archive readers that stop at their
    own end marker cannot accidentally skip full-payload verification.
    """
    store = Path(store).absolute()
    manifest = read_manifest(store)
    marker = store / ".gbi" / "complete.json"
    before = fingerprint(marker)
    raw = _ChunkReader(store, manifest)
    with io.BufferedReader(raw, BLOCK_SIZE) as reader:
        reader.manifest = manifest
        yield reader
        while reader.read(BLOCK_SIZE):
            pass
        if fingerprint(marker) != before or read_manifest(store) != manifest:
            raise ValueError("chunk completion changed during reconstruction")


def restore_chunks(store, target, *, progress=None):
    """Restore to an exclusive output, deleting only our failed output inode."""
    target = Path(target).absolute()
    _directory(target.parent)
    created_identity = None
    try:
        with open_chunks(store) as source:
            _directory(target.parent)
            fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            created_identity = _identity(os.fstat(fd))
            with os.fdopen(fd, "wb") as output:
                size = 0
                for block in iter(lambda: source.read(BLOCK_SIZE), b""):
                    output.write(block)
                    size += len(block)
                    if progress:
                        progress({"bytes": size, "total_bytes": source.manifest["size"]})
                output.flush()
                os.fsync(output.fileno())
            target_before = fingerprint(target)
            with os.fdopen(_regular(target), "rb") as result:
                digest, size = _hash(result)
            if (digest != source.manifest["sha256"] or size != source.manifest["size"] or
                    _identity(target.lstat()) != created_identity or
                    fingerprint(target) != target_before):
                raise ValueError("restored file checksum or identity differs")
            return {"bytes": size, "sha256": digest, "parts": len(source.manifest["parts"]),
                    "target_fingerprint": target_before}
    except BaseException:
        if created_identity is not None and target.exists() and _identity(target.lstat()) == created_identity:
            _directory(target.parent)
            target.unlink()
        raise
