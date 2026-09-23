"""Portable, marked tar archives. Payloads stream; extraction never follows links.

Streams may be seekable binary objects or zero-argument factories returning a
fresh binary reader. Factories allow chunked storage without staging a tar file.
The manifest is bounded to 64 MiB; payload size is not bounded by memory.
"""

from contextlib import contextmanager
import gzip
import fnmatch
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import stat
import tarfile
import uuid

from .selection import matches

FORMAT = "gbi-portable-archive"
VERSION = 1
MARKER = ".gbi-archive/format.json"
MANIFEST = ".gbi-archive/manifest.json"
MAX_MANIFEST = 64 * 1024 * 1024
BLOCK = 1024 * 1024


class ArchiveError(ValueError):
    """An archive or source cannot be verified safely."""


def _json(value):
    result = json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()
    if len(result) > MAX_MANIFEST:
        raise ArchiveError("archive manifest exceeds the 64 MiB supported limit")
    return result


def _relative(name):
    if (not isinstance(name, str) or not name or "\0" in name
            or name.startswith("/") or any(p in ("", ".", "..") for p in name.split("/"))
            or PurePosixPath(name).as_posix() != name):
        raise ArchiveError(f"unsafe archive path: {name!r}")
    return name


def _observation(info):
    return [info.st_dev, info.st_ino, info.st_mode, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns]


def _kind(info):
    for test, name in ((stat.S_ISREG, "file"), (stat.S_ISDIR, "directory"),
                       (stat.S_ISLNK, "symlink")):
        if test(info.st_mode):
            return name
    return "special"


def _reserved(name, patterns):
    return any(fnmatch.fnmatchcase(part, pattern) for part in name.split("/") for pattern in patterns)


def _walk(root, reserved=()):
    # No scandir recursion through a symlink. Directory identity is checked
    # before and after walking, and once more after the complete archive closes.
    def visit(directory, prefix=""):
        before = _observation(directory.lstat())
        if not stat.S_ISDIR(before[2]):
            raise ArchiveError("source directory changed into a non-directory")
        with os.scandir(directory) as children:
            for child in children:
                name = f"{prefix}/{child.name}" if prefix else child.name
                if _reserved(name, reserved):
                    continue
                info = child.stat(follow_symlinks=False)
                kind = _kind(info)
                yield name, info, kind
                if kind == "directory":
                    yield from visit(Path(child.path), name)
        if _observation(directory.lstat()) != before:
            raise ArchiveError("source directory changed during packing; source kept")
    yield from visit(root)


class _HashReader:
    def __init__(self, stream):
        self.stream = stream
        self.hash = hashlib.sha256()

    def read(self, size=-1):
        data = self.stream.read(size)
        self.hash.update(data)
        return data


def _add_json(archive, name, value):
    data = _json(value)
    member = tarfile.TarInfo(name)
    member.size, member.mode = len(data), 0o600
    archive.addfile(member, io.BytesIO(data))


def pack(source, output, compression=None, includes=(), exclusions=(), reserved_names=(), max_bytes=None):
    """Stream a directory into standard tar/tar+gzip; return cleanup observations.

    Output is caller-owned. Publish it only after success and independent
    ``verify_archive`` readback. This function never deletes sources.
    """
    if compression not in (None, "tar", "gzip", "tar.gz"):
        raise ArchiveError("compression must be tar or gzip")
    source = Path(source)
    if source.is_symlink() or not source.is_dir() or source.resolve() != source.absolute():
        raise ArchiveError("archive source must be a directory without symlink parents")
    root_before = _observation(source.lstat())
    reserved = (".gbi", ".prefect-*", "transfer-locks", *reserved_names)
    entries, observations, hardlinks = [], {}, {}
    directories = {}
    manifest_bytes, observation_bytes, selected_bytes = 0, 0, 0
    mode = "w|gz" if compression in ("gzip", "tar.gz") else "w|"
    with _directory(source) as root_fd, tarfile.open(fileobj=output, mode=mode, format=tarfile.PAX_FORMAT) as archive:
        _add_json(archive, MARKER, {"format": FORMAT, "version": VERSION})
        for name, info, kind in _walk(source, reserved):
            _relative(name)
            path = source / name
            observations[name] = _observation(info)
            observation_bytes += len(_json([name, observations[name]]))
            if observation_bytes > MAX_MANIFEST:
                raise ArchiveError("source observation inventory exceeds the 64 MiB supported limit")
            if kind == "directory":
                directories[name] = info
                continue
            if not matches(path.name, includes, exclusions):
                continue
            if kind == "special":
                raise ArchiveError(f"selected source is an unsupported special file: {name!r}")
            selected_bytes += info.st_size
            if max_bytes is not None and selected_bytes > max_bytes:
                raise ArchiveError("selected source exceeds the foreground byte budget; source kept; retry through Slurm")
            entry = {"path": name, "type": kind, "mode": stat.S_IMODE(info.st_mode) & 0o777,
                     "mtime_ns": info.st_mtime_ns, "size": info.st_size}
            member = tarfile.TarInfo("data/" + name)
            member.mode, member.mtime = entry["mode"], info.st_mtime_ns / 1_000_000_000
            member.pax_headers = {"mtime": str(info.st_mtime_ns // 1_000_000_000)
                                  + "." + f"{info.st_mtime_ns % 1_000_000_000:09d}"}
            if kind == "symlink":
                with _parent(root_fd, name, create=False) as (parent, leaf):
                    target = os.readlink(leaf, dir_fd=parent)
                entry.update(target=target, sha256=hashlib.sha256(os.fsencode(target)).hexdigest())
                member.type, member.linkname = tarfile.SYMTYPE, target
                archive.addfile(member)
            elif (info.st_dev, info.st_ino) in hardlinks:
                target = hardlinks[(info.st_dev, info.st_ino)]
                entry.update(type="hardlink", target=target["path"], sha256=target["sha256"])
                member.type, member.linkname = tarfile.LNKTYPE, "data/" + target["path"]
                archive.addfile(member)
            else:
                with _parent(root_fd, name, create=False) as (parent, leaf):
                    fd = os.open(leaf, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=parent)
                with os.fdopen(fd, "rb") as stream:
                    if _observation(os.fstat(stream.fileno())) != observations[name]:
                        raise ArchiveError("source changed before packing; source kept")
                    reader = _HashReader(stream)
                    member.size = info.st_size
                    archive.addfile(member, reader)
                    if (stream.read(1) or _observation(os.fstat(stream.fileno())) != observations[name]):
                        raise ArchiveError("source changed during packing; source kept")
                    entry["sha256"] = reader.hash.hexdigest()
                hardlinks[(info.st_dev, info.st_ino)] = entry
            if _observation(path.lstat()) != observations[name]:
                raise ArchiveError("source changed during packing; source kept")
            entries.append(entry)
            manifest_bytes += len(_json(entry)) + 1
            if manifest_bytes > MAX_MANIFEST:
                raise ArchiveError("archive manifest exceeds the 64 MiB supported limit")
        # Retain ancestors of selections and genuinely empty directories. Do
        # not add unrelated nonempty directories when filters select nothing.
        required = {str(parent) for e in entries for parent in PurePosixPath(e["path"]).parents
                    if str(parent) != "."}
        nonempty = {str(PurePosixPath(name).parent) for name in observations}
        required.update(name for name in directories
                        if name not in nonempty and matches(Path(name).name, includes, exclusions))
        for name in list(required):
            required.update(str(parent) for parent in PurePosixPath(name).parents if str(parent) != ".")
        for name, info in directories.items():
            if name not in required:
                continue
            entry = {"path": name, "type": "directory", "mode": stat.S_IMODE(info.st_mode) & 0o777,
                     "mtime_ns": info.st_mtime_ns, "size": 0}
            member = tarfile.TarInfo("data/" + name)
            member.type, member.mode, member.mtime = tarfile.DIRTYPE, entry["mode"], info.st_mtime
            member.pax_headers = {"mtime": str(info.st_mtime_ns // 1_000_000_000)
                                  + "." + f"{info.st_mtime_ns % 1_000_000_000:09d}"}
            archive.addfile(member)
            entries.append(entry)
        manifest = {"format": FORMAT, "version": VERSION, "entries": entries}
        _add_json(archive, MANIFEST, manifest)
    manifest["_source"] = {"root": root_before, "observations": observations, "reserved": list(reserved)}
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
        self.prefix, self.stream = prefix, stream

    def read(self, size=-1):
        if size < 0:
            value, self.prefix = self.prefix, b""
            return value + self.stream.read()
        value, self.prefix = self.prefix[:size], self.prefix[size:]
        return value + self.stream.read(size - len(value))


@contextmanager
def _tar_reader(source):
    with _reader(source) as stream:
        prefix = stream.read(2)
        wrapped = _PrefixReader(prefix, stream)
        decoded = gzip.GzipFile(fileobj=wrapped, mode="rb") if prefix == b"\x1f\x8b" else wrapped
        try:
            header = decoded.read(512)
            try:
                with tarfile.open(fileobj=_PrefixReader(header, decoded), mode="r|") as archive:
                    yield archive
            except tarfile.ReadError as error:
                if header[:100].split(b"\0", 1)[0] == MARKER.encode():
                    raise ArchiveError("GBI archive header is corrupt or truncated") from error
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
    return {key: value for key, value in manifest.items() if not key.startswith("_")}


def _scan(source, expected=None, consume=None):
    """Read and hash every member, including members excluded from restoration."""
    marked = False
    expected_entries = {entry["path"]: entry for entry in expected["entries"]} if expected else None

    def check_expected(item):
        if expected_entries is not None:
            saved = expected_entries.get(item["path"])
            if saved is None or any(saved.get(key) != value for key, value in item.items()):
                raise ArchiveError("archive member metadata or checksum changed since verification/packing")
    try:
        with _tar_reader(source) as archive:
            first = archive.next()
            if first is None or first.name != MARKER:
                return None
            marked = True
            if _read_json(archive, first) != {"format": FORMAT, "version": VERSION}:
                raise ArchiveError("unsupported GBI archive format or version")
            observed, manifest, manifest_bytes = {}, None, 0
            for member in archive:
                if member is first:
                    continue
                if manifest is not None:
                    raise ArchiveError("members follow the final archive manifest")
                if member.name == MANIFEST:
                    manifest = _read_json(archive, member)
                    continue
                if not member.name.startswith("data/"):
                    raise ArchiveError("unexpected archive member")
                name = _relative(member.name[5:])
                if _reserved(name, (".gbi", ".prefect-*", "transfer-locks")):
                    raise ArchiveError("archive member enters reserved transfer state")
                if name in observed or member.mode & ~0o777:
                    raise ArchiveError("duplicate member or privileged mode in archive")
                if member.sparse is not None or any(k.startswith("GNU.sparse") for k in member.pax_headers):
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
                    raise ArchiveError("non-file member contains unexpected payload")
                item = {"path": name, "type": kind, "mode": member.mode,
                        "mtime_ns": _mtime_ns(member), "size": member.size}
                if kind == "file":
                    reader = _HashReader(archive.extractfile(member))
                    check_expected(item)
                    if consume:
                        consume(item, reader)
                    while reader.read(BLOCK):
                        pass
                    item["sha256"] = reader.hash.hexdigest()
                elif kind == "symlink":
                    item.update(target=member.linkname, size=len(os.fsencode(member.linkname)),
                                sha256=hashlib.sha256(os.fsencode(member.linkname)).hexdigest())
                elif kind == "hardlink":
                    if not member.linkname.startswith("data/"):
                        raise ArchiveError("unsafe hardlink target")
                    target = _relative(member.linkname[5:])
                    prior = observed.get(target)
                    if prior is None or prior["type"] != "file":
                        raise ArchiveError("hardlink must refer to an earlier regular member")
                    item.update(target=target, size=prior["size"], sha256=prior["sha256"])
                elif member.size != 0:
                    raise ArchiveError("directory contains unexpected payload")
                check_expected(item)
                if kind != "file" and consume:
                    consume(item, None)
                observed[name] = item
                manifest_bytes += len(_json(item)) + 1
                if manifest_bytes > MAX_MANIFEST:
                    raise ArchiveError("archive manifest exceeds the 64 MiB supported limit")
            if manifest is None:
                raise ArchiveError("truncated archive: final manifest missing")
            if set(manifest) != {"format", "version", "entries"} or manifest.get("format") != FORMAT or manifest.get("version") != VERSION:
                raise ArchiveError("unsupported archive manifest format or version")
            entries = manifest.get("entries")
            if not isinstance(entries, list) or len(entries) != len(observed):
                raise ArchiveError("manifest does not match archive members")
            seen = set()
            for entry in entries:
                if (not isinstance(entry, dict) or not isinstance(entry.get("path"), str)
                        or entry["path"] in seen or observed.get(entry["path"]) != entry):
                    raise ArchiveError("archive member checksum or metadata differs from manifest")
                seen.add(entry["path"])
                for parent in PurePosixPath(entry["path"]).parents:
                    if str(parent) != "." and (str(parent) not in observed or observed[str(parent)]["type"] != "directory"):
                        raise ArchiveError("archive parent is absent or is not a directory")
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
                raise ArchiveError("stored archive does not match the packed source manifest")
            return manifest
    except (tarfile.TarError, EOFError, gzip.BadGzipFile, UnicodeError) as error:
        if not marked and isinstance(error, tarfile.ReadError):
            return None
        raise ArchiveError("archive is corrupt or truncated; source kept") from error


def _mtime_ns(member):
    from decimal import Decimal
    return int(Decimal(str(member.pax_headers.get("mtime", member.mtime))) * 1_000_000_000)


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
            prefix = stream.read(2)
            wrapped = _PrefixReader(prefix, stream)
            decoded = gzip.GzipFile(fileobj=wrapped, mode="rb") if prefix == b"\x1f\x8b" else wrapped
            try:
                header = decoded.read(512)
                claimed = header[:100].split(b"\0", 1)[0] == MARKER.encode()
                if not claimed:
                    return False
                member = tarfile.TarInfo.frombuf(header, "utf-8", "surrogateescape")
                if not member.isfile() or not 0 < member.size <= 4096:
                    raise ArchiveError("invalid GBI format marker")
                marker = json.loads(decoded.read(member.size))
                if marker != {"format": FORMAT, "version": VERSION}:
                    raise ArchiveError("unsupported GBI archive format or version")
                return True
            finally:
                if decoded is not wrapped:
                    decoded.close()
    except (tarfile.TarError, EOFError, gzip.BadGzipFile, UnicodeError, json.JSONDecodeError) as error:
        if claimed:
            raise ArchiveError("GBI format marker is corrupt or truncated") from error
        return False


def verify_archive(source, expected):
    """Independently read the complete stored archive against the pack result."""
    before = _observation(Path(source).lstat()) if isinstance(source, (str, os.PathLike)) else None
    if _scan(source, expected) is None:
        raise ArchiveError("stored file is not a marked GBI archive")
    if before is not None and _observation(Path(source).lstat()) != before:
        raise ArchiveError("stored archive changed during independent readback")
    return _public(expected)


def verify_source(source, manifest):
    """Reconcile the complete observed source tree, without reading file payloads."""
    source = Path(source)
    evidence = manifest.get("_source")
    if not evidence or _observation(source.lstat()) != evidence["root"]:
        raise ArchiveError("source changed since packing; source kept")
    current = {name: _observation(info) for name, info, _ in _walk(source, evidence.get("reserved", ()))}
    if current != evidence["observations"]:
        raise ArchiveError("source inventory changed since packing; source kept")


def _verify_source_content(root_fd, entry, before, allow_ctime_change=False):
    with _parent(root_fd, entry["path"], create=False) as (parent, leaf):
        info = os.stat(leaf, dir_fd=parent, follow_symlinks=False)
        observation = _observation(info)
        if (observation[:5] != before[:5] if allow_ctime_change else observation != before):
            raise ArchiveError("selected source changed before removal; source kept")
        if entry["type"] == "symlink":
            digest = hashlib.sha256(os.fsencode(os.readlink(leaf, dir_fd=parent))).hexdigest()
        else:
            fd = os.open(leaf, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=parent)
            try:
                if _observation(os.fstat(fd)) != observation:
                    raise ArchiveError("source replaced before cleanup read; source kept")
                digest = _hash_fd(fd)
                if _observation(os.fstat(fd)) != observation:
                    raise ArchiveError("source changed during cleanup read; source kept")
            finally:
                os.close(fd)
        if digest != entry["sha256"]:
            raise ArchiveError("source checksum changed since packing; source kept")


def cleanup_source(source, manifest, stored_archive, destination_unchanged=None):
    """Explicit move cleanup: full archive readback, stable tree, exact unlinks.

    Caller must retain its normal source/destination lock and immutable receipt.
    No recursive deletion; new or unselected entries are never removed.
    """
    if destination_unchanged is None:
        if isinstance(stored_archive, (str, os.PathLike)):
            destination_before = _observation(Path(stored_archive).lstat())

            def destination_unchanged():
                if _observation(Path(stored_archive).lstat()) != destination_before:
                    raise ArchiveError("stored archive changed before source removal; remaining sources kept")
        elif hasattr(stored_archive, "getvalue"):
            destination_digest = hashlib.sha256(stored_archive.getvalue()).hexdigest()

            def destination_unchanged():
                if hashlib.sha256(stored_archive.getvalue()).hexdigest() != destination_digest:
                    raise ArchiveError("archive stream changed before source removal; sources kept")
        else:
            raise ArchiveError("opaque archive cleanup requires a destination stability callback")
    destination_unchanged()
    verify_archive(stored_archive, manifest)
    destination_unchanged()
    verify_source(source, manifest)
    source = Path(source)
    removed = 0
    evidence = manifest["_source"]["observations"]
    unlinked_inodes = set()
    with _directory(source) as root_fd:
        # Read EVERY selected payload before the first unlink. A late corrupt
        # or changed entry cannot turn a failed preflight into a partial move.
        for entry in manifest["entries"]:
            if entry["type"] != "directory":
                _verify_source_content(root_fd, entry, evidence[entry["path"]])
        verify_source(source, manifest)
        for entry in manifest["entries"]:
            if entry["type"] == "directory":
                continue
            before = evidence[entry["path"]]
            identity = tuple(before[:2])
            _verify_source_content(root_fd, entry, before, allow_ctime_change=identity in unlinked_inodes)
            with _parent(root_fd, entry["path"], create=False) as (parent, name):
                now = os.stat(name, dir_fd=parent, follow_symlinks=False)
                before = evidence[entry["path"]]
                # Unlinking another name of a hardlinked inode changes ctime;
                # content, identity, size, mode and mtime must still match.
                if _observation(now)[:5] != before[:5]:
                    raise ArchiveError("selected source changed before removal; remaining sources kept")
                destination_unchanged()
                os.unlink(name, dir_fd=parent)
                unlinked_inodes.add(identity)
                removed += 1
        for entry in sorted(manifest["entries"], key=lambda e: e["path"].count("/"), reverse=True):
            if entry["type"] != "directory":
                continue
            with _parent(root_fd, entry["path"], create=False) as (parent, name):
                now = os.stat(name, dir_fd=parent, follow_symlinks=False)
                if [now.st_dev, now.st_ino, now.st_mode] != evidence[entry["path"]][:3]:
                    raise ArchiveError("selected source directory changed; directory kept")
                try:
                    os.rmdir(name, dir_fd=parent)
                except OSError as error:
                    import errno
                    if error.errno not in (errno.ENOTEMPTY, errno.EEXIST):
                        raise
    return removed


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
def _parent(root_fd, relative, create=True):
    parts = _relative(relative).split("/")
    current = os.dup(root_fd)
    try:
        for part in parts[:-1]:
            if create:
                try:
                    os.mkdir(part, 0o700, dir_fd=current)
                except FileExistsError:
                    pass
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=current)
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


def restore(source, destination, includes=(), exclusions=(), phase=None):
    """Verify all members, restore selected entries, then independently verify.

    Native POSIX destinations only (Lustre/FSS), not Alluxio extraction. Existing
    files are reused only when content and representable metadata agree.
    Existing directory permissions are kept. No overwrite or archive deletion.
    A partial/interrupted
    restore keeps its archive and completed entries for an identical retry.
    """
    if phase:
        phase("verifying archive")
    manifest = inspect_archive(source)
    if manifest is None:
        raise ArchiveError("ordinary unmarked archives are not automatically extracted")
    all_entries = {entry["path"]: entry for entry in manifest["entries"]}
    selected = {name for name, entry in all_entries.items()
                if matches(PurePosixPath(name).name, includes, exclusions)}
    for name in list(selected):
        selected.update(str(p) for p in PurePosixPath(name).parents if str(p) != ".")
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
        destination.mkdir(mode=0o700)
    with _directory(destination) as root_fd:
        reused_directories = set()
        for name in selected:
            if all_entries[name]["type"] != "directory":
                continue
            try:
                with _parent(root_fd, name, create=False) as (parent, leaf):
                    info = os.stat(leaf, dir_fd=parent, follow_symlinks=False)
                    if not stat.S_ISDIR(info.st_mode):
                        raise ArchiveError("destination directory conflicts with existing entry")
                    reused_directories.add(name)
            except FileNotFoundError:
                pass

        def regular(name, reader, entry):
            with _parent(root_fd, name) as (parent, leaf):
                temporary = ".gbi-restore-" + uuid.uuid4().hex
                fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW, 0o600, dir_fd=parent)
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
                        raise ArchiveError("restored content checksum mismatch; archive kept")
                    os.fchmod(fd, entry["mode"])
                    os.utime(fd, ns=(entry["mtime_ns"], entry["mtime_ns"]))
                    try:
                        os.link(temporary, leaf, src_dir_fd=parent, dst_dir_fd=parent, follow_symlinks=False)
                    except FileExistsError:
                        _verify_entry(root_fd, name, entry)
                finally:
                    os.close(fd)
                    os.unlink(temporary, dir_fd=parent)

        def consume(item, reader):
            name = item["path"]
            wanted = name in selected
            entry = all_entries.get(name)
            if entry is None:
                raise ArchiveError("archive changed between verification and restoration")
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
                        os.mkdir(leaf, 0o700, dir_fd=parent)
                    except FileExistsError:
                        if not stat.S_ISDIR(os.stat(leaf, dir_fd=parent, follow_symlinks=False).st_mode):
                            raise ArchiveError("destination directory conflicts with existing entry")
            elif wanted and item["type"] == "symlink":
                with _parent(root_fd, name) as (parent, leaf):
                    try:
                        os.symlink(entry["target"], leaf, dir_fd=parent)
                        os.utime(leaf, ns=(entry["mtime_ns"], entry["mtime_ns"]), dir_fd=parent, follow_symlinks=False)
                    except FileExistsError:
                        _verify_entry(root_fd, name, entry)

        if phase:
            phase("extracting archive")
        _scan(source, manifest, consume)
        if phase:
            phase("verifying restored archive")
        for name in sorted(selected, key=lambda p: p.count("/"), reverse=True):
            entry = all_entries[name]
            if entry["type"] == "directory" and name not in reused_directories:
                with _parent(root_fd, name) as (parent, leaf):
                    fd = os.open(leaf, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
                    try:
                        os.fchmod(fd, entry["mode"])
                        os.utime(fd, ns=(entry["mtime_ns"], entry["mtime_ns"]))
                    finally:
                        os.close(fd)
            _verify_entry(root_fd, name, entry, preserve_directory=name in reused_directories)
    return {"selected": len(selected), "total": len(all_entries), "verified": True,
            "partial": len(selected) != len(all_entries), "manifest": manifest,
            "reused_directories": len(reused_directories),
            "metadata_limits": ["Ownership, ACLs, xattrs and privileged bits are not restored.",
                                "Modification times may be truncated to destination precision (less than one second).",
                                "Existing directory permissions are kept; additions can change their modification time.",
                                "Symlink targets and modification times are restored; symlink modes are filesystem-defined."]}


def _link(root_fd, target, alias, entry):
    with _parent(root_fd, target) as (source_parent, source_leaf):
        with _parent(root_fd, alias) as (parent, leaf):
            try:
                os.link(source_leaf, leaf, src_dir_fd=source_parent, dst_dir_fd=parent, follow_symlinks=False)
            except FileExistsError:
                _verify_entry(root_fd, alias, entry)
                original = os.stat(source_leaf, dir_fd=source_parent, follow_symlinks=False)
                existing = os.stat(leaf, dir_fd=parent, follow_symlinks=False)
                if (original.st_dev, original.st_ino) != (existing.st_dev, existing.st_ino):
                    raise ArchiveError("existing destination does not preserve the recorded hardlink relationship")


def _verify_entry(root_fd, name, entry, preserve_directory=False):
    with _parent(root_fd, name, create=False) as (parent, leaf):
        info = os.stat(leaf, dir_fd=parent, follow_symlinks=False)
        kind = entry["type"]
        if kind == "symlink":
            okay = stat.S_ISLNK(info.st_mode) and os.readlink(leaf, dir_fd=parent) == entry["target"]
        elif kind == "directory":
            okay = stat.S_ISDIR(info.st_mode)
            if preserve_directory and okay:
                return
        else:
            okay = stat.S_ISREG(info.st_mode) and info.st_size == entry["size"]
            if okay:
                fd = os.open(leaf, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=parent)
                try:
                    okay = _hash_fd(fd) == entry["sha256"] and _observation(os.fstat(fd)) == _observation(info)
                finally:
                    os.close(fd)
        if kind != "symlink":
            okay = okay and stat.S_IMODE(info.st_mode) == entry["mode"]
        if not okay or not 0 <= entry["mtime_ns"] - info.st_mtime_ns < 1_000_000_000:
            raise ArchiveError(f"destination content or metadata conflict: {name!r}; archive kept")
