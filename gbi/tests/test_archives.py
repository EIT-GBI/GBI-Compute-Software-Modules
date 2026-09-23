"""Portable archive round trips and unsafe/corrupt input refusal."""

import io
import json
import os
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from gbi_data import archives


class Archives(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name).resolve()
        self.source = self.base / "source"
        self.source.mkdir()

    def fixture(self):
        (self.source / "empty").mkdir()
        (self.source / "nested").mkdir()
        (self.source / "nested" / "file\n odd.txt").write_bytes(b"portable\0payload" * 200)
        (self.source / "dangling").symlink_to("absent")
        (self.source / "relative").symlink_to("nested/file\n odd.txt")
        (self.source / "absolute").symlink_to("/outside/never-follow")
        os.link(self.source / "nested" / "file\n odd.txt", self.source / "alias")
        for path in (self.source / "empty", self.source / "nested", self.source / "alias"):
            os.chmod(path, 0o750 if path.is_dir() else 0o640)
            os.utime(path, ns=(1700000000123456789, 1700000000123456789))

    def packed(self, **kwargs):
        stream = io.BytesIO()
        manifest = archives.pack(self.source, stream, **kwargs)
        return stream, manifest

    def test_round_trip_tar_and_gzip(self):
        self.fixture()
        for compression in (None, "gzip"):
            with self.subTest(compression=compression):
                stream, packed = self.packed(compression=compression)
                archives.verify_archive(stream, packed)
                target = self.base / str(compression)
                result = archives.restore(stream, target)
                self.assertTrue(result["verified"])
                self.assertEqual((target / "alias").read_bytes(), (self.source / "alias").read_bytes())
                self.assertEqual((target / "alias").stat().st_ino, (target / "nested" / "file\n odd.txt").stat().st_ino)
                self.assertEqual(os.readlink(target / "absolute"), "/outside/never-follow")
                self.assertEqual((target / "empty").stat().st_mtime_ns, 1700000000123456789)
                archives.restore(stream, target)  # identical retry

    def test_filtered_hardlink_without_primary(self):
        self.fixture()
        stream, packed = self.packed()
        alias = next(e for e in packed["entries"] if e["type"] == "hardlink")
        target = self.base / "filtered"
        result = archives.restore(stream, target, includes=[Path(alias["path"]).name])
        self.assertEqual((target / alias["path"]).read_bytes(), (self.source / alias["path"]).read_bytes())
        self.assertFalse((target / alias["target"]).exists())
        self.assertTrue(result["partial"])

    def test_unmarked_tar_is_not_extracted(self):
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode="w") as tar:
            member = tarfile.TarInfo("ordinary")
            tar.addfile(member)
        self.assertIsNone(archives.inspect_archive(stream))
        with self.assertRaisesRegex(archives.ArchiveError, "unmarked"):
            archives.restore(stream, self.base / "target")
        self.assertFalse((self.base / "target").exists())

    def test_corrupt_payload_and_truncated_gzip_retain_sources(self):
        (self.source / "file").write_bytes(b"recognizable-payload")
        stream, manifest = self.packed()
        damaged = stream.getvalue().replace(b"recognizable-payload", b"Recognizable-payload")
        with self.assertRaisesRegex(archives.ArchiveError, "checksum"):
            archives.cleanup_source(self.source, manifest, io.BytesIO(damaged))
        self.assertTrue((self.source / "file").exists())
        gzip_stream, _ = self.packed(compression="gzip")
        with self.assertRaises(archives.ArchiveError):
            archives.inspect_archive(io.BytesIO(gzip_stream.getvalue()[:-8]))

    def test_stable_inventory_before_any_deletion(self):
        (self.source / "a").write_text("a")
        (self.source / "b").write_text("b")
        stream, manifest = self.packed()
        (self.source / "b").write_text("changed")
        with self.assertRaises(archives.ArchiveError):
            archives.cleanup_source(self.source, manifest, stream)
        self.assertTrue((self.source / "a").exists())

    def test_filtered_cleanup_keeps_unselected_and_root(self):
        (self.source / "yes.txt").write_text("yes")
        (self.source / "no.bin").write_text("no")
        stream, manifest = self.packed(includes=["*.txt"])
        self.assertEqual(archives.cleanup_source(self.source, manifest, stream), 1)
        self.assertTrue((self.source / "no.bin").exists())
        self.assertTrue(self.source.exists())

    def test_reserved_state_never_packed(self):
        for directory in (".gbi", ".prefect-receipts", "transfer-locks", "site-private"):
            (self.source / directory).mkdir()
            (self.source / directory / "secret").write_text("private")
        (self.source / "public").write_text("yes")
        _, manifest = self.packed(reserved_names=["site-private"])
        self.assertEqual([e["path"] for e in manifest["entries"]], ["public"])

    def test_excluded_special_file_does_not_block_selection(self):
        os.mkfifo(self.source / "pipe")
        (self.source / "wanted.txt").write_text("yes")
        _, manifest = self.packed(includes=["*.txt"])
        self.assertEqual([e["path"] for e in manifest["entries"]], ["wanted.txt"])
        with self.assertRaisesRegex(archives.ArchiveError, "special file"):
            self.packed()

    def test_existing_directory_permissions_kept(self):
        self.fixture()
        stream, _ = self.packed()
        target = self.base / "target"
        target.mkdir()
        (target / "nested").mkdir(mode=0o700)
        result = archives.restore(stream, target)
        self.assertEqual((target / "nested").stat().st_mode & 0o777, 0o700)
        self.assertEqual(result["reused_directories"], 1)

    def test_nested_empty_directory_ancestors_preserved(self):
        (self.source / "parent" / "empty").mkdir(parents=True)
        stream, _ = self.packed(includes=["empty"])
        archives.restore(stream, self.base / "target")
        self.assertTrue((self.base / "target" / "parent" / "empty").is_dir())

    def test_destination_whole_second_mtime_is_supported(self):
        self.fixture()
        stream, _ = self.packed()
        original = os.utime

        def whole_seconds(path, *args, **kwargs):
            kwargs["ns"] = tuple(value // 1_000_000_000 * 1_000_000_000 for value in kwargs["ns"])
            return original(path, *args, **kwargs)

        with patch.object(archives.os, "utime", side_effect=whole_seconds):
            self.assertTrue(archives.restore(stream, self.base / "seconds")["verified"])

    def test_changed_header_refused_before_restore_writes(self):
        (self.source / "a").write_text("source")
        stream, _ = self.packed()
        raw = stream.getvalue()
        changed = io.BytesIO()
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as before:
            with tarfile.open(fileobj=changed, mode="w", format=tarfile.PAX_FORMAT) as after:
                for member in before:
                    if member.name == "data/a":
                        member.mode = 0o777
                    after.addfile(member, before.extractfile(member) if member.isfile() else None)
        calls = 0

        def factory():
            nonlocal calls
            calls += 1
            return io.BytesIO(raw if calls == 1 else changed.getvalue())

        with self.assertRaisesRegex(archives.ArchiveError, "changed"):
            archives.restore(factory, self.base / "changed")
        self.assertFalse((self.base / "changed" / "a").exists())

    def test_cleanup_rehashes_all_before_any_unlink(self):
        (self.source / "a").write_text("a")
        (self.source / "b").write_text("b")
        stream, manifest = self.packed()
        original = archives._hash_fd
        calls = 0

        def wrong_late(fd):
            nonlocal calls
            calls += 1
            return "0" * 64 if calls == 2 else original(fd)

        with patch.object(archives, "_hash_fd", side_effect=wrong_late):
            with self.assertRaisesRegex(archives.ArchiveError, "checksum"):
                archives.cleanup_source(self.source, manifest, stream)
        self.assertTrue((self.source / "a").exists())
        self.assertTrue((self.source / "b").exists())

    def test_cleanup_checks_archive_after_each_source_unlink(self):
        (self.source / "a").write_text("a")
        (self.source / "b").write_text("b")
        stored = self.base / "stored.gbi.tar"
        with stored.open("wb") as output:
            manifest = archives.pack(self.source, output)
        original = os.unlink
        calls = 0

        def mutate_after_unlink(path, *args, **kwargs):
            nonlocal calls
            result = original(path, *args, **kwargs)
            calls += 1
            if calls == 1:
                stored.write_bytes(b"replaced")
            return result

        with patch.object(archives.os, "unlink", side_effect=mutate_after_unlink):
            with self.assertRaisesRegex(archives.ArchiveError, "archive changed"):
                archives.cleanup_source(self.source, manifest, stored)
        self.assertEqual(len(list(self.source.iterdir())), 1)

    def test_new_source_entry_retained(self):
        (self.source / "a").write_text("a")
        stream, manifest = self.packed()
        (self.source / "new").write_text("never selected")
        with self.assertRaises(archives.ArchiveError):
            archives.cleanup_source(self.source, manifest, stream)
        self.assertTrue((self.source / "a").exists())
        self.assertTrue((self.source / "new").exists())

    def test_hardlink_cleanup(self):
        self.fixture()
        stream, manifest = self.packed()
        self.assertEqual(archives.cleanup_source(self.source, manifest, stream), 5)
        self.assertEqual(list(self.source.iterdir()), [])

    def test_collision_keeps_existing_file_and_archive(self):
        (self.source / "a").write_text("source")
        stream, _ = self.packed()
        target = self.base / "target"
        target.mkdir()
        (target / "a").write_text("different")
        with self.assertRaisesRegex(archives.ArchiveError, "conflict"):
            archives.restore(stream, target)
        self.assertEqual((target / "a").read_text(), "different")
        self.assertTrue((self.source / "a").exists())

    def test_missing_end_records_refused(self):
        (self.source / "a").write_text("a")
        stream, _ = self.packed()
        raw = stream.getvalue()
        # Keep the first zero end record only, dropping the required second.
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as archive:
            last = archive.getmembers()[-1]
            end = last.offset_data + ((last.size + 511) // 512) * 512
        with self.assertRaisesRegex(archives.ArchiveError, "end records"):
            archives.inspect_archive(io.BytesIO(raw[:end + 512]))

    def test_corrupt_marked_header_is_not_treated_as_ordinary_file(self):
        stream, _ = self.packed()
        damaged = bytearray(stream.getvalue())
        damaged[100] = ord("7")
        with self.assertRaisesRegex(archives.ArchiveError, "header"):
            archives.inspect_archive(io.BytesIO(damaged))

    def test_destination_link_and_collision_refused(self):
        self.fixture()
        stream, _ = self.packed()
        outside = self.base / "outside"
        outside.mkdir()
        target = self.base / "target"
        target.mkdir()
        (target / "nested").symlink_to(outside)
        with self.assertRaises((archives.ArchiveError, OSError)):
            archives.restore(stream, target)
        self.assertEqual(list(outside.iterdir()), [])

    def test_bad_members_and_version_refused(self):
        for name, kind, link in (("data/../escape", tarfile.REGTYPE, ""),
                                 ("/absolute", tarfile.REGTYPE, ""),
                                 ("data/fifo", tarfile.FIFOTYPE, ""),
                                 ("data/link", tarfile.LNKTYPE, "../escape")):
            with self.subTest(name=name):
                stream = io.BytesIO()
                with tarfile.open(fileobj=stream, mode="w") as tar:
                    archives._add_json(tar, archives.MARKER, {"format": archives.FORMAT, "version": 1})
                    member = tarfile.TarInfo(name)
                    member.type, member.linkname = kind, link
                    tar.addfile(member)
                with self.assertRaises(archives.ArchiveError):
                    archives.inspect_archive(stream)
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode="w") as tar:
            archives._add_json(tar, archives.MARKER, {"format": archives.FORMAT, "version": 99})
        with self.assertRaisesRegex(archives.ArchiveError, "version"):
            archives.inspect_archive(stream)

    def test_stream_factory_readback(self):
        (self.source / "file").write_text("hello")
        stream, manifest = self.packed()
        factory = lambda: io.BytesIO(stream.getvalue())
        archives.verify_archive(factory, manifest)
        archives.restore(factory, self.base / "restored")

    def test_bounded_content_marker(self):
        (self.source / "large").write_bytes(bytes(2 * 1024 * 1024))
        for compression in (None, "gzip"):
            stream, _ = self.packed(compression=compression)
            self.assertTrue(archives.archive_marker(stream))
            if compression is None:
                self.assertLess(stream.tell(), 5000)
        for data in (b"\xff\xfe" * 500, b"\x1f\x8bBAD", b"ordinary"):
            self.assertFalse(archives.archive_marker(io.BytesIO(data)))


if __name__ == "__main__":
    unittest.main()
