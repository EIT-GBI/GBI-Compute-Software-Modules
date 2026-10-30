"""Parallel packing and cleanup: identical bytes, single reads, fail-closed."""

import hashlib
import io
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from gbi_data import archives


class ParallelArchives(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base   = Path(self.temporary.name).resolve()
        self.source = self.base / "source"
        self.source.mkdir()

    def fixture(self, files = 300):
        for index in range(files):
            directory = self.source / f"dir{index % 7}" / f"sub{index % 3}"
            directory.mkdir(parents = True, exist_ok = True)
            (directory / f"file{index:04d}.dat").write_bytes(
                hashlib.sha256(str(index).encode()).digest() * (index % 50 + 1)
            )
        (self.source / "empty").mkdir()
        (self.source / "large.bin").write_bytes(os.urandom(2 * 1024 * 1024))
        (self.source / "link").symlink_to("dir1/sub1")
        (self.source / "dangling").symlink_to("absent")
        os.link(self.source / "large.bin", self.source / "dir2" / "large-alias.bin")
        os.link(
            self.source / "dir0" / "sub0" / "file0000.dat",
            self.source / "dir6" / "first-alias.dat",
        )
        (self.source / "odd name\twith tab").write_bytes(b"x" * 10)

    def pack(self, **options):
        output   = io.BytesIO()
        manifest = archives.pack(self.source, output, **options)
        return output.getvalue(), manifest

    def test_parallel_pack_bytes_and_manifest_match_sequential(self):
        self.fixture()
        for compression in (None, "gzip"):
            sequential, manifest_one = self.pack(
                compression = compression, readers = 1, inline_bytes = 0,
            )
            parallel, manifest_many = self.pack(
                compression = compression, readers = 16,
                inline_bytes = 1024 * 1024,
            )
            if compression is None:
                self.assertEqual(sequential, parallel)
            self.assertEqual(
                archives._public(manifest_one), archives._public(manifest_many)
            )
            self.assertEqual(manifest_one["_source"], manifest_many["_source"])
            archives.verify_archive(io.BytesIO(parallel), manifest_many)
            kinds = {e["type"] for e in manifest_many["entries"]}
            self.assertEqual(
                kinds, {"file", "directory", "symlink", "hardlink"}
            )
            self.assertEqual(
                sum(e["type"] == "hardlink" for e in manifest_many["entries"]), 2
            )

    def test_parallel_pack_reads_every_file_exactly_once(self):
        self.fixture(files = 120)
        opened = []
        original = archives._read_entry

        def counting(root_fd, name, observation):
            opened.append(name)
            return original(root_fd, name, observation)

        with patch.object(archives, "_read_entry", counting):
            _payload, manifest = self.pack(readers = 8, inline_bytes = 1 << 20)
        files = [
            e["path"] for e in manifest["entries"]
            if e["type"] == "file" and e["size"] <= 1 << 20
        ]
        self.assertEqual(sorted(opened), sorted(files))
        self.assertTrue(any(
            e["type"] == "file" and e["size"] > 1 << 20
            for e in manifest["entries"]
        ))
        self.assertEqual(len(opened), len(set(opened)))

    def test_restore_of_parallel_archive_round_trips(self):
        self.fixture(files = 60)
        payload, _manifest = self.pack(readers = 6)
        archives.restore(io.BytesIO(payload), self.base / "restored")
        for relative in ("dir0/sub0/file0000.dat", "large.bin", "odd name\twith tab"):
            self.assertEqual(
                (self.base / "restored" / relative).read_bytes(),
                (self.source / relative).read_bytes(),
            )
        self.assertEqual(
            os.readlink(self.base / "restored" / "link"), "dir1/sub1"
        )
        self.assertTrue((self.base / "restored" / "empty").is_dir())

    def test_source_change_during_prefetch_fails_closed(self):
        self.fixture(files = 40)
        victim = self.source / "dir3" / "sub0" / "file0003.dat"
        original = archives._read_entry

        def mutate_then_read(root_fd, name, observation):
            if name.endswith("file0003.dat"):
                victim.write_bytes(b"changed after the walk observed it")
            return original(root_fd, name, observation)

        with patch.object(archives, "_read_entry", mutate_then_read):
            with self.assertRaises(archives.ArchiveError) as caught:
                self.pack(readers = 4, inline_bytes = 1 << 20)
        self.assertIn("source changed", str(caught.exception))

    def test_reader_failure_propagates_and_stops_the_pool(self):
        self.fixture(files = 200)
        calls = []
        original = archives._read_entry

        def failing(root_fd, name, observation):
            calls.append(name)
            if name.endswith("file0010.dat"):
                raise OSError(5, "simulated read error")
            return original(root_fd, name, observation)

        with patch.object(archives, "_read_entry", failing):
            with self.assertRaises(OSError):
                self.pack(readers = 4, inline_bytes = 1 << 20)
        self.assertTrue(any(name.endswith("file0010.dat") for name in calls))

    def test_invalid_reader_settings_are_refused(self):
        self.fixture(files = 10)
        with self.assertRaises(archives.ArchiveError):
            self.pack(readers = 0)
        with self.assertRaises(archives.ArchiveError):
            self.pack(prefetch_bytes = -1)
        with patch.dict(os.environ, {"GBI_PACK_READERS": "3"}):
            self.assertEqual(archives._pack_readers(None), 3)
        with patch.dict(os.environ, {"GBI_PACK_READERS": "zero"}):
            self.assertEqual(archives._pack_readers(None), archives.PACK_READERS)

    def cleanup_fixture(self):
        self.fixture(files = 150)
        payload, manifest = self.pack(readers = 8)
        return io.BytesIO(payload), manifest

    def test_parallel_cleanup_unlinks_without_rereading_fresh_entries(self):
        stored, manifest = self.cleanup_fixture()
        removed = []
        checks  = []

        def destination_unchanged(force = False):
            checks.append(force)

        with patch.object(
            archives, "_hash_fd",
            side_effect = AssertionError("content re-read"),
        ):
            count = archives.cleanup_source(
                self.source, manifest, stored, destination_unchanged,
                on_remove = removed.append, fresh_source_manifest = manifest,
                workers = 8, verify_stored_archive = lambda: None,
            )
        files = [e for e in manifest["entries"] if e["type"] != "directory"]
        self.assertEqual(count, len(files))
        self.assertEqual(len(removed), len(files))
        self.assertEqual(
            sorted(e["path"] for e in removed), sorted(e["path"] for e in files)
        )
        for entry in files:
            self.assertFalse((self.source / entry["path"]).exists())
            self.assertFalse(os.path.lexists(self.source / entry["path"]))
        self.assertFalse((self.source / "empty").exists())
        self.assertTrue(self.source.is_dir())
        self.assertGreaterEqual(checks.count(True), 2)

    def test_parallel_cleanup_retains_everything_when_a_file_changed(self):
        stored, manifest = self.cleanup_fixture()
        victim = self.source / "dir5" / "sub2" / "file0005.dat"
        victim.write_bytes(b"modified after packing")
        removed = []
        with self.assertRaises(archives.ArchiveError) as caught:
            archives.cleanup_source(
                self.source, manifest, stored, lambda force = False: None,
                on_remove = removed.append, fresh_source_manifest = manifest,
                workers = 8, verify_stored_archive = lambda: None,
            )
        self.assertIn("source", str(caught.exception))
        self.assertEqual(removed, [])
        for entry in manifest["entries"]:
            self.assertTrue(os.path.lexists(self.source / entry["path"]))

    def test_parallel_cleanup_stops_when_the_destination_changes(self):
        stored, manifest = self.cleanup_fixture()
        state = {"calls": 0}

        def destination_unchanged(force = False):
            state["calls"] += 1
            if state["calls"] > 3:
                raise archives.ArchiveError("stored archive changed; remaining sources kept")

        removed = []
        with self.assertRaises(archives.ArchiveError):
            archives.cleanup_source(
                self.source, manifest, stored, destination_unchanged,
                on_remove = removed.append, fresh_source_manifest = manifest,
                workers = 4, verify_stored_archive = lambda: None,
                destination_check_interval = 0.0,
            )
        files = [e for e in manifest["entries"] if e["type"] != "directory"]
        self.assertLess(len(removed), len(files))
        for entry in removed:
            self.assertFalse(os.path.lexists(self.source / entry["path"]))

    def test_parallel_resume_cleanup_matches_sequential_outcome(self):
        stored, manifest = self.cleanup_fixture()
        already = self.source / "dir1" / "sub1" / "file0001.dat"
        already.unlink()
        removed = []
        result = archives.resume_cleanup_source(
            self.source, manifest, stored, lambda force = False: None,
            on_remove = removed.append, workers = 8,
            verify_stored_archive = lambda: None,
        )
        files = [e for e in manifest["entries"] if e["type"] != "directory"]
        self.assertEqual(result["removed"], len(files) - 1)
        self.assertEqual(len(removed), len(files) - 1)
        self.assertGreater(result["freed_bytes"], 0)
        for entry in files:
            self.assertFalse(os.path.lexists(self.source / entry["path"]))

    def test_parallel_cleanup_is_thread_safe_for_on_remove(self):
        stored, manifest = self.cleanup_fixture()
        seen = []
        lock = threading.Lock()

        def record(entry):
            with lock:
                seen.append(threading.get_ident())

        archives.cleanup_source(
            self.source, manifest, stored, lambda force = False: None,
            on_remove = record, fresh_source_manifest = manifest, workers = 8,
            verify_stored_archive = lambda: None,
        )
        self.assertGreater(len(set(seen)), 1)


if __name__ == "__main__":
    unittest.main()
