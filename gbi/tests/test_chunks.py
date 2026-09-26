"""Filesystem chunk resume and reconstruction, including adversarial retries."""

import hashlib
from contextlib import contextmanager
import io
import json
import os
import shutil
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from gbi_data import chunks


class ManifestParsing(unittest.TestCase):
    def setUp(self):
        self.manifest = {
            "format": chunks.FORMAT, "version": chunks.VERSION,
            "original_name": "original.dat", "size": 3, "chunk_size": 2,
            "sha256": hashlib.sha256(b"abc").hexdigest(),
            "source": "/source/original.dat", "source_fingerprint": [1, 2, 3, 4, 5],
            "parts": [
                {"name": "00000000.part", "size": 2,
                 "sha256": hashlib.sha256(b"ab").hexdigest()},
                {"name": "00000001.part", "size": 1,
                 "sha256": hashlib.sha256(b"c").hexdigest()},
            ],
        }
        self.encoded = chunks._encoded(self.manifest)
        self.completion = chunks._encoded(chunks._completion(self.manifest, self.encoded))

    def test_complete_validation_has_no_filesystem_io(self):
        with patch.object(chunks, "_directory", side_effect=AssertionError("filesystem I/O")), \
                patch.object(chunks, "_regular", side_effect=AssertionError("filesystem I/O")):
            self.assertEqual(chunks.parse_manifest(self.encoded, self.completion),
                             {**self.manifest, "state": "complete"})

    def test_malformed_and_foreign_manifests_are_rejected_for_resume_too(self):
        cases = [b"{", b"null", b"[]", b"{}"]
        for key, value in (("format", "foreign"), ("version", 999),
                           ("original_name", "../escape"), ("size", -1),
                           ("chunk_size", 0), ("sha256", "bad"),
                           ("source_fingerprint", []), ("parts", [])):
            cases.append(chunks._encoded({**self.manifest, key: value}))
        cases.append(chunks._encoded({**self.manifest, "parts": self.manifest["parts"][::-1]}))
        for encoded in cases:
            for complete in (True, False):
                with self.subTest(encoded=encoded, complete=complete), self.assertRaises(ValueError):
                    chunks.parse_manifest(encoded, self.completion, complete=complete)

    def test_missing_or_unfinished_marker_is_incomplete_only_for_resume(self):
        for marker in (None, b"", b'{"format":'):
            with self.subTest(marker=marker):
                self.assertEqual(chunks.parse_manifest(self.encoded, marker, complete=False)["state"],
                                 "incomplete")
                with self.assertRaisesRegex(ValueError, "incomplete"):
                    chunks.parse_manifest(self.encoded, marker)
        self.assertEqual(chunks.parse_manifest(self.encoded, self.completion,
                                              complete=False)["state"], "complete")

    def test_foreign_mismatched_and_oversized_markers_are_never_resumable(self):
        foreign = {**chunks._completion(self.manifest, self.encoded), "format": "foreign"}
        for marker in (b"null", b"{}", chunks._encoded(foreign), b" " * 4097):
            for complete in (True, False):
                with self.subTest(marker=marker[:80], complete=complete), \
                        self.assertRaisesRegex(ValueError, "does not match"):
                    chunks.parse_manifest(self.encoded, marker, complete=complete)
        changed = chunks._encoded({**self.manifest, "original_name": "renamed.dat"})
        with self.assertRaisesRegex(ValueError, "does not match"):
            chunks.parse_manifest(changed, self.completion, complete=False)

    def test_manifest_and_part_limits_are_preserved(self):
        with patch.object(chunks, "MAX_MANIFEST_BYTES", len(self.encoded) - 1):
            with self.assertRaisesRegex(ValueError, "manifest exceeds"):
                chunks.parse_manifest(self.encoded, self.completion, complete=False)
        with patch.object(chunks, "MAX_PARTS", 1):
            with self.assertRaisesRegex(ValueError, "too many chunks"):
                chunks.parse_manifest(self.encoded, self.completion, complete=False)

    def test_transport_reader_opens_parts_in_order_and_closes_them(self):
        opened = []
        streams = [io.BytesIO(b"ab"), io.BytesIO(b"c")]

        def open_part(part):
            opened.append(part)
            return streams[len(opened) - 1]

        with chunks._ChunkReader(None, self.manifest, open_part=open_part) as reader:
            self.assertEqual(reader.read(), b"abc")
            self.assertTrue(reader.verified)
        self.assertEqual(opened, self.manifest["parts"])
        self.assertTrue(all(stream.closed for stream in streams))

    def test_transport_reader_rejects_corrupt_truncated_and_oversized_parts(self):
        for payload in (b"ax", b"a", b"abc"):
            stream = io.BytesIO(payload)
            with self.subTest(payload=payload), \
                    chunks._ChunkReader(None, self.manifest, open_part=lambda part: stream) as reader:
                with self.assertRaisesRegex(ValueError, "corrupt chunk"):
                    reader.read()
                self.assertFalse(reader.verified)
            self.assertTrue(stream.closed)

    def test_transport_reader_rejects_wrong_full_checksum(self):
        streams = iter([io.BytesIO(b"ab"), io.BytesIO(b"c")])
        with chunks._ChunkReader(None, {**self.manifest, "sha256": "0" * 64},
                                 open_part=lambda part: next(streams)) as reader:
            with self.assertRaisesRegex(ValueError, "full reconstructed checksum"):
                reader.read()
            self.assertFalse(reader.verified)

    def test_transport_reader_early_close_does_not_claim_verification(self):
        stream = io.BytesIO(b"ab")
        with chunks._ChunkReader(None, self.manifest, open_part=lambda part: stream) as reader:
            self.assertEqual(reader.read(1), b"a")
        self.assertTrue(stream.closed)
        self.assertFalse(reader.verified)


class Chunks(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name).resolve()
        self.source = self.base / "original.dat"
        self.payload = b"first---second--third---last"
        self.source.write_bytes(self.payload)
        self.store = self.base / "stored.dat"
        self.target = self.base / "restored.dat"
        self.state = self.base / "state"
        self.state.mkdir()

    def write(self, **kwargs):
        return chunks.write_chunks(self.source, self.store, state=self.state, chunk_size=8, **kwargs)

    def part(self, number=0):
        return self.store / ".gbi" / "parts" / f"{number:08d}.part"

    def interrupt(self):
        def stop(_):
            raise InterruptedError("stopped after first verified part")
        with self.assertRaises(InterruptedError):
            self.write(progress=stop)

    def test_full_reconstruction_and_manifest(self):
        rows = []
        manifest = self.write(progress=rows.append)
        self.assertEqual(manifest["format"], "gbi-chunks")
        self.assertEqual(manifest["version"], 1)
        self.assertEqual(manifest["state"], "complete")
        self.assertEqual(manifest["original_name"], self.source.name)
        self.assertEqual(manifest["sha256"], hashlib.sha256(self.payload).hexdigest())
        self.assertEqual([part["size"] for part in manifest["parts"]], [8, 8, 8, 4])
        self.assertEqual(rows[-1]["bytes"], len(self.payload))
        self.assertFalse(any(row["reused"] for row in rows))
        result = chunks.restore_chunks(self.store, self.target)
        self.assertEqual(self.target.read_bytes(), self.payload)
        self.assertEqual(result["sha256"], manifest["sha256"])
        self.assertEqual(self.source.read_bytes(), self.payload)

    def test_filesystem_reader_delegates_to_pure_parser(self):
        self.write()
        with patch.object(chunks, "parse_manifest", wraps=chunks.parse_manifest) as parse:
            result = chunks.read_manifest(self.store)
        parse.assert_called_once_with((self.store / "manifest.json").read_bytes(),
                                      (self.store / ".gbi" / "complete.json").read_bytes(),
                                      complete=True)
        self.assertEqual(result["state"], "complete")

    def test_interruption_resumes_verified_part_without_rewriting(self):
        self.interrupt()
        before = self.part().stat()
        self.assertEqual(chunks.read_manifest(self.store, complete=False)["state"], "incomplete")
        with self.assertRaisesRegex(ValueError, "incomplete"):
            chunks.restore_chunks(self.store, self.target)
        self.assertFalse(self.target.exists())
        rows = []
        self.write(progress=rows.append)
        self.assertTrue(rows[0]["reused"])
        self.assertEqual(self.part().stat().st_ino, before.st_ino)
        self.assertEqual(self.part().stat().st_mtime_ns, before.st_mtime_ns)
        chunks.restore_chunks(self.store, self.target)
        self.assertEqual(self.target.read_bytes(), self.payload)

    def test_changed_source_keeps_prior_parts(self):
        self.interrupt()
        original = self.part().read_bytes()
        self.source.write_bytes(b"new data")
        with self.assertRaisesRegex(ValueError, "source or chunk format changed"):
            self.write()
        self.assertEqual(self.part().read_bytes(), original)

    def test_same_size_same_mtime_edit_is_not_reused(self):
        self.interrupt()
        before = self.source.stat()
        self.source.write_bytes(b"X" + self.payload[1:])
        os.utime(self.source, ns=(before.st_atime_ns, before.st_mtime_ns))
        with self.assertRaisesRegex(ValueError, "source or chunk format changed"):
            self.write()

    def test_chunk_size_change_is_rejected(self):
        self.interrupt()
        with self.assertRaisesRegex(ValueError, "source or chunk format changed"):
            chunks.write_chunks(self.source, self.store, state=self.state, chunk_size=9)

    def test_owned_corrupt_and_missing_parts_repaired(self):
        self.write()
        self.part().write_bytes(b"corrupt!")
        self.part(1).unlink()
        rows = []
        self.write(progress=rows.append)
        self.assertEqual([row["reused"] for row in rows], [False, False, True, True])
        chunks.restore_chunks(self.store, self.target)
        self.assertEqual(self.target.read_bytes(), self.payload)

    def test_unrelated_replacement_is_never_removed(self):
        self.write()
        other = self.base / "unrelated"
        other.write_bytes(b"science")
        os.replace(other, self.part())
        with self.assertRaisesRegex(ValueError, "unowned chunk collision"):
            self.write()
        self.assertEqual(self.part().read_bytes(), b"science")

    def test_preexisting_destination_directory_is_not_claimed(self):
        self.store.mkdir()
        marker = self.store / "manifest.json"
        marker.write_text('{"format": "some other program"}')
        with self.assertRaises(ValueError):
            self.write()
        self.assertEqual(marker.read_text(), '{"format": "some other program"}')
        self.assertFalse((self.store / ".gbi").exists())

    def test_manifest_path_collision_does_not_overwrite_file(self):
        self.store.write_bytes(b"user payload")
        with self.assertRaises(ValueError):
            self.write()
        self.assertEqual(self.store.read_bytes(), b"user payload")

    def test_symlink_part_cannot_redirect_repair(self):
        self.write()
        other = self.base / "other"
        other.write_bytes(b"untouched")
        self.part().unlink()
        self.part().symlink_to(other)
        with self.assertRaisesRegex(ValueError, "unowned chunk collision"):
            self.write()
        self.assertEqual(other.read_bytes(), b"untouched")
        self.assertTrue(self.part().is_symlink())

    def test_concurrent_writer_refuses_held_lock(self):
        self.interrupt()
        with chunks._locked(self.state, self.store):
            with self.assertRaisesRegex(ValueError, "already in use"):
                self.write()
        self.write()

    def test_reader_needs_no_target_lock_and_unchanged_retry_is_safe(self):
        self.write()
        with chunks.open_chunks(self.store) as reader:
            self.assertEqual(reader.read(3), self.payload[:3])
            self.write()

    def test_corrupt_or_missing_restore_never_succeeds(self):
        for missing in (False, True):
            with self.subTest(missing=missing):
                self.write()
                if missing:
                    self.part(1).unlink()
                else:
                    self.part(1).write_bytes(b"corrupt!")
                with self.assertRaisesRegex(ValueError, "chunk"):
                    chunks.restore_chunks(self.store, self.target)
                self.assertFalse(self.target.exists())

    def test_archive_style_early_exit_still_checks_full_stream(self):
        self.write()
        self.part(3).write_bytes(b"bad")
        with self.assertRaisesRegex(ValueError, "corrupt chunk"):
            with chunks.open_chunks(self.store) as reader:
                self.assertEqual(reader.read(1), self.payload[:1])

    def test_full_payload_hash_and_part_order_are_checked(self):
        self.write()
        manifest = chunks.read_manifest(self.store)
        manifest.pop("state")
        manifest["sha256"] = "0" * 64
        encoded = chunks._encoded(manifest)
        (self.store / "manifest.json").write_bytes(encoded)
        (self.store / ".gbi" / "complete.json").write_bytes(chunks._encoded(chunks._completion(manifest, encoded)))
        with self.assertRaisesRegex(ValueError, "full reconstructed checksum"):
            chunks.restore_chunks(self.store, self.target)
        self.assertFalse(self.target.exists())
        manifest["parts"].reverse()
        (self.store / "manifest.json").write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ValueError, "order"):
            chunks.read_manifest(self.store)

    def test_existing_restored_file_is_never_overwritten(self):
        self.write()
        self.target.write_bytes(b"keep this")
        with self.assertRaises(FileExistsError):
            chunks.restore_chunks(self.store, self.target)
        self.assertEqual(self.target.read_bytes(), b"keep this")

    def test_output_readback_detects_corruption(self):
        self.write()
        real_regular = chunks._regular
        def corrupt_before_read(path, flags=os.O_RDONLY):
            if Path(path) == self.target:
                self.target.write_bytes(b"corruption")
            return real_regular(path, flags)
        with patch.object(chunks, "_regular", side_effect=corrupt_before_read):
            with self.assertRaisesRegex(ValueError, "restored file checksum"):
                chunks.restore_chunks(self.store, self.target)
        self.assertFalse(self.target.exists())

    def test_source_changed_during_write_cannot_publish_completion(self):
        def change_source(_):
            self.source.write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "source changed"):
            self.write(progress=change_source)
        self.assertEqual(chunks.read_manifest(self.store, complete=False)["state"], "incomplete")

    def test_verified_part_changed_later_cannot_publish_completion(self):
        def corrupt_prior_part(row):
            if row["parts"] == 2:
                before = self.part().stat()
                self.part().write_bytes(b"bad part")
                # Exercise filesystems whose timestamp granularity hides a
                # same-size rewrite from the metadata fingerprint.
                os.utime(self.part(), ns=(before.st_atime_ns, before.st_mtime_ns))
        with self.assertRaisesRegex(ValueError, "verified chunk changed"):
            self.write(progress=corrupt_prior_part)
        self.assertEqual(chunks.read_manifest(self.store, complete=False)["state"], "incomplete")

    def test_existing_empty_directory_is_not_a_chunk_store(self):
        self.store.mkdir()
        with self.assertRaisesRegex(ValueError, "not a readable GBI chunk manifest"):
            self.write()
        self.assertEqual(list(self.store.iterdir()), [])

    def test_hardlinked_source_can_be_archived_without_mutation(self):
        other = self.base / "hardlink"
        os.link(self.source, other)
        self.write()
        self.assertEqual(other.read_bytes(), self.payload)

    def test_empty_file_roundtrip(self):
        self.source.write_bytes(b"")
        manifest = self.write()
        self.assertEqual(manifest["parts"], [])
        chunks.restore_chunks(self.store, self.target)
        self.assertEqual(self.target.read_bytes(), b"")

    def test_interrupted_part_write_reuses_recorded_ownership(self):
        real_write = chunks._write_part
        def interrupted(*args):
            real_write(*args)
            args[1].write_bytes(b"partial")
            raise InterruptedError("write interrupted")
        with patch.object(chunks, "_write_part", side_effect=interrupted):
            with self.assertRaises(InterruptedError):
                self.write()
        self.write()
        chunks.restore_chunks(self.store, self.target)
        self.assertEqual(self.target.read_bytes(), self.payload)

    def test_copied_store_is_portable_without_original_scratch_or_inodes(self):
        self.write()
        copied = self.base / "copied-store"
        shutil.copytree(self.store, copied)
        chunks.restore_chunks(copied, self.target)
        self.assertEqual(self.target.read_bytes(), self.payload)

    def test_remote_writes_are_exclusive_and_never_renamed(self):
        real_open, real_replace = os.open, os.replace
        def checked_open(path, flags, *args, **kwargs):
            if self.store in Path(path).parents and flags & (os.O_WRONLY | os.O_RDWR):
                self.assertTrue(flags & os.O_EXCL)
                self.assertFalse(flags & os.O_TRUNC)
            return real_open(path, flags, *args, **kwargs)
        def checked_replace(source, target, *args, **kwargs):
            self.assertNotIn(self.store, Path(target).parents)
            return real_replace(source, target, *args, **kwargs)
        with patch.object(chunks.os, "open", side_effect=checked_open), \
                patch.object(chunks.os, "replace", side_effect=checked_replace):
            self.write()
            self.part().write_bytes(b"bad part")
            self.write()
        self.assertFalse((self.store / ".gbi" / "lock").exists())

    def test_manifest_digest_binds_completion_marker(self):
        self.write()
        manifest = chunks.read_manifest(self.store)
        manifest.pop("state")
        manifest["original_name"] = "changed.dat"
        (self.store / "manifest.json").write_bytes(chunks._encoded(manifest))
        with self.assertRaisesRegex(ValueError, "completion marker does not match"):
            chunks.restore_chunks(self.store, self.target)
        self.assertFalse(self.target.exists())

    def test_too_many_parts_fail_before_destination_creation(self):
        self.source.write_bytes(b"x" * (chunks.MAX_PARTS + 1))
        with self.assertRaisesRegex(ValueError, "too many chunks"):
            chunks.write_chunks(self.source, self.store, state=self.state, chunk_size=1)
        self.assertFalse(self.store.exists())

    def test_oversized_manifest_is_not_loaded(self):
        self.write()
        with patch.object(chunks, "MAX_MANIFEST_BYTES", 32):
            with self.assertRaisesRegex(ValueError, "manifest exceeds"):
                chunks.read_manifest(self.store)

    def test_part_parent_symlink_replacement_is_refused(self):
        outside = self.base / "outside"
        outside.mkdir()
        def replace_parent(row):
            if row["parts"] == 1:
                parent = self.part().parent
                parent.rename(parent.with_name("retained-parts"))
                parent.symlink_to(outside, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "symlink"):
            self.write(progress=replace_parent)
        self.assertEqual(list(outside.iterdir()), [])

    def test_restore_parent_rechecked_after_reading_manifest(self):
        self.write()
        parent = self.base / "output"
        parent.mkdir()
        outside = self.base / "outside"
        outside.mkdir()
        original = chunks.open_chunks
        @contextmanager
        def replaced_parent(store):
            with original(store) as reader:
                parent.rename(self.base / "retained-output")
                parent.symlink_to(outside, target_is_directory=True)
                yield reader
        with patch.object(chunks, "open_chunks", side_effect=replaced_parent):
            with self.assertRaisesRegex(ValueError, "symlink"):
                chunks.restore_chunks(self.store, parent / "file")
        self.assertEqual(list(outside.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
