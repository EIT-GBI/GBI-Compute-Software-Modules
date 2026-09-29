"""Cleanup guard cost (chunk stores and plain stored archives) and shared lock stripe width."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from gbi_data import formats, storage
from gbi_data.transfer import transfer


class ChunkGuardThrottle(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.store = Path(self.temporary.name).resolve() / "bundle.gbi.tar.gbi-chunks"
        (self.store / ".gbi" / "parts").mkdir(parents=True)
        (self.store / "manifest.json").write_text("{}")
        self.manifest_key = str(self.store / "manifest.json")
        self.snapshot = ({"parts": []}, {self.manifest_key: storage.fingerprint(self.store / "manifest.json")})
        self.now = 1000.0

    def clock(self):
        return self.now

    def test_complete_check_runs_first_then_at_most_once_per_interval(self):
        with patch.object(formats, "_require_chunk_snapshot") as complete:
            guard = formats._ChunkGuard(self.store, self.snapshot, interval=60, clock=self.clock)
            guard()
            self.assertEqual(complete.call_count, 1)
            for _ in range(500):
                self.now += 0.01
                guard()
            self.assertEqual(complete.call_count, 1)
            self.assertEqual(guard.cheap_checks, 500)
            self.now += 60
            guard()
            self.assertEqual(complete.call_count, 2)
            guard.final()
            self.assertEqual(complete.call_count, 3)
            complete.assert_called_with(self.store, self.snapshot)

    def test_cheap_check_still_trips_on_manifest_change(self):
        with patch.object(formats, "_require_chunk_snapshot"):
            guard = formats._ChunkGuard(self.store, self.snapshot, interval=60, clock=self.clock)
            guard()
            self.now += 1
            guard()
            (self.store / "manifest.json").write_text('{"changed": true}')
            self.now += 1
            with self.assertRaisesRegex(ValueError, "verified chunk destination changed"):
                guard()

    def test_final_check_reports_a_changed_part(self):
        guard = formats._ChunkGuard(self.store, self.snapshot, interval=60, clock=self.clock)
        with patch.object(formats, "_require_chunk_snapshot", side_effect=ValueError("verified chunk destination changed")):
            with self.assertRaisesRegex(ValueError, "destination changed"):
                guard.final()


class ChunkedPackCleanupCost(unittest.TestCase):
    """A chunked archive move must not re-fingerprint every part per unlinked entry."""

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name).resolve()
        self.source = self.base / "source"
        self.target_root = self.base / "target"
        self.scratch = self.base / "lustre"
        self.state = self.scratch / ".gbi" / "state"
        for path in (self.source, self.target_root, self.state / "locks", self.state / "pending"):
            path.mkdir(parents=True)
        for index in range(40):
            (self.source / f"entry-{index:02d}.txt").write_text(f"payload {index}\n" * 20)

    def task(self, **overrides):
        return {"format_action": "pack", "source": str(self.source),
                "format_target": str(self.target_root / "bundle.gbi.tar"),
                "source_root": str(self.base), "target_root": str(self.target_root),
                "source_kind": "fss", "target_kind": "alluxio", "state": str(self.state),
                "scratch_root": str(self.scratch), "receipt": str(self.base / "receipts.jsonl"),
                "progress": str(self.base / "progress.json"), "settle_seconds": 0,
                "delete": True, "pack": "tar", "chunk_size": 4096, **overrides}

    def test_chunked_pack_move_cleans_up_with_bounded_complete_checks(self):
        original = formats._require_chunk_snapshot
        calls = []

        def counted(store, snapshot):
            calls.append(store)
            return original(store, snapshot)

        with patch.object(formats, "_require_chunk_snapshot", side_effect=counted):
            result = transfer(self.task())
        self.assertTrue(result["deleted"])
        self.assertEqual(sorted(path.name for path in self.source.iterdir()), [])
        rows = [json.loads(line) for line in (self.base / "receipts.jsonl").read_text().splitlines()]
        self.assertEqual([row["event"] for row in rows], ["verified", "deleted"])
        # First call, the final call and at most one interval-driven recheck for 40 entries.
        self.assertLessEqual(len(calls), 3, calls)
        self.assertGreaterEqual(len(calls), 2, calls)


class StoredArchiveGuardThrottle(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.stored = Path(self.temporary.name).resolve() / "bundle.gbi.tar"
        self.stored.write_bytes(b"tar" * 100)
        self.now = 1000.0

    def clock(self):
        return self.now

    def test_lstat_runs_first_then_at_most_once_per_interval(self):
        guard = formats._StoredArchiveGuard(self.stored, interval=60, clock=self.clock)
        with patch.object(Path, "lstat", wraps=Path.lstat, autospec=True) as lstat:
            guard()
            self.assertEqual(lstat.call_count, 1)
            for _ in range(500):
                self.now += 0.01
                guard()
            self.assertEqual(lstat.call_count, 1)
            self.assertEqual(guard.cheap_checks, 500)
            self.now += 60
            guard()
            self.assertEqual(lstat.call_count, 2)
            guard.final()
            self.assertEqual(lstat.call_count, 3)
        self.assertEqual(guard.complete_checks, 3)

    def test_final_check_reports_a_changed_archive(self):
        guard = formats._StoredArchiveGuard(self.stored, interval=60, clock=self.clock)
        guard()
        self.stored.write_bytes(b"replaced")
        with self.assertRaisesRegex(ValueError, "stored archive changed"):
            guard.final()

    def test_interval_check_reports_a_changed_archive(self):
        guard = formats._StoredArchiveGuard(self.stored, interval=60, clock=self.clock)
        guard()
        self.stored.unlink()
        self.stored.write_bytes(b"tar" * 100)
        self.now += 61
        with self.assertRaisesRegex(ValueError, "stored archive changed"):
            guard()


class PlainPackCleanupCost(ChunkedPackCleanupCost):
    """An unchunked archive move must not lstat the stored archive per unlinked entry."""

    def task(self, **overrides):
        return super().task(chunk_size=None, **overrides)

    # The inherited chunked case does not apply; a non-callable attribute is not collected.
    test_chunked_pack_move_cleans_up_with_bounded_complete_checks = None

    def test_plain_pack_move_cleans_up_with_bounded_lstat_checks(self):
        real = formats._StoredArchiveGuard
        guards = []

        def recording(*args, **kwargs):
            guards.append(real(*args, **kwargs))
            return guards[-1]

        with patch.object(formats, "_StoredArchiveGuard", side_effect=recording):
            result = transfer(self.task())
        self.assertTrue(result["deleted"])
        self.assertEqual(sorted(path.name for path in self.source.iterdir()), [])
        rows = [json.loads(line) for line in (self.base / "receipts.jsonl").read_text().splitlines()]
        self.assertEqual([row["event"] for row in rows], ["verified", "deleted"])
        self.assertEqual(len(guards), 1)
        # First call, the final call and at most one interval-driven recheck for 40 entries.
        self.assertLessEqual(guards[0].complete_checks, 3)
        self.assertGreaterEqual(guards[0].complete_checks, 2)
        self.assertGreaterEqual(guards[0].cheap_checks, 39)


class LockStripes(unittest.TestCase):
    def test_stripe_is_four_hex_digits(self):
        key = hashlib.sha256(b"/any/target").hexdigest()
        self.assertEqual(storage.lock_stripe(key), key[:4])
        self.assertEqual(storage.LOCK_STRIPE_HEX, 4)

    def test_transfer_holds_a_four_digit_stripe(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        base = Path(temporary.name).resolve()
        source_root, target_root, state = base / "source", base / "destination", base / "state"
        for path in (source_root, target_root, state / "locks", state / "pending"):
            path.mkdir(parents=True)
        source = source_root / "file.dat"
        source.write_bytes(b"x" * 4096)
        task = {"source": str(source), "target": str(target_root / "file.dat"),
                "source_root": str(source_root), "target_root": str(target_root),
                "selection_root": str(source_root), "state": str(state),
                "receipt": str(base / "receipt.jsonl"), "progress": str(base / "progress.json"),
                "rclone": None, "native_bytes": 1 << 20, "settle_seconds": 0,
                "target_kind": "fss", "delete": False, "encode_link": False, "decode_link": False}
        transfer(task)
        stripes = sorted(path.name for path in (state / "locks").iterdir())
        expected = hashlib.sha256(str(target_root / "file.dat").encode()).hexdigest()[:4]
        self.assertEqual(stripes, [expected])


if __name__ == "__main__":
    unittest.main()
