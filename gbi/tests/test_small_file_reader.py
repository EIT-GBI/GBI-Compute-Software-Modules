"""The existing native stream avoids rclone startup for tiny bulk members."""

import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from gbi_data.transfer import transfer


class SmallFileReader(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.source = self.root / "source"
        self.target = self.root / "target"
        self.state = self.root / "state"
        for path in (self.state / "locks", self.state / "pending"):
            path.mkdir(parents=True)
        self.source.write_bytes(b"bounded regular file")
        self.rclone = shutil.which("rclone")
        self.assertIsNotNone(self.rclone, "tests require maintained rclone on PATH")
        self.task = {"source": str(self.source), "target": str(self.target),
                     "source_root": str(self.root), "target_root": str(self.root),
                     "selection_root": str(self.root), "state": str(self.state),
                     "receipt": str(self.root / "receipts.jsonl"), "progress": str(self.root / "progress.json"),
                     "rclone": self.rclone, "settle_seconds": 1, "target_kind": "fss",
                     "delete": False, "encode_link": False, "decode_link": False}

    def recorded_reader(self, **changes):
        expected = hashlib.sha256(self.source.read_bytes()).hexdigest()
        transfer({**self.task, **changes})
        row = json.loads((self.root / "receipts.jsonl").read_text().splitlines()[0])
        self.assertEqual(row["sha256"], expected)
        self.assertEqual(hashlib.sha256(self.target.read_bytes()).hexdigest(), expected)
        return row["reader"]

    def test_small_regular_file_uses_native_and_retains_verified_move(self):
        self.assertEqual(self.recorded_reader(native_bytes=32, delete=True), "native")
        self.assertFalse(self.source.exists())
        rows = [json.loads(line) for line in (self.root / "receipts.jsonl").read_text().splitlines()]
        self.assertEqual([row["event"] for row in rows], ["verified", "deleted"])

    def test_above_bound_remains_rclone(self):
        with patch("gbi_data.transfer.sys.platform", "linux"):
            self.assertEqual(self.recorded_reader(native_bytes=4), "rclone")

    def test_existing_tasks_without_threshold_keep_reader(self):
        with patch("gbi_data.transfer.sys.platform", "linux"):
            self.assertEqual(self.recorded_reader(), "rclone")

    def test_darwin_regular_file_uses_native_above_threshold(self):
        with patch("gbi_data.transfer.sys.platform", "darwin"):
            self.assertEqual(self.recorded_reader(native_bytes=0), "native")


if __name__ == "__main__":
    unittest.main()
