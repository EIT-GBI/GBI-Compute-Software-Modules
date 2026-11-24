"""The move table's courtesy checks stay bounded and name the failing path."""

import os
from pathlib import Path
import tempfile
import unittest

from gbi_data import moves


class Removable(unittest.TestCase):
    def setUp(self):
        if os.geteuid() == 0:
            self.skipTest("root bypasses permission bits")
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.source = Path(temporary.name).resolve() / "source"
        self.locked = self.source / "a" / "b" / "locked"
        self.locked.mkdir(parents = True)
        self.locked.chmod(0o500)
        self.addCleanup(self.locked.chmod, 0o700)

    def test_a_folder_the_caller_cannot_change_is_named(self):
        with self.assertRaisesRegex(
            ValueError, f"cannot remove from {self.locked}"
        ):
            moves.check_removable(self.source, 100, 60)

    def test_the_walk_stops_quietly_at_its_budget(self):
        # The walk goes depth first: source, a, b, then the locked folder.
        moves.check_removable(self.source, 3, 60)
        moves.check_removable(self.source, 100, 0)

    def test_a_single_file_needs_only_its_folder(self):
        payload = self.source / "a" / "file"
        payload.write_bytes(b"payload")
        moves.check_removable(payload, 100, 60)
        (self.source / "a").chmod(0o500)
        self.addCleanup((self.source / "a").chmod, 0o700)
        with self.assertRaisesRegex(ValueError, "cannot remove from"):
            moves.check_removable(payload, 100, 60)


if __name__ == "__main__":
    unittest.main()
