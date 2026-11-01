"""Resumed cleanup and source verification across NFS clients.

Every NFS client numbers the same export with its own ``st_dev``.  A journal
written on one Slurm node therefore records device numbers that no other node
reproduces, while the inode, mode, size, mtime and ctime fields stay the same.
The codec renumbers such a journal onto the resuming client when the source
root is still the same inode; anything else keeps failing closed.
"""

import copy
import io
from pathlib import Path
import tempfile
import unittest

from gbi_data import archives


def _renumbered(manifest, offset = 1):
    """A deep copy of ``manifest`` as another NFS client would have recorded it."""
    other    = copy.deepcopy(manifest)
    evidence = other["_source"]
    device   = evidence["root"][0]

    def renumber(observation):
        if observation[0] == device:
            return [device + offset, *observation[1:]]
        return observation

    evidence["root"]         = renumber(evidence["root"])
    evidence["observations"] = {
        name: renumber(observation)
        for name, observation in evidence["observations"].items()
    }
    return other


class ArchiveCrossNodeResume(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base   = Path(self.temporary.name).resolve()
        self.source = self.base / "source"
        (self.source / "nested").mkdir(parents = True)
        (self.source / "a.txt").write_text("alpha")
        (self.source / "nested" / "b.txt").write_text("beta")
        (self.source / "nested" / "link").symlink_to("b.txt")

    def packed(self):
        sink     = io.BytesIO()
        manifest = archives.pack(self.source, sink)
        sink.seek(0)
        return sink, manifest

    def test_journal_from_another_client_resumes_cleanup(self):
        stored, manifest = self.packed()
        other            = _renumbered(manifest)
        self.assertNotEqual(other["_source"]["root"][0], manifest["_source"]["root"][0])

        result = archives.resume_cleanup_source(
            self.source, other, stored, destination_unchanged = lambda: None,
        )

        self.assertEqual(result["removed"], 3)
        self.assertFalse((self.source / "a.txt").exists())
        self.assertFalse((self.source / "nested" / "b.txt").exists())
        self.assertFalse((self.source / "nested" / "link").is_symlink())
        # The journal handed in is not rewritten; the caller may persist it again.
        self.assertEqual(other["_source"]["root"][0], manifest["_source"]["root"][0] + 1)

    def test_journal_from_another_client_passes_source_verification(self):
        _, manifest = self.packed()
        archives.verify_source(self.source, _renumbered(manifest))

    def test_same_client_journal_is_untouched(self):
        stored, manifest = self.packed()
        before = copy.deepcopy(manifest["_source"])
        root_expected, observations = archives._rebased_evidence(
            manifest["_source"], self.source.lstat()
        )
        self.assertEqual(root_expected, before["root"])
        self.assertEqual(observations, before["observations"])
        result = archives.resume_cleanup_source(
            self.source, manifest, stored, destination_unchanged = lambda: None,
        )
        self.assertEqual(result["removed"], 3)

    def test_replaced_root_still_fails_closed_under_a_new_device_number(self):
        stored, manifest = self.packed()
        other            = _renumbered(manifest)
        # Replace the source directory by another inode with the same contents.
        replacement = self.base / "replacement"
        replacement.mkdir()
        (replacement / "nested").mkdir()
        (replacement / "a.txt").write_text("alpha")
        (replacement / "nested" / "b.txt").write_text("beta")
        (replacement / "nested" / "link").symlink_to("b.txt")
        moved = self.base / "moved-away"
        self.source.rename(moved)
        replacement.rename(self.source)

        with self.assertRaisesRegex(
                    archives.ArchiveError,
                    "source root changed during resumed cleanup; remaining sources kept",
                ):
            archives.resume_cleanup_source(
                self.source, other, stored, destination_unchanged = lambda: None,
            )
        self.assertTrue((self.source / "a.txt").exists())
        self.assertTrue((moved / "a.txt").exists())
        with self.assertRaisesRegex(archives.ArchiveError, "source changed since packing"):
            archives.verify_source(self.source, other)

    def test_entry_recorded_on_another_device_keeps_failing_closed(self):
        stored, manifest = self.packed()
        other            = _renumbered(manifest)
        # A file recorded below a mount of its own does not share the root's
        # device; renumbering must leave it alone, so it still mismatches.
        foreign = other["_source"]["observations"]["nested/b.txt"]
        other["_source"]["observations"]["nested/b.txt"] = [foreign[0] + 7, *foreign[1:]]

        with self.assertRaisesRegex(
                    archives.ArchiveError, "selected source changed before removal; source kept",
                ):
            archives.resume_cleanup_source(
                self.source, other, stored, destination_unchanged = lambda: None,
            )
        self.assertTrue((self.source / "a.txt").exists())
        self.assertTrue((self.source / "nested" / "b.txt").exists())

    def test_changed_entry_is_still_detected_after_renumbering(self):
        stored, manifest = self.packed()
        other            = _renumbered(manifest)
        (self.source / "a.txt").write_text("alpha, edited")

        with self.assertRaisesRegex(
                    archives.ArchiveError, "selected source changed before removal; source kept",
                ):
            archives.resume_cleanup_source(
                self.source, other, stored, destination_unchanged = lambda: None,
            )
        self.assertEqual((self.source / "a.txt").read_text(), "alpha, edited")
        self.assertTrue((self.source / "nested" / "b.txt").exists())


if __name__ == "__main__":
    unittest.main()
