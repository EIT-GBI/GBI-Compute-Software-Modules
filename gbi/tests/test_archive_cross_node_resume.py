"""Resumed cleanup and source verification across NFS clients.

Every NFS client numbers the same export with its own ``st_dev``.  A journal
written on one Slurm node therefore records device numbers that no other node
reproduces, while the inode, mode, size, mtime and ctime fields stay the same.
The codec renumbers such a journal onto the resuming client when the source
root is still the same inode; anything else keeps failing closed.
"""

import copy
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

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

    def test_partial_first_attempt_with_hardlinks_resumes_from_another_client(self):
        (self.source / "nested" / "twin").hardlink_to(self.source / "nested" / "b.txt")
        stored, manifest = self.packed()
        other            = _renumbered(manifest)
        # Attempt one, on the first node, unlinked a root entry and one of the
        # two hardlink names before it stopped; the surviving name's ctime moved.
        (self.source / "a.txt").unlink()
        (self.source / "nested" / "twin").unlink()

        result = archives.resume_cleanup_source(
            self.source, other, stored, destination_unchanged = lambda: None,
        )

        self.assertEqual(result["removed"], 2)
        self.assertFalse((self.source / "nested" / "b.txt").exists())
        self.assertFalse((self.source / "nested" / "link").is_symlink())

    def test_recorded_filesystem_identity_must_match_before_renumbering(self):
        stored, manifest = self.packed()
        export = ["nfs4", "10.1.12.97:/gbi-shared"]
        other  = _renumbered(manifest)
        other["_source"]["filesystem"] = export

        with patch.object(archives, "_filesystem_identity", return_value = ["nfs4", "10.1.12.98:/clone"]):
            with self.assertRaisesRegex(
                        archives.ArchiveError,
                        "source root changed during resumed cleanup; remaining sources kept",
                    ):
                archives.resume_cleanup_source(
                    self.source, other, stored, destination_unchanged = lambda: None,
                )
            with self.assertRaisesRegex(archives.ArchiveError, "source changed since packing"):
                archives.verify_source(self.source, other)
        self.assertTrue((self.source / "a.txt").exists())

        with patch.object(archives, "_filesystem_identity", return_value = None):
            with self.assertRaisesRegex(archives.ArchiveError, "source root changed during resumed cleanup"):
                archives.resume_cleanup_source(
                    self.source, other, stored, destination_unchanged = lambda: None,
                )
        self.assertTrue((self.source / "a.txt").exists())

        with patch.object(archives, "_filesystem_identity", return_value = list(export)):
            result = archives.resume_cleanup_source(
                self.source, other, stored, destination_unchanged = lambda: None,
            )
        self.assertEqual(result["removed"], 3)

    def test_journal_without_identity_relies_on_the_root_inode(self):
        stored, manifest = self.packed()
        other            = _renumbered(manifest)
        other["_source"].pop("filesystem", None)
        with patch.object(archives, "_filesystem_identity", return_value = ["nfs4", "10.1.12.98:/clone"]):
            result = archives.resume_cleanup_source(
                self.source, other, stored, destination_unchanged = lambda: None,
            )
        self.assertEqual(result["removed"], 3)

    def test_pack_records_the_filesystem_identity(self):
        with patch.object(archives, "_filesystem_identity", return_value = ["nfs4", "10.1.12.97:/gbi-shared"]):
            _, manifest = self.packed()
        self.assertEqual(manifest["_source"]["filesystem"], ["nfs4", "10.1.12.97:/gbi-shared"])
        with patch.object(archives, "_filesystem_identity", return_value = None):
            _, manifest = self.packed()
        self.assertIsNone(manifest["_source"]["filesystem"])

    def test_mountinfo_parser_finds_the_owning_mount(self):
        text = "\n".join([
            "22 29 0:21 / /proc rw,nosuid,nodev,noexec,relatime shared:5 - proc proc rw",
            "29 1 8:1 / / rw,relatime shared:1 - ext4 /dev/root rw",
            "150 29 0:52 / /mnt/gbi-shared rw,relatime shared:80 - nfs4 10.1.12.97:/gbi-shared rw,vers=4.1",
            "151 29 0:52 /home /mnt/bind rw,relatime shared:80 - nfs4 10.1.12.97:/gbi-shared rw,vers=4.1",
            "malformed line without separator",
        ])
        self.assertEqual(
            archives._mountinfo_identity(text, os.makedev(0, 52)),
            ["nfs4", "10.1.12.97:/gbi-shared"],
        )
        self.assertEqual(archives._mountinfo_identity(text, os.makedev(8, 1)), ["ext4", "/dev/root"])
        self.assertIsNone(archives._mountinfo_identity(text, os.makedev(8, 2)))
        self.assertIsNone(archives._mountinfo_identity("", os.makedev(0, 52)))

    def test_journal_without_root_is_refused_as_unresumable(self):
        stored, manifest = self.packed()
        del manifest["_source"]["root"]
        with self.assertRaisesRegex(archives.ArchiveError, "no resumable source observations"):
            archives.resume_cleanup_source(
                self.source, manifest, stored, destination_unchanged = lambda: None,
            )


if __name__ == "__main__":
    unittest.main()
