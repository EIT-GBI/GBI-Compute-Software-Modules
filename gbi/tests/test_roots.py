"""`gbi data roots` derives team and read-only roots on the host and explains each."""

from contextlib import redirect_stdout
import io
from pathlib import Path
import pwd
import os
import tempfile
import unittest

from gbi_data import cli, roots
from gbi_data.storage import Site


class RootsReportTest(unittest.TestCase):
    def setUp(self):
        self.user = pwd.getpwuid(os.getuid()).pw_name
        self.tmp  = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name).resolve()
        for kind in ("lustre", "fss"):
            (self.base / kind / self.user).mkdir(parents = True)
        (self.base / "user-data").mkdir()
        self.team = self.base / "team"
        self.conf = self.base / "site.conf"

    def tearDown(self):
        self.tmp.cleanup()

    def site(self, **extra):
        values = {
            "lustre_root": self.base / "lustre", "fss_root": self.base / "fss",
            "bucket_root": self.base / "user-data", "team_root": self.team,
            "readonly_roots": " ".join(
                str(self.base / name)
                for name in ("instrument-data", "lmb-data", "shared", "missing")
            ),
            **extra,
        }
        self.conf.write_text(
            "".join(f"{key} = {value}\n" for key, value in values.items())
        )
        return Site(self.conf)

    def join_team(self, name, member = True, shared = True):
        folder = self.team / name
        if member:
            (folder / "members" / self.user).mkdir(parents = True)
        if shared:
            (folder / "shared").mkdir(parents = True)
        return folder

    def test_personal_alias_into_a_team_folder_prints_the_member_folder(self):
        folder = self.join_team("scientific-computing")
        member = folder / "members" / self.user
        alias  = self.base / "user-data" / self.user
        alias.symlink_to(member)
        lines = roots.lines(self.site())
        text  = "\n".join(lines)
        self.assertIn(f"lustre   {self.base / 'lustre' / self.user}", text)
        self.assertIn(f"fss      {self.base / 'fss' / self.user}", text)
        self.assertIn(f"alluxio  {member}", text)
        self.assertIn(f"also reachable as {alias}", text)
        self.assertIn(f"team     {folder / 'shared'}", text)
        self.assertIn("read-write for everyone in scientific-computing", text)
        self.assertNotIn("not in a team folder", text)
        self.assertEqual(lines[-1], roots.FOOTER)

    def test_member_folder_without_a_personal_path_is_the_archive_root(self):
        folder = self.join_team("chin-lab")
        text = "\n".join(roots.lines(self.site()))
        self.assertIn(f"alluxio  {folder / 'members' / self.user}", text)
        self.assertNotIn("also reachable as", text)

    def test_legacy_personal_path_on_its_own_backend_prints_the_path(self):
        backend = self.base / ".alluxio-personal" / "personal-shard-4" / self.user
        backend.mkdir(parents = True)
        alias = self.base / "user-data" / self.user
        alias.symlink_to(backend)
        folder = self.join_team("pellock-lab")
        text = "\n".join(roots.lines(self.site()))
        self.assertIn(f"alluxio  {alias}", text)
        self.assertNotIn(".alluxio-personal", text)
        self.assertNotIn("also reachable as", text)
        self.assertIn(f"team     {folder / 'shared'}", text)

    def test_no_team_membership_is_stated(self):
        (self.base / "user-data" / self.user).mkdir()
        self.join_team("other-lab", member = False)
        text = "\n".join(roots.lines(self.site()))
        self.assertIn(f"alluxio  {self.base / 'user-data' / self.user}", text)
        self.assertIn("team     none: you are not in a team folder", text)

    def test_two_teams_list_both_shared_folders(self):
        first  = self.join_team("alpha")
        second = self.join_team("beta")
        (self.base / "user-data" / self.user).symlink_to(
            first / "members" / self.user
        )
        text = "\n".join(roots.lines(self.site()))
        self.assertIn(f"team     {first / 'shared'}", text)
        self.assertIn(f"         {second / 'shared'}", text)

    def test_readonly_roots_show_families_and_skip_missing_paths(self):
        (self.base / "instrument-data" / "illumina_nextseq_1").mkdir(parents = True)
        (self.base / "shared").mkdir()  # exists, but holds nothing yet
        (self.base / "lmb-data").symlink_to(self.base / "instrument-data")
        (self.base / "user-data" / self.user).mkdir()
        text = "\n".join(roots.lines(self.site()))
        self.assertIn(
            f"read     {self.base / 'instrument-data'}/*  {self.base / 'lmb-data'}"
            f"  {self.base / 'shared'}",
            text,
        )
        self.assertNotIn("missing", text)
        self.assertIn(roots.READ_NOTE, text)

    @unittest.skipIf(os.geteuid() == 0, "root opens every directory")
    def test_unreadable_collection_is_left_out_and_the_rest_named(self):
        archive = self.base / "archive"
        closed  = archive / "parts-lab-sanger"
        open_   = archive / "open-collection"
        for path in (closed, open_, self.base / "shared" / "results"):
            path.mkdir(parents = True)
        closed.chmod(0)
        (self.base / "user-data" / self.user).mkdir()
        site = self.site(readonly_roots = f"{archive} {self.base / 'shared'}")
        try:
            text = "\n".join(roots.lines(site))
        finally:
            closed.chmod(0o700)
        self.assertIn(f"read     {open_}  {self.base / 'shared'}/*", text)
        self.assertNotIn("parts-lab-sanger", text)
        self.assertNotIn(f"{archive}/*", text)
        self.assertIn(roots.READ_NOTE, text)

    def test_fully_readable_family_keeps_the_star_form(self):
        archive = self.base / "archive"
        for name in ("one", "two"):
            (archive / name).mkdir(parents = True)
        (self.base / "user-data" / self.user).mkdir()
        text = "\n".join(roots.lines(self.site(readonly_roots = str(archive))))
        self.assertIn(f"read     {archive}/*", text)
        self.assertNotIn(f"{archive / 'one'}", text)

    def test_site_notes_are_printed_and_editable(self):
        (self.base / "user-data" / self.user).mkdir()
        text = "\n".join(roots.lines(self.site(
            lustre_note = "expiry starts on 1 December",
            fss_note = "", alluxio_note = "ask before archiving over 10 TB",
        )))
        self.assertIn("expiry starts on 1 December", text)
        self.assertIn("ask before archiving over 10 TB", text)
        self.assertNotIn("100 GB quota", text)
        self.assertIn("compiling software", text)

    def test_print_report_writes_the_lines(self):
        (self.base / "user-data" / self.user).mkdir()
        out = io.StringIO()
        with redirect_stdout(out):
            self.assertEqual(roots.print_report(self.site()), 0)
        self.assertEqual(out.getvalue().splitlines()[0], "Your storage roots")

    def test_help_describes_the_report(self):
        out = io.StringIO()
        with redirect_stdout(out), self.assertRaises(SystemExit):
            cli.parser().parse_args(["data", "roots", "--help"])
        text = out.getvalue().lower()
        self.assertIn("team folders", text)
        self.assertIn("read-only", text)


if __name__ == "__main__":
    unittest.main()
