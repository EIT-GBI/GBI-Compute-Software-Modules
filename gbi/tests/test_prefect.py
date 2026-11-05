"""Prefect submission sends resolved paths and retries one saved request."""

import contextlib
import errno
import io
import json
import os
from pathlib import Path
import pwd
import ssl
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from urllib import error as urlerror

from gbi_data import cli, prefect
from gbi_data.storage import Site


class Prefect(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        user      = pwd.getpwuid(os.getuid()).pw_name
        for name in ("lustre", "fss", "bucket"):
            (self.base / name / user).mkdir(parents = True)
        config = self.base / "site.conf"
        config.write_text(
            "".join(
                f"{name}_root = {self.base / name}\n"
                for name in ("lustre", "fss", "bucket")
            )
        )
        self.site = Site(config)
        self.home = self.site.roots["lustre"] / ".gbi"
        for root in self.site.roots.values():
            (root / "experiment").mkdir()
        # A test never waits on a [y/N] question.
        terminal = patch("gbi_data.moves.terminal", return_value = False)
        terminal.start()
        self.addCleanup(terminal.stop)
        prefect._TOKEN = None
        prefect._TOKEN_EXPIRES = 0

    def path(self, kind, name = "experiment"):
        return str(self.site.roots[kind] / name)

    def options(self, source = "lustre", target = "alluxio", *flags):
        return cli.parser().parse_args(
            [
                "data", "copy", self.path(source), self.path(target),
                "--prefect", *flags,
            ]
        )

    def moving(self, source, target, *flags):
        return cli.parser().parse_args(
            ["data", "move", source, target, "--prefect", *flags]
        )

    def team_site(self):
        """A site with a team folder and a read-only collection."""
        user   = pwd.getpwuid(os.getuid()).pw_name
        member = self.base / "team" / "liu-lab" / "members" / user
        shared = self.base / "team" / "liu-lab" / "shared"
        run    = self.base / "instrument-data" / "nextseq" / "run-1"
        for path in (member, shared, run):
            path.mkdir(parents = True)
        config = self.base / "site.conf"
        config.write_text(
            config.read_text()
            + f"team_root = {self.base / 'team'}\n"
            + f"readonly_roots = {self.base / 'instrument-data'}\n"
        )
        site = Site(config)
        return site, member.resolve(), shared.resolve(), run.resolve()

    def test_routes_send_resolved_absolute_paths(self):
        for source, target, operation in (
            ("lustre", "alluxio", "archive-lustre"),
            ("fss", "alluxio", "archive-fss"),
            ("alluxio", "lustre", "stage-lustre"),
            ("alluxio", "fss", "stage-fss"),
        ):
            with self.subTest(operation = operation):
                request = prefect.prepare(
                    self.options(source, target), self.site
                )
                self.assertEqual(request["operation"], operation)
                self.assertEqual(request["source"], self.path(source))
                self.assertEqual(request["destination"], self.path(target))
                self.assertEqual(request["verb"], "copy")
        options             = self.options("alluxio", "alluxio")
        options.destination = self.path("alluxio", "copy-of-experiment")
        request             = prefect.prepare(options, self.site)
        self.assertEqual(request["operation"], "copy-objects")
        self.assertEqual(
            request["destination"], self.path("alluxio", "copy-of-experiment")
        )
        with self.assertRaisesRegex(ValueError, "without --prefect"):
            prefect.prepare(self.options("lustre", "fss"), self.site)

    def test_explicit_runner_excludes_other_execution_flags(self):
        for flag in ("--detach", "--local"):
            with (
                contextlib.redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit),
            ):
                self.options("lustre", "alluxio", flag)
        self.assertFalse(
            cli.parser().parse_args(["data", "copy", "a", "b"]).prefect
        )

    def test_team_folders_and_collections_need_no_personal_root(self):
        site, member, shared, run = self.team_site()
        lustre = str(site.roots["lustre"] / "experiment")
        for target, operation in (
            (member / "results" / "run-1", "archive-lustre"),
            (shared / "results", "archive-lustre"),
        ):
            options = cli.parser().parse_args(
                ["data", "copy", lustre, str(target), "--prefect"]
            )
            request = prefect.prepare(options, site)
            self.assertEqual(request["operation"], operation)
            self.assertEqual(request["destination"], str(target))
        between = cli.parser().parse_args([
            "data", "copy", str(member), str(shared / "from-member"),
            "--prefect",
        ])
        request = prefect.prepare(between, site)
        self.assertEqual(request["operation"], "copy-objects")
        self.assertEqual(request["source"], str(member))
        restore = cli.parser().parse_args([
            "data", "copy", str(run), str(site.roots["lustre"] / "run-1"),
            "--prefect",
        ])
        request = prefect.prepare(restore, site)
        self.assertEqual(request["operation"], "stage-lustre")
        self.assertEqual(request["source"], str(run))

    def test_alias_is_sent_as_the_folder_it_resolves_to(self):
        user    = pwd.getpwuid(os.getuid()).pw_name
        backend = self.base / ".alluxio-personal" / "personal-shard-4" / user
        backend.mkdir(parents = True)
        entry = self.base / "bucket" / user
        (entry / "experiment").rmdir()
        entry.rmdir()
        entry.symlink_to(backend)
        site    = Site(self.base / "site.conf")
        options = cli.parser().parse_args([
            "data", "copy", str(site.roots["lustre"] / "experiment"),
            str(entry / "saved"), "--prefect",
        ])
        self.assertEqual(
            prefect.prepare(options, site)["destination"],
            str(backend.resolve() / "saved"),
        )

    def test_copy_between_object_mounts_refuses_archive_options(self):
        for flags in (
            ("--archive",), ("--pack", "tar"), ("--pack-small",),
            ("--chunk-size", "64MiB"), ("--job-size", "small"),
        ):
            options             = self.options("alluxio", "alluxio", *flags)
            options.destination = self.path("alluxio", "copy.gbi.tar")
            with (
                self.subTest(flags = flags),
                self.assertRaisesRegex(ValueError, "do not apply to a copy"),
            ):
                prefect.prepare(options, self.site)

    def test_automatic_archive_intent_is_explicit_and_uses_managed_preview(
                self,
            ):
        for source in ("lustre", "fss"):
            self.assertNotIn(
                "archive_auto",
                prefect.prepare(self.options(source, "alluxio"), self.site),
            )
            request = prefect.prepare(
                self.options(source, "alluxio", "--archive", "--dry-run"),
                self.site,
            )
            self.assertTrue(request["archive_auto"])
            self.assertTrue(request["dry_run"])
            self.assertTrue(
                {
                    "archive_format", "pack_small", "chunk_size",
                    "packing_policy",
                }.isdisjoint(request)
            )

    def test_automatic_archiving_rejects_restore_and_manual_chunk_size(self):
        with self.assertRaisesRegex(ValueError, "only.*archives"):
            prefect.prepare(
                self.options("alluxio", "lustre", "--archive"), self.site
            )
        with self.assertRaisesRegex(ValueError, "chooses packing"):
            prefect.prepare(
                self.options(
                    "lustre", "alluxio", "--archive", "--chunk-size", "64MiB"
                ), self.site,
            )

    def test_restore_job_size_preserves_default_and_explicit_named_values(self):
        for target in ("lustre", "fss"):
            self.assertNotIn(
                "job_size",
                prefect.prepare(self.options("alluxio", target), self.site),
            )
            for size in ("small", "large"):
                request = prefect.prepare(
                    self.options("alluxio", target, "--job-size", size),
                    self.site,
                )
                self.assertEqual(request["job_size"], size)

    def test_job_size_refuses_archive_ordinary_and_unknown_requests(self):
        for source in ("lustre", "fss"):
            with self.assertRaisesRegex(ValueError, "only.*restores"):
                prefect.prepare(
                    self.options(source, "alluxio", "--job-size", "small"),
                    self.site,
                )
        ordinary = cli.parser().parse_args(
            ["data", "copy", "missing", "also-missing", "--job-size", "small"]
        )
        with self.assertRaisesRegex(ValueError, "only.*restores"):
            cli.plan(ordinary, self.site)
        with (
            contextlib.redirect_stderr(io.StringIO()),
            self.assertRaises(SystemExit),
        ):
            self.options("alluxio", "lustre", "--job-size", "medium")

    def test_restore_job_size_dry_run_displays_choice_without_state(self):
        output = io.StringIO()
        with (
            patch("gbi_data.prefect.exchange") as exchange,
            contextlib.redirect_stdout(output),
        ):
            options = self.options(
                "alluxio", "lustre", "--job-size", "small", "--dry-run"
            )
            self.assertEqual(prefect.start(options, self.site, self.home), 0)
        exchange.assert_not_called()
        self.assertFalse(self.home.exists())
        self.assertIn("Restore job size: small", output.getvalue())

    def test_deletion_flag_paths_and_overlap_refused(self):
        with self.assertRaisesRegex(ValueError, "applies only to move"):
            prefect.prepare(
                self.options("lustre", "alluxio", "--delete-source"), self.site
            )
        plain = self.base / "plain"
        plain.mkdir()
        reserved = self.site.roots["lustre"] / ".gbi" / "state"
        reserved.mkdir(parents = True)
        for path, message in (
            (self.base / "missing", "does not exist"),
            (plain, "supports Lustre, FSS and Object Storage"),
            (reserved, "reserved"),
        ):
            options        = self.options()
            options.source = str(path)
            with self.subTest(path = path):
                with self.assertRaisesRegex(ValueError, message):
                    prefect.prepare(options, self.site)
        options             = self.options("alluxio", "alluxio")
        options.destination = self.path("alluxio", "experiment/inside")
        with self.assertRaisesRegex(ValueError, "overlap"):
            prefect.prepare(options, self.site)

    def test_courtesy_checks_name_the_path_the_caller_cannot_use(self):
        if os.geteuid() == 0:
            self.skipTest("root bypasses permission bits")
        source = Path(self.path("lustre"))
        locked = self.site.roots["alluxio"] / "locked"
        locked.mkdir()
        locked.chmod(0o500)
        self.addCleanup(locked.chmod, 0o700)
        options             = self.options()
        options.destination = str(locked / "new")
        with self.assertRaisesRegex(ValueError, f"cannot write {locked}"):
            prefect.prepare(options, self.site)
        source.chmod(0o300)
        self.addCleanup(source.chmod, 0o700)
        with self.assertRaisesRegex(ValueError, f"cannot read {source}"):
            prefect.prepare(self.options(), self.site)
        source.chmod(0o700)
        kept = source / "kept"
        kept.mkdir()
        kept.chmod(0o500)
        self.addCleanup(kept.chmod, 0o700)
        moving = self.moving(str(source), self.path("alluxio", "moved"))
        with self.assertRaisesRegex(ValueError, f"cannot remove from {kept}"):
            prefect.prepare(moving, self.site)
        copying = cli.parser().parse_args(
            ["data", "copy", str(source), self.path("alluxio", "moved"),
             "--prefect"]
        )
        self.assertEqual(prefect.prepare(copying, self.site)["verb"], "copy")

    def test_move_removes_filesystem_and_object_to_object_sources(self):
        for source, target, operation in (
            ("lustre", "alluxio", "archive-lustre"),
            ("fss", "alluxio", "archive-fss"),
            ("alluxio", "alluxio", "copy-objects"),
        ):
            with (
                self.subTest(operation = operation),
                patch("builtins.input") as ask,
            ):
                request = prefect.prepare(
                    self.moving(self.path(source), self.path(target, "moved")),
                    self.site,
                )
                self.assertEqual(request["operation"], operation)
                self.assertEqual(request["verb"], "move")
                ask.assert_not_called()

    def test_object_to_filesystem_move_asks_or_keeps_the_source(self):
        source = self.path("alluxio")
        target = self.path("lustre", "restored")
        for answer, verb in (("y", "move"), ("yes", "move"), ("", "copy"),
                             ("n", "copy")):
            output = io.StringIO()
            with (
                self.subTest(answer = answer),
                patch("gbi_data.moves.terminal", return_value = True),
                patch("builtins.input", return_value = answer) as ask,
                contextlib.redirect_stdout(output),
            ):
                request = prefect.prepare(self.moving(source, target), self.site)
            self.assertEqual(request["verb"], verb)
            self.assertIn("[y/N]", ask.call_args.args[0])
            if verb == "copy":
                self.assertIn("Keeping the Object Storage source", output.getvalue())
        output = io.StringIO()
        with (
            patch("builtins.input") as ask, contextlib.redirect_stdout(output),
        ):
            request = prefect.prepare(self.moving(source, target), self.site)
        ask.assert_not_called()
        self.assertEqual(request["verb"], "copy")
        self.assertIn("No terminal and no --delete-source", output.getvalue())
        self.assertEqual(len(output.getvalue().splitlines()), 1)
        with (
            patch("gbi_data.moves.terminal", return_value = True),
            patch("builtins.input") as ask,
        ):
            request = prefect.prepare(
                self.moving(source, target, "--delete-source"), self.site
            )
        ask.assert_not_called()
        self.assertEqual(request["verb"], "move")
        output = io.StringIO()
        with (
            patch("gbi_data.prefect.exchange") as exchange,
            patch("builtins.input") as ask,
            contextlib.redirect_stdout(output),
        ):
            moving = self.moving(source, target, "--dry-run")
            self.assertEqual(prefect.start(moving, self.site, self.home), 0)
        ask.assert_not_called()
        exchange.assert_not_called()
        self.assertIn("a real move asks", output.getvalue())

    def test_chunk_file_directory_and_renamed_destination(self):
        for source in ("lustre", "fss"):
            options = self.options(source, "alluxio", "--chunk-size", "64MiB")
            options.destination += "-renamed"
            path = Path(options.source)
            path.rmdir()
            path.write_bytes(b"payload")
            request = prefect.prepare(options, self.site)
            self.assertEqual(request["chunk_size"], 64 * 1024 * 1024)
            self.assertTrue(request["source_is_file"])
            self.assertEqual(
                request["destination"], self.path("alluxio", "experiment-renamed")
            )
            options.destination += ".gbi-chunks"
            with self.assertRaisesRegex(ValueError, "automatic"):
                prefect.prepare(options, self.site)
            path.unlink()
            path.mkdir()
            self.assertNotIn(
                "source_is_file", prefect.prepare(options, self.site)
            )

    def test_chunked_gzip_keeps_exact_target_without_file_shape(self):
        options = self.options(
            "lustre", "alluxio", "--pack", "gzip", "--chunk-size", "64MiB",
            "--dry-run",
        )
        options.destination += ".gbi.tar.gz"
        request             = prefect.prepare(options, self.site)
        self.assertEqual(request["archive_format"], "gzip")
        self.assertEqual(
            request["destination"], self.path("alluxio", "experiment.gbi.tar.gz")
        )
        self.assertNotIn("source_is_file", request)
        self.assertTrue(request["dry_run"])

    def test_symlinked_sources_resolve_and_chunk_options_are_checked(self):
        options = self.options("lustre", "alluxio", "--chunk-size", "64MiB")
        actual  = self.site.roots["lustre"] / "actual"
        actual.mkdir()
        (actual / "file").write_bytes(b"payload")
        Path(options.source).rmdir()
        Path(options.source).symlink_to(actual, target_is_directory = True)
        request = prefect.prepare(options, self.site)
        self.assertEqual(request["source"], str(actual))
        self.assertNotIn("source_is_file", request)
        options.source = str(Path(options.source) / "file")
        request        = prefect.prepare(options, self.site)
        self.assertEqual(request["source"], str(actual / "file"))
        self.assertTrue(request["source_is_file"])
        for size in (True, False, 0, -1, 1.5, "64MiB"):
            options.chunk_size = size
            with self.assertRaisesRegex(ValueError, "positive integer"):
                prefect.prepare(options, self.site)
        for target in ("lustre", "fss"):
            with self.assertRaisesRegex(ValueError, "only.*archives"):
                prefect.prepare(
                    self.options("alluxio", target, "--chunk-size", "64MiB"),
                    self.site,
                )

    def test_portable_archive_formats_and_default_omission(self):
        for source in ("lustre", "fss"):
            default = prefect.prepare(
                self.options(source, "alluxio"), self.site
            )
            self.assertTrue(
                {
                    "archive_format", "pack_small", "packing_policy", "dry_run",
                    "chunk_size", "source_is_file",
                }.isdisjoint(default)
            )
            for compression, suffix in (
                ("tar", ".gbi.tar"), ("gzip", ".gbi.tar.gz"),
            ):
                options = self.options(source, "alluxio", "--pack", compression)
                options.destination += suffix
                request = prefect.prepare(options, self.site)
                self.assertEqual(request["archive_format"], compression)
                self.assertEqual(
                    request["destination"], self.path("alluxio") + suffix
                )
                self.assertNotIn("pack_small", request)
                self.assertNotIn("dry_run", request)

    def test_a_regular_file_takes_the_chunk_route_without_chunk_size(self):
        """A plain or --archive request for one file is the direct file store."""
        from gbi_data import archive_plan, chunks

        for source in ("lustre", "fss"):
            for flags in ((), ("--archive",)):
                with self.subTest(source = source, flags = flags):
                    options = self.options(source, "alluxio", *flags)
                    path    = Path(options.source)
                    if path.is_dir():
                        path.rmdir()
                    path.write_bytes(b"payload")
                    request = prefect.prepare(options, self.site)
                    self.assertTrue(request["source_is_file"])
                    self.assertEqual(
                        request["chunk_size"], chunks.DEFAULT_CHUNK_SIZE
                    )
                    self.assertNotIn("archive_auto", request)
                    self.assertEqual(request["source"], self.path(source))
                    self.assertEqual(
                        request["destination"], self.path("alluxio")
                    )
                    self.assertEqual(request["include"], [])
                    path.unlink()
                    path.mkdir()
                    request = prefect.prepare(options, self.site)
                    self.assertNotIn("source_is_file", request)
                    self.assertNotIn("chunk_size", request)
                    self.assertEqual(
                        request.get("archive_auto"), True if flags else None
                    )
        self.assertEqual(
            archive_plan.auto_chunk_size(0), chunks.DEFAULT_CHUNK_SIZE
        )
        self.assertEqual(
            archive_plan.auto_chunk_size(2 ** 40),
            max(chunks.DEFAULT_CHUNK_SIZE, -(-(2 ** 40) // 4096)),
        )
        with self.assertRaises(ValueError):
            archive_plan.auto_chunk_size(-1)

    def test_a_single_file_keeps_an_explicit_chunk_size_and_dry_run(self):
        options = self.options("fss", "alluxio", "--chunk-size", "64MiB", "--dry-run")
        path    = Path(options.source)
        path.rmdir()
        path.write_bytes(b"payload")
        request = prefect.prepare(options, self.site)
        self.assertEqual(request["chunk_size"], 64 * 1024 * 1024)
        self.assertTrue(request["source_is_file"])
        self.assertTrue(request["dry_run"])
        options = self.options("fss", "alluxio", "--dry-run")
        request = prefect.prepare(options, self.site)
        self.assertTrue(request["source_is_file"])
        self.assertTrue(
            request["dry_run"], "an automatic single-file store is a managed dry run"
        )

    def test_a_directory_destination_receives_the_file_by_name(self):
        options = self.options("fss", "alluxio")
        path    = Path(options.source)
        path.rmdir()
        path.write_bytes(b"payload")
        options.destination = self.path("alluxio", "folder") + "/"
        request = prefect.prepare(options, self.site)
        self.assertEqual(
            request["destination"], self.path("alluxio", "folder/experiment")
        )
        options.destination = self.path("alluxio", "renamed")
        request = prefect.prepare(options, self.site)
        self.assertEqual(request["destination"], self.path("alluxio", "renamed"))
        options.destination = self.path("alluxio", "renamed.gbi-chunks")
        with self.assertRaisesRegex(ValueError, "automatic"):
            prefect.prepare(options, self.site)

    def test_a_single_file_refuses_filters_missing_and_special_files(self):
        options = self.options("fss", "alluxio", "--include", "*.txt")
        path    = Path(options.source)
        path.rmdir()
        path.write_bytes(b"payload")
        with self.assertRaisesRegex(ValueError, "single-file source"):
            prefect.prepare(options, self.site)
        options = self.options("fss", "alluxio")
        path.unlink()
        path.symlink_to(self.site.roots["fss"] / "elsewhere")
        with self.assertRaisesRegex(ValueError, "does not exist"):
            prefect.prepare(options, self.site)
        path.unlink()
        os.mkfifo(path)
        with self.assertRaisesRegex(ValueError, "regular file or directory"):
            prefect.prepare(options, self.site)

    def test_small_file_policy_matches_ordinary_site_settings(self):
        options = self.options("lustre", "alluxio", "--pack-small")
        self.site.values.update(
            pack_small_bytes = "2048", pack_min_files = "7",
            pack_max_bytes = "65536", inline_scan_entries = "321",
            inline_probe_seconds = "2.5",
        )
        request = prefect.prepare(options, self.site)
        self.assertTrue(request["pack_small"])
        self.assertEqual(
            request["packing_policy"],
            cli.plan(options, self.site)["packing_policy"],
        )
        self.assertNotIn("archive_format", request)

    def test_packing_rejects_restore_and_invalid_destination_before_submission(
                self,
            ):
        for target in ("lustre", "fss"):
            for flags in (("--pack", "tar"), ("--pack-small",)):
                with (
                    self.subTest(target = target, flags = flags),
                    self.assertRaisesRegex(ValueError, "only.*archives"),
                ):
                    prefect.prepare(
                        self.options("alluxio", target, *flags), self.site
                    )
        with self.assertRaisesRegex(ValueError, "exact destination"):
            prefect.prepare(
                self.options("lustre", "alluxio", "--pack", "gzip"), self.site
            )

    def test_managed_packing_dry_run_saves_exact_request_and_wait_choice(self):
        for flags in (
            ("--pack-small",), ("--pack", "tar"), ("--chunk-size", "64MiB"),
        ):
            options = self.options(
                "fss", "alluxio", *flags, "--dry-run", "--wait"
            )
            if options.pack:
                options.destination += ".gbi.tar"
            output = io.StringIO()
            with (
                patch("gbi_data.prefect.submit", return_value = 0) as submit,
                contextlib.redirect_stdout(output),
            ):
                self.assertEqual(
                    prefect.start(options, self.site, self.home), 0
                )
            run_dir, site, wait = submit.call_args.args
            request = json.loads((run_dir / "request.json").read_text())[
                "broker_request"
            ]
            self.assertTrue(request["dry_run"])
            self.assertTrue(wait)
            self.assertIs(site, self.site)
            self.assertIn("tracking records", output.getvalue())
            self.assertIn(
                "no data, receipt, destination writes", output.getvalue()
            )
            self.assertNotIn("no migration submitted", output.getvalue())

    def test_exclusions_preserve_selection_for_all_routes(self):
        for source, target in (
            ("lustre", "alluxio"), ("fss", "alluxio"), ("alluxio", "lustre"),
            ("alluxio", "fss"),
        ):
            options = self.options(
                source, target, "--include", "*.pt", "--include", "*.pt.*",
                "--exclude", "*.tmp", "--exclude", "unfinished*",
            )
            with self.subTest(source = source, target = target):
                request = prefect.prepare(options, self.site)
                self.assertEqual(request["include"], ["*.pt", "*.pt.*"])
                self.assertEqual(request["exclude"], ["*.tmp", "unfinished*"])
        self.assertNotIn("exclude", prefect.prepare(self.options(), self.site))

    def test_invalid_exclusions_are_rejected_before_submission(self):
        for patterns in (
            ["subdir/*.pt"], [""], ["a\nb"], ["{a,b}"], ["x" * 256],
            ["*.tmp"] * 101,
        ):
            options         = self.options()
            options.exclude = patterns
            with (
                self.subTest(patterns = patterns),
                self.assertRaisesRegex(ValueError, "--exclude"),
            ):
                prefect.prepare(options, self.site)

    def test_distinct_destination_and_filters_preserved_for_all_routes(self):
        for source, target in (
            ("lustre", "alluxio"), ("fss", "alluxio"), ("alluxio", "lustre"),
            ("alluxio", "fss"),
        ):
            with self.subTest(source = source, target = target):
                options = self.options(
                    source, target, "--include", "*.pt", "--exclude", "*.tmp"
                )
                options.destination = self.path(target, "renamed/subdir")
                request = prefect.prepare(options, self.site)
                self.assertEqual(request["source"], self.path(source))
                self.assertEqual(
                    request["destination"], self.path(target, "renamed/subdir")
                )
                self.assertEqual(request["include"], ["*.pt"])
                self.assertEqual(request["exclude"], ["*.tmp"])

    def test_destination_rejects_unknown_storage_reserved_and_wildcard_paths(
                self,
            ):
        for value in (
            self.base / "other", self.site.roots["fss"] / ".." / "other",
            self.site.roots["fss"] / ".prefect-receipts/data",
            self.site.roots["fss"] / "renamed" / "transfer-locks/data",
            self.site.roots["fss"] / "renamed/*.pt",
            self.site.roots["fss"] / "missing/../other",
        ):
            options             = self.options("alluxio", "fss")
            options.destination = str(value)
            with self.subTest(value = value), self.assertRaises(ValueError):
                prefect.prepare(options, self.site)

    def test_dry_run_creates_no_state_or_submission(self):
        output = io.StringIO()
        with (
            patch("gbi_data.prefect.exchange") as exchange,
            contextlib.redirect_stdout(output),
        ):
            options = self.options(
                "lustre", "alluxio", "--dry-run", "--include", "*.pt",
                "--exclude", "unfinished*",
            )
            self.assertEqual(prefect.start(options, self.site, self.home), 0)
        exchange.assert_not_called()
        self.assertFalse(self.home.exists())
        self.assertIn('Include basenames: ["*.pt"]', output.getvalue())
        self.assertIn('Exclude basenames: ["unfinished*"]', output.getvalue())

    def test_socket_exchange_uses_saved_request_and_validates_reply(self):
        request = prefect.prepare(self.options(), self.site)
        run_id  = "74ec9296-0446-4a46-a428-f004e556f066"
        response = {
            "version": 1, "run_id": run_id, "state": "COMPLETED",
            "terminal": False,
        }
        with patch("gbi_data.prefect.socket.socket") as factory:
            client = factory.return_value.__enter__.return_value
            client.makefile.return_value = io.BytesIO(
                json.dumps(response).encode() + b"\n"
            )
            result = prefect.exchange(self.site, request)
            client.connect.assert_called_once_with(
                self.site.values["prefect_socket"]
            )
            self.assertEqual(
                json.loads(client.sendall.call_args.args[0]), request
            )
            self.assertTrue(result["terminal"])
        for raw in (
            b"broken\n", b"{}",
            b'{"version":1,"state":"COMPLETED","run_id":null}\n',
        ):
            with (
                self.subTest(raw = raw),
                patch("gbi_data.prefect.socket.socket") as factory,
            ):
                factory.return_value.__enter__.return_value.makefile.return_value = io.BytesIO(
                    raw
                )
                with self.assertRaisesRegex(ValueError, "same transfer ID"):
                    prefect.exchange(self.site, request)

    def test_https_fallback_only_handles_unavailable_socket(self):
        request = prefect.prepare(self.options(), self.site)
        response = {
            "version": 1, "state": "SCHEDULED",
            "run_id": "74ec9296-0446-4a46-a428-f004e556f066",
        }
        with (
            patch(
                "gbi_data.prefect._exchange_socket",
                side_effect = prefect._BrokerUnavailable("missing"),
            ),
            patch("gbi_data.prefect._exchange_https") as https,
        ):
            with self.assertRaisesRegex(
                ValueError, "login node.*No request was sent"
            ):
                prefect.exchange(self.site, request)
        https.assert_not_called()
        self.site.values["prefect_url"] = "https://broker.example/prefect"
        with (
            patch(
                "gbi_data.prefect._exchange_socket",
                side_effect = prefect._BrokerUnavailable("missing"),
            ),
            patch(
                "gbi_data.prefect._exchange_https", return_value = response
            ) as https,
        ):
            self.assertEqual(prefect.exchange(self.site, request), response)
        https.assert_called_once_with(self.site.values["prefect_url"], request)
        with (
            patch(
                "gbi_data.prefect._exchange_socket",
                side_effect = ValueError("partial reply"),
            ),
            patch("gbi_data.prefect._exchange_https") as https,
        ):
            with self.assertRaisesRegex(ValueError, "partial reply"):
                prefect.exchange(self.site, request)
        https.assert_not_called()
        with (
            patch(
                "gbi_data.prefect._exchange_socket",
                side_effect = PermissionError("denied"),
            ),
            patch("gbi_data.prefect._exchange_https") as https,
        ):
            with self.assertRaisesRegex(ValueError, "denied"):
                prefect.exchange(self.site, request)
        https.assert_not_called()

    def test_cache_transport_failure_names_the_cache_service(self):
        request = {"version": 1, "action": "status", "operation": "cache"}
        with patch(
            "gbi_data.prefect._exchange_socket", side_effect = ValueError("lost")
        ):
            with self.assertRaisesRegex(ValueError, "Cache status unavailable"):
                prefect.exchange(self.site, request)

    def test_https_url_rejects_unsafe_forms(self):
        for value in (
            "http://broker.example", "https://user:pass@broker.example",
            "https://broker.example/?x=1", "https://broker.example/#fragment",
        ):
            with self.subTest(value = value):
                self.site.values["prefect_url"] = value
                with self.assertRaises(ValueError):
                    prefect._https_url(self.site)

    def test_slurm_token_is_cached_without_leaking_command_output(self):
        completed = subprocess.CompletedProcess(
            ("scontrol", "token"), 0, stdout = "SLURM_JWT=secret-token\n",
            stderr = "",
        )
        with (
            patch(
                "gbi_data.prefect.subprocess.run", return_value = completed
            ) as run,
            patch(
                "gbi_data.prefect.time.monotonic", side_effect = [100, 140, 146]
            ),
            patch.dict(
                os.environ,
                {"SLURM_JWT": "inherited-secret", "PATH": "/bin"},
                clear = True,
            ),
        ):
            self.assertEqual(prefect._token(), "secret-token")
            self.assertEqual(prefect._token(), "secret-token")
            self.assertEqual(run.call_count, 1)
            self.assertEqual(prefect._token(), "secret-token")
            self.assertEqual(run.call_count, 2)
        self.assertEqual(
            run.call_args.args, (("scontrol", "token", "lifespan=60"),)
        )
        self.assertEqual(run.call_args.kwargs["env"], {"PATH": "/bin"})

    def test_token_failures_do_not_expose_process_output(self):
        for error in (
            FileNotFoundError("private command details"),
            subprocess.CalledProcessError(
                1, "scontrol", output = "private token", stderr = "private error"
            ),
            subprocess.TimeoutExpired("scontrol", 10, output = "private token"),
        ):
            with (
                self.subTest(error = type(error).__name__),
                patch("gbi_data.prefect.subprocess.run", side_effect = error),
                self.assertRaisesRegex(
                    ValueError, "cannot obtain Slurm user token"
                ) as raised,
            ):
                prefect._token()
            self.assertNotIn("private", str(raised.exception))
            self.assertTrue(raised.exception.__suppress_context__)

    def test_https_preserves_request_and_public_errors_but_bounds_untrusted_responses(
                self,
            ):
        request = prefect.prepare(self.options(), self.site)
        response = {
            "version": 1, "state": "COMPLETED",
            "run_id": "74ec9296-0446-4a46-a428-f004e556f066",
        }
        with (
            patch("gbi_data.prefect._token", return_value = "private-token"),
            patch("gbi_data.prefect.urlrequest.build_opener") as factory,
        ):
            opener = factory.return_value
            opener.open.return_value.__enter__.return_value.read.return_value = json.dumps(
                response
            ).encode()
            self.assertTrue(
                prefect._exchange_https(
                    "https://broker.example/migrations", request
                )["terminal"]
            )
            submitted = opener.open.call_args.args[0]
            self.assertEqual(json.loads(submitted.data), request)
            self.assertEqual(
                submitted.get_header("Authorization"), "Bearer private-token"
            )
            for raw, message in (
                (
                    b'{"version":1,"error":"your route is unavailable"}',
                    "your route is unavailable",
                ),
                (b"private broken body", "response was invalid"),
                (b"x" * (prefect._MAX_RESPONSE + 1), "response was invalid"),
            ):
                opener.open.return_value.__enter__.return_value.read.return_value = (
                    raw
                )
                with self.assertRaisesRegex(ValueError, message) as raised:
                    prefect._exchange_https(
                        "https://broker.example/migrations", request
                    )
                self.assertNotIn("private", str(raised.exception))

    def test_https_transport_failures_and_redirects_do_not_expose_credentials(
                self,
            ):
        errors = (
            urlerror.HTTPError(
                "https://private.example", 401, "private-token", {},
                io.BytesIO(b"private body"),
            ), urlerror.URLError("private-token"),
            TimeoutError("private-token"),
        )
        for error in errors:
            with (
                self.subTest(error = type(error).__name__),
                patch("gbi_data.prefect._token", return_value = "private-token"),
                patch("gbi_data.prefect.urlrequest.build_opener") as factory,
            ):
                factory.return_value.open.side_effect = error
                with self.assertRaisesRegex(
                    ValueError, "HTTPS request failed"
                ) as raised:
                    prefect._exchange_https(
                        "https://broker.example/migrations", {}
                    )
                self.assertNotIn("private", str(raised.exception))
                self.assertTrue(raised.exception.__suppress_context__)
        with self.assertRaisesRegex(ValueError, "redirected"):
            prefect._NoRedirect().redirect_request(
                None, None, 307, None, {}, "https://other.example"
            )

    def test_socket_write_failure_never_switches_transport(self):
        self.site.values["prefect_url"] = "https://broker.example/migrations"
        for error in (
            BrokenPipeError(errno.EPIPE, "lost write"),
            TimeoutError("lost reply"),
        ):
            with (
                self.subTest(error = type(error).__name__),
                patch("gbi_data.prefect.socket.socket") as factory,
                patch("gbi_data.prefect._exchange_https") as remote,
            ):
                client = factory.return_value.__enter__.return_value
                client.sendall.side_effect = error
                with self.assertRaisesRegex(ValueError, "same transfer ID"):
                    prefect.exchange(
                        self.site, prefect.prepare(self.options(), self.site)
                    )
                remote.assert_not_called()

    def test_missing_socket_falls_back_without_changing_request(self):
        self.site.values["prefect_url"] = "https://broker.example/migrations"
        request = prefect.prepare(self.options(), self.site)
        for code in (errno.ENOENT, errno.ECONNREFUSED):
            with (
                self.subTest(code = code),
                patch("gbi_data.prefect.socket.socket") as factory,
                patch(
                    "gbi_data.prefect._exchange_https",
                    return_value = {"state": "RUNNING"},
                ) as remote,
            ):
                client = factory.return_value.__enter__.return_value
                client.connect.side_effect = OSError(code, "unavailable")
                prefect.exchange(self.site, request)
                client.sendall.assert_not_called()
                remote.assert_called_once_with(
                    self.site.values["prefect_url"], request
                )

    def test_lost_reply_retry_resends_identical_saved_request(self):
        sent = []

        def lost_reply(_site, request):
            sent.append(request)
            self.assertEqual(
                len(list((self.home / "runs").glob("*/request.json"))), 1
            )
            raise ValueError("lost reply")

        with (
            patch("gbi_data.prefect.exchange", side_effect = lost_reply),
            patch("sys.stdout.isatty", return_value = False),
        ):
            with self.assertRaisesRegex(ValueError, "lost reply"):
                prefect.start(
                    self.options(
                        "lustre", "alluxio", "--include", "*.pt", "--exclude",
                        "unfinished*",
                    ), self.site, self.home,
                )
        run_dir   = next((self.home / "runs").iterdir())
        persisted = (run_dir / "request.json").read_bytes()
        self.assertEqual(sent[0]["exclude"], ["unfinished*"])
        response = {"version": 1, "state": "RUNNING", "terminal": False}
        with patch(
            "gbi_data.prefect.exchange", return_value = response
        ) as exchange:
            self.assertEqual(prefect.submit(run_dir, self.site), 0)
        self.assertEqual(exchange.call_args.args[1], sent[0])
        self.assertEqual((run_dir / "request.json").read_bytes(), persisted)

    def test_restore_retry_keeps_saved_job_size_and_request_identity(self):
        options             = self.options("alluxio", "lustre", "--job-size", "small")
        options.destination = str(self.site.roots["lustre"] / "restored/subdir")
        with (
            patch("gbi_data.prefect.exchange", side_effect = ValueError("lost")),
            patch("sys.stdout.isatty", return_value = False),
        ):
            with self.assertRaises(ValueError):
                prefect.start(options, self.site, self.home)
        run_dir   = next((self.home / "runs").iterdir())
        persisted = (run_dir / "request.json").read_bytes()
        saved     = prefect.stored_request(run_dir)
        self.assertEqual(saved["job_size"], "small")
        self.assertEqual(saved["destination"], self.path("lustre", "restored/subdir"))
        options.job_size    = "large"
        options.destination = str(self.site.roots["lustre"] / "later-change")
        with patch(
            "gbi_data.prefect.exchange",
            return_value = {"version": 1, "state": "RUNNING", "terminal": False},
        ) as exchange:
            self.assertEqual(prefect.submit(run_dir, self.site), 0)
        self.assertEqual(exchange.call_args.args[1], saved)
        self.assertEqual((run_dir / "request.json").read_bytes(), persisted)

    def test_packing_plan_retry_preserves_original_policy_and_dry_run(self):
        options = self.options("fss", "alluxio", "--pack-small", "--dry-run")
        with (
            patch("gbi_data.prefect.exchange", side_effect = ValueError("lost")),
            patch("sys.stdout.isatty", return_value = False),
        ):
            with self.assertRaisesRegex(ValueError, "lost"):
                prefect.start(options, self.site, self.home)
        run_dir = next((self.home / "runs").iterdir())
        saved   = prefect.stored_request(run_dir)
        self.assertTrue(saved["pack_small"] and saved["dry_run"])
        self.site.values["pack_min_files"] = "999"
        options.dry_run = False
        response        = {"version": 1, "state": "RUNNING", "terminal": False}
        with patch(
            "gbi_data.prefect.exchange", return_value = response
        ) as exchange:
            self.assertEqual(prefect.submit(run_dir, self.site), 0)
        self.assertEqual(exchange.call_args.args[1], saved)

    def test_status_recovers_by_request_id_without_resubmission(self):
        with (
            patch("gbi_data.prefect.exchange", side_effect = ValueError("lost")),
            patch("sys.stdout.isatty", return_value = False),
        ):
            with self.assertRaises(ValueError):
                prefect.start(self.options(), self.site, self.home)
        run_dir = next((self.home / "runs").iterdir())
        request = prefect.stored_request(run_dir)
        with patch(
            "gbi_data.prefect.exchange",
            return_value = {"version": 1, "state": "COMPLETED", "terminal": True},
        ) as exchange:
            self.assertEqual(prefect.follow(run_dir, self.site, True), 0)
        self.assertEqual(
            exchange.call_args.args[1],
            {
                "version": 1, "action": "status",
                "request_id": request["request_id"],
            },
        )
        self.assertTrue((run_dir / "request.json").exists())

    def test_ordinary_request_cannot_be_retried_through_broker(self):
        run_dir = self.base / "ordinary"
        run_dir.mkdir()
        (run_dir / "request.json").write_text(
            json.dumps({"execution": "slurm", "uid": os.getuid()})
        )
        with (
            patch("gbi_data.prefect.exchange") as exchange,
            self.assertRaisesRegex(ValueError, "re-running"),
        ):
            prefect.submit(run_dir, self.site)
        exchange.assert_not_called()

    def test_native_cache_retry_names_the_original_cache_command(self):
        run_dir = self.base / "cache"
        run_dir.mkdir()
        (run_dir / "request.json").write_text(json.dumps({
            "execution": "cache", "uid": os.getuid(),
        }))
        with (
            patch("gbi_data.prefect.exchange") as exchange,
            self.assertRaisesRegex(ValueError, "original gbi data cache command"),
        ):
            prefect.submit(run_dir, self.site)
        exchange.assert_not_called()

    def test_terminal_failure_and_missing_submission_are_not_success(self):
        run_dir = self.base / "record"
        run_dir.mkdir()
        (run_dir / "request.json").write_text(
            json.dumps(
                {
                    "execution": "prefect", "uid": os.getuid(),
                    "broker_request": prefect.prepare(
                        self.options(), self.site
                    ),
                }
            )
        )
        for state, terminal in (("FAILED", True), ("NOT_SUBMITTED", False)):
            with (
                self.subTest(state = state),
                patch(
                    "gbi_data.prefect.exchange",
                    return_value = {
                        "version": 1, "state": state, "terminal": terminal,
                    },
                ),
            ):
                self.assertEqual(prefect.follow(run_dir, self.site, True), 1)

    def test_terminal_state_shows_the_flows_reason(self):
        run_dir = self.base / "refused"
        run_dir.mkdir()
        (run_dir / "request.json").write_text(json.dumps({
            "execution": "prefect", "uid": os.getuid(),
            "broker_request": prefect.prepare(self.options(), self.site),
        }))
        for state, message, shown in (
            ("FAILED",
             "Flow run encountered an exception: TransferRefused: "
             "cannot read /mnt/lustre/u/x/locked: permission denied",
             "Prefect: cannot read /mnt/lustre/u/x/locked: permission denied"),
            ("COMPLETED", "shared: 3 of 4 entries set", "Prefect: shared: 3 of 4 entries set"),
        ):
            output = io.StringIO()
            with (
                self.subTest(state = state),
                patch("gbi_data.prefect.exchange", return_value = {
                    "version": 1, "state": state, "terminal": True,
                    "run_id": "11111111-2222-3333-4444-555555555555", "message": message,
                }),
                contextlib.redirect_stdout(output),
            ):
                prefect.follow(run_dir, self.site, True)
            self.assertIn(shown + "\n", output.getvalue())

    def monitoring_record(self):
        run_dir = self.base / "monitoring"
        run_dir.mkdir()
        request = prefect.prepare(self.options(), self.site)
        (run_dir / "request.json").write_text(json.dumps({
            "execution": "prefect", "uid": os.getuid(),
            "broker_request": request,
        }))
        return run_dir, request

    def test_status_retries_transport_and_backend_loss_by_confirmed_run(self):
        run_dir, request = self.monitoring_record()
        initial = {
            "version": 1, "state": "RUNNING", "terminal": False,
            "run_id": "11111111-2222-3333-4444-555555555555",
        }
        completed = {**initial, "state": "COMPLETED", "terminal": True}
        saved = (run_dir / "request.json").read_bytes()
        with (
            patch("gbi_data.prefect._exchange_socket", side_effect = [
                TimeoutError(), prefect.TransientError("backend unavailable"),
                completed,
            ]) as exchange,
            patch("gbi_data.prefect.time.sleep") as sleep,
            contextlib.redirect_stdout(io.StringIO()),
            contextlib.redirect_stderr(io.StringIO()) as errors,
        ):
            self.assertEqual(prefect.follow(run_dir, self.site, True, initial), 0)
        expected = {"version": 1, "action": "status", "run_id": initial["run_id"]}
        self.assertEqual([call.args[1] for call in exchange.call_args_list], [expected] * 3)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [2, 2, 4])
        self.assertIn("Monitoring temporarily unavailable", errors.getvalue())
        self.assertEqual((run_dir / "request.json").read_bytes(), saved)

    def test_monitoring_exhaustion_keeps_last_state_and_exact_reconnect(self):
        run_dir, _request = self.monitoring_record()
        initial = {
            "version": 1, "state": "RUNNING", "terminal": False,
            "run_id": "11111111-2222-3333-4444-555555555555",
        }
        with (
            patch("gbi_data.prefect._exchange_socket", side_effect = TimeoutError()) as exchange,
            patch("gbi_data.prefect.time.sleep"),
            contextlib.redirect_stdout(io.StringIO()),
            contextlib.redirect_stderr(io.StringIO()) as errors,
        ):
            self.assertEqual(prefect.follow(run_dir, self.site, True, initial), 2)
        self.assertEqual(exchange.call_count, 4)
        self.assertEqual(json.loads((run_dir / "prefect.json").read_text()), initial)
        self.assertIn("current transfer state is unknown", errors.getvalue())
        self.assertIn("Last observed state: RUNNING", errors.getvalue())
        self.assertIn(f"gbi data status {run_dir.name} --watch", errors.getvalue())
        self.assertNotIn("request not confirmed", errors.getvalue())

    def test_saved_result_locates_live_run_and_corrupt_result_uses_request(self):
        run_dir, request = self.monitoring_record()
        run_id = "11111111-2222-3333-4444-555555555555"
        completed = {"version": 1, "state": "COMPLETED", "run_id": run_id}
        for saved, key, value in (
            (json.dumps(completed), "run_id", run_id),
            ('{"version":1,"state":"RUNNING","run_id":"invalid"}\n', "request_id", request["request_id"]),
            ('{"version":1,"state":"NOT_SUBMITTED","run_id":"invalid"}', "request_id", request["request_id"]),
            ("not-json\n", "request_id", request["request_id"]),
        ):
            (run_dir / "prefect.json").write_text(saved)
            with (
                self.subTest(saved = saved),
                patch("gbi_data.prefect.exchange", return_value = {**completed, "terminal": True}) as exchange,
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(prefect.follow(run_dir, self.site, True), 0)
                exchange.assert_called_once()
                self.assertEqual(exchange.call_args.args[1], {
                    "version": 1, "action": "status", key: value,
                })

    def test_status_classifies_http_errors_without_exposing_credentials(self):
        query = {"version": 1, "action": "status", "request_id": "saved"}
        self.site.values["prefect_url"] = "https://broker.example/prefect"
        for code in (408, 429, 500, 502, 503, 504, 401, 403, 404):
            transient = code in (408, 429, 500, 502, 503, 504)
            error = urlerror.HTTPError("https://private-token", code, "private-token", {}, None)
            with (
                self.subTest(code = code),
                patch("gbi_data.prefect._exchange_socket", side_effect = prefect._BrokerUnavailable()),
                patch("gbi_data.prefect._token", return_value = "private-token"),
                patch("gbi_data.prefect.urlrequest.build_opener") as factory,
                patch("gbi_data.prefect.time.sleep"),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                opener = factory.return_value
                opener.open.side_effect = error
                with self.assertRaises(ValueError) as raised:
                    prefect.status(self.site, query)
                self.assertEqual(opener.open.call_count, 4 if transient else 1)
                self.assertEqual(isinstance(raised.exception, prefect.TransientError), transient)
                self.assertNotIn("private-token", str(raised.exception))

    def test_permanent_status_errors_and_submission_never_retry(self):
        query = {"version": 1, "action": "status", "request_id": "saved"}
        for error in (
            prefect._BrokerError("run belongs to another user"),
            prefect._BrokerError("Prefect service authentication is unavailable"),
            PermissionError("permission denied"), ssl.SSLCertVerificationError("certificate"),
            ValueError("unsupported broker response"),
        ):
            with (
                self.subTest(error = error),
                patch("gbi_data.prefect._exchange_socket", side_effect = error) as exchange,
                patch("gbi_data.prefect.time.sleep") as sleep,
                self.assertRaises(ValueError),
            ):
                prefect.status(self.site, query)
            exchange.assert_called_once()
            sleep.assert_not_called()
        with (
            patch("gbi_data.prefect._exchange_socket", side_effect = TimeoutError()) as exchange,
            self.assertRaisesRegex(prefect.TransientError, "request not confirmed"),
        ):
            prefect.exchange(self.site, {**query, "action": "submit"})
        exchange.assert_called_once()

    def test_broker_error_codes_override_legacy_retry_classification(self):
        for extra, transient in (
            ({}, True), ({"error_code": "backend_unavailable"}, True),
            ({"error_code": "backend_authentication"}, False),
        ):
            raw = json.dumps({
                "version": 1, "error": "Prefect CSRF authentication is unavailable", **extra,
            }).encode() + b"\n"
            with self.subTest(extra = extra), self.assertRaises(ValueError) as raised:
                prefect._validate_result(raw)
            self.assertEqual(isinstance(raised.exception, prefect.TransientError), transient)
