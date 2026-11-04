"""Public CLI help and actionable option errors."""

import contextlib
import io
from pathlib import Path
import unittest

from gbi_data import cli


class CLIHelp(unittest.TestCase):
    def test_module_help_describes_archive_intent_and_compatibility(self):
        template = (
            Path(__file__).resolve().parents[1]
            / "sm-config/module_template.lua"
        )
        text = " ".join(template.read_text().split())
        for explanation in (
            "--archive", "chooses packing and part sizes",
            "Restores detect archives/chunks automatically",
            "--help-all", "compatible with older deployments",
            "unsupported requested options fail closed",
            "Moving Object Storage data to Lustre or FSS asks first",
            "delete_source=True",
        ):
            self.assertIn(explanation, text)
        self.assertNotIn("cannot use packing", text)

    def help_text(self, *arguments):
        output = io.StringIO()
        with (
            contextlib.redirect_stdout(output),
            self.assertRaises(SystemExit) as raised,
        ):
            cli.parser().parse_args([*arguments, "--help"])
        self.assertEqual(raised.exception.code, 0)
        return output.getvalue()

    def test_overviews_explain_routes_and_hide_internal_runner(self):
        self.assertIn("Move, archive, restore, and inspect", self.help_text())
        data = self.help_text("data")
        self.assertIn("copy", data)
        self.assertIn("owner-scoped Lustre snapshot", data)
        copy = self.help_text("data", "copy")
        self.assertIn("source file or directory", copy)
        self.assertIn("any path you can", copy)
        self.assertNotIn("always retained", copy)
        self.assertNotIn("--delete-source", copy)
        move = " ".join(self.help_text("data", "move").split())
        self.assertIn("Moving Object Storage data to Lustre or FSS asks first", move)
        self.assertIn("--delete-source", move)
        self.assertNotIn("_run", data)

    def test_overviews_explain_everyday_options_without_exposing_tuning(self):
        for arguments in ((), ("data",)):
            with self.subTest(arguments = arguments):
                text       = self.help_text(*arguments)
                normalized = " ".join(text.split())
                for flag in (
                    "--include GLOB", "--exclude GLOB", "--archive",
                    "--help-all", "--dry-run", "--detach", "--wait",
                    "--prefect", "--watch", "--depth DEPTH", "--limit LIMIT",
                ):
                    self.assertIn(flag, text)
                for explanation in (
                    "matching any supplied pattern", "wins over --include",
                    "choose packing and resumable parts automatically",
                    "follow until the transfer finishes", "levels below PATH",
                    "top N folders at each level",
                ):
                    self.assertIn(explanation, normalized)
                self.assertIn(
                    "Moving Object Storage data to Lustre or FSS asks first",
                    normalized,
                )
                self.assertIn(
                    "remove an Object Storage source after verification "
                    "without asking", normalized,
                )
                self.assertNotIn("always retained", normalized)
                for hidden in (
                    "--pack", "--pack-small", "--chunk-size", "--local",
                    "--job-size",
                ):
                    self.assertNotIn(hidden, text)
                self.assertNotIn("ordinary route only", normalized)
                self.assertNotIn("_run", text)

    def test_full_help_exposes_overrides_and_deletion_only_on_move(self):
        for arguments in ([], ["data"], ["data", "copy"], ["data", "move"]):
            output = io.StringIO()
            with (
                contextlib.redirect_stdout(output),
                self.assertRaises(SystemExit) as raised,
            ):
                cli.parser().parse_args([*arguments, "--help-all"])
            self.assertEqual(raised.exception.code, 0)
            for flag in (
                "--archive", "--archive-unit", "--pack", "--pack-small",
                "--chunk-size", "--local", "--job-size",
            ):
                self.assertIn(flag, output.getvalue())
            if arguments == ["data", "copy"]:
                self.assertNotIn("--delete-source", output.getvalue())
            else:
                self.assertIn("--delete-source", output.getvalue())

    def test_hidden_overrides_remain_parseable_for_existing_scripts(self):
        options = cli.parser().parse_args(
            [
                "data", "copy", "src", "dst", "--pack-small", "--chunk-size",
                "64MiB",
            ]
        )
        self.assertTrue(options.pack_small)
        self.assertEqual(options.chunk_size, 64 * 1024**2)

    def test_usage_help_defines_path_and_top_n_levels(self):
        usage = self.help_text("data", "usage")
        self.assertIn("relative to your own Lustre root", usage)
        self.assertIn("top N folders at each level", usage)
        self.assertIn("not live", usage)
        self.assertIn("recursive usage", usage)

    def test_execution_conflict_names_flags_and_alternative(self):
        output = io.StringIO()
        with (
            contextlib.redirect_stderr(output),
            self.assertRaises(SystemExit) as raised,
        ):
            cli.parser().parse_args(
                ["data", "copy", "src", "dst", "--local", "--prefect"]
            )
        self.assertEqual(raised.exception.code, 2)
        message = output.getvalue()
        self.assertIn("unsupported option combination", message)
        self.assertIn("--local", message)
        self.assertIn("--prefect", message)
        self.assertIn("choose one execution mode", message)


if __name__ == "__main__":
    unittest.main()
