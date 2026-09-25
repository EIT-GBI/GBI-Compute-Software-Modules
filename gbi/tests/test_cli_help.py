"""Public CLI help and actionable option errors."""

import contextlib
import io
import unittest

from gbi_data import cli


class CLIHelp(unittest.TestCase):
    def help_text(self, *arguments):
        output = io.StringIO()
        with contextlib.redirect_stdout(output), self.assertRaises(SystemExit) as raised:
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
        self.assertIn("ordinary transfer", copy)
        self.assertNotIn("_run", data)

    def test_overviews_explain_all_public_flags_without_another_help_command(self):
        for arguments in ((), ("data",)):
            with self.subTest(arguments=arguments):
                text = self.help_text(*arguments)
                normalized = " ".join(text.split())
                for flag in ("--include GLOB", "--exclude GLOB", "--pack {tar,gzip}",
                             "--pack-small", "--chunk-size SIZE", "--delete-source",
                             "--dry-run", "--detach", "--wait", "--local", "--prefect",
                             "--watch", "--depth DEPTH", "--limit LIMIT"):
                    self.assertIn(flag, text)
                for explanation in ("matching any supplied pattern", "wins over --include",
                                    "tar is uncompressed, gzip compresses",
                                    "transfer the rest as individual files",
                                    "verified resumable parts", "ordinary move only",
                                    "run inside the current Slurm allocation",
                                    "follow until the transfer finishes",
                                    "levels below PATH", "top N folders at each level"):
                    self.assertIn(explanation, normalized)
                self.assertIn("unavailable with --prefect", normalized)
                self.assertNotIn("_run", text)

    def test_usage_help_defines_path_and_top_n_levels(self):
        usage = self.help_text("data", "usage")
        self.assertIn("relative to your own Lustre root", usage)
        self.assertIn("top N folders at each level", usage)
        self.assertIn("not live", usage)
        self.assertIn("recursive usage", usage)

    def test_execution_conflict_names_flags_and_alternative(self):
        output = io.StringIO()
        with contextlib.redirect_stderr(output), self.assertRaises(SystemExit) as raised:
            cli.parser().parse_args(["data", "copy", "src", "dst", "--local", "--prefect"])
        self.assertEqual(raised.exception.code, 2)
        message = output.getvalue()
        self.assertIn("unsupported option combination", message)
        self.assertIn("--local", message)
        self.assertIn("--prefect", message)
        self.assertIn("choose one execution mode", message)


if __name__ == "__main__":
    unittest.main()
