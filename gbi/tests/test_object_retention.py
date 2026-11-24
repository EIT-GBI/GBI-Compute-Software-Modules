"""Object Storage source removal follows the move table, inside one mount."""

import contextlib
import io
import json
import os
from pathlib import Path
import pwd
import tempfile
import unittest
from unittest.mock import patch

from gbi_data import cli, formats, prefect
from gbi_data.storage import Site
from gbi_data.transfer import transfer, validate_source_retention


class ObjectRetention(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name).resolve()
        user      = pwd.getpwuid(os.getuid()).pw_name
        for kind in ("lustre", "fss", "bucket"):
            (self.base / kind / user).mkdir(parents = True)
        config = self.base / "site.conf"
        config.write_text(
            "".join(
                f"{kind}_root = {self.base / kind}\n"
                for kind in ("lustre", "fss", "bucket")
            )
        )
        self.site   = Site(config)
        self.source = self.site.roots["alluxio"] / "source"
        self.source.write_bytes(b"durable original")
        self.target = self.site.roots["fss"] / "restore"

    def test_copy_deletion_flag_rejected_before_route_or_format_inspection(self):
        for flags in (
            [], ["--pack", "tar"], ["--pack", "gzip"], ["--pack-small"],
            ["--chunk-size", "4KiB"],
        ):
            options = cli.parser().parse_args(
                [
                    "data", "copy", "missing", "missing-target",
                    "--delete-source", *flags,
                ]
            )
            with (
                self.subTest(flags = flags),
                patch.object(
                    self.site, "classify",
                    side_effect = AssertionError("route inspected"),
                ),
            ):
                for planner in (cli.plan, prefect.prepare):
                    with self.assertRaisesRegex(
                        ValueError, "applies only to move"
                    ):
                        planner(options, self.site)

    def test_object_source_moves_ask_and_keep_without_a_terminal(self):
        options = cli.parser().parse_args(
            ["data", "move", str(self.source), str(self.target)]
        )
        output = io.StringIO()
        with (
            patch("gbi_data.moves.terminal", return_value = False),
            contextlib.redirect_stdout(output),
        ):
            self.assertFalse(cli.plan(options, self.site)["delete"])
        self.assertIn("keeping the Object Storage source", output.getvalue())
        for answer, delete in (("y", True), ("N", False)):
            with (
                self.subTest(answer = answer),
                patch("gbi_data.moves.terminal", return_value = True),
                patch("builtins.input", return_value = answer),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(cli.plan(options, self.site)["delete"], delete)
        options.delete_source = True
        with patch("builtins.input") as ask:
            self.assertTrue(cli.plan(options, self.site)["delete"])
        ask.assert_not_called()
        between = cli.parser().parse_args(
            [
                "data", "move", str(self.source),
                str(self.site.roots["alluxio"] / "moved"),
            ]
        )
        with patch("builtins.input") as ask:
            self.assertTrue(cli.plan(between, self.site)["delete"])
        ask.assert_not_called()

    def test_retired_delete_source_field_fails_before_state_or_destination(
                self,
            ):
        options = cli.parser().parse_args(
            ["data", "copy", str(self.source), str(self.target)]
        )
        specification = cli.plan(options, self.site)
        run_dir = self.base / "saved-request"
        run_dir.mkdir()
        request = run_dir / "request.json"
        home    = self.base / "scratch-state"
        request.write_text(json.dumps({**specification, "delete_source": True}))
        before = request.read_bytes()
        with self.assertRaisesRegex(ValueError, "retired delete_source"):
            cli.run(run_dir, self.site, home)
        self.assertEqual(request.read_bytes(), before)
        self.assertEqual(self.source.read_bytes(), b"durable original")
        self.assertFalse(home.exists())
        self.assertFalse(self.target.exists())

    def test_retired_worker_field_fails_before_io(self):
        for action in (
            None, "pack", "chunk", "restore_archive", "restore_chunks",
        ):
            task = {
                "source_kind": "alluxio", "delete": True,
                "delete_source": True,
                "source": str(self.source), "target": str(self.target),
            }
            if action:
                task["format_action"] = action
            for empty in (False, True):
                task["empty"] = empty
                workers = (
                    (transfer, formats.transfer) if action else (transfer,)
                )
                for worker in workers:
                    with (
                        self.subTest(
                            action = action, empty = empty, worker = worker.__module__
                        ),
                        self.assertRaisesRegex(ValueError, "retired"),
                    ):
                        worker(task)
        self.assertEqual(self.source.read_bytes(), b"durable original")
        self.assertFalse(self.target.exists())

    def test_object_source_must_lie_inside_one_object_mount(self):
        root = self.site.roots["alluxio"]
        for source_kind, source, allowed in (
            ("alluxio", root / "source", True),
            ("alluxio", root, False),
            ("alluxio", root.parent, False),
        ):
            task = {
                "source_kind": source_kind, "delete": True,
                "source": str(source),
            }
            with self.subTest(source = source):
                if allowed:
                    validate_source_retention(task, self.site.storage_roots)
                else:
                    with self.assertRaisesRegex(
                        ValueError, "overlaps an Object Storage root"
                    ):
                        validate_source_retention(task, self.site.storage_roots)

    def test_filesystem_parent_of_object_mount_fails_closed_including_saved_pack(
                self,
            ):
        source = self.site.roots["lustre"] / "mixed"
        source.mkdir()
        nested = source / "object-mount"
        nested.mkdir()
        (nested / "durable").write_bytes(b"retained")
        native = source / "native"
        native.mkdir()
        with patch(
            "gbi_data.storage.mounted_roots", return_value = [("alluxio", nested)]
        ):
            site = Site(self.site.path)
        for flags in (
            [], ["--pack", "tar"], ["--pack-small"], ["--chunk-size", "4KiB"],
        ):
            options = cli.parser().parse_args(
                ["data", "move", str(source), str(self.target), *flags]
            )
            with (
                self.subTest(flags = flags),
                self.assertRaisesRegex(
                    ValueError, "overlaps an Object Storage root"
                ),
            ):
                cli.plan(options, site)
        options = cli.parser().parse_args(
            ["data", "move", str(source), str(self.target)]
        )
        old = cli.plan(
            options, self.site
        )  # Saved before the nested mount was visible.
        old.update(pack = "tar", format_units = True)
        run_dir = self.base / "saved-mixed"
        run_dir.mkdir()
        (run_dir / "request.json").write_text(json.dumps(old))
        with self.assertRaisesRegex(
            ValueError, "overlaps an Object Storage root"
        ):
            cli.run(run_dir, site, self.base / "state")
        self.assertFalse(self.target.exists())
        self.assertFalse((self.base / "state").exists())
        self.assertEqual((nested / "durable").read_bytes(), b"retained")
        options.verb = "copy"
        self.assertFalse(cli.plan(options, site)["delete"])
        options.verb   = "move"
        options.source = str(native)
        self.assertTrue(cli.plan(options, site)["delete"])

    def test_configured_filesystem_root_cannot_shadow_known_object_mount(self):
        root = self.site.roots["lustre"]
        for object_root in (root, root.parent):
            with patch(
                "gbi_data.storage.mounted_roots",
                return_value = [("alluxio", object_root)],
            ):
                site = Site(self.site.path)
            self.assertEqual(site.classify(root)[1], "lustre")
            for source in (root, root / "file"):
                if source != root:
                    source.write_bytes(b"retained")
                options = cli.parser().parse_args(
                    ["data", "move", str(source), str(self.target)]
                )
                with (
                    self.subTest(object_root = object_root, source = source),
                    self.assertRaisesRegex(
                        ValueError, "overlaps an Object Storage root"
                    ),
                ):
                    cli.plan(options, site)

    def test_plain_object_move_preserves_bytes_and_records_no_deletion(self):
        state = self.base / "state"
        for name in ("locks", "pending"):
            (state / name).mkdir(parents = True)
        receipt = self.base / "receipts.jsonl"
        task = {
            "source_kind": "alluxio", "target_kind": "fss", "delete": False,
            "source": str(self.source), "target": str(self.target),
            "source_root": str(self.source.parent),
            "target_root": str(self.target.parent),
            "selection_root": str(self.source.parent), "state": str(state),
            "receipt": str(receipt),
            "progress": str(self.base / "progress.json"),
            "rclone": None, "native_bytes": 1024, "settle_seconds": 0,
        }
        before = self.source.stat()
        result = transfer(task)
        self.assertFalse(result["deleted"])
        self.assertEqual(self.source.read_bytes(), b"durable original")
        self.assertEqual(self.target.read_bytes(), b"durable original")
        self.assertEqual(
            (self.source.stat().st_ino, self.source.stat().st_mtime_ns),
            (before.st_ino, before.st_mtime_ns),
        )
        self.assertEqual(
            [
                json.loads(line)["event"]
                for line in receipt.read_text().splitlines()
            ], ["verified"],
        )
