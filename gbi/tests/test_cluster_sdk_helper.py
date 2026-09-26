"""SDK smoke helper argument and forwarding checks without cluster operations."""

import contextlib
import io
import os
from pathlib import Path
import shutil
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import cluster_sdk_test as helper


class ClusterSDKHelper(unittest.TestCase):
    def test_default_and_explicit_prefect_job_sizes(self):
        with patch.object(helper, "main") as ordinary, patch.object(helper, "prefect_main") as prefect:
            helper.cli([])
            ordinary.assert_called_once_with()
            prefect.assert_not_called()
            for size in (None, "small", "large"):
                helper.cli(["--prefect", *(["--job-size", size] if size else [])])
                prefect.assert_called_with(job_size=size)

    def test_invalid_arguments_fail_before_any_fixture_or_transfer(self):
        for arguments in (["--job-size", "small"], ["--job-size", "large"],
                          ["--prefect", "--job-size", "medium"]):
            with self.subTest(arguments=arguments), patch.object(helper, "main") as ordinary, \
                    patch.object(helper, "prefect_main") as prefect, contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as error:
                    helper.cli(arguments)
                self.assertEqual(error.exception.code, 2)
                ordinary.assert_not_called()
                prefect.assert_not_called()

    def test_job_size_is_forwarded_to_restore_only(self):
        for size in (None, "small", "large"):
            with self.subTest(size=size), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                roots = {kind: root / kind for kind in ("lustre", "alluxio")}
                for path in roots.values():
                    path.mkdir()

                def move(source, target, **options):
                    target.mkdir()
                    for name in ("model.pt", "model.pt.ready.json"):
                        shutil.move(source / name, target / name)

                def restore(source, target, **options):
                    for path in source.iterdir():
                        shutil.copy2(path, target / path.name)

                with patch.dict(os.environ, {"SLURM_JOB_ID": "123", "GBI_DATA_SITE_CONF": "unused"}), \
                        patch.object(helper, "Site", return_value=SimpleNamespace(roots=roots)), \
                        patch.object(helper.data, "move", side_effect=move) as archive, \
                        patch.object(helper.data, "copy", side_effect=restore) as copy, \
                        contextlib.redirect_stdout(io.StringIO()):
                    helper.prefect_main(job_size=size)
                source, target = archive.call_args.args
                archive.assert_called_once_with(source, target, include=["*.pt", "*.pt.*"], prefect=True)
                copy.assert_called_once_with(target, source, include=["*.pt", "*.pt.*"],
                                             prefect=True, job_size=size)


if __name__ == "__main__":
    unittest.main()
