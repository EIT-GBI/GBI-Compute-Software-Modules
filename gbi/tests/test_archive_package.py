from importlib import metadata, util
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
import zipfile


class ArchivePackageTest(unittest.TestCase):
    def test_wheel_uses_source_and_round_trips(self):
        if util.find_spec("pip") is None:
            self.skipTest("wheel integration needs pip and setuptools>=61")
        try:
            if int(metadata.version("setuptools").split(".")[0]) < 61:
                self.skipTest("wheel integration needs setuptools>=61")
        except metadata.PackageNotFoundError:
            self.skipTest("wheel integration needs setuptools>=61")
        with tempfile.TemporaryDirectory() as directory:
            tmp_path = Path(directory).resolve()
            project = tmp_path / "project"
            wheelhouse, install = tmp_path / "wheelhouse", tmp_path / "installed"
            project.mkdir()
            shutil.copy2(Path(__file__).parents[1] / "pyproject.toml", project)
            shutil.copytree(Path(__file__).parents[1] / "src/lib/gbi_data", project / "src/lib/gbi_data")
            wheelhouse.mkdir()
            try:
                subprocess.run(
                    [sys.executable, "-m", "pip", "wheel", ".", "--no-index", "--no-deps",
                     "--no-build-isolation", "-w", str(wheelhouse)],
                    cwd=project, check=True, capture_output=True, text=True,
                )
            except subprocess.CalledProcessError as error:
                if "BackendUnavailable" in error.stderr or "No module named 'setuptools'" in error.stderr:
                    self.skipTest("local Python has no usable offline wheel build backend")
                raise
            wheels = tuple(wheelhouse.glob("*.whl"))
            self.assertEqual(len(wheels), 1)
            with zipfile.ZipFile(wheels[0]) as wheel:
                for name in ("archives.py", "selection.py"):
                    self.assertEqual(wheel.read("gbi_data/" + name),
                                     (project / "src/lib/gbi_data" / name).read_bytes())
            subprocess.run(
                [sys.executable, "-m", "pip", "install", "--no-index", "--no-deps",
                 "--target", str(install), str(wheels[0])],
                check=True, capture_output=True, text=True,
            )
            source = tmp_path / "source"
            source.mkdir()
            (source / "keep.txt").write_text("keep\n")
            (source / "skip.txt").write_text("skip\n")
            script = r'''
from pathlib import Path
from importlib.metadata import version
import sys
from gbi_data import archives, __version__
assert version("gbi-archive-codec") == __version__
source, archive, restored = map(Path, sys.argv[1:4])
compression = sys.argv[4] or None
with archive.open("wb") as output:
    manifest = archives.pack(source, output, compression=compression,
                             includes=("*.txt",), exclusions=("skip.txt",))
archives.verify_archive(archive, manifest)
archives.restore(archive, restored)
assert (restored / "keep.txt").read_text() == "keep\n"
assert not (restored / "skip.txt").exists()
'''
            for suffix, compression in (("tar", ""), ("tar.gz", "gzip")):
                result = subprocess.run(
                    [sys.executable, "-c", script, str(source), str(tmp_path / f"archive.{suffix}"),
                     str(tmp_path / f"restored-{suffix}"), compression],
                    env={"PATH": "", "PYTHONPATH": str(install)},
                    capture_output=True, text=True,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
