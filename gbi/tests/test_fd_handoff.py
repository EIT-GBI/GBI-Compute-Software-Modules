"""Bounded real descriptor handoff regression; no source pathname reopening."""

from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

from gbi_data.transfer import copy_stream


class DescriptorHandoff(unittest.TestCase):
    def test_real_pinned_descriptor_direct_and_engine(self):
        rclone = shutil.which("rclone")
        self.assertIsNotNone(rclone, "this regression requires the pinned rclone on PATH")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            expected = b"descriptor-handoff-fixture\n" * 128
            failures = []
            observations = []

            def attempt(index):
                source, destination = root / f"source-{index}", root / f"destination-{index}"
                source.write_bytes(expected)
                source_fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW)
                before = os.fstat(source_fd)
                detail = {"mode": "direct" if index < 24 else "engine", "source_fd": source_fd,
                          "parent_inheritable": os.get_inheritable(source_fd), "size": before.st_size}
                if index % 2:
                    source.unlink()  # The descriptor must work without reopening its pathname.
                try:
                    if index < 24:
                        command = [rclone, "cat", "/dev/fd", "--files-from-raw", "-", "--no-traverse",
                                   "--copy-links", "--config", "/dev/null", "--retries", "1",
                                   "--low-level-retries", "1", "--multi-thread-streams", "0"]
                        if index < 12:
                            probe = ("import json,os,sys; fd=int(sys.argv[1]); info=os.fstat(fd); "
                                     "print(json.dumps({'fd':fd,'inheritable':os.get_inheritable(fd),"
                                     "'dev':info.st_dev,'inode':info.st_ino,'size':info.st_size}),file=sys.stderr,flush=True); "
                                     "os.execv(sys.argv[2],sys.argv[2:])")
                            command = [sys.executable, "-B", "-c", probe, str(source_fd), *command]
                        child = subprocess.run(
                            command,
                            input=f"{source_fd}\n".encode(), capture_output=True, timeout=5,
                            pass_fds=(source_fd,),
                            env={key: value for key, value in os.environ.items() if not key.startswith("RCLONE_")})
                        detail.update(exit=child.returncode, bytes=len(child.stdout), stderr=child.stderr.decode(errors="replace"))
                        if index < 12:
                            inherited = json.loads(detail["stderr"].splitlines()[0])
                            detail["child_identity_matches"] = (inherited["dev"], inherited["inode"], inherited["size"]) == (
                                before.st_dev, before.st_ino, before.st_size)
                            detail["child_inheritable"] = inherited["inheritable"]
                            if not detail["child_identity_matches"] or not inherited["inheritable"]:
                                return {**detail, "error": "descriptor inheritance differs"}
                        if child.returncode or child.stdout != expected:
                            return detail
                    else:
                        with (root / f"lock-{index}").open("w") as lock:
                            fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                            digest, size = copy_stream(source, destination, fd, rclone, lock,
                                                       root / f"progress-{index}.json", "fss", {}, None,
                                                       len(expected), source_fd)
                        if size != len(expected) or digest != hashlib.sha256(expected).hexdigest():
                            return {**detail, "error": "incorrect bytes or digest"}
                    after = os.fstat(source_fd)
                    if (before.st_dev, before.st_ino, before.st_size) != (after.st_dev, after.st_ino, after.st_size):
                        return {**detail, "error": "parent source identity changed"}
                except Exception as error:
                    return {**detail, "error": str(error)}
                finally:
                    os.close(source_fd)
                    observations.append(detail)
                return None

            with ThreadPoolExecutor(max_workers=4) as pool:
                for outcome in pool.map(attempt, range(48)):
                    if outcome is not None:
                        failures.append(outcome)
            self.assertEqual(failures, [], json.dumps(failures, sort_keys=True))
            print(json.dumps({"attempts": len(observations), "direct": 24, "engine": 24,
                              "source_descriptors": sorted({item["source_fd"] for item in observations}),
                              "parent_inheritable": sorted({item["parent_inheritable"] for item in observations}),
                              "probed_children": sum("child_identity_matches" in item for item in observations),
                              "all_child_identities_match": all(item.get("child_identity_matches", True) for item in observations),
                              "unlinked_source_paths": 24, "failures": len(failures)}, sort_keys=True))


if __name__ == "__main__":
    unittest.main()
