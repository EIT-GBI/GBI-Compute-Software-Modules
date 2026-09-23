#!/usr/bin/env python3
"""Check tracked CLI/module files for recognisable private site material."""

import ipaddress
import os
from pathlib import Path
import re
import subprocess
import sys


SCOPE = ("gbi", "rclone", ".github", "README.md", "AGENTS.md")
PATTERNS = {
    "cloud resource identifier": re.compile(r"\bocid1\.[a-z0-9]+\.oc[0-9]+\.[A-Za-z0-9.]+"),
    "private key": re.compile(r"-----BEGIN (?:[A-Z0-9]+ )?PRIVATE KEY-----"),
    "literal Object Storage namespace/bucket": re.compile(r"/n/[A-Za-z0-9_-]+/b/[A-Za-z0-9_.-]+"),
    "literal personal path": re.compile(
        r"/(?:mnt/lustre/users|mnt/user-data|mnt/gbi-shared/home|Users)/[A-Za-z0-9][A-Za-z0-9_.-]*"),
}
IPV4 = re.compile(r"(?<![\w.])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?![\w.])")
# RFC 1918 network integers keep the detector's own source free of site-like
# address literals without exempting this file from the scan.
PRIVATE_NETWORKS = tuple(ipaddress.ip_network(value) for value in
                         ((10 << 24, 8), ((172 << 24) + (16 << 16), 12),
                          ((192 << 24) + (168 << 16), 16)))


def scan_file(path):
    """Return (line, rule), never the potentially sensitive matching text."""
    if path.is_symlink():
        return [(0, "symlink in public scan scope; review its target explicitly")]
    if path.name == "site.conf":
        return [(0, "generated site configuration must not be committed")]
    findings = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        for label, pattern in PATTERNS.items():
            if pattern.search(line):
                findings.append((number, label))
        for match in IPV4.finditer(line):
            try:
                address = ipaddress.ip_address(match.group())
            except ValueError:
                continue
            if any(address in network for network in PRIVATE_NETWORKS):
                findings.append((number, "private IPv4 address"))
                break
    return findings


def scan_repository(root):
    names = subprocess.check_output(["git", "ls-files", "-z", "--", *SCOPE], cwd=root).split(b"\0")
    paths = [os.fsdecode(name) for name in names if name]
    if not any(name.startswith("gbi/src/") for name in paths):
        raise ValueError("no tracked gbi/src files; refusing an empty hygiene check")
    findings = []
    for name in paths:
        try:
            findings.extend((name, number, rule) for number, rule in scan_file(root / name))
        except (OSError, UnicodeError):
            findings.append((name, 0, "file could not be scanned as UTF-8 text"))
    return len(paths), findings


def main():
    root = Path(__file__).resolve().parents[2]
    try:
        count, findings = scan_repository(root)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"Public hygiene scan could not run: {error}", file=sys.stderr)
        return 1
    for name, number, rule in findings:
        print(f"{name}:{number}: {rule}")
    print(f"Public hygiene: {count} tracked files scanned, {len(findings)} findings.")
    return int(bool(findings))


if __name__ == "__main__":
    sys.exit(main())
