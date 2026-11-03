"""The ``gbi data roots`` summary: where each kind of storage is and what it is for.

Everything printed is derived on the host: the three personal roots from the
site configuration, team folders from the directories the caller can open
under ``team_root``, and the read-only collections from the paths that exist.
Operational notes that change on a schedule (expiry rules, quotas) come from
the site configuration so they can be edited without a release.
"""

import os
from pathlib import Path

INDENT = " " * 9

LUSTRE_NOTES = (
    "scratch for running jobs: inputs, intermediate files, checkpoints, logs,"
    " compiling software",
    "best for small files and in-place writes · not backed up",
)
FSS_NOTES = (
    "home: code, scripts, notebooks, conda and venv environments;"
    " no datasets or results",
    "most expensive per TB · not backed up",
)
ALLUXIO_NOTES = (
    "keep finished runs, results and reference data; Object Storage underneath,"
    " jobs read it directly",
    "write each file once and whole; not for git, appends or many small files",
    "warm before a job: gbi data cache PATH --include '*.fastq.gz'",
)
READ_NOTE = (
    "readable by every HPC user; a shared collection is written by the team"
    " that owns it"
)
FOOTER = "bulk moves between roots: gbi data move SRC DEST --prefect"


def _accessible_directory(path):
    try:
        return path.is_dir()
    except OSError:
        return False


def teams(site):
    """Team folders the caller belongs to: (team, members path, shared path)."""
    team_root = Path(site.values["team_root"]).expanduser().absolute()
    found = {}
    alluxio = site.roots.get("alluxio")
    if alluxio is not None and team_root in alluxio.parents:
        relative = alluxio.relative_to(team_root).parts
        if len(relative) == 3 and relative[1] == "members":
            found[relative[0]] = team_root / relative[0]
    try:
        children = sorted(child for child in team_root.iterdir())
    except OSError:
        children = []
    for child in children:
        if child.name not in found and _accessible_directory(
            child / "members" / site.user
        ):
            found[child.name] = child
    result = []
    for name in sorted(found):
        team = found[name]
        members = team / "members" / site.user
        shared = team / "shared"
        result.append((
            name,
            members if _accessible_directory(members) else None,
            shared if _accessible_directory(shared) else None,
        ))
    return result


def readable_roots(site):
    """Configured read-only collection roots that exist, with ``/*`` for families."""
    result = []
    for value in site.values["readonly_roots"].split():
        path = Path(value).expanduser().absolute()
        try:
            if not path.exists():
                continue
            family = not path.is_symlink() and any(
                child.is_dir() for child in path.iterdir()
            )
        except OSError:
            family = False
        result.append(f"{path}/*" if family else str(path))
    return result


def _block(name, path, notes, site_note):
    lines = [f"{name:8} {path}"]
    lines.extend(f"{INDENT}{note}" for note in notes)
    if site_note:
        lines.append(f"{INDENT}{site_note}")
    return lines


def lines(site):
    """The report, one entry per printed line."""
    report = ["Your storage roots", ""]
    aliases = site.root_aliases
    if "lustre" in aliases:
        report.extend(_block(
            "lustre", aliases["lustre"], LUSTRE_NOTES, site.values["lustre_note"]
        ))
        report.append("")
    if "fss" in aliases:
        report.extend(_block(
            "fss", aliases["fss"], FSS_NOTES, site.values["fss_note"]
        ))
        report.append("")
    members = [team for team in teams(site) if team[1] is not None]
    if "alluxio" in aliases:
        alias, resolved = aliases["alluxio"], site.roots["alluxio"]
        if not os.path.lexists(alias) and members:
            # No personal path: the member folder is the user's archive root.
            resolved, alias = members[0][1], None
        notes = list(ALLUXIO_NOTES)
        if alias is not None and resolved != alias:
            notes.insert(0, f"also reachable as {alias}")
        report.extend(_block(
            "alluxio", resolved, notes, site.values["alluxio_note"]
        ))
        report.append("")
    shared = [(name, path) for name, _, path in teams(site) if path is not None]
    if shared:
        for index, (name, path) in enumerate(shared):
            report.append(f"{'team' if index == 0 else '':8} {path}")
            report.append(f"{INDENT}read-write for everyone in {name}")
    else:
        report.append(f"{'team':8} none: you are not in a team folder")
    readable = readable_roots(site)
    if readable:
        report.append(f"{'read':8} {'  '.join(readable)}")
        report.append(f"{INDENT}{READ_NOTE}")
    report.extend(["", FOOTER])
    return report


def print_report(site):
    print("\n".join(lines(site)))
    return 0
