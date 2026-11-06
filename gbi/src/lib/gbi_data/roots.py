"""The ``gbi data roots`` summary: where each kind of storage is and what it is for.

Everything printed is derived on the host: the three personal roots from the
site configuration, team folders from the directories the caller can open
under ``team_root``, and the read-only collections from the ones the caller
can open. The printed Object Storage root is always a public path: the member
folder, or the ``/mnt/user-data`` path of an account that still has its own
backend, never the mount a link resolves to.
Operational notes that change on a schedule (expiry rules, quotas) come from
the site configuration so they can be edited without a release.
"""

import os
from pathlib import Path

INDENT = " " * 9

LUSTRE_NOTES = (
    "Scratch for running jobs, e.g. inputs, intermediate files, checkpoints, logs,"
    " compiling software",
    "Best for small files and in-place writes",
    "Not backed up",
)
FSS_NOTES = (
    "home, e.g. code, conda and venv environments; no datasets or results",
    "Not backed up",
)
ALLUXIO_NOTES = (
    "Keep finished runs, results, and data; object storage",
    "Write a file once; no appending. Not suitable for git or many small files",
    "Prepare data before a job using: gbi data cache PATH --include '*.fastq.gz'",
)
READ_NOTE = (
    "what you can read; shared collections are written by the team that owns"
    " them, archives are read-only"
)
FOOTER = "bulk moves between roots: gbi data move SRC DEST --prefect"


def _accessible_directory(path):
    try:
        return path.is_dir()
    except OSError:
        return False


def _openable(path):
    """Whether the caller can list ``path``: the test a job's read would face."""
    try:
        os.scandir(path).close()
    except OSError:
        return False
    return True


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
    """Configured read-only collection roots the caller can open.

    A family root (a directory of collections) prints as ``ROOT/*`` when every
    collection in it opens for the caller and otherwise as the collections
    that do; a collection the caller cannot open is left out, as is a path
    that does not exist.
    """
    result = []
    for value in site.values["readonly_roots"].split():
        path = Path(value).expanduser().absolute()
        try:
            if not path.exists():
                continue
            collections = [] if path.is_symlink() else sorted(
                child for child in path.iterdir() if child.is_dir()
            )
        except OSError:
            collections = []
        if not collections:
            if _openable(path):
                result.append(str(path))
            continue
        openable = [child for child in collections if _openable(child)]
        if len(openable) == len(collections):
            result.append(f"{path}/*")
        else:
            result.extend(str(child) for child in openable)
    return result


def archive_root(site):
    """The caller's Object Storage root and the older path that also reaches it.

    Returns ``(root, alias)``: the member folder and the ``/mnt/user-data``
    entry when that entry links into the team folder; the member folder and
    ``None`` when there is no entry; the entry itself and ``None`` when it
    still points at its own backend or when the caller is in no team. The
    same choice drives ``gbi data roots`` and the ``--prefect`` path check.
    """
    alias = site.root_aliases.get("alluxio")
    if alias is None:
        return None, None
    resolved = site.roots["alluxio"]
    team_root = Path(site.values["team_root"]).expanduser().absolute()
    members = [team for team in teams(site) if team[1] is not None]
    if not os.path.lexists(alias):
        # No personal path: the member folder is the user's archive root.
        return (members[0][1] if members else alias), None
    if team_root in resolved.parents:
        # The personal path is a link into the team folder: print the
        # folder and name the older path that still reaches it.
        return resolved, (alias if resolved != alias else None)
    # A personal path still on its own backend: print the path the user
    # knows, never the mount it resolves to.
    return alias, None


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
    if "alluxio" in aliases:
        root, alias = archive_root(site)
        notes = list(ALLUXIO_NOTES)
        if alias is not None:
            notes.insert(0, f"also reachable as {alias}")
        report.extend(_block(
            "alluxio", root, notes, site.values["alluxio_note"]
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
