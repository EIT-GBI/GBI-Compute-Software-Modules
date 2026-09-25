# GBI data transfers

`gbi data` moves and verifies files between the HPC filesystems available to
your account. It runs as your user, keeps a receipt for every verified file,
and removes a source only after verification and a final source-identity check.
It does not grant permissions or require a Prefect UI session.

Read the [full user guide](docs/user-guide.md) or
[download the PDF](docs/user-guide.pdf). It covers the
command modes, Python SDK, archive and chunk formats, filters, status and
troubleshooting. Replace `/your/lustre` and `/your/object` in examples with
paths printed by `gbi data roots`. Start here for the shortest working path.

## Quick start

```bash
module load gbi
gbi --version
gbi data roots
gbi data copy SOURCE DESTINATION
```

Use the paths printed by `gbi data roots` in place of the placeholder paths
below. A directory's contents go into the destination directory you name.
A single file goes to the destination file, or into an existing destination
directory under its original name.

```bash
gbi data move /your/lustre/run /your/object/run
gbi data status TRANSFER_ID --watch
gbi data usage --depth 1 --limit 20
```

`copy` keeps sources. `move` verifies each selected source before removing it.
For an Object Storage/Alluxio source, `move` keeps the original unless you
explicitly add `--delete-source`. Existing different destination files are
kept and reported; GBI does not silently overwrite them.

## Choose a route

Without a route flag, GBI chooses a bounded foreground run for small work and
uses Slurm for larger or slow-to-discover trees. Inside an existing Slurm
allocation it reuses that allocation. `--detach` submits a separate Slurm job;
`--wait` waits for it even in a script. `--local` requires an existing
allocation and is normally unnecessary because allocation reuse is automatic.

Use `--prefect` for a managed personal Object Storage migration. It writes
individual objects directly and does not create tar archives. The Prefect route
supports repeatable include and exclude filters (exclusions require the updated
site broker and flows). See the route matrix in
[the user guide](docs/user-guide.md).
Ordinary routing can use any mounted path your Unix account can access; the
configured roots are path and storage-type hints. Prefect applies stricter
personal-root rules.

## Select and package

Patterns match file names recursively, are case-sensitive, and use shell glob
syntax. Quote them so the shell does not expand them first. Exclusions win over
includes:

```bash
gbi data move /your/lustre/run /your/object/run.gbi.tar \
  --pack tar --include '*.pt' --include '*.pt.*' --exclude '*.tmp'
```

Use `--pack tar` or `--pack gzip` with an exact `.gbi.tar` or `.gbi.tar.gz`
destination. GBI archives restore to Lustre or FSS, including empty directories
and safe symlinks. Use `--pack-small --dry-run` to inspect subdirectories that
fit the archive metadata budget. Use `--chunk-size 64MiB` for a large regular
file that should resume from verified chunks.

## Python

The SDK is a thin, blocking wrapper around the installed CLI. Use absolute
paths from `gbi data roots` in a training script:

```python
from gbi import data

source = "/your/lustre/finished-run"
archive = "/your/object/run.gbi.tar"
data.move(source, archive, pack="tar", include=["*.pt", "*.pt.*"])
data.copy(archive, "/your/lustre/restored-run")
```

Load the module before starting Python. The SDK accepts paths as strings or
`pathlib.Path`, returns `subprocess.CompletedProcess`, and raises
`subprocess.CalledProcessError` for a failed transfer. It never retries a
failed command automatically.

## Maintainer reference

This repository is an Lmod module farm. The maintained installation, site
settings and validation details are in [docs/maintainer.md](docs/maintainer.md).
The short installation form is:

```bash
GBI_SITE_LUSTRE_ROOT=/site/scratch/users \
GBI_SITE_FSS_ROOT=/site/home/users \
GBI_SITE_BUCKET_ROOT=/site/personal/users \
GBI_SITE_PARTITION=site-partition \
GBI_MODULE_PATH=/site/shared/software make gbi
```

Use `make help` and the repository [AGENTS.md](../AGENTS.md) for harness rules.
Run the public tests with:

```bash
PYTHONPATH=gbi/src/lib python3 -B -m unittest discover -s gbi/tests -v
```
