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
gbi --help
gbi data roots
gbi data copy SOURCE DESTINATION
```

`gbi --help` and `gbi data --help` explain every public flag, grouped by
command, including defaults, examples and execution-mode restrictions. Use
`gbi data copy --help` (or another command's `--help`) for its focused usage.

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
individual objects by default; archive `--pack tar/gzip`, `--pack-small` and `--chunk-size`
require matching updated broker and flow deployments. The Prefect route
supports repeatable include and exclude filters (exclusions require the updated
site broker and flows). See the route matrix in
[the user guide](docs/user-guide.md).
Ordinary routing can use any mounted path your Unix account can access; the
configured roots are path and storage-type hints. Prefect applies stricter
personal-root rules.

**GBI 0.4.9 was installed and passed normal-user CLI/SDK checks on 2026-09-26.**
This confirms module installation, not acceptance of every Prefect feature.
Check `gbi --version`; new Prefect options require the matching broker and
affected flow, and unsupported requested options fail closed. Restoring encoded
archives or chunks also requires a compatible restore flow.

You may choose a different destination within your own destination root. FSS
archives and restores require the matching broker and flow update for this;
older deployments reject the renamed request before submission. For example:

```bash
gbi data copy /your/fss/experiment /your/object/saved-experiment --prefect --wait
gbi data copy /your/object/saved-experiment /your/lustre/restored-experiment --prefect --wait
```

These copy commands retain their sources. Renaming does not change include/
exclude selection, source-deletion rules or destination collision checks.

For a Prefect restore to Lustre or FSS, `--job-size small` requests 2 CPUs and
16 GiB instead of the existing `large` reservation (8 CPUs and 48 GiB):

```bash
gbi data copy /your/object/run /your/lustre/run --prefect --job-size small --wait
```

The Python equivalent is `data.copy(source, destination, prefect=True,
job_size="small")`. This option requires the matching site broker and restore
flow update; unsupported deployments reject it before submission. Omit it to
preserve the deployment's existing setting. The option applies only to restores,
does not change the time limit, and does not guarantee immediate scheduling.

With the portable archive update, `--prefect --pack gzip` uses the exact
`.gbi.tar.gz` destination you name; `--pack-small` uses the ordinary site's
small-file packing policy. They cannot be combined, and apply to archives from
Lustre/FSS, not restores. For example:

```bash
gbi data copy /your/fss/run /your/object/run.gbi.tar.gz --prefect --pack gzip --wait
gbi data copy /your/lustre/run /your/object/run --prefect --pack-small --dry-run --wait
```

A **Prefect packing/chunking dry run** submits a metadata-only planning run and creates
Prefect/local tracking records, but no data, receipts, destination writes or
source deletions. Without packing or chunking, `--prefect --dry-run` remains a local route
preview with no submission. Use the printed run ID to inspect a non-waiting
plan. Python `data.copy(..., prefect=True, pack_small=True, dry_run=True)` waits
for the managed plan. Older deployments fail closed; these source changes do
not establish that your site has installed the update. Object Storage source
deletion remains unavailable with Prefect; no delete authority is granted.

Candidate chunk support uses the same portable store for raw files and packed
archives. A direct file must be regular and have no symlink path components;
the CLI checks its type so the backend can grant only its exact store prefix.
Leave `.gbi-chunks` off a direct file target: it is added automatically, as it
is to a whole-pack target. Directory members are encoded independently.
Raw-file chunks preserve content and the stored filename, not POSIX modes or
modification times. Use a packed archive when those metadata need preserving;
chunking that archive retains its archive metadata.

```bash
gbi data copy /your/lustre/checkpoint.bin /your/object/saved.bin --prefect --chunk-size 64MiB --wait
gbi data copy /your/fss/run /your/object/run.gbi.tar.gz --prefect --pack gzip --chunk-size 64MiB --wait
gbi data copy /your/object/run.gbi.tar.gz.gbi-chunks /your/lustre/restored --prefect --job-size small --wait
```

Restore detection is automatic; omit packing and chunk-size flags on restores.
Existing different data is never overwritten. After an interrupted move,
inspect retained sources and receipts before starting another run; a changed
packing layout fails closed rather than creating conflicting logical outputs.

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
