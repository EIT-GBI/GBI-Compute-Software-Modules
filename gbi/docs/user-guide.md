# GBI data user guide

<img src="assets/gbi-cli-logo.png" alt="GBI CLI logo" width="160">

GBI copies, moves, archives and restores your research data between Lustre,
FSS and Object Storage. It runs with your Unix permissions and verifies the
destination before removing an eligible filesystem source. Object Storage is
the durable store: normal moves and restores always retain its originals.

This guide describes the installed **0.4.14 release** (0.4.11 plus the archive-move cleanup and lock-stripe fixes), accepted for ordinary
users on 28 September 2026. Check `gbi --version` and the
[README](../README.md) before using new options. Its matching typed broker and
production FSS/Lustre archive and restore flows are deployed and accepted,
including automatic `--archive --prefect`.

## Start here

Load the module and find your storage paths:

```bash
module load gbi
gbi --version
gbi data roots
```

Replace the placeholder paths below with paths printed by `roots`.

```bash
# Copy files and keep the originals.
gbi data copy /your/lustre/results /your/object/results

# Move finished files, removing verified Lustre sources.
gbi data move /your/lustre/results /your/object/results

# Archive a finished directory; GBI chooses packaging and part sizes.
gbi data move /your/lustre/experiment /your/object/experiment --archive

# Restore the complete archive directory to ordinary files.
gbi data copy /your/object/experiment /your/lustre/restored-experiment
```

For a directory, its contents go inside the destination directory you name.
For one file, the destination is its new filename, unless that destination
already is a directory; then GBI keeps the original filename inside it.

An existing identical destination can be verified and reused. A different
existing file is retained and reported as a collision, never overwritten
silently. Stop all writers before moving a finished dataset.

Normal help shows everyday choices. Use `gbi data copy --help` or
`gbi data move --help`. Expert overrides for existing scripts are described
under `--help-all` and at the end of this guide.

## Copy, move and durable storage

`copy` retains the source. `move` removes each selected Lustre or FSS source
only after independent destination readback, a verification receipt and source
identity checks. A packed move also verifies the stored archive before
removing the selected original entries.

Moving or restoring from Object Storage retains objects, object versions,
archive containers and chunk stores, including unselected archive members.
The obsolete `--delete-source` flag and Python `delete_source=True` are
rejected. They are not a way to override durable retention.

Every selected regular file is hashed while reading the source and again
while reading the destination. Symlinks are represented without following
their targets. Unsupported special files fail safely. A worker exit status,
file size or destination that merely exists is not deletion evidence.

A large transfer is not an atomic whole-directory transaction. Some units
can be verified and complete while others are still running or have failed.
Use the terminal summary and receipts to determine whether the entire dataset
is complete; do not treat a partly populated destination as a finished backup.

## Archives without tuning flags

Use `--archive` when you want a directory packaged for storage. Leave it off
when you want directly readable loose files. No packing threshold or chunk-size
choice is needed.

GBI keeps the destination you name as the single layout root. It packs child
directories into uncompressed GBI tar archives. If one exceeds the archive
metadata limit, GBI splits it further while preserving its original relative
paths. Root-level files, links and empty directories remain represented at
their original locations. Large packed units use verified resumable parts
automatically. Chunking the archive preserves its stored member metadata.

For example, a source containing `samples/` and `manifest.tsv` can be
stored as `samples.gbi.tar` and `manifest.tsv` under the chosen destination.
Restoring that destination recreates `samples/` and `manifest.tsv`, not an
extra competing directory tree. A chunked archive is recognized automatically
as well. Do not rename generated containers inside a larger archive directory.

The ordinary route records the complete archive partition on Lustre before
starting payload writes. A retry reuses that partition even when earlier
units have already removed their verified sources. It does not repartition
the remaining files into a conflicting layout. A nonempty destination without
a matching saved plan is rejected for inspection.

```bash
gbi data copy /your/lustre/experiment /your/object/experiment --archive
gbi data copy /your/object/experiment /your/fss/restored-experiment
```

Restoring needs no archive or chunking flags. You can also restore one generated
archive into an explicitly named directory:

```bash
gbi data copy /your/object/experiment/samples.gbi.tar /your/lustre/samples
```

GBI archives preserve member relative paths, empty directories, safe symlinks,
ordinary modes, modification times and hardlink relationships within a
container. They do not preserve ownership, ACLs, extended attributes,
privileged mode bits or hardlinks across different containers. A hardlink
whose other name is excluded becomes a regular file. Existing destination
directory permissions are preserved.

Timestamp verification permits truncation of less than one second. Lustre
restores have shown whole-second timestamps; do not depend on exact
nanosecond preservation. The archive's outer source directory is the layout
root, not a stored member with its own restored metadata.

A marked GBI archive is recognized even if explicitly named with a different
suffix. An ordinary unmarked tar file is copied as a file, not extracted.
Within a directory, retain generated `.gbi.tar`, `.gbi.tar.gz` and
`.gbi-chunks` names so discovery can recognize the containers.

## Selecting files

Use repeatable `--include` and `--exclude` filters. They match filenames
recursively, are case-sensitive, and use shell-style globs. Includes form a
union; an exclusion always wins. Quote globs so your shell passes them
unchanged.

```bash
gbi data move /your/lustre/run /your/object/run --archive \
  --include '*.pt' --include '*.pt.*' --exclude '*.tmp'
```

This selects `model.pt` and `model.pt.ready.json` at any depth, but not
`model.ptx`. Unselected files stay in the source. Without includes, files
are selected by default unless excluded.

The same filters can select members during restore:

```bash
gbi data copy /your/object/run /your/lustre/selected-run --include '*.pt'
```

For an archive, filters match member filenames recursively. For a chunked
regular file, they match the original filename recorded in its manifest.
Object Storage retains the full original even for a partial restore.

## Execution and preview

GBI normally chooses the execution location for you:

- Small plain transfers can run in your shell after a bounded metadata probe.
  The usual foreground limit is 8 GiB, configurable by the site.
- Larger or slow-to-discover work goes to Slurm. Automatic archive planning
  runs on an execution node, not as a recursive login-node scan.
- Inside an existing Slurm allocation, ordinary CLI and Python calls reuse
  that allocation.
- `--detach` explicitly submits another Slurm job. Add `--wait` if your
  script needs to wait for it.
- `--prefect` requests a managed personal Object Storage transfer instead
  of an ordinary mounted-filesystem transfer.

```bash
gbi data copy SOURCE DESTINATION --dry-run
gbi data move SOURCE DESTINATION --archive --detach --wait
```

An ordinary `--dry-run` writes no files and submits no job. For automatic
archives it explains the policy and destination root; the complete saved
partition is constructed on the execution node when you start the transfer.
It does not claim a huge directory has already been fully inventoried.

A foreground Ctrl-C stops foreground work. Ctrl-C while following a submitted
Slurm job only detaches the display; the job continues. Reconnect using the
printed transfer ID. Automatic thresholds are not completion-time promises.

## Prefect route

Use `--prefect` for direct personal Object Storage transfers through the
site's identity-bound broker. No Prefect UI or cloud credentials are needed
in your script. One endpoint must be your personal Lustre or FSS path and
the other your personal Object Storage path. Ordinary transfers instead use
mounted paths available through your Unix permissions.

```bash
gbi data copy /your/fss/experiment /your/object/saved-experiment --prefect --wait
gbi data copy /your/object/saved-experiment /your/lustre/restored --prefect --wait
```

The default writes individual objects. The deployed typed broker and production
flows accept renamed FSS destinations, exclusions, restore reservation
overrides, portable archive options and automatic `--archive --prefect`.
The CLI refuses unsupported requested options before submission; do not remove
a filter just to bypass that refusal.

Plain `--prefect --dry-run` is a local preview and submits nothing.
Supported packing/chunking previews submit a managed metadata-only plan:
they create tracking records, but no payload writes, receipts or source
deletions. Add `--wait` or inspect the printed run ID.

Inside a Slurm allocation, Prefect still submits a separate managed job.
Allow for its queue time in the calling job's time limit. It does not reuse
your allocation.

For archive destination reuse, Prefect requires its expected integrity
metadata. Objects uploaded through Alluxio or another tool may lack it.
Use the ordinary route to verify and reuse those destinations. Prefect
restores still verify ordinary objects without that archive-upload metadata.

## Status and recovery

```bash
gbi data status TRANSFER_ID
gbi data status TRANSFER_ID --watch
```

Use the printed transfer ID or a numeric Slurm ID when it identifies exactly
one transfer. If an allocation contains multiple transfers, GBI lists their
IDs rather than guessing. Exact transfer IDs remain usable after completed
history publication. Existing jobs retain their saved code even after a
new module is installed.

For an ordinary failed transfer, first confirm the job and workers have
stopped, inspect its receipts and retained sources, then rerun the same
command with the same source, destination and selection. A saved automatic
archive plan preserves completed units and the original layout. Interrupted
packed cleanup uses its saved original manifest, rechecks the destination,
and removes only unchanged selected entries still present. Do not delete
locks, journals, partial output or verification receipts to force a retry.

The retry command has a narrower meaning:

```bash
gbi data retry TRANSFER_ID
```

It resends an unchanged Prefect request after a lost submission reply.
It is not an ordinary Slurm retry or a retry of an already failed Prefect
flow. Inspect the existing run before submitting any new request.

A timeout observing the cluster does not prove a transfer stopped. Recheck
the same job. `scancel JOB_ID` cancels an identified Slurm job; it does not
undo earlier verified transfers or authorize deletion of incomplete sources.

## Python SDK

Load the module before starting Python, including inside a virtual environment.
The SDK calls the same installed CLI and waits for completion:

```python
from pathlib import Path
from gbi import data

finished = Path("/your/lustre/finished-run")
stored = Path("/your/object/finished-run")

data.move(finished, stored, archive=True, include=["*.pt", "*.pt.*"])
data.copy(stored, "/your/lustre/restored-run")
```

Call after all writers have closed, normally once from rank zero. Use
`data.copy` to retain filesystem originals. Both functions accept strings
or `pathlib.Path`, stream progress to the current log and return
`subprocess.CompletedProcess`. Failure raises
`subprocess.CalledProcessError`; a missing executable raises
`FileNotFoundError`. The SDK never silently retries a failed command.

A batch job can load the module and call that code directly:

```bash
#!/usr/bin/env bash
#SBATCH --job-name=train-and-archive
#SBATCH --cpus-per-task=2
#SBATCH --time=02:00:00

set -euo pipefail
module load gbi
python train_and_archive.py
```

Use `prefect=True` only when you want a separate managed transfer and the
personal route requirements are met. Ordinary calls reuse the allocation.

Complete signatures, including compatible expert arguments:

```text
copy(source, destination, *, include=(), exclude=(), pack=None,
     pack_small=False, chunk_size=None, dry_run=False, prefect=False,
     job_size=None, archive=False)

move(source, destination, *, include=(), exclude=(), pack=None,
     pack_small=False, chunk_size=None, dry_run=False, prefect=False,
     delete_source=False, job_size=None, archive=False)
```

Filters accept one string or an iterable of strings. Boolean arguments must
be actual booleans. `delete_source=False` is compatibility-only:
`True` is rejected. Source retention is identical to the CLI.

## Cached usage

`usage` shows live UID quota when the Lustre client is available, plus a
cached owner-scoped inventory snapshot:

```bash
gbi data usage
gbi data usage results --depth 2 --limit 20
```

The optional path must be below your own Lustre root. Depth and limit must
be positive. The report shows capture time, age and incomplete/stale states.
It reports logical apparent bytes, not allocated filesystem space, and does
not recursively scan Lustre or start the separate inventory publisher.

## Expert options

Normal archive use does not need these choices. Existing scripts may keep
using them; `gbi data copy --help-all` exposes them.

| Option | Ordinary / Slurm | Existing allocation | Prefect |
| --- | --- | --- | --- |
| `--archive` | Automatic directory archive | Yes | Production archive flows |
| `--include / --exclude` | Recursive filename globs | Yes | Up to 100 plain globs |
| `--pack tar/gzip` | One exact archive target | Yes | Archive only |
| `--pack-small` | Legacy selective small-file packing | Yes | Archive only |
| `--chunk-size SIZE` | Regular file or new archive | Yes | Archive only |
| `--job-size small/large` | No | No | Restore reservation override |
| `--local` | Requires allocation | Guard only | No |
| `--detach` | Submit another Slurm job | Submit another job | No |
| `--wait` | Wait for completion | Yes | Yes |
| `--dry-run` | No writes/submission | Yes | Local preview or supported managed plan |
| `--delete-source` | Rejected | Rejected | Rejected |

Do not combine `--archive` with `--pack`, `--pack-small` or `--chunk-size`.
Restore without encoding flags. `--detach`, `--local` and `--prefect` are
mutually exclusive; so are `--pack` and `--pack-small`.

### Exact archives and legacy small-file packing

`--pack tar` creates one uncompressed archive at the exact
`.gbi.tar` target; `--pack gzip` uses an exact `.gbi.tar.gz` target.
Explicit gzip is available when compression is worth its CPU cost.
Automatic archives use tar; they do not guess that checkpoints compress well.

`--pack-small` is the older selective-packing mode. Its usual site defaults
are at most 64 KiB per file, at least 40 files per group and at most 1 GiB per
archive. Larger siblings remain loose. Its bounded `--dry-run` can report
an incomplete lower-bound inventory; that is not authorization to archive
or delete anything. Prefer `--archive` for a saved whole-directory layout.

The archive format limits both manifest and source-observation metadata to
64 MiB. Explicit whole-directory packing may exceed that limit even for a
small payload. Automatic archives split such directories internally. A larger
chunk size does not increase the metadata limit.

### Explicit chunks and restore reservation

`--chunk-size 64MiB` writes verified resumable parts with a final manifest
and completion marker. For a direct file, give the original destination name
without `.gbi-chunks`; GBI adds that suffix. Whole-pack targets keep their
`.gbi.tar` or `.gbi.tar.gz` suffix and gain `.gbi-chunks` as well.
An incomplete store is not restorable before its completion marker validates.

Raw-file chunks preserve content and filename, not POSIX modes or timestamps.
Archive chunks retain archive member metadata. Mutable journals and optional
archive staging use configured Lustre scratch, not local temporary disk.

`--job-size small` requests 2 CPUs/16 GiB for a Prefect restore;
`large` requests 8 CPUs/48 GiB. Omit it to retain the deployment setting.
It changes neither the time limit nor the caller's allocation and does not
guarantee immediate scheduling. Python uses `job_size="small"` or
`job_size="large"`.

## Troubleshooting and records

If a destination differs, keep it and inspect the collision. If a source
changed, stop writers and inspect the saved request before retrying.
If a site rejects a requested option, keep its intended selection and use
a supported route rather than silently changing which files will move.

For a mount wait, worker deadline or missing terminal summary, retain the
printed phase, path, receipt location and Slurm log. Confirm process state;
do not infer completion or cancellation from a transport timeout.

Active state is private under `.gbi` on Lustre. Completed ordinary history
is published under the user's Object Storage mount at
`.gbi/transfers/PARENT_ID/TRANSFER_ID/`. The parent is the Slurm job/allocation
ID, or the transfer ID for a foreground run. Follow the printed history path.
Requests record code identity and route parameters; receipts record
verification and deletion evidence. Prefect publishes its managed receipts
through its own maintained flow.

An archive may finish before the rest of a dataset. A complete recovery means
every selected path is accounted for, the destination is independently
verified and its original directory layout is recoverable.
