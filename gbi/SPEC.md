# GBI data CLI: filesystem movement

## User contract

`gbi data copy SOURCE DESTINATION` and `gbi data move SOURCE DESTINATION` work
between the invoking user's FSS, Lustre and personal Alluxio roots. The paths
select the storage route. The user does not select rclone, Prefect, a partition
or cloud credentials. Foreground and Slurm execution use that same identity.

Move is copy → independent SHA-256 verification → receipt → source deletion.
Alluxio/Object Storage sources are retained unless `--delete-source` is
explicit. Existing different destinations are preserved. Native filesystem
permissions remain the authority; the program has no elevated identity.

This replaces the earlier unshipped archive/stage-only CLI design. It reuses
the working parcopy final-name, per-file concurrency and owned-partial retry
approach. It adopts hash-during-copy and bounded discovery from the bulk-worker
lessons. Prefect is not a runtime dependency or a hidden size-based detour.

## Storage roles

- Alluxio: retained data, archives and completed transfer logs/receipts.
- Lustre: computation and temporary transfer execution state.
- FSS: installed code and site configuration.

Live Slurm output and atomic progress snapshots are temporary scratch files.
Closed records are copied to unique final Alluxio paths and read back before
scratch cleanup. Alluxio is never asked to append an active log or repeatedly
overwrite a progress file. A failed archival attempt retains temporary evidence.

## Runtime structure

- `storage.py`: data-only site settings and canonical own-root checks.
- `selection.py`: bounded metadata probe and streaming directory selection.
- `cli.py`: execution/reader choice, bounded worker supervision, history.
- `transfer.py`: one-file transfer and source-deletion boundary.
- `jobs.py`: direct Slurm submission, status and terminal rendering.

Each submitted job has a code/configuration snapshot and source digest. Each
file worker is a separate process with an overall timeout, including FUSE
metadata calls, write close and readback. Native I/O or rclone supplies a
sequential input stream. The worker hashes while writing to a newly created final-name file,
then hashes a new destination read. No whole-tree checksum pass precedes work.
Moves rehash the source before recording verification and deleting it, because
whole-second Lustre timestamps can hide same-size edits. Quiescent inputs are
still required; the CLI does not lock applications out of their source files.

The foreground probe retains a bounded selection of metadata (at most 100,000
visited entries). Bulk discovery uses a depth-first scandir iterator. The
orchestrator holds at most the configured file concurrency in flight. Each unfinished file has a small journal.
Completed journals are removed. A fixed set of cross-node lock stripes survives
individual attempts; kernel-blocked workers and their rclone children inherit
the lock so a new attempt cannot write over them.

Incomplete outputs can be replaced only if the journal identifies both the
unchanged source and the current destination inode. Otherwise the CLI leaves
both versions intact. Cross-node flock semantics are a deployment prerequisite.

## Automatic execution

Selections up to 8 GiB start in the current shell. Up to four files run in
parallel; up to 8 MiB and 32 files uses native reads, with rclone for larger
selections. Reader choice is independent of placement. Larger than 8 GiB or
a probe exceeding five seconds / 100,000 visited entries selects Slurm.
An existing allocation is reused automatically, regardless of size.
`--detach` explicitly chooses a new background Slurm job.

There is no short total foreground deadline. Existing per-file and Alluxio
settling timeouts still apply. Ctrl-C or shell hangup stops foreground workers;
there is no automatic mid-transfer handoff to Slurm. Foreground selection
fingerprints are checked before copying; late directory arrivals are outside
that snapshot. Sources must be quiescent.

Foreground history is one immutable JSON bundle; bulk history retains the
separate log and receipt files. Both are verified through Alluxio before
scratch cleanup. All controls are site settings in the maintained module recipe.

## Progress

Foreground commands report progress directly and wait even in scripts.
Interactive Slurm commands follow the submitted job. Ctrl-C detaches the viewer;
the Slurm job continues. `status JOB_ID --watch` reconnects on the HPC.
Piped/scripted calls return after submission unless `--wait` is requested.

Counters distinguish discovered, verified, active, removed-source and failed
work. Discovery has no invented total; the percentage appears only when the
iterator completes. Final state comes from the transfer summary, with Slurm
state as a fallback for killed jobs. Scheduler success without a final transfer
summary is not reported as successful data movement.
Per-file receipts distinguish copy, client close, destination readback and
source recheck time. Readback includes waits for asynchronous completion;
these timings alone do not identify an Alluxio backend bottleneck.

## Boundaries and validation

Independent mount readback is the normal user proof. Direct Object Storage
readback requires credentials and is not silently introduced. Alluxio must be
configured for accepted write-through persistence, and actual large-file,
failure/retry and multi-node tests are required before calling a site supported.

Tests must distinguish ordinary local filesystems from actual FSS/Lustre/Alluxio.
Synthetic local Slurm adapters test the interface only. Cluster tests run as
the invoking user, inside that user's test roots, with no IAM expansion or
unrelated transfer cancellation.

Symlink targets are data, never traversed. Alluxio uses `.rclonelink` records;
ordinary files with that suffix on other filesystems are refused. Original
permissions/timestamps are not persisted through Alluxio in this implementation.
Concurrent applications must not modify files being moved. Extended attributes,
ACL replication and preservation of hard-link relationships are outside scope.

No automatic tar packing, data catalogue, cloud-credential broker, Prefect
gateway, new service, new execution partition or infrastructure redesign is
part of this implementation.
