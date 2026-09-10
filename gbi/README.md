# Move data with `gbi data`

For the execution and data paths, see the [architecture report](ARCHITECTURE.md).

Load the module, give it a source and destination, and watch the transfer:

```bash
module load gbi
gbi data roots
gbi data move /your/lustre/experiment /your/alluxio/experiment
```

Use the paths printed by `roots`. A directory's **contents** go into the exact
destination directory you name. A single file goes to the named destination
file, or into an existing destination directory under its original name.
Spaces and unusual file names are supported; quote paths in your shell.

`roots` prints the configured user-facing mount paths, preserving aliases such
as `/mnt/user-data/$USER` for Alluxio instead of exposing its internal shard
directory. These paths can be used directly in copy and move commands.

Everyday transfers start immediately in your current shell. Up to **8 GiB**
of selected data runs in the foreground, including Alluxio transfers, with up
to four files copying concurrently. Tiny selections (up to 8 MiB and 32 files)
use native I/O; larger foreground selections use parallel rclone readers.
The tiny-selection limit chooses a reader, **not whether to queue a job**.
Every reader uses the same checksum, receipt and source-deletion safeguards.

Above 8 GiB the CLI submits to the configured Slurm partition. It also uses
Slurm when a directory cannot be sized within a bounded metadata scan (five
seconds or 100,000 visited entries). This avoids an unbounded login-node tree
walk. Already inside a Slurm allocation, the CLI reuses it automatically.
These are configurable defaults, not an estimate of completion time.

Foreground progress appears immediately. Ctrl-C stops foreground work;
unfinished files keep their sources. There is no five-minute foreground
cutoff. Use `--detach` to request a Slurm job explicitly when work should
survive logging out. For submitted jobs, the terminal follows progress and
Ctrl-C detaches only the display. Reconnect using the printed transfer ID:

```bash
gbi data status TRANSFER_ID --watch
```

The display shows verified files and bytes, active transfer bytes, average
verified throughput and bytes removed from the source. It shows a percentage
only after file discovery finishes. Verified throughput includes copy and
readback time. Source-byte counts are logical bytes: hard links, filesystem
accounting and concurrent writes can make the change in free space different.
Active phases distinguish copying, closing, destination verification and source
rechecking, so a long close does not look like a stalled copy. Receipts include
separate timings for these operations. Close timing measures the client call;
asynchronous completion waits are included in destination readback time.

`gbi data status TRANSFER_ID` prints one update. Slurm's usual `scancel JOB_ID`
cancels the transfer itself; already verified moves remain moved and incomplete
files retain their sources.

## Copy, move and bring data back

```bash
gbi data copy SOURCE DESTINATION   # keep every source
gbi data move SOURCE DESTINATION   # verify each file, then remove its source
```

Both work between FSS, Lustre and personal Alluxio storage. Lustre is scratch,
FSS is for code and configuration, and Alluxio is for retained data and archives.
There is no required intermediate filesystem for FSS ↔ Lustre.

**Alluxio/Object Storage sources are kept by default**, including with `move`.
Bring an experiment back to scratch without losing the archived copy:

```bash
gbi data move /your/alluxio/experiment /your/lustre/experiment
```

Only explicitly asking removes the retained copy:

```bash
gbi data move /your/alluxio/experiment /your/lustre/experiment --delete-source
```

## Existing files and interruptions

An existing identical destination is independently checked and reused. A
different destination is preserved and reported as a failure; the CLI never
silently overwrites it. Re-run the same command after an interruption. The
CLI can replace its own incomplete output only when the source identity is
unchanged and the destination still has the inode recorded by that attempt.

Files must be quiescent while moved. Changed inode, type, size or modification
time prevents source deletion. Permission changes alone do not constitute a
content change. User programs must not edit incomplete destination files.

Each completed source stream is SHA-256 hashed; the destination is then reopened
and independently hashed. Deletion requires matching hashes, unchanged source
identity and a successfully written verification receipt. A writer's exit status
alone never authorizes deletion. Moves also rehash the source before deletion
to catch edits hidden by coarse filesystem timestamps. On Alluxio this verifies bytes through the
mount, whose configured write-through persistence is a deployment prerequisite;
it is not a credentialed, independent Object Storage GET.

For Alluxio, writes are sequential to exclusively created final-name files.
There are no large-object renames, truncation or concurrent writers to one file.
The CLI waits for completed files to become readable, with a bounded deadline.

Symbolic links are never followed. FSS/Lustre destinations receive real links;
Alluxio stores the target string as `NAME.rclonelink`, following rclone's portable
representation. Bringing that tree back reconstructs links without reading their
targets. `.rclonelink` is reserved for these records: existing ordinary files
with that suffix on FSS/Lustre are reported instead of silently reinterpreted.
Object-backed copies do not preserve executable bits or original timestamps;
FSS/Lustre-to-FSS/Lustre regular files preserve mode and modification time to
the destination filesystem's timestamp precision.

## Logs and receipts

Live progress, retry state and the Slurm log use a private `.gbi` directory on
Lustre scratch. No transfer history accumulates on FSS. Finished records are
published as immutable files under your Alluxio `.gbi/transfers/TRANSFER_ID/` tree,
independently read back, and then the temporary run directory is removed.
`status` finds both active and archived records. If Alluxio history publication
fails, the command reports failure and retains its scratch records for recovery.

Foreground history bundles the request, final progress and receipts in one
immutable `history.json`, avoiding multiple tiny Alluxio uploads. Slurm history
keeps separate records including its output log.

The append-only JSONL receipt records each verified source/destination,
SHA-256, source fingerprint and subsequent deletion. The request records the
exact submitted code hash. A queued job uses a snapshot of that code and its
site configuration, so changing the module default does not change queued work.

## Useful options

```bash
gbi data move SOURCE DESTINATION --dry-run
gbi data move SOURCE DESTINATION --detach
gbi data move SOURCE DESTINATION --include '*.pt' --include '*.pt.*'
gbi data copy SOURCE DESTINATION --wait
```

Dry-run prints paths, deletion policy and the selected execution mode without
writing anything. It uses the same bounded metadata probe as a real command;
larger tree discovery streams into the worker pool on the execution node.
`--include` matches file names recursively, not full paths. A foreground
selection is a metadata snapshot: files added after the probe are not included,
and changes to selected files are rejected before copying.

Foreground calls wait for completion, including in scripts. Slurm submission
returns immediately in scripts unless `--wait` is specified. `--detach`
explicitly submits background work. `--local` requires an existing allocation;
allocation reuse is automatic without it too.

Exit 0 means successful submission, or a fully completed transfer when waiting.
Exit 1 means a failed/interrupted transfer or failed history publication;
inspect the receipt for files already moved. Exit 2 reports an invalid request
or unavailable command/configuration. Detaching is successful and returns 0.

## Installation and validation

The existing module harness installs in-tree source; no image or Prefect change
is needed. Python 3.9+ must be available on every selected node. The module loads
the pinned rclone dependency automatically.

```bash
make rclone
GBI_SITE_LUSTRE_ROOT=/site/scratch/users \
GBI_SITE_FSS_ROOT=/site/home/users \
GBI_SITE_BUCKET_ROOT=/site/personal/users \
GBI_SITE_PARTITION=site-partition make gbi
```

For a shared cluster installation, run the recipe as a software maintainer
from the reviewed release checkout and add `GBI_MODULE_PATH=/site/shared/software`
to the `make` command. Software goes under `gbi/0.3.1` and the modulefile under
`modules/gbi/0.3.1.lua` in that tree. Use the same install root as the cluster's
existing rclone module. When its `modules` directory is already in the shared
Lmod environment, users only need `module load gbi`; no per-user installation,
container rebuild or login-node restart is required. Check `module show gbi`,
`gbi --version` and `gbi data roots` from a normal user shell after installation.

Each root is a parent directory; the authenticated local username is appended
and its individual root alias resolved internally for path-safety checks.
The `roots` command displays the alias itself. Site settings come from `GBI_SITE_*`
environment variables at installation, never from public source code. Supported
keys and defaults are in [storage.py](src/lib/gbi_data/storage.py). Defaults
are four parallel files, two CPUs and 4 GiB, with a one-day per-file deadline
and a 20-minute Alluxio readback settling deadline. Choose a partition with all
three real mounts. Missing user roots are refused rather than created.

Local tests use real rclone and fault injection. Set `TMPDIR` inside your
workspace before running them:

```bash
PYTHONPATH=gbi/src/lib python3 -B -m unittest discover -s gbi/tests -v
shellcheck gbi/src/bin/gbi gbi/sm-config/install.sh
```

See [SPEC.md](SPEC.md) for the implementation boundary and validation limits.

The maintained cluster tests run inside Slurm using newly created directories
under the invoking user's configured roots. `tests/cluster_selftest.py` checks
all six directions and accepts `--large-bytes N` for four parallel files;
`tests/cluster_retry_test.py` checks recovery from a real Alluxio partial write.
Set `PYTHONPATH` to the installed module's `lib` directory when running these
Python tests directly. They retain fixtures and evidence for inspection.
`tests/cluster_lock_test.py NEW_SCRATCH_DIRECTORY` runs with `srun --nodes=2
--ntasks=2 --ntasks-per-node=1`; select Slurm nodes on distinct physical hosts.

`tests/cluster_adaptive_test.py` checks fresh foreground fixtures from a login
shell, native and parallel rclone readers, the bulk planning boundary and
explicit background execution. Run it inside an allocation to check reuse.
It retains fixtures and receipt-backed results for inspection.
