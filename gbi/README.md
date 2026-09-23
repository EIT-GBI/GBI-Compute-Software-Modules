# Move data with `gbi data`

For the execution and data paths, see the [architecture report](ARCHITECTURE.md).

Load the module, give it a source and destination, and watch the transfer:

```bash
module load gbi
gbi data roots
gbi data move /your/lustre/experiment /your/alluxio/experiment
gbi data usage
```

Use the paths printed by `roots`. A directory's **contents** go into the exact
destination directory you name. A single file goes to the named destination
file, or into an existing destination directory under its original name.
Spaces and unusual file names are supported; quote paths in your shell.

`usage` reports your current Lustre quota when the `lfs` client is available,
plus the latest owner-scoped inventory snapshot. The inventory is a cached
logical-byte estimate, labelled with its snapshot age; it is not allocated
filesystem space and does not recursively scan Lustre. Select a directory
below your own Lustre root and choose how many levels to show:

```bash
gbi data usage /your/lustre/experiment --depth 2 --limit 20
```

The published inventory is read-only and may be partial or stale.
Those states are shown in the output. Shared paths and another user's root are
rejected. If no snapshot has been published yet, the live UID quota still
appears and the cached folder report is reported as unavailable.

`roots` prints the configured user-facing mount paths, preserving aliases such
as `/mnt/user-data/$USER` for Alluxio instead of exposing its internal shard
directory. These paths can be used directly in copy and move commands.

The printed roots are convenient starting points, not an access allowlist.
You can copy or move shared directories, instrument data and other users' paths
where Unix permissions allow it. The CLI runs as you and does not grant access.
For example:

```bash
gbi data copy /mnt/instrument-data/illumina_nextseq_1/RUN \
  /mnt/lustre/shared/instrument-data-testbed/RUN
```

On Linux, the CLI detects Lustre, FSS/NFS and Alluxio from the mounted
filesystems, including Alluxio-backed instrument NFS exports. Instrument and
other Object Storage originals are retained with `move` unless you explicitly
pass `--delete-source`; Unix permissions and read-only mounts still apply.
The same verification, overwrite protection, progress and automatic
foreground/Slurm execution apply to shared paths.

Everyday transfers start immediately in your current shell. Up to **8 GiB**
of selected data runs in the foreground, including Alluxio transfers, with up
to four files copying concurrently. Tiny selections (up to 8 MiB and 32 files)
use native I/O; bulk transfers also read individual regular files up to 8 MiB
natively, while larger files use rclone readers.
The tiny-selection limit chooses a reader, **not whether to queue a job**.
Every reader uses the same checksum, receipt and source-deletion safeguards.

Above 8 GiB the CLI submits to the configured Slurm partition. It also uses
Slurm when a directory cannot be sized within a bounded metadata scan (five
seconds or 100,000 visited entries). This avoids an unbounded login-node tree
walk. Already inside a Slurm allocation, the CLI reuses it automatically.
These are configurable defaults, not an estimate of completion time.

Bulk discovery runs independently of the file workers, so completed files can
be receipted while a slow mounted tree is still being traversed. A source
metadata lookup failure is recorded for that path and discovery continues with
the remaining entries. Source metadata and empty-directory preparation run in
the same supervised worker slots, so a slow preparation does not stop receipts
for other entries. Source deletion still requires the usual independent
verification receipt. Empty directories are carried through as placeholders;
they do not inflate verified-file or byte counters. Discovery and other mount
waits have deadlines. A deadline stops waiting and reports the affected phase
and path; it does not repair an unresponsive mount or prove kernel I/O stopped.

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

Slurm logs print timestamped progress on separate lines, including unchanged
progress at least every 15 seconds while the coordinator is responsive. Use
`tail -f` on the printed run directory's `slurm.out` to follow them. Discovery,
copy/close/readback and saving history remain distinct; active bytes are not
yet independently verified bytes.

If a mount deadline expires, the CLI exits with failure and prints the phase,
path, last observed verified/deleted byte counts, retained record directory and
any worker PIDs that still need checking. Those counts may be incomplete.
Inspect receipts and confirm the Slurm job and reported workers have stopped
before re-running the same command. Never remove its lock files or partial
outputs to force a retry: a process stuck in kernel I/O may still hold the lock.
The CLI never substitutes local storage for a missing mount. If the state mount
itself is unavailable, writing a final failure record is not guaranteed; retain
the command output or Slurm log. `status` detects a dead foreground coordinator
on the same host; from another host, check the original process and output.

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

## Pack a directory and bring it back

Use `--pack tar` to store many files as one portable GBI archive. Give the
destination its exact `.gbi.tar` filename. `--pack gzip` compresses the same
format and requires `.gbi.tar.gz`; it uses more CPU and may save little space
for already compressed data.

```bash
gbi data copy /your/lustre/experiment /your/alluxio/experiment.gbi.tar --pack tar
gbi data move /your/fss/project /your/alluxio/project.gbi.tar.gz --pack gzip
gbi data copy /your/alluxio/experiment.gbi.tar /your/fss/restored-experiment
```

The normal copy/move command recognizes a marked GBI archive and restores its
contents directly into the destination directory. Restore onto either Lustre
or FSS, regardless of the origin. An ordinary unmarked tar file is copied as a
file. Recognition validates the format marker and manifest, not just its name.
An explicitly named GBI archive can be restored even after renaming it. Within
a larger directory, keep generated `.gbi.tar`, `.gbi.tar.gz` and `.gbi-chunks`
names so discovery can find containers without opening every ordinary file.

Includes and exclusions apply to the **files inside** the archive:

```bash
gbi data copy /your/lustre/experiment /your/alluxio/experiment.gbi.tar \
  --pack tar --exclude '*.tmp'
gbi data move /your/alluxio/experiment.gbi.tar /your/lustre/selected \
  --include '*.pt' --exclude '*.pt.ready.json'
```

Archive sources stay by default. A partial restore always retains the containing
archive, even with `--delete-source`. A complete explicitly requested removal
requires verified restored files. Before deleting packed filesystem originals,
GBI reads the entire stored archive back, checks the source inventory and every
selected checksum, and removes only unchanged selected entries. New or excluded
files are never removed by recursive directory cleanup.

Archives preserve relative paths, empty directories, symlinks (including dangling
links), hardlink relationships, ordinary modes and modification times. A selected
hardlink whose other name was excluded is restored as a regular file. Ownership,
ACLs, extended attributes and privileged mode bits are not restored. Symlink
modes follow the destination filesystem; modification times may lose less than
one second of precision. Existing identical files are reused only when their
representable metadata also agrees. Existing directory permissions are kept.
Special files, unsafe member paths and conflicting destinations fail clearly.

Packing streams directly to its destination unless chunking is requested.
Archive restore uses Slurm when the decoded size is unknown; an existing
allocation is reused. Progress shows packing, verification and extraction as
separate phases. The file count treats each archive as one transfer unit, while
verified bytes count the selected logical payload.

## Pack small subdirectories and leave large files accessible

Use `--pack-small` for mixed trees. Preview the exact proposed archive layout:

```bash
gbi data copy /your/lustre/project /your/alluxio/project --pack-small --dry-run
gbi data move /your/lustre/project /your/alluxio/project --pack-small --exclude '*.tmp'
gbi data copy /your/alluxio/project /your/fss/restored-project
```

GBI qualifies the deepest non-overlapping small-file subdirectories. Their names
become `SUBDIRECTORY.gbi.tar`; adjacent large files remain ordinary files.
The default requires at least 40 selected regular files, each no larger than
64 KiB, and at most 1 GiB of selected payload per archive. These conservative
opt-in limits keep larger siblings accessible. Bounded Linux measurements found
about 70% lower elapsed time for 40-file cases at 4 KiB and 64 KiB; shared-storage
load varies, so this is not a promised speedup. Maintainers can set
`pack_small_bytes`, `pack_min_files` and `pack_max_bytes` at installation.
The directory you named as the source remains the layout root; use `--pack` if
you want to pack that whole directory instead. Names such as `.git` or
`node_modules` are candidates under the same policy and filters as other names.

Dry-run shows the thresholds, the complete list of proposed archives, grouped
counts and examples for unqualified or loose entries, and staging requirements.
These are metadata observations, not verified payload totals. An unfinished scan
is labelled as a lower bound and does not authorize packing; narrow the source if
the scan budget is exceeded.
The worker rechecks qualification and verifies the resulting archive before any
source removal. A mixed restore unpacks generated archives alongside loose files.

## Resume large files or archives in verified parts

`--chunk-size SIZE` stores a file as a directory of verified parts. For example,
`64MiB` means 64 binary mebibytes per part. Repeat the same command after an
interruption; GBI verifies existing parts and resumes only the unchanged source.

```bash
gbi data copy /your/lustre/checkpoint.bin /your/alluxio/checkpoint.bin --chunk-size 64MiB
gbi data copy /your/alluxio/checkpoint.bin.gbi-chunks /your/lustre/checkpoint.bin
gbi data copy /your/lustre/project /your/alluxio/project.gbi.tar \
  --pack tar --chunk-size 64MiB
gbi data copy /your/alluxio/project.gbi.tar.gbi-chunks /your/fss/restored-project
```

The generated `.gbi-chunks` directory contains a versioned manifest, isolated
parts and a completion record. Do not rename or edit its contents. Restore
reconstructs the original file or archive automatically and verifies part order,
part checksums and the complete payload checksum. Arbitrary directories with a
`manifest.json` are not interpreted as GBI chunks. The same total worker limit
applies; each worker handles its parts sequentially.

Chunked archives need a stable archive staged on **Lustre scratch**. GBI checks
capacity before writing, bounds staging to the measured input plus format
overhead, and reuses valid owned staging on retry. No archive is staged on FSS.
Useful recovery state is retained after failure; only owned staging is removed
after success. Chunking an ordinary file does not need an extra full-file stage.
Changes to the source or chunk format refuse resume. Invalid parts are replaced
only when GBI's private journal proves ownership; unrelated files stay untouched.
Resume rereads the source and completed parts to establish integrity; it saves
verified part rewrites, not all read I/O. Progress reports verified part counts
and bytes separately from completion of the whole logical file or archive.

## Submit a managed migration with Prefect

Use `--prefect` explicitly when you want the maintained migration flows to
handle a personal-storage transfer. The CLI never selects Prefect based on size.
Ordinary transfers keep their automatic foreground/Slurm behavior.

The Prefect flow name "archive" means moving files to Object Storage. It does
not create tar/gzip archives: each selected file remains a separate object.
Use the ordinary transfer route for `--pack` or `--pack-small`; those options
cannot currently be combined with `--prefect`.

```bash
gbi data copy /your/lustre/experiment /your/alluxio/experiment --prefect --dry-run
gbi data move /your/lustre/experiment /your/alluxio/experiment --prefect --wait
gbi data status TRANSFER_ID --watch
gbi data retry TRANSFER_ID --wait
```

These flows transfer payload bytes directly between the filesystem and Object
Storage, bypassing the Alluxio payload write/read path. Prefect orchestrates Slurm
jobs; archive completion still includes an Alluxio ownership/presentation step.
No Prefect UI, token, bucket name or deployment name is
needed: the local broker binds the submission to your Unix account and approved
personal route. The service must be installed at your site.

This route supports personal Lustre/FSS ↔ Object Storage paths. Shared paths,
wildcards in paths, exclusions, packing and chunks use ordinary transfers.
FSS archives and all restores require the same relative path on both sides;
Lustre archives may use a different relative destination. `--include` uses
file-name patterns recursively, but **each pattern must match at least one
file** in the Prefect route. Restores retain Object Storage originals;
`--delete-source` is unavailable. `copy` retains filesystem sources, while
`move` archives remove them only through the flow's verification/deletion rules.

Interactive submissions follow Prefect state; scripts return after submission
unless `--wait` is given. Ctrl-C detaches the display and leaves the migration
running. `scancel` is not a Prefect cancellation command. The displayed status
is the Prefect flow state, not an invented CLI byte count or verification receipt.
The flows keep their maintained migration receipts.

Before submission, the CLI saves an exact request and request ID under its
private Lustre `.gbi/runs/TRANSFER_ID` directory. If the reply is lost, first use
`status`; `retry TRANSFER_ID` resends that same request ID and parameters. It
never creates a replacement request. Do not repeat the original copy/move
command to recover a lost reply: that creates a new request. Broker request and
status records currently remain on scratch, including after terminal completion;
they are not a replacement for the flows' durable receipts.

## Existing files and interruptions

An existing identical regular-file destination is independently checked and
reused. Its first full destination digest is retained through the final
identity check; any destination change before the receipt fails the transfer.
This avoids a duplicate full destination read on the reuse path without
changing parent safety checks, transfer concurrency, or conflict handling. A
different destination is preserved and reported as a failure; the CLI never
silently overwrites it. Re-run the same command after an interruption. The
ordinary-file worker can replace its own incomplete output only when the source identity is
unchanged and the destination still has the inode recorded by that attempt.
Source-open failures happen before exclusive destination creation, so an
unreadable source cannot leave a new empty destination behind.
An interrupted bulk run keeps discovery incomplete if enumeration had not
finished, so its final progress does not claim a percentage. Retrying the same command rechecks the source tree and
reuses verified destinations.

An interrupted unchunked archive is rebuilt from the current source only when
its partial destination is still owned by that attempt. It receives a new full
manifest and readback before any source cleanup. Chunked payloads instead bind
resume to the original source revision and format; changed sources refuse resume.

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
If publication succeeds but temporary-directory cleanup fails, the transfer
keeps its terminal result and reports the saved history and retained scratch path.

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
gbi data move SOURCE DESTINATION --include '*.pt' --exclude 'temporary*'
gbi data copy SOURCE DESTINATION --detach --wait
```

Dry-run prints paths, deletion policy and the selected execution mode without
writing anything. It uses the same bounded metadata probe as a real command;
larger tree discovery streams into the worker pool on the execution node.
`--include` matches file names recursively, not full paths. Repeat `--exclude`
to leave out matching file names even when they match an include. Both use
case-sensitive shell-style patterns; quote them so your shell does not expand
them. Filters also apply to a single named source file and to symbolic links.
Directory names do not exclude their contents. Excluded sources stay untouched
with `move`. A foreground
selection is a metadata snapshot: files added after the probe are not included,
and changes to selected files are rejected before copying.

Foreground calls wait for completion, including in scripts. Slurm submission
returns immediately in scripts unless `--wait` is specified. `--detach`
explicitly submits background work and returns immediately. Combine
`--detach --wait` to submit to Slurm and wait for its result, including in
scripts. Ctrl-C then detaches the display while the job continues.
`--local` requires an existing allocation;
allocation reuse is automatic without it too.

Exit 0 means successful submission, or a fully completed transfer when waiting.
Exit 1 means a failed/interrupted transfer or failed history publication;
inspect the receipt for files already moved. Exit 2 reports an invalid request
or unavailable command/configuration. Detaching is successful and returns 0.

## Installation and validation

The existing module harness installs in-tree source. Ordinary transfers need no
image or Prefect change; `--prefect` additionally requires the maintained login
broker and catalogue configuration. Python 3.9+ must be available on every selected node. The module loads
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
to the `make` command. Software goes under `gbi/0.4.1` and the modulefile under
`modules/gbi/0.4.1.lua` in that tree. Use the same install root as the cluster's
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

Mount-dependent startup/state operations default to a 120-second deadline;
discovery defaults to 120 seconds without an emitted entry or directory-scan
progress while worker capacity is available; history publication defaults to 30 minutes without a
finished history-file worker. Only actual worker completion resets that history
deadline; periodic progress messages cannot hide a stalled history operation.
Set `mount_timeout`, `discovery_timeout` and `history_timeout` through the
corresponding `GBI_SITE_*` installation settings. Loading the site configuration
itself has a 120-second bootstrap deadline before configured values are known.
These bound waiting, not the time needed to prove a kernel-blocked writer stopped.

Personal roots provide scratch/history locations and storage-type hints; they
do not restrict transfer access. Shared paths use the execution node's mount
table. Alluxio FUSE types and NFS export names containing `alluxio` identify
Object Storage, preserving its default source-retention policy. Other mounted
filesystems use ordinary POSIX transfer behavior. On systems without Linux
mount information, configured personal roots provide the storage-type hints.

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
