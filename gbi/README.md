# GBI data transfers

<img src="docs/assets/gbi-cli-logo.png" alt="GBI CLI logo" width="160">

Copy, move, archive and restore research data using your normal HPC account.
GBI verifies destinations before removing eligible Lustre/FSS sources.
**Object Storage originals and versions are always retained.**

Read the [user guide](docs/user-guide.md) or [PDF](docs/user-guide.pdf) for
Python, selection, recovery and expert options.

## Release status

The **0.4.11 CLI and typed Prefect broker are deployed and ordinary-user
accepted**, checked on 28 September 2026. **0.4.14** is a maintenance release
for large archive moves: the cleanup guards for chunk stores and plain
archives no longer touch the destination mount between their complete checks
(each guard runs on the first entry, once a minute and once after the last
removal; on an Alluxio FUSE mount every `lstat`, including the 0.4.12 manifest
tripwire, is a master round trip per path component and held 22,604- and
136,542-entry moves to eight to twelve files per second), and shared lock
stripes widen from 4,096 to 65,536 so an hours-long archive pack no longer
blocks unrelated file moves that hash to the same stripe. Formats, receipts and
journals are unchanged. **0.4.15** changes only `gbi data usage`: an aligned
Lustre/FSS storage summary, readable sizes and counts, clipped folder names,
and an explicit `not published` FSS state; transfer behaviour is identical to
0.4.14. **0.4.16** names a failed weekly FSS lookup: the row keeps last
week's value with its real age and adds `(latest lookup failed)` instead of
reporting `not published`. Automatic `--archive --prefect`,
selection filters, portable archive formats, distinct destinations and small
restore reservations are active through the production FSS and Lustre archive
and restore flows. Existing jobs and explicitly loaded older modules are
unchanged. Object Storage originals and versions remain retained. A published
module or image alone does not establish runtime acceptance; unsupported
requests fail rather than silently changing the operation.

**0.4.42** reports verified destination reuse accurately in ordinary transfers.
Per-file output says `REUSED` when a copy verifies an identical destination;
a move that removes its source after reuse says `MOVED` with
`(reused destination)`. Progress and saved history retain the verified total
and show how many worker results explicitly reported reuse. Older histories
without that count keep their existing display. Verification, conflict checks
and per-file source removal are unchanged. This addresses
[truthful reuse reporting](https://github.com/EIT-GBI/Scientific-Computing-Infrastructure/issues/123).
When `sbatch` is unavailable, Slurm submission now points to a Slurm login node
or explains when omitting `--detach` allows foreground execution; no job is
submitted. This addresses
[submission guidance from non-Slurm hosts](https://github.com/EIT-GBI/Scientific-Computing-Infrastructure/issues/131).
An unfiltered ordinary move also removes its now-empty named source folder
after discovery and all file work succeed. The caller's current directory,
its ancestors and storage roots are retained. Filtered or packed moves keep
their existing behavior. This addresses
[empty source folders left after ordinary moves](https://github.com/EIT-GBI/Scientific-Computing-Infrastructure/issues/117).

**0.4.41** uses the configured HTTPS broker on both login and compute nodes,
so broker updates can run independently of login sessions. The route uses the
caller's existing Slurm user token. Sites without an HTTPS URL continue using
the local Unix socket; an HTTPS error never switches to that socket. This is
the CLI part of [independent broker delivery](https://github.com/EIT-GBI/gbi-sandpit-cluster/issues/1796);
site broker routing and installation are separate delivery steps.

**0.4.40** keeps transfer and cache monitoring connected through temporary
broker outages. Status reads retry up to three times, after 2, 4 and 8 seconds;
submissions are never retried automatically. Confirmed Prefect runs are polled
by their run UUID, including when reconnecting with
`gbi data status TRANSFER_ID --watch`. A lost submission reply still uses the
original saved request ID. If monitoring remains unavailable, the CLI reports
the last observed state and the exact reconnect command; it does not report
the transfer or load as failed. Cache monitoring loss exits 3 (partial), while
a confirmed failed load still exits 1. Authentication and permission refusals
are reported immediately. These are source changes; site installation and
runtime acceptance are separate.

**0.4.39** shows why a `--prefect` transfer stopped. When the job refuses a
path you cannot read or write, the CLI now prints that path and the reason
instead of only the run's state; `gbi data status` shows the same line.

**0.4.38** lets `--prefect` move data between any path you can read and any
path you can write (Scientific-Computing-Infrastructure#120). The CLI sends the
resolved absolute paths instead of paths relative to your personal roots; the
broker maps the mounts and the transfer job checks permissions as you. Object
Storage to Object Storage is the new `copy-objects` operation. `move` now
follows one table on both routes: Lustre and FSS sources are removed after
verification as before, and so are Object Storage sources moved to Object
Storage. Moving Object Storage data to Lustre or FSS asks `[y/N]` on a
terminal; `--delete-source` answers yes, and without a terminal or the flag
the source is kept with one line saying so. `copy --delete-source` is refused.
Python `data.move(..., delete_source=True)` passes the flag and never asks.
Needs the matching broker and flows; older brokers refuse absolute paths.

**0.4.37** stops restores reading what they just wrote
(gbi-data-platform-tracking#44, pooled single-read restores). A file extracted
from a packed archive is proved by the hash of the bytes handed to the writer
and by its identity afterwards; it is no longer read back twice. A chunked file
is proved by the chunk reader's per-part and whole-stream checks as it streams,
not by a hash of the written file. `archives.restore` takes an optional
`manifest` from an earlier `inspect_archive`, so a caller that has already
verified the archive extracts it in one further read instead of two; the
Prefect restore flows use it. Files that already existed at the destination are
still read to prove they match. Formats, receipts and journals are unchanged.

**0.4.36** lets `--prefect` take the Object Storage root that `gbi data roots`
prints (Scientific-Computing-Infrastructure#112, GBI CLI enhancements). A team
member whose account has no `/mnt/user-data` entry names their member folder,
`/mnt/team/TEAM/members/USER`, and the request goes to the broker with the same
relative path as before; accounts with the entry are unchanged. The broker and
the Prefect route still decide whether that account may transfer. The help and
the guide say "the roots that gbi data roots prints" instead of "personal
storage roots".

**0.4.35** corrects two `gbi data roots` lines (Scientific-Computing-Infrastructure#112, GBI CLI enhancements).
The Object Storage root is always a public path: a `/mnt/user-data` entry that
still points at its own backend is printed as that entry, never as the mount it
resolves to, and gets no "also reachable as" line. The read line lists only the
collections the caller can open: a family prints as `ROOT/*` only when every
collection in it opens, otherwise as the ones that do, so an archive restricted
to one team appears for that team alone. Nothing else changes.

**0.4.34** makes `gbi data roots` explain each storage root and find your team
folders (GBI-Compute-Software-Modules#122). Each root gets one line on what
belongs there and what does not: Lustre scratch, FSS home, Object Storage
archive. The Object Storage root is printed as your member folder in your
team's bucket, with the older `/mnt/user-data` alias named on the next line
when it exists. The team folders you can open under `/mnt/team` and the
read-only collection roots follow. `gbi data usage` now says why the live
Lustre quota can differ from the folder breakdown. Site operators set the
per-root notes (`lustre_note`, `fss_note`, `alluxio_note`), `team_root` and
`readonly_roots` in site.conf, all with defaults, so no existing site.conf
needs a change. Transfers, formats, receipts and journals are unchanged.

**0.4.33** makes the direct route read each byte the minimum number of times
(GBI-Compute-Software-Modules#114). A packed chunked move reads the source once
and the Lustre stage twice: the pack sink hashes what it writes, so the stage is
not scanned or digested after packing, and the chunk writer's final pass over
the stage and every part is off for packed archives, whose proof is the full
readback of the stored archive through the mount before any receipt. Plain
packed moves unlink on a pool of 8. Per-file moves no longer re-read the source
before the receipt (identity is the inode, size and nanosecond mtime, checked
after the readback and before the unlink); progress files are no longer
fsynced, journals and receipts still are. Formats, receipts and journals are
unchanged.

**0.4.17** removes a duplicate full destination archive read from ordinary
archive moves, including resumed cleanup. The complete archive content and
source checks now precede verified receipt publication in the cleanup path;
receipt failure prevents deletion. The final chunk integrity pass checks four
parts concurrently, using four 1 MiB streaming buffers per transfer worker.
Every part and the reconstructed archive still receive full checksum checks.
Saved plans, journals, archive formats and filters remain compatible. This is
source release status; site installation and runtime acceptance are separate.

**0.4.18** reuses hashes from the current in-memory pack manifest during move
cleanup while each source entry's identity, size, mode, mtime and ctime remain
unchanged. Journal-only resumes hash each remaining source payload once per
invocation, then reuse that verified observation before unlink; persisted
journal digests alone are never reused. Unlinking a hardlink forces its
remaining aliases to be rehashed. The bounded maintainer
recovery helper can also reuse exact, source-bound native OCI checksum proof
instead of downloading an already verified archive. Ordinary CLI archive
verification and saved-plan compatibility remain unchanged. Site installation
and runtime acceptance are pending.

**0.4.22** fixes direct single-file restores of `.rclonelink` symlink
representations: GBI preserves the explicit destination filename, while
directory restores continue to remove the representation suffix.

**0.4.24** opens each parent directory once per cleanup pass instead of once
per file: the parallel cleanup verifies and unlinks the single-name entries of
one directory under one descriptor, hardlinked inodes still go as one group,
and the module recipe version now moves with the package version (0.4.23's
recipe had stayed at 0.4.22, which `gbi install` refuses).

**0.4.23** makes archive packing and move cleanup parallel in the codec.
`archives.pack` reads selected files up to 32 MiB ahead of the tar writer on
16 reader threads (`GBI_PACK_READERS`, bounded to 256 MiB in flight), so one
file-system round trip per file no longer serialises an archive; larger files
stream inline; the tar bytes and manifest are identical for any reader count
and every file is still read exactly once with its identity checked before and
after. `cleanup_source` and `resume_cleanup_source` accept `workers`: entries
are verified and unlinked on that many threads, one hardlink group per call,
with the destination check once before the first unlink, every 30 seconds
while unlinking, and once after, instead of before every unlink. The later
names of a hardlinked inode are no longer re-hashed after its first name is
unlinked. `verify_stored_archive` is documented as the caller's destination
proof. Sequential behaviour (`workers=1`, the default) is unchanged; formats,
receipts, journals and filters are compatible.

Site operators can preserve nondefault installation settings with the optional
`GBI_SITE_CONFIG_FILE` input; see the [maintainer reference](docs/maintainer.md)
for its format and environment override behavior.

## Everyday commands

```bash
module load gbi
gbi --version
gbi data roots

gbi data copy SOURCE DESTINATION
gbi data move SOURCE DESTINATION
gbi data move SOURCE DESTINATION --archive
gbi data status TRANSFER_ID --watch
gbi data usage --depth 1 --limit 20
```

Warm Object Storage data as the first step of your job script with
`gbi data cache PATH`. The site's always-on cache service submits the native
Alluxio load, and the command waits for its result. Use `--detach` to return
after submission, `gbi data status PATH` for current whole-path cache state,
or the printed transfer ID to follow the original selection. Historical cache
IDs retain their Prefect status lookup. See the
[cache guide](docs/user-guide.md#load-data-into-the-cache-before-a-job).
This source change does not establish site deployment or runtime acceptance.
Install this CLI before switching the broker to the cache service. Until that
switch, submissions and saved IDs continue to use the existing Prefect route;
the service response selects native path status after the switch.

`usage` prints a compact Lustre/FSS summary followed by the largest folders
from the latest published Lustre inventory. Lustre is your live UID quota; FSS
and the folder report come from the snapshot the site publishes weekly
(Sunday 02:00 UTC). It never starts a filesystem walk. FSS shows
`not published` until your snapshot includes an OCI measurement, and
`(latest lookup failed)` when the weekly lookup failed and last week's value is shown.

Use paths printed by `roots`. For directories, contents go inside the
destination you name. For a single file, name its destination file or an
existing destination directory.

- `copy` keeps the source.
- `move` removes selected filesystem sources after verification. Object
  Storage originals remain intact, including during restore.
- `--archive` packages a finished directory. GBI chooses archive boundaries
  and resumable part sizes. It is available in 0.4.11.
- Without `--archive`, ordinary copy/move keeps directly readable file paths.

An existing different destination is retained and reported as a collision.
Stop writers before moving finished data.

## Archive and restore

With 0.4.11 installed:

```bash
gbi data move /your/lustre/experiment /your/object/experiment --archive
gbi data copy /your/object/experiment /your/lustre/restored-experiment
```

One destination root represents the original layout. GBI packs child
directories, splits further only when metadata limits require it, and uses
verified parts for large archives. A saved partition keeps retries from
creating competing layouts after verified source cleanup.

Restore with an ordinary copy: no packing or chunk-size flags. Containers
expand into their original relative directories. Object Storage retains the
complete originals. A partly populated destination is not a finished backup;
check the terminal summary and receipts.

For a recovery that must run only named units from an existing saved archive
plan, use the expert `--archive-unit SOURCE_RELATIVE` option once per unit:

```bash
gbi data move /your/lustre/experiment /your/object/experiment --archive \
  --archive-unit results/run-a \
  --archive-unit results/run-b \
  --detach --wait
```

Each value must exactly match a source-relative unit in the saved plan. The
request keeps the original plan and destination lock, rejects completed units,
and schedules only the named unfinished units. It cannot create a plan or
change the original selection, destination, or archive layout.

Routine archiving needs neither `--pack-small` nor `--chunk-size`.
Expert overrides remain compatible with scripts and are exposed by
`gbi data copy --help-all`. Normal `--help` shows everyday choices.
The retired `--delete-source` option is rejected.

## Selection and execution

Filters match filenames recursively; exclusions win. Quote globs:

```bash
gbi data move /your/lustre/run /your/object/run --archive \
  --include '*.pt' --include '*.pt.*' --exclude '*.tmp'
```

Unselected files remain. The same filters can select archive members on
restore without changing the Object Storage original.

GBI chooses bounded foreground execution for small plain transfers and Slurm
for larger work. Archive planning runs on an execution node. Ordinary CLI
and Python calls reuse an existing Slurm allocation. Use `--detach` for a
separate job and `--wait` to wait for it.

`--dry-run` previews without payload writes. Ordinary archive previews
describe policy; execution freezes the complete layout on the worker.
Supported Prefect packing previews create managed tracking records only.

Use `--prefect` for a separate managed personal Object Storage transfer,
without opening the Prefect UI. One endpoint must be personal Lustre/FSS
and the other personal Object Storage. Ordinary routing can use any mounted
path your Unix account can access. See the guide for site-version requirements.

After failure, confirm workers stopped and inspect receipts before repeating
the same command. Keep recovery state. `gbi data retry TRANSFER_ID` is only
for a lost Prefect submission reply, not for restarting an already failed flow
or ordinary transfer. An observation timeout does not prove a job stopped.

## Python

Load the module before Python. The SDK calls the installed CLI and waits:

```python
from gbi import data

data.move("/your/lustre/finished-run", "/your/object/finished-run",
          archive=True, include=["*.pt", "*.pt.*"])
data.copy("/your/object/finished-run", "/your/lustre/restored-run")
```

Paths may be strings or `pathlib.Path`. Calls stream progress, return
`subprocess.CompletedProcess` and raise `subprocess.CalledProcessError`
on failure. They never retry implicitly. Use `prefect=True` only for a
separate managed job on a supported personal route.

## Maintainer reference

Installation, site settings and validation are in
[docs/maintainer.md](docs/maintainer.md). Use the existing Lmod module harness:

The David archive supersets are handled only by the incident helper
`gbi_data.david_recovery.recover_completed_superset_unit`. It requires frozen
writers and the exact saved plan hash, verifies the existing chunk layout and
selected source contents, records provenance before guarded cleanup, and
creates a completion result only after destination readback. For recovery from
an interrupted cleanup, `retain_safety_copy=True` reconstructs and verifies a
read-only tar under the user's `.gbi/recovery-payloads/` before cleanup; the
copy remains for independent recovery. The bounded maintainer entrypoint is
`python3 -B /path/to/david_recovery.py`, with `PYTHONPATH` set to the frozen
run's `runtime/` directory. This lets recovery use the exact run-time archive
helpers while loading only this maintained incident helper from a separate,
hash-verified recovery-code directory. Repeated `--unit-sha256` values select
only explicitly named saved units. It is not a general retry or retarget path.

For an exact historical unit with an accepted native checksum proof, the
maintainer may pass the SHA-pinned enriched cleanup binding and a maintained
Storage identity reader to the same helper. `storage_identity_reader()` adapts
the maintained `transfer_core.archive.OciSdkStorage.head()` interface; callers
must construct it through the existing scoped Slurm worker environment, using
its private `PREFECT_OBJECT_CREDENTIAL_FILE`. The helper does not read, create,
copy, or broaden credentials. The adapter performs at most four concurrent
HEADs and requires ETag, version ID, size, and metadata for every pinned object.
This path is eligible only when
the freshly hashed FSS manifest exactly matches the producer's per-file
manifest digest and every native part/control identity still matches before
and after cleanup. It retains the already staged tar as the recovery copy and
keeps the per-file source hash/unlink guards; it does not reread the chunk
archive payload or reconstruct another tar. Ordinary recovery and archive
superset cases retain the existing full readback path.

The optional `expected_request_sha256` argument pins the original saved request
bytes both before and after acquiring the existing archive-plan lock. A
content-addressed cleanup package should carry that request and plan alongside
the native binding, source proof, terminal receipt, producer semantic proof,
and frozen worklist; the worker still checks the live saved plan and all source
and destination guards before unlinking.

Older plans that have no saved run request use `recovery_request_path` and
`recovery_request_sha256` for a separately recorded request under
`.gbi/recovery-requests/`; its source, target and filters must match the frozen
plan. Existing `verified_journal_sha256`, `stage_journal_sha256` and
`chunk_journal_sha256` pins remain mandatory for any retained transfer journals,
and `expected_current_root=[device, inode, mode]` permits an independently
observed NFS mount device change while preserving the original inode and mode
guards. The helper retains those journals and the staged tar byte-for-byte,
rechecks their identities during cleanup, and records their hashes in
provenance.

```bash
GBI_SITE_LUSTRE_ROOT=/site/scratch/users \
GBI_SITE_FSS_ROOT=/site/home/users \
GBI_SITE_BUCKET_ROOT=/site/personal/users \
GBI_SITE_PARTITION=site-partition \
GBI_MODULE_PATH=/site/shared/software make gbi
```

Use `make help` and [AGENTS.md](../AGENTS.md) for harness rules.
Keep test dependencies and caches in the system temporary directory.

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=gbi/src/lib \
  python3 -B -m unittest discover -s gbi/tests -v
```
