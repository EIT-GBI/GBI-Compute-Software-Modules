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

**0.4.17** removes a duplicate full destination archive read from ordinary
archive moves, including resumed cleanup. The complete archive content and
source checks now precede verified receipt publication in the cleanup path;
receipt failure prevents deletion. The final chunk integrity pass checks four
parts concurrently, using four 1 MiB streaming buffers per transfer worker.
Every part and the reconstructed archive still receive full checksum checks.
Saved plans, journals, archive formats and filters remain compatible. This is
source release status; site installation and runtime acceptance are separate.

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
