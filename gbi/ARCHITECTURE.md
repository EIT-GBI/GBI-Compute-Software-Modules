# GBI data CLI architecture

This describes the **0.4.0 candidate source**, not an installed release or live
acceptance. CLI code, module installation, the optional broker deployment and
each storage route require separate evidence. Site identities and credentials
are omitted from this public document.

## Data paths and identity

| Route | Payload and authority |
| --- | --- |
| Ordinary `copy` / `move` | Native I/O or rclone reads a pinned source descriptor; Python writes and independently verifies mounted filesystem paths. Alluxio persists its mounted writes to Object Storage. FSS ↔ Lustre payloads travel directly between filesystems. |
| Explicit `--prefect` | A local broker submits an approved personal archive/stage flow. The maintained worker transfers payload directly with the Object Storage SDK, retaining existing Slurm execution, route authorization, verification and Alluxio ownership/presentation checks. |

Prefect is never selected by an automatic size threshold. The CLI has no cloud
credentials and does not acquire them for this route. The [adapter](src/lib/gbi_data/prefect.py)
sends a versioned request over a local Unix socket. The infrastructure-owned
broker obtains the caller UID from the kernel, binds it to an enabled personal
route and selects approved deployments. Users cannot choose another account,
bucket, deployment or execution identity. API authentication stays with the
broker; Object Storage credentials stay with the maintained flow services.

The direct SDK path was checked against the maintained transfer worker source
on 23 September 2026. It bypasses Alluxio payload transfer, not all presentation
dependencies. Source review does not prove broker deployment, route acceptance
or a performance advantage.

## Ordinary execution

[Site settings](src/lib/gbi_data/storage.py) default to foreground execution for
selections up to 8 GiB. Larger selections, unknown decoded archive size or a
probe exceeding five seconds / 100,000 visited entries select Slurm. Existing
allocations are reused; `--detach` requests a new job. Defaults remain two CPUs,
4 GiB, 24 hours and four workers per invocation. No concurrency increase or new
partition is introduced.

Tiny selections (up to 8 MiB and 32 files) need no rclone. Larger requests still
resolve the pinned dependency, but each ordinary regular file up to 8 MiB uses
native reading after its source descriptor is pinned and checked. Larger files
use rclone. Placement and reader selection remain independent.

Rclone reads the inherited source descriptor through a one-entry
`--files-from-raw` list and `--no-traverse`, with an empty configuration.
Python creates exclusive final-name outputs. Alluxio files are not truncated
or renamed over existing objects. Mount readback can use cache; it is not an
independent SDK GET. Accepted write-through persistence is a site prerequisite.

The coordinator snapshots code and site configuration onto Lustre so queued
jobs retain their submitted version. Unix permissions govern ordinary shared
and instrument paths; personal roots are discovery/state defaults, not an
ordinary-transfer allowlist.

## Selection and portable formats

[Selection](src/lib/gbi_data/selection.py) shares case-sensitive basename
matching between probes and streamed work. Exclusions override includes,
including for explicit files and symbolic links; directory names do not prune
their descendants. Excluded sources remain untouched.

[Logical selection](src/lib/gbi_data/format_selection.py) presents each packed
tree, recognized archive or complete chunk store as one worker unit. It does
not copy internal chunk parts as ordinary user files. Content markers and
validated manifests authorize decoding; suffixes alone do not.

- `--pack tar|gzip` streams a directory to an exact `.gbi.tar` or
  `.gbi.tar.gz` destination with a versioned member manifest.
- `--pack-small` uses a bounded read-only planner for non-overlapping
  subtrees, leaving other files loose. Dry-run shows the proposed layout.
  Configurable policy defaults are implementation choices, not qualified
  optimal throughput thresholds.
- `--chunk-size SIZE` produces a `.gbi-chunks` store with ordered part
  hashes, a full-payload hash and a manifest-bound completion marker.
- Packing with chunks uses a bounded, capacity-checked Lustre archive stage.
  Source observations/content bind stage reuse; verified output precedes
  stage cleanup. Plain packing streams without a payload staging copy.

Restoration verifies the complete container, including excluded members, and
extracts only supported entries to Lustre/FSS. Unsafe paths/parents, special
members and conflicting destinations are refused. Archives represent modes,
timestamps, symbolic links and hardlinks; ownership, ACLs and xattrs are not
restored. Loose Alluxio files retain their existing metadata limits. Filtered
restoration retains the complete source container.

## Verification and recovery

Ordinary movement remains source stream → independent destination SHA-256
readback → source recheck when deleting → durable receipt → guarded unlink.
`copy` retains sources. `move` retains Object Storage originals unless
`--delete-source` is explicit; that option is unavailable with `--prefect`.
Sources must remain quiescent; this is not an application write lock.

[Format workers](src/lib/gbi_data/formats.py) retain destination locks and
write verification receipts before cleanup. Archive cleanup checks source
content and destination stability before each selected unlink. Chunk
manifests/parts and restored targets remain guarded across cleanup; unknown
additions are never recursively removed.

Ordinary partial files and chunk resume require unchanged source evidence.
A journal-owned incomplete **unchunked archive** may instead be rebuilt from
current source: fresh packing, manifest, full readback and source revalidation
must all precede cleanup. It does not reuse old archive bytes. Chunked archive
stages and partial raw chunk restores remain bound to source/manifest evidence.

Ordinary history is published as immutable Alluxio records and read back before
scratch cleanup. Publication failure retains scratch and fails the command.
Failed scratch removal after verified publication is a cleanup warning, not a
data-verification failure. FSS holds installed code/configuration, not accumulated
transfer logs.

Prefect requests save their exact parameters and UUID on Lustre before sending.
Status can recover by request ID after a lost reply; `retry` resends that
unchanged request. Client records currently stay on scratch. The flows own
durable migration receipts; the CLI reports Prefect state without inventing
aggregate verification counters.

## Deadlines and progress

[The watchdog](src/lib/gbi_data/deadlines.py) runs outside mount-dependent
operations. Its serialized event pipe carries phase/path/counters and worker
lifetimes. Configurable state, discovery and history deadlines bound waiting.
Only finished history-file work refreshes the history deadline; periodic
heartbeats do not hide blocked I/O. Slurm submissions discard inherited
watchdog environment and establish fresh supervision on the execution node.

A deadline reports last observed counters, retained records and worker PIDs
needing terminal-state checks. It does not prove kernel I/O stopped. Locks are
not bypassed, missing mounts are not replaced with local directories, and
engines are not silently changed. An unavailable state mount can prevent a
final failure record; retain the original output or Slurm log.

Interactive and timestamped newline logs distinguish discovery, copying,
close/readback, packing/staging/parts/extraction and history publication.
Active bytes are separate from verified bytes. Ctrl-C stops foreground work
but detaches a submitted-job viewer. `--detach --wait` submits and waits.
Same-host Linux status uses process identity to detect stopped/reused PIDs.

## Source map and evidence limits

| Modules | Responsibility |
| --- | --- |
| [storage.py](src/lib/gbi_data/storage.py), [selection.py](src/lib/gbi_data/selection.py) | Site/path policy, filters and bounded selection |
| [packing.py](src/lib/gbi_data/packing.py), [format_selection.py](src/lib/gbi_data/format_selection.py) | Packing plans and logical units |
| [transfer.py](src/lib/gbi_data/transfer.py), [formats.py](src/lib/gbi_data/formats.py) | Supervised work, receipts and cleanup |
| [archives.py](src/lib/gbi_data/archives.py), [chunks.py](src/lib/gbi_data/chunks.py) | Portable formats, verification and reconstruction |
| [cli.py](src/lib/gbi_data/cli.py), [jobs.py](src/lib/gbi_data/jobs.py), [deadlines.py](src/lib/gbi_data/deadlines.py) | Submission, progress, watchdog and ordinary history |
| [prefect.py](src/lib/gbi_data/prefect.py) | Broker adapter and exact-request recovery |

Linux is the operational target. Local tests do not qualify real FSS, Lustre or
Alluxio semantics. Preliminary bounded Linux small-file measurements showed
about 2% lower wall time and 47% lower CPU with native reading, at unchanged
concurrency. These are not throughput guarantees or qualified policy defaults.
See [test guidance](tests/README.md) for the unresolved macOS rclone limitation.
No candidate release or broker deployment is established by this document.
