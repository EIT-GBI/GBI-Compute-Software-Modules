# GBI data CLI specification — 0.4.0 candidate

This describes the candidate implementation contract. It does not claim that
0.4.0 or its optional infrastructure broker is released, deployed or accepted
on a production storage route. [Architecture](ARCHITECTURE.md) maps the contract
to the current modules; [README](README.md) is the command guide.

## Commands and identity

`gbi data copy SOURCE DESTINATION` retains sources.
`gbi data move SOURCE DESTINATION` deletes only verified filesystem sources;
Object Storage originals remain unless `--delete-source` is explicit.
Existing differing destinations are preserved. A directory's contents go into
the named destination. Unix permissions govern ordinary shared/instrument
paths; personal roots are defaults for discovery and state.

Foreground and ordinary Slurm work run with the invoking identity. The user
does not choose an engine, cloud credential or execution partition.
`--prefect` explicitly selects a managed personal migration through a
kernel-UID-bound broker. It is mutually exclusive with `--detach` and
`--local`; ordinary automatic placement never escalates to Prefect by size.

## Ordinary placement and source readers

Selections up to 8 GiB run in the current shell by default. Larger or
unbounded selections use Slurm; unknown decoded archive size also selects
Slurm. Metadata probing is bounded by five seconds and 100,000 visited entries.
Existing allocations are reused. `--detach` requests a new background job,
and explicit `--wait` wins even when combined with `--detach`.

The configured default is four workers, two CPUs and 4 GiB. Native reading
covers tiny requests and individual ordinary regular files up to 8 MiB after
source pinning. Larger requests retain the pinned-rclone dependency check;
larger regular files use rclone. This is a per-file reader decision, not a
concurrency change. Both paths retain identical independent verification,
receipt and deletion rules.

Code/configuration snapshots and a source digest bind submitted jobs to their
request. Later module updates do not change queued execution.

## Selection and encoding

Repeatable `--include` and `--exclude` use case-sensitive basenames.
Exclusions win, including for explicit files and links. Directory names do not
prune descendants. Ordinary foreground selection is a bounded metadata
snapshot; selected files are rechecked before copying. Bulk discovery streams
independently of workers so a slow lookup does not hide completed receipts.

Format work uses logical units rather than descending into encoded payloads:

- `--pack tar|gzip` requires a directory and an exact generated archive
  filename. A versioned manifest binds member names, types, metadata and hashes.
- `--pack-small` requires a complete bounded plan of non-overlapping
  qualifying subtrees. Dry-run explains selected layouts; unselected files
  stay loose. An incomplete plan cannot authorize packing.
- `--chunk-size SIZE` accepts human-readable binary sizes and writes ordered,
  individually verified parts plus a full-payload hash. A manifest-bound
  completion marker is required for restoration.
- Packing plus chunks uses bounded, capacity-checked Lustre staging. Strict
  source/manifest evidence governs reuse. Plain packing does not stage an
  extra archive payload.

Source content markers authorize automatic restoration; a filename alone
does not. Complete container verification includes unselected members.
Restoration goes to Lustre/FSS, with member filtering and no traversal of
symbolic-link targets. Archives preserve supported mode/time/link/hardlink
relationships; ownership, ACLs and xattrs are outside the format contract.
Existing directory permissions remain intact. Partial filtered restores keep
the complete encoded source.

## Verification, source removal and retries

Ordinary move is stream/hash → independently reopen/hash destination →
recheck source content/identity → fsynced verification receipt → guarded
unlink. A reader's successful exit alone never authorizes deletion.
Alluxio output uses exclusive final-name creation, without large-object rename
or truncation. Accepted write-through persistence and cross-node lock semantics
are deployment prerequisites; mount readback is not a direct Object Storage GET.

Format workers verify containers/reconstructed data, retain their destination
locks and write receipts before cleanup. Selected sources and destinations are
rechecked across removal; unknown additions are preserved. Chunk source removal
checks the restored destination before each owned metadata/part unlink.
Concurrent applications must not modify sources or incomplete destinations.

Ordinary owned-partial retry requires the recorded source and destination
identity. Chunk resume additionally binds the complete source/format manifest;
verified parts are read back before reuse. Unchanged source evidence is
required for chunked archive stages and partial raw chunk restoration.

An owned incomplete **unchunked archive** may restart from current source,
rather than resume old bytes. It must produce a fresh manifest and pass full
archive readback/source revalidation before deletion. A foreign output is never
adopted or overwritten by this exception.

## State, deadlines and progress

Lustre holds active requests, journals, progress and logs. Ordinary completed
records are published immutably to Alluxio and independently read back before
scratch cleanup. Publication failure retains scratch and fails the command;
failure to remove scratch after verified publication is a cleanup warning.
No accumulated FSS log store is introduced.

The outer watchdog uses a dedicated process/event-pipe boundary for initial
site/path resolution, state I/O and history. Defaults are a 120-second bootstrap
before configuration is readable, 120-second mount/state waiting, 120 seconds
waiting for discovery entries while capacity is available, and 30 minutes
without finished history-file work. Per-file and Alluxio settling deadlines
remain separate. Site settings control the configured values.

Timeout means bounded command failure, not proof that kernel I/O stopped.
Report phase/path, last observed verified/deleted counts, retained records and
unresolved workers. Preserve lock ownership; do not start a competing writer,
substitute a local directory for an unavailable mount, or change engines.
Writing final failure state may itself be impossible on an unavailable mount.

Interactive and timestamped newline output share progress states. Logs emit
periodic updates while responsive, distinguish active from verified bytes,
and cover discovery, copy/close/readback, packing/staging/chunks/extraction and
history. Unknown totals have no invented percentage. Foreground Ctrl-C stops
work; Slurm/Prefect viewer Ctrl-C detaches. Scheduler completion without a final
ordinary-transfer summary is not accepted as data completion.

## Explicit Prefect contract

The broker selects only approved personal Lustre/FSS archive/stage routes.
The peer's Unix UID binds the route; user-supplied identities, buckets,
deployments and credentials are unavailable. Broker API authentication and
flow Object Storage credentials remain service-side.

Maintained flows use direct Object Storage SDK payload transfer, with existing
Slurm execution and Alluxio ownership/presentation checks. This source-verified
distinction does not establish deployment or performance benefit.
FSS archives and all restores require matching relative paths; Lustre archives
may remap the relative destination. Each include pattern must match at least
one file. Exclusions, packing, chunks, shared paths and `--delete-source` are
not supported on this route.

The exact request/UUID is saved on Lustre before submission. Status can query
by request ID after a lost reply; `gbi data retry TRANSFER_ID` resends that
unchanged request. Repeating the original copy/move creates a different
request and is not lost-reply recovery. Client records remain on scratch;
the maintained flows own durable receipts. Report Prefect state explicitly,
without manufacturing CLI aggregate byte/verification evidence.

## Validation boundary

Linux is the operational target. Local ordinary filesystems and synthetic
Slurm adapters test interfaces, not real FSS/Lustre/Alluxio semantics.
Encoded-format corruption/cleanup/retry tests and broker schema tests do not
substitute for deployed user-path acceptance. Tests use newly created fixtures
under the invoking user's roots, without unrelated cleanup or IAM expansion.

Preliminary small-file measurements (about 2% wall and 47% CPU reduction) do
not qualify packing thresholds, higher concurrency or throughput guarantees.
The intermittent macOS rclone descriptor failure remains tracked separately;
passing bounded reproductions do not resolve it. No external data catalogue,
automatic Prefect size tier, cloud credentials for users or new execution
partition is part of this candidate.
