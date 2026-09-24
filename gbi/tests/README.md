# Transfer benchmarks

Linux is the operational target. Workstation tests validate local behavior;
they do not qualify FSS, Lustre, Alluxio, cross-node locks or the deployed
Prefect broker. Use the cluster checks below to validate an installed release
on its intended routes.

`cluster_sdk_test.py --prefect` checks the installed Python SDK from an existing
Slurm allocation. Load `gbi` first and keep `cluster_selftest.py` beside the
helper. It creates two fresh checkpoints, moves them through the authenticated
HTTPS broker, then restores them to the same Lustre path and independently
compares hashes. An unselected file remains throughout. The helper retains its
fixtures and an `acceptance.json`; join the printed Prefect run IDs with their
receipts when recording operational acceptance. Without `--prefect`, the helper
checks ordinary SDK packing and restore within the current allocation.

On macOS, regular files select the existing pinned native reader before the
transfer, avoiding rclone's intermittent `/dev/fd` reopen failure recorded in
[the macOS descriptor bug](https://github.com/EIT-GBI/GBI-Compute-Software-Modules/issues/28).
The native path still hashes the pinned descriptor, independently reads back the
destination, and publishes the receipt before an optional source deletion.
Linux retains the pinned-rclone path for bulk files. The direct rclone
`test_fd_handoff.py` diagnostic is therefore skipped on Darwin and remains
covered on Linux; it is not a fallback or a pathname retry.

`cluster_benchmark.py` compares the maintained native and pinned rclone source
readers without changing the installed module or its configuration. Both paths
perform independent destination readback and publish verified receipts. Four
file workers remain the limit. Fixtures and reports are retained for review;
the helper never removes existing user data.

Inside an already allocated Slurm CPU job, as your own user:

```sh
module load gbi
python3 cluster_benchmark.py --site-conf "$GBI_DATA_SITE_CONF" \
  --source-kind lustre --target-kind alluxio \
  --rclone "$(command -v rclone)" --repetitions 1 --large-bytes 16777216
```

Choose `lustre`, `fss` or `alluxio` for each route. Real-route mode preserves the
configured roots and actual filesystem behavior. Each invocation creates new
token directories for its fixture sources, destinations and Lustre report. The
CLI also uses its normal user-owned `.gbi` scratch and retained Alluxio history.
The helper supports both foreground history bundles and the separate records
published from a Slurm allocation. It never submits a Slurm job itself.

The bounded cases are 40 files of 4 KiB; 36 files of 4 KiB plus four larger
files; and four larger files. Larger files default to 16 MiB each. Reports
include end-to-end elapsed time through receipt publication, CPU, process
lifetime peak RSS (explicit units), errors, actual receipt reader counts, and
phase timings. A separate single-file engine profile counts Python-visible
filesystem calls; those counts exclude child-process work and are **not**
filesystem RPC counts. Resource block counters can be zero on cached local
filesystems and should not be interpreted as zero storage traffic.

`--readers default` measures the unchanged site selection policy; `--readers
rclone native` forces the two comparison readers on Linux through temporary
fixture configuration. On macOS, use `--readers default` or `--readers native`;
both use native regular-file reads, and the profiler reports only that reader.
For a baseline/candidate comparison, point `PYTHONPATH` at each
source snapshot and repeat the same arguments. The report records source
hashes and rejects conclusions from changing code (`code_unchanged_during_run`).

For workstation-only synthetic profiling, omit `--site-conf` and supply three
existing directories under your workspace:

```sh
PYTHONPATH=gbi/src/lib python3 gbi/tests/cluster_benchmark.py \
  --source-root "$WORKSPACE/fixtures" --target-root "$WORKSPACE/fixtures" \
  --state-root "$WORKSPACE/reports" --rclone /path/to/pinned/rclone
```

Synthetic mode refuses to run inside Slurm because simulated filesystem labels
cannot qualify a real storage route.

On a local APFS fixture using GBI 0.3.6 and rclone 1.75.1, three-repeat median
end-to-end times were 1.066 s (rclone) versus 0.767 s (native) for 40 × 4 KiB;
1.228 s versus 0.811 s for the mixed case; and 0.301 s versus 0.214 s for four
16 MiB files. All cases had verified receipts and zero errors. These numbers
show local process-startup overhead; they do **not** establish Lustre, FSS or
Alluxio throughput, packing thresholds, or permission to increase concurrency.

Early bounded Linux small-file comparisons showed about 2% lower wall time and
47% lower CPU with native reading, at the unchanged four-worker limit. These
are preliminary measurements, not qualified policy defaults. The per-file
native threshold on Linux remains 8 MiB; larger Linux files use pinned rclone.

One bounded Linux CPU allocation on the configured Lustre→Alluxio route
completed all six comparisons with zero failures. Rclone/native elapsed times
were 28.58/27.92 s (small), 29.09/28.48 s (mixed), and 9.06/7.78 s (large).
For small files native reduced CPU from 5.21 to 2.78 s, but shared-storage
verification and metadata dominated elapsed time. This is one observation,
not a throughput guarantee or a reason to raise concurrency.

`packing_benchmark.py` measures six cases: 8 and 40 files at each of 4 KiB,
64 KiB and 1 MiB. It compares ordinary loose copies with `--pack tar`, then
restores each archive to both Lustre and FSS and independently hashes every
restored file. All operations retain their sources. Run in one existing CPU
allocation with two CPUs, 4 GiB memory, and the unchanged four-worker limit:

```sh
python3 packing_benchmark.py --site-conf "$GBI_DATA_SITE_CONF" --source-kind lustre
```

Use `--source-kind fss` for the other native source route. The helper preserves
real site roots, records immutable source-code hashes, receipts and resource
counters, and retains its unique fixture directories. Its temporary site
configuration includes explicit experimental packing bounds; these are not
recommended production thresholds. The measured tar comparison uses explicit
`--pack tar`, so it does not depend on automatic small-file policy selection.
No helper submits jobs or removes fixtures.

One retained Linux Lustre→Alluxio run measured the following end-to-end
copy times, including verified history publication. Every archive also passed
independent restore checks to both Lustre and FSS:

| Files | File size | Loose | Tar |
| ---: | ---: | ---: | ---: |
| 8 | 4 KiB | 8.04 s | 5.50 s |
| 40 | 4 KiB | 17.90 s | 5.47 s |
| 8 | 64 KiB | 7.53 s | 5.24 s |
| 40 | 64 KiB | 17.94 s | 5.41 s |
| 8 | 1 MiB | 7.38 s | 5.52 s |
| 40 | 1 MiB | 18.22 s | 6.84 s |

These single repetitions support a conservative opt-in packing policy; they
do not establish universal thresholds. Shared-route timings varied between
runs. Uncompressed tar reduces file-count overhead but adds archive metadata:
40 × 4 KiB occupied 235,520 archive bytes for 163,840 payload bytes.

For a smaller installed-version check, run `cluster_formats_test.py --site-conf
"$GBI_DATA_SITE_CONF" --case selective` inside an existing allocation. Keep
`cluster_benchmark.py` and `packing_benchmark.py` alongside the helper. Load
the desired `gbi` module and set `PYTHONPATH` to that installation's `lib`
directory when invoking the Python helper directly; the `gbi` wrapper normally
sets this itself. The report records the actual imported package path,
version and source hashes. This case checks a no-write dry run, selective
packing with an excluded file and a loose larger file, then mixed restore to
FSS with independent parity. All source fixtures are retained.
