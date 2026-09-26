# GBI maintainer reference

This page preserves the installation and site-configuration reference for the
GBI module. User commands are in [the user guide](user-guide.md); this page is
for software maintainers and site operators.

## Installation

The existing module harness installs the in-tree source. Ordinary transfers do
not need an image or Prefect change. The `--prefect` route additionally needs
the maintained login broker and site catalogue configuration. Python 3.9 or
newer must be available on every selected node. The module loads the pinned
rclone dependency automatically. If installed at the site, the optional Lmod
`lfs/2.16.1` module lets `gbi data usage` read live Lustre quotas.

```bash
make rclone
GBI_SITE_LUSTRE_ROOT=/site/scratch/users \
GBI_SITE_FSS_ROOT=/site/home/users \
GBI_SITE_BUCKET_ROOT=/site/personal/users \
GBI_SITE_PARTITION=site-partition \
GBI_MODULE_PATH=/site/shared/software make gbi
```

For a shared cluster installation, run the recipe as a software maintainer from
the reviewed release checkout. Software goes under `gbi/<version>` and the
modulefile under `modules/gbi/<version>.lua` in the selected module path. Use
the same install root as the cluster's existing rclone module. When its
`modules` directory is already in the shared Lmod environment, users only need
`module load gbi`; no per-user installation, container rebuild or login-node
restart is required.

After installation, check the module and roots from a normal user shell:

```bash
module show gbi
gbi --version
gbi data roots
```

The recipe reads the version from `gbi/VERSION`. Site roots and other settings
are supplied through `GBI_SITE_*` variables at installation, never from public
source code. The configured parent roots have the authenticated username
appended by the module's site configuration.

## Site settings

Supported keys and defaults are defined in
[`src/lib/gbi_data/storage.py`](../src/lib/gbi_data/storage.py) and rendered
into the installed site configuration. The principal settings are:

| Setting | Purpose | Default |
| --- | --- | --- |
| `GBI_SITE_LUSTRE_ROOT` | Parent of each user's Lustre root | site supplied |
| `GBI_SITE_FSS_ROOT` | Parent of each user's FSS root | site supplied |
| `GBI_SITE_BUCKET_ROOT` | Parent of each user's Object Storage mount | site supplied |
| `GBI_SITE_PARTITION` | Slurm partition for queued work | site supplied |
| `GBI_SITE_PREFECT_URL` | Optional HTTPS broker endpoint | empty |
| `GBI_SITE_JOBS` | Concurrent file workers | 4 |
| `GBI_SITE_CPUS` | CPUs requested for Slurm work | 2 |
| `GBI_SITE_MEM` | Memory requested for Slurm work | 4 GiB |
| `GBI_SITE_FILE_TIMEOUT` | Per-file worker deadline | 1 day |
| `GBI_SITE_VERIFY_SETTLE_SECONDS` | Alluxio readback settling deadline | 20 minutes |
| `GBI_SITE_MOUNT_TIMEOUT` | Startup and mount wait bound | 120 seconds |
| `GBI_SITE_DISCOVERY_TIMEOUT` | Discovery progress wait bound | 120 seconds |
| `GBI_SITE_HISTORY_TIMEOUT` | History publication wait bound | 30 minutes |
| `GBI_SITE_PACK_SMALL_BYTES` | Maximum selected size per small archive | 64 KiB/file |
| `GBI_SITE_PACK_MIN_FILES` | Minimum selected files per small archive | 40 |
| `GBI_SITE_PACK_MAX_BYTES` | Maximum selected payload per small archive | 1 GiB |

The `pack_small` values are conservative opt-in defaults. A site may configure
them, so user documentation should describe them as defaults rather than
immutable limits. The archive format still enforces its separate 64 MiB
metadata and observation limits.

`GBI_SITE_PREFECT_URL` must be an HTTPS URL without credentials, query
parameters or fragments. The CLI tries the local broker socket first and uses
the HTTPS endpoint only when that socket is unavailable. HTTPS uses the caller's
short-lived Slurm token in memory; no token or cloud credential belongs in a
request file or public documentation.

Storage roots are conveniences and storage-type hints, not a general ordinary
transfer allowlist. The ordinary route classifies mounted filesystem paths and
uses normal Unix permissions. The Prefect route is stricter: it accepts only
personal Lustre, FSS and Object Storage roots because the broker binds the
request to the user's approved route.

## Validation

The PDF is generated from the user guide, so examples have one maintained
source. After changing the guide or release version, rebuild it and visually
check the pages before committing the Markdown and PDF together:

```bash
uv run gbi/docs/build_pdf.py --date YYYY-MM-DD
```

The builder declares its Python dependencies and writes `user-guide.pdf`
beside `user-guide.md`. Use the release date in place of `YYYY-MM-DD`.

Run the ordinary unit suite and shell checks from the reviewed checkout. Keep
package caches and temporary environments in the system temporary directory.

```bash
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPYCACHEPREFIX="$TMPDIR/gbi-pycache" \
PYTHONPATH=gbi/src/lib python3 -B -m unittest discover -s gbi/tests -v
shellcheck gbi/src/bin/gbi gbi/sm-config/install.sh
```

The maintained cluster tests create fresh directories under the invoking user's
configured roots. `cluster_selftest.py` checks the six filesystem directions,
`cluster_retry_test.py` checks Alluxio partial-write recovery, and
`cluster_adaptive_test.py` checks foreground and Slurm route selection. Run
these only through the maintained cluster test procedure and with fresh fixture
paths.

See [SPEC.md](../SPEC.md), [ARCHITECTURE.md](../ARCHITECTURE.md) and the
repository [AGENTS.md](../../AGENTS.md) for implementation boundaries and
harness rules.
### Python archive codec package

`pyproject.toml` builds the existing `gbi_data` archive implementation as the
dependency-free `gbi-archive-codec` wheel. The package boundary reuses
`gbi_data.archives` and its selection helper without copying codec source; the
candidate Prefect Object Storage backend consumes the same codec package; its
publication, image pin and runtime acceptance are separate from local tests.
It contains no
installed CLI executable or `gbi` SDK; researchers should load the normal module.
From the repository root, test the isolated wheel with an interpreter that has
`pip`, `setuptools>=61` and `wheel` installed:

```bash
python -B -m unittest discover -s gbi/tests -p test_archive_package.py -v
```

The test builds and installs only into a temporary directory, checks that the
wheel contains the unchanged codec source, and exercises tar and gzip with
include/exclude filters. It skips when offline build tooling is unavailable.

Codec callers may explicitly set `selection_mode="relative"` on
`archives.check_metadata_budget`, `archives.pack`, `archives.restore` and
`packing.plan`. This matches source-relative POSIX path segments: `*.pt` selects
only root files, `nested/*.pt` selects one level and `**/*.pt` selects any depth.
It follows the Prefect transfer core's segment matching; include patterns form
a union and exclusions win. Patterns do not implicitly select a directory's
descendants. Selected empty directories and ancestors needed by selected entries
are preserved. Restore patterns are relative to the archive's stored root.
Use the same mode and filters for preflight and packing. For a selected subtree,
`check_metadata_budget` and `pack` accept `selection_prefix="parent/subtree"`
in relative mode: patterns still match original-root paths, while archived
member names remain subtree-relative. The prefix must be a canonical relative
POSIX path, or empty. The default remains `basename`,
including for all ordinary CLI and SDK calls. This codec option does not itself
enable portable Prefect transfers or add a public CLI flag.

Preflight also returns `selected_bytes`, counting selected file, symlink and
hardlink logical sizes as an upper bound on payload bytes. Archive headers,
metadata and compression overhead still need a separate output bound.
`archives.cleanup_source(..., on_remove=callback)` can record each successfully
unlinked non-directory entry for partial-failure accounting; the callback must
not mutate that entry. Full readback and source-stability checks still apply.

`chunks.parse_manifest(encoded, completion=None, *, complete=True)` validates
raw manifest and completion-marker JSON bytes without filesystem or payload I/O.
It returns the manifest with computed `state` (`complete` or `incomplete`) and
uses the existing version-1 format, checksum binding, part-count and byte limits.
The default requires a matching completion marker. `complete=False` permits a
missing or truncated JSON marker for resume; a parsed but mismatched marker or
an oversized marker still fails. `chunks.read_manifest` delegates to this parser
and additionally checks filesystem directories and no-follow regular files.
Transport adapters must bound reads themselves and independently verify payloads;
parsing metadata alone does not prove a transfer complete or authorize cleanup.
The internal `_ChunkReader(store, manifest, *, open_part=None)` accepts an
optional transport callback receiving each part record and returning a binary
reader supporting `readinto` and `close`. It retains sequential per-part size,
SHA-256 and full-stream checks; callers must drain the reader to verify the full
stream and separately pin/recheck remote metadata. The filesystem default is
unchanged.

The 0.4.9 candidate broker forwards positive integer `chunk_size` only for
archives; default `None` is omitted. For an un-packed direct regular file, the
CLI verifies personal-root metadata without symlink traversal and sends
`source_is_file=True`; otherwise it omits the field. The backend validates the
same shape and grants the exact generated `.gbi-chunks` store, not its parent.
Whole-pack targets also gain that suffix; directory targets retain their prefix
and encode selected members independently. Restore format detection is automatic.
Typed deployment schema checks and replay identity include both fields. Never
drop a rejected option to make an older deployment accept the request.

`formats._owned_remove(path, identity, root)` and `_owned_rmdir` use a shared
descriptor-relative no-follow ancestor walk and inode/type checks before unlink
or rmdir. Regular files must have one link. Capture `_identity(path)` before
payload or other cleanup and retain it; recapturing afterwards could authorize
a replacement. The ordinary worker binds stage/pending journals and stage
directories this way. These helpers do not authorize Object Storage deletion.
