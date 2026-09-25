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
