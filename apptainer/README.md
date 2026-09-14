# `apptainer` — containers for HPC without root

Provides `apptainer` and `singularity` on nodes where neither is installed. The
GBI compute nodes already carry a system apptainer; the login node does not,
which is what this module is for.

```bash
module load apptainer
apptainer exec docker://alpine:3 /bin/echo hello
```

## Installing it

The install must run **from a compute node**: it unpacks a `.deb` with `tar`,
which shells out to `xz`, and login nodes have no `xz`. No GBI node has `make`,
so use the direct command — see [Deploying where there is no
`make`](../README.md#deploying-where-there-is-no-make):

```bash
cd $GBI_MODULE_PATH/GBI-Compute-Software-Modules
bash -c "source opt/share/env.sh; ./run.sh opt/bin/build.sh \
         -m $GBI_MODULE_PATH opt/bin/render.sh apptainer"
```

The result is used from any node, login included.

## What the node has to provide

The install is rootless, but the kernel has to permit what containers do:

| Requirement | Check |
| --- | --- |
| unprivileged user namespaces | `unshare -Ur true` |
| `/dev/fuse` | `ls -l /dev/fuse` |
| `libseccomp.so.2` | `ldconfig -p \| grep libseccomp.so.2` |

Everything else the helpers need is either vendored with the module or ships
with `dpkg`. Linux `x86_64` only — upstream publishes no macOS build and the
GitHub release carries no `arm64` asset, so `install.sh` fails fast elsewhere
with no source recipe to fall back to.

## `check.sh` — testing without the framework

`apptainer/check.sh` performs the same download-and-unpack the recipe does,
using only bash, curl and tar. It exists because no GBI node can bootstrap the
module farm (`make bootstrap` compiles Lua and Lmod from source, and there is
no compiler), so the recipe's assumptions would otherwise be untestable on the
cluster.

```bash
./apptainer/check.sh fetch    # on a node with xz, i.e. a compute node
./apptainer/check.sh run      # on the node you want to use apptainer from
```

`run` reports unresolved libraries, then `--version`, then attempts a real
container. It reads its pinned version and URLs from `sm-config/settings.toml`,
so it cannot drift from the recipe it is checking. It writes only into `$DEST`
(default `~/apptainer-check`) and installs nothing.

## Why the recipe looks the way it does

Four things in `sm-config/install.sh` are non-obvious. Each was a failure
first.

### The deb is unpacked by hand

No GBI node has `ar`, nor the `rpm2cpio`/`cpio` pair that upstream's
`tools/install-unprivileged.sh` requires, so that installer cannot be used
here. `install.sh` walks the `.deb`'s `ar` container itself — an 8-byte magic,
then 60-byte member headers, payloads padded to even boundaries — with `tail`
and `head`.

Upstream's installer would have added nothing anyway: the deb already bundles
`mksquashfs`, `squashfuse_ll`, `unsquashfs`, `fuse-overlayfs`, `fuse2fs`,
`gocryptfs` and `starter`, so no `squashfs-tools` or `fuse-overlayfs` package
is needed on the host.

### `usr/*` is lifted to the install root

The deb is built `--prefix=/usr --sysconfdir=/etc`, but apptainer relocates by
taking the parent of its own `bin/` as `${prefix}` and looking for
`${prefix}/etc` there — it does not carry that split across a move. Unpacked
verbatim it hunts for `<root>/usr/etc/apptainer/apptainer.conf` and refuses to
start:

```
FATAL: couldn't parse configuration file
       .../tree/usr/etc/apptainer/apptainer.conf
```

So `bin`, `libexec`, `share`, `etc` and `var` are made siblings at the install
root. Upstream's installer performs the same lift, for the same reason.

### Helpers are wrapped, and the modulefile sets no `LD_LIBRARY_PATH`

Two libraries the bundled helpers link against are genuinely optional on a slim
Ubuntu image, and GBI login nodes have neither:

| Library | Needed by |
| --- | --- |
| `libfuse3.so.3` | `squashfuse_ll`, `fuse-overlayfs`, `fuse2fs` |
| `liblzo2.so.2` | `squashfuse_ll`, `mksquashfs` |

Both are vendored into `lib/` from the Ubuntu noble pool, pinned in
`sm-config/settings.toml`. A 404 on those URLs means Ubuntu pruned the version
and it needs bumping.

Reaching them is the subtle part. Setting `LD_LIBRARY_PATH` in the modulefile
does **not** work: apptainer scrubs `LD_*` from the environment before
launching its image drivers, so it never arrives —

```
FATAL: image driver squashfuse_ll instance exited with error: squashfuse_ll:
error while loading shared libraries: liblzo2.so.2
```

— while still shadowing system libraries for every other program in the shell.
Instead each helper in `libexec/apptainer/bin` is a symlink to a generated
`.wrapper` that derives the install root from its own `$0` and exports the path
immediately before `exec`, where nothing strips it again. The real binaries
move to `libexec/apptainer/libexec`. Again, upstream's mechanism.

Note that `LD_*` survives into *build* helpers (`mksquashfs` and friends); only
the runtime image drivers are scrubbed. The wrapper covers both, so the
distinction does not matter in practice.

### `proot` is deleted, so `--fakeroot` is unavailable

Apptainer runs `proot` inside its own build namespace to emulate root
ownership. On GBI nodes that exits 1, and apptainer reports the failure against
`mksquashfs` — the command it asked proot to run — which had actually
succeeded:

```
FATAL: while creating squashfs: .../bin/mksquashfs command failed: exit status 1
```

proot itself is fine. It works from a plain shell as a real binary, through a
wrapper, running a wrapped `mksquashfs`, and over a realistic tree — every
combination including the exact nesting apptainer uses. Only inside apptainer's
build namespace does it fail, which points at `ptrace` being blocked there.
That is a node policy question, not something the recipe can fix.

Without proot, apptainer falls back to building without ownership emulation,
and that path works end to end. Deleting proot selects the fallback
deterministically rather than leaving it contingent on whether
`libprotobuf-c.so.1` happens to be installed — vendoring that library is what
switched the broken path on in the first place.

**Do not "fix" this by restoring proot.** Nothing is lost in practice:
`--fakeroot` also needs an `/etc/subuid` entry, which GBI users do not have, so
the subuid route cannot work either. Pulling, running and building ordinary
images are unaffected.

## Notes

- No `MODE=build` recipe. Upstream's unprivileged build ships as a deb that
  already bundles its helpers, so there is nothing a source build would add.
- `--fakeroot` is unavailable (above).
- Loading this module on a compute node shadows the system apptainer. That is
  usually wanted — one version everywhere — but it is a behaviour change for
  anyone relying on the system one.
- Images are cached in `~/.apptainer/cache`; set `APPTAINER_CACHEDIR` somewhere
  roomier before pulling large images.
