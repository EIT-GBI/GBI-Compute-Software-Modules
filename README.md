# Software and Environment Modules for GBI's Scientific Compute Systems

A self-contained framework for standing up an [Lmod](https://lmod.readthedocs.io)
environment — and populating it with CLI utilities — using
[simple-modules](https://gitlab.blaschke.science/nersc/simple-modules) and
[simple-templates](https://gitlab.blaschke.science/nersc/simple-templates).

Sometimes Spack is too much of a faff — and how do you use Spack without a
local Python in the first place? This repo takes the other route: a small set
of Bash and Lua pieces that

1. bootstrap a private Lua (with `luaposix` and friends) and a private Lmod into
   `opt/`, then
2. install software and generate the matching `.lua` modulefiles via
   `simple-modules`, driven by declarative TOML recipes.

The module framework needs no root, system package manager or Python.
Everything is self-contained under this checkout (or wherever you point
`GBI_MODULE_PATH`). Individual tools can require Python: `rust MODE=build`
uses `x.py`, and the `gbi` data CLI requires Python 3.9+ on execution nodes.

## Quick start

```bash
make update                     # fetch simple-modules.ex + simple-templates.ex
make bootstrap                  # build Lua + Lmod into opt/, write opt/share/env.*
source opt/share/env.sh         # puts opt/bin on PATH, inits Lmod, `ml use` the modules dir
make all                        # install every module in TARGETS
```

>[!NOTE]
> GBI sets the `GBI_MODULE_PATH` environment variable in all shells, this tells
> that makefile where to install modules and software to.

Then use it like any other Lmod deployment:

```bash
module avail
module load rust eza bat
```

`make` on its own prints the built-in help, which is the authoritative list of
targets and environment variables.

`bootstrap` auto-detects OS (linux/macos/freebsd), arch (x86_64/arm64) and libc
(glibc-latest/glibc-legacy/musl) and pulls the matching
[lua-regolith](https://github.com/JBlaschke/lua-regolith) release. It then
configures and installs the vendored Lmod against that Lua, and writes shell
init files for bash, fish and Nushell:

| File | Shell |
| --- | --- |
| [opt/share/env.sh](opt/share/env.sh) | bash / zsh |
| `opt/share/env.fish` | fish |
| `opt/share/env.nu` | Nushell |


### Building Modules on GBI's Compute Systems

>[!IMPORTANT]
> Before starting, ensure that you're a member of the `gbi-sandpit-software`
> group in Okta PAM

>[!WARNING]
> We set the `GBI_MODULE_PATH` environment variable to "point" to the official
> software install location on the HPC cluster. While you're testing, please
> overwrite this to a temporary location by setting `export
> GBI_MODULE_PATH=<temporary location>`. Note that the `export` keyword is
> necessary for `make` to use the environment variable.

Please refer to the [relevant section below](#Adding-a-module) for how to add a
module to this repository. After you have finished creating a module recipe,
you will need to test and deploy it.

The process of testing modules is:
1. Create a temporary workspace (this can be in your home, or in `/tmp`)
2. Ensure that `GBI_MODULE_PATH` is either unset, or points to your temporary
   workspace.
3. Bootstrap: `make bootstrap`, followed by `source opt/share/env.sh`. This
   will make sure that lmod is configured to use your temporary workspace.
4. Build your module: `make <name of your module>`.
5. Test your module: `ml load <name of your module>` followed by any software
   tests you want to run.

The process of deploying your module (after successful tests) is:
1. In a fresh shell (check tat `GBI_MODULE_PATH` is
   `/mnt/gbi-shared/software`); and that `lmod` is the GBI LMod install.
2. Go to the main config repo: `cd
   $GBI_MODULE_PATH/GBI-Compute-Software-Module` and pull the latest version
   (containing your module).
3. Build the module: `make <your module name>`

### How it is deployed at GBI

At GBI we define a `/mnt/gbi-shared/system/etc/bash_env.sh` script, which is
automatically loaded during every shell startup. Its contents is roughly:
```
if [ -z "${__GBI_SHARED_ENV_LOADED:-}" ]; then
  __GBI_SHARED_ENV_LOADED=1

  if [ -r /mnt/gbi-shared/software/GBI-Compute-Software-Modules/opt/share/env.sh ]; then
    . /mnt/gbi-shared/software/GBI-Compute-Software-Modules/opt/share/env.sh
  fi

  export GBI_MODULE_PATH=/mnt/gbi-shared/software
fi
```
which does two things: 1) initialize the module system; 2) sets the
`GBI_MODULE_PATH` environment variable. The later is used by this project to
deploy all assets into the correct place.

## How it works

The build is a chain of three small Bash scripts, each of which only adds
context to the environment before `exec`ing the next one. This is what lets
every recipe use paths relative to the repo root without knowing where the repo
lives.

```
make <target>
  └── run.sh                 sets __PREFIX__  (physical path of the repo root)
        └── opt/bin/build.sh sets __MODULE_PATH__, __MODE__, __LIBC__, __DIR__
              └── opt/bin/render.sh <target>
                    └── opt/bin/simple-modules.ex sm-config[-build] --sm-root=…
                          ├── runs pre_install.sh / install.sh
                          ├── relocates the install tree
                          └── renders module_template.lua → <GBI_MODULE_PATH>/modules/<name>/<version>.lua
```

* [run.sh](run.sh) — resolves its own *physical* directory (symlinks and `..`
  resolved by traversal, portable back to bash 3.2 / pre-12.3 macOS), exports it
  as `__PREFIX__` along with the `__realpath` / `__script_dir` helpers, and execs
  its first argument.
* [opt/bin/build.sh](opt/bin/build.sh) — the build harness. `-m <path>` sets
  `__MODULE_PATH__`, `-b` selects `__MODE__=build`, `-g` selects
  `__LIBC__=musl`. `-b` and `-g` are mutually exclusive.
* [opt/bin/render.sh](opt/bin/render.sh) — picks the recipe directory for the
  requested target (`sm-config-build` under `MODE=build`, otherwise `sm-config`),
  invokes `simple-modules.ex`, and then runs an optional `extra_render.sh` hook
  from the target directory.

Note that `simple-modules.ex` and `simple-templates.ex` are Lua scripts that
require the bundled interpreter (they `require "posix"`). Running them outside
`source opt/share/env.sh` — or without `opt/bin` on `PATH` — fails with
`module 'posix' not found`.

## Anatomy of a module recipe

Each top-level directory (`eza/`, `bat/`, `rust/`, …) is one module. Inside it,
one or two `simple-modules` config directories:

| Path | Selected by |
| --- | --- |
| `<target>/sm-config/` | default — install a prebuilt binary release |
| `<target>/sm-config-build/` | `MODE=build` — compile from source |

The Makefile discovers its targets from exactly this layout — any root
directory holding one of these config dirs becomes a `make` target, with no
Makefile edit needed. Two optional marker files round out a recipe: `sm-help`
(line 1: a one-line summary, remaining lines: notes — shown by `make help`)
and `sm-opt-in` (its presence keeps the target out of `make all`, like
`cmake`, `llvm` and `zig-bootstrap`).

A config directory holds up to five files:

* **`settings.toml`** — the recipe proper. `[env]` variables are exported into
  `install.sh`; `[install]` declares `versions`, the `prefix` to search after
  installing, a `format` (a **Lua** pattern, so `-` and `+` must be escaped with
  `%`) to recognise the installed tree, an optional relocation `destination`, and
  whether to `stage`; `[module]` declares the values handed to the modulefile
  template; `[post]` controls the mandatory-relocate safety check and staging
  cleanup.
* **`local_settings.toml`** — site layout: module `name`, and where software and
  modulefiles go (`{SM_ROOT}/<name>` and `{SM_ROOT}/modules`). `variant` is set
  here too — the `sm-config-build` recipes use `variant = "compiled"` so a
  source build and a binary release of the same version don't collide.
* **`install.sh`** — the actual install, run inside the staging directory with
  `[env]` in scope. It can `module load` earlier modules (this is how `eza`,
  `bat` and `nu` pick up `rust`, how `fish` and `neovim` pick up `cmake`, and
  how `ncdu` picks up `zig`).
* **`pre_install.sh`** *(optional)* — runs before the per-version loop; `rust/`
  uses it to install `rustup` once into `RUSTUP_HOME`.
* **`module_template.lua`** — the modulefile, as a Mustache template. `{{{...}}}`
  placeholders are filled from `[module]`.

`simple-modules` substitutes `{BRACED}` names (`{SITE_DESTINATION}`,
`{STAGE_DIR}`, `{INSTALL_DIR}`, `{INSTALL_VERSION}`,
`{INSTALL_VERSION_VARIANT}`, `{RUNTIME_TARGET_TRIPLE}`, `{SITE_CONFIG_DIR}`,
`{SM_ROOT}`) and exposes helpers such as `resolve_archive_name` to
`install.sh`. `{RUNTIME_TARGET_TRIPLE}` plus `resolve_archive_name` is what lets
the binary-release recipes name the right upstream artifact per platform — see
[eza/sm-config/settings.toml](eza/sm-config/settings.toml) for the canonical
example.

### The `extra_render.sh` hook

If a target directory contains `extra_render.sh`, `render.sh` runs it after
`simple-modules` with `__DIR__`, `__PREFIX__`, `__MODULE_PATH__`, `__MODE__` and
`__LIBC__` exported. [rust/extra_render.sh](rust/extra_render.sh) uses it to
render a second, version-independent `rustup` modulefile via
`simple-templates.ex` — and, because it sees `__MODE__`, skips that modulefile
under `MODE=build`, where the toolchain is compiled and there is no rustup to
point at.

## Checking whether the installer has to be re-run

[opt/bin/check_versions.nu](opt/bin/check_versions.nu) answers the question
"did someone bump a `versions` list since this tree was installed?" without
running an installer. For every version in a recipe's `[install].versions` it
reconstructs the two artefacts `simple-modules` would have produced —

* the modulefile `<modules>/<name>/<version>[-<variant>].lua`, and
* the install tree `[install].destination`, with `{SITE_DESTINATION}`,
  `{INSTALL_VERSION}`, `{INSTALL_VERSION_VARIANT}` and `{STAGE_DIR}` expanded
  the way `simple-modules` expands them,

taking `name`, `variant`, `modules` and `destination` from
`local_settings.toml` (with `{SM_ROOT}` := the install root, and `variant`
overridden by `musl` in default mode, exactly as `render.sh` does) — and checks
whether both are on disk:

| Status | Meaning |
| --- | --- |
| `ok` | both artefacts present, neither older than the recipe |
| `missing` | neither present — this version was never installed |
| `partial` | only one present — an interrupted or half-cleaned install |
| `stale` | present, but a file in the recipe directory was modified afterwards |
| `rolling` | present and current, but the version is a moving target (`stable`, `nightly`, `master`, …) so presence proves nothing about freshness |

It also lists modulefiles that are on disk but that **no** recipe for that name
declares any more — the leftovers of a `versions` list that has since been
edited. Those do not need an installer run, only a `make clean`.

The exit code is 0 when nothing needs doing and 1 as soon as any version is
`missing`, `partial` or `stale`, so it can gate a build. `make check` wraps it
and honours the same `GBI_MODULE_PATH`, `MODE` and `VARIANT` knobs as the
install rules, with `TARGET` narrowing it to one module:

```bash
make check                                  # every default-mode recipe
make check MODE=build                       # every source-build recipe
make check TARGET=nu GBI_MODULE_PATH=$HOME/local
```

`check` needs a nushell — it is the one target that does, which is a little
circular given that `nu` is one of the modules here. Any `nu` on `PATH` will
do (`make nu && module load nu`, or a system one); point `NU` at an interpreter
that is not on `PATH`.

Called directly it takes recipe directories as positional arguments and has a
few knobs `make` does not expose:

```bash
nu opt/bin/check_versions.nu --mode all              # both recipes per target
nu opt/bin/check_versions.nu --json rust zig         # machine-readable report
nu opt/bin/check_versions.nu --quiet && echo current # exit code only
nu opt/bin/check_versions.nu --ignore-mtime          # presence only, never `stale`
```

Without `--module-path`/`--prefix` it falls back to `$__MODULE_PATH__` /
`$__PREFIX__` if it was launched through the harness, and otherwise to
`<repo>/usr` and its own location — so it works both inside a `run.sh` chain
and standalone.

## Makefile knobs

| Variable | Default | Meaning |
| --- | --- | --- |
| `GBI_MODULE_PATH` | `<repo>/usr` | Install root: software in `$GBI_MODULE_PATH/<name>`, modulefiles in `$GBI_MODULE_PATH/modules` |
| `VARIANT` | `gnu` | `gnu` or `musl`; `musl` passes `-g` → `--variant=musl` |
| `MODE` | *(empty)* | `build` selects `sm-config-build` recipes |
| `ML_INIT_FILE` | `<repo>/opt/lmod/lmod/init` | Lmod init directory baked into the generated `env.*` files |
| `ML_INIT` | `source <repo>/opt/share/env.sh` | Prelude for each install shell; set `ML_INIT=` to skip Lmod init |
| `TARGET` | — | Required by `clean`; narrows `check`; must name a discovered recipe |
| `NU` | `nu` | The nushell interpreter `check` runs |

`VARIANT=musl` and `MODE=build` cannot be combined — `build.sh` rejects it.

## Available modules

`make help` prints the live, auto-discovered version of this list — summaries
come from each recipe's `sm-help`, versions from its `settings.toml`, and
build dependencies from the `module load` lines of its `install.sh`.

| Target | What it is | `MODE=build` |
| --- | --- | --- |
| `rust` | the Rust toolchain, via `rustup` | yes, needs a **source-built** `llvm` + a `python3` |
| `eza` | `ls`, but better | yes, needs `rust` |
| `bat` | `cat`, but better | yes, needs `rust` |
| `nu` | Nushell — a completely new way to think about a shell | yes, needs `rust` |
| `fish` | the friendly interactive shell | yes, needs `cmake` + `rust` |
| `neovim` | the Neovim editor | yes, needs `cmake` |
| `uv` | a better way to manage Python | yes, needs `rust` |
| `zig` | the Zig language, and `zig cc` — a drop-in C/C++ cross compiler | yes, needs `cmake` + a **source-built** `llvm` |
| `ncdu` | `du`, but with a text-mode user interface | yes, needs `zig` |
| `parallel-tar` | multi-threaded archival tools: compress large data sets, and validate their quality | yes, needs `rust` |
| `go` | the Go programming language toolchain | no — bootstrapping needs an existing go |
| [`gbi`](gbi/README.md) | verified HPC data movement; immediate foreground transfers and automatic Slurm for bulk work | no — in-tree Python source; needs Python 3.9+ and rclone |
| `rclone` | rsync for cloud storage | no — upstream ships static go binaries for every platform |
| `apptainer` | containers for HPC — `apptainer`/`singularity` without root | no — upstream ships a relocatable unprivileged deb |

Upstream `eza` and `ncdu` ship no macOS binaries, so their default-mode recipes
fail fast on darwin with a pointer to `make <target> MODE=build`.

`apptainer` is linux x86_64 only and has no source recipe to fall back to, so it
fails fast everywhere else. It also needs the *node* to allow unprivileged user
namespaces and expose `/dev/fuse` — the install itself is rootless, but the
kernel has to permit what the containers do. The recipe unpacks upstream's
non-setuid `.deb`, which bundles the container helpers — `mksquashfs`,
`squashfuse_ll`, `fuse-overlayfs`, `fuse2fs`, `proot` — so no `squashfs-tools`
or `fuse-overlayfs` package is needed. Those helpers are dynamically linked,
and the two libraries that are genuinely optional on a slim image
(`libfuse3.so.3`, `liblzo2.so.2` — GBI login nodes have neither) are vendored
into `lib/` and put on `LD_LIBRARY_PATH` by the modulefile. The node must still
provide `libseccomp.so.2`, which `apptainer` and `starter` link against; the
rest of what they need ships with `dpkg`. Unpacking needs `xz` on PATH, which
login nodes may lack and compute nodes have.

Three more targets are **opt-in** — valid for `make <target>` and `make clean`,
but skipped by `make all`, since they are either large or only interesting as
build dependencies:

| Target | What it is | `MODE=build` |
| --- | --- | --- |
| `cmake` | the CMake build system | yes, no module dependencies |
| `llvm` | clang, lld and the LLVM development libraries | yes, needs `cmake` |
| `zig-bootstrap` | zig rebuilt with only a C compiler; installs as `zig/<version>-bootstrap` | source build only |

## Adding a module

Take a look at [the docs on this subject](./docs/adding-modules.md) an
operational runbook.

### By Hand

Copy an existing directory whose install shape matches yours (`eza/` for a
tarball release, `uv/` for a vendor install script, `neovim/` for a CMake
source build), adjust `settings.toml` / `local_settings.toml` / `install.sh` /
`module_template.lua` — and that's it: the [Makefile](Makefile) discovers any
root directory holding an `sm-config/` or `sm-config-build/` recipe, so the new
name is immediately a target, walked by `all`, and listed by `make help`.
Optionally add an `sm-help` file (line 1: summary, remaining lines: notes) to
describe it in the help output, and an `sm-opt-in` marker file to keep it out
of `make all`. Build-mode dependencies are not declared anywhere extra — they
are read from the `module load` lines of `install.sh`.

### From a template

[templates/](templates/) holds parameterised recipes that `simple-templates`
renders into a complete recipe directory — `sm-config/` plus an `sm-help` — one
per common install strategy:

| Template | Install strategy |
| --- | --- |
| [templates/github](templates/github) | download a per-platform binary from a GitHub release — the [eza](eza/sm-config/settings.toml) pattern, with `resolve_archive_name` picking the artifact for this OS/arch/libc |
| [templates/targz](templates/targz) | `curl` one fixed tarball URL and unpack it — for non-GitHub hosts, and for platform-independent bundles such as script collections |
| [templates/cargo](templates/cargo) | `module load rust; cargo install <name> --version <version> --locked` |
| [templates/uv](templates/uv) | `module load uv; uv tool install <name>==<version>`, with a uv-managed python kept inside the module tree |

The last two render `sm-config/` (not `sm-config-build/`) deliberately: for a
tool that only exists on crates.io or PyPI, the package-manager install *is*
the default mode — not everything needs a `-build` recipe, and not everything
has a binary release.

[templates/render.sh](templates/render.sh) takes the template, the module name
and the first version, plus template-specific `key=value` parameters:

```
templates/render.sh <template> <name> <version> [key=value ...]
```

Each template documents its parameters in `templates/<template>/settings.toml`.
A parameter listed there has a default and may be omitted; the others are
required, and rendering stops with `Variable '<key>' needed but not defined`
when one is missing. Because that settings file re-emits
`{{{INSTALL_VERSION}}}` and `{{{PATH}}}` untouched, those placeholders survive
template expansion and are filled in later by `simple-modules` — and
single-braced `{NAMES}` pass through Mustache anyway, so parameter values can
use simple-modules substitutions directly: an `asset` of
`eza_{RUNTIME_TARGET_TRIPLE}`, a `source` of
`https://…/v{INSTALL_VERSION}/tool`.

Run it through the same harness so `__PREFIX__` and friends are set:

```bash
./run.sh opt/bin/build.sh -m ./usr templates/render.sh github dust 1.2.5 \
    repo=https://github.com/bootandy/dust \
    'asset=dust-v{INSTALL_VERSION}-{RUNTIME_TARGET_TRIPLE}'
./run.sh opt/bin/build.sh -m ./usr templates/render.sh cargo tokei 12.1.2
./run.sh opt/bin/build.sh -m ./usr templates/render.sh uv ruff 0.13.0
```

The expanded recipe lands in `templates/rendered/<name>/` (gitignored) — a
staging area the Makefile deliberately does not see. Test-install it from
there, then promote it to the repo root, where target discovery takes over:

```bash
./run.sh opt/bin/build.sh -m ./usr opt/bin/render.sh templates/rendered/dust
mv templates/rendered/dust ./dust      # now a make target, walked by `all`
```

A rendered recipe is a starting point like any hand-copied one: edit the
summary in its `sm-help` (or set it at render time, `summary='…'`), drop in an
`sm-opt-in` marker if `make all` should skip it, and adjust `install.sh` where
upstream naming is unusual — [neovim](neovim/sm-config/install.sh),
[fish](fish/sm-config/install.sh) and
[parallel-tar](parallel-tar/sm-config/install.sh) show what those tweaks tend
to look like.

### With an AI agent

Direct your agent to [AGENTS.md](AGENTS.md) (repo invariants + condensed
workflow; picked up automatically by most coding agents) and
[docs/adding-modules.md](docs/adding-modules.md) (the full playbook, gotchas
included). Claude Code triggers on these automatically via
`.claude/skills/add-module/`.

## Repository layout

```
Makefile              discovers recipes and generates the per-target rules
run.sh                __PREFIX__ bootstrap + portable physical-path helpers
opt/
  update_bin.sh       curls the standalone simple-modules / simple-templates
  bin/                simple-modules.ex, simple-templates.ex, lua, luac, build.sh, render.sh
    check_versions.nu declared-vs-installed version check (see above)
    help.sh           renders `make help` from the discovered recipes
  lmod/
    bootstrap.sh      installs lua-regolith + Lmod, writes opt/share/env.*
    github.com/…/Lmod vendored Lmod source
    lua-5.1.4.9/      TACC Lua fallback (see opt/lmod/README.md for the macOS patch)
  share/env.{sh,fish,nu}   generated shell init (gitignored)
<target>/             one directory per module: sm-config/ and/or sm-config-build/,
                      plus optional sm-help (description) and sm-opt-in (marker)
templates/            simple-templates recipes for generating new modules
test/test_macos.sh    full-matrix smoke test
usr/                  default install root (gitignored)
```

`make update` re-downloads the two standalone Lua executables from GitLab, so
you pick up upstream `simple-modules` / `simple-templates` fixes without
vendoring them.

## Testing

[test/test_macos.sh](test/test_macos.sh) does a full teardown and rebuild —
`make realclean`, `bootstrap`, then every target in both default and `build`
mode into `test/usr`. It expects `__PREFIX__`, so run it through `run.sh`:

```bash
./run.sh test/test_macos.sh
```

## Rough edges

* `bootstrap.sh` reads its three positional arguments (`VERSION`, `MODULEPATH`,
  `ML_INIT_FILE`) before its `getopts` loop, so the documented `-n/-a/-l/-f`
  overrides — including the `-f` fallback that builds the vendored TACC Lua
  instead of `lua-regolith` — are never reached in practice. Auto-detection is
  the only path today.

## Additional Information

Module specific notes can be found in the [appendix](./docs/appendix.md).
