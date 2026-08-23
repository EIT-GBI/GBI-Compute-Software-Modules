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

No root, no system package manager, no Python. Everything is self-contained
under this checkout (or wherever you point `MODULE_PATH`). The single exception
is `rust MODE=build` — rustc's own build system is driven by `x.py`, so that
one recipe needs a `python3` on `PATH`.

## Quick start

```bash
make update                     # fetch simple-modules.ex + simple-templates.ex
make bootstrap                  # build Lua + Lmod into opt/, write opt/share/env.*
source opt/share/env.sh         # puts opt/bin on PATH, inits Lmod, `ml use` the modules dir
make all                        # install every module in TARGETS
```

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
                          └── renders module_template.lua → <MODULE_PATH>/modules/<name>/<version>.lua
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

## Makefile knobs

| Variable | Default | Meaning |
| --- | --- | --- |
| `MODULE_PATH` | `<repo>/usr` | Install root: software in `$MODULE_PATH/<name>`, modulefiles in `$MODULE_PATH/modules` |
| `VARIANT` | `gnu` | `gnu` or `musl`; `musl` passes `-g` → `--variant=musl` |
| `MODE` | *(empty)* | `build` selects `sm-config-build` recipes |
| `ML_INIT_FILE` | `<repo>/opt/lmod/lmod/init` | Lmod init directory baked into the generated `env.*` files |
| `ML_INIT` | `source <repo>/opt/share/env.sh` | Prelude for each install shell; set `ML_INIT=` to skip Lmod init |
| `TARGET` | — | Required by `clean`; must be one of `TARGETS` or `AUX_TARGETS` |

`VARIANT=musl` and `MODE=build` cannot be combined — `build.sh` rejects it.

## Available modules

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

Three more targets are **opt-in** — valid for `make <target>` and `make clean`,
but skipped by `make all`, since they are either large or only interesting as
build dependencies:

| Target | What it is | `MODE=build` |
| --- | --- | --- |
| `cmake` | the CMake build system | yes, no module dependencies |
| `llvm` | clang, lld and the LLVM development libraries | yes, needs `cmake` |
| `zig-bootstrap` | zig rebuilt with only a C compiler; installs as `zig/<version>-bootstrap` | source build only |

### Building zig from source

Two routes, for two different reasons.

**`make zig MODE=build`** is the one to use if you want a compiler. It is the
full CMake build against `cmake` + `llvm`, and produces a complete zig — `zig
cc`, `zig ar`, release optimisation, cross-compilation.

The `llvm` it links has to be the **source-built** one (`make llvm MODE=build`,
installed as `llvm/<version>-compiled`). The upstream binary release is an LTO
build, so its static archives contain LLVM bitcode rather than native objects,
and zig's own linker rejects them when it links stage3 (`unknown cpu
architecture: 0`). The prebuilt `llvm` is still a perfectly good clang/lld
install — it is just a dead end for this, and its modulefile says so.

**`make zig-bootstrap MODE=build`** is the one to use if you care where your
compiler came from. Zig is written in zig, so building it normally means
already having it; `bootstrap.c` breaks that cycle using nothing but a C
compiler. Reach for it to:

* port zig to a machine or architecture with no zig binary yet,
* establish a trust anchor — a toolchain you compiled rather than downloaded,
* work on the compiler itself, where the bootstrap stages are the point.

It stops at zig's intermediate `zig2` (upstream advises against going further
for now), so it is provenance rather than a toolchain. The `bootstrap` variant
lets it sit alongside a real zig.

### Building rust from source

`make rust` (default mode) installs [rustup](https://rustup.rs/) and lets it
fetch a binary toolchain — quick, and what you want most of the time.
`make rust MODE=build` instead compiles the official source tarball from
`static.rust-lang.org`, and installs it as `rust/<version>-compiled` next to
the rustup one. Reach for it to:

* have a toolchain whose sources you hold, rather than a binary rustup handed
  you,
* build a version rustup will not give you as a release — the recipe's
  `versions` list also accepts the rolling `beta` and `nightly` source
  tarballs, and derives the release channel from whichever you name,
* run the compiler's own build system, which is the point if you are working on
  rustc itself.

Two things it needs that the other recipes do not:

* **a `python3` on `PATH`.** `./configure` and `x.py` are python; there is no
  way around an interpreter here. Any python3 will do.
* **the source-built `llvm`** (`make llvm MODE=build`, installed as
  `llvm/<version>-compiled`), for the same reason zig needs it: rustc links
  LLVM's static archives into `librustc_driver`, and the upstream binary
  release is an LTO build against LLVM's own unstable-ABI libc++. The version
  pin lives in
  [rust/sm-config-build/settings.toml](rust/sm-config-build/settings.toml) —
  rustc checks the major itself and 1.98.0 wants ≥ 21. Building the copy of
  llvm-project that ships inside the rust tarball would work too, at the price
  of a second full LLVM build.

Unlike `zig-bootstrap` this is **not** a bootstrap from nothing: rust is written
in rust, and there is no `bootstrap.c` to break the cycle with. By default
`x.py` downloads the stage0 compiler pinned in the source tree's `src/stage0`
(the previous release, with its sha256s) and builds this compiler with that.
If you would rather bootstrap from a compiler you already have, point
`STAGE0_ROOT` at its prefix, e.g.

```toml
STAGE0_ROOT = "{SM_ROOT}/rust/stable"
```

Bootstrap insists the stage0 be the same version as the source being built or
one minor behind it, so this only works while the two are in step.

The module it renders has no rustup in it — `cargo` and `rustc` are the plain
binaries, and `rustup toolchain` and friends do not exist. Worth knowing before
you install it: with both modules present Lmod resolves a bare `module load
rust` to `rust/<version>-compiled`, not to `rust/stable`. Every recipe that
`cargo install`s something (`eza`, `bat`, `nu`, `uv`, `fish`) loads rust that
way, so they will pick up the source-built toolchain from then on. Name
`rust/stable` explicitly if you want the rustup one back.

### Cost

Measured on a 14-core Apple Silicon machine at the recipes' default `NPROCS=8`:

| Step | Wall time | Installed |
| --- | --- | --- |
| `make llvm` (prebuilt) | ~1 min | 7.8 GB |
| `make llvm MODE=build` | ~16 min | 3.6 GB |
| `make zig-bootstrap MODE=build` | ~36 min | 0.3 GB |
| `make zig MODE=build` | ~2 h | 1.1 GB |
| `make rust MODE=build` | ~12 min | 0.4 GB |

`zig MODE=build` is the long pole, and heavier than the bootstrap rather than
lighter: it runs the same bootstrap stages internally — two ~250 MB
single-translation-unit C compiles, which are serial and so ignore `NPROCS` —
and then links LLVM on top. Peak RSS stayed between 2 and 6 GB with no swap, so
the `NPROCS`/`NLINK` defaults are conservative; raise them if you have the
headroom.

`rust MODE=build` is cheap by comparison only because the `llvm` it links is
already built — that ~16 min belongs to its total if you do not have it yet.
Its staging tree is the transient cost instead: 4 GB of unpacked sources plus
the build on top, deleted once the install lands.

Individual targets are just `make <target>`, e.g.:

```bash
make MODULE_PATH=$HOME/local neovim
make MODULE_PATH=$HOME/local MODE=build neovim
```

`make all` walks `TARGETS` (`rust eza bat nu fish neovim uv zig ncdu`) —
`cmake`, `llvm` and `zig-bootstrap` are in `AUX_TARGETS` instead, so ask for
them by name. Under `MODE=build`, `all` installs `rust` and `zig` in default
mode first — both *can* be built from source, but only against the opt-in
`llvm` module, and everything else needs them — and then builds
`eza bat nu neovim ncdu`.

## Adding a module

**By hand:** copy an existing directory whose install shape matches yours
(`eza/` for a tarball release, `uv/` for a vendor install script, `neovim/` for
a CMake source build), adjust `settings.toml` / `local_settings.toml` /
`install.sh` / `module_template.lua`, then add a target block to the
[Makefile](Makefile) and its name to `TARGETS`.

**From a template:** [templates/](templates/) holds parameterised recipes that
`simple-templates` expands into a complete `sm-config` directory. Two shapes
ship today:

| Template | Install strategy |
| --- | --- |
| [templates/targz](templates/targz) | `curl` a `.tar.<ext>` from a URL and unpack it |
| [templates/cargo](templates/cargo) | `module load rust; cargo install <name>` |

[templates/render.sh](templates/render.sh) takes the template directory plus the
four parameters declared in [templates/settings.toml](templates/settings.toml):

```
templates/render.sh <template> <name> <version> <source> <ext>
```

and writes the expanded recipe to `templates/rendered/<name>/` (gitignored).
It renders the whole directory (`--dir`) and treats the nested `render.sh` as a
verbatim resource rather than a template, so each rendered recipe ships with its
own standalone installer. Because the templated `settings.toml` re-emits
`{{{INSTALL_VERSION}}}`, `{{{PATH}}}` and `{{{LD_LIBRARY_PATH}}}` untouched,
those placeholders survive template expansion and are filled in later by
`simple-modules`.

Run it through the same harness so `__PREFIX__` and friends are set:

```bash
./run.sh opt/bin/build.sh -m ./usr templates/render.sh targz mytool 1.2.3 https://example.com/mytool.tar.gz gz
./run.sh opt/bin/build.sh -m ./usr opt/bin/render.sh templates/rendered/mytool
```

## Repository layout

```
Makefile              per-target rules and the help text
run.sh                __PREFIX__ bootstrap + portable physical-path helpers
opt/
  update_bin.sh       curls the standalone simple-modules / simple-templates
  bin/                simple-modules.ex, simple-templates.ex, lua, luac, build.sh, render.sh
  lmod/
    bootstrap.sh      installs lua-regolith + Lmod, writes opt/share/env.*
    github.com/…/Lmod vendored Lmod source
    lua-5.1.4.9/      TACC Lua fallback (see opt/lmod/README.md for the macOS patch)
  share/env.{sh,fish,nu}   generated shell init (gitignored)
<target>/             one directory per module: sm-config/ and/or sm-config-build/
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
