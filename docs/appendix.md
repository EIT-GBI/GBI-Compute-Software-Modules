# GBI Module System Documentation Appendices

## Building zig from source

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

## Building rust from source

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

## Cost

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
make GBI_MODULE_PATH=$HOME/local neovim
make GBI_MODULE_PATH=$HOME/local MODE=build neovim
```

`make all` walks every discovered target without an `sm-opt-in` marker
(today: `bat eza fish gbi go ncdu neovim nu parallel-tar rclone rust uv zig`) — `cmake`,
`llvm` and `zig-bootstrap` carry the marker, so ask for them by name. Under
`MODE=build`, `all` installs `rust` and `zig` in default mode first — both
*can* be built from source, but only against the opt-in `llvm` module, and
everything else needs them — and then builds every target whose `module load`
dependencies those two cover (today: `bat eza ncdu nu parallel-tar uv`);
targets that need anything else are skipped and reported (`fish` and `neovim`
load the opt-in `cmake`, and `gbi`, `go` and `rclone` have no build recipes).
