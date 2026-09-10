# Adding a module — the field guide

The README's "Adding a module" section is the authoritative reference; this is
the operational playbook — the order that works, and the traps. It is written
for AI coding agents of any vendor (see [AGENTS.md](../AGENTS.md) for
repo-wide invariants) and is just as usable by humans.

## Golden path

```bash
# 1. render into the gitignored staging area templates/rendered/<name>/
./run.sh opt/bin/build.sh -m ./usr templates/render.sh <template> <name> <version> [key=value ...]

# 2. tweak the rendered recipe (see below) -- a render is a starting point

# 3. test-install from staging (env.sh is NOT optional, see gotchas)
bash -c "source opt/share/env.sh; ./run.sh opt/bin/build.sh -m ./usr opt/bin/render.sh templates/rendered/<name>"

# 4. verify: binary, modulefile, and a real Lmod round trip
./usr/<name>/<version>/bin/<tool> --version
cat usr/modules/<name>/<version>.lua
bash -c "source opt/share/env.sh; module use $(pwd)/usr/modules; module load <name> && <tool> --version"

# 5. promote -- the Makefile auto-discovers it, no Makefile edit ever
mv templates/rendered/<name> ./<name>
make help            # target listed, with summary + versions?
make -n <name>       # resolves to the install rule?

# 6. recipe-vs-installed consistency check (needs a nushell, and must run
#    after promotion -- make validates TARGET against discovered recipes)
bash -c "source opt/share/env.sh; module load nu; make check TARGET=<name> GBI_MODULE_PATH=./usr"
```

Then update the docs (see checklist at the end).

## Picking the template

Parameters and defaults live in `templates/<template>/settings.toml` — read it
first. A parameter without a default is required; missing ones fail with
`Variable '<key>' needed but not defined`.

| Template | Use when | Canonical example |
| --- | --- | --- |
| `github` | per-platform asset on a GitHub release (`resolve_archive_name` + `{RUNTIME_TARGET_TRIPLE}` picks gnu/musl per platform) | `eza/sm-config` |
| `targz` | one fixed tarball URL — or per-platform naming you'll hand-write in `install.sh` | `zig/sm-config`, `go/sm-config` |
| `cargo` | tool only exists on crates.io (loads `rust`) | `bat` MODE=build |
| `uv` | tool only exists on PyPI (loads `uv`, installs unstaged — see the no-op-relocate trick in its install.sh) | `templates/uv` |

**Always fetch the real current version at render time** — never trust model
memory. E.g. `curl -s 'https://go.dev/VERSION?m=text'`-style endpoints, or
`gh api repos/<owner>/<repo>/releases/latest --jq .tag_name`.

## Render mechanics

- `name` and `version` are positional; everything else is `key=value`.
- Single-braced `{PLACEHOLDERS}` pass through Mustache untouched, so parameter
  values can carry simple-modules substitutions — quote them in the shell:
  `'source=https://go.dev/dl/go{INSTALL_VERSION}'`,
  `'asset=dust-v{INSTALL_VERSION}-{RUNTIME_TARGET_TRIPLE}'`.
- Inside template files, `{{x}}` HTML-escapes (`/` → `&#x2F;`) — anything
  URL-ish must be `{{{x}}}`.
- **Re-rendering wipes `templates/rendered/<name>/`** — render first, tweak
  after the last render, or your edits are gone.

## Expected post-render tweaks

A rendered recipe is a starting point (README says so); these are the house
idioms:

- **Per-platform download naming** (`targz`): `RUNTIME_OS`/`RUNTIME_ARCH` are
  raw `uname -s`/`-m` (`Darwin`, `arm64`/`x86_64`/`aarch64`). Map them in
  `install.sh` with the exported `to_lower` helper plus arch/OS fixups, then
  assemble `SOURCE` from a `SOURCE_PREFIX`. Mind that upstreams disagree on
  spelling: go wants `amd64`/`arm64` + `darwin`; zig wants `x86_64`/`aarch64`
  + `macos` (and arch-before-OS). Compare `go/sm-config/install.sh` and
  `zig/sm-config/install.sh`.
- **Binaries in `bin/`**: keep `[module] PATH = "{INSTALL_DIR}"` and append in
  the template instead — `prepend_path("PATH", "{{{PATH}}}/bin")` (the
  `neovim`/`go` idiom). Tools that sit at the tree root (zig) prepend the dir
  itself.
- **`module_template.lua`**: add a `help([[...]])` block and a
  `whatis("URL: ...")` (zig style). No GOROOT-style env vars unless the tool
  can't find its own root relative to the binary.
- **`sm-help`**: line 1 = pithy lowercase summary (`ls but better`), further
  lines = notes. Don't write "MODE=build is not supported" — `make help`
  prints that automatically for sm-config-only recipes; state the *why*
  instead (`bootstrapping go from source needs an existing go, hence no
  MODE=build recipe`).
- **No upstream binary for some platform?** Gate at the top of `install.sh`
  with a clear error naming the alternative — `eza`, `ncdu` and `llvm` show
  the pattern (check `to_lower "$RUNTIME_OS"`, explain, point at
  `make <name> MODE=build`, `exit 1`). And keep `curl --fail` (both download
  templates render it): a 404 must stop at the download, not hand GitHub's
  9-byte "Not Found" body to tar.
- **`[install].format`** is a **Lua pattern** — escape `-` and `+` as `%-` `%+`.
- Not everything needs an `sm-config-build`: if the package-manager or binary
  install *is* the sensible default (cargo/uv/go), ship sm-config only.
  Heavy or dependency-only targets get an `sm-opt-in` marker file.
- Build recipes set `variant = "compiled"` in `local_settings.toml` so source
  and binary installs of one version don't collide — and note Lmod then
  resolves a bare `module load <name>` to the `-compiled` one.
- Build-mode dependencies are **only** declared as `module load` lines in
  `sm-config-build/install.sh` — `make help` and `make all MODE=build` read
  them from there.

## Gotchas (each cost real time)

- `module 'posix' not found` from a `lua:` stack trace ⇒ a system Lua ran the
  `.ex` tools. Anything that invokes `simple-modules.ex` must run inside
  `bash -c "source opt/share/env.sh; ..."` — exactly what the Makefile does.
- `module load <name>` → "unknown module" right after a successful install ⇒
  the MODULEPATH baked into `opt/share/env.sh` points elsewhere (a
  `test/test_macos.sh` run bakes `test/usr/modules`). Always
  `module use $(pwd)/usr/modules` explicitly when verifying.
- Verify beyond `--version` when cheap: for a toolchain, compile and run a
  hello-world (in a scratch/temp directory, with caches redirected — e.g.
  `GOCACHE`/`GOPATH` — so nothing lands in `$HOME` or the repo).
- You can only exercise the current platform's leg of an OS/arch mapping —
  keep the mapping table small and mirrored from zig/go so the untested legs
  are correct by inspection, and state in your report which legs actually ran.
- `make <name> MODE=build` on an sm-config-only recipe errors with a friendly
  stub — that's correct behavior, not a bug.
- Cleanup of a test install: `make clean TARGET=<name> GBI_MODULE_PATH=./usr`.

## Docs checklist on promote

1. `README.md` "Available modules" table — one row; the third column states
   MODE=build deps, or why there is no build mode.
2. `docs/appendix.md` — the two "(today: `...`)" target lists (alphabetical): the
   `make all` walk list, and the MODE=build skipped/covered note.
3. `test/test_macos.sh` — hardcoded target lists: add to the default-mode
   block (with a comment if the platform lacks a binary, like eza/ncdu) and
   to the MODE=build block if a build recipe exists.
4. Commit only when asked; `templates/rendered/` is gitignored staging and
   should stay empty after promotion.
