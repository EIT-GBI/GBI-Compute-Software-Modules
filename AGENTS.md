# Agent guide — GBI-Compute-Software-Modules

Guidance for AI coding agents of any vendor (this file follows the
[agents.md](https://agents.md) convention; Claude Code additionally has a
trigger skill in `.claude/skills/`). Humans: start with [README.md](README.md).

## What this repo is

A self-contained Lmod module farm: Bash + a vendored Lua toolchain
(`simple-modules` / `simple-templates`) install CLI software from declarative
TOML recipes — no root, no system package manager, no Python. One directory per
module at the repo root; the Makefile discovers them by layout.

## Invariants — read before running anything

- Everything runs through the harness:
  `./run.sh opt/bin/build.sh -m <install-root> <script> [args]`. Recipe and
  helper scripts assume `__PREFIX__` / `__MODULE_PATH__` / `__DIR__` from that
  chain and fail without it.
- The vendored Lua tools (`opt/bin/simple-modules.ex`, `simple-templates.ex`)
  need the bundled interpreter. Run anything that invokes them inside
  `bash -c "source opt/share/env.sh; ..."` — otherwise whatever system `lua`
  is on `PATH` runs them and dies with `module 'posix' not found`.
- `make help` is the authoritative, live list of targets and knobs. Targets
  are auto-discovered from `<name>/sm-config[-build]/` — **never** edit the
  Makefile to add one.
- Installs land under `GBI_MODULE_PATH` (default `./usr`, gitignored);
  modulefiles under `$GBI_MODULE_PATH/modules`. When verifying with Lmod,
  `module use` that path explicitly — the MODULEPATH baked into
  `opt/share/env.sh` may point at a stale tree (e.g. `test/usr/modules` after
  a smoke-test run).
- Consistency check: `make check TARGET=<name> GBI_MODULE_PATH=./usr`
  (needs a nushell — `module load nu` first).
- Destructive: `make realclean` deletes every installed module + modulefile
  **and** the bootstrapped Lua/Lmod. Deletions are scoped to the per-recipe
  dirs and `modules/` under `GBI_MODULE_PATH` — never the prefix itself, which
  on shared deployments holds more than the install (it can even contain this
  checkout). `./run.sh test/test_macos.sh` is a full teardown-and-rebuild
  (long, network-heavy). Don't run either casually.
- Don't commit or push unless asked.

## Adding a module (condensed)

```bash
# render a recipe skeleton (templates: github, targz, cargo, uv --
# parameters documented in templates/<template>/settings.toml)
./run.sh opt/bin/build.sh -m ./usr templates/render.sh <template> <name> <version> [key=value ...]

# tweak templates/rendered/<name>/ (platform naming, PATH/bin, sm-help),
# then test-install from staging and verify through Lmod
bash -c "source opt/share/env.sh; ./run.sh opt/bin/build.sh -m ./usr opt/bin/render.sh templates/rendered/<name>"
bash -c "source opt/share/env.sh; module use $(pwd)/usr/modules; module load <name> && <tool> --version"

# promote: now a make target, discovered automatically
mv templates/rendered/<name> ./<name>
```

Afterwards update `README.md` (module table + the two "(today: …)" lists) and
`test/test_macos.sh`.

**The full playbook — template choice, house idioms, and the gotchas that cost
real time — is [docs/adding-modules.md](docs/adding-modules.md). Read it
before starting that task.**
