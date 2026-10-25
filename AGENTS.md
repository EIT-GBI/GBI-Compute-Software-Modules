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
- Shared deployment source boundary: build the reviewed shared release from
  `$HOME/software-builds/GBI-Compute-Software-Modules`
  with `GBI_MODULE_PATH=/mnt/gbi-shared/software`. Do not put additional
  source checkouts, staged source, build directories, worktrees, or
  hash-named retry copies under `/mnt/gbi-shared/software`. The existing
  checkout used by the login startup bootstrap is a legacy canonical
  bootstrap retained until a separately reviewed startup change; do not add
  copies or relocate it in a module change. Local testing may continue to use
  the default `./usr` install root.
- Consistency check: `make check TARGET=<name> GBI_MODULE_PATH=./usr`
  (needs a nushell — `module load nu` first).
- Destructive: `make realclean` deletes every installed module + modulefile
  **and** the bootstrapped Lua/Lmod. Deletions are scoped to the per-recipe
  dirs and `modules/` under `GBI_MODULE_PATH` — never the prefix itself, which
  on shared deployments may contain other managed installations. The legacy
  canonical startup bootstrap is retained separately pending its own reviewed
  relocation. `./run.sh test/test_macos.sh` is a full teardown-and-rebuild
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

# Code style

Use these preferences for code you write or edit, across languages. They are
intentional and may differ from a language's conventional style. Aim for
compact, readable code with clear visual structure. Apply judgment, not
mechanical rules.

**The examples use Julia and Python, but these style rules apply to all
languages.** Adapt the layout to each language's syntax; do not limit these
preferences to Julia or Python files.

## Indentation and line length

- Use **4 spaces** per indentation level, not tabs, except where the language
  or required tooling mandates otherwise.
- Aim for **about 80 characters per line**, including indentation. This is a
  soft target. A modest overrun is preferable to awkward wrapping, cryptic
  names, or unnecessary intermediate variables.
- Keep short calls and function signatures on one line when they read
  naturally.

## One action per line; align related code

- Put independent statements or actions on separate lines. Do not combine
  mutations just because they update related state. Avoid semicolon-separated
  statements and other tricks that squeeze sequential actions onto one line.
- This does not prohibit multiple arguments on a line, ordinary nested
  expressions, or semicolons required by syntax, such as Julia keyword argument
  separators or C-style loop headers.
- Vertically align related assignments, field declarations, type annotations,
  and named arguments when a small local group benefits from it.
- Keep alignment local and restrained. Do not add excessive padding, align
  unrelated code, or split compact argument groups solely to align their `=`,
  except for the declaration layout with default values described below.

For example:

```julia
used         += 1
kept         += 1
buffer[used]  = value

push!(result.x, x)
push!(result.y, y)
```

## Calls, constructors, and argument grouping

- When a call needs wrapping, keep the function name and opening `(` together,
  then start the arguments on the next line.
- Indent argument lines **4 spaces beyond the containing statement's base
  indentation**. Never align them under the opening parenthesis or first
  argument.
- Put the closing `)` on its own line, aligned with the containing statement's
  base indentation. Keep any required punctuation with it.
- Prefer **several short, related arguments on each line**. This applies to
  positional arguments, keyword/named arguments, and declaration parameters,
  with an exception for declarations that specify default values (see below).
- Break near the width target, or earlier when it keeps a meaningful group
  together. Do not fill every line to capacity at the expense of grouping.
- Use one argument per line when arguments are long or complex, or when a
  vertical list materially improves readability. It is not the default simply
  because a call or signature spans multiple lines.
- Apply the same rules to constructor calls and object initialization. Group
  short related fields where syntax allows; give complex fields their own
  lines.

```julia
next_edges, splits, merges = refine_edges(
    edges, counts;
    max_cells, min_cells, min_width = width,
    split_factor, merge_factor, merge_tolerance
)

grid = learn_grid(
    source;
    initial_cells = 32, max_cells = 256, min_cells = 6,
    split_factor = 1.5, merge_factor = 0.5, merge_tolerance = 0.25
)
```

## Wrapped function declarations and definitions

Declarations use extra indentation to distinguish parameters from the body.
Relative to the indentation of the declaration itself:

- Parameter lines: **12 spaces** (three indentation levels).
- Closing `)`: **8 spaces** (two indentation levels).
- Function body: **4 spaces** (one indentation level).

Keep the opening `(` on the declaration line. Keep necessary syntax such as a
colon, return annotation, or opening brace with the closing `)` where
appropriate. This applies to functions, methods, and constructor definitions.
Calls still use the ordinary call layout above. All offsets are relative,
including in nested code.

```julia
function refine_edges(
            edges, counts; max_cells, min_cells, min_width,
            split_factor, merge_factor, merge_tolerance
        )
    target = sum(counts) / max_cells
    widths = diff(edges)
    # ...
end
```

For declarations that specify default values, **one parameter per line is also
fine**, even when the parameters are short. Align related defaults when
helpful. This is an option, not a requirement: compact groups remain fine, and
required parameters can stay grouped while parameters with defaults get their
own lines.

For example, in Python:

```python
def build_report(
            records, columns, filters,
            title          = "Summary",
            include_totals = True,
            precision      = 2
        ):
    rows = select_rows(records, columns, filters)
    # ...
```

## Apply with judgment

Preserve language syntax and program behavior. These examples illustrate
layout, not naming conventions, API design, or a requirement to rewrite code
into Julia idioms. Use the equivalent layout where a language has different
delimiters.

Apply the style to new and changed code without reformatting unrelated
sections. Do not run a formatter that silently replaces these choices with its
defaults. Respect mandatory tooling constraints; where formatting is
configurable, prefer these rules. Readability and useful grouping take
precedence over rigid column limits, excessive alignment, or blanket
one-argument-per-line formatting.
