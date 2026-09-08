#!/usr/bin/env bash
set -euo pipefail

# Renders `make help`. Run through run.sh (which sets __PREFIX__); the
# Makefile hands over the discovered target lists and current settings via the
# environment: TARGETS, AUX_TARGETS, VARIANTS, VARIANT, MODE, MODULE_PATH.
#
# Everything shown per target is read from the recipe directory itself:
#   <name>/sm-help                          line 1: summary, rest: notes
#   <name>/sm-config[-build]/settings.toml  versions + which modes exist
#   <name>/sm-config-build/install.sh       build deps (`module load` lines)

ROOT=${__PREFIX__:?help.sh must be run through run.sh}
TARGETS=${TARGETS:-}
AUX_TARGETS=${AUX_TARGETS:-}
VARIANTS=${VARIANTS:-gnu musl}
VARIANT=${VARIANT:-gnu}
MODE=${MODE:-}
MODULE_PATH=${MODULE_PATH:-${ROOT}/usr}


# --- per-recipe metadata readers -----------------------------------------

# first line of <target>/sm-help => one-line summary
sm_summary() {
    [ -f "${ROOT}/$1/sm-help" ] && head -n 1 "${ROOT}/$1/sm-help" || true
}

# remaining lines of <target>/sm-help => free-form notes
sm_notes() {
    [ -f "${ROOT}/$1/sm-help" ] && tail -n +2 "${ROOT}/$1/sm-help" || true
}

# the `versions` array of <target>/<mode dir>/settings.toml
sm_versions() {
    local f="${ROOT}/$1/$2/settings.toml"
    [ -f "$f" ] || return 0
    sed -n 's/^[[:space:]]*versions[[:space:]]*=[[:space:]]*\[\(.*\)\].*/\1/p' "$f" \
        | head -n 1 | tr -d '"'
}

# `module load` lines of the build-mode install script, version suffixes
# (e.g. zig/0.16.0) stripped
sm_deps() {
    local f="${ROOT}/$1/sm-config-build/install.sh"
    [ -f "$f" ] || return 0
    sed -n 's/^[[:space:]]*module load[[:space:]]*//p' "$f" \
        | tr ' \t' '\n\n' | sed -e '/^$/d' -e 's,/.*,,' | sort -u \
        | tr '\n' ' ' | sed 's/ $//'
}


# --- tree rendering -------------------------------------------------------

# print_target <target> <is_last: 0|1>
print_target() {
    local t=$1 last=$2
    local summary notes_line versions deps
    local bullets=()

    if [ -f "${ROOT}/${t}/sm-config/settings.toml" ]; then
        versions=$(sm_versions "$t" sm-config)
        bullets+=("default: [${versions}]")
    else
        bullets+=("source build only -- always use MODE=build")
    fi

    if [ -f "${ROOT}/${t}/sm-config-build/settings.toml" ]; then
        versions=$(sm_versions "$t" sm-config-build)
        deps=$(sm_deps "$t")
        if [ -n "$deps" ]; then
            bullets+=("MODE=build: [${versions}], loads modules: ${deps}")
        else
            bullets+=("MODE=build: [${versions}], no module dependencies")
        fi
    else
        bullets+=("binary release only -- MODE=build is not supported")
    fi

    while IFS= read -r notes_line; do
        [ -n "$notes_line" ] && bullets+=("$notes_line")
    done < <(sm_notes "$t")

    local head="├──" cont="│  "
    if [ "$last" = 1 ]; then
        head="└──"
        cont="   "
    fi

    summary=$(sm_summary "$t")
    if [ -n "$summary" ]; then
        echo "${head} ${t} [${summary}]"
    else
        echo "${head} ${t}"
    fi

    local n=${#bullets[@]} i sub
    for (( i = 0; i < n; i++ )); do
        sub="├──"
        [ $(( i + 1 )) -eq "$n" ] && sub="└──"
        echo "${cont}  ${sub} ${bullets[$i]}"
    done
}

# print_tree <space separated target list>
print_tree() {
    # shellcheck disable=SC2086  # word splitting is the point
    set -- $1
    local i=0 n=$#
    for t in "$@"; do
        i=$(( i + 1 ))
        local last=0
        [ "$i" -eq "$n" ] && last=1
        print_target "$t" "$last"
    done
}


# --- the help text --------------------------------------------------------

cat <<EOF
 ----------------- install local modules ---------------------
Sometimes Spack is just too much of a headache -- also how do you
use spack without a local python? -- anyway, this is a collection
of bash a lua scripts to generate a bare-bones set of LMod
modules

Environment variables:
├── VARIANT [must be one of: '${VARIANTS}'; current: '${VARIANT}']
│      └── Specify which glibc variant to use
├── MODULE_PATH [can be any valid path; current: '${MODULE_PATH}']
│      └── Specify where to install local modules to
├── ML_INIT_FILE [default: ${ROOT}/opt/lmod/lmod/init/bash]
│      └── Path of the LMod init file, use ML_INIT= to stop lmod initialization
└── MODE [default '']
       ├── If MODE=build, this will build the module from source
       └── WARNING: VARIANT=musl is not permitted with MODE=build

Available make targets that generate modules [auto-discovered]:
EOF

print_tree "$TARGETS"

cat <<EOF

Opt-in targets [NOT built by 'all', ask for them by name]:
EOF

print_tree "$AUX_TARGETS"

cat <<EOF

Targets are discovered: any directory in the project root holding an
sm-config/ (default) or sm-config-build/ (MODE=build) recipe is a target.
Add sm-help (line 1: summary, rest: notes) to describe it here, and an
sm-opt-in marker file to keep it out of 'make all'.

Auxilliary make targets:
├── help [print this help prompt]
├── check [does the installer need re-running?]
│    ├── compares settings.toml versions against MODULE_PATH
│    ├── honours MODE and VARIANT; set TARGET to check one module
│    └── exits non-zero if anything is missing/partial/stale
├── realclean [deletes ALL installed modules]
│    └── must set MODULE_PATH to the location to be cleaned
├── clean [clean module specified by TARGET]
│    ├── must set MODULE_PATH to the location of the target module
│    └── must set TARGET to the name of the module to be cleaned
├── bootstrap [bootstraps a Lua and LMod install to /opt/lmod]
└── update [updates this project's dependencies]
EOF
