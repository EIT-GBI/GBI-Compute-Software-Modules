#!/usr/bin/env bash
set -euo pipefail

# General purpose bash runner script: execute the first arg in an environment
# with __PREFIX__ set to the parent directory of this script. Used to tie
# together bash workflows by enabling relative paths with respect to
# __PREFIX__.


# === framework for relative paths in bash scripts =======================
#
# CONTRACT: every path produced here is PHYSICAL — all symlinks resolved, `..`
# resolved by traversal, never lexically. Containment tests elsewhere must
# therefore only ever compare canonical-to-canonical.
#
# Hard deps: bash >= 3.2 (macOS default ok), POSIX dirname/basename, readlink
# with NO flags (universal, incl. pre-12.3 macOS). realpath / readlink -f are
# used opportunistically, but not required.

# --- physical directory containing a path (builtins only) ---------------
__phys_dir() {
    # CDPATH='' : an inherited CDPATH can redirect `cd` AND print to stdout
    ( CDPATH='' cd -P -- "$(dirname -- "$1")" && pwd -P )
}

# --- physical directory of a script --------------------------------------
# Call from the TOP LEVEL of the script to locate, passing its own source:
#
#     __PREFIX__=$(__script_dir "${BASH_SOURCE[0]:-$0}") || exit 1
#     export __PREFIX__
#
# (Pass it in: inside a function, BASH_SOURCE[0] names the file where the
# FUNCTION is defined — this library — not the caller.)
__script_dir() {
    local src=$1 dir hops=0
    while [ -h "$src" ]; do                          # resolve file-level chain
        dir=$(__phys_dir "$src")        || return 1
        src=$(readlink -- "$src")       || return 1
        [[ $src != /* ]] && src=$dir/$src            # relative link targets
        if (( ++hops > 40 )); then                   # realpath(3) gives ELOOP;
            printf '__script_dir: symlink loop at %s\n' "$1" >&2
            return 1                             # a bare loop spins forever
        fi
    done
    __phys_dir "$src"
}

# --- general-purpose physical canonicalizer ------------------------------
# For EXISTING paths (our use case: the script and resolved binaries always
# exist; this sidesteps the -e/-E divergence between implementations).
__realpath() {
    [ $# -eq 1 ] || { printf 'usage: __realpath <path>\n' >&2; return 2; }
    # Flag-less realpath = physical semantics on GNU, BSD, macOS 13+, busybox,
    # and uutils alike. The flags are where portability dies.
    if command -v realpath >/dev/null 2>&1; then
        realpath -- "$1"; return
    fi
    if command -v grealpath >/dev/null 2>&1; then   # Homebrew coreutils
        grealpath -- "$1"; return
    fi
    if readlink -f -- / >/dev/null 2>&1; then       # macOS >= 12.3
        readlink -f -- "$1"; return
    fi
    # Pure-shell fallback — same physical semantics as the branches above.
    local p=$1 dir hops=0
    if [ -d "$p" ]; then ( CDPATH='' cd -P -- "$p" && pwd -P ); return; fi
    while [ -h "$p" ]; do
        dir=$(__phys_dir "$p")    || return 1
        p=$(readlink -- "$p")     || return 1
        [[ $p != /* ]] && p=$dir/$p
        (( ++hops > 40 )) && { printf '__realpath: symlink loop\n' >&2; return 1; }
    done
    dir=$(__phys_dir "$p") || return 1
    printf '%s/%s\n' "${dir%/}" "$(basename -- "$p")"   # %/: avoid '//x' at root
}

export -f __phys_dir
export -f __script_dir
export -f __realpath

# ========================================================================


# Get the current directory => __PREFIX__
__PREFIX__=$(__script_dir "${BASH_SOURCE[0]}") || return 1
export __PREFIX__

# Get the script to run
script="${__PREFIX__}/$1"
shift

# Execute the script with remaining arguments
exec "$script" "$@"
