#!/usr/bin/env bash
set -euo pipefail

VERSION="$1"
MODULEPATH="$2"
ML_INIT_FILE="$3"

BASE_URL="https://github.com/JBlaschke/lua-regolith/releases/download/${VERSION}"

#______________________________________________________________________________
# Helper Scripts for Lua Regolith Install
#
detect_os() {
    case "$(uname -s)" in
        Linux*)     echo "linux"   ;;
        Darwin*)    echo "macos"   ;;
        FreeBSD*)   echo "freebsd" ;;
        *)          echo "unknown" ;;
    esac
}

detect_arch() {
    case "$(uname -m)" in
        x86_64|amd64)       echo "x86_64" ;;
        aarch64|arm64)      echo "arm64"  ;;
        *)                  echo "unknown" ;;
    esac
}

detect_libc() {
    # Only meaningful on Linux; returns empty string otherwise
    if [ "$(detect_os)" != "linux" ]; then
        echo ""
        return
    fi

    # Check if the system uses musl (e.g. Alpine)
    if command -v ldd >/dev/null 2>&1 && ldd --version 2>&1 | grep -qi musl; then
        echo "musl"
        return
    fi

    # Detect glibc version and choose latest vs legacy
    # "latest" is built on ubuntu:25.10 (glibc 2.42).
    # "legacy" is built on rockylinux:8 (glibc 2.28) for both x86_64 and arm64.
    # Systems with glibc >= 2.42 get "latest"; older systems get "legacy".
    local GLIBC_LATEST_MIN="2.42"

    local glibc_ver
    glibc_ver="$(ldd --version 2>&1 | head -1 | grep -oE '[0-9]+\.[0-9]+' | tail -1)"

    if [ -z "$glibc_ver" ]; then
        echo "Warning: could not detect glibc version, defaulting to glibc-legacy" >&2
        echo "glibc-legacy"
        return
    fi

    # Compare major.minor versions using sort -V (version sort)
    local older
    older="$(printf '%s\n%s\n' "$glibc_ver" "$GLIBC_LATEST_MIN" | sort -V | head -1)"

    if [ "$older" = "$GLIBC_LATEST_MIN" ]; then
        # glibc_ver >= GLIBC_LATEST_MIN
        echo "glibc-latest"
    else
        echo "glibc-legacy"
    fi
}

# ---------------------------------------------------------------------------
# Defaults: auto-detect OS, arch, and libc
# ---------------------------------------------------------------------------
DEFAULT_OS="$(detect_os)"
DEFAULT_ARCH="$(detect_arch)"
DEFAULT_LIBC="$(detect_libc)"
#------------------------------------------------------------------------------

#______________________________________________________________________________
# CLI functions
#
usage() {
    cat <<EOF
Usage: $0 [-n OS] [-a ARCH] [-l LIBC] [-h]

Downloads and installs lua-regolith ${VERSION}, and configures Lmod to 

Options:
  -n OS      Operating system: linux, macos, freebsd
             (detected: ${DEFAULT_OS})
  -a ARCH    Architecture: x86_64, arm64
             (detected: ${DEFAULT_ARCH})
  -l LIBC    C library variant (Linux only): glibc-latest, glibc-legacy, musl
             (detected: ${DEFAULT_LIBC:-N/A})
  -f         Fall back to TACC's Lua-5.1.4.9 bundle (vendored with this repo)
             for Lua backend. Use this if lua-regolith doesn't work (and
             doesn't build).
  -h         Show this help message

All arguments are optional — sensible defaults are auto-detected.
EOF
    exit 0
}

# ---------------------------------------------------------------------------
# Parse arguments (override auto-detected defaults)
# ---------------------------------------------------------------------------
name="${DEFAULT_OS}"
arch="${DEFAULT_ARCH}"
libc="${DEFAULT_LIBC}"
fallback=0

OPTIND=1
while getopts "hn:a:l:f" opt; do
    case "$opt" in
        h)  usage ;;
        n)  name="$OPTARG" ;;
        a)  arch="$OPTARG" ;;
        l)  libc="$OPTARG" ;;
        f)  fallback=1 ;;
        *)  echo "Error: unknown option -${opt}" >&2; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Validate inputs
# ---------------------------------------------------------------------------
case "$name" in
    linux|macos|freebsd) ;;
    *) echo "Error: unsupported OS '${name}' (expected: linux, macos, freebsd)" >&2; exit 1 ;;
esac

case "$arch" in
    x86_64|arm64) ;;
    *) echo "Error: unsupported architecture '${arch}' (expected: x86_64, arm64)" >&2; exit 1 ;;
esac

# Libc suffix is only used (and required) on Linux
libc_suffix=""
if [ "$name" = "linux" ]; then
    case "$libc" in
        glibc-latest|glibc-legacy|musl) libc_suffix="-${libc}" ;;
        "") echo "Error: -l LIBC is required on Linux (glibc-latest, glibc-legacy, musl)" >&2; exit 1 ;;
        *)  echo "Error: unsupported libc '${libc}' (expected: glibc-latest, glibc-legacy, musl)" >&2; exit 1 ;;
    esac
elif [ -n "$libc" ]; then
    echo "Warning: -l LIBC is ignored on ${name} (only applies to Linux)" >&2
fi

# FreeBSD only ships x86_64
if [ "$name" = "freebsd" ] && [ "$arch" != "x86_64" ]; then
    echo "Error: FreeBSD release is only available for x86_64" >&2
    exit 1
fi
#------------------------------------------------------------------------------

#______________________________________________________________________________
# Install Lua backend (bundled with all dependencies required by Lmod)
#
if (( ! fallback )); then
    # -------------------------------------------------------------------------
    # Download
    # -------------------------------------------------------------------------
    FILENAME="lua-regolith-${VERSION}-${name}-${arch}${libc_suffix}.tar.gz"
    URL="${BASE_URL}/${FILENAME}"

    echo "Downloading: ${URL}"

    pushd "${__PREFIX__}/opt" > /dev/null

    curl -fLO "${URL}"
    tar xzf "${FILENAME}"
    rm -f "${FILENAME}"

    popd > /dev/null

    echo "Done — lua-regolith ${VERSION} installed to ${__PREFIX__}/opt/bin"
fi
if (( fallback )); then
    # -------------------------------------------------------------------------
    # Fallback: Install TACC Lua and all of its dependencies
    # -------------------------------------------------------------------------
    pushd ${__PREFIX__}/opt/lmod/lua-5.1.4.9/

    ./configure --prefix=${__PREFIX__}/opt
    make -j 16
    make install

    popd
fi
#------------------------------------------------------------------------------

#______________________________________________________________________________
# Install LMOD and all of its dependencies
#
pushd ${__PREFIX__}/opt/lmod/github.com/TACC/Lmod

./configure --prefix=${__PREFIX__}/opt           \
    --with-lua=${__PREFIX__}/opt/bin/lua         \
    --with-luac=${__PREFIX__}/opt/bin/luac       \
    --with-lua_include=${__PREFIX__}/opt/include \
    --with-fastTCLInterp=no
make install

popd
#------------------------------------------------------------------------------

#______________________________________________________________________________
# Configure LMOD and all of its dependencies
#
mkdir -p $MODULEPATH

# Ensure that paths are clean
__clean_path() {
    local value
    value=${!1}

    while [[ "$value" == *//* ]]; do
        value=${value//\/\//\/}
    done

    printf -v "$1" '%s' "$value"
}

ML_INIT_FILE_BASH="${ML_INIT_FILE}/bash"
ML_INIT_FILE_FISH="${ML_INIT_FILE}/fish"
ML_INIT_FILE_NU="${ML_INIT_FILE}/nushell"

__clean_path ML_INIT_FILE_BASH
__clean_path ML_INIT_FILE_FISH
__clean_path ML_INIT_FILE_NU
__clean_path MODULEPATH

cat > "${__PREFIX__}/opt/share/env.sh" <<EOF
# Source this file to add local module and Lua deployment to PATH.

# Utilities and batteries-included Lua
#______________________________________________________________________________
_path_to_add=${__PREFIX__}/opt/bin

case ":\${PATH:-}:" in
  *":\$_path_to_add:"*) ;;
  *) PATH="\$_path_to_add\${PATH:+:\$PATH}" ;;
esac

export PATH
unset _path_to_add
#------------------------------------------------------------------------------


# LMod install + MODULEPATH
#______________________________________________________________________________
source ${ML_INIT_FILE_BASH}
ml use ${MODULEPATH}
#------------------------------------------------------------------------------
EOF

quote_fish() {
    local s=$1
    s=${s//\\/\\\\}
    s=${s//\'/\\\'}
    printf "'%s'" "$s"
}

cat > "${__PREFIX__}/opt/share/env.fish" <<EOF
# Source this file to add local module and Lua deployment to PATH.

# Utilities and batteries-included Lua
#______________________________________________________________________________
set -l _path_to_add $(quote_fish "${__PREFIX__}/opt/bin")

if not contains -- \$_path_to_add \$PATH
    set -gx PATH \$_path_to_add \$PATH
end

set -e _path_to_add
#------------------------------------------------------------------------------


# LMod install + MODULEPATH
#______________________________________________________________________________
source $(quote_fish "${ML_INIT_FILE_FISH}")
ml use $(quote_fish "${MODULEPATH}")
#------------------------------------------------------------------------------
EOF


quote_nu() {
    local s=$1
    s=${s//\\/\\\\}
    s=${s//\"/\\\"}
    s=${s//$'\n'/\\n}
    s=${s//$'\r'/\\r}
    s=${s//$'\t'/\\t}
    printf '"%s"' "$s"
}

cat > "${__PREFIX__}/opt/share/env.nu" <<EOF
# Source this file to add local module and Lua deployment to PATH.

# Utilities and batteries-included Lua
#______________________________________________________________________________
let _path_to_add = $(quote_nu "${__PREFIX__}/opt/bin")

let _path_list = do {
    let p = (\$env.PATH? | default [])

    if ((\$p | describe) == "string") {
        if \$p == "" {
            []
        } else {
            \$p | split row (char esep)
        }
    } else {
        \$p
    }
}

if not (\$_path_to_add in \$_path_list) {
    \$env.PATH = ([\$_path_to_add] ++ \$_path_list)
}
#------------------------------------------------------------------------------


# LMod install + MODULEPATH
#______________________________________________________________________________
overlay use $(quote_nu "${ML_INIT_FILE_NU}")
lmod-module use $(quote_nu "${MODULEPATH}")
#------------------------------------------------------------------------------
EOF

#------------------------------------------------------------------------------
