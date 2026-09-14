#!/usr/bin/env bash
# Standalone apptainer feasibility check -- no make, no Lua, no Lmod.
#
#   apptainer/check.sh fetch    # on a node that has xz (compute node)
#   apptainer/check.sh run      # on the node you want to USE apptainer from
#
# It performs exactly the steps sm-config/install.sh performs, reading the same
# pinned URLs out of sm-config/settings.toml, so a pass here means the recipe
# is sound and any remaining problem is in the build tooling around it.
#
# Why this exists: the module farm's `make bootstrap` compiles Lua and Lmod
# from source, so it needs make plus a C toolchain. GBI compute nodes have
# neither, and the login node has no xz to unpack the deb. This script lets the
# recipe's assumptions be tested on a node where the framework cannot run.
#
# It writes only into $DEST (default ~/apptainer-check) and installs nothing.
set -eu

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CONF="${HERE}/sm-config/settings.toml"
DEST=${DEST:-$HOME/apptainer-check}

[[ -r $CONF ]] || { echo "cannot read ${CONF}"; exit 1; }

# single source of truth: pull the pinned values out of the recipe
toml_str() { sed -n "s/^$1[[:space:]]*=[[:space:]]*\"\([^\"]*\)\".*/\1/p" "$CONF" | head -1; }

VER=${VER:-$(sed -n 's/^versions[[:space:]]*=[[:space:]]*\["\([^"]*\)".*/\1/p' "$CONF" | head -1)}
SOURCE_PREFIX=$(toml_str SOURCE_PREFIX)
SOURCE_PREFIX=${SOURCE_PREFIX//\{INSTALL_VERSION\}/${VER}}
APPTAINER_DEB="${SOURCE_PREFIX}/apptainer_${VER}_amd64.deb"
FUSE3_DEB=$(toml_str FUSE3_DEB)
LZO2_DEB=$(toml_str LZO2_DEB)

for v in VER APPTAINER_DEB FUSE3_DEB LZO2_DEB; do
    [[ -n ${!v} ]] || { echo "could not parse ${v} from ${CONF}"; exit 1; }
done

# A .deb is an `ar` archive: an 8-byte magic, then per member a 60-byte header
# (name[16] mtime[12] uid[6] gid[6] mode[8] size[10] magic[2]) followed by
# `size` bytes padded to an even boundary. No node here has `ar`, so walk it by
# hand -- same reader as sm-config/install.sh. No pipefail, because `head -c`
# exits early and leaves `tail` killed by SIGPIPE.
#   $1 = .deb file    $2 = directory to unpack data.tar.* into
extract_deb() {
    local deb="$1" dest="$2"
    local offset=8 header name size
    while :; do
        header=$(tail -c "+$((offset + 1))" "$deb" | head -c 60)
        [[ -z ${header// } ]] && { echo "no data.tar.* member in ${deb}"; exit 1; }
        name=${header:0:16}; name=${name%% *}; name=${name%/}
        size=${header:48:10}; size=$((10#${size// }))
        if [[ $name == data.tar.* ]]; then
            mkdir -p "$dest"
            tail -c "+$((offset + 61))" "$deb" | head -c "$size" > payload.tar
            tar xf payload.tar -C "$dest"
            rm -f payload.tar
            return 0
        fi
        offset=$((offset + 60 + size + size % 2))
    done
}

fetch() {
    echo "== $(hostname): $(uname -sm), apptainer ${VER} =="
    [[ $(uname -s) == Linux  ]] || { echo "linux only"; exit 1; }
    [[ $(uname -m) == x86_64 ]] || { echo "x86_64 only"; exit 1; }
    command -v xz >/dev/null || { echo "no xz on PATH -- run 'fetch' on a compute node"; exit 1; }

    rm -rf "$DEST"; mkdir -p "$DEST/tree/lib"; cd "$DEST"

    echo "-- ${APPTAINER_DEB##*/}"
    curl --fail -sS -L -o a.deb "$APPTAINER_DEB"
    extract_deb a.deb tree
    rm -f a.deb

    for u in "$FUSE3_DEB" "$LZO2_DEB"; do
        echo "-- ${u##*/}"
        curl --fail -sS -L -o l.deb "$u"
        rm -rf t; extract_deb l.deb t
        find t -name 'lib*.so*' -exec cp -a {} tree/lib/ \;
        rm -rf t l.deb
    done

    test -x tree/usr/bin/apptainer
    test -x tree/usr/libexec/apptainer/bin/squashfuse_ll
    test -r tree/lib/libfuse3.so.3
    test -r tree/lib/liblzo2.so.2

    echo "== unpacked $(du -sh tree | cut -f1) into ${DEST}/tree =="
    echo "now run '$0 run' on the node you want to use apptainer from"
}

run() {
    cd "$DEST" 2>/dev/null || { echo "no ${DEST} -- run '$0 fetch' on a compute node first"; exit 1; }
    export LD_LIBRARY_PATH="${DEST}/tree/lib${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
    local app="${DEST}/tree/usr/bin/apptainer"

    echo "== $(hostname): $(uname -sm) =="
    [[ $(uname -s) == Linux ]] || { echo "linux only"; exit 1; }
    # without ldd the check below would silently report success
    command -v ldd >/dev/null || { echo "no ldd on PATH -- cannot verify linking"; exit 1; }

    echo; echo "== 1. unresolved libraries =="
    local found=0
    for b in tree/usr/bin/apptainer tree/usr/libexec/apptainer/bin/*; do
        [[ -f $b ]] || continue
        local m
        m=$(ldd "$b" 2>/dev/null | awk '/not found/{print $1}' | tr '\n' ' ')
        [[ -n $m ]] && { printf '   %-16s MISSING: %s\n' "${b##*/}" "$m"; found=1; }
    done
    [[ $found == 0 ]] && echo "   all resolved"

    echo; echo "== 2. does it start? =="
    "$app" --version

    echo; echo "== 3. real container run =="
    export APPTAINER_CACHEDIR="${DEST}/cache"
    if "$app" exec docker://alpine:3 /bin/echo "container ran ok"; then
        echo "   PASS -- user namespaces, FUSE mount and squashfuse all work"
    else
        echo "   FAILED -- if that was a registry or network error rather than a"
        echo "   linker or namespace error, the module itself is still fine."
        exit 1
    fi
}

case "${1:-}" in
    fetch) fetch ;;
    run)   run ;;
    *) echo "usage: ${0##*/} fetch|run"; exit 1 ;;
esac
