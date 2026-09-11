# shellcheck shell=bash
# simple-modules runs this with a plain `bash install.sh`, so only the last
# command's status would otherwise be seen -- and this recipe has several
# decisive steps, not just a closing `tar`. Fail on the first one (the gbi
# idiom) and verify the unpacked tree at the end.
#
# Deliberately no `pipefail`: the deb reader below pipes `tail` into `head -c`,
# which exits as soon as it has its bytes and leaves `tail` killed by SIGPIPE.
# Under pipefail that reads as a fatal 141. The pipelines here are only ever
# tail|head, and a failing `tail` yields empty output that the checks catch.
set -eu

OS=$(to_lower "$RUNTIME_OS")
ARCH=$(to_lower "$RUNTIME_ARCH")

# upstream publishes the unprivileged (non-setuid) build as a linux deb and rpm
# only -- no darwin build exists at all, so there is nothing to fall back to
if [[ $OS != "linux" ]]
then
    echo "apptainer ships binary releases for linux only (detected: '${OS}')."
    echo "There is no macOS build upstream; install this on a linux node."
    exit 1
fi

# the GitHub release carries an amd64 deb and an x86_64 rpm, nothing else
if [[ $ARCH != "x86_64" ]]
then
    echo "apptainer's GitHub release ships an amd64 deb only (detected: '${ARCH}')."
    echo "The apptainer PPA has arm64 debs if a node ever needs one."
    exit 1
fi

# `tar` shells out to `xz` for the deb payload. Login nodes may not have it
# even though compute nodes do, so say which one to use rather than letting
# tar fail with "Cannot exec".
if ! command -v xz >/dev/null
then
    echo "apptainer needs 'xz' to unpack the deb payload, and it is not on PATH."
    echo "Run this install from a compute node."
    exit 1
fi

SOURCE="${SOURCE_PREFIX}/apptainer_${VERSION}_amd64.deb"

echo "Downloading ${SOURCE}"

curl --fail --output downloaded.deb -L "${SOURCE}"

# A .deb is an `ar` archive, and the GBI nodes have neither `ar` nor the
# rpm2cpio/cpio pair that upstream's tools/install-unprivileged.sh requires --
# hence this hand-rolled reader. The format is simple: an 8-byte magic, then
# per member a 60-byte header (name[16] mtime[12] uid[6] gid[6] mode[8]
# size[10] magic[2]) followed by `size` bytes padded to an even boundary.
OFFSET=8
while :
do
    HEADER=$(tail -c "+$((OFFSET + 1))" downloaded.deb | head -c 60)
    if [[ -z ${HEADER// } ]]
    then
        echo "no data.tar.* member found in downloaded.deb"
        exit 1
    fi

    NAME=${HEADER:0:16}
    NAME=${NAME%% *}
    NAME=${NAME%/}
    SIZE=${HEADER:48:10}
    SIZE=$((10#${SIZE// }))

    if [[ $NAME == data.tar.* ]]
    then
        tail -c "+$((OFFSET + 61))" downloaded.deb | head -c "${SIZE}" > payload.tar
        break
    fi

    OFFSET=$((OFFSET + 60 + SIZE + SIZE % 2))
done

# Keep the deb's usr/ + etc/ layout intact instead of stripping it. Apptainer
# relocates itself by applying the offset between its compiled BINDIR and
# /proc/self/exe to every other compiled path, so flattening the tree would
# break libexec (starter, squashfuse_ll, mksquashfs) and config lookup.
mkdir -p downloaded
tar xf payload.tar -C downloaded

rm -f downloaded.deb payload.tar

# post-conditions: the modulefile puts usr/bin on PATH and apptainer resolves
# libexec relative to its own binary, so fail here rather than installing a
# module that points at nothing
test -x downloaded/usr/bin/apptainer
test -x downloaded/usr/libexec/apptainer/bin/starter
test -f downloaded/etc/apptainer/apptainer.conf
