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

# `tar` shells out to `xz` for the deb payloads. Login nodes may not have it
# even though compute nodes do, so say which one to use rather than letting
# tar fail with "Cannot exec".
if ! command -v xz >/dev/null
then
    echo "apptainer needs 'xz' to unpack the deb payload, and it is not on PATH."
    echo "Run this install from a compute node."
    exit 1
fi

# A .deb is an `ar` archive, and the GBI nodes have neither `ar` nor the
# rpm2cpio/cpio pair that upstream's tools/install-unprivileged.sh requires --
# hence this hand-rolled reader. The format is simple: an 8-byte magic, then
# per member a 60-byte header (name[16] mtime[12] uid[6] gid[6] mode[8]
# size[10] magic[2]) followed by `size` bytes padded to an even boundary.
#   $1 = .deb file    $2 = directory to unpack data.tar.* into
extract_deb()
{
    local deb="$1" dest="$2"
    local offset=8 header name size

    while :
    do
        header=$(tail -c "+$((offset + 1))" "${deb}" | head -c 60)
        if [[ -z ${header// } ]]
        then
            echo "no data.tar.* member found in ${deb}"
            exit 1
        fi

        name=${header:0:16}
        name=${name%% *}
        name=${name%/}
        size=${header:48:10}
        size=$((10#${size// }))

        if [[ $name == data.tar.* ]]
        then
            mkdir -p "${dest}"
            tail -c "+$((offset + 61))" "${deb}" | head -c "${size}" > payload.tar
            tar xf payload.tar -C "${dest}"
            rm -f payload.tar
            return 0
        fi

        offset=$((offset + 60 + size + size % 2))
    done
}

SOURCE="${SOURCE_PREFIX}/apptainer_${VERSION}_amd64.deb"

echo "Downloading ${SOURCE}"
curl --fail --output apptainer.deb -L "${SOURCE}"

extract_deb apptainer.deb downloaded
rm -f apptainer.deb

# The deb is built --prefix=/usr --sysconfdir=/etc, but apptainer relocates by
# taking the parent of its own bin/ as ${prefix} and looking for ${prefix}/etc
# -- it does not carry the split layout across a move. Left as unpacked it goes
# looking for <root>/usr/etc/apptainer/apptainer.conf and dies. Lift usr/* up
# beside etc/ and var/ so bin, libexec, share, etc and var are siblings; this
# is exactly what upstream's tools/install-unprivileged.sh does.
mv downloaded/usr/* downloaded/
rmdir downloaded/usr

# The deb bundles the container helpers but links them dynamically, and three
# of their libraries are genuinely optional on a slim image -- GBI login nodes
# have none of them. Everything else they need (libseccomp, libzstd, liblzma,
# liblz4, libz, libuuid) is present anywhere dpkg is, so only these get
# vendored:
#   libfuse3.so.3       -> squashfuse_ll, fuse-overlayfs, fuse2fs (SIF mounts)
#   liblzo2.so.2        -> squashfuse_ll, mksquashfs        (LZO squashfs)
#   libprotobuf-c.so.1  -> proot                            (--fakeroot fallback)
mkdir -p downloaded/lib
for LIB_URL in "${FUSE3_DEB}" "${LZO2_DEB}" "${PROTOBUF_C_DEB}"
do
    echo "Downloading ${LIB_URL}"
    curl --fail --output lib.deb -L "${LIB_URL}"

    rm -rf libtmp
    extract_deb lib.deb libtmp
    # -a keeps the soname symlink a symlink; find keeps this working whether
    # the deb is usr-merged (./usr/lib/...) or not (./lib/...)
    find libtmp -name 'lib*.so*' -exec cp -a {} downloaded/lib/ \;
    rm -rf libtmp lib.deb
done

# post-conditions: the modulefile puts bin on PATH and lib on LD_LIBRARY_PATH,
# and apptainer finds libexec and etc relative to its own bin/, so fail here
# rather than installing a module that points at nothing. The apptainer.conf
# check is the one that catches a regression in the usr/ lift above.
# -r follows the soname symlinks, so a dangling one is caught too.
test -x downloaded/bin/apptainer
test -x downloaded/libexec/apptainer/bin/starter
test -x downloaded/libexec/apptainer/bin/squashfuse_ll
test -f downloaded/etc/apptainer/apptainer.conf
test -r downloaded/lib/libfuse3.so.3
test -r downloaded/lib/liblzo2.so.2
test -r downloaded/lib/libprotobuf-c.so.1
