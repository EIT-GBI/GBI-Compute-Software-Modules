module load cc/${CC_MODULE}
module load make/${MAKE_MODULE}

# zig's cache stays inside the staging directory (cleaned up with it) rather
# than landing in ~/.cache
export ZIG_GLOBAL_CACHE_DIR=$(pwd)/zig-cache

# sha256 of xz-<version>.tar.gz, one entry per version in settings.toml. The
# 5.6.0 and 5.6.1 release tarballs of this very project carried a backdoor
# (CVE-2024-3094) that was not in the git tree, so what gets built here is
# pinned to a digest, and the install stops on anything else. To add a
# version: download the tarball and the .sig next to it, `gpg --verify` the
# pair against Lasse Collin's release key
# (3690 C240 CE51 B467 0D30 AD1C 38EE 757D 6918 4620, published at
# https://tukaani.org/misc/lasse_collin_pubkey.txt), then record the sha256.
case ${VERSION} in
    5.8.4) SHA256="0014c7886930454fe8bd4228665b51af55eeae560ea135c9c4cd33f55b2591d9" ;;
    *)
        echo "xz: no pinned sha256 for xz-${VERSION}.tar.gz -- add it to the table in xz/sm-config/install.sh"
        exit 1
        ;;
esac

SOURCE="${SOURCE_PREFIX}/${SOURCE_NAME}-${VERSION}.tar.gz"

echo "Downloading ${SOURCE}"

curl --fail --output downloaded.tar.gz -L ${SOURCE}
echo "${SHA256}  downloaded.tar.gz" | sha256sum -c -
mkdir -p downloaded
tar xf downloaded.tar.gz -C downloaded --strip-components=1

cd downloaded

# simple-modules runs this script with BASH_ENV pointing at its strict-mode
# prelude (set -Eeuo pipefail), and every non-interactive bash started from
# here inherits it. On Debian-family hosts autoconf picks /bin/bash for
# config.status, which then dies in nounset mode on an autoconf string that
# reads $CONFIG_FILES before assigning it ("CONFIG_FILES: unbound variable").
# Drop it for the children; this shell keeps its options and helpers.
unset BASH_ENV

# CC/AR/RANLIB come from the cc module's environment.
#
# --disable-shared --with-pic: one static liblzma, linked into the tools, so
#   they depend on nothing but libc -- no LD_LIBRARY_PATH to get right, and
#   on linux the glibc floor is all that ties them to a host. The archive is
#   PIC so that builds which embed it in a shared object (a Python _lzma
#   extension, say) can; liblzma.a, lzma.h and liblzma.pc are installed for
#   them.
# --disable-nls: keeps a host libintl out, for the same portability reason
#   (the shims search no host directories anyway, so this only makes the
#   outcome explicit).
# --disable-rpath: nothing to hardcode without shared libraries.
# --disable-dependency-tracking: a one-shot build has no use for the .deps
#   bookkeeping.
./configure --prefix="${INSTALL_PREFIX}" \
    --disable-shared --enable-static --with-pic \
    --disable-nls --disable-rpath --disable-dependency-tracking
make -j ${NPROCS}
make install

#______________________________________________________________________________
# Smoke test: the installed tools report the version built here, and data
# survives a round trip through the threaded encoder -- also through the
# xzcat link and the xzgrep script, which find `xz` on PATH
#
PATH="${INSTALL_PREFIX}/bin:${PATH}"
mkdir -p smoke

# (capture, then grep: a `| grep -q` would close the pipe on the first match
# and turn the writer's next write into a SIGPIPE failure)
xz --version > smoke/version.log
cat smoke/version.log
grep -q "^xz (XZ Utils) ${VERSION}$" smoke/version.log
grep -q "^liblzma ${VERSION}$"       smoke/version.log

# ~6 MB of text ending in a known line: at preset -1 (1 MiB dictionary) the
# threaded encoder splits it into several blocks, so two threads really do
# run. (awk, not seq: BSD seq prints large numbers in %g form, "1e+06".)
awk 'BEGIN { for (i = 1; i <= 500000; i++) print "line " i; print "the last line" }' > smoke/data.txt
xz --keep --threads=2 -1 smoke/data.txt
xz --test smoke/data.txt.xz
xz --list smoke/data.txt.xz > smoke/list.log
unxz --keep --stdout smoke/data.txt.xz > smoke/roundtrip.txt
cmp smoke/data.txt smoke/roundtrip.txt
xzcat smoke/data.txt.xz > smoke/xzcat.txt
cmp smoke/data.txt smoke/xzcat.txt
xzgrep -c '^the last line$' smoke/data.txt.xz > smoke/grep.log
grep -qx '1' smoke/grep.log
echo "xz: smoke test passed"
#------------------------------------------------------------------------------
