OS=$(to_lower "$RUNTIME_OS")
ARCH=$(to_lower "$RUNTIME_ARCH")

# lfs and lctl drive the Lustre *kernel* client through ioctls on a mounted
# Lustre filesystem: there is nothing they could do on darwin, and Whamcloud
# builds the Ubuntu client packages for amd64 only.
if [[ $OS != "linux" || $ARCH != "x86_64" ]]
then
    echo "lfs: Whamcloud ships lustre-client-utils for linux/x86_64 only (detected: '${OS}/${ARCH}')."
    echo "There is no MODE=build recipe either: the utilities are useless without the host's Lustre kernel client."
    exit 1
fi

# A .deb is an ar archive around data.tar.<zst|xz>. dpkg-deb -x unpacks it
# without root and exists on every Debian/Ubuntu host; binutils' ar is the
# fallback for other distributions.
extract_deb()
{
    local deb=$1 dest=$2 member
    mkdir -p "${dest}"
    if command -v dpkg-deb >/dev/null 2>&1
    then
        dpkg-deb -x "${deb}" "${dest}"
    elif command -v ar >/dev/null 2>&1
    then
        member=$(ar t "${deb}" | grep '^data\.tar')
        ar p "${deb}" "${member}" > "${dest}.${member}"
        tar xf "${dest}.${member}" -C "${dest}"
        rm -f "${dest}.${member}"
    else
        echo "lfs: unpacking a .deb needs dpkg-deb or ar on PATH"
        exit 1
    fi
}

SOURCE="${SOURCE_PREFIX}/lustre-client-utils_${VERSION}-1_amd64.deb"

echo "Downloading ${SOURCE}"
curl --fail --output lustre-client-utils.deb -L "${SOURCE}"
echo "Downloading ${LIBNL_GENL_SOURCE}"
curl --fail --output libnl-genl.deb -L "${LIBNL_GENL_SOURCE}"

extract_deb lustre-client-utils.deb lustre
extract_deb libnl-genl.deb libnl

# The deb's usr/ tree (bin, sbin, lib, share) becomes the module root. Left out
# on purpose: etc/ (lnet and modprobe config), /sbin/mount.lustre and the
# systemd units -- root-only parts of a host install, meaningless in a module.
mkdir -p downloaded
mv lustre/usr/bin lustre/usr/sbin lustre/usr/lib lustre/usr/share downloaded/
rm -rf downloaded/lib/systemd

# lib/ now holds liblustreapi + liblnetconfig; add libnl-genl-3 next to them
# (-P keeps the .so.200 -> .so.200.x.y symlink a symlink) and carry its licence.
cp -P libnl/usr/lib/x86_64-linux-gnu/libnl-genl-3.so.200* downloaded/lib/
mkdir -p downloaded/share/doc/libnl-genl-3-200
cp libnl/usr/share/doc/libnl-genl-3-200/copyright downloaded/share/doc/libnl-genl-3-200/

# Smoke test that needs no Lustre mount: are all shared libraries present?
# Only a *missing* library fails the install. The Ubuntu 24.04 build wants
# glibc >= 2.38, and GBI installs modules from the Ubuntu 22.04 OOD host, so a
# "version GLIBC_2.38 not found" from ldd or lfs is expected there -- the
# binary is exercised on the slurm nodes, where it is used.
if LD_LIBRARY_PATH="$(pwd)/downloaded/lib" ldd downloaded/bin/lfs | grep ' => not found'
then
    echo "lfs: unresolved shared libraries (see above)"
    exit 1
fi
LD_LIBRARY_PATH="$(pwd)/downloaded/lib" downloaded/bin/lfs --version \
    || echo "lfs: did not run on this build host (older glibc?) -- verify with 'module load lfs; lfs --version' on an Ubuntu 24.04 node"
