module load llvm/${LLVM_VERSION}

# Rust from source, the way the distributions build it: the official source
# tarball, bootstrapped by `x.py`, linked against an external LLVM rather than
# the ~1GB copy of llvm-project that the tarball carries in src/llvm-project.
#
# WHY THE SOURCE-BUILT LLVM
#
# rustc links LLVM's static archives into librustc_driver, so it hits the same
# two problems with the upstream *binary* LLVM releases that zig does (see
# zig/sm-config-build/install.sh, where both were verified against
# LLVM-21.1.8-macOS-ARM64): those releases are LTO builds whose archive members
# are bitcode rather than native objects, and they are built against LLVM's own
# libc++ in unstable-ABI mode. Building against the in-tree llvm-project
# instead would work, but costs another full LLVM build on top of this one.
#
# TRUST
#
# Rust is written in Rust, so this is not a bootstrap from nothing -- there is
# no `bootstrap.c` equivalent to reach for. By default `x.py` downloads the
# stage0 compiler pinned in src/stage0 (previous release, sha256 recorded in
# the source tree) and builds this compiler with it. What you get out of it is
# a toolchain whose *sources* you have, built by a compiler whose identity is
# pinned by the sources -- rather than a binary rustup handed you. Set
# STAGE0_ROOT to bootstrap from a rustc you already trust instead.

# `./configure` is a shim over src/bootstrap/configure.py, and x.py itself is
# python -- there is no way around an interpreter here. Note that the `x`
# wrapper would also accept `uv run`, but `./configure` does not, so insist on
# a real python3.
PYTHON=$(command -v python3 || true)
if [[ -z ${PYTHON} ]]
then
    echo "ERROR: building rust needs a python3 on PATH (used by ./configure"
    echo "       and x.py). Any python3 will do -- a system one, or one you"
    echo "       put on PATH yourself."
    exit 1
fi

# static.rust-lang.org serves a source tarball per release, plus rolling
# "beta" and "nightly" ones. The release channel has to match the tarball: left
# alone, bootstrap builds a "dev" compiler, which reports itself as
# <version>-dev and gates features like a nightly.
case ${INSTALL_VERSION} in
    beta|nightly) CHANNEL=${INSTALL_VERSION} ;;
    *)            CHANNEL="stable"           ;;
esac

ARCHIVE="${SOURCE_NAME}-${INSTALL_VERSION}-src.tar.xz"

echo "Downloading ${SOURCE_PREFIX}/${ARCHIVE}"

curl --output ${ARCHIVE}        -L ${SOURCE_PREFIX}/${ARCHIVE}
curl --output ${ARCHIVE}.sha256 -L ${SOURCE_PREFIX}/${ARCHIVE}.sha256

# This catches a truncated or tampered-in-transit download; it is not a
# provenance check -- the checksum comes from the same host as the tarball. The
# real check would be the detached ${ARCHIVE}.asc against the rust signing key,
# which needs a gpg we cannot assume is here.
if command -v sha256sum > /dev/null 2>&1
then
    sha256sum -c ${ARCHIVE}.sha256
else
    shasum -a 256 -c ${ARCHIVE}.sha256
fi

mkdir -p rust-src
tar xf ${ARCHIVE} -C rust-src --strip-components=1

cd rust-src

# The source tarball ships vendor/ and .cargo/config.toml, so bootstrap turns
# vendoring on by itself and the build needs no crates.io access -- the stage0
# download is the only thing it fetches.
CONFIGURE_ARGS=(
    --prefix="${INSTALL_PREFIX}"
    # relative => lands inside the prefix. The default is the absolute "/etc",
    # which we are in no position to write to.
    --sysconfdir=etc
    --release-channel="${CHANNEL}"
    --python="${PYTHON}"
    # sets target.<host-triple>.llvm-config; configure works out the triple,
    # which is why this recipe uses it rather than writing bootstrap.toml
    --llvm-root="${LLVM_ROOT}"
    --enable-extended
    --tools="${TOOLS}"
    --enable-locked-deps
    # rustc and its libs find each other relative to the install tree
    --enable-rpath
    --disable-docs
    # we never run the test suites, and leaving the codegen tests enabled makes
    # bootstrap's sanity check insist on LLVM's FileCheck -- which an external
    # LLVM only ships if it was installed with LLVM_INSTALL_UTILS=ON, and the
    # `llvm` module is not
    --disable-codegen-tests
    --set build.jobs=${NPROCS}
    # bootstrap otherwise warns about every bootstrap.toml change since this
    # release; we are not tracking upstream's config, we regenerate it
    --set change-id=ignore
)

if [[ -n ${STAGE0_ROOT} ]]
then
    echo "Bootstrapping from the local rust in ${STAGE0_ROOT}"
    CONFIGURE_ARGS+=( --enable-local-rust --local-rust-root="${STAGE0_ROOT}" )
fi

./configure "${CONFIGURE_ARGS[@]}"

# x.py install builds stage2 and installs rustc, the standard library and the
# tools listed above into --prefix
${PYTHON} x.py install
