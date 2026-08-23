module load zig/${ZIG_VERSION}

OS=$(to_lower "$RUNTIME_OS")
ARCH=$(to_lower "$RUNTIME_ARCH")

[[ $ARCH == "arm64" ]] && ARCH="aarch64"
[[ $ARCH == "amd64" ]] && ARCH="x86_64"

DEPS=$(pwd)/deps
mkdir -p ${DEPS}/lib ${DEPS}/include

# keep zig's global cache inside the staging directory, so that it is cleaned
# up along with everything else instead of landing in ~/.cache
export ZIG_GLOBAL_CACHE_DIR=$(pwd)/zig-cache

# By default zig builds for the host, which on macOS means linking against the
# SDK's `.tbd` stubs -- and zig 0.15 rejects those, because they advertise
# `arm64e-macos` while zig asks for `arm64-macos`. Naming an explicit macOS
# target makes zig fall back to its own bundled libSystem instead. Note: no
# arrays here, these are word-split on purpose (macOS still ships bash 3.2).
ZIG_TARGET=""
SDK_INC=""
if [[ $OS == "darwin" ]]
then
    SDK=$(xcrun --show-sdk-path)
    ZIG_TARGET="-target ${ARCH}-macos"
    SDK_INC="-I${SDK}/usr/include"
fi

#______________________________________________________________________________
# libzstd -- built static, straight into ${DEPS}
#
curl --output zstd.tar.gz -L \
    ${ZSTD_PREFIX}/v${ZSTD_VERSION}/zstd-${ZSTD_VERSION}.tar.gz
mkdir -p zstd
tar xf zstd.tar.gz -C zstd --strip-components=1

# `zig cc` is a complete C toolchain, so the zig module is the only compiler
# this recipe needs
make -C zstd/lib -j ${NPROCS} libzstd.a \
    CC="zig cc ${ZIG_TARGET}" AR="zig ar" RANLIB="zig ranlib" \
    ZSTD_LIB_DICTBUILDER=0 ZSTD_LIB_MINIFY=1

cp zstd/lib/libzstd.a "${DEPS}/lib/"
cp zstd/lib/zstd.h zstd/lib/zstd_errors.h "${DEPS}/include/"
#------------------------------------------------------------------------------

#______________________________________________________________________________
# libncursesw -- taken from the system, but macOS needs a nudge
#
if [[ $OS == "darwin" ]]
then
    # macOS's libncurses *is* built with wide-char support, it just isn't
    # shipped under the `ncursesw` name that ncdu asks for. Re-badging a copy
    # of the SDK stub gives zig something to resolve `-lncursesw` against, and
    # the `arm64e` -> `arm64` rewrite is the same target-matching fix as above
    # (a no-op on intel macs, where the copy alone is enough).
    sed 's/arm64e-macos/arm64-macos/g' \
        "${SDK}/usr/lib/libncurses.tbd" > "${DEPS}/lib/libncursesw.tbd"
fi
#------------------------------------------------------------------------------

#______________________________________________________________________________
# ncdu itself
#
curl --output ncdu.tar.gz -L \
    ${SOURCE_PREFIX}/${SOURCE_NAME}-${INSTALL_VERSION}.tar.gz
mkdir -p ncdu-src
tar xf ncdu.tar.gz -C ncdu-src --strip-components=1

cd ncdu-src

# NOTE: this deliberately skips upstream's `zig build` / Makefile. `zig build`
# compiles its build runner for the *host*, which fails on macOS for the same
# .tbd reason as above -- and unlike the compile itself, that can't be
# retargeted. Driving `build-exe` directly is what upstream's own static
# release target does anyway.
zig build-exe ${ZIG_TARGET} -OReleaseFast -fstrip -femit-bin=ncdu \
    -lc -lncursesw -lzstd \
    ${SDK_INC} -I"${DEPS}/include" -L"${DEPS}/lib" \
    src/main.zig

mkdir -p "${INSTALL_PREFIX}/bin" "${INSTALL_PREFIX}/share/man/man1"
install -m0755 ncdu "${INSTALL_PREFIX}/bin/ncdu"
install -m0644 ncdu.1 "${INSTALL_PREFIX}/share/man/man1/ncdu.1"
#------------------------------------------------------------------------------
