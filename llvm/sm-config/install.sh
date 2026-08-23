OS=$(to_lower "$RUNTIME_OS")
ARCH=$(to_lower "$RUNTIME_ARCH")

# upstream's own capitalisation for the monolithic release archives
[[ $OS   == "darwin"  ]] && OS="macOS"
[[ $OS   == "linux"   ]] && OS="Linux"
[[ $ARCH == "arm64"   ]] && ARCH="ARM64"
[[ $ARCH == "aarch64" ]] && ARCH="ARM64"
[[ $ARCH == "amd64"   ]] && ARCH="X64"
[[ $ARCH == "x86_64"  ]] && ARCH="X64"

if [[ $OS == "macOS" ]] && [[ $ARCH == "X64" ]]
then
    echo "LLVM publishes no macOS-X64 archive for ${VERSION}."
    echo "Use 'make llvm MODE=build' to compile from source instead."
    exit 1
fi

SOURCE="${SOURCE_PREFIX}/llvmorg-${VERSION}/${SOURCE_NAME}-${VERSION}-${OS}-${ARCH}.tar.xz"

echo "Downloading ${SOURCE}"
echo "NOTE: this archive is >1GB -- it carries the full clang/lld development"
echo "      libraries that zig's CMake build links against"

curl --output downloaded.tar.xz -L ${SOURCE}
mkdir -p downloaded
tar xf downloaded.tar.xz -C downloaded --strip-components=1
