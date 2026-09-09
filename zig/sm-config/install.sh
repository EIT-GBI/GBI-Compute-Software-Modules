OS=$(to_lower "$RUNTIME_OS")
ARCH=$(to_lower "$RUNTIME_ARCH")

[[ $ARCH == "arm64"  ]] && ARCH="aarch64"
[[ $ARCH == "amd64"  ]] && ARCH="x86_64"
[[ $OS   == "darwin" ]] && OS="macos"

# Note: zig's releases are statically linked and libc-agnostic, so there is no
# gnu/musl split to resolve here -- hence no `resolve_archive_name`. Also note
# that the arch comes *before* the OS in zig's archive names.
SOURCE="${SOURCE_PREFIX}/${SOURCE_NAME}-${ARCH}-${OS}-${VERSION}.tar.xz"

echo "Downloading ${SOURCE}"

curl --fail --output downloaded.tar.xz -L ${SOURCE}
mkdir -p downloaded
tar xf downloaded.tar.xz -C downloaded --strip-components=1
