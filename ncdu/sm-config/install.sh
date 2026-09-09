OS=$(to_lower "$RUNTIME_OS")
ARCH=$(to_lower "$RUNTIME_ARCH")

[[ $ARCH == "arm64" ]] && ARCH="aarch64"
[[ $ARCH == "amd64" ]] && ARCH="x86_64"

# upstream only publishes the convenient static binaries for linux
if [[ $OS != "linux" ]]
then
    echo "ncdu ships static binary releases for linux only (detected: '${OS}')."
    echo "Use 'make ncdu MODE=build' to compile from source instead."
    exit 1
fi

SOURCE="${SOURCE_PREFIX}/${SOURCE_NAME}-${VERSION}-linux-${ARCH}.tar.gz"

echo "Downloading ${SOURCE}"

curl --fail --output downloaded.tar.gz -L ${SOURCE}
mkdir -p downloaded
# these tarballs hold a bare `ncdu` binary, so there is nothing to strip
tar xf downloaded.tar.gz -C downloaded
