OS=$(to_lower "$RUNTIME_OS")

[[ $OS == "darwin" ]] && OS="macos"

SOURCE="${SOURCE_PREFIX}/${SOURCE_NAME}-${OS}-${RUNTIME_ARCH}.tar.gz"

echo "Downloading ${SOURCE}"

curl --output downloaded.tar.gz -L ${SOURCE}
mkdir -p downloaded
tar xf downloaded.tar.gz -C downloaded --strip-components=1
