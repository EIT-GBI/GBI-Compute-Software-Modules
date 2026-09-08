OS=$(to_lower "$RUNTIME_OS")

# upstream publishes release binaries for linux (and windows) only -- no darwin
if [[ $OS != "linux" ]]
then
    echo "eza ships binary releases for linux only (detected: '${OS}')."
    echo "Use 'make eza MODE=build' to compile from source instead."
    exit 1
fi

NAME=$(resolve_archive_name "$SOURCE_NAME")

SOURCE="${SOURCE_PREFIX}/download/v${VERSION}/${NAME}"
[[ ${VERSION} == "latest" ]] && SOURCE="${SOURCE_PREFIX}/latest/download/${NAME}"

echo "Downloading ${SOURCE}"

curl --fail --output downloaded.tar.gz -L ${SOURCE}
mkdir -p downloaded
tar xf downloaded.tar.gz -C downloaded --strip-components=1
