NAME=$(resolve_archive_name "$SOURCE_NAME")

SOURCE="${SOURCE_PREFIX}/download/v${VERSION}/${NAME}"
[[ $VERSION == "latest" ]] && SOURCE="${SOURCE_PREFIX}/latest/download/${NAME}"

echo "Downloading ${SOURCE}"

curl --fail --output downloaded.tar.gz -L ${SOURCE}
mkdir -p downloaded
tar xf downloaded.tar.gz -C downloaded --strip-components=1
