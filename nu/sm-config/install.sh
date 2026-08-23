NAME=$(resolve_archive_name "$SOURCE_NAME")
SOURCE="${SOURCE_PREFIX}/${NAME}"

echo "Downloading ${SOURCE}"

curl --output downloaded.tar.gz -L ${SOURCE}
mkdir -p downloaded
tar xf downloaded.tar.gz -C downloaded --strip-components=1
