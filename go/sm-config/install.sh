OS=$(to_lower "$RUNTIME_OS")
ARCH=$(to_lower "$RUNTIME_ARCH")

[[ $ARCH == "aarch64" ]] && ARCH="arm64"
[[ $ARCH == "x86_64"  ]] && ARCH="amd64"

# Note: go's releases carry no gnu/musl split, so there is no
# `resolve_archive_name` here -- but go spells its arches its own way
# (amd64/arm64), hence the mapping above.
SOURCE="${SOURCE_PREFIX}/go${VERSION}.${OS}-${ARCH}.tar.gz"

echo "Downloading ${SOURCE}"

curl --fail --output downloaded.tar.gz -L ${SOURCE}
mkdir -p downloaded
tar xf downloaded.tar.gz -C downloaded --strip-components=1
