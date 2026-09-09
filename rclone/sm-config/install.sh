OS=$(to_lower "$RUNTIME_OS")
ARCH=$(to_lower "$RUNTIME_ARCH")

[[ $ARCH == "aarch64" ]] && ARCH="arm64"
[[ $ARCH == "x86_64"  ]] && ARCH="amd64"
[[ $OS   == "darwin"  ]] && OS="osx"

# Note: rclone's releases are static Go binaries (no gnu/musl split), with
# go-spelled arches (amd64/arm64) and `osx` for macOS -- shipped as .zip only.
# unzip has no --strip-components, so rename the single top-level directory
# (named like the archive) into place instead.
ARCHIVE="rclone-v${VERSION}-${OS}-${ARCH}"
SOURCE="${SOURCE_PREFIX}/${ARCHIVE}.zip"

echo "Downloading ${SOURCE}"

curl --fail --output downloaded.zip -L ${SOURCE}
unzip -q downloaded.zip
mv "${ARCHIVE}" downloaded
