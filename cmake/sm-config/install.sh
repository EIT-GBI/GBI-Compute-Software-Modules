OS=$(to_lower "$RUNTIME_OS")
ARCH=$(to_lower "$RUNTIME_ARCH")

[[ $ARCH == "arm64"  ]] && ARCH="aarch64"
[[ $OS   == "darwin" ]] && OS="macos-universal"
[[ $OS   == "linux"  ]] && OS="linux-${ARCH}"

SOURCE="${SOURCE_PREFIX}/${SOURCE_NAME}-${OS}.tar.gz"

echo "Downloading ${SOURCE}"

curl --output downloaded.tar.gz -L ${SOURCE}
mkdir -p downloaded
tar xf downloaded.tar.gz -C downloaded --strip-components=1

# The MacOS tars just bundle a MacOS app => link to the internal bin directory
if [[ $(to_lower "$RUNTIME_OS") == "darwin" ]]
then
    cd downloaded
    ln -s CMake.app/Contents/bin
fi
