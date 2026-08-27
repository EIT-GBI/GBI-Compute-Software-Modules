OS=$(to_lower "$RUNTIME_OS")
ARCH=$(to_lower "$RUNTIME_ARCH")

[[ $ARCH == "arm64"  ]] && ARCH="aarch64"
[[ $OS   == "darwin" ]] && PKG=".app" && EXT="zip"
[[ $OS   == "linux"  ]] && PKG="-linux-${ARCH}" && EXT="tar.xz"

SOURCE="${SOURCE_PREFIX}/${SOURCE_NAME}${PKG}.${EXT}"
TARGET="downloaded.${EXT}"

echo "Downloading ${SOURCE}"

curl --output ${TARGET} -L ${SOURCE}
mkdir -p downloaded

if [[ $OS == "linux" ]]
then
    mkdir -p bin
    tar xf $TARGET -C downloaded --strip-components=0 -C bin/
elif [[ $OS == "darwin" ]]
then
    unzip $TARGET -d downloaded
    # The MacOS tars just bundle a MacOS app => link to the internal bin
    # directory
    cd downloaded
    ln -s fish-${VERSION}.app/Contents/Resources/base/usr/local/bin
fi

