SOURCE="${SOURCE_PREFIX}/${SOURCE_NAME}.tar.gz"

echo "Downloading ${SOURCE}"

curl --output downloaded.tar.gz -L ${SOURCE}
mkdir -p downloaded
tar xf downloaded.tar.gz -C downloaded --strip-components=1

echo "Building ${SOURCE_NAME}"

cd downloaded

./bootstrap --prefix=${INSTALL_DIR}/${INSTALL_VERSION_VARIANT} --parallel=${N_PROCS}
make -j ${N_PROCS}
make install
