module load cmake rust

SOURCE="${SOURCE_PREFIX}/${SOURCE_NAME}"

echo "Downloading ${SOURCE}"

curl --output downloaded.tar.xz -L ${SOURCE}
mkdir -p downloaded/build
tar xf downloaded.tar.xz -C downloaded --strip-components=1

echo "Building ${SOURCE_NAME}"

cd downloaded/build
cmake .. -DCMAKE_INSTALL_PREFIX=${INSTALL_DIR}/${INSTALL_VERSION_VARIANT}
cmake --build .  -j ${N_PROCS}
cmake --install .
