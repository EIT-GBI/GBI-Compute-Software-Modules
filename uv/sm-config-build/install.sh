module load rust

mkdir -p "${INSTALL_ROOT}/bin"
mkdir -p "${INSTALL_ROOT}/opt"

if [[ $VERSION == "stable" ]]
then
    cargo install uv --locked -j ${NPROCS}
else
    cargo install uv --version ${VERSION} --locked -j ${NPROCS}
fi
