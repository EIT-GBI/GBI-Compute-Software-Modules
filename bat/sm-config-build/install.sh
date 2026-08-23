module load rust

if [[ $VERSION == "stable" ]]
then
    cargo install bat --locked -j ${NPROCS}
else
    cargo install bat --version ${VERSION} --locked -j ${NPROCS}
fi
