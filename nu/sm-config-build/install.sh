module load rust

if [[ $VERSION == "stable" ]]
then
    cargo install nu --locked -j ${NPROCS}
else
    cargo install nu --version ${VERSION} --locked -j ${NPROCS}
fi
