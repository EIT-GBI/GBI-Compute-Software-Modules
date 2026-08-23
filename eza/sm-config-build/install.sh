module load rust

if [[ $VERSION == "stable" ]]
then
    cargo install eza --locked --features vendored-libgit2 -j ${NPROCS}
else
    cargo install eza --version ${VERSION} --locked --features vendored-libgit2 -j ${NPROCS}
fi
