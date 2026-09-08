module load rust

# `stable` installs whatever the latest crates.io release is; anything else is
# pinned with --version
if [[ ${VERSION} == "stable" ]]
then
    cargo install {{name}} --locked -j ${NPROCS}
else
    cargo install {{name}} --version ${VERSION} --locked -j ${NPROCS}
fi
