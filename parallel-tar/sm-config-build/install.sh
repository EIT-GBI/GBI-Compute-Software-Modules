module load rust

# parallel-tar is not on crates.io -- install straight from the GitHub repo
cargo install parallel-tar --locked -j ${NPROCS} \
    --git https://github.com/JBlaschke/parallel-tar --tag v${VERSION}
