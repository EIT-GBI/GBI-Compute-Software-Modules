module load cmake

# LLVM from source: clang and lld only -- that is exactly what zig's CMake
# build looks for, and building the rest of the monorepo would double an
# already long build.
#
# Measured at ~16 minutes on a 14-core Apple Silicon machine at NPROCS=8, for
# ~3.6GB installed; the build tree itself is much larger, so keep some room.
# NPROCS and NLINK are deliberately below the core count: the failure mode of
# over-parallelising LLVM is the machine swapping itself to death rather than a
# tidy build error. Raise them if you have the headroom.

curl --output llvm-src.tar.xz -L \
    ${SOURCE_PREFIX}/llvmorg-${INSTALL_VERSION}/${SOURCE_NAME}-${INSTALL_VERSION}.src.tar.xz
mkdir -p llvm-src
tar xf llvm-src.tar.xz -C llvm-src --strip-components=1

cd llvm-src

# Defaults matter here: LLVM_TARGETS_TO_BUILD defaults to "all", which is what
# makes the resulting zig a useful cross compiler, and LLVM_BUILD_LLVM_DYLIB
# defaults off, so we get the static archives that -DZIG_STATIC_LLVM=ON wants.
cmake -S llvm -B build \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}" \
    -DLLVM_ENABLE_PROJECTS="clang;lld" \
    -DLLVM_ENABLE_ASSERTIONS=OFF \
    -DLLVM_INCLUDE_TESTS=OFF \
    -DLLVM_INCLUDE_BENCHMARKS=OFF \
    -DLLVM_INCLUDE_EXAMPLES=OFF \
    -DLLVM_ENABLE_ZSTD=OFF \
    -DLLVM_ENABLE_LIBXML2=OFF \
    -DLLVM_ENABLE_TERMINFO=OFF \
    -DLLVM_PARALLEL_LINK_JOBS=${NLINK}

cmake --build build -j ${NPROCS}
cmake --install build
