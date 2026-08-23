module load cmake
module load llvm/${LLVM_VERSION}

# WHY THE SOURCE-BUILT LLVM IS MANDATORY HERE
#
# The upstream *binary* LLVM releases cannot build zig, for two independent
# reasons -- both verified against LLVM-21.1.8-macOS-ARM64:
#
#   1. They are built against LLVM's own libc++ in unstable-ABI mode (symbols
#      carry an `abi:un170006` tag), so their archives do not resolve against
#      the platform libc++. Worked around below, but see (2).
#   2. Fatally: they are LTO builds. The members of e.g. libLLVMSupport.a are
#      `LLVM bitcode, wrapper`, not Mach-O objects. CMake's zig2 links them
#      fine via clang, but stage3 is linked by zig itself -- and zig's
#      self-hosted linker rejects every one of them with
#      `error: unknown cpu architecture: 0`. There is no flag that fixes this.
#
# A source-built llvm has neither problem: it uses the system C++ toolchain,
# and LLVM_ENABLE_LTO is off by default, so the archives hold real objects.

# The "regular" zig build: CMake, against the LLVM/clang/lld development
# libraries from the `llvm` module. This produces a *complete* zig -- with the
# LLVM backend, and therefore `zig cc`, `zig ar` and release-mode optimisation.
#
# !! MEMORY WARNING !!
# This is strictly heavier than `make zig-bootstrap MODE=build`, not lighter.
# CMake runs the very same bootstrap stages internally (stage1/zig1.wasm ->
# a ~250MB zig1.c -> a ~220MB zig2.c, each compiled as a single translation
# unit) and *then* links the result against LLVM's static libraries. The big
# C compiles are serial, so NPROCS does not bound the peak -- each one alone
# peaks at tens of GB of RSS.

curl --output zig-src.tar.xz -L \
    ${SOURCE_PREFIX}/${INSTALL_VERSION}/${SOURCE_NAME}-${INSTALL_VERSION}.tar.xz
mkdir -p zig-src
tar xf zig-src.tar.xz -C zig-src --strip-components=1

cd zig-src
mkdir -p build
cd build

# ZIG_STATIC_LLVM: the upstream monolithic LLVM release ships static archives
# (`llvm-config --shared-mode` reports `static`), so ask zig to match. Without
# this zig defaults to looking for shared libLLVM and the configure step fails.
# The upstream *binary* LLVM releases are built against LLVM's own libc++ in
# unstable-ABI mode (symbols carry an `abi:un170006` tag), so their static
# archives will not resolve against the platform libc++ -- linking zig2 fails
# with undefined `std::terminate()` / `operator delete[]`. When that libc++ is
# present in the LLVM tree, link it statically and tell clang++ not to add the
# platform one. A source-built llvm (`make llvm MODE=build`) uses the system
# C++ toolchain and ships no libc++.a, so this correctly stays empty there.
LINKER_FLAGS=""
if [[ -f "${LLVM_ROOT}/lib/libc++.a" ]]
then
    echo "Detected a bundled libc++ in ${LLVM_ROOT} => linking it statically"
    LINKER_FLAGS="-nostdlib++ ${LLVM_ROOT}/lib/libc++.a ${LLVM_ROOT}/lib/libc++abi.a"
fi

cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}" \
    -DCMAKE_PREFIX_PATH="${LLVM_ROOT}" \
    -DCMAKE_EXE_LINKER_FLAGS="${LINKER_FLAGS}" \
    -DZIG_STATIC_LLVM=ON

cmake --build . -j ${NPROCS}
cmake --install .
