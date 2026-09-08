# zig's own bootstrap.c path. The only system dependency is a C compiler: no
# LLVM, no CMake, and no pre-existing zig.
#
# Cost: ~36 minutes measured on a 14-core Apple Silicon machine, ~5.6GB peak
# RSS. `./bootstrap` translates stage1/zig1.wasm into a ~250MB C file and
# compiles it as a single translation unit, then does the same for the compiler
# proper -- both serial, so -j does not affect them.
#
# This stops at zig's intermediate `zig2`, which upstream recommends against
# taking further for now (the `./zig2 build` step that would produce a usable
# stage3 is deliberately not run). It is therefore a provenance/porting
# artifact, not a toolchain: verified on aarch64-macos, `zig version` and `zig
# cc` panic and `zig build-exe` fails in compiler_rt. `make zig MODE=build`
# builds the real thing via the `llvm` module.

curl --fail --output zig-src.tar.xz -L \
    ${SOURCE_PREFIX}/${INSTALL_VERSION}/${SOURCE_NAME}-${INSTALL_VERSION}.tar.xz
mkdir -p zig-src
tar xf zig-src.tar.xz -C zig-src --strip-components=1

cd zig-src

cc -o bootstrap bootstrap.c
./bootstrap

# a zig installation is just the executable plus a lib/ directory that it finds
# relative to itself
mkdir -p "${INSTALL_PREFIX}"
install -m0755 zig2 "${INSTALL_PREFIX}/zig"
cp -R lib "${INSTALL_PREFIX}/lib"
