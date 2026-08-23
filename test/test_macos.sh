#!/usr/bin/env bash
set -euxo pipefail

cd ${__PREFIX__}

MODULE_PATH=${__PREFIX__}/test/usr
make realclean
make MODULE_PATH=${MODULE_PATH} bootstrap

source ${__PREFIX__}/opt/share/env.sh

mkdir -p ${__PREFIX__}/test/usr

make MODULE_PATH=${__PREFIX__}/test/usr            rust
make MODULE_PATH=${__PREFIX__}/test/usr            cmake
make MODULE_PATH=${__PREFIX__}/test/usr MODE=build cmake
make MODULE_PATH=${__PREFIX__}/test/usr            bat
make MODULE_PATH=${__PREFIX__}/test/usr MODE=build bat
# NO EZA binary releases (outside of homebrew) for macOS
make MODULE_PATH=${__PREFIX__}/test/usr MODE=build eza
make MODULE_PATH=${__PREFIX__}/test/usr            fish
make MODULE_PATH=${__PREFIX__}/test/usr MODE=build fish
make MODULE_PATH=${__PREFIX__}/test/usr            neovim
make MODULE_PATH=${__PREFIX__}/test/usr MODE=build neovim
make MODULE_PATH=${__PREFIX__}/test/usr            nu
make MODULE_PATH=${__PREFIX__}/test/usr MODE=build nu
make MODULE_PATH=${__PREFIX__}/test/usr            uv
make MODULE_PATH=${__PREFIX__}/test/usr MODE=build uv
make MODULE_PATH=${__PREFIX__}/test/usr            zig
# NO NCDU binary releases for macOS -- upstream ships linux static builds only
make MODULE_PATH=${__PREFIX__}/test/usr MODE=build ncdu

# The infrastructure targets are opt-in: llvm alone is a >1GB download (or a
# ~16min source build), and `zig MODE=build` runs for ~2h on top of it.
# Run them with:  TEST_HEAVY=1 ./run.sh test/test_macos.sh
if [[ ${TEST_HEAVY:-0} == 1 ]]
then
    # zig via bootstrap.c -- no dependencies, installs as zig/<ver>-bootstrap
    make MODULE_PATH=${__PREFIX__}/test/usr MODE=build zig-bootstrap

    # NOTE: the *binary* llvm release cannot build zig (LTO archives), so the
    # source build is not optional here -- see README. The same llvm is what
    # `rust MODE=build` links against.
    make MODULE_PATH=${__PREFIX__}/test/usr MODE=build llvm
    make MODULE_PATH=${__PREFIX__}/test/usr MODE=build zig

    # rust from source: needs the llvm above, and a python3 for x.py
    make MODULE_PATH=${__PREFIX__}/test/usr MODE=build rust
fi
