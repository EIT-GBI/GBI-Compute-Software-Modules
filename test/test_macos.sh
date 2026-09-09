#!/usr/bin/env bash
set -euxo pipefail

cd ${__PREFIX__}

# GBI_MODULE_PATH is used by make to locate where modules and software are
# installed to. Needs to be set before `bootstrap` so that generated enviroment
# config scripts know where to point lmod to.
export GBI_MODULE_PATH=${__PREFIX__}/test/usr

make realclean
make bootstrap

source ${__PREFIX__}/opt/share/env.sh

mkdir -p ${__PREFIX__}/test/usr

make rust
make cmake
make bat
# NO EZA binary releases (outside of homebrew) for macOS
make fish
make neovim
make nu
make uv
make zig
# NO NCDU binary releases for macOS -- upstream ships linux static builds only
make parallel-tar
make go

# Buuild tests potentially take more time => leave them as optional. Assumed to
# be true for heavy testing
[[ 1 == ${TEST_HEAVY:-0} ]] && TEST_BUILD=1
if [[ 1 == ${TEST_BUILD:-0} ]]
then
    make MODE=build cmake
    make MODE=build bat
    make MODE=build eza
    make MODE=build fish
    make MODE=build neovim
    make MODE=build nu
    make MODE=build uv
    make MODE=build ncdu
make MODE=build parallel-tar
fi

# The infrastructure targets are opt-in: llvm alone is a >1GB download (or a
# ~16min source build), and `zig MODE=build` runs for ~2h on top of it.
# Run them with:  TEST_HEAVY=1 ./run.sh test/test_macos.sh
if [[ 1 == ${TEST_HEAVY:-0} ]]
then
    # zig via bootstrap.c -- no dependencies, installs as zig/<ver>-bootstrap
    make MODE=build zig-bootstrap

    # NOTE: the *binary* llvm release cannot build zig (LTO archives), so the
    # source build is not optional here -- see README. The same llvm is what
    # `rust MODE=build` links against.
    make MODE=build llvm
    make MODE=build zig

    # rust from source: needs the llvm above, and a python3 for x.py
    make MODE=build rust
fi
