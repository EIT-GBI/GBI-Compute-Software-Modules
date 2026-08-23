help([[
Zig (bootstrap build)
Zig rebuilt from source with nothing but a C compiler, via zig's own
bootstrap.c -- no LLVM, no CMake, no pre-existing zig.

Zig is written in zig, so building it normally means already having it. This
breaks that cycle. Use it to:
  * port zig to a machine or architecture with no zig binary yet
  * establish a trust anchor -- a toolchain you compiled, not downloaded
  * work on the compiler itself, where the bootstrap stages are the point

It stops at zig's intermediate `zig2`, so it is provenance rather than a
working toolchain -- load `zig/{{{INSTALL_VERSION}}}` or build
`make zig MODE=build` for a compiler you can use.
https://ziglang.org/
]])

whatis("Name: zig")
whatis("Version: {{{INSTALL_VERSION}}} (bootstrap: provenance, not a toolchain)")
whatis("URL: https://ziglang.org/")

prepend_path("PATH", "{{{ZIG_PATH}}}")
