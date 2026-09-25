help([[
cc: C and C++ compiler front ends over zig
Puts cc, gcc, c++, g++, ar, ranlib, objcopy (and, on linux, ld) on PATH --
each a two-line shim around the matching `zig` subcommand -- and exports CC,
CXX, AR and RANLIB. Every shim passes one fixed target, printed by
`cc-target`: on linux that pins the fleet's oldest glibc, so binaries built
here run on every host; on macOS it means no SDK is needed. zig carries the
libc headers and stubs itself, so nothing from build-essential is required,
not even libc6-dev.

What this is not: gcc. `cc --version` says clang, and `c++` links LLVM's
libc++ statically -- fine for self-contained programs and for Python
extensions (a C ABI boundary), wrong for code that must link against host
C++ libraries built with libstdc++; unload this module and use the system
compiler for those. Host headers and libraries are not searched (the target
is explicit, so zig is hermetic) -- pass -I/-L to reach them. The first C++
compile per user and target builds libc++ into ~/.cache/zig and prints a
wall of harmless libc++ warnings while doing so, once.
https://ziglang.org/
]])

whatis("Name: cc")
whatis("Version: {{{INSTALL_VERSION}}}")
whatis("URL: https://ziglang.org/")

-- one compiler front end at a time: a gcc-based cc/<version>-gnu can share
-- the family later and swap this one out
family("compiler")
depends_on("zig/{{{INSTALL_VERSION}}}")

prepend_path("PATH", "{{{PATH}}}/bin")
setenv("CC",     "{{{PATH}}}/bin/cc")
setenv("CXX",    "{{{PATH}}}/bin/c++")
setenv("AR",     "{{{PATH}}}/bin/ar")
setenv("RANLIB", "{{{PATH}}}/bin/ranlib")
-- Rust's cc crate: there is no libstdc++ behind these shims, link libc++
setenv("CXXSTDLIB", "c++")
