help([[
cargo-zigbuild
`cargo zigbuild` is `cargo build` with zig as the linker: rust on hosts that
have no system linker, and binaries pinned to the cc module's glibc floor, so
they run on every host of the fleet. Loading this module also points plain
`cargo build` / `cargo install` for the host target at that linker (through
CARGO_TARGET_<triple>_LINKER), so `cargo install <crate>` and the repo's own
cargo-based recipes work on a bare host too. Crates that compile C or C++
along the way (the cc crate) are pointed at the same patched driver through
CC_<triple> / CXX_<triple>, since they pass clang-style --target= flags that
the cc module's plain shims do not understand.
Load a rust module yourself -- any of them works, rustup or source-built.
https://github.com/rust-cross/cargo-zigbuild
]])

whatis("Name: cargo-zigbuild")
whatis("Version: {{{INSTALL_VERSION}}}")
whatis("URL: https://github.com/rust-cross/cargo-zigbuild")

-- the cc module supplies zig, the pinned target the linker wrapper was
-- generated with, and CC/CXX for build scripts that compile C
depends_on("cc/{{{CC_MODULE}}}")

prepend_path("PATH", "{{{PATH}}}")

-- cargo reads CARGO_TARGET_<TRIPLE>_LINKER for the target it builds, and the
-- cc crate reads CC_<triple> / CXX_<triple> before CC / CXX; spell the host's
-- triple the way rustc does (simple-modules' triple carries no -gnu)
local triple = "{{{RUNTIME_TARGET_TRIPLE}}}"
if triple:match("%-linux$") then
    triple = triple .. "-gnu"
end
local triple_ = (triple:gsub("%-", "_"))
setenv("CARGO_TARGET_" .. triple_:upper() .. "_LINKER", "{{{PATH}}}/zig-linker")
setenv("CC_"  .. triple_, "{{{PATH}}}/zig-cc")
setenv("CXX_" .. triple_, "{{{PATH}}}/zig-c++")
