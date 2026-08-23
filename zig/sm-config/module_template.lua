help([[
Zig
A general-purpose programming language and toolchain for maintaining robust,
optimal and reusable software. Also ships `zig cc` / `zig c++`: a self-contained,
cross-compiling C/C++ toolchain.
https://ziglang.org/
]])

whatis("Name: zig")
whatis("Version: {{{INSTALL_VERSION}}}")
whatis("URL: https://ziglang.org/")

-- the `zig` binary lives at the root of the install tree and finds its `lib/`
-- relative to itself => prepend the install directory itself, not `bin`
prepend_path("PATH", "{{{PATH}}}")
