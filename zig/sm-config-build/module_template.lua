help([[
Zig (built from source against LLVM)
A general-purpose programming language and toolchain. Unlike the `-bootstrap`
variant this is a complete compiler: it has the LLVM backend, and so provides
`zig cc` / `zig c++`, `zig ar`, and release-mode optimisation.
https://ziglang.org/
]])

whatis("Name: zig")
whatis("Version: {{{INSTALL_VERSION}}} (compiled against LLVM)")
whatis("URL: https://ziglang.org/")

prepend_path("PATH", "{{{ZIG_PATH}}}/bin")
