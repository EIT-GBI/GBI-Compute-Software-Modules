help([[
The Rust Programming Language (built from source)
A language empowering everyone to build reliable and efficient software.

Unlike the default `rust` module this toolchain was not downloaded through
rustup: it is the official source tarball, bootstrapped with x.py and linked
against the source-built `llvm` module. Use it when you want a compiler whose
sources you have, or a version rustup will not give you (beta, nightly).

There is no rustup here -- `rustup`, `rustup toolchain` and friends do not
exist, and `cargo`/`rustc` are the plain binaries. Load the rustup-installed
`rust/stable` instead if you want the managed toolchain.
https://www.rust-lang.org/
]])

whatis("Name: Rust")
whatis("Version: {{{INSTALL_VERSION}}} (compiled from source)")
whatis("URL: https://www.rust-lang.org/")

prepend_path("PATH", "{{{RUST_PATH}}}")
prepend_path("LD_LIBRARY_PATH", "{{{RUST_LD_LIBRARY_PATH}}}")
