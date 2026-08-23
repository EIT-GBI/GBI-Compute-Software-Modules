help([[
The Rust Programming Language Toolchain Installer
A language empowering everyone to build reliable and efficient software.
https://www.rust-lang.org/
]])

whatis("Name: Rustup")
whatis("Version: {{{INSTALL_VERSION}}}")
whatis("URL: https://www.rust-lang.org/")

prepend_path("PATH", "{{{RUST_PATH}}}")
pushenv("RUSTUP_HOME", "{{{RUSTUP_HOME}}}")
