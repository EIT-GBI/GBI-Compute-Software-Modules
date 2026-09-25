help([[
GNU Make
Controls the generation of executables and other non-source files of a
program from its source files. This one is built through the `cc` module's
zig shims: it needs nothing from the host and runs on every host of the
fleet. It is plain make, though -- load `cc` as well for a compiler.
https://www.gnu.org/software/make/
]])

whatis("Name: make")
whatis("Version: {{{INSTALL_VERSION}}}")
whatis("URL: https://www.gnu.org/software/make/")

prepend_path("PATH", "{{{PATH}}}/bin")
prepend_path("MANPATH", "{{{PATH}}}/share/man")
