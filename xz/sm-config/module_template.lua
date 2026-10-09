help([[
XZ Utils
General-purpose data compression with the .xz format: `xz`, `unxz`, `xzcat`,
the `lzma` / `unlzma` / `lzcat` links for the legacy .lzma format, `xzdec`,
`lzmainfo`, and the `xzgrep` / `xzdiff` / `xzless` / `xzmore` scripts.
Compression is multi-threaded (`xz -T0` uses every core). Built through the
`cc` module's zig shims with one static liblzma: the tools depend on nothing
but libc and run on every host of the fleet. For builds that want the
library, `lzma.h`, a PIC `liblzma.a` and `liblzma.pc` are installed, and the
pkg-config and CMake search paths point at them -- there is no shared
liblzma here, so no library path is needed at run time.
https://tukaani.org/xz/
]])

whatis("Name: xz")
whatis("Version: {{{INSTALL_VERSION}}}")
whatis("URL: https://tukaani.org/xz/")

prepend_path("PATH", "{{{PATH}}}/bin")
prepend_path("MANPATH", "{{{PATH}}}/share/man")
-- the static liblzma, for builds that look it up through pkg-config or CMake
-- (CMake's FindLibLZMA searches CMAKE_PREFIX_PATH itself, no pkg-config needed)
prepend_path("PKG_CONFIG_PATH", "{{{PATH}}}/lib/pkgconfig")
prepend_path("CMAKE_PREFIX_PATH", "{{{PATH}}}")
