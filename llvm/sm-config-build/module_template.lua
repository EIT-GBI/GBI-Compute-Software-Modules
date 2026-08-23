help([[
LLVM
The LLVM compiler infrastructure -- clang, lld and the LLVM development
libraries. Installed here mainly so that `zig` can be built from source
against it.
https://llvm.org/
]])

whatis("Name: llvm")
whatis("Version: {{{INSTALL_VERSION}}}")
whatis("URL: https://llvm.org/")

prepend_path("PATH", "{{{LLVM_PATH}}}/bin")
prepend_path("CMAKE_PREFIX_PATH", "{{{LLVM_PATH}}}")

setenv("LLVM_ROOT", "{{{LLVM_PATH}}}")

-- NOTE: deliberately NOT touching {DY,}LD_LIBRARY_PATH. This tree ships its
-- own libc++, and putting its lib/ on the dynamic loader path shadows the
-- system one for *every* binary in the shell -- which breaks llvm-config
-- itself ("Symbol not found: ___cxa_guard_release"). The tools carry rpaths,
-- and the libraries zig links against here are static anyway.
