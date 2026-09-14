help([[
Apptainer
Container platform for HPC -- run and build OCI/SIF images without root, using
unprivileged user namespaces. Provides both `apptainer` and `singularity`.
https://apptainer.org/

Needs a node with unprivileged user namespaces enabled, /dev/fuse present, and
libseccomp.so.2 installed. libfuse3 and liblzo2 are vendored with the module.

`--fakeroot` is not available. proot is deliberately not shipped -- apptainer
runs it inside its own build namespace, where it fails on these nodes -- and
the subuid path needs an /etc/subuid entry, which cluster users do not have.
Pulling, running and building ordinary images are unaffected.

Images are cached in ~/.apptainer/cache by default; set APPTAINER_CACHEDIR to
somewhere roomier before pulling large images.
]])

whatis("Name: apptainer")
whatis("Version: {{{INSTALL_VERSION}}}")
whatis("URL: https://apptainer.org/")

-- install.sh lifts the deb's usr/* up to the tree root, because apptainer takes
-- the parent of its own bin/ as ${prefix} and looks for ${prefix}/etc there
prepend_path("PATH", "{{{PATH}}}/bin")
prepend_path("MANPATH", "{{{PATH}}}/share/man")

-- No LD_LIBRARY_PATH here on purpose. The vendored libfuse3 and liblzo2 in
-- lib/ are reached by the wrapper scripts install.sh puts in
-- libexec/apptainer/bin, because apptainer scrubs LD_* from the environment
-- before launching its image drivers -- setting it here would be stripped
-- before squashfuse_ll ever saw it, and would shadow system libraries for
-- every other program in the shell meanwhile. apptainer itself needs only
-- libseccomp, which the host provides.
