help([[
Apptainer
Container platform for HPC -- run and build OCI/SIF images without root, using
unprivileged user namespaces. Provides both `apptainer` and `singularity`.
https://apptainer.org/

Needs a node with unprivileged user namespaces enabled, /dev/fuse present, and
libseccomp.so.2 installed. libfuse3 and liblzo2 are vendored with the module.
`--fakeroot` falls back to the bundled proot: cluster users have no /etc/subuid
entry, so the subuid-mapping path is unavailable.

Images are cached in ~/.apptainer/cache by default; set APPTAINER_CACHEDIR to
somewhere roomier before pulling large images.
]])

whatis("Name: apptainer")
whatis("Version: {{{INSTALL_VERSION}}}")
whatis("URL: https://apptainer.org/")

-- the deb's usr/ + etc/ pair is kept intact so apptainer can relocate itself,
-- so the binaries sit under usr/bin rather than at the tree root
prepend_path("PATH", "{{{PATH}}}/usr/bin")
prepend_path("MANPATH", "{{{PATH}}}/usr/share/man")

-- libfuse3 and liblzo2 are vendored into lib/ because they are absent from the
-- GBI login nodes, and the deb's bundled squashfuse_ll / fuse-overlayfs /
-- mksquashfs are dynamically linked against them
prepend_path("LD_LIBRARY_PATH", "{{{PATH}}}/lib")
