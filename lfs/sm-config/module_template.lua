help([[
Lustre client utilities
lfs is the user-facing Lustre command: `lfs quota -u <user> /mnt/lustre`,
`lfs df -h`, `lfs find`, `lfs getstripe` / `lfs setstripe`, `lfs migrate`.
Also ships lctl, lfs_migrate, llstat and friends. They drive the Lustre kernel
client through ioctls on the mounted filesystem, so they only do anything on a
node with Lustre mounted -- and quota queries for *other* users need root.
https://wiki.whamcloud.com/
]])

whatis("Name: lfs")
whatis("Version: {{{INSTALL_VERSION}}}")
whatis("URL: https://www.lustre.org/")

-- the deb's usr/ tree is the module root: bin/ (lfs, lfs_migrate, llstat),
-- sbin/ (lctl, lnetctl), lib/ (liblustreapi, liblnetconfig + vendored
-- libnl-genl-3), share/man
prepend_path("PATH", "{{{PATH}}}/bin")
prepend_path("PATH", "{{{PATH}}}/sbin")
prepend_path("LD_LIBRARY_PATH", "{{{PATH}}}/lib")
prepend_path("MANPATH", "{{{PATH}}}/share/man")
