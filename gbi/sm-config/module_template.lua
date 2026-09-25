help([[
Move data between your HPC filesystems. Copy, independently verify, then remove
verified sources. Alluxio originals stay unless --delete-source is explicit.

  gbi data move SOURCE DESTINATION
  gbi data copy SOURCE DESTINATION
  gbi data status JOB_ID --watch
  gbi data retry TRANSFER_ID
  gbi data roots
  gbi data usage [PATH] --depth N --limit N

Python: from gbi import data; data.copy(source, destination)
data.move() verifies then removes selected filesystem originals; both calls wait.
]])
whatis("Name: gbi")
whatis("Version: {{{INSTALL_VERSION}}}")
depends_on("rclone/1.75.1")
-- The packaged Lustre client also needs its module's shared-library paths.
if isAvail("lfs/2.16.1") then
    depends_on("lfs/2.16.1")
end
prepend_path("PATH", "{{{PATH}}}/bin")
prepend_path("PYTHONPATH", "{{{PATH}}}/lib")
setenv("GBI_DATA_SITE_CONF", "{{{PATH}}}/etc/site.conf")
