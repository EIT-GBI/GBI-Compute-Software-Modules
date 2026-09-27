help([[
Move data between your HPC filesystems. Copy, independently verify, then remove
verified filesystem sources. Object Storage originals are durable and always retained.

  gbi data move SOURCE DESTINATION
  gbi data copy SOURCE DESTINATION
  gbi data status JOB_ID --watch
  gbi data retry TRANSFER_ID
  gbi data roots
  gbi data usage [PATH] --depth N --limit N

Python: from gbi import data; data.copy(source, destination)
data.move() verifies then removes selected filesystem originals; both calls wait.

Use --archive for a directory archive: GBI chooses packing and part sizes.
Ordinary copy/move keeps directly readable files. Restore an archive root with copy.
Use --prefect / Python prefect=True for managed Object Storage transfers.
Archive features require matching site broker/flows.
Restores detect archives/chunks automatically. Plain requests remain
compatible with older deployments; unsupported requested options fail closed.
Use --help-all for expert overrides and older scripted options.
Inside a Slurm job, ordinary CLI and Python calls reuse the current allocation.

Command help: gbi data copy --help
Guide and PDF: https://github.com/EIT-GBI/GBI-Compute-Software-Modules/tree/main/gbi/docs
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
