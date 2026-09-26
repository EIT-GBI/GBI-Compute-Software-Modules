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

Use --pack tar or --pack gzip to make a portable archive. Restore with copy.
Use --prefect / Python prefect=True for managed Object Storage transfers.
Prefect archives support --pack, --pack-small, --chunk-size and exclusions with
updated site broker/flows. Restores detect archives/chunks automatically; use
--job-size small for a smaller restore reservation. Plain requests remain
compatible with older deployments; unsupported requested options fail closed.
Object Storage source removal (--delete-source) is unavailable with Prefect.
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
