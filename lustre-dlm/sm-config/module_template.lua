help([[
lustre-dlm
Lustre data-lifecycle tooling. `lustre-dlm-usage` collects one owner's Lustre
inventory inside a Slurm job and publishes the cached report that
`gbi data usage` reads. The weekly Prefect storage-usage flow runs it for
every owner; you do not need to run it yourself.
https://github.com/EIT-GBI/Lustre-DLM
]])

whatis("Name: lustre-dlm")
whatis("Version: {{{INSTALL_VERSION}}}")
whatis("URL: https://github.com/EIT-GBI/Lustre-DLM")

prepend_path("PATH", "{{{PATH}}}")
