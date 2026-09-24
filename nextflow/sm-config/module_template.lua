help([[
Nextflow
A workflow system for data-driven computational pipelines: write pipelines
once, run them locally, on Slurm, in containers or in the cloud.
This is the standalone distribution: core dependencies are bundled, plugins
are still fetched on first use (into $NXF_HOME, default ~/.nextflow).
https://www.nextflow.io/
]])

whatis("Name: nextflow")
whatis("Version: {{{INSTALL_VERSION}}}")
whatis("URL: https://www.nextflow.io/")

-- nextflow runs on any Java 17 through 26 (upstream's documented range; the
-- trailing `<` makes the upper bound exclusive). A range rather than a pin so
-- that an already-loaded in-range java is reused instead of swapped out, and
-- so java patch releases don't require re-rendering this module. Re-check the
-- bounds when bumping the nextflow version -- 25.04 dropped Java <17.
depends_on(between("java", "17", "27<"))
prepend_path("PATH", "{{{PATH}}}/bin")
