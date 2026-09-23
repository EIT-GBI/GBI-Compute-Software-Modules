help([[
Java (Eclipse Temurin)
The OpenJDK Java runtime and development kit (`java`, `javac`, `jar`, ...),
built and tested by the Eclipse Adoptium project.
https://adoptium.net/
]])

whatis("Name: java")
whatis("Version: {{{INSTALL_VERSION}}}")
whatis("URL: https://adoptium.net/")

-- JVM-based tools (nextflow, gradle, ...) locate the JDK through JAVA_HOME
setenv("JAVA_HOME", "{{{PATH}}}")
prepend_path("PATH", "{{{PATH}}}/bin")
