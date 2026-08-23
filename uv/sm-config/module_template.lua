help([[
UV
An extremely fast Python package and project manager, written in Rust.
https://docs.astral.sh/uv
]])

whatis("Name: uv")
whatis("Version: {{{INSTALL_VERSION}}}")
whatis("URL: https://docs.astral.sh/uv")

prepend_path("PATH", "{{{UV_TOOL_BIN_DIR}}}")
prepend_path("PATH", "{{{UV_TOOL_SCRIPTS_DIR}}}")

setenv("UV_TOOL_BIN_DIR", "{{{UV_TOOL_BIN_DIR}}}")
setenv("UV_TOOL_DIR", "{{{UV_TOOL_DIR}}}")

setenv("UV_CONCURRENT_INSTALLS", "{{{UV_CONCURRENT_INSTALLS}}}")
setenv("UV_CONCURRENT_BUILDS", "{{{UV_CONCURRENT_BUILDS}}}")
