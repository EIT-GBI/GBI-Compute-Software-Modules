module load uv

# `module load uv` points UV_TOOL_DIR / UV_TOOL_BIN_DIR at the uv module's own
# tree -- redirect them so the tool lands in this module's prefix instead
export UV_TOOL_DIR="${TOOL_DIR}"
export UV_TOOL_BIN_DIR="${TOOL_BIN_DIR}"

# `stable` installs whatever the latest PyPI release is; anything else is
# pinned. --reinstall makes re-running the recipe idempotent
if [[ ${VERSION} == "stable" ]]
then
    uv tool install --reinstall {{name}}
else
    uv tool install --reinstall {{name}}==${VERSION}
fi
