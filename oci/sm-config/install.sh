module load uv

# Override the tool paths set by the uv module.
export UV_TOOL_DIR="${TOOL_DIR}"
export UV_TOOL_BIN_DIR="${TOOL_BIN_DIR}"

if [[ ${VERSION} == "stable" ]]; then
    uv tool install --reinstall --python "${PYTHON_VERSION}" oci-cli
else
    uv tool install --reinstall --python "${PYTHON_VERSION}" "oci-cli==${VERSION}"
fi

# Local smoke tests; no OCI credentials or cloud requests required.
"${TOOL_BIN_DIR}/oci" --version
"${TOOL_BIN_DIR}/oci" --help > /dev/null
