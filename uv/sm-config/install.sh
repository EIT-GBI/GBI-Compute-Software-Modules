export UV_INSTALL_DIR=$(pwd)/${UV_INSTALL_DIR_NAME}
export HOME=$(pwd)/${HOME_NAME}

curl -LsSf https://astral.sh/uv/install.sh | sh

# note that the whole `{STAGE_DIR}` gets relocated => that includes
# `UV_INSTALL_DIR_NAME` and `HOME`:
# * `UV_INSTALL_DIR_NAME` => `{SITE_DESTINATION}/{INSTALL_VERSION_VARIANT}/bin`
# * `HOME`                => `{SITE_DESTINATION}/{INSTALL_VERSION_VARIANT}/opt`
