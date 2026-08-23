#!/usr/bin/env bash
set -euo pipefail

pushd ${__DIR__}

mkdir -p ${__MODULE_PATH__}
${__PREFIX__}/opt/bin/simple-modules.ex sm-config --sm-root=${__MODULE_PATH__}

popd
