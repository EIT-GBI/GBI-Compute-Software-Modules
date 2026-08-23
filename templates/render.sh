#!/usr/bin/env bash
set -euo pipefail

pushd ${__DIR__}

${__PREFIX__}/opt/bin/simple-templates.ex                                      \
    --overwrite "{\"name\":\"$2\",\"version\":\"$3\",\"source\":\"$4\",\"ext\":\"$5\"}" \
    --dir --resource "^.*/render.sh$"                                          \
    $1                                                                         \
    settings.toml                                                              \
    "./rendered/{{name}}"

popd
