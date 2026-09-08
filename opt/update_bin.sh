#!/usr/bin/env bash
set -euo pipefail

# Get the absolute path of this script
__get_script_dir() {
    local source="$1"
    echo "$(dirname "$(realpath "$source")")"
}

STSRC="https://gitlab.blaschke.science/nersc/simple-templates/-/raw/main/standalone/simple-templates.ex"
SMSRC="https://gitlab.blaschke.science/nersc/simple-modules/-/raw/main/standalone/simple-modules.ex"

pushd $(__get_script_dir ${BASH_SOURCE[0]})

mkdir -p bin
pushd bin

curl --fail --output simple-templates.ex ${STSRC}
curl --fail --output simple-modules.ex ${SMSRC}

chmod u+x,o+x,g+x simple-templates.ex
chmod u+x,o+x,g+x simple-modules.ex

popd
popd
