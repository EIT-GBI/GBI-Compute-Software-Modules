#!/usr/bin/env bash
set -euo pipefail

# Renders a recipe template into templates/rendered/<name>/ -- a complete
# simple-modules recipe (sm-config/ + sm-help) that can be test-driven in place
# and then promoted to the repo root, where the Makefile discovers it as a
# target.
#
# Run through the harness so __PREFIX__ and __DIR__ are set:
#
#   ./run.sh opt/bin/build.sh -m ./usr templates/render.sh <template> <name> <version> [key=value ...]

if [[ -z ${__PREFIX__:-} || -z ${__DIR__:-} ]]
then
    echo "error: __PREFIX__ / __DIR__ are not set -- run this through the harness:"
    echo "  ./run.sh opt/bin/build.sh -m ./usr templates/render.sh <template> <name> <version> [key=value ...]"
    exit 1
fi

usage()
{
    echo "Usage: templates/render.sh <template> <name> <version> [key=value ...]"
    echo ""
    echo "Renders templates/<template> into templates/rendered/<name>/. <name>"
    echo "becomes the module name, <version> the initial [install] version; the"
    echo "remaining key=value arguments fill the template's parameters."
    echo ""
    echo "Each template documents its parameters -- and the defaults, where one"
    echo "exists -- in templates/<template>/settings.toml. A parameter without a"
    echo "default is required: rendering stops with \"Variable '<key>' needed"
    echo "but not defined\" if it is missing."
    echo ""
    echo "Available templates:"
    for t in "${__DIR__}"/*/settings.toml
    do
        [[ -e ${t} ]] || continue
        echo "  * $(basename "$(dirname "${t}")")"
    done
}

if [[ $# -lt 3 ]]
then
    usage
    exit 1
fi

TEMPLATE=$1
NAME=$2
VERSION=$3
shift 3

if [[ ! -d ${__DIR__}/${TEMPLATE}/recipe || ! -e ${__DIR__}/${TEMPLATE}/settings.toml ]]
then
    echo "error: no such template: '${TEMPLATE}'"
    echo ""
    usage
    exit 1
fi

# <name> becomes a directory name and a make target => keep it simple
if [[ ! ${NAME} =~ ^[A-Za-z0-9_][A-Za-z0-9._-]*$ ]]
then
    echo "error: '${NAME}' is not a valid module name"
    exit 1
fi

# assemble the --overwrite JSON from <name>, <version>, and the key=value
# arguments
json_escape() { local s=${1//\\/\\\\}; printf '%s' "${s//\"/\\\"}"; }

OVERWRITE="{\"name\":\"$(json_escape "${NAME}")\",\"version\":\"$(json_escape "${VERSION}")\""
for kv in "$@"
do
    if [[ ${kv} != *=* ]]
    then
        echo "error: expected key=value, got: '${kv}'"
        exit 1
    fi
    key=${kv%%=*}
    val=${kv#*=}
    if [[ ! ${key} =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]
    then
        echo "error: '${key}' is not a valid parameter name"
        exit 1
    fi
    if [[ ${key} == "name" || ${key} == "version" ]]
    then
        echo "error: pass '${key}' positionally, not as ${key}=..."
        exit 1
    fi
    OVERWRITE+=",\"${key}\":\"$(json_escape "${val}")\""
done
OVERWRITE+="}"

pushd ${__DIR__}

# clear out any previous render so stale files cannot linger
rm -rf "./rendered/${NAME}"

${__PREFIX__}/opt/bin/simple-templates.ex   \
    --overwrite "${OVERWRITE}"              \
    --dir                                   \
    "${TEMPLATE}/recipe"                    \
    "${TEMPLATE}/settings.toml"             \
    "./rendered/${NAME}"

popd

echo ""
echo "Rendered: templates/rendered/${NAME}"
echo "Test it:     ./run.sh opt/bin/build.sh -m ./usr opt/bin/render.sh templates/rendered/${NAME}"
echo "Promote it:  mv templates/rendered/${NAME} ./${NAME}    # it is then a make target"
