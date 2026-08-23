#!/usr/bin/env bash
set -euo pipefail

__DIR__=${__PREFIX__}/$1

echo "-----------------------------------------------------------------------"
echo " Rendering ${__DIR__} with settings:"
echo "    __MODULE_PATH__=${__MODULE_PATH__}"
echo "    __LIBC__=${__LIBC__}"
echo "    __MODE__=${__MODE__}"
echo "-----------------------------------------------------------------------"

pushd ${__DIR__}

mkdir -p ${__MODULE_PATH__}
case ${__MODE__} in
    build)
        if [[ ! -d sm-config-build ]]
        then
            echo "Cannot install using 'build' mode => no such recipe exists"
            exit 1
        fi
        ${__PREFIX__}/opt/bin/simple-modules.ex sm-config-build \
            --sm-root=${__MODULE_PATH__}
        ;;
    *) case ${__LIBC__} in
        musl)
            ${__PREFIX__}/opt/bin/simple-modules.ex sm-config \
                --sm-root=${__MODULE_PATH__} --variant=${__LIBC__}

            ;;
        *)
            ${__PREFIX__}/opt/bin/simple-modules.ex sm-config \
                --sm-root=${__MODULE_PATH__}
            ;;
        esac
        ;;
esac

# hook for extra render recipe

if [[ -e extra_render.sh ]]
then
    export __DIR__
    export __PREFIX__
    export __MODULE_PATH__
    export __MODE__
    export __LIBC__
    exec ./extra_render.sh; 
fi

popd
