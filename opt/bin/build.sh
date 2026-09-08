#!/usr/bin/env bash
set -euo pipefail

OPTIND=1 # Reset in case getopts has been used previously in the shell.
__MODULE_PATH__=${__PREFIX__}/usr  # Default value for __MODULE_PATH__
__MODE__="default"
__LIBC__="glibc"

while getopts "h?m:bg" opt; do
    case "$opt" in
    h|\\?)
        echo "Usage: $0 [-m module_path] [-b] [-g] <module> [args...]"
        echo "Build harness for simpe-modules. Builds <module> given the following options:"
        echo "If -m is provided, then __MODULE_PATH__ is set to the value of <module_path>"
        echo "If -m is not provided, then __MODULE_PATH__ is set to __PREFIX__/usr"
        echo "If -b is providded, then set __MODE__=build"
        echo "If -b is not providded, then set __MODE__=default"
        echo "If -g is provided then set __LIBC__=musl"
        echo "If -g is not provided then set __LIBC__=glibc"
        echo ""
        echo "Note: inherits __PREFIX__=${__PREFIX__} from run.sh"
        exit 0
        ;;
    m)  __MODULE_PATH__=$OPTARG
        ;;
    b)  __MODE__="build"
        ;;
    g)  __LIBC__="musl"
        ;;
    *)  echo "Error parsing input argument: $opt"
        exit 1
        ;;
    esac
done

shift $((OPTIND-1))

if [[ ${__MODE__} == "build" && ${__LIBC__} != "glibc" ]]
then
    echo "You cannot set -g with -b; these settings are mutually exclusive"
    exit 1
fi

# Export context variable to sub-processes, __PREFIX__,  __realpath, and
# __script_dir inherited from run.sh. __realpath only resolves existing paths,
# so make sure the module path exists before canonicalizing it
mkdir -p ${__MODULE_PATH__}
export __PREFIX__
export __MODULE_PATH__=$(__realpath ${__MODULE_PATH__})
export __MODE__
export __LIBC__

# Get the script to run
script="${__PREFIX__}/$1"
shift

# Get the directory of the called script => __DIR__
export __DIR__=$(__script_dir $script)

# Execute the script with remaining arguments
exec "$script" "$@"
