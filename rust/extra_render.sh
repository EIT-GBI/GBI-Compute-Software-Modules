#!/usr/bin/env bash

# The source-built toolchain has no rustup in it -- rendering a rustup
# modulefile in build mode would point at a RUSTUP_HOME that the build never
# creates. (An earlier default-mode install keeps its own rustup module.)
if [[ ${__MODE__} == "build" ]]
then
    echo "MODE=build => source-built rust ships no rustup, skipping its modulefile"
    exit 0
fi

pushd ${__DIR__}

RUH=${__MODULE_PATH__}/rust/rustup
PTH=${__MODULE_PATH__}/rust/cargo/bin

${__PREFIX__}/opt/bin/simple-templates.ex                                     \
    --overwrite "{\"RUSTUP_HOME\": \"${RUH}\", \"RUST_PATH\": \"${PTH}\"}"    \
    rustup_template/module_template.lua                                       \
    rustup_template/settings.toml                                             \
    ${__MODULE_PATH__}/modules/rustup/latest.lua

popd
