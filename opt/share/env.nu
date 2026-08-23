# Source this file to add local module and Lua deployment to PATH.

# Utilities and batteries-included Lua
#______________________________________________________________________________
let _path_to_add = "/Users/johannes.blaschke/Developer/cli-util-mods/opt/bin"

let _path_list = do {
    let p = ($env.PATH? | default [])

    if (($p | describe) == "string") {
        if $p == "" {
            []
        } else {
            $p | split row (char esep)
        }
    } else {
        $p
    }
}

if not ($_path_to_add in $_path_list) {
    $env.PATH = ([$_path_to_add] ++ $_path_list)
}
#------------------------------------------------------------------------------


# LMod install + MODULEPATH
#______________________________________________________________________________
overlay use "/Users/johannes.blaschke/Developer/cli-util-mods/opt/lmod/lmod/init/nushell"
lmod-module use "/Users/johannes.blaschke/Developer/cli-util-mods/test/usr/modules"
#------------------------------------------------------------------------------
