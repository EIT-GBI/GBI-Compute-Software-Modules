# Source this file to add local module and Lua deployment to PATH.

# Utilities and batteries-included Lua
#______________________________________________________________________________
set -l _path_to_add '/Users/johannes.blaschke/Developer/cli-util-mods/opt/bin'

if not contains -- $_path_to_add $PATH
    set -gx PATH $_path_to_add $PATH
end

set -e _path_to_add
#------------------------------------------------------------------------------


# LMod install + MODULEPATH
#______________________________________________________________________________
source '/Users/johannes.blaschke/Developer/cli-util-mods/opt/lmod/lmod/init/fish'
ml use '/Users/johannes.blaschke/Developer/cli-util-mods/test/usr/modules'
#------------------------------------------------------------------------------
