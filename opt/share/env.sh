# Source this file to add local module and Lua deployment to PATH.

# Utilities and batteries-included Lua
#______________________________________________________________________________
_path_to_add=/Users/johannes.blaschke/Developer/cli-util-mods/opt/bin

case ":${PATH:-}:" in
  *":$_path_to_add:"*) ;;
  *) PATH="$_path_to_add${PATH:+:$PATH}" ;;
esac

export PATH
unset _path_to_add
#------------------------------------------------------------------------------


# LMod install + MODULEPATH
#______________________________________________________________________________
source /Users/johannes.blaschke/Developer/cli-util-mods/opt/lmod/lmod/init/bash
ml use /Users/johannes.blaschke/Developer/cli-util-mods/test/usr/modules
#------------------------------------------------------------------------------
