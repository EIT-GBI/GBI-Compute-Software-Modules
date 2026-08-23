help([[
Hyperextensible Vim-based text editor
https://neovim.io/
]])

whatis("Name: neovim")
whatis("Version: {{{INSTALL_VERSION}}}")
whatis("URL: https://neovim.io/")

prepend_path("PATH", "{{{NVIM_PATH}}}/bin")
prepend_path("LD_LIBRARY_PATH", "{{{NVIM_PATH}}}/lib")
