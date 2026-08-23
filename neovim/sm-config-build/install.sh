module load cmake

if [[ ${INSTALL_VERSION} == "master" ]] || [[ ${INSTALL_VERSION} == "stable" ]]
then
    git clone --depth 1 --single-branch --branch ${INSTALL_VERSION} \
        ${GH_PROJECT}/neovim gh
else
    git clone --depth 1 --single-branch --branch v${INSTALL_VERSION} \
        ${GH_PROJECT}/neovim gh
fi

cd gh 

make CMAKE_BUILD_TYPE=RelWithDebInfo CMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}
make install
