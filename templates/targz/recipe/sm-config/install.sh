# SOURCE is fully resolved by simple-modules (any {INSTALL_VERSION} in it is
# expanded once per version). RUNTIME_OS / RUNTIME_ARCH are exported for
# recipes that need to pick the URL per platform -- see zig/sm-config and
# neovim/sm-config for examples of that kind of install.sh.
echo "Downloading ${SOURCE}"

curl --output downloaded.tar.{{{ext}}} -L ${SOURCE}
mkdir -p downloaded
tar xf downloaded.tar.{{{ext}}} -C downloaded --strip-components=1
