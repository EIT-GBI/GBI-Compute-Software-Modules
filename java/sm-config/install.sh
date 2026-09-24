OS=$(to_lower "$RUNTIME_OS")
ARCH=$(to_lower "$RUNTIME_ARCH")

[[ $ARCH == "arm64" ]] && ARCH="aarch64"
[[ $ARCH == "amd64" ]] && ARCH="x64"
[[ $ARCH == "x86_64" ]] && ARCH="x64"
[[ $OS   == "darwin" ]] && OS="mac"

# Note: Temurin's releases are glibc builds on linux (musl ones exist under
# os=alpine-linux, but GBI's nodes are glibc) -- hence no
# `resolve_archive_name`. The Adoptium API redirects to the GitHub release
# asset; the `+` in the release name (jdk-25.0.4.1+1) must be URL-encoded.
SOURCE="${SOURCE_PREFIX}/jdk-${VERSION//+/%2B}/${OS}/${ARCH}/jdk/hotspot/normal/eclipse"

echo "Downloading ${SOURCE}"

curl --fail --output downloaded.tar.gz -L ${SOURCE}
mkdir -p downloaded
tar xf downloaded.tar.gz -C downloaded --strip-components=1

# the macOS tarball is a .jdk bundle: the actual JAVA_HOME lives in
# Contents/Home -- hoist it so JAVA_HOME is the tree root on every platform
if [[ -d downloaded/Contents/Home ]]
then
    mv downloaded/Contents/Home downloaded.home
    rm -rf downloaded
    mv downloaded.home downloaded
fi
