# The standalone `-dist` release is one platform-independent executable (a
# launcher script with the nextflow jar and its core dependencies appended),
# so there is no OS/arch mapping and nothing to unpack. It still needs a JVM
# at runtime -- the module depends on the java module for that.
SOURCE="${SOURCE_PREFIX}/v${VERSION}/nextflow-${VERSION}-dist"

echo "Downloading ${SOURCE}"

mkdir -p downloaded/bin
curl --fail --output downloaded/bin/nextflow -L ${SOURCE}
chmod +x downloaded/bin/nextflow
