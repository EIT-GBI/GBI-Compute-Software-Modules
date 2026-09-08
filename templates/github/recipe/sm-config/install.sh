# Pick the release asset for this platform: on linux, resolve_archive_name
# appends the gnu/musl suffix matching VARIANT. Adjust the suffixes here if
# upstream names them differently -- see parallel-tar/sm-config for an example
# that resolves glibc baselines too.
NAME=$(resolve_archive_name "$SOURCE_NAME" "-musl" "-gnu" ".tar.{{{ext}}}")

SOURCE="${SOURCE_PREFIX}/download/${TAG}/${NAME}"
[[ ${VERSION} == "latest" ]] && SOURCE="${SOURCE_PREFIX}/latest/download/${NAME}"

echo "Downloading ${SOURCE}"

curl --output downloaded.tar.{{{ext}}} -L ${SOURCE}
mkdir -p downloaded
tar xf downloaded.tar.{{{ext}}} -C downloaded --strip-components=1
