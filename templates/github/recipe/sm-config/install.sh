# Pick the release asset for this platform: on linux, resolve_archive_name
# appends the gnu/musl suffix matching VARIANT. Adjust the suffixes here if
# upstream names them differently -- see parallel-tar/sm-config for an example
# that resolves glibc baselines too. `curl --fail` stops at the 404 when a
# platform has no matching asset; if the gap is known upfront, guard early and
# point at a -build recipe instead, like eza/ and ncdu/ do.
NAME=$(resolve_archive_name "$SOURCE_NAME" "-musl" "-gnu" ".tar.{{{ext}}}")

SOURCE="${SOURCE_PREFIX}/download/${TAG}/${NAME}"
[[ ${VERSION} == "latest" ]] && SOURCE="${SOURCE_PREFIX}/latest/download/${NAME}"

echo "Downloading ${SOURCE}"

curl --fail --output downloaded.tar.{{{ext}}} -L ${SOURCE}
mkdir -p downloaded
tar xf downloaded.tar.{{{ext}}} -C downloaded --strip-components=1
