# The linux gnu releases come in two glibc baselines (built on glibc 2.32 and
# 2.43) -- pick the newest one this system satisfies. musl and non-linux
# assets carry no glibc version.
GNU_SUFFIX="-gnu.2.32"
if [[ $(to_lower "$RUNTIME_OS") == "linux" ]]
then
    GLIBC_VER=$(ldd --version 2>&1 | head -1 | grep -oE '[0-9]+\.[0-9]+' | tail -1) || true
    if [[ -n ${GLIBC_VER} && $(printf '%s\n%s\n' "2.43" "${GLIBC_VER}" | sort -V | head -1) == "2.43" ]]
    then
        GNU_SUFFIX="-gnu.2.43"
    fi
fi

NAME=$(resolve_archive_name "$SOURCE_NAME" "-musl" "$GNU_SUFFIX")

SOURCE="${SOURCE_PREFIX}/download/v${VERSION}/${NAME}"

echo "Downloading ${SOURCE}"

curl --output downloaded.tar.gz -L ${SOURCE}
mkdir -p downloaded
tar xf downloaded.tar.gz -C downloaded --strip-components=1
