set -euo pipefail

curl --output downloaded.tar.{{{ext}}} -L ${SOURCE}
mkdir -p downloaded
tar xvf downloaded.tar.{{{ext}}} -C downloaded --strip-components=1
