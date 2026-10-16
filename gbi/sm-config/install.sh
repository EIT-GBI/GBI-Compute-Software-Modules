# shellcheck shell=bash
set -euo pipefail
command -v python3 >/dev/null
python3 -c 'import sys; assert sys.version_info >= (3, 9), "gbi requires Python 3.9+"'
[[ $(cat "${GBI_VERSION_FILE}") == "${VERSION}" ]] || {
    echo "gbi: recipe and VERSION disagree" >&2
    exit 1
}
mkdir -p downloaded/etc
cp -R "${GBI_SOURCE_DIR}/." downloaded/
chmod +x downloaded/bin/gbi
PYTHONPATH=downloaded/lib python3 -B -c '
import os
from pathlib import Path
from gbi_data.storage import DEFAULTS, Site
config_file = os.environ.get("GBI_SITE_CONFIG_FILE")
values = Site(config_file).values if config_file else DEFAULTS.copy()
values.update({key: os.environ["GBI_SITE_" + key.upper()]
               for key in DEFAULTS if "GBI_SITE_" + key.upper() in os.environ})
if any("\n" in value or "\r" in value for value in values.values()):
    raise ValueError("site configuration values must be single lines")
output = Path("downloaded/etc/site.conf")
output.write_text("".join(f"{key} = {value}\n" for key, value in values.items()))
Site(output)
'
downloaded/bin/gbi --version
