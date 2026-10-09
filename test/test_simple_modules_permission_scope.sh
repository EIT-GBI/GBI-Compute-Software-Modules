#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${__PREFIX:-}" || ! -f "${__PREFIX}/opt/share/env.sh" ]]; then
    echo "run this test through run.sh after make bootstrap" >&2
    exit 2
fi

source "${__PREFIX}/opt/share/env.sh"

tmp=$(mktemp -d "${TMPDIR:-/tmp}/simple-modules-permissions.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

config="${tmp}/config"
software="${tmp}/shared/software"
active="${software}/fixture-1.0"
older="${software}/fixture-0.4.16"
mkdir -p "${config}" "${active}/bin" "${older}" "${software}/unrelated"

cat > "${config}/settings.toml" <<'EOF'
[install]
versions = ["1.0"]
prefix = "{SITE_DESTINATION}"
format = "^fixture%-{INSTALL_VERSION}$"
stage = false
relocate = false

[module]
INSTALL_DIR = "{INSTALL_DIR}"

[post]
clean = false
safe = true
EOF

cat > "${config}/local_settings.toml" <<'EOF'
name = "fixture"
relocate = false
destination = "{SM_ROOT}/shared/software"
modules = "{SM_ROOT}/modules"
EOF

cat > "${config}/module_template.lua" <<'EOF'
-- permission-scope integration fixture
EOF

printf 'active readable file\n' > "${active}/plain.txt"
printf '#!/bin/sh\nexit 0\n' > "${active}/bin/tool"
printf 'older private file\n' > "${older}/private.txt"
printf 'unrelated private file\n' > "${software}/unrelated/private.txt"
chmod 0700 "${active}" "${active}/bin" "${older}" "${software}/unrelated"
chmod 0600 "${active}/plain.txt" "${older}/private.txt" "${software}/unrelated/private.txt"
chmod 0700 "${active}/bin/tool"

"${__PREFIX}/opt/bin/simple-modules.ex" "${config}" --sm-root="${tmp}"

mode_of() {
    case "$(uname -s)" in
        Darwin) stat -f '%Lp' "$1" ;;
        *) stat -c '%a' "$1" ;;
    esac
}

assert_mode() {
    local path=$1 expected=$2 actual
    actual=$(mode_of "$path")
    if [[ "$actual" != "$expected" ]]; then
        echo "${path}: expected mode ${expected}, got ${actual}" >&2
        exit 1
    fi
}

# The selected version becomes accessible; its older and unrelated siblings do not.
assert_mode "${active}" 705
assert_mode "${active}/plain.txt" 604
assert_mode "${active}/bin/tool" 705
assert_mode "${older}" 700
assert_mode "${older}/private.txt" 600
assert_mode "${software}/unrelated" 700
assert_mode "${software}/unrelated/private.txt" 600

echo "simple-modules permissions stay within the installed version"
