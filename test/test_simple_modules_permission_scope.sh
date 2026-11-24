#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${__PREFIX__:-}" || ! -f "${__PREFIX__}/opt/share/env.sh" ]]; then
    echo "run this test through run.sh after make bootstrap" >&2
    exit 2
fi

source "${__PREFIX__}/opt/share/env.sh"

tmp=$(mktemp -d "${TMPDIR:-/tmp}/simple-modules-permissions.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

# Modulefiles are then written 0600, so they always need o+r adding
umask 077

# chmod shim: records calls on the modules/<name> directory, and with
# SM_TEST_FOREIGN set refuses them the way chmod does for a directory owned by
# another maintainer (no root needed to simulate the shared module tree)
shim="${tmp}/shim"
mkdir -p "${shim}"
cat > "${shim}/chmod" << 'EOF'
#!/usr/bin/env bash
for arg in "$@"; do
    [[ "$arg" -ef "$SM_TEST_MODULE_DIR" ]] || continue
    printf '%s\n' "$*" >> "$SM_TEST_CHMOD_LOG"
    if [[ -n "${SM_TEST_FOREIGN:-}" ]]; then
        printf "chmod: changing permissions of '%s': %s\n" \
            "$arg" "Operation not permitted" >&2
        exit 1
    fi
done
exec "$SM_TEST_CHMOD" "$@"
EOF
chmod 0700 "${shim}/chmod"
export SM_TEST_CHMOD
SM_TEST_CHMOD=$(command -v chmod)

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

# Fixture install tree under $1, with modules/fixture pre-created at mode $2
setup_fixture() {
    local root=$1 module_dir_mode=$2
    local config="${root}/config" software="${root}/shared/software"
    local active="${software}/fixture-1.0" older="${software}/fixture-0.4.16"
    mkdir -p "${config}" "${active}/bin" "${older}" "${software}/unrelated" \
        "${root}/modules/fixture"

    # no install.relocate: it trips an unrelated `setting` typo upstream
    cat > "${config}/settings.toml" << 'EOF'
[install]
versions = ["1.0"]
prefix = "{SITE_DESTINATION}"
format = "^fixture%-{INSTALL_VERSION}$"
stage = false

[module]
INSTALL_DIR = "{INSTALL_DIR}"

[post]
clean = false
safe = true
EOF

    cat > "${config}/local_settings.toml" << 'EOF'
name = "fixture"
relocate = false
destination = "{SM_ROOT}/shared/software"
modules = "{SM_ROOT}/modules"
EOF

    cat > "${config}/module_template.lua" << 'EOF'
-- permission-scope integration fixture
EOF

    printf 'active readable file\n' > "${active}/plain.txt"
    printf '#!/bin/sh\nexit 0\n' > "${active}/bin/tool"
    printf 'older private file\n' > "${older}/private.txt"
    printf 'unrelated private file\n' > "${software}/unrelated/private.txt"
    chmod 0700 "${active}" "${active}/bin" "${older}" "${software}/unrelated"
    chmod 0600 "${active}/plain.txt" "${older}/private.txt" \
        "${software}/unrelated/private.txt"
    chmod 0700 "${active}/bin/tool"
    chmod "${module_dir_mode}" "${root}/modules/fixture"
}

# Run simple-modules on the fixture under $1; with $2 = foreign, its
# modules/fixture directory belongs to another maintainer
run_simple_modules() {
    local root=$1 foreign=${2:-}
    PATH="${shim}:${PATH}" \
        SM_TEST_FOREIGN="${foreign}" \
        SM_TEST_MODULE_DIR="${root}/modules/fixture" \
        SM_TEST_CHMOD_LOG="${root}/chmod.log" \
        "${__PREFIX__}/opt/bin/simple-modules.ex" "${root}/config" \
        --sm-root="${root}"
}

# The selected version becomes accessible; its older and unrelated siblings do
# not
assert_install_tree() {
    local software="$1/shared/software"
    assert_mode "${software}/fixture-1.0" 705
    assert_mode "${software}/fixture-1.0/plain.txt" 604
    assert_mode "${software}/fixture-1.0/bin/tool" 705
    assert_mode "${software}/fixture-0.4.16" 700
    assert_mode "${software}/fixture-0.4.16/private.txt" 600
    assert_mode "${software}/unrelated" 700
    assert_mode "${software}/unrelated/private.txt" 600
}

# Own module directory missing o+rX: it gets fixed
own="${tmp}/own"
setup_fixture "${own}" 0700
run_simple_modules "${own}"
assert_install_tree "${own}"
assert_mode "${own}/modules/fixture" 705
assert_mode "${own}/modules/fixture/1.0.lua" 604
if [[ ! -s "${own}/chmod.log" ]]; then
    echo "modules/fixture was missing o+rX but was not chmod'ed" >&2
    exit 1
fi

# Shared module directory, already o+rx, owned by another maintainer: chmod
# would fail, so it must not be called, and the install still completes
foreign="${tmp}/foreign"
setup_fixture "${foreign}" 0755
run_simple_modules "${foreign}" foreign
assert_install_tree "${foreign}"
assert_mode "${foreign}/modules/fixture" 755
assert_mode "${foreign}/modules/fixture/1.0.lua" 604
if [[ -s "${foreign}/chmod.log" ]]; then
    echo "chmod called on an already o+rx module directory:" >&2
    cat "${foreign}/chmod.log" >&2
    exit 1
fi

echo "simple-modules permissions stay within the installed version"
