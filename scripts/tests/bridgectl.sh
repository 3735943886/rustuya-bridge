#!/usr/bin/env bash
# Run with: bash scripts/tests/bridgectl.sh (no root, network, or systemd needed).
set -euo pipefail
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
test_dir=$(mktemp -d)
trap 'rm -rf -- "$test_dir"' EXIT
export BRIDGECTL_SCRIPT="$script_dir/bridgectl.sh" TEST_DIR="$test_dir"

# Each invocation has fresh globals and a fake installed binary/service. Only
# system-facing operations are replaced; parsing, selection, confirmation,
# version comparison, download ordering, and upgrade/install flow remain real.
cat > "$test_dir/run" <<'MOCK'
set -euo pipefail
source "$BRIDGECTL_SCRIPT"
BIN_PATH="$TEST_DIR/binary"
[[ "${FRESH_INSTALL:-0}" == 1 ]] && BIN_PATH="$TEST_DIR/absent"
require_root() { :; }
require_systemd() { :; }
require_tools() { :; }
detect_target() { echo x86_64-unknown-linux-musl; }
current_version() { echo "${CURRENT_VERSION:-0.4.2}"; }
api_latest_tag() {
    echo "latest $PRERELEASE" >> "$TEST_DIR/events"
    if [[ "$PRERELEASE" == 1 ]]; then echo v0.5.0-rc.1; else echo v0.4.2; fi
}
real_download=$(declare -f download_release)
download_release() {
    echo "download $1" >> "$TEST_DIR/events"
    [[ "${FAIL_DOWNLOAD:-0}" != 1 ]] || return 1
}
install_binary() { echo install >> "$TEST_DIR/events"; }
install_self() { :; }
ensure_user() { :; }
ensure_data_dir() { :; }
write_config_file() { :; }
write_unit() { :; }
print_install_summary() { echo "installed $1"; }
systemctl() {
    echo "systemctl $*" >> "$TEST_DIR/events"
    if [[ "$1" == is-active ]]; then return "${SERVICE_INACTIVE:-0}"; fi
    return 0
}
if [[ "${REAL_DOWNLOAD:-0}" == 1 ]]; then
    eval "$real_download"
    curl() {
        local name="${4##*/}"
        [[ "${FAIL_CURL:-0}" != 1 ]] || return 22
        cp "$TEST_DIR/assets/$name" "$3"
    }
fi
main "$@"
MOCK
: > "$test_dir/binary"
chmod +x "$test_dir/binary"

run() {
    : > "$test_dir/events"
    bash "$test_dir/run" "$@" > "$test_dir/output" 2>&1
}
assert_output() { grep -Fq -- "$1" "$test_dir/output"; }
assert_event() { grep -Fxq -- "$1" "$test_dir/events"; }
assert_no_event() { ! grep -Eq -- "$1" "$test_dir/events"; }
reject() {
    if run "$@"; then echo "Unexpected success: $*" >&2; exit 1; fi
    [[ ! -s "$test_dir/events" ]]
}

run upgrade --version 0.4.1 --yes
assert_output 'Downgraded to 0.4.1.'
assert_event 'download v0.4.1'
assert_no_event '^latest'
expected=$'download v0.4.1\nsystemctl is-active --quiet rustuya-bridge\nsystemctl stop rustuya-bridge\ninstall\nsystemctl start rustuya-bridge'
[[ "$(cat "$test_dir/events")" == "$expected" ]]

run --version=v0.4.1 upgrade -y
assert_event 'download v0.4.1'
FRESH_INSTALL=1 run install --version v0.4.1 --yes
assert_output 'installed v0.4.1'
assert_event 'download v0.4.1'
assert_event 'systemctl enable --now rustuya-bridge'

run upgrade --version=0.4.2 --yes
assert_output 'Already at requested version (0.4.2).'
[[ ! -s "$test_dir/events" ]]

run upgrade --version 0.4.1-rc.1 --yes
assert_event 'download v0.4.1-rc.1'
SERVICE_INACTIVE=1 run upgrade --version 0.4.1 --yes
assert_event install
assert_no_event '^systemctl (stop|start) '

if FAIL_DOWNLOAD=1 run upgrade --version 9.9.9 --yes; then
    echo 'Failed download unexpectedly succeeded' >&2; exit 1
fi
assert_event 'download v9.9.9'
assert_no_event '^(install|systemctl)'

if run upgrade --version 0.4.1; then
    echo 'Noninteractive downgrade bypassed confirmation' >&2; exit 1
fi
assert_output 'Downgrade rustuya-bridge 0.4.2 → 0.4.1?'
assert_no_event '^(download|install|systemctl)'

CURRENT_VERSION=0.4.1 run upgrade --yes
assert_event 'latest 0'
assert_event 'download v0.4.2'
run upgrade --prerelease --yes
assert_event 'latest 1'
assert_event 'download v0.5.0-rc.1'

reject upgrade --version
reject upgrade --version ''
reject upgrade --version=
reject upgrade --version --yes
reject upgrade --version ../0.4.1
reject upgrade --version 0.4
reject upgrade --version 0.4.1 --prerelease
reject upgrade --version 0.4.1 --version 0.4.2
reject status --version 0.4.1
reject remove --version 0.4.1
reject purge --version 0.4.1
reject --version 0.4.1
reject upgrade 0.4.1

# Exercise the real download/extract/checksum path against local release assets.
mkdir "$test_dir/assets" "$test_dir/payload"
printf '#!/bin/sh\n' > "$test_dir/payload/rustuya-bridge"
archive=rustuya-bridge-x86_64-unknown-linux-musl.tar.gz
tar -czf "$test_dir/assets/$archive" -C "$test_dir/payload" rustuya-bridge
(cd "$test_dir/assets" && sha256sum "$archive" > "${archive%.tar.gz}.sha256")
REAL_DOWNLOAD=1 run upgrade --version 0.4.1 --yes
assert_event install

if REAL_DOWNLOAD=1 FAIL_CURL=1 run upgrade --version 0.4.1 --yes; then
    echo 'Missing release unexpectedly succeeded' >&2; exit 1
fi
assert_output 'Could not download v0.4.1'
assert_no_event '^(install|systemctl)'

printf 'corrupted' >> "$test_dir/assets/$archive"
if REAL_DOWNLOAD=1 run upgrade --version 0.4.1 --yes; then
    echo 'Corrupt archive unexpectedly succeeded' >&2; exit 1
fi
assert_output 'sha256 verification failed'
assert_no_event '^(install|systemctl)'

# The documented curl | bash -s entry point must still invoke main.
bash -s -- help < "$BRIDGECTL_SCRIPT" > "$test_dir/output"
assert_output '--version VERSION'
echo 'bridgectl tests passed'
