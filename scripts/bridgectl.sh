#!/usr/bin/env bash
# bridgectl.sh — install / upgrade / remove rustuya-bridge as a systemd service.
#
# Usage:
#   ./bridgectl.sh                       # show status (default)
#   ./bridgectl.sh status
#   sudo ./bridgectl.sh install [--yes]  # downloads + installs + enables + starts
#   sudo ./bridgectl.sh upgrade [--yes]
#   sudo ./bridgectl.sh remove  [--yes]
#   sudo ./bridgectl.sh purge   [--yes]
#   ./bridgectl.sh help
#
# One-line install (Linux, x86_64/aarch64/armv7):
#   curl -fsSL https://raw.githubusercontent.com/3735943886/rustuya-bridge/master/scripts/bridgectl.sh \
#     | sudo bash -s -- install --yes

set -euo pipefail

# ── Configuration ──────────────────────────────────────────────────────────────
GH_OWNER="3735943886"
GH_REPO="rustuya-bridge"
SERVICE="rustuya-bridge"
SVC_USER="rustuya"

INSTALL_DIR="/usr/local/bin"
BIN_PATH="${INSTALL_DIR}/${SERVICE}"
UNIT_PATH="/etc/systemd/system/${SERVICE}.service"
DATA_DIR="/var/lib/rustuya"
CONFIG_FILE="${DATA_DIR}/config.json"
STATE_FILE="${DATA_DIR}/rustuya.json"

# Self-install location so subsequent invocations can use `sudo bridgectl <cmd>`
# instead of re-fetching the script via curl every time.
SELF_PATH="${INSTALL_DIR}/bridgectl"
SELF_URL="https://raw.githubusercontent.com/${GH_OWNER}/${GH_REPO}/master/scripts/bridgectl.sh"

ASSUME_YES=0

# ── Logging helpers ────────────────────────────────────────────────────────────
if [ -t 1 ]; then
    C_RED=$'\033[31m'; C_GREEN=$'\033[32m'; C_YELLOW=$'\033[33m'
    C_BOLD=$'\033[1m'; C_RESET=$'\033[0m'
else
    C_RED=""; C_GREEN=""; C_YELLOW=""; C_BOLD=""; C_RESET=""
fi

log()  { printf '%s\n' "$*"; }
warn() { printf '%s! %s%s\n' "$C_YELLOW" "$*" "$C_RESET" >&2; }
err()  { printf '%s✗ %s%s\n' "$C_RED" "$*" "$C_RESET" >&2; }
ok()   { printf '%s✓ %s%s\n' "$C_GREEN" "$*" "$C_RESET"; }
die()  { err "$*"; exit 1; }

# ── Pre-flight ─────────────────────────────────────────────────────────────────
require_root() {
    if [ "$(id -u)" -ne 0 ]; then
        err "Root privileges required for this command."
        log "Re-run with: ${C_BOLD}sudo bridgectl ${*:-install}${C_RESET}"
        exit 1
    fi
}

require_systemd() {
    [ -d /run/systemd/system ] || die "systemd not detected (this script only supports systemd-based Linux)."
    command -v systemctl >/dev/null 2>&1 || die "systemctl not found in PATH."
}

require_tools() {
    local missing=()
    for cmd in curl tar sha256sum install; do
        command -v "$cmd" >/dev/null 2>&1 || missing+=("$cmd")
    done
    if [ ${#missing[@]} -gt 0 ]; then
        die "Missing required tool(s): ${missing[*]}"
    fi
}

confirm() {
    local prompt="$1"
    if [ "$ASSUME_YES" -eq 1 ]; then return 0; fi
    if [ ! -t 0 ]; then
        die "$prompt — non-interactive shell, pass --yes to proceed."
    fi
    read -r -p "$prompt [y/N] " ans
    [[ "$ans" =~ ^[Yy]$ ]]
}

# ── Detection ──────────────────────────────────────────────────────────────────
detect_target() {
    local arch
    arch="$(uname -m)"
    case "$arch" in
        x86_64|amd64)        echo "x86_64-unknown-linux-musl" ;;
        aarch64|arm64)       echo "aarch64-unknown-linux-musl" ;;
        armv7l|armv6l|armhf) echo "armv7-unknown-linux-musleabihf" ;;
        *) die "Unsupported architecture: $arch" ;;
    esac
}

api_latest_tag() {
    local url="https://api.github.com/repos/${GH_OWNER}/${GH_REPO}/releases/latest"
    local body
    # Stream curl output via grep -m1 trips pipefail when grep closes early.
    # Buffer the response into a variable first, then parse it.
    body="$(curl -fsSL "$url" 2>/dev/null)" || return 1
    if command -v jq >/dev/null 2>&1; then
        printf '%s' "$body" | jq -r '.tag_name'
    else
        printf '%s' "$body" | awk -F'"' '/"tag_name"/ { print $4; exit }'
    fi
}

current_version() {
    if [ -x "$BIN_PATH" ]; then
        "$BIN_PATH" --version 2>/dev/null | awk '{print $NF}' || true
    fi
}

# ── Download + verify ──────────────────────────────────────────────────────────
download_release() {
    local tag="$1" target="$2" tmpdir="$3"
    local base="https://github.com/${GH_OWNER}/${GH_REPO}/releases/download/${tag}"
    local archive="rustuya-bridge-${target}.tar.gz"
    local sumfile="rustuya-bridge-${target}.sha256"

    log "  Downloading ${archive}..."
    curl -fsSL -o "${tmpdir}/${archive}" "${base}/${archive}"
    log "  Downloading ${sumfile}..."
    curl -fsSL -o "${tmpdir}/${sumfile}" "${base}/${sumfile}"

    log "  Verifying sha256..."
    ( cd "$tmpdir" && sha256sum -c "$sumfile" >/dev/null ) \
        || die "sha256 verification failed for ${archive}"

    log "  Extracting..."
    tar -xzf "${tmpdir}/${archive}" -C "$tmpdir" rustuya-bridge
    [ -f "${tmpdir}/rustuya-bridge" ] || die "binary 'rustuya-bridge' not found inside archive"
    chmod +x "${tmpdir}/rustuya-bridge"
}

# ── System integration ─────────────────────────────────────────────────────────
install_binary() { install -m 755 "$1" "$BIN_PATH"; }

ensure_user() {
    if ! id -u "$SVC_USER" >/dev/null 2>&1; then
        useradd --system --no-create-home --shell /usr/sbin/nologin "$SVC_USER"
        log "  Created system user '${SVC_USER}'."
    fi
}

ensure_data_dir() {
    install -d -m 750 -o "$SVC_USER" -g "$SVC_USER" "$DATA_DIR"
}

write_config_file() {
    if [ -f "$CONFIG_FILE" ]; then
        log "  Keeping existing $CONFIG_FILE."
        return
    fi
    # Full config with every supported field populated. Unknown fields (like
    # `_doc`) are ignored by serde_json, so we use it as an inline doc pointer.
    # See: https://github.com/${GH_OWNER}/${GH_REPO}#configuration
    cat > "$CONFIG_FILE" <<EOF
{
  "_doc": "Edit this file then run: sudo systemctl restart ${SERVICE} -- see https://github.com/${GH_OWNER}/${GH_REPO}#configuration",
  "mqtt_broker": "mqtt://localhost:1883",
  "mqtt_user": null,
  "mqtt_password": null,
  "mqtt_root_topic": "rustuya",
  "mqtt_command_topic": "{root}/command",
  "mqtt_event_topic": "{root}/event/{type}/{id}",
  "mqtt_client_id": "{root}-bridge",
  "mqtt_message_topic": "{root}/{level}/{id}",
  "mqtt_payload_template": "{value}",
  "mqtt_scanner_topic": "{root}/scanner",
  "mqtt_retain": false,
  "state_file": "${STATE_FILE}",
  "save_debounce_secs": 30,
  "log_level": "info"
}
EOF
    chmod 640 "$CONFIG_FILE"
    chown root:"$SVC_USER" "$CONFIG_FILE"
    log "  Wrote ${CONFIG_FILE} (full config; default broker: mqtt://localhost:1883)."
}

write_unit() {
    cat > "$UNIT_PATH" <<EOF
[Unit]
Description=Rustuya Bridge - Tuya MQTT bridge
Documentation=https://github.com/${GH_OWNER}/${GH_REPO}
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SVC_USER}
Group=${SVC_USER}
WorkingDirectory=${DATA_DIR}
ExecStart=${BIN_PATH} --config ${CONFIG_FILE}
Restart=on-failure
RestartSec=5

# Hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
ReadWritePaths=${DATA_DIR}

[Install]
WantedBy=multi-user.target
EOF
    chmod 644 "$UNIT_PATH"
    systemctl daemon-reload
    log "  Wrote ${UNIT_PATH}."
}

install_self() {
    log "  Fetching bridgectl helper to ${SELF_PATH}..."
    curl -fsSL "$SELF_URL" -o "${SELF_PATH}.tmp"
    chmod +x "${SELF_PATH}.tmp"
    mv "${SELF_PATH}.tmp" "$SELF_PATH"
}

stop_disable_service() {
    # Stop and disable unconditionally — `systemctl` returns non-zero when the
    # unit is unknown (e.g. cleaned up by an earlier purge), which is fine.
    systemctl stop "$SERVICE" 2>/dev/null || true
    systemctl disable "$SERVICE" 2>/dev/null || true

    # Catch processes that survive the above (orphaned because the unit file
    # was removed while the service was still running, or graceful stop timed
    # out). Send SIGTERM, wait a few seconds, then SIGKILL anything left.
    if pgrep -x rustuya-bridge >/dev/null 2>&1; then
        warn "Found running rustuya-bridge process(es); terminating..."
        pkill -TERM -x rustuya-bridge 2>/dev/null || true
        for _ in 1 2 3 4 5; do
            pgrep -x rustuya-bridge >/dev/null 2>&1 || break
            sleep 1
        done
        if pgrep -x rustuya-bridge >/dev/null 2>&1; then
            warn "Process did not exit on SIGTERM; sending SIGKILL."
            pkill -KILL -x rustuya-bridge 2>/dev/null || true
        fi
    fi
}

# ── Commands ───────────────────────────────────────────────────────────────────
cmd_status() {
    local cur latest latest_ver target active enabled
    cur="$(current_version)" || true
    target="$(detect_target 2>/dev/null || echo unknown)"

    printf '%srustuya-bridge%s\n' "$C_BOLD" "$C_RESET"

    if [ -n "$cur" ]; then
        printf '  Binary:    %s  (%s%s%s)\n' "$BIN_PATH" "$C_BOLD" "$cur" "$C_RESET"
    else
        printf '  Binary:    %snot installed%s\n' "$C_YELLOW" "$C_RESET"
    fi

    if latest="$(api_latest_tag 2>/dev/null)" && [ -n "$latest" ]; then
        latest_ver="${latest#v}"
        if [ -n "$cur" ] && [ "$cur" = "$latest_ver" ]; then
            printf '  Latest:    %s  %s(up to date)%s\n' "$latest_ver" "$C_GREEN" "$C_RESET"
        elif [ -n "$cur" ]; then
            printf '  Latest:    %s  %s⬆ upgrade available%s  (run: sudo bridgectl upgrade)\n' \
                "$latest_ver" "$C_YELLOW" "$C_RESET"
        else
            printf '  Latest:    %s  (run: sudo bridgectl install)\n' "$latest_ver"
        fi
    else
        printf '  Latest:    %s(could not query GitHub)%s\n' "$C_YELLOW" "$C_RESET"
    fi

    if [ -f "$UNIT_PATH" ]; then
        active="$(systemctl is-active "$SERVICE" 2>/dev/null || true)"
        enabled="$(systemctl is-enabled "$SERVICE" 2>/dev/null || true)"
        printf '  Service:   %s\n' "${active:-unknown}"
        printf '  Enabled:   %s\n' "${enabled:-unknown}"
    else
        printf '  Service:   %snot registered%s\n' "$C_YELLOW" "$C_RESET"
    fi

    if [ -f "$STATE_FILE" ]; then
        local count mtime
        count="$(grep -c '"id":' "$STATE_FILE" 2>/dev/null || echo 0)"
        mtime="$(date -r "$STATE_FILE" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || echo unknown)"
        printf '  State:     %s  (%s devices, modified %s)\n' "$STATE_FILE" "$count" "$mtime"
    fi

    if [ -f "$CONFIG_FILE" ]; then
        printf '  Config:    %s\n' "$CONFIG_FILE"
    fi

    if id -u "$SVC_USER" >/dev/null 2>&1; then
        printf '  User:      %s (uid %s)\n' "$SVC_USER" "$(id -u "$SVC_USER")"
    fi

    [ "$target" != "unknown" ] && printf '  Target:    %s\n' "$target"
}

cmd_install() {
    require_root install
    require_systemd
    require_tools
    [ ! -x "$BIN_PATH" ] || die "rustuya-bridge already installed at $BIN_PATH. Use: sudo bridgectl upgrade"

    local target tag tmpdir
    target="$(detect_target)"
    tag="$(api_latest_tag)" || die "Failed to query latest release from GitHub."
    [ -n "$tag" ] || die "Empty tag from GitHub API."

    tmpdir="$(mktemp -d)"
    # Bake the path into the trap (double-quoted) so it survives `local`
    # going out of scope when the function returns.
    trap "rm -rf -- '$tmpdir'" EXIT

    log "Installing rustuya-bridge ${tag} for ${target}..."
    download_release "$tag" "$target" "$tmpdir"
    install_binary "${tmpdir}/rustuya-bridge"
    ensure_user
    ensure_data_dir
    write_config_file
    write_unit
    install_self

    systemctl enable --now "$SERVICE"

    print_install_summary "$tag"
}

print_install_summary() {
    local tag="$1"
    printf '\n%sInstallation complete%s\n' "$C_BOLD" "$C_RESET"
    printf '  Binary:    %s  (%s)\n' "$BIN_PATH" "$tag"
    printf '  Helper:    %s  (run %sbridgectl%s anywhere)\n' "$SELF_PATH" "$C_BOLD" "$C_RESET"
    printf '  Config:    %s\n' "$CONFIG_FILE"
    printf '  State:     %s  (auto-created on first run)\n' "$STATE_FILE"
    printf '  Unit:      %s\n' "$UNIT_PATH"
    printf '  User:      %s\n' "$SVC_USER"
    printf '  Service:   %senabled and running%s (broker: mqtt://localhost:1883)\n' "$C_GREEN" "$C_RESET"

    printf '\n%sNext steps%s\n' "$C_BOLD" "$C_RESET"
    printf '  • To change broker, MQTT topics, or other settings, edit\n'
    printf '       %s%s%s\n' "$C_BOLD" "$CONFIG_FILE" "$C_RESET"
    printf '    then restart:\n'
    printf '       %ssudo systemctl restart %s%s\n' "$C_BOLD" "$SERVICE" "$C_RESET"
    printf '  • View logs:\n'
    printf '       %sjournalctl -u %s -f%s\n' "$C_BOLD" "$SERVICE" "$C_RESET"
    printf '  • Manage later:\n'
    printf '       %ssudo bridgectl {status,upgrade,remove,purge}%s\n' "$C_BOLD" "$C_RESET"
}

cmd_upgrade() {
    require_root upgrade
    require_systemd
    require_tools
    [ -x "$BIN_PATH" ] || die "rustuya-bridge is not installed. Use: sudo bridgectl install"

    local cur target tag latest_ver tmpdir was_active=0
    cur="$(current_version)"
    target="$(detect_target)"
    tag="$(api_latest_tag)" || die "Failed to query latest release."
    latest_ver="${tag#v}"

    if [ "$cur" = "$latest_ver" ]; then
        ok "Already up to date (${cur})."
        return 0
    fi

    confirm "Upgrade rustuya-bridge ${cur} → ${latest_ver}?" || { log "Aborted."; return 0; }

    tmpdir="$(mktemp -d)"
    # Bake the path into the trap (double-quoted) so it survives `local`
    # going out of scope when the function returns.
    trap "rm -rf -- '$tmpdir'" EXIT

    download_release "$tag" "$target" "$tmpdir"

    if systemctl is-active --quiet "$SERVICE" 2>/dev/null; then
        was_active=1
        systemctl stop "$SERVICE"
    fi

    install_binary "${tmpdir}/rustuya-bridge"
    install_self
    ok "Upgraded to ${latest_ver}."

    if [ "$was_active" -eq 1 ]; then
        systemctl start "$SERVICE"
        ok "Service restarted."
    fi
}

cmd_remove() {
    require_root remove
    require_systemd
    if [ ! -x "$BIN_PATH" ] && [ ! -f "$UNIT_PATH" ]; then
        die "rustuya-bridge is not installed. (Use 'purge' to clean up data if needed.)"
    fi

    confirm "Remove rustuya-bridge? (data dir and user preserved)" || { log "Aborted."; return 0; }

    stop_disable_service
    rm -f "$UNIT_PATH" "$BIN_PATH"
    systemctl daemon-reload
    ok "Removed binary and unit. Preserved: ${DATA_DIR}, user ${SVC_USER}."
    log "  Run 'sudo bridgectl purge' to also delete the data dir and user."
}

cmd_purge() {
    require_root purge
    require_systemd
    confirm "PURGE rustuya-bridge AND all data (${DATA_DIR}, user ${SVC_USER})?" \
        || { log "Aborted."; return 0; }

    stop_disable_service
    rm -f "$UNIT_PATH" "$BIN_PATH" "$SELF_PATH"
    systemctl daemon-reload || true
    rm -rf "$DATA_DIR"

    if id -u "$SVC_USER" >/dev/null 2>&1; then
        userdel "$SVC_USER" 2>/dev/null || warn "Could not remove user '${SVC_USER}' (still has running processes?)."
    fi
    ok "Purged rustuya-bridge and all related data."
}

cmd_help() {
    cat <<EOF
${C_BOLD}bridgectl${C_RESET} — install / manage rustuya-bridge as a systemd service.

${C_BOLD}USAGE${C_RESET}
  bridgectl [status]              Show install / service / version state (default)
  sudo bridgectl install [opts]   Download latest release, register + start systemd unit
  sudo bridgectl upgrade [opts]   Replace binary with latest release (preserves config)
  sudo bridgectl remove  [opts]   Stop, disable, remove binary + unit (keeps data dir)
  sudo bridgectl purge   [opts]   Remove everything: binary, unit, data dir, user, helper
  bridgectl help                  Show this help

${C_BOLD}OPTIONS${C_RESET}
  --yes, -y          Skip confirmation prompts (required for non-interactive shells)

${C_BOLD}FILES MANAGED${C_RESET}
  Binary:   ${BIN_PATH}
  Helper:   ${SELF_PATH}    (this script, copied here on install)
  Unit:     ${UNIT_PATH}
  Config:   ${CONFIG_FILE}
  State:    ${STATE_FILE}
  DataDir:  ${DATA_DIR}/

${C_BOLD}FIRST INSTALL${C_RESET} (one-liner via curl)
  curl -fsSL https://raw.githubusercontent.com/${GH_OWNER}/${GH_REPO}/master/scripts/bridgectl.sh \\
    | sudo bash -s -- install --yes
EOF
}

# ── Argument parsing ───────────────────────────────────────────────────────────
main() {
    local subcmd="" rest=()
    while [ $# -gt 0 ]; do
        case "$1" in
            --yes|-y)     ASSUME_YES=1 ;;
            -h|--help)    cmd_help; exit 0 ;;
            -*)           die "Unknown flag: $1" ;;
            *)
                if [ -z "$subcmd" ]; then
                    subcmd="$1"
                else
                    rest+=("$1")
                fi
                ;;
        esac
        shift
    done

    case "${subcmd:-status}" in
        status)   cmd_status ;;
        install)  cmd_install ;;
        upgrade)  cmd_upgrade ;;
        remove)   cmd_remove ;;
        purge)    cmd_purge ;;
        help)     cmd_help ;;
        *)        err "Unknown command: $subcmd"; cmd_help; exit 2 ;;
    esac
}

main "$@"
