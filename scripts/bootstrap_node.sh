#!/usr/bin/env bash
# ==============================================================================
# Hermes Orchestrator Fleet Node Bootstrapper (Modular & Idempotent)
# Location: scripts/bootstrap_node.sh
# Purpose: Fully automates pre-requisite installation and node configuration.
#          Supports both interactive menu selection and CLI flags:
#          - v2rayN (.NET 8 + Avalonia GUI + Xray/V2ray cores)
#          - Antigravity Tools (LLM Proxy & Account Manager daemon on :8045)
#          - Hermes Agent CLI (~/.hermes, config, providers)
#          - Hermes Orchestrator (Venv, Arize Phoenix :6006, Control Center :5050)
#          - Experience & Skills hydration (sync_experience.sh)
# ==============================================================================
set -euo pipefail

NODE_USER="${SUDO_USER:-$USER}"
USER_HOME=$(eval echo "~${NODE_USER}")
AUTO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
RED='\033[0;31m'
NC='\033[0m'

log_info() { echo -e "${CYAN}[INFO]${NC} $1"; }
log_ok()   { echo -e "${GREEN}[OK]${NC}   $1"; }
log_skip() { echo -e "${BLUE}[SKIP]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_err()  { echo -e "${RED}[ERR]${NC}  $1"; }

# Component Toggles
DO_V2RAYN=0
DO_ANTIGRAVITY=0
DO_HERMES=0
DO_ORCHESTRATOR=0
DO_EXPERIENCE=0
EXPERIENCE_ARCHIVE=""

show_help() {
    echo "Hermes Fleet Node Bootstrapper"
    echo ""
    echo "Usage: $0 [OPTIONS] [experience_bundle.tar.gz]"
    echo ""
    echo "Interactive Mode:"
    echo "  $0                      (Launches interactive component selector menu)"
    echo "  $0 -i, --interactive   (Explicitly open the menu)"
    echo ""
    echo "Component Flags:"
    echo "  --all, -a               Install and configure ALL components (Default in batch)"
    echo "  --v2rayn, -v            Install/configure v2rayN network proxy & cores"
    echo "  --antigravity, --ag     Install/configure Antigravity Tools daemon on port 8045"
    echo "  --hermes                Install/configure Hermes Agent CLI (~/.hermes)"
    echo "  --orchestrator, -o      Install/configure Hermes Orchestrator & Arize Phoenix"
    echo "  --experience, -e [path] Import/sync 139+ learned skills and memories"
    echo "  --help, -h              Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --v2rayn --antigravity     (Only network & LLM router)"
    echo "  $0 --hermes --orchestrator   (Only AI agent core & multi-repo orchestrator)"
    echo "  $0 --all                     (Full fleet installation)"
    echo ""
}

parse_selection_input() {
    local raw_input="$1"
    DO_V2RAYN=0
    DO_ANTIGRAVITY=0
    DO_HERMES=0
    DO_ORCHESTRATOR=0
    DO_EXPERIENCE=0

    # 1. Normalize Persian and Arabic numerals to ASCII
    local norm
    norm=$(echo "$raw_input" | sed 'y/۰۱۲۳۴۵۶۷۸۹/0123456789/')
    # 2. Normalize separators: Persian "و", commas, pluses, dashes, semicolons to spaces
    norm=$(echo "$norm" | sed -e 's/و/ /g' -e 's/,/ /g' -e 's/+/ /g' -e 's/-/ /g' -e 's/;/ /g')

    if [[ "$norm" =~ ^[qQ] ]]; then
        echo "Installation cancelled by user."
        exit 0
    fi

    if [[ "$norm" =~ ^[1aA]$ || "$norm" == "all" || "$norm" == "ALL" ]]; then
        DO_V2RAYN=1
        DO_ANTIGRAVITY=1
        DO_HERMES=1
        DO_ORCHESTRATOR=1
        DO_EXPERIENCE=1
        return 0
    fi

    for token in $norm; do
        case "$token" in
            1)
                DO_V2RAYN=1
                DO_ANTIGRAVITY=1
                DO_HERMES=1
                DO_ORCHESTRATOR=1
                DO_EXPERIENCE=1
                ;;
            2) DO_V2RAYN=1 ;;
            3) DO_ANTIGRAVITY=1 ;;
            4) DO_HERMES=1 ;;
            5) DO_ORCHESTRATOR=1 ;;
            6) DO_EXPERIENCE=1 ;;
        esac
    done

    if [[ $DO_V2RAYN -eq 0 && $DO_ANTIGRAVITY -eq 0 && $DO_HERMES -eq 0 && $DO_ORCHESTRATOR -eq 0 && $DO_EXPERIENCE -eq 0 ]]; then
        return 1
    fi
    return 0
}

show_menu() {
    while true; do
        echo -e "${CYAN}┌────────────────────────────────────────────────────────────────────────┐${NC}"
        echo -e "${CYAN}│       🚀 Hermes Fleet Node Modular Bootstrapper - Component Menu       │${NC}"
        echo -e "${CYAN}└────────────────────────────────────────────────────────────────────────┘${NC}"
        echo "Target User: ${NODE_USER} | Base Dir: ${AUTO_DIR}"
        echo ""
        echo "Select components to install / configure:"
        echo "(You can enter single numbers like '3', or multi-selection like '3,6' or '3و6')"
        echo ""
        echo -e "  ${GREEN}[1] 🌟 Full Stack Fleet Node${NC}      (Install & verify EVERYTHING)"
        echo -e "  ${CYAN}[2] 🌐 v2rayN Only${NC}                (Network Proxy, .NET 8 & Xray Cores)"
        echo -e "  ${PURPLE}[3] ⚡ Antigravity Tools Only${NC}     (LLM Proxy & Account Manager daemon :8045)"
        echo -e "  ${YELLOW}[4] 🤖 Hermes Agent Only${NC}          (CLI, ~/.hermes runtime & provider config)"
        echo -e "  ${BLUE}[5] 🏛️ Hermes Orchestrator Only${NC}   (Venv, Cockpit :5050 & Arize Phoenix :6006)"
        echo -e "  ${GREEN}[6] 🧠 Collective Intelligence${NC}    (Sync 139+ procedural skills & memories)"
        echo -e "  ${RED}[Q] ❌ Exit / Cancel${NC}"
        echo ""
        read -r -p "Enter your choices (e.g. 1, 3, 3,6, or 3و6) [Default: 1]: " user_choice
        user_choice="${user_choice:-1}"

        if parse_selection_input "$user_choice"; then
            break
        else
            log_warn "Unrecognized choice: '$user_choice'. Please select from [1-6, 3,6, 3و6, or Q]."
            echo ""
        fi
    done
}

# ------------------------------------------------------------------------------
# STEP 0: System Prerequisites & Dependencies
# ------------------------------------------------------------------------------
install_system_deps() {
    log_info "Step 0: Checking Linux system dependencies..."
    local missing_pkgs=()
    for cmd in curl wget jq unzip rsync git python3; do
        if ! command -v "$cmd" &>/dev/null; then
            missing_pkgs+=("$cmd")
        fi
    done

    if [[ ${#missing_pkgs[@]} -gt 0 ]]; then
        log_info "Installing missing dependencies: ${missing_pkgs[*]}..."
        if command -v apt-get &>/dev/null; then
            if sudo -n true 2>/dev/null; then
                sudo apt-get update -y
                sudo apt-get install -y                     curl wget jq unzip rsync git                     python3 python3-pip python3-venv                     libicu-dev libfuse2 desktop-file-utils                     wmctrl xdotool
                log_ok "System dependencies installed via apt."
            else
                log_warn "sudo password required for apt packages. Please install if needed: ${missing_pkgs[*]}"
            fi
        fi
    else
        log_skip "All basic CLI utilities (curl, wget, jq, unzip, rsync, git, python3) are already installed."
    fi

    # Ensure ~/.local/bin is in user PATH
    mkdir -p "${USER_HOME}/.local/bin"
    if ! grep -q 'export PATH="$HOME/.local/bin:$PATH"' "${USER_HOME}/.bashrc" 2>/dev/null; then
        echo 'export PATH="$HOME/.local/bin:$PATH"' >> "${USER_HOME}/.bashrc"
    fi
}

# ------------------------------------------------------------------------------
# STEP 1: v2rayN Installation (.NET 8 + Avalonia GUI + Cores)
# ------------------------------------------------------------------------------
install_v2rayn() {
    log_info "Step 1: Checking v2rayN installation & runtime..."
    local V2RAYN_DIR="/opt/v2rayN"

    # 1. First check if v2rayN is already installed and executable
    if [[ -x "/usr/bin/v2rayn" || -x "/usr/local/bin/v2rayn" || (-d "$V2RAYN_DIR" && -x "$V2RAYN_DIR/v2rayN") || (-d "$V2RAYN_DIR" && -f "$V2RAYN_DIR/v2rayN.Desktop.dll") ]]; then
        log_skip "v2rayN is already installed at ${V2RAYN_DIR}. Skipping installation."
        return 0
    fi

    # 2. Check .NET 8 Runtime only if self-contained binary is not present
    if ! command -v dotnet &>/dev/null; then
        log_info "Installing Microsoft .NET 8 Runtime..."
        if command -v apt-get &>/dev/null; then
            if sudo -n true 2>/dev/null; then
                sudo apt-get install -y dotnet-runtime-8.0 2>/dev/null || true
            else
                log_warn "sudo privileges required to install dotnet-runtime-8.0. Please run with sudo if needed."
            fi
        fi
    else
        log_skip ".NET runtime already installed: $(dotnet --version 2>/dev/null || echo 'OK')"
    fi

    # 3. Target directory and download
    if [[ ! -d "$V2RAYN_DIR" ]]; then
        if sudo -n true 2>/dev/null; then
            sudo mkdir -p "$V2RAYN_DIR"
            sudo chown -R "${NODE_USER}:${NODE_USER}" "$V2RAYN_DIR"
        else
            V2RAYN_DIR="${USER_HOME}/.local/share/v2rayN"
            mkdir -p "$V2RAYN_DIR"
        fi

        local LATEST_TAG
        LATEST_TAG=$(curl -s https://api.github.com/repos/2dust/v2rayN/releases/latest | jq -r '.tag_name' 2>/dev/null || echo "6.49")
        log_info "Downloading v2rayN Linux release (${LATEST_TAG})..."
        local DL_URL="https://github.com/2dust/v2rayN/releases/download/${LATEST_TAG}/v2rayN-linux-64.zip"
        if wget -q --show-progress "$DL_URL" -O /tmp/v2rayN-linux.zip 2>/dev/null; then
            unzip -q -o /tmp/v2rayN-linux.zip -d "$V2RAYN_DIR"
            rm -f /tmp/v2rayN-linux.zip
            log_ok "v2rayN extracted to ${V2RAYN_DIR}"
        fi
    fi

    # Launcher check
    if [[ ! -x /usr/local/bin/v2rayn && ! -x /usr/bin/v2rayn ]]; then
        log_info "Creating launcher in ~/.local/bin/v2rayn..."
        cat << 'LAUNCHER_EOF' > "${USER_HOME}/.local/bin/v2rayn"
#!/usr/bin/env bash
set -euo pipefail
DIR="/opt/v2rayN"
[[ ! -d "$DIR" ]] && DIR="$HOME/.local/share/v2rayN"
cd "$DIR"
if [[ -x "$DIR/v2rayN" ]]; then
  exec "$DIR/v2rayN" "$@"
fi
for dll in v2rayN.Desktop.dll v2rayN.dll; do
  if [[ -f "$DIR/$dll" ]]; then
    exec dotnet "$DIR/$dll" "$@"
  fi
done
echo "v2rayN launcher: no executable or DLL found in $DIR" >&2
exit 1
LAUNCHER_EOF
        chmod +x "${USER_HOME}/.local/bin/v2rayn"
        log_ok "Created launcher ~/.local/bin/v2rayn"
    else
        log_skip "v2rayN launcher binary already available in system PATH."
    fi
}

# ------------------------------------------------------------------------------
# STEP 2: Antigravity Tools Installation (LLM Proxy & Account Manager :8045)
# Specifically downloads and extracts the universal .AppImage release (NOT .deb)
# ------------------------------------------------------------------------------
install_antigravity_tools() {
    log_info "Step 2: Checking Antigravity Tools installation & configuration..."
    local AG_TOOLS_DIR="${USER_HOME}/antigravity_tools"
    local AG_CONFIG_DIR="${USER_HOME}/.antigravity_tools"
    mkdir -p "$AG_TOOLS_DIR" "$AG_CONFIG_DIR" "${USER_HOME}/.local/bin"

    # 1. Check for existing .AppImage in directory
    local APPIMAGE_FILE=""
    for candidate in "${AG_TOOLS_DIR}"/Antigravity*.AppImage "${AG_TOOLS_DIR}"/*.AppImage; do
        if [[ -f "$candidate" && -s "$candidate" ]]; then
            APPIMAGE_FILE="$candidate"
            break
        fi
    done

    # 2. If no .AppImage is found, download the official .AppImage release
    if [[ -z "$APPIMAGE_FILE" ]]; then
        APPIMAGE_FILE="${AG_TOOLS_DIR}/Antigravity.Tools_amd64.AppImage"
        log_info "Querying latest Antigravity Tools .AppImage release from GitHub..."

        local DL_URL=""
        # Query official lbjlaq/Antigravity-Manager repo for amd64 .AppImage asset
        DL_URL=$(curl -s --max-time 10 https://api.github.com/repos/lbjlaq/Antigravity-Manager/releases/latest 2>/dev/null | jq -r '.assets[] | select(.name | test("amd64.*\.AppImage$")) | .browser_download_url' | head -n 1 || echo "")
        
        # Try through local proxy if direct fetch is blocked
        if [[ -z "$DL_URL" || "$DL_URL" == "null" ]]; then
            DL_URL=$(curl -s --max-time 10 --proxy http://127.0.0.1:10809 https://api.github.com/repos/lbjlaq/Antigravity-Manager/releases/latest 2>/dev/null | jq -r '.assets[] | select(.name | test("amd64.*\.AppImage$")) | .browser_download_url' | head -n 1 || echo "")
        fi

        # Known stable fallback if API rate-limited
        if [[ -z "$DL_URL" || "$DL_URL" == "null" ]]; then
            DL_URL="https://github.com/lbjlaq/Antigravity-Manager/releases/download/v5.0.0/Antigravity.Tools_5.0.0_amd64.AppImage"
        fi

        log_info "Downloading Antigravity Tools (.AppImage format): ${DL_URL}..."
        if ! wget -q --show-progress "$DL_URL" -O "$APPIMAGE_FILE"; then
            log_warn "wget direct download failed. Trying through local proxy..."
            if command -v curl &>/dev/null; then
                curl -L -o "$APPIMAGE_FILE" --proxy http://127.0.0.1:10809 "$DL_URL" 2>/dev/null || curl -L -o "$APPIMAGE_FILE" "$DL_URL"
            fi
        fi

        if [[ -f "$APPIMAGE_FILE" && -s "$APPIMAGE_FILE" ]]; then
            chmod +x "$APPIMAGE_FILE"
            log_ok "Downloaded Antigravity Tools AppImage to ${APPIMAGE_FILE}"
        else
            log_err "Failed to download Antigravity Tools .AppImage. Please verify internet/proxy connection."
        fi
    else
        log_skip "Antigravity Tools .AppImage already present: $(basename "$APPIMAGE_FILE")"
    fi

    # 3. Extract AppImage for headless background daemon if app directory is missing
    if [[ ! -d "${AG_TOOLS_DIR}/app" ]]; then
        if [[ -n "$APPIMAGE_FILE" && -f "$APPIMAGE_FILE" ]]; then
            log_info "Extracting AppImage for headless service runtime..."
            cd "$AG_TOOLS_DIR"
            chmod +x "$APPIMAGE_FILE"
            ./"$(basename "$APPIMAGE_FILE")" --appimage-extract > /dev/null 2>&1 || true
            if [[ -d "${AG_TOOLS_DIR}/squashfs-root" ]]; then
                mv "${AG_TOOLS_DIR}/squashfs-root" "${AG_TOOLS_DIR}/app"
                log_ok "Extracted headless runtime to ${AG_TOOLS_DIR}/app"
            fi
        fi
    else
        log_skip "Headless app runtime already extracted at ${AG_TOOLS_DIR}/app."
    fi

    # 4. Create smart launcher at ~/.local/bin/antigravity-tools
    cat << 'LAUNCHER_AG' > "${USER_HOME}/.local/bin/antigravity-tools"
#!/usr/bin/env bash
USER_HOME=$(eval echo "~${SUDO_USER:-$USER}")
AG_TOOLS_DIR="${USER_HOME}/antigravity_tools"
APPIMAGE_PATH="$(find "${AG_TOOLS_DIR}" -maxdepth 1 -name "Antigravity*.AppImage" 2>/dev/null | head -n 1 || echo "${AG_TOOLS_DIR}/Antigravity.Tools_amd64.AppImage")"

# Headless background execution
if [[ "${1:-}" == "--headless" ]]; then
    if [[ -x "${AG_TOOLS_DIR}/app/usr/bin/antigravity-tools" ]]; then
        exec "${AG_TOOLS_DIR}/app/usr/bin/antigravity-tools" "$@"
    elif [[ -x "$APPIMAGE_PATH" ]]; then
        exec "$APPIMAGE_PATH" "$@"
    fi
fi

# GUI execution
if [[ -x "$APPIMAGE_PATH" ]]; then
    exec "$APPIMAGE_PATH" "$@"
elif [[ -x "${AG_TOOLS_DIR}/app/usr/bin/antigravity-tools" ]]; then
    exec "${AG_TOOLS_DIR}/app/usr/bin/antigravity-tools" "$@"
else
    echo "[-] Antigravity Tools binary or AppImage not found in ${AG_TOOLS_DIR}" >&2
    exit 1
fi
LAUNCHER_AG
    chmod +x "${USER_HOME}/.local/bin/antigravity-tools"
    log_ok "Configured ~/.local/bin/antigravity-tools launcher (AppImage GUI + Headless daemon)"

    # Smart configuration & port verification
    local AG_CONFIG="${AG_CONFIG_DIR}/gui_config.json"
    if [[ ! -f "$AG_CONFIG" ]]; then
        log_info "Generating default Antigravity Tools gui_config.json on port 8045..."
        cat << 'CFG_EOF' > "$AG_CONFIG"
{
  "api_key": "sk-antigravity-fleet-node",
  "proxy": {
    "enabled": true,
    "auto_start": true,
    "allow_lan_access": true,
    "port": 8045,
    "custom_mapping": {
      "gemini-3.8-flash-high": "gemini-3.8-flash-tiered",
      "gemini-3.8-flash-medium": "gemini-3.8-flash-tiered",
      "gemini-3.8-flash-low": "gemini-3.8-flash-tiered",
      "gemini-2.5-flash": "gemini-3-flash",
      "gemini-2.5-flash-lite": "gemini-3.1-flash-lite"
    }
  }
}
CFG_EOF
        log_ok "Created new config at ${AG_CONFIG}"
    else
        log_info "Verifying existing gui_config.json (preserving accounts, ensuring port 8045)..."
        python3 -c "
import json
cfg_path = '$AG_CONFIG'
try:
    with open(cfg_path, 'r') as f:
        cfg = json.load(f)
    proxy = cfg.setdefault('proxy', {})
    proxy['enabled'] = True
    proxy['auto_start'] = True
    proxy['allow_lan_access'] = True
    proxy['port'] = 8045
    mapping = proxy.setdefault('custom_mapping', {})
    desired = {
        'gemini-3.8-flash-high': 'gemini-3.8-flash-tiered',
        'gemini-3.8-flash-medium': 'gemini-3.8-flash-tiered',
        'gemini-3.8-flash-low': 'gemini-3.8-flash-tiered',
        'gemini-2.5-flash': 'gemini-3-flash',
        'gemini-2.5-flash-lite': 'gemini-3.1-flash-lite'
    }
    mapping.update(desired)
    with open(cfg_path, 'w') as f:
        json.dump(cfg, f, indent=2)
    print('  [OK] Successfully synchronized existing config without overwriting user credentials.')
except Exception as e:
    print('  [WARN] Could not patch config:', e)
"
    fi

    # Systemd headless service
    local SYS_DIR="${USER_HOME}/.config/systemd/user"
    mkdir -p "$SYS_DIR"
    local SVC_FILE="${SYS_DIR}/antigravity-tools.service"
    if [[ ! -f "$SVC_FILE" ]]; then
        cat << 'SVC_EOF' > "$SVC_FILE"
[Unit]
Description=Antigravity Tools - Headless LLM Proxy & Router Service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=%h/.local/bin/antigravity-tools --headless
WorkingDirectory=%h
Environment="DISPLAY="
Environment="WAYLAND_DISPLAY="
Environment="WEBKIT_DISABLE_COMPOSITING_MODE=1"
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
SVC_EOF
        log_ok "Created antigravity-tools systemd service."
    else
        log_skip "antigravity-tools.service already defined."
    fi
}

# ------------------------------------------------------------------------------
# STEP 3: Hermes Agent CLI Installation (~/.hermes & Port 8045 Config)
# ------------------------------------------------------------------------------
install_hermes_agent() {
    log_info "Step 3: Checking Hermes Agent CLI installation & configuration..."
    local HERMES_DIR="${USER_HOME}/.hermes"
    mkdir -p "$HERMES_DIR" "${USER_HOME}/.local/bin"

    local AGENT_REPO="${HERMES_DIR}/hermes-agent"
    if [[ -d "$AGENT_REPO" || -x "${USER_HOME}/.local/bin/hermes" ]]; then
        log_skip "Hermes Agent repo or CLI is already present. Skipping clone."
    else
        log_info "Cloning hermes-agent..."
        git clone https://github.com/nousresearch/hermes-agent.git "$AGENT_REPO" || {
            log_warn "Could not clone from remote; if another node has it, copy to ${AGENT_REPO}."
        }
    fi

    # Launcher
    if [[ ! -f "${USER_HOME}/.local/bin/hermes" ]]; then
        cat << 'HERMES_LAUNCHER' > "${USER_HOME}/.local/bin/hermes"
#!/usr/bin/env bash
USER_HOME=$(eval echo "~${SUDO_USER:-$USER}")
AGENT_REPO="${USER_HOME}/.hermes/hermes-agent"
if [[ -f "${AGENT_REPO}/.hermes/bin/hermes" ]]; then
    exec "${AGENT_REPO}/.hermes/bin/hermes" "$@"
elif command -v hermes &>/dev/null && [[ "$(command -v hermes)" != "${USER_HOME}/.local/bin/hermes" ]]; then
    exec "$(command -v hermes)" "$@"
elif [[ -f "${AGENT_REPO}/cli.py" ]]; then
    exec python3 "${AGENT_REPO}/cli.py" "$@"
else
    echo "Hermes binary or cli.py not found in ${AGENT_REPO}" >&2
    exit 1
fi
HERMES_LAUNCHER
        chmod +x "${USER_HOME}/.local/bin/hermes"
        log_ok "Created launcher: ~/.local/bin/hermes"
    else
        log_skip "Launcher ~/.local/bin/hermes already exists."
    fi

    # Config
    local CONFIG_YAML="${HERMES_DIR}/config.yaml"
    if [[ ! -f "$CONFIG_YAML" ]]; then
        cat << 'YAML_EOF' > "$CONFIG_YAML"
model:
  default: gemini-3.8-flash-tiered
  provider: antigravity_tools
  base_url: http://127.0.0.1:8045/v1
  key_env: HERMES_ANTIGRAVITY_TOOLS_API_KEY
  api_mode: chat_completions
providers:
  antigravity_tools:
    name: antigravity_tools
    base_url: http://127.0.0.1:8045/v1
    model: claude-opus-4-6-thinking
    discover_models: true
    models:
      claude-opus-4-6-thinking: {}
      claude-sonnet-4-6-thinking: {}
      gemini-3-flash: {}
      gemini-3.1-flash-lite: {}
      gemini-3.8-flash-tiered: {}
    key_env: HERMES_ANTIGRAVITY_TOOLS_API_KEY
agent:
  max_turns: 150
  verbose: false
  reasoning_effort: high
terminal:
  backend: local
  cwd: .
  timeout: 180
YAML_EOF
        log_ok "Created ~/.hermes/config.yaml pointed to port 8045"
    else
        log_skip "Config ~/.hermes/config.yaml already exists. Verifying router URL..."
        if ! grep -q "127.0.0.1:8045" "$CONFIG_YAML" 2>/dev/null; then
            sed -i 's|127.0.0.1:7836|127.0.0.1:8045|g' "$CONFIG_YAML" || true
            log_ok "Updated router port to 8045 in config.yaml"
        fi
    fi

    local HERMES_ENV="${HERMES_DIR}/.env"
    if [[ ! -f "$HERMES_ENV" ]]; then
        cat << 'ENV_EOF' > "$HERMES_ENV"
HERMES_ANTIGRAVITY_TOOLS_API_KEY=sk-antigravity-fleet-node
ROUTER_URL=http://127.0.0.1:8045/v1
ENV_EOF
        log_ok "Created ~/.hermes/.env"
    fi
}

# ------------------------------------------------------------------------------
# STEP 4: Hermes Orchestrator Setup (Python Venv, Phoenix, Systemd)
# ------------------------------------------------------------------------------
install_orchestrator() {
    log_info "Step 4: Checking Hermes Orchestrator & Python environment..."
    local VENV_DIR="${USER_HOME}/.phoenix-venv"
    if [[ ! -d "$VENV_DIR" ]]; then
        log_info "Creating Python virtualenv at ${VENV_DIR}..."
        python3 -m venv "$VENV_DIR"
    else
        log_skip "Python virtual environment already exists at ${VENV_DIR}."
    fi

    if "${VENV_DIR}/bin/python3" -c "import phoenix, openinference, flask, requests" 2>/dev/null; then
        log_skip "Core dependencies (phoenix, openinference, flask, requests) are already installed in venv."
    else
        log_info "Installing Orchestrator & Arize Phoenix dependencies..."
        "${VENV_DIR}/bin/pip" install --upgrade pip
        "${VENV_DIR}/bin/pip" install -r "${AUTO_DIR}/requirements.txt"
        log_ok "Dependencies installed successfully."
    fi

    local SYS_DIR="${USER_HOME}/.config/systemd/user"
    mkdir -p "$SYS_DIR"

    local CC_SVC="${SYS_DIR}/hermes-control-center.service"
    if [[ ! -f "$CC_SVC" ]]; then
        cat << CC_EOF > "$CC_SVC"
[Unit]
Description=Hermes Control Center & Kanban Web UI
After=network.target

[Service]
Type=simple
ExecStart=/usr/bin/python3 ${AUTO_DIR}/control_center/server.py
WorkingDirectory=${AUTO_DIR}/control_center
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
CC_EOF
        log_ok "Created hermes-control-center.service (Port 5050)"
    else
        log_skip "hermes-control-center.service already defined."
    fi

    local PHX_SVC="${SYS_DIR}/arize-phoenix.service"
    if [[ ! -f "$PHX_SVC" ]]; then
        cat << PHX_EOF > "$PHX_SVC"
[Unit]
Description=Arize Phoenix AI Agent Observability & Tracing Server
After=network.target

[Service]
Type=simple
Environment=PHOENIX_PORT=6006
Environment=PHOENIX_HOST=127.0.0.1
Environment=PHOENIX_WORKING_DIR=${USER_HOME}/.phoenix
ExecStart=${VENV_DIR}/bin/phoenix serve
WorkingDirectory=${USER_HOME}
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
PHX_EOF
        log_ok "Created arize-phoenix.service (Port 6006)"
    else
        log_skip "arize-phoenix.service already defined."
    fi
}

# ------------------------------------------------------------------------------
# STEP 5: Experience & Collective Intelligence Import
# ------------------------------------------------------------------------------
import_experience_bundle() {
    local bundle="${1:-}"
    if [[ -n "$bundle" && -f "$bundle" ]]; then
        log_info "Step 5: Hydrating collective experience from ${bundle}..."
        "${AUTO_DIR}/scripts/sync_experience.sh" import "$bundle"
    else
        log_info "Step 5: Checking local experience inventory..."
        "${AUTO_DIR}/scripts/sync_experience.sh" status
    fi
}

# ------------------------------------------------------------------------------
# CLI ARGS PARSING & INTERACTIVE DISPATCH
# ------------------------------------------------------------------------------
if [[ $# -eq 0 ]]; then
    if [[ -t 0 ]]; then
        show_menu
    else
        # Non-interactive without args (e.g. pipe/cron): execute full suite
        DO_V2RAYN=1
        DO_ANTIGRAVITY=1
        DO_HERMES=1
        DO_ORCHESTRATOR=1
        DO_EXPERIENCE=1
    fi
else
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --all|-a)
                DO_V2RAYN=1
                DO_ANTIGRAVITY=1
                DO_HERMES=1
                DO_ORCHESTRATOR=1
                DO_EXPERIENCE=1
                shift
                ;;
            --v2rayn|--v2ray|-v)
                DO_V2RAYN=1
                shift
                ;;
            --antigravity|--ag)
                DO_ANTIGRAVITY=1
                shift
                ;;
            --hermes)
                DO_HERMES=1
                shift
                ;;
            --orchestrator|--orc|-o)
                DO_ORCHESTRATOR=1
                shift
                ;;
            --experience|--exp|-e)
                DO_EXPERIENCE=1
                if [[ $# -gt 1 && ! "$2" =~ ^-- ]]; then
                    EXPERIENCE_ARCHIVE="$2"
                    shift
                fi
                shift
                ;;
            --menu|-i|--interactive)
                show_menu
                shift
                ;;
            --help|-h)
                show_help
                exit 0
                ;;
            *)
                if [[ -f "$1" ]]; then
                    EXPERIENCE_ARCHIVE="$1"
                    DO_V2RAYN=1
                    DO_ANTIGRAVITY=1
                    DO_HERMES=1
                    DO_ORCHESTRATOR=1
                    DO_EXPERIENCE=1
                else
                    echo "[-] Unknown option: $1"
                    show_help
                    exit 1
                fi
                shift
                ;;
        esac
    done
fi

echo "======================================================================"
echo "🚀 Hermes Orchestrator Fleet Node Bootstrapper"
echo "Execution Plan:"
[[ $DO_V2RAYN -eq 1 ]]       && echo "  • [ENABLED] v2rayN (.NET 8 + Xray/V2ray)"         || echo "  • [SKIPPED] v2rayN"
[[ $DO_ANTIGRAVITY -eq 1 ]]  && echo "  • [ENABLED] Antigravity Tools (Port 8045 Router)" || echo "  • [SKIPPED] Antigravity Tools"
[[ $DO_HERMES -eq 1 ]]       && echo "  • [ENABLED] Hermes Agent CLI (~/.hermes)"         || echo "  • [SKIPPED] Hermes Agent"
[[ $DO_ORCHESTRATOR -eq 1 ]] && echo "  • [ENABLED] Hermes Orchestrator (:5050 & :6006)"  || echo "  • [SKIPPED] Hermes Orchestrator"
[[ $DO_EXPERIENCE -eq 1 ]]   && echo "  • [ENABLED] Collective Intelligence Sync"          || echo "  • [SKIPPED] Collective Intelligence Sync"
echo "======================================================================"

# Run base dependencies
install_system_deps

# Run selected modules
[[ $DO_V2RAYN -eq 1 ]]       && install_v2rayn
[[ $DO_ANTIGRAVITY -eq 1 ]]  && install_antigravity_tools
[[ $DO_HERMES -eq 1 ]]       && install_hermes_agent
[[ $DO_ORCHESTRATOR -eq 1 ]] && install_orchestrator
[[ $DO_EXPERIENCE -eq 1 ]]   && import_experience_bundle "$EXPERIENCE_ARCHIVE"

echo "======================================================================"
log_ok "FLEET NODE SETUP / CONFIGURATION CYCLE FINISHED!"
echo "Summary of Enabled Components:"
[[ $DO_V2RAYN -eq 1 ]]       && echo "  1. Network Proxy:       v2rayN ready at /opt/v2rayN"
[[ $DO_ANTIGRAVITY -eq 1 ]]  && echo "  2. LLM Gateway:         Antigravity Tools verified on http://127.0.0.1:8045"
[[ $DO_HERMES -eq 1 ]]       && echo "  3. Agent Core:          Hermes Agent CLI verified (~/.local/bin/hermes)"
[[ $DO_ORCHESTRATOR -eq 1 ]] && echo "  4. Multi-Repo Engine:   Hermes Orchestrator ready in ${AUTO_DIR}"
[[ $DO_ORCHESTRATOR -eq 1 ]] && echo "     - Web Cockpit UI:    http://127.0.0.1:5050"
[[ $DO_ORCHESTRATOR -eq 1 ]] && echo "     - Arize Phoenix:     http://127.0.0.1:6006"
echo ""
echo "To reload services:"
echo "  systemctl --user daemon-reload"
echo "  systemctl --user restart antigravity-tools hermes-control-center arize-phoenix"
echo "======================================================================"
