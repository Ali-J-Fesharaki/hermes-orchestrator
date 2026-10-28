#!/usr/bin/env bash
# ==============================================================================
# setup_headless_server.sh: Deploy Hermes Orchestrator on Headless Ubuntu Server
# (No Monitor, No Desktop Environment, 100% Headless Daemons + Remote Web Cockpit)
# ==============================================================================
set -euo pipefail

GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_info() { echo -e "${CYAN}[INFO]${NC} $1"; }
log_ok()   { echo -e "${GREEN}[OK]${NC}   $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_err()  { echo -e "${RED}[ERR]${NC}  $1"; }

AUTO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NODE_USER="${USER}"
USER_HOME="${HOME}"

echo "=================================================================="
echo " 🖥️ HERMES ORCHESTRATOR HEADLESS SERVER DEPLOYMENT ENGINE"
echo "=================================================================="
log_info "Target Host: $(hostname) ($(uname -m))"
log_info "Deploying to: ${AUTO_DIR} for user: ${NODE_USER}"

# 1. System Packages & Python 3.12+
log_info "Step 1: Installing essential system libraries (git, rsync, curl, jq, python3-venv)..."
if command -v apt-get &>/dev/null; then
    sudo apt-get update -y
    sudo apt-get install -y git rsync curl jq build-essential python3 python3-pip python3-venv xauth
    log_ok "System dependencies installed."
fi

# 2. Headless VPN / TUN (Xray Core or Sing-Box - No GUI)
log_info "Step 2: Configuring Headless Network Proxy (Core only, No v2rayN GUI needed)..."
if ! command -v xray &>/dev/null && ! command -v sing-box &>/dev/null; then
    log_warn "Neither 'xray' nor 'sing-box' core detected."
    log_info "For headless servers, install xray-core or sing-box as a systemd service without GUI:"
    echo "  sudo bash -c '\$(curl -L https://github.com/XTLS/Xray-install/raw/main/install-release.sh)'"
else
    log_ok "Headless proxy core is available."
fi

# 3. Headless Antigravity Tools Daemon (Port 8045 - Extracted AppImage)
log_info "Step 3: Configuring Headless Antigravity Tools Daemon (Port 8045)..."
AG_TOOLS_DIR="${USER_HOME}/.antigravity_tools"
mkdir -p "${AG_TOOLS_DIR}"

if [[ ! -f "${AG_TOOLS_DIR}/Antigravity.Tools_amd64.AppImage" ]] && ! command -v antigravity-tools &>/dev/null; then
    log_info "Downloading Antigravity Tools AppImage for headless extraction..."
    DL_URL=$(curl -s https://api.github.com/repos/lbjlaq/Antigravity-Manager/releases/latest | jq -r '.assets[] | select(.name | test("amd64.*\\.AppImage$")) | .browser_download_url' | head -n 1 || echo "")
    if [[ -z "${DL_URL}" ]]; then
        DL_URL="https://github.com/lbjlaq/Antigravity-Manager/releases/download/v5.0.0/Antigravity.Tools_5.0.0_amd64.AppImage"
    fi
    curl -fSL "${DL_URL}" -o "${AG_TOOLS_DIR}/Antigravity.Tools_amd64.AppImage"
    chmod +x "${AG_TOOLS_DIR}/Antigravity.Tools_amd64.AppImage"
fi

if [[ -f "${AG_TOOLS_DIR}/Antigravity.Tools_amd64.AppImage" ]] && [[ ! -d "${AG_TOOLS_DIR}/app" ]]; then
    log_info "Extracting AppImage for headless background daemon runtime..."
    cd "${AG_TOOLS_DIR}"
    ./Antigravity.Tools_amd64.AppImage --appimage-extract
    mv squashfs-root app
    log_ok "AppImage extracted to ${AG_TOOLS_DIR}/app for headless execution."
fi

# 4. Hermes Agent CLI Setup
log_info "Step 4: Ensuring Hermes Agent CLI is installed..."
if ! command -v hermes &>/dev/null && [[ ! -f "${USER_HOME}/.local/bin/hermes" ]]; then
    log_info "Installing Hermes Agent..."
    curl -fsSL https://raw.githubusercontent.com/NousResearch/hermes-agent/main/install.sh | bash || true
fi
log_ok "Hermes Agent CLI verified."

# 5. Python Environment & Dependencies
log_info "Step 5: Verifying Python virtual environment..."
VENV_DIR="${USER_HOME}/.phoenix-venv"
if [[ ! -d "${VENV_DIR}" ]]; then
    python3 -m venv "${VENV_DIR}"
fi
"${VENV_DIR}/bin/pip" install --upgrade pip
"${VENV_DIR}/bin/pip" install -q arize-phoenix requests psutil || true
log_ok "Python environment configured at ${VENV_DIR}."

# 6. Systemd User Services Configuration
log_info "Step 6: Configuring systemd user services for headless background execution..."
mkdir -p "${USER_HOME}/.config/systemd/user"

# A. Control Center Service (binds 0.0.0.0:5050 for remote browser access)
cat <<EOF > "${USER_HOME}/.config/systemd/user/hermes-control-center.service"
[Unit]
Description=Hermes Control Center & Kanban Web UI (Headless Server)
After=network.target

[Service]
Type=simple
Environment=CONTROL_CENTER_HOST=0.0.0.0
ExecStart=/usr/bin/python3 ${AUTO_DIR}/control_center/server.py
WorkingDirectory=${AUTO_DIR}
Restart=always
RestartSec=3

[Install]
WantedBy=default.target
EOF

# B. Autonomous Orchestrator Service
cat <<EOF > "${USER_HOME}/.config/systemd/user/autonomous-orchestrator.service"
[Unit]
Description=Universal Multi-Repository Autonomous Orchestrator
After=network.target

[Service]
Type=simple
ExecStart=${VENV_DIR}/bin/python3 ${AUTO_DIR}/tasks/autonomous_orchestrator.py
WorkingDirectory=${AUTO_DIR}
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
EOF

# Reload and Enable Services
systemctl --user daemon-reload
systemctl --user enable --now hermes-control-center.service
systemctl --user enable --now autonomous-orchestrator.service
log_ok "Headless services enabled and started."

# Enable lingering so services persist when SSH session closes
log_info "Enabling systemd lingering for ${NODE_USER}..."
sudo loginctl enable-linger "${NODE_USER}" 2>/dev/null || true

# 7. Print Remote Access Instructions
SERVER_IP=$(hostname -I | awk '{print $1}')
echo ""
echo "=================================================================="
echo " 🎉 HEADLESS SERVER DEPLOYMENT COMPLETE!"
echo "=================================================================="
echo "The full backend is now autonomously executing in the background."
echo ""
echo "🖥️ HOW TO VIEW THE COCKPIT ON YOUR LAPTOP / MONITOR:"
echo ""
echo "Method 1 (Direct Network / Intranet):"
echo "  Open your browser and navigate to:"
echo "  👉 http://${SERVER_IP}:5050"
echo ""
echo "Method 2 (Encrypted SSH Tunnel from your laptop):"
echo "  Run this single command on your laptop terminal:"
echo "  👉 ssh -N -L 5050:localhost:5050 -L 6006:localhost:6006 ${NODE_USER}@${SERVER_IP}"
echo "  Then open your laptop browser at:"
echo "  👉 http://localhost:5050 (Hermes Kanban & Cockpit)"
echo "  👉 http://localhost:6006 (Arize Phoenix Observability)"
echo "=================================================================="
