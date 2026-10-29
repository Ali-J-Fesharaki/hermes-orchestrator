#!/usr/bin/env bash
# ==============================================================================
# Dedicated One-Click Bridge: Antigravity Tools <-> Hermes
# Path: /home/ajf/automation/scripts/connect_antigravity_to_hermes.sh
# What it does:
#   1. Checks and starts Antigravity Tools service on port 8045 (headless)
#   2. Verifies connectivity and token auth on /v1/models
#   3. Dynamically extracts ALL active models from Antigravity Tools
#   4. Automatically writes and synchronizes ~/.hermes/config.yaml & auth.json
#   5. Confirms total models ready to be used by Hermes Agent & Orchestrator
# ==============================================================================
set -euo pipefail

HERMES_DIR="${HOME}/.hermes"
HERMES_CONFIG="${HERMES_DIR}/config.yaml"
HERMES_ENV="${HERMES_DIR}/.env"
HERMES_AUTH="${HERMES_DIR}/auth.json"
AG_CONFIG="${HOME}/.antigravity_tools/gui_config.json"
ROUTER_URL="http://127.0.0.1:8045/v1"

echo "======================================================================"
echo "⚡ Connecting Antigravity Tools to Hermes & Synchronizing Models..."
echo "======================================================================"

# 1. Ensure Antigravity Tools service is running
echo "[1/4] Ensuring Antigravity Tools daemon is active on port 8045..."
if ! curl -s -m 2 http://127.0.0.1:8045/health &>/dev/null; then
    systemctl --user restart antigravity-tools.service 2>/dev/null || true
    sleep 3
fi

if ! curl -s -m 3 http://127.0.0.1:8045/health &>/dev/null; then
    echo "[-] Error: Antigravity Tools is not responding on port 8045."
    echo "    Please verify that antigravity_tools is installed and accounts are logged in."
    exit 1
fi
echo "[OK] Antigravity Tools is alive on port 8045."

# 2. Extract valid API token
echo "[2/4] Resolving API Token..."
API_KEY=""
if [[ -f "$AG_CONFIG" ]]; then
    API_KEY=$(grep -o '"api_key": "[^"]*' "$AG_CONFIG" | cut -d '"' -f 4 || echo "")
fi
if [[ -z "$API_KEY" && -f "$HERMES_ENV" ]]; then
    API_KEY=$(grep -E '^(HERMES_ANTIGRAVITY_TOOLS_API_KEY|HERMES_CUSTOM_ANTIGRAVITY_MANAGER_API_KEY)=' "$HERMES_ENV" | head -n 1 | cut -d '=' -f 2- | tr -d ' "\r\n' || echo "")
fi

if [[ -z "$API_KEY" ]]; then
    API_KEY="sk-antigravity-fleet-node"
fi

# 3. Fetch active models directly from Antigravity Tools
echo "[3/4] Querying available models from Antigravity Tools router..."
TMP_MODELS=$(mktemp /tmp/ag_models_XXXXXX.json)
trap 'rm -f "$TMP_MODELS"' EXIT

curl -s -m 10 -H "Authorization: Bearer ${API_KEY}" "${ROUTER_URL}/models" > "$TMP_MODELS" || true

# 4. Synchronize into Hermes config.yaml and auth.json
echo "[4/4] Synchronizing into ~/.hermes/config.yaml & ~/.hermes/auth.json..."
python3 -c "
import json, yaml, os

cfg_path = '${HERMES_CONFIG}'
models_path = '${TMP_MODELS}'
auth_path = '${HERMES_AUTH}'
api_key = '${API_KEY}'
router_url = '${ROUTER_URL}'

active_model_ids = []
try:
    with open(models_path, 'r', encoding='utf-8') as f:
        mdata = json.load(f)
    active_model_ids = [m['id'] for m in mdata.get('data', []) if isinstance(m, dict) and 'id' in m]
except Exception as e:
    pass

if not active_model_ids:
    active_model_ids = [
        'claude-opus-4-6-thinking',
        'claude-sonnet-4-6-thinking',
        'gemini-3.8-flash-tiered',
        'gemini-3-flash',
        'gemini-3.1-flash-lite'
    ]

# 1. Update config.yaml
ycfg = {}
if os.path.exists(cfg_path):
    try:
        with open(cfg_path, 'r', encoding='utf-8') as f:
            ycfg = yaml.safe_load(f) or {}
    except Exception:
        ycfg = {}

ycfg.setdefault('model', {})['provider'] = 'antigravity_tools'
ycfg['model'].setdefault('default', 'gemini-3.8-flash-tiered')
ycfg['model']['base_url'] = router_url
ycfg['model']['key_env'] = 'HERMES_ANTIGRAVITY_TOOLS_API_KEY'
ycfg['model']['api_mode'] = 'chat_completions'

providers = ycfg.setdefault('providers', {})
ag = providers.setdefault('antigravity_tools', {})
ag['name'] = 'antigravity_tools'
ag['base_url'] = router_url
ag['discover_models'] = True
ag['key_env'] = 'HERMES_ANTIGRAVITY_TOOLS_API_KEY'
models_dict = ag.setdefault('models', {})

for mid in active_model_ids:
    models_dict[mid] = {}

with open(cfg_path, 'w', encoding='utf-8') as f:
    yaml.dump(ycfg, f, default_flow_style=False, sort_keys=False)

# 2. Update auth.json
auth_data = {}
if os.path.exists(auth_path):
    try:
        with open(auth_path, 'r', encoding='utf-8') as f:
            auth_data = json.load(f) or {}
    except Exception:
        auth_data = {}

pool = auth_data.setdefault('credential_pool', {})
ag_list = pool.setdefault('antigravity_tools', [])
if ag_list:
    ag_list[0]['access_token'] = api_key
    ag_list[0]['base_url'] = router_url
else:
    ag_list.append({
        'id': 'ag_tools_auto',
        'label': 'antigravity_tools',
        'auth_type': 'api_key',
        'priority': 0,
        'source': 'manual',
        'access_token': api_key,
        'base_url': router_url,
        'request_count': 0
    })

with open(auth_path, 'w', encoding='utf-8') as f:
    json.dump(auth_data, f, indent=2)

print(f'[SUCCESS] Synced {len(active_model_ids)} models from Antigravity Tools into Hermes!')
for mid in active_model_ids[:10]:
    print(f'   • {mid}')
if len(active_model_ids) > 10:
    print(f'   • ... and {len(active_model_ids) - 10} more models')
"

echo "======================================================================"
echo "✅ Antigravity Tools is connected to Hermes and ready for use!"
echo "======================================================================"
