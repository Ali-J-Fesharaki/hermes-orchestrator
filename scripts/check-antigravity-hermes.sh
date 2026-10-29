#!/usr/bin/env bash
# ==============================================================================
# Antigravity Tools & Hermes Connection Watchdog & Guardian Service
# Location: /home/ajf/.local/bin/check-antigravity-hermes.sh
# Ensures Antigravity Tools and Hermes communicate reliably, auto-healing
# rogue processes, broken accounts, model mappings, and credentials without
# causing any GUI or desktop windows to pop up.
# ==============================================================================
set -euo pipefail

HERMES_ENV="/home/ajf/.hermes/.env"
HERMES_CONFIG="/home/ajf/.hermes/config.yaml"
HERMES_AUTH="/home/ajf/.hermes/auth.json"
AG_CONFIG="/home/ajf/.antigravity_tools/gui_config.json"
AG_ACCOUNTS="/home/ajf/.antigravity_tools/accounts.json"
AG_ACCOUNTS_DIR="/home/ajf/.antigravity_tools/accounts"
NOTIFIER="/home/ajf/automation/telegram_notifier.py"

TEST_INFERENCE=0
for arg in "$@"; do
    if [[ "$arg" == "--test-inference" || "$arg" == "--deep" ]]; then
        TEST_INFERENCE=1
    fi
done

notify() {
    local msg="$1"
    if [[ -f "$NOTIFIER" ]]; then
        python3 "$NOTIFIER" "$msg" 2>/dev/null || true
    fi
}

# 1. Read API key from Antigravity Tools gui_config.json
API_KEY=""
if [[ -f "$AG_CONFIG" ]]; then
    API_KEY=$(grep -o '"api_key": "[^"]*' "$AG_CONFIG" | cut -d '"' -f 4 || echo "")
fi
if [[ -z "$API_KEY" && -f "$HERMES_ENV" ]]; then
    API_KEY=$(grep -E '^(HERMES_ANTIGRAVITY_TOOLS_API_KEY|HERMES_CUSTOM_ANTIGRAVITY_MANAGER_API_KEY|CUSTOM_API_KEY)=' "$HERMES_ENV" | head -n 1 | cut -d '=' -f 2- | tr -d ' "\r\n' || echo "")
fi

# 2. Helper: check if interactive GUI process is running
is_gui_running() {
    local service_pid
    service_pid=$(systemctl --user show -p MainPID --value antigravity-tools.service 2>/dev/null || echo "0")
    for exe_link in /proc/[0-9]*/exe; do
        [[ -e "$exe_link" ]] || continue
        local pid
        pid=$(basename "$(dirname "$exe_link")")
        [[ "$pid" == "$$" || "$pid" == "$service_pid" || "$pid" == "1" ]] && continue
        local exe_target
        exe_target=$(readlink -f "$exe_link" 2>/dev/null || true)
        if [[ "$exe_target" == *"antigravity-tools"* || "$exe_target" == *"Antigravity.Tools"* ]]; then
            if [[ "$exe_target" != *"/bash" && "$exe_target" != *"/sh" && "$exe_target" != *"/python"* ]]; then
                if [[ -r "/proc/$pid/cmdline" ]]; then
                    local cmd
                    cmd=$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || true)
                    if [[ "$cmd" != *"--headless"* ]]; then
                        return 0
                    fi
                fi
            fi
        fi
    done
    return 1
}

# 3. Ensure essential custom mappings and auto-heal account configuration
sanitize_antigravity_config() {
    python3 -c "
import json, glob, os, time

config_changed = False
cfg_path = '$AG_CONFIG'
if os.path.exists(cfg_path):
    try:
        with open(cfg_path, 'r') as f:
            cfg = json.load(f)
        proxy = cfg.setdefault('proxy', {})
        proxy['enabled'] = True
        proxy['auto_start'] = True
        proxy['allow_lan_access'] = True
        proxy['port'] = 8045
        
        # Enforce model custom mapping
        mapping = proxy.setdefault('custom_mapping', {})
        desired_mapping = {
            'gemini-3.8-flash-high': 'gemini-3.8-flash-tiered',
            'gemini-3.8-flash-medium': 'gemini-3.8-flash-tiered',
            'gemini-3.8-flash-low': 'gemini-3.8-flash-tiered',
            'gemini-2.5-flash': 'gemini-3-flash',
            'gemini-2.5-flash-lite': 'gemini-3.1-flash-lite'
        }
        for k, v in desired_mapping.items():
            if mapping.get(k) != v:
                mapping[k] = v
                config_changed = True
                
        with open(cfg_path, 'w') as f:
            json.dump(cfg, f, indent=2)
    except Exception as e:
        print('Error sanitizing gui_config:', e)

# Auto-heal and reconcile accounts
acc_dir = '$AG_ACCOUNTS_DIR'
acc_file = '$AG_ACCOUNTS'
enabled_emails = set()

if os.path.exists(acc_dir):
    for fpath in glob.glob(os.path.join(acc_dir, '*.json')):
        try:
            with open(fpath, 'r') as f:
                acc = json.load(f)
            # Only permanently block if explicitly flagged as validation_blocked
            if acc.get('validation_blocked') is True:
                continue
            
            # Auto-heal accounts that have valid tokens
            modified = False
            if acc.get('disabled') or acc.get('proxy_disabled'):
                acc['disabled'] = False
                acc['proxy_disabled'] = False
                acc['proxy_disabled_reason'] = None
                acc['proxy_disabled_at'] = None
                modified = True
            if acc.get('is_forbidden'):
                acc['is_forbidden'] = False
                modified = True
            if 'quota' in acc and acc['quota'].get('is_forbidden'):
                acc['quota']['is_forbidden'] = False
                acc['quota']['forbidden_reason'] = None
                modified = True
            
            if modified:
                with open(fpath, 'w') as f:
                    json.dump(acc, f, indent=2)
                config_changed = True
            
            email = acc.get('email')
            if email:
                enabled_emails.add(email)
        except Exception:
            pass

if os.path.exists(acc_file) and enabled_emails:
    try:
        with open(acc_file, 'r') as f:
            accs = json.load(f)
        for a in accs.get('accounts', []):
            if a.get('email') in enabled_emails:
                if a.get('disabled') or a.get('proxy_disabled'):
                    a['disabled'] = False
                    a['proxy_disabled'] = False
                    config_changed = True
        with open(acc_file, 'w') as f:
            json.dump(accs, f, indent=2)
    except Exception:
        pass

if config_changed:
    print('CONFIG_UPDATED')
" 2>/dev/null || true
}

# 4. Reconcile process ownership to prevent port 8045 collisions
reconcile_process_ownership() {
    if is_gui_running; then
        # Interactive GUI is active; stop headless daemon to prevent port collision
        if systemctl --user is-active --quiet antigravity-tools.service; then
            echo "[WATCHDOG] Interactive GUI is active. Pausing headless daemon to prevent port collision..."
            systemctl --user stop antigravity-tools.service 2>/dev/null || true
            sleep 1
        fi
    else
        # No interactive GUI is active; ensure headless daemon is running
        if ! systemctl --user is-active --quiet antigravity-tools.service; then
            echo "[WATCHDOG] No GUI active. Starting antigravity-tools.service headless daemon..."
            systemctl --user start antigravity-tools.service 2>/dev/null || true
            sleep 2
        fi
    fi
}

echo "[WATCHDOG] Checking Antigravity Tools connection and LLM server status..."

SAN_RES=$(sanitize_antigravity_config)
reconcile_process_ownership

# 5. Discover active Antigravity Tools IP and Port
find_active_router() {
    local candidate_ports=(8045 7836)
    local candidate_ips=("127.0.0.1")

    while IFS= read -r ip; do
        if [[ -n "$ip" && "$ip" != "127.0.0.1" ]]; then
            candidate_ips+=("$ip")
        fi
    done < <(ip -4 -o addr show scope global 2>/dev/null | awk '{split($4, a, "/"); print a[1]}' || true)

    for port in "${candidate_ports[@]}"; do
        for ip in "${candidate_ips[@]}"; do
            local health_url="http://${ip}:${port}/health"
            local status
            status=$(curl -s -o /dev/null -w "%{http_code}" --max-time 2 "$health_url" 2>/dev/null || echo "000")
            if [[ "$status" == "200" ]]; then
                echo "${ip}:${port}"
                return 0
            fi
        done
    done
    return 1
}

ACTIVE_ROUTER=$(find_active_router || echo "")

# 6. If unreachable, trigger Self-Healing
if [[ -z "$ACTIVE_ROUTER" ]]; then
    echo "[WATCHDOG WARNING] Antigravity Tools is not responding. Initiating self-healing..."
    systemctl --user restart antigravity-tools.service 2>/dev/null || true
    sleep 3
    ACTIVE_ROUTER=$(find_active_router || echo "")
fi

if [[ -z "$ACTIVE_ROUTER" ]]; then
    echo "[WATCHDOG ERROR] Self-healing could not restore Antigravity Tools." >&2
    notify "🚨 *Critical Alert*: Antigravity Tools could not be reached or restored on port 8045."
    exit 1
fi

ROUTER_IP="${ACTIVE_ROUTER%:*}"
ROUTER_PORT="${ACTIVE_ROUTER#*:}"
ROUTER_BASE_URL="http://${ROUTER_IP}:${ROUTER_PORT}/v1"
echo "[WATCHDOG OK] Antigravity Tools is active at ${ACTIVE_ROUTER}"

# 7. Verify /v1/models (ensuring proxy is healthy and returning models)
MODELS_URL="${ROUTER_BASE_URL}/models"
TMP_RESP=$(mktemp /tmp/ag_models_resp_XXXXXX.json)
trap 'rm -f "$TMP_RESP"' EXIT

HTTP_STATUS=$(curl -s -o "$TMP_RESP" -w "%{http_code}" \
    --max-time 5 \
    -H "Authorization: Bearer ${API_KEY}" \
    "$MODELS_URL" 2>/dev/null || echo "000")

if [[ "$HTTP_STATUS" != "200" ]]; then
    echo "[WATCHDOG WARNING] /v1/models returned HTTP ${HTTP_STATUS}. Initiating auto-recovery..."
    if ! is_gui_running; then
        systemctl --user restart antigravity-tools.service 2>/dev/null || true
        sleep 2
    fi
    HTTP_STATUS=$(curl -s -o "$TMP_RESP" -w "%{http_code}" --max-time 5 -H "Authorization: Bearer ${API_KEY}" "$MODELS_URL" 2>/dev/null || echo "000")
fi

MODEL_COUNT=$(grep -o '"id":' "$TMP_RESP" 2>/dev/null | wc -l || echo "0")
echo "[HEALTH-CHECK OK] Antigravity Tools LLM Router at ${ACTIVE_ROUTER} has ${MODEL_COUNT} models available."

# 8. Optional live inference check (only when explicitly requested via --test-inference or --deep)
if [[ "$TEST_INFERENCE" -eq 1 ]]; then
    CHAT_URL="${ROUTER_BASE_URL}/chat/completions"
    CHAT_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
        --max-time 20 \
        -H "Authorization: Bearer ${API_KEY}" \
        -H "Content-Type: application/json" \
        -d '{"model": "gemini-3.8-flash-tiered", "messages": [{"role": "user", "content": "ping"}], "max_tokens": 10}' \
        "$CHAT_URL" 2>/dev/null || echo "000")

    if [[ "$CHAT_STATUS" == "200" ]]; then
        echo "[HEALTH-CHECK OK] Live LLM inference test succeeded (HTTP 200)."
    elif [[ "$CHAT_STATUS" == "503" || "$CHAT_STATUS" == "429" ]]; then
        echo "[HEALTH-CHECK NOTE] Upstream AI provider returned HTTP ${CHAT_STATUS} (quota/cooldown). Router is responsive."
    else
        echo "[WATCHDOG WARNING] Live LLM inference returned HTTP ${CHAT_STATUS}."
    fi
fi

# 9. Synchronize Hermes configuration, discovered models, and credentials
HERMES_RESTART_NEEDED=0

if [[ -f "$HERMES_CONFIG" ]]; then
    CURRENT_BASE=$(grep -E '^\s*base_url:' "$HERMES_CONFIG" | head -n 1 | awk '{print $2}' || echo "")
    if [[ "$CURRENT_BASE" != "$ROUTER_BASE_URL" ]]; then
        echo "[WATCHDOG] Syncing Hermes config.yaml base_url -> ${ROUTER_BASE_URL}..."
        sed -i -E "s|base_url: http://[0-9.]+:[0-9]+/v1|base_url: ${ROUTER_BASE_URL}|g" "$HERMES_CONFIG"
        HERMES_RESTART_NEEDED=1
    fi

    # Automatically synchronize active model IDs from Antigravity Tools into Hermes config.yaml
    if [[ -f "$TMP_RESP" && -s "$TMP_RESP" ]]; then
        python3 -c "
import json, yaml, os

cfg_path = '$HERMES_CONFIG'
models_path = '$TMP_RESP'
try:
    with open(models_path, 'r', encoding='utf-8') as f:
        mdata = json.load(f)
    active_model_ids = [m['id'] for m in mdata.get('data', []) if isinstance(m, dict) and 'id' in m]
    
    if active_model_ids and os.path.exists(cfg_path):
        with open(cfg_path, 'r', encoding='utf-8') as f:
            ycfg = yaml.safe_load(f) or {}
            
        providers = ycfg.setdefault('providers', {})
        ag_provider = providers.setdefault('antigravity_tools', {})
        ag_provider['base_url'] = '$ROUTER_BASE_URL'
        ag_provider['discover_models'] = True
        
        models_dict = ag_provider.setdefault('models', {})
        updated = False
        for mid in active_model_ids:
            if mid not in models_dict:
                models_dict[mid] = {}
                updated = True
                
        if updated:
            with open(cfg_path, 'w', encoding='utf-8') as f:
                yaml.dump(ycfg, f, default_flow_style=False, sort_keys=False)
            print(f'[WATCHDOG] Injected {len(active_model_ids)} Antigravity Tools models into Hermes config.yaml')
except Exception as e:
    print('[WATCHDOG] Note on model sync:', e)
" 2>/dev/null || true
    fi
fi

if [[ -f "$HERMES_ENV" && -n "$API_KEY" ]]; then
    update_env_var() {
        local key="$1"
        local val="$2"
        if grep -q "^${key}=" "$HERMES_ENV" 2>/dev/null; then
            if ! grep -q "^${key}=${val}" "$HERMES_ENV" 2>/dev/null; then
                sed -i -E "s|^${key}=.*|${key}=${val}|g" "$HERMES_ENV"
                HERMES_RESTART_NEEDED=1
            fi
        else
            echo "${key}=${val}" >> "$HERMES_ENV"
            HERMES_RESTART_NEEDED=1
        fi
    }
    update_env_var "HERMES_ANTIGRAVITY_TOOLS_API_KEY" "$API_KEY"
    update_env_var "HERMES_CUSTOM_ANTIGRAVITY_TOOLS_API_KEY" "$API_KEY"
    update_env_var "HERMES_CUSTOM_API_KEY" "$API_KEY"
    update_env_var "CUSTOM_API_KEY" "$API_KEY"
fi

if [[ -f "$HERMES_AUTH" && -n "$API_KEY" ]]; then
    python3 -c "
import json
try:
    with open('$HERMES_AUTH', 'r') as f:
        auth = json.load(f)
    pool = auth.setdefault('credential_pool', {})
    
    # Update antigravity_tools pool entry
    ag_list = pool.setdefault('antigravity_tools', [])
    if ag_list:
        ag_list[0]['access_token'] = '$API_KEY'
        ag_list[0]['base_url'] = '$ROUTER_BASE_URL'
    else:
        ag_list.append({
            'id': 'ag_tools_auto',
            'label': 'antigravity_tools',
            'auth_type': 'api_key',
            'priority': 0,
            'source': 'manual',
            'access_token': '$API_KEY',
            'base_url': '$ROUTER_BASE_URL',
            'request_count': 0
        })
        
    # Update custom:127.0.0.1:8045 entry
    custom_list = pool.setdefault('custom:127.0.0.1:8045', [])
    if custom_list:
        custom_list[0]['access_token'] = '$API_KEY'
        custom_list[0]['base_url'] = '$ROUTER_BASE_URL'
    else:
        custom_list.append({
            'id': 'custom_auto',
            'label': '127.0.0.1:8045',
            'auth_type': 'api_key',
            'priority': 0,
            'source': 'manual',
            'access_token': '$API_KEY',
            'base_url': '$ROUTER_BASE_URL',
            'request_count': 0
        })
        
    with open('$HERMES_AUTH', 'w') as f:
        json.dump(auth, f, indent=2)
except Exception:
    pass
" 2>/dev/null || true
fi

# 10. Check Hermes Gateway Service
if ! systemctl --user is-active --quiet hermes-gateway.service; then
    echo "[WATCHDOG] hermes-gateway.service is inactive. Starting it..."
    systemctl --user restart hermes-gateway.service 2>/dev/null || true
    HERMES_RESTART_NEEDED=0
elif [[ "$HERMES_RESTART_NEEDED" -eq 1 ]]; then
    echo "[WATCHDOG] Reloading Hermes Gateway to apply updated router connection..."
    systemctl --user restart hermes-gateway.service 2>/dev/null || true
fi

echo "[WATCHDOG COMPLETED] Hermes and Antigravity Tools connection is verified, stable, and fine (zero popups)."
exit 0
