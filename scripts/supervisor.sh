#!/usr/bin/env bash
# supervisor.sh: On-Demand Control for Platform Supervisor Agent
# Usage:
#   ./supervisor.sh start     -> Starts 10-minute automated auditing
#   ./supervisor.sh stop      -> Stops automated auditing
#   ./supervisor.sh status    -> Shows current status and last audit
#   ./supervisor.sh once      -> Runs a single audit right now

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AUTO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PYTHON_BIN="/usr/bin/python3"
AGENT_SCRIPT="${AUTO_DIR}/tasks/platform_supervisor_agent.py"
PID_FILE="/tmp/platform_supervisor_loop.pid"

action="${1:-status}"

case "$action" in
    start)
        echo "=================================================================="
        echo " 🛡️  STARTING PLATFORM SUPERVISOR AGENT"
        echo "=================================================================="
        
        # 1. Run an immediate audit right now
        echo "[*] Running initial pre-flight audit..."
        PYTHONPATH="${AUTO_DIR}:${AUTO_DIR}/dashboards:${AUTO_DIR}/tasks" \
            "${PYTHON_BIN}" "${AGENT_SCRIPT}" || true

        # 2. Start the 10-minute systemd timer (clean user-level daemon)
        systemctl --user start platform-supervisor.timer
        
        echo ""
        echo "✅ [SUCCESS] Platform Supervisor Agent is now active!"
        echo "• Schedule: Auditing every 10 minutes in the background."
        echo "• Stop anytime by running: ${AUTO_DIR}/stop_supervisor.sh"
        echo "=================================================================="
        ;;

    stop)
        echo "=================================================================="
        echo " 🛑 STOPPING PLATFORM SUPERVISOR AGENT"
        echo "=================================================================="
        
        # Stop systemd timer & service
        systemctl --user stop platform-supervisor.timer 2>/dev/null || true
        systemctl --user stop platform-supervisor.service 2>/dev/null || true

        # Stop any standalone background loop if running
        if [[ -f "${PID_FILE}" ]]; then
            pid="$(cat "${PID_FILE}")"
            if kill -0 "${pid}" 2>/dev/null; then
                kill "${pid}" 2>/dev/null || true
            fi
            rm -f "${PID_FILE}"
        fi

        echo "✅ [SUCCESS] Platform Supervisor Agent has been stopped."
        echo "• No background audits will run until you start it again."
        echo "=================================================================="
        ;;

    status)
        echo "=================================================================="
        echo " 🔍 PLATFORM SUPERVISOR AGENT STATUS"
        echo "=================================================================="
        
        timer_state="$(systemctl --user is-active platform-supervisor.timer 2>/dev/null || echo "inactive")"
        if [[ "$timer_state" == "active" ]]; then
            echo "• Status: 🟢 ACTIVE (Running in background every 10 minutes)"
            next_run="$(systemctl --user list-timers platform-supervisor.timer --no-pager 2>/dev/null | grep platform-supervisor || true)"
            if [[ -n "$next_run" ]]; then
                echo "• Next trigger info: ${next_run}"
            fi
        else
            echo "• Status: ⚪ STOPPED (Inactive / Standby)"
        fi

        REPORT_FILE="${AUTO_DIR}/dashboards/SUPERVISOR_AUDIT_REPORT.json"
        if [[ -f "${REPORT_FILE}" ]]; then
            echo ""
            echo "--- Latest Audit Report ---"
            python3 -c "
import json
try:
    with open('${REPORT_FILE}') as f:
        d = json.load(f)
    print(f\"• Overall: {d.get('supervisor_status')}\")
    print(f\"• Healthy Components: {d.get('healthy_components')}\")
    print(f\"• Last Checked: {d.get('audit_timestamp')}\")
    print(f\"• Critical Failures: {d.get('critical_failures_count')}\")
except Exception as e:
    print('Could not parse report:', e)
"
        fi
        echo "=================================================================="
        ;;

    once)
        echo "[*] Running single on-demand audit..."
        PYTHONPATH="${AUTO_DIR}:${AUTO_DIR}/dashboards:${AUTO_DIR}/tasks" \
            "${PYTHON_BIN}" "${AGENT_SCRIPT}"
        ;;

    *)
        echo "Usage: $0 {start|stop|status|once}"
        exit 1
        ;;
esac
