#!/usr/bin/env bash
# ==============================================================================
# Master Automation Runner (Research, Quota Scheduling & Extensible Tasks)
# Location: run_automation.sh
# ==============================================================================
set -euo pipefail

AUTO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCHEDULER="${AUTO_DIR}/scheduler.py"
PYTHON_BIN="python3"
if [[ -f "${HOME}/.phoenix-venv/bin/python3" ]]; then
    PYTHON_BIN="${HOME}/.phoenix-venv/bin/python3"
elif command -v python3 &>/dev/null; then
    PYTHON_BIN="$(command -v python3)"
fi

show_help() {
    echo "Usage: ./run_automation.sh [status | loop | planner | executor | once | notes | boards | research | watch | all]"
    echo ""
    echo "Commands:"
    echo "  status               Show active router service, accounts, quotas, and registered tasks"
    echo "  loop                 Continuous multi-repo orchestrator (Prioritized Gemini worker + Roving Opus planner)"
    echo "  planner              Run a strategic planning audit pass across all repositories (Opus 4.6)"
    echo "  executor             Run prioritized execution worker on pending milestones (Gemini 3.8)"
    echo "  once                 Execute a single pass across repositories and exit"
    echo "  notes                Launch 3 live auto-updating sticky notes on Desktop"
    echo "  boards               Update & render the Control Tower Dashboards"
    echo "  research             Run/Resume the PhD Research & Persian LaTeX generation pipeline"
    echo "  watch                Start continuous dashboard & goal watcher daemon"
    echo "  all                  Run research check + all background modular tasks"
    echo ""
}

ACTION="${1:-status}"

case "$ACTION" in
    status)
        python3 "$SCHEDULER" status
        ;;
    loop)
        "$PYTHON_BIN" "${AUTO_DIR}/tasks/autonomous_orchestrator.py"
        ;;
    planner)
        "$PYTHON_BIN" "${AUTO_DIR}/tasks/autonomous_orchestrator.py" --planner
        ;;
    executor)
        "$PYTHON_BIN" "${AUTO_DIR}/tasks/autonomous_orchestrator.py" --executor
        ;;
    once)
        "$PYTHON_BIN" "${AUTO_DIR}/tasks/autonomous_orchestrator.py" --once
        ;;
    notes)
        python3 "${AUTO_DIR}/desktop_sticky_notes.py"
        ;;
    boards)
        python3 "${AUTO_DIR}/dashboards/board_engine.py"
        python3 "${AUTO_DIR}/dashboards/note_converter.py"
        ;;
    research)
        python3 "$SCHEDULER" research
        ;;
    watch)
        python3 "${AUTO_DIR}/watcher_daemon.py"
        ;;
    task)
        TASK_NAME="${2:-}"
        if [[ -z "$TASK_NAME" ]]; then
            echo "Error: Please specify the task file name located in ${AUTO_DIR}/tasks/"
            exit 1
        fi
        shift 2 || shift 1
        python3 "$SCHEDULER" run --task "$TASK_NAME" "$@"
        ;;
    all)
        echo "[+] Running all automation pipelines..."
        python3 "$SCHEDULER" status
        python3 "$SCHEDULER" research
        for t in "${AUTO_DIR}"/tasks/*; do
            if [[ -f "$t" ]]; then
                echo "[+] Running task: $(basename "$t")"
                python3 "$SCHEDULER" run --task "$(basename "$t")"
            fi
        done
        ;;
    *)
        show_help
        ;;
esac
