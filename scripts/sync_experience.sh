#!/usr/bin/env bash
# ==============================================================================
# Hermes Agent & Orchestrator: Collective Intelligence & Experience Sync Tool
# Location: scripts/sync_experience.sh
# Purpose: Exports, imports, or Git-syncs procedural skills, memories, negative
#          heuristics, and rate governor state across fleet nodes.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ORCHESTRATOR_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
HERMES_DIR="${HOME}/.hermes"
SKILLS_DIR="${HERMES_DIR}/skills"
MEMORIES_DIR="${HERMES_DIR}/memories"
SOUL_FILE="${HERMES_DIR}/SOUL.md"
DASHBOARDS_DIR="${ORCHESTRATOR_DIR}/dashboards"

EXPERIENCE_FILES=(
    "${DASHBOARDS_DIR}/architect_steering_history.json"
    "${DASHBOARDS_DIR}/failed_tasks_registry.json"
    "${DASHBOARDS_DIR}/rate_governor_state.json"
    "${DASHBOARDS_DIR}/quota_optimizer_state.json"
)

show_help() {
    echo "Hermes Experience & Intelligence Sync Tool"
    echo ""
    echo "Usage: $0 [export | import <archive> | git-init | status]"
    echo ""
    echo "Commands:"
    echo "  export [output.tar.gz]   Package learned skills, memories, and negative heuristics"
    echo "  import <archive.tar.gz>  Merge skills, memories, and learned heuristics on this machine"
    echo "  git-init <remote-url>    Initialize or link ~/.hermes/skills to a Git repository"
    echo "  status                   Show inventory of skills, memories, and heuristic entries"
    echo ""
}

cmd_status() {
    echo "=========================================================="
    echo "🧠 Hermes Fleet Intelligence Status"
    echo "=========================================================="
    
    # Skills count
    if [[ -d "$SKILLS_DIR" ]]; then
        local skill_count
        skill_count=$(find "$SKILLS_DIR" -name "SKILL.md" 2>/dev/null | wc -l)
        echo "✅ Procedural Skills:       ${skill_count} active SKILL.md modules"
    else
        echo "⚠️ Procedural Skills:       Directory not found ($SKILLS_DIR)"
    fi

    # Memories
    if [[ -d "$MEMORIES_DIR" ]]; then
        local mem_count
        mem_count=$(find "$MEMORIES_DIR" -type f 2>/dev/null | wc -l)
        echo "✅ Durable Memories:        ${mem_count} memory profiles"
    else
        echo "ℹ️ Durable Memories:        Not found ($MEMORIES_DIR)"
    fi

    # SOUL
    if [[ -f "$SOUL_FILE" ]]; then
        echo "✅ Agent Soul / Persona:    Present ($SOUL_FILE)"
    else
        echo "ℹ️ Agent Soul / Persona:    Default system prompt"
    fi

    # Orchestrator Negative Memory & Registry
    local steering_count=0
    local steering_file="${DASHBOARDS_DIR}/architect_steering_history.json"
    if [[ -f "$steering_file" ]]; then
        steering_count=$(jq 'length' "$steering_file" 2>/dev/null || echo 0)
    fi
    echo "✅ Negative Blacklist:      ${steering_count} steering constraints"

    local failed_count=0
    local failed_file="${DASHBOARDS_DIR}/failed_tasks_registry.json"
    if [[ -f "$failed_file" ]]; then
        failed_count=$(jq 'length' "$failed_file" 2>/dev/null || echo 0)
    fi
    echo "✅ Bugfix & Recovery Mem:   ${failed_count} recorded failure patterns"
    echo "=========================================================="
}

cmd_export() {
    local target_archive="${1:-hermes_experience_$(date +%Y%m%d_%H%M%S).tar.gz}"
    echo "[+] Packaging Hermes experience into: ${target_archive}..."
    
    local tmp_dir
    tmp_dir=$(mktemp -d /tmp/hermes_exp_XXXXXX)
    mkdir -p "${tmp_dir}/hermes" "${tmp_dir}/orchestrator"

    # 1. Copy skills (excluding locks and curator backups)
    if [[ -d "$SKILLS_DIR" ]]; then
        echo " -> Copying skills..."
        mkdir -p "${tmp_dir}/hermes/skills"
        rsync -a --exclude='.locks' --exclude='.curator_backups' "$SKILLS_DIR/" "${tmp_dir}/hermes/skills/"
    fi

    # 2. Copy memories and SOUL
    if [[ -d "$MEMORIES_DIR" ]]; then
        echo " -> Copying memories..."
        mkdir -p "${tmp_dir}/hermes/memories"
        rsync -a "$MEMORIES_DIR/" "${tmp_dir}/hermes/memories/"
    fi
    if [[ -f "$SOUL_FILE" ]]; then
        cp "$SOUL_FILE" "${tmp_dir}/hermes/SOUL.md"
    fi

    # 3. Copy orchestrator heuristic dashboards
    for f in "${EXPERIENCE_FILES[@]}"; do
        if [[ -f "$f" ]]; then
            echo " -> Copying $(basename "$f")..."
            cp "$f" "${tmp_dir}/orchestrator/"
        fi
    done

    # 4. Pack into tar.gz
    tar -czf "$target_archive" -C "$tmp_dir" hermes orchestrator
    rm -rf "$tmp_dir"

    echo "✅ Experience bundle successfully exported to: ${target_archive}"
    echo "   File size: $(du -h "$target_archive" | cut -f 1)"
}

cmd_import() {
    local archive="$1"
    if [[ ! -f "$archive" ]]; then
        echo "[-] Error: Archive file not found: $archive"
        exit 1
    fi

    echo "[+] Importing Hermes experience from: ${archive}..."
    local tmp_dir
    tmp_dir=$(mktemp -d /tmp/hermes_imp_XXXXXX)
    tar -xzf "$archive" -C "$tmp_dir"

    # 1. Merge skills
    if [[ -d "${tmp_dir}/hermes/skills" ]]; then
        mkdir -p "$SKILLS_DIR"
        echo " -> Merging procedural skills into ${SKILLS_DIR}..."
        rsync -av --update "${tmp_dir}/hermes/skills/" "${SKILLS_DIR}/"
    fi

    # 2. Merge memories
    if [[ -d "${tmp_dir}/hermes/memories" ]]; then
        mkdir -p "$MEMORIES_DIR"
        echo " -> Merging memories into ${MEMORIES_DIR}..."
        rsync -av --update "${tmp_dir}/hermes/memories/" "${MEMORIES_DIR}/"
    fi

    # 3. Soul
    if [[ -f "${tmp_dir}/hermes/SOUL.md" ]]; then
        if [[ ! -f "$SOUL_FILE" ]]; then
            cp "${tmp_dir}/hermes/SOUL.md" "$SOUL_FILE"
            echo " -> Installed agent SOUL.md"
        else
            echo " -> SOUL.md already exists, keeping existing (backup saved)"
            cp "${tmp_dir}/hermes/SOUL.md" "${SOUL_FILE}.imported"
        fi
    fi

    # 4. Merge orchestrator heuristics
    if [[ -d "${tmp_dir}/orchestrator" ]]; then
        mkdir -p "$DASHBOARDS_DIR"
        for src in "${tmp_dir}/orchestrator"/*.json; do
            if [[ -f "$src" ]]; then
                local bname
                bname=$(basename "$src")
                local dest="${DASHBOARDS_DIR}/${bname}"
                if [[ ! -f "$dest" ]]; then
                    cp "$src" "$dest"
                    echo " -> Initialized ${bname}"
                else
                    echo " -> Merging heuristics in ${bname}..."
                    python3 -c "
import json, os, sys
src = '$src'
dest = '$dest'
try:
    with open(src) as f1, open(dest) as f2:
        d1 = json.load(f1)
        d2 = json.load(f2)
    if isinstance(d1, list) and isinstance(d2, list):
        # Union list items by unique string representation
        seen = set(json.dumps(x, sort_keys=True) for x in d2)
        for item in d1:
            key = json.dumps(item, sort_keys=True)
            if key not in seen:
                d2.append(item)
                seen.add(key)
        with open(dest, 'w') as out:
            json.dump(d2, out, indent=2)
    elif isinstance(d1, dict) and isinstance(d2, dict):
        d1.update(d2)
        with open(dest, 'w') as out:
            json.dump(d1, out, indent=2)
except Exception as e:
    print('Warning: merge failed, keeping target:', e)
" 2>/dev/null || true
                fi
            fi
        done
    fi

    rm -rf "$tmp_dir"
    echo "✅ Hermes experience imported and merged successfully!"
    cmd_status
}

cmd_git_init() {
    local remote_url="${1:-}"
    mkdir -p "$SKILLS_DIR"
    cd "$SKILLS_DIR"

    if [[ ! -d ".git" ]]; then
        echo "[+] Initializing Git repository in skills directory..."
        git init
        git config user.name "Hermes Agent Fleet"
        git config user.email "hermes-fleet@local"
        echo ".locks/" > .gitignore
        echo ".curator_backups/" >> .gitignore
        git add .
        git commit -m "feat(skills): initial snapshot of Hermes agent procedural skills" || true
    fi

    if [[ -n "$remote_url" ]]; then
        echo "[+] Setting remote origin: $remote_url"
        git remote remove origin 2>/dev/null || true
        git remote add origin "$remote_url"
        echo "[+] To synchronize skills across nodes, run:"
        echo "    cd ~/.hermes/skills && git pull origin main && git push origin main"
    fi
}

ACTION="${1:-status}"
case "$ACTION" in
    status)
        cmd_status
        ;;
    export)
        shift 1 || true
        cmd_export "$@"
        ;;
    import)
        if [[ $# -lt 2 ]]; then
            echo "[-] Error: Please specify the archive file to import."
            echo "    Usage: $0 import /path/to/hermes_experience.tar.gz"
            exit 1
        fi
        cmd_import "$2"
        ;;
    git-init)
        shift 1 || true
        cmd_git_init "$@"
        ;;
    *)
        show_help
        ;;
esac
