#!/usr/bin/env python3
"""
board_engine.py: Multi-Repository Control Tower Dashboard Engine.

Generates:
  1. ROADMAP_BOARD.md: Dynamic milestone status (Done, In-Progress, Added by Opus).
  2. TOKEN_FIT_BOARD.md: Granular Token Consumption (In/Out/Reasoning) per Repo & Agent.
  3. SCALING_ADVISOR.md: Hardware and Account Capacity Advisor.
  4. HUMAN_ACTION_BOARD.md: Actionable unblockers and human inputs needed.
"""

import os
import sys
import json
import time
import sqlite3
import subprocess
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
AUTO_DIR = BASE_DIR.parent
REPOS_FILE = BASE_DIR / "repositories.json"
DB_PATH = Path.home() / ".antigravity-agent/cloud_accounts.db"
NOTIFIER = AUTO_DIR / "telegram_notifier.py"

# Import Telemetry
try:
    from telemetry import load_telemetry
except ImportError:
    sys.path.insert(0, str(BASE_DIR))
    from telemetry import load_telemetry

# Constants for Google Pro Fleet
TOKENS_PER_ACCOUNT_PER_5H = 150000
CYCLES_PER_DAY = 3
DAILY_CAPACITY_PER_ACCOUNT = TOKENS_PER_ACCOUNT_PER_5H * CYCLES_PER_DAY

def load_repos():
    if not REPOS_FILE.exists():
        return []
    with open(REPOS_FILE, "r") as f:
        repos = json.load(f).get("repositories", [])
    repos.sort(key=lambda r: r.get("priority", 99))
    return repos

def get_git_info(repo_path):
    p = Path(repo_path)
    if not p.exists() or not (p / ".git").exists():
        return {"branch": "unknown", "commit": "none", "clean": True}
    
    branch = subprocess.run(["git", "branch", "--show-current"], cwd=p, capture_output=True, text=True).stdout.strip()
    commit = subprocess.run(["git", "log", "-1", "--format=%h %s"], cwd=p, capture_output=True, text=True).stdout.strip()
    status = subprocess.run(["git", "status", "--porcelain"], cwd=p, capture_output=True, text=True).stdout.strip()
    return {"branch": branch, "commit": commit, "clean": len(status) == 0}

def get_account_count():
    if not DB_PATH.exists():
        return 4
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM accounts WHERE status='active'")
        count = cur.fetchone()[0]
        conn.close()
        return count or 4
    except Exception:
        return 4

# ==============================================================================
# 📌 DASHBOARD 1: ROADMAP & DYNAMIC MILESTONE TRACKER
# ==============================================================================
def generate_roadmap_board(repos, telemetry):
    active_agents = telemetry.get("active_agents", {})
    
    lines = [
        "# Dashboard 1: Multi-Repository Development & Roadmap",
        f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  ",
        f"**Active Executing Agents:** **{len(active_agents)} agent(s) currently working**\n",
        "| Priority | Repository | Active Branch | Planner | Executor | Auto-Approve | Working Tree | Status |",
        "| :---: | :--- | :--- | :--- | :--- | :---: | :--- | :--- |"
    ]
    
    for r in repos:
        git_info = get_git_info(r["path"])
        tree_status = "🟢 Clean" if git_info["clean"] else "🟡 Modified"
        prio = r.get("priority", 99)
        auto_appr = "⚡ ON" if r.get("auto_approve_plans") else "⏸️ Manual"
        planner_m = f"`{r.get('planner_model')}`" if r.get('planner_model') else "🌟 Cascade"
        executor_m = f"`{r.get('executor_model')}`" if r.get('executor_model') else "`gemini-3.8-flash`"
        lines.append(f"| **P{prio}** | **{r['name']}** | `{git_info['branch']}` | {planner_m} | {executor_m} | {auto_appr} | {tree_status} | Active |")
        
    lines.append("\n---\n\n## Repository Milestones Breakdown\n")
    
    for r in repos:
        prio = r.get("priority", 99)
        lines.append(f"### 📂 [Priority {prio}] {r['name']} (`{r['path']}`)")
        lines.append("| Milestone | Status | Details / Technical Deliverable | Output Artifact |")
        lines.append("| :--- | :---: | :--- | :--- |")
        
        # Check if an agent is currently executing a task in this repo
        working_tasks = [a["current_task"] for a in active_agents.values() if a.get("repo_id") == r.get("id")]
        
        for m in r.get("milestones", []):
            st = m.get("status", "pending")
            name = m.get("name", "")
            details = m.get("details", "")
            target_file = m.get("relative_filepath", "—")
            
            # Dynamic badge
            if st == "completed":
                status_badge = "✅ Done"
                if "commit_hash" in m:
                    status_badge += f" (`{m['commit_hash']}`)"
            elif any(name in wt for wt in working_tasks):
                status_badge = "🔄 In Progress"
            else:
                status_badge = "⏳ Pending"
                if m.get("added_by"):
                    status_badge += f" *(via {m['added_by']})*"
                    
            lines.append(f"| **{name}** | {status_badge} | {details} | `{target_file}` |")
            
        lines.append("")
        
    out_file = BASE_DIR / "ROADMAP_BOARD.md"
    out_file.write_text("\n".join(lines), encoding="utf-8")
    return out_file

# ==============================================================================
# ⚡ DASHBOARD 2: AGENT CONCURRENCY & TOKEN ACCOUNTING
# ==============================================================================
def generate_token_fit_board(repos, account_count, telemetry):
    active_agents = telemetry.get("active_agents", {})
    totals = telemetry.get("totals", {})
    repo_stats = telemetry.get("repo_stats", {})
    model_stats = telemetry.get("model_stats", {})
    
    total_in = totals.get("total_input_tokens", 0)
    total_out = totals.get("total_output_tokens", 0)
    total_reasoning = totals.get("total_reasoning_tokens", 0)
    total_tokens = totals.get("total_tokens", total_in + total_out)
    total_calls = totals.get("total_api_calls", 0)
    
    total_daily_supply = account_count * DAILY_CAPACITY_PER_ACCOUNT
    
    lines = [
        "# Dashboard 2: AI Agent Concurrency & Token Consumption",
        f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  ",
        f"**Fleet Capacity:** {account_count} Google Pro Accounts (~{total_daily_supply:,} tokens/24h)\n",
        "## 1. Active Executing Agents (Live Concurrency)",
    ]
    
    if not active_agents:
        lines.append("🟢 **No agents currently executing.** System is idle and armed for the next scheduled/event cycle.\n")
    else:
        lines.append(f"🔥 **{len(active_agents)} Agent(s) in Active Flight:**\n")
        lines.append("| Agent ID | Target Repository | Role | AI Model | Current Task | Running Duration |")
        lines.append("| :--- | :--- | :--- | :--- | :--- | :--- |")
        for aid, a in active_agents.items():
            dur = int(time.time() - a.get("start_ts", time.time()))
            dur_str = f"{dur}s" if dur < 60 else f"{dur // 60}m {dur % 60}s"
            lines.append(f"| `{aid}` | **{a['repo_name']}** | {a['role']} | `{a['model']}` | {a['current_task']} | ⏱️ {dur_str} |")
        lines.append("")
        
    lines.append("## 2. Granular Token Consumption per Repository")
    lines.append("| Repository | Priority | Input Tokens (Prompt) | Output Tokens (Completion) | Total Tokens | API Calls | % Total Usage |")
    lines.append("| :--- | :---: | :---: | :---: | :---: | :---: | :---: |")
    
    for r in repos:
        rid = r.get("id")
        rs = repo_stats.get(rid, {})
        in_t = rs.get("input_tokens", 0)
        out_t = rs.get("output_tokens", 0)
        tot_t = rs.get("total_tokens", in_t + out_t)
        calls = rs.get("api_calls", 0)
        share = (tot_t / total_tokens * 100) if total_tokens > 0 else 0.0
        prio = r.get("priority", 99)
        lines.append(f"| **{r['name']}** | P{prio} | {in_t:,} | {out_t:,} | **{tot_t:,}** | {calls} | {share:.1f}% |")
        
    lines.append(f"| **TOTALS (All Repos)** | — | **{total_in:,}** | **{total_out:,}** | **{total_tokens:,}** | **{total_calls}** | **100%** |")
    lines.append("\n---\n")
    
    lines.append("## 3. Token Breakdown per AI Model & Agent Role")
    lines.append("| Agent Role & Model | Invocations | Input Tokens | Output Tokens | Reasoning Tokens (Thinking) | Total Tokens |")
    lines.append("| :--- | :---: | :---: | :---: | :---: | :---: |")
    
    for mname, ms in model_stats.items():
        m_in = ms.get("input_tokens", 0)
        m_out = ms.get("output_tokens", 0)
        m_reason = ms.get("reasoning_tokens", 0)
        m_tot = ms.get("total_tokens", m_in + m_out)
        calls = ms.get("calls", 0)
        role = ms.get("role", "Worker")
        lines.append(f"| **{role}** (`{mname}`) | {calls} | {m_in:,} | {m_out:,} | {m_reason:,} | **{m_tot:,}** |")
        
    lines.append("\n---\n")
    lines.append("## 4. Recent Agent Lifecycle Events")
    events = telemetry.get("recent_events", [])
    if not events:
        lines.append("- *(No agent events recorded yet)*")
    else:
        for ev in events[:8]:
            lines.append(f"- {ev}")
            
    out_file = BASE_DIR / "TOKEN_FIT_BOARD.md"
    out_file.write_text("\n".join(lines), encoding="utf-8")
    return out_file

# ==============================================================================
# 🛒 DASHBOARDS 3 & 4
# ==============================================================================
def generate_scaling_advisor(repos, account_count):
    total_daily_demand = sum(r.get("daily_token_budget_est", 100000) for r in repos)
    total_daily_supply = account_count * DAILY_CAPACITY_PER_ACCOUNT
    recommended_target_supply = total_daily_demand * 1.5
    needed_accounts = max(1, int((recommended_target_supply + DAILY_CAPACITY_PER_ACCOUNT - 1) // DAILY_CAPACITY_PER_ACCOUNT))
    account_delta = max(0, needed_accounts - account_count)
    
    lines = [
        "# Dashboard 3: AI Sizing & Account Purchasing Advisor",
        f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  ",
        f"- **Current Active Accounts:** {account_count} accounts (~{total_daily_supply:,} tokens/day)",
        f"- **Recommended Accounts:** **{needed_accounts} accounts**",
        f"- **Additional Accounts Needed:** **{account_delta}**\n",
        "## Multi-Repo Scaling Matrix",
        "| Active Projects | Recommended Fleet | Est Cost | Concurrency Speed |",
        "| :--- | :---: | :---: | :--- |",
        "| 1 Repo (PhD Proposal) | 2–3 accounts | ~$40–60/mo | Fast Continuous |",
        "| 2–3 Repos (+ Robotics + Web + Intranet) | 4 accounts (Current) | ~$80/mo | Balanced (Zero Idle) |",
        "| 4+ Repos (High Velocity Parallel) | 6 accounts | ~$120/mo | Maximum Concurrency |"
    ]
    out_file = BASE_DIR / "SCALING_ADVISOR.md"
    out_file.write_text("\n".join(lines), encoding="utf-8")
    return out_file

def generate_human_action_board():
    action_file = BASE_DIR / "human_action_items.json"
    items = []
    if action_file.exists():
        try:
            with open(action_file, "r") as f:
                items = json.load(f).get("active_blockers", [])
        except Exception:
            items = []
            
    lines = [
        "# Dashboard 4: Human-in-the-Loop Action & Assistance Board",
        f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  ",
        "| Blocker ID | Priority | Target Repo | Needed Action / Resource |",
        "| :---: | :---: | :--- | :--- |"
    ]
    if not items:
        lines.append("| — | 🟢 None | All Repositories | 🎉 All AI agent pipelines running autonomously without blockers! |")
    else:
        for idx, item in enumerate(items, 1):
            lines.append(f"| #{idx} | {item.get('priority', 'Medium')} | {item.get('repo', 'General')} | {item.get('action')} |")
            
    out_file = BASE_DIR / "HUMAN_ACTION_BOARD.md"
    out_file.write_text("\n".join(lines), encoding="utf-8")
    return out_file

def render_all():
    repos = load_repos()
    telemetry = load_telemetry()
    account_count = get_account_count()
    
    f1 = generate_roadmap_board(repos, telemetry)
    f2 = generate_token_fit_board(repos, account_count, telemetry)
    f3 = generate_scaling_advisor(repos, account_count)
    f4 = generate_human_action_board()
    print(f"[+] Rendered: {f1.name}, {f2.name}, {f3.name}, {f4.name}")

if __name__ == "__main__":
    render_all()
