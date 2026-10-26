#!/usr/bin/env python3
"""
note_converter.py: Transforms verbose raw dashboard markdown files
into high-signal, readable sticky note summaries with bold key highlights.
Output target: dashboards/notes_view/

Directly parses ROADMAP_BOARD.md so ANY manual edits made by the user
to 1_Roadmap_and_Progress.md or ROADMAP_BOARD.md are immediately reflected!
"""

import os
import sys
import json
import re
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
AUTO_DIR = BASE_DIR.parent
NOTES_DIR = BASE_DIR / "notes_view"
NOTES_DIR.mkdir(parents=True, exist_ok=True)
ROADMAP_MD = BASE_DIR / "ROADMAP_BOARD.md"

try:
    from telemetry import load_telemetry
except ImportError:
    sys.path.insert(0, str(BASE_DIR))
    from telemetry import load_telemetry

def parse_markdown_roadmap():
    """
    Parses ROADMAP_BOARD.md directly so manual user edits to the markdown file
    (e.g., removing a repo or adding a custom milestone) are respected 100%.
    """
    if not ROADMAP_MD.exists():
        return []
        
    content = ROADMAP_MD.read_text(encoding="utf-8")
    sections = re.split(r"(?m)^### 📂\s+", content)
    
    parsed_repos = []
    
    # Load skills map from repositories.json if available
    skills_map = {}
    repos_json_file = BASE_DIR / "repositories.json"
    if repos_json_file.exists():
        try:
            with open(repos_json_file, "r", encoding="utf-8") as f:
                rdata = json.load(f)
            for r in rdata.get("repositories", []):
                for m in r.get("milestones", []):
                    if m.get("name") and m.get("skills"):
                        skills_map[m["name"]] = m["skills"]
        except Exception:
            pass

    for sec in sections[1:]:
        lines = sec.strip().splitlines()
        if not lines:
            continue
            
        header = lines[0].strip()
        # Header format: [Priority 1] PhD Proposal & Research Engine (`~/PhD_Proposal`)
        prio_match = re.search(r"\[Priority\s*(\d+)\]\s*([^(`]+)", header)
        if prio_match:
            prio = int(prio_match.group(1))
            rname = prio_match.group(2).strip()
        else:
            prio = 1
            rname = header
            
        # Parse milestones from markdown table
        milestones = []
        for line in lines[1:]:
            line = line.strip()
            if not line.startswith("|") or "---" in line or "Details / Technical Deliverable" in line:
                continue
            cols = [c.strip() for c in line.split("|")[1:-1]]
            if len(cols) >= 2:
                mname = re.sub(r"\*\*", "", cols[0]).strip()
                status = cols[1].strip()
                details = cols[2].strip() if len(cols) > 2 else ""
                
                is_done = "✅" in status or "Done" in status
                m_dict = {
                    "name": mname,
                    "status": "completed" if is_done else "pending",
                    "details": details
                }
                if mname in skills_map:
                    m_dict["skills"] = skills_map[mname]
                milestones.append(m_dict)
                
        parsed_repos.append({
            "name": rname,
            "priority": prio,
            "milestones": milestones
        })
        
    return parsed_repos

def summarize_roadmap():
    # 1. Try parsing ROADMAP_BOARD.md directly
    repos = parse_markdown_roadmap()
    
    # 2. Fallback to repositories.json if markdown has no breakdown
    if not repos:
        repos_file = BASE_DIR / "repositories.json"
        if repos_file.exists():
            try:
                with open(repos_file, "r") as f:
                    repos = json.load(f).get("repositories", [])
            except Exception:
                repos = []
                
    if not repos:
        out = NOTES_DIR / "NOTE_1_ROADMAP.txt"
        out.write_text("No active repositories configured.", encoding="utf-8")
        return out
        
    repos.sort(key=lambda r: r.get("priority", 99))
    
    telemetry = load_telemetry()
    active_agents = telemetry.get("active_agents", {})
    working_tasks = [a.get("current_task", "") for a in active_agents.values()]
    
    # Read Quota Optimizer & Alarm status
    opt_line = "⚡ **ENGINE:** Autonomous Active (Quota Optimal)"
    try:
        import sys
        sys.path.insert(0, str(AUTO_DIR / "tasks"))
        from token_quota_optimizer import get_current_optimizer_state
        q_state = get_current_optimizer_state()
        if q_state.get("status") == "WAITING_FOR_QUOTA" or q_state.get("alarm_active"):
            opt_line = f"⏳ **ALARM:** Quota Cooldown | Resumes in {q_state.get('minutes_remaining', 0)}m (at {q_state.get('next_run_iso')})"
        elif q_state.get("status") == "IN_PROGRESS" or len(active_agents) > 0:
            opt_line = f"🤖 **ENGINE:** Pass Active ({len(active_agents)} agent running)"
    except Exception:
        pass

    lines = [
        "╔══════════════════════════════════════╗",
        "║   📌 ROADMAP & ACTIVE MILESTONES     ║",
        "╚══════════════════════════════════════╝\n",
        f"{opt_line}\n"
    ]
    for r in repos:
        prio = r.get("priority", 99)
        lines.append(f"📁 **[P{prio}] {r['name']}**")
        if r.get("active_branch"):
            lines.append(f"🌿 Branch: `{r.get('active_branch')}`")
        
        milestones = r.get("milestones", [])
        completed = [m for m in milestones if m.get("status") == "completed"]
        pending = [m for m in milestones if m.get("status") != "completed"]
        
        lines.append(f"📊 Progress: **{len(completed)} Done** / **{len(milestones)} Total**")
        
        # Show recent completed
        if completed:
            last_done = completed[-1]
            lines.append(f" ✅ **DONE:** {last_done['name']}")
            if last_done.get("skills"):
                lines.append(f"    🛠️ Skills: {', '.join(last_done['skills'][:3])}")
            
        # Show pending / active milestones
        for m in pending[:3]:
            name = m.get("name", "")
            is_active = any(name in wt for wt in working_tasks)
            if is_active:
                lines.append(f" 🔄 **ACTIVE:** {name}")
            else:
                lines.append(f" ⏳ **NEXT:** {name}")
                
        lines.append("")
        
    out = NOTES_DIR / "NOTE_1_ROADMAP.txt"
    out.write_text("\n".join(lines), encoding="utf-8")
    return out

def summarize_token_fit():
    telemetry = load_telemetry()
    active_agents = telemetry.get("active_agents", {})
    totals = telemetry.get("totals", {})
    repo_stats = telemetry.get("repo_stats", {})
    model_stats = telemetry.get("model_stats", {})
    
    tot_in = totals.get("total_input_tokens", 0)
    tot_out = totals.get("total_output_tokens", 0)
    tot_all = totals.get("total_tokens", tot_in + tot_out)
    tot_calls = totals.get("total_api_calls", 0)
    
    lines = [
        "╔══════════════════════════════════════╗",
        "║     ⚡ AGENTS & TOKEN CONSUMPTION    ║",
        "╚══════════════════════════════════════╝\n",
        f"🤖 **ACTIVE RUNNING AGENTS:** **{len(active_agents)} Active**"
    ]
    
    if active_agents:
        for aid, a in active_agents.items():
            lines.append(f" • **{a.get('role', 'Agent')}** on `{a.get('repo_name', 'Repo')}`")
            lines.append(f"   Task: {a.get('current_task', '')[:38]}...")
    else:
        lines.append(" • System Status: **Idle / Ready for Next Pass**")
        
    lines.append("\n📊 **TOKEN BURN PER REPO:**")
    if not repo_stats:
        lines.append(" • *(No invocations logged in session yet)*")
    else:
        for rid, rs in repo_stats.items():
            rname = rs.get("repo_name", rid)
            r_in = rs.get("input_tokens", 0)
            r_out = rs.get("output_tokens", 0)
            r_tot = rs.get("total_tokens", r_in + r_out)
            lines.append(f" • **{rname}:**")
            lines.append(f"   **In:** {r_in:,} | **Out:** {r_out:,} | **Tot:** **{r_tot:,}**")
            
    lines.append("\n🧠 **BY AGENT ROLE / MODEL:**")
    for mname, ms in model_stats.items():
        role = ms.get("role", "Worker")
        m_in = ms.get("input_tokens", 0)
        m_out = ms.get("output_tokens", 0)
        lines.append(f" • **{role}** ({mname.split('-')[0]}): {m_in:,} In / {m_out:,} Out")
        
    lines.append(f"\n🔋 **SESSION TOTAL:** **{tot_all:,} tokens** ({tot_calls} calls)")
    lines.append("👥 **FLEET CAPACITY:** **4 Google Pro Accounts**")
    
    out = NOTES_DIR / "NOTE_2_CAPACITY.txt"
    out.write_text("\n".join(lines), encoding="utf-8")
    return out

def summarize_human_actions():
    action_file = BASE_DIR / "human_action_items.json"
    items = []
    if action_file.exists():
        try:
            with open(action_file, "r") as f:
                items = json.load(f).get("active_blockers", [])
        except Exception:
            items = []
            
    lines = [
        "╔══════════════════════════════════════╗",
        "║     🚨 HUMAN ACTIONS & UNBLOCKERS    ║",
        "╚══════════════════════════════════════╝\n",
        f"⚠️ **ACTIVE BLOCKERS:** **{len(items)} Items Pending**\n"
    ]
    
    if not items:
        lines.append("🎉 **No action needed!** Agents are running smoothly.")
    else:
        for idx, item in enumerate(items, 1):
            prio = item.get("severity", "medium").upper()
            lines.append(f"**[{idx}] {prio}:** {item.get('title')}")
            lines.append(f" └─ Target: `{item.get('target_repository')}`")
            lines.append(f" └─ Action: {item.get('action')}\n")
            
    out = NOTES_DIR / "NOTE_3_ACTIONS.txt"
    out.write_text("\n".join(lines), encoding="utf-8")
    return out

def convert_all():
    summarize_roadmap()
    summarize_token_fit()
    summarize_human_actions()
    print("[+] Formatted sticky note summaries generated in notes_view/")

if __name__ == "__main__":
    convert_all()
