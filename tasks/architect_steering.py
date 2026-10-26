#!/usr/bin/env python3
"""
architect_steering.py: Manages Human Architectural Edits, AI In-Place Card Refinement,
and Persistent Steering History for Autonomous Planning Continuity.

Key Functions:
  1. ai_refine_card():
     - Takes rough human speech/text (Persian or English) and polishes it into
       rigorous academic LaTeX or robotics code milestone specifications.
  2. record_card_edit():
     - Logs the human architect's intervention to architect_steering_history.json.
  3. get_steering_context_for_repo():
     - Extracts the accumulated human preferences & technical trajectory so that
       autonomous agents never drift when planning future phases independently.
"""

import os
import sys
import json
import time
from pathlib import Path
from datetime import datetime

AUTO_DIR = Path(__file__).resolve().parent.parent
DASHBOARDS_DIR = AUTO_DIR / "dashboards"
HISTORY_FILE = DASHBOARDS_DIR / "architect_steering_history.json"
ROUTER_URL = os.environ.get("ROUTER_URL", "http://127.0.0.1:8045/v1/chat/completions")
ENV_FILE = Path.home() / ".hermes/.env"

# Top Refinement Model
REFINE_MODEL = "claude-sonnet-4-6-thinking"
REFINE_FALLBACK = "gemini-3.1-pro-high"

def get_router_key() -> str:
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            if line.startswith("HERMES_CUSTOM_ANTIGRAVITY_MANAGER_API_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return os.environ.get("HERMES_CUSTOM_ANTIGRAVITY_MANAGER_API_KEY", "")

def load_steering_history() -> dict:
    if not HISTORY_FILE.exists():
        return {"edits": [], "repo_summaries": {}}
    try:
        return json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"edits": [], "repo_summaries": {}}

def save_steering_history(data: dict):
    DASHBOARDS_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

def ai_refine_card(repo_id: str, repo_name: str, current_title: str, current_details: str, current_filepath: str, user_raw_speech: str) -> dict:
    """
    Takes user's rough thoughts/speech and formulates a rigorous, production-grade
    milestone specification for LaTeX research or robotics code.
    """
    import urllib.request
    
    api_key = get_router_key()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    system_prompt = (
        "You are the Lead Research Architect AI for an advanced multi-repo robotics & PhD research system.\n"
        "The Principal Human Architect wants to edit an existing milestone card on the project Kanban board.\n"
        "They provided their rough thoughts or instructions (which may be in Persian or informal English).\n"
        "Your task is to synthesize and formalize their input into a clear, mathematically sound, or production-grade engineering milestone.\n"
        "Return ONLY a clean JSON object with the following fields:\n"
        "  'title': refined concise English milestone title (e.g. 'Milestone 3: Continuous Social Air-Gap Reward Formulation'),\n"
        "  'details': 2-3 detailed sentences in English describing the formal mathematical equations, ROS 2 nodes, or deliverable criteria,\n"
        "  'relative_filepath': target source/LaTeX file relative to repo root,\n"
        "  'architectural_principle': a one-sentence summary of the human architect's core preference (e.g. 'Prefers continuous potential field penalties over binary distance thresholds')."
    )

    user_prompt = (
        f"--- CONTEXT ---\n"
        f"Repository: {repo_name} ({repo_id})\n"
        f"Current Milestone Title: {current_title}\n"
        f"Current Details: {current_details}\n"
        f"Current Target File: {current_filepath}\n\n"
        f"--- HUMAN ARCHITECT INPUT / VOICE DICTATION ---\n"
        f"\"{user_raw_speech}\"\n\n"
        f"Refine and formalize this milestone according to the architect's intent."
    )

    payload = {
        "model": REFINE_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "temperature": 0.2,
        "max_tokens": 1500
    }

    resp_text = None
    for model_candidate in [REFINE_MODEL, REFINE_FALLBACK]:
        payload["model"] = model_candidate
        try:
            req = urllib.request.Request(ROUTER_URL, data=json.dumps(payload).encode("utf-8"), headers=headers)
            with urllib.request.urlopen(req, timeout=40) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                resp_text = data["choices"][0]["message"]["content"]
                break
        except Exception as e:
            print(f"[-] Refine fallback from {model_candidate}: {e}")

    if not resp_text:
        return {
            "title": current_title,
            "details": f"{current_details}\n[User note: {user_raw_speech}]",
            "relative_filepath": current_filepath,
            "architectural_principle": user_raw_speech
        }

    clean = resp_text.strip()
    for fence in ["```json", "```"]:
        if fence in clean:
            clean = clean.split(fence, 1)[1].split("```", 1)[0].strip()
            break

    try:
        parsed = json.loads(clean)
        return {
            "title": parsed.get("title", current_title),
            "details": parsed.get("details", current_details),
            "relative_filepath": parsed.get("relative_filepath", current_filepath),
            "architectural_principle": parsed.get("architectural_principle", user_raw_speech)
        }
    except Exception:
        return {
            "title": current_title,
            "details": current_details,
            "relative_filepath": current_filepath,
            "architectural_principle": user_raw_speech
        }

def record_card_edit(repo_id: str, old_milestone: dict, new_milestone: dict, user_notes: str, architectural_principle: str = ""):
    """
    Saves the edit to persistent history so Opus / Planner continues this exact trajectory.
    """
    history = load_steering_history()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    entry = {
        "timestamp": now_str,
        "repo_id": repo_id,
        "milestone_old_name": old_milestone.get("name", ""),
        "milestone_new_name": new_milestone.get("name", ""),
        "details_diff": {
            "before": old_milestone.get("details", ""),
            "after": new_milestone.get("details", "")
        },
        "user_raw_notes": user_notes,
        "architectural_principle": architectural_principle or user_notes
    }

    history.setdefault("edits", []).append(entry)

    # Update rolling repository summary of human architectural principles
    repo_principles = history.setdefault("repo_summaries", {}).setdefault(repo_id, [])
    if architectural_principle and architectural_principle not in repo_principles:
        repo_principles.append(architectural_principle)
        if len(repo_principles) > 8:
            repo_principles.pop(0)

    save_steering_history(history)
    print(f"📌 [STEERING HISTORY] Recorded human architectural intervention for '{repo_id}': {entry['milestone_new_name']}")

def record_discarded_milestone(repo_id: str, milestone_name: str, milestone_details: str = "", user_reason: str = ""):
    """
    Records a discarded/deleted milestone into persistent negative constraint memory.
    Autonomous planners are strictly forbidden from recreating or re-proposing this scope.
    """
    history = load_steering_history()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    discarded_entry = {
        "timestamp": now_str,
        "repo_id": repo_id,
        "name": milestone_name,
        "details": milestone_details,
        "reason": user_reason or "Explicitly deleted/rejected by Human Architect"
    }

    history.setdefault("discarded_milestones", []).append(discarded_entry)

    repo_blacklist = history.setdefault("repo_blacklists", {}).setdefault(repo_id, [])
    if milestone_name not in repo_blacklist:
        repo_blacklist.append(milestone_name)

    save_steering_history(history)
    print(f"🚫 [BLACKLIST RECORDED] Milestone '{milestone_name}' marked as FORBIDDEN for repo '{repo_id}'.")

def get_steering_context_for_repo(repo_id: str, max_items: int = 4) -> str:
    """
    Constructs an authoritative steering guidance prompt for autonomous planners
    based on the human architect's actual edit history and negative constraints.
    """
    history = load_steering_history()
    edits = [e for e in history.get("edits", []) if e.get("repo_id") == repo_id]
    principles = history.get("repo_summaries", {}).get(repo_id, [])
    blacklisted = history.get("repo_blacklists", {}).get(repo_id, [])

    if not edits and not principles and not blacklisted:
        return ""

    lines = ["\n--- HUMAN ARCHITECT HISTORICAL EDITS & PERSISTENT STEERING ---"]
    lines.append("The Principal Human Architect has actively refined and steered milestones for this repository.")
    lines.append("Autonomous agents executing unattended MUST strictly adhere to these established engineering principles:\n")

    if principles:
        lines.append("Core Architectural Guidelines & Preferences:")
        for p in principles[-max_items:]:
            lines.append(f"• {p}")
        lines.append("")

    if edits:
        lines.append("Recent Human Milestone Refinements:")
        for e in edits[-max_items:]:
            lines.append(f"• Milestone '{e['milestone_new_name']}':")
            lines.append(f"  - Architect Input: \"{e['user_raw_notes']}\"")
            lines.append(f"  - Adopted Specification: {e['details_diff']['after'][:160]}...")
        lines.append("")

    if blacklisted:
        lines.append("🚫 STRICT NEGATIVE CONSTRAINTS (FORBIDDEN TOPICS):")
        lines.append("The Human Architect has explicitly deleted/rejected the following milestone topics.")
        lines.append("DO NOT regenerate, re-propose, or synthesize any duplicate or near-duplicate milestone for these:")
        for b in blacklisted[-8:]:
            lines.append(f"❌ {b}")
        lines.append("Focus solely on advancing NEW or alternative architectures.\n")

    lines.append("MANDATE: Continue the development roadmap along this established human architectural trajectory.\n")
    return "\n".join(lines)
