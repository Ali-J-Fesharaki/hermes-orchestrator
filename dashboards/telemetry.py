#!/usr/bin/env python3
"""
telemetry.py: Real-time agent registry, token accounting, and metrics tracker.

Tracks:
  1. Active executing agents (concurrency, repo, model, current task, uptime).
  2. Granular token consumption per repository (input prompt vs output completion tokens).
  3. Token breakdown per agent role (Opus Strategic Planner vs Gemini Task Executor).
  4. Real-time updates to dashboards and sticky notes.
"""

import os
import json
import time
import threading
import subprocess
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
AUTO_DIR = BASE_DIR.parent
TELEMETRY_FILE = BASE_DIR / "telemetry.json"
_TELEMETRY_LOCK = threading.Lock()

def _init_telemetry() -> dict:
    return {
        "active_agents": {},
        "repo_stats": {},
        "model_stats": {},
        "totals": {
            "total_input_tokens": 0,
            "total_output_tokens": 0,
            "total_reasoning_tokens": 0,
            "total_tokens": 0,
            "total_api_calls": 0
        },
        "recent_events": [],
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

def load_telemetry() -> dict:
    if not TELEMETRY_FILE.exists():
        data = _init_telemetry()
        save_telemetry(data)
        return data
    try:
        with open(TELEMETRY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return _init_telemetry()

def save_telemetry(data: dict):
    with _TELEMETRY_LOCK:
        data["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        tmp = TELEMETRY_FILE.with_suffix(f".tmp.{os.getpid()}.{threading.get_ident()}")
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                f.write("\n")
            tmp.replace(TELEMETRY_FILE)
        except Exception:
            if tmp.exists():
                tmp.unlink(missing_ok=True)

def register_agent(agent_id: str, repo_id: str, repo_name: str, role: str, model: str, task: str, hermes_session_id: str | None = None):
    """Registers an active running agent in the live concurrency registry."""
    data = load_telemetry()
    data["active_agents"][agent_id] = {
        "agent_id": agent_id,
        "repo_id": repo_id,
        "repo_name": repo_name,
        "role": role,
        "model": model,
        "current_task": task,
        "hermes_session_id": hermes_session_id,
        "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "start_ts": time.time()
    }
    session_tag = f" [Session: {hermes_session_id}]" if hermes_session_id else ""
    event = f"[{datetime.now().strftime('%H:%M:%S')}] Started {role} ({model}) on '{repo_name}'{session_tag}: {task}"
    data["recent_events"] = [event] + data.get("recent_events", [])[:15]
    save_telemetry(data)

def update_agent_session(agent_id: str, hermes_session_id: str):
    """Updates the hermes_session_id of a running registered agent."""
    data = load_telemetry()
    if agent_id in data.get("active_agents", {}):
        data["active_agents"][agent_id]["hermes_session_id"] = hermes_session_id
        save_telemetry(data)

def is_task_actively_running_on_os(repo_id: str, task_name: str) -> bool:
    """Checks whether an actual Hermes CLI process is actively running for this repository."""
    try:
        out = subprocess.run(["pgrep", "-a", "-f", "hermes.*--in"], capture_output=True, text=True).stdout
        for line in out.splitlines():
            if repo_id and repo_id in line:
                return True
            if task_name and task_name[:20].lower() in line.lower():
                return True
    except Exception:
        pass
    return False

def clean_stale_agents(max_age_seconds: int = 1500, force: bool = False) -> int:
    """
    Safely removes ghost / abandoned agents from telemetry.
    CRITICAL: Never removes an agent if its underlying Hermes subprocess is actively running on the OS!
    """
    data = load_telemetry()
    now = time.time()
    stale_ids = []
    for aid, a in data.get("active_agents", {}).items():
        start_ts = a.get("start_ts", 0)
        repo_id = a.get("repo_id", "")
        task_name = a.get("current_task", "")
        
        # Process is actively running on OS — DO NOT prune!
        if not force and is_task_actively_running_on_os(repo_id, task_name):
            continue

        if force or (now - start_ts) > max_age_seconds:
            stale_ids.append(aid)

    for aid in stale_ids:
        agent = data["active_agents"].pop(aid)
        event = f"[{datetime.now().strftime('%H:%M:%S')}] Pruned orphaned agent session '{aid}' ({agent.get('role', 'Agent')})"
        data["recent_events"] = [event] + data.get("recent_events", [])[:15]
    if stale_ids:
        save_telemetry(data)
    return len(stale_ids)

def unregister_agent(agent_id: str, status: str = "completed", error_msg: str | None = None):
    """Removes an agent from the active running pool upon task completion or failure."""
    data = load_telemetry()
    if agent_id in data["active_agents"]:
        agent = data["active_agents"].pop(agent_id)
        elapsed = int(time.time() - agent.get("start_ts", time.time()))
        if status == "failed" or error_msg:
            err_snippet = f": {error_msg[:120]}" if error_msg else ""
            event = f"[{datetime.now().strftime('%H:%M:%S')}] ❌ FAILED {agent['role']} on '{agent['repo_name']}' ({elapsed}s){err_snippet}"
            err_entry = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "agent_id": agent_id,
                "role": agent.get("role", "Agent"),
                "repo_name": agent.get("repo_name", "Unknown"),
                "task": agent.get("current_task", "Task"),
                "error": error_msg or "Execution failed",
                "elapsed_s": elapsed
            }
            data.setdefault("recent_errors", []).insert(0, err_entry)
            data["recent_errors"] = data["recent_errors"][:20]
        else:
            event = f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Finished {agent['role']} on '{agent['repo_name']}' ({elapsed}s)"
        data["recent_events"] = [event] + data.get("recent_events", [])[:20]
    save_telemetry(data)

def record_failure_event(agent_id: str, repo_name: str, role: str, milestone_name: str, error_msg: str):
    """Explicitly records an agent execution failure to live telemetry stream."""
    data = load_telemetry()
    now_str = datetime.now().strftime("%H:%M:%S")
    event = f"[{now_str}] ❌ FAILED {role} on '{repo_name}' for '{milestone_name}': {error_msg[:140]}"
    data["recent_events"] = [event] + data.get("recent_events", [])[:20]
    err_entry = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "agent_id": agent_id,
        "role": role,
        "repo_name": repo_name,
        "task": milestone_name,
        "error": error_msg,
        "elapsed_s": 0
    }
    data.setdefault("recent_errors", []).insert(0, err_entry)
    data["recent_errors"] = data["recent_errors"][:20]
    save_telemetry(data)

def record_tokens(repo_id: str, repo_name: str, role: str, model: str, usage: dict):
    """
    Records detailed prompt (input) and completion (output) tokens from router response.
    """
    if not usage:
        return
        
    prompt_tokens = usage.get("prompt_tokens", 0)
    completion_tokens = usage.get("completion_tokens", 0)
    total_tokens = usage.get("total_tokens", prompt_tokens + completion_tokens)
    
    # Extract reasoning tokens (for thinking models)
    comp_details = usage.get("completion_tokens_details", {}) or {}
    reasoning_tokens = comp_details.get("reasoning_tokens", 0)
    
    data = load_telemetry()
    
    # 1. Update Totals
    totals = data.setdefault("totals", {
        "total_input_tokens": 0, "total_output_tokens": 0,
        "total_reasoning_tokens": 0, "total_tokens": 0, "total_api_calls": 0
    })
    totals["total_input_tokens"] += prompt_tokens
    totals["total_output_tokens"] += completion_tokens
    totals["total_reasoning_tokens"] += reasoning_tokens
    totals["total_tokens"] += total_tokens
    totals["total_api_calls"] += 1
    
    # 2. Update Per-Repository Stats
    repo_stats = data.setdefault("repo_stats", {})
    if repo_id not in repo_stats:
        repo_stats[repo_id] = {
            "repo_id": repo_id,
            "repo_name": repo_name,
            "input_tokens": 0,
            "output_tokens": 0,
            "reasoning_tokens": 0,
            "total_tokens": 0,
            "api_calls": 0,
            "by_agent": {}
        }
    rs = repo_stats[repo_id]
    rs["input_tokens"] += prompt_tokens
    rs["output_tokens"] += completion_tokens
    rs["reasoning_tokens"] += reasoning_tokens
    rs["total_tokens"] += total_tokens
    rs["api_calls"] += 1
    
    agent_key = f"{role} ({model})"
    agent_dict = rs["by_agent"].setdefault(agent_key, {
        "role": role, "model": model, "input_tokens": 0, "output_tokens": 0, "total_tokens": 0, "calls": 0
    })
    agent_dict["input_tokens"] += prompt_tokens
    agent_dict["output_tokens"] += completion_tokens
    agent_dict["total_tokens"] += total_tokens
    agent_dict["calls"] += 1
    
    # 3. Update Per-Model Stats
    model_stats = data.setdefault("model_stats", {})
    if model not in model_stats:
        model_stats[model] = {
            "model": model,
            "role": role,
            "input_tokens": 0,
            "output_tokens": 0,
            "reasoning_tokens": 0,
            "total_tokens": 0,
            "calls": 0
        }
    ms = model_stats[model]
    ms["input_tokens"] += prompt_tokens
    ms["output_tokens"] += completion_tokens
    ms["reasoning_tokens"] += reasoning_tokens
    ms["total_tokens"] += total_tokens
    ms["calls"] += 1
    
    save_telemetry(data)

def get_telemetry_summary() -> dict:
    return load_telemetry()

if __name__ == "__main__":
    t = load_telemetry()
    print("Telemetry Status:")
    print(f"Active Agents: {len(t.get('active_agents', {}))}")
    print(f"Total Tokens:  {t.get('totals', {}).get('total_tokens', 0):,}")
    print(f"Total Calls:   {t.get('totals', {}).get('total_api_calls', 0)}")
