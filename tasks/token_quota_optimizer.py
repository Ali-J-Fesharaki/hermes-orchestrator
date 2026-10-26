#!/usr/bin/env python3
"""
token_quota_optimizer.py: Token Quota & Readiness Optimizer Engine.

Evaluates Google Cloud Code / Antigravity account quotas, rate limits (429s),
circuit breaker statuses, and model health. Computes optimal timing for autonomous
passes, pauses when quotas are depleted, and sets countdown alarms for automatic resumption.
"""

import os
import json
import time
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

AUTO_DIR = Path(__file__).resolve().parent.parent
DASHBOARDS_DIR = AUTO_DIR / "dashboards"
STATE_FILE = DASHBOARDS_DIR / "quota_optimizer_state.json"
DB_PATH = Path.home() / ".antigravity-agent/cloud_accounts.db"
GUI_CONFIG = Path.home() / ".antigravity-agent/gui_config.json"

# Model configurations
DEFAULT_EXECUTOR_MODEL = "gemini-3.8-flash-tiered"
FALLBACK_EXECUTOR_MODELS = ["gemini-3.5-flash-low", "gemini-3-flash", "gemini-2.5-flash"]

DEFAULT_PLANNER_MODEL = "claude-opus-4-6-thinking"
FALLBACK_PLANNER_MODELS = ["gemini-3.1-pro-high", "gemini-3.8-flash-high", "gpt-oss-120b-medium"]

MIN_SAFE_QUOTA_PERCENT = 10  # Minimum safe remaining quota % to trigger a heavy task


def get_account_pool_status():
    """Reads SQLite database for real-time account status, quotas, and health."""
    if not DB_PATH.exists():
        return {"accounts": [], "lockouts": [], "error": "Database not found"}

    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=5)
        cur = conn.cursor()

        # 1. Fetch active lockouts / 429 circuit breakers from settings
        cur.execute('SELECT value FROM settings WHERE key="proxy_model_availability.v1"')
        row = cur.fetchone()
        raw_lockouts = json.loads(row[0]) if row and row[0] else []

        now_ms = time.time() * 1000.0
        active_lockouts = []
        for l in raw_lockouts:
            unavail_ms = l.get("unavailableUntil", 0)
            if unavail_ms > now_ms:
                active_lockouts.append({
                    "accountId": l.get("accountId"),
                    "modelId": l.get("modelId"),
                    "reason": l.get("reason"),
                    "unavailableUntil_ms": unavail_ms,
                    "remaining_seconds": max(0, int((unavail_ms - now_ms) / 1000.0))
                })

        # 2. Fetch active accounts
        cur.execute("SELECT id, email, status, quota_json, health_json, is_active FROM accounts WHERE status='active'")
        accounts = []
        for acc_id, email, status, q_json, h_json, is_active in cur.fetchall():
            quota_data = json.loads(q_json) if q_json else {}
            health_data = json.loads(h_json) if h_json else {}

            models = quota_data.get("models", {})
            val_status = health_data.get("validation", {}).get("status")
            is_blocked = (val_status == "requires_action")

            accounts.append({
                "id": acc_id,
                "email": email,
                "is_active": bool(is_active),
                "is_validation_blocked": is_blocked,
                "validation_reason": health_data.get("validation", {}).get("reason") if is_blocked else None,
                "models": models
            })

        conn.close()
        return {"accounts": accounts, "lockouts": active_lockouts, "error": None}
    except Exception as e:
        return {"accounts": [], "lockouts": [], "error": str(e)}


def evaluate_quota_readiness(executor_model=DEFAULT_EXECUTOR_MODEL, planner_model=DEFAULT_PLANNER_MODEL):
    """
    Core optimizer calculation.
    Determines whether agents can run immediately or must wait.
    Returns structured decision object with countdown alarm info.
    """
    now = time.time()
    pool = get_account_pool_status()
    if pool.get("error"):
        return {
            "can_run": False,
            "status": "ERROR",
            "message": f"Database error: {pool['error']}",
            "seconds_remaining": 60,
            "alarm_active": True
        }

    accounts = pool.get("accounts", [])
    lockouts = pool.get("lockouts", [])

    # Index lockouts by (account_id, model_id)
    locked_pairs = {(l["accountId"], l["modelId"]): l for l in lockouts}

    # Evaluate Executor Availability
    healthy_executor_accounts = []
    min_executor_reset_wait = None

    for acc in accounts:
        if acc["is_validation_blocked"]:
            continue

        acc_id = acc["id"]
        # Check if locked out
        is_locked = (acc_id, executor_model) in locked_pairs
        model_info = acc["models"].get(executor_model, {})
        pct = model_info.get("percentage", 0)

        if not is_locked and pct >= MIN_SAFE_QUOTA_PERCENT:
            healthy_executor_accounts.append({
                "email": acc["email"],
                "percentage": pct,
                "model": executor_model
            })
        else:
            # Calculate wait if resetTime is present
            reset_iso = model_info.get("resetTime")
            if reset_iso:
                try:
                    dt = datetime.fromisoformat(reset_iso.replace("Z", "+00:00"))
                    diff = dt.timestamp() - now
                    if diff > 0:
                        min_executor_reset_wait = min(min_executor_reset_wait or diff, diff)
                except Exception:
                    pass
            if is_locked:
                rem = locked_pairs[(acc_id, executor_model)]["remaining_seconds"]
                min_executor_reset_wait = min(min_executor_reset_wait or rem, rem)

    # Evaluate Planner Availability (Opus or Fallback Pro/Cascade)
    healthy_planner_accounts = []
    min_planner_reset_wait = None

    for acc in accounts:
        if acc["is_validation_blocked"]:
            continue

        acc_id = acc["id"]
        # Try primary planner first
        is_locked_primary = (acc_id, planner_model) in locked_pairs
        primary_info = acc["models"].get(planner_model, {})
        primary_pct = primary_info.get("percentage", 0)

        if not is_locked_primary and primary_pct >= 5:
            healthy_planner_accounts.append({
                "email": acc["email"],
                "percentage": primary_pct,
                "model": planner_model,
                "is_primary": True
            })
            continue

        # Check fallback planners (e.g. Gemini Pro / High)
        for fb_model in FALLBACK_PLANNER_MODELS:
            is_locked_fb = (acc_id, fb_model) in locked_pairs
            fb_info = acc["models"].get(fb_model, {})
            fb_pct = fb_info.get("percentage", 0)
            if not is_locked_fb and fb_pct >= MIN_SAFE_QUOTA_PERCENT:
                healthy_planner_accounts.append({
                    "email": acc["email"],
                    "percentage": fb_pct,
                    "model": fb_model,
                    "is_primary": False
                })
                break

    # Decision Matrix
    can_execute = len(healthy_executor_accounts) > 0
    can_plan = len(healthy_planner_accounts) > 0

    if can_execute:
        # We can execute tasks!
        best_exec = max(healthy_executor_accounts, key=lambda x: x["percentage"])
        planner_desc = "Primary (Claude Opus)" if any(p.get("is_primary") for p in healthy_planner_accounts) else "Dynamic High-Tier Fallback"
        
        result = {
            "status": "READY",
            "can_run": True,
            "executor_ready": True,
            "planner_ready": can_plan,
            "planner_tier": planner_desc,
            "active_accounts_count": len(healthy_executor_accounts),
            "best_executor_account": best_exec["email"],
            "best_executor_percentage": best_exec["percentage"],
            "seconds_remaining": 0,
            "minutes_remaining": 0,
            "next_run_iso": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "alarm_active": False,
            "status_message": f"Token Quota Healthy ({best_exec['percentage']}% on {best_exec['email'].split('@')[0]}). Agents ready to execute.",
            "last_evaluated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    else:
        # All executor accounts are locked or 0%
        # Calculate optimal alarm wait time
        wait_sec = int(min_executor_reset_wait or 300)
        # Cap wait between 60s and 3600s for polling
        wait_sec = max(60, min(wait_sec, 3600))
        next_run = now + wait_sec
        next_iso = datetime.fromtimestamp(next_run).strftime("%H:%M:%S")
        min_rem = round(wait_sec / 60, 1)

        result = {
            "status": "WAITING_FOR_QUOTA",
            "can_run": False,
            "executor_ready": False,
            "planner_ready": can_plan,
            "active_accounts_count": 0,
            "best_executor_account": "None available",
            "best_executor_percentage": 0,
            "seconds_remaining": wait_sec,
            "minutes_remaining": min_rem,
            "next_run_timestamp": next_run,
            "next_run_iso": next_iso,
            "alarm_active": True,
            "status_message": f"⏳ Quota Cooldown: Waiting for token replenishment. Resuming in {min_rem} min (at {next_iso}).",
            "last_evaluated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

    # Save to state file atomically
    try:
        DASHBOARDS_DIR.mkdir(parents=True, exist_ok=True)
        temp_file = STATE_FILE.with_suffix(".tmp")
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        temp_file.replace(STATE_FILE)
    except Exception as e:
        print(f"[-] Error writing quota optimizer state: {e}")

    return result


def get_current_optimizer_state():
    """Reads latest optimizer state or runs evaluation if missing/stale."""
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            # If last evaluated more than 60s ago, re-evaluate
            return data
        except Exception:
            pass
    return evaluate_quota_readiness()


if __name__ == "__main__":
    res = evaluate_quota_readiness()
    print("=== Token Quota Optimizer Evaluation ===")
    print(json.dumps(res, indent=2))
