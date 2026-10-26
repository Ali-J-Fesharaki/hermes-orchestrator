#!/usr/bin/env python3
"""
dynamic_planner_ranker.py: Online, Dynamic Model Ranking for Strategic Planning.

Features:
  1. Real-time Model Discovery:
     - Queries local router (http://127.0.0.1:8045/v1/models).
     - Cross-references with local active account quotas from ~/.antigravity-agent/cloud_accounts.db.
  2. Live Benchmark & Ranking Evaluation:
     - Ranks models specifically on High-Level Strategic Planning & Reasoning suitability:
       * Thinking / Reasoning token support (Chain-of-Thought).
       * Frontier architecture tier.
       * Live quota availability (models with 0% quota or unmapped models are deprioritized).
  3. Dynamic Cascade Output:
     - Returns a live sorted list of models ready for the planning cascade.
     - Persists rankings to dashboards/model_planning_rankings.json for Cockpit UI inspection.
"""

import os
import sys
import json
import time
import sqlite3
import urllib.request
import urllib.error
from pathlib import Path
from datetime import datetime

AUTO_DIR = Path(__file__).resolve().parent.parent
DASHBOARDS_DIR = AUTO_DIR / "dashboards"
RANKINGS_CACHE_FILE = DASHBOARDS_DIR / "model_planning_rankings.json"
ROUTER_URL = os.environ.get("ROUTER_URL", "http://127.0.0.1:8045/v1/models")
ENV_FILE = Path.home() / ".hermes/.env"
OPENROUTER_MODELS_URL = "https://openrouter.ai/api/v1/models"
ACCOUNTS_DB = Path.home() / ".antigravity-agent" / "cloud_accounts.db"

CACHE_TTL_HOURS = 1  # 1 hour cache so quota changes are reflected promptly

def get_router_api_key() -> str:
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            if line.startswith("HERMES_CUSTOM_ANTIGRAVITY_MANAGER_API_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return os.environ.get("HERMES_CUSTOM_ANTIGRAVITY_MANAGER_API_KEY", "")

def get_live_account_quotas() -> dict:
    """Reads cloud_accounts.db to determine which models currently have remaining quota on active accounts."""
    if not ACCOUNTS_DB.exists():
        return {}
    try:
        con = sqlite3.connect(str(ACCOUNTS_DB), timeout=3)
        cur = con.cursor()
        cur.execute("SELECT status, quota_json, health_json FROM accounts")
        model_quotas = {}
        for status, q_json, h_json in cur.fetchall():
            if status != "active":
                continue
            if h_json:
                try:
                    h = json.loads(h_json)
                    if h.get("validation", {}).get("status") == "requires_action":
                        continue  # Skip quarantined / unverified accounts
                except Exception:
                    pass
            if q_json:
                try:
                    q = json.loads(q_json)
                    for m, minfo in q.get("models", {}).items():
                        pct = minfo.get("percentage", 0)
                        model_quotas[m] = max(model_quotas.get(m, 0), pct)
                except Exception:
                    pass
        con.close()
        return model_quotas
    except Exception as e:
        print(f"[-] Quota lookup note: {e}")
        return {}

def fetch_available_router_models() -> list[str]:
    """Queries local router to discover active live models."""
    api_key = get_router_api_key()
    headers = {"Authorization": f"Bearer {api_key}"}
    req = urllib.request.Request(ROUTER_URL, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            raw_models = [m.get("id") for m in data.get("data", []) if m.get("id")]
            # Filter out deprecated/offline model IDs that always fail
            deprecated = ["3.5-flash-high", "3.5-flash-low", "3.5-flash-medium", "3-flash"]
            return [m for m in raw_models if not any(dep in m for dep in deprecated)]
    except Exception as e:
        print(f"[-] Router model discovery error: {e}")
        return ["gemini-3.8-flash-tiered", "gemini-2.5-flash"]

def fetch_openrouter_catalog() -> dict:
    """Fetches live OpenRouter catalog with model architectures and metadata."""
    req = urllib.request.Request(OPENROUTER_MODELS_URL, headers={"User-Agent": "Hermes-Dynamic-Ranker"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return {m.get("id", "").lower(): m for m in data.get("data", [])}
    except Exception:
        return {}

def score_model_for_planning(model_id: str, openrouter_data: dict, live_quotas: dict) -> dict:
    """
    Computes a comprehensive Strategic Planning Score (0 - 100)
    evaluating reasoning, architectural thinking, live quota, and benchmark tier.
    """
    score = 50.0
    mid = model_id.lower()
    
    # 1. Reasoning & Thinking Capabilities (Max +30 pts)
    # Gemini 3.8 Flash Tiered natively supports thinking tokens and 1M context
    is_thinking = "thinking" in mid or "reasoning" in mid or "3.8-flash" in mid or "o1" in mid or "o3" in mid
    if "thinking" in mid:
        score += 28.0
    elif "3.8-flash" in mid:
        score += 26.0
    elif "high" in mid and "pro" in mid:
        score += 24.0
    elif "high" in mid:
        score += 15.0

    # 2. Frontier Model Family & Architecture Tier (Max +25 pts)
    if "opus" in mid:
        score += 25.0
    elif "sonnet" in mid:
        score += 20.0
    elif "3.8-flash" in mid:
        score += 22.0
    elif "pro" in mid:
        score += 15.0
    elif "flash" in mid:
        score += 8.0

    # 3. Live Quota Availability Assessment (CRITICAL: Never freeze on 0% quota)
    # Match model_id against live_quotas
    matched_quota = live_quotas.get(model_id)
    if matched_quota is None:
        # Try stripped key (e.g. claude-sonnet-4-6-thinking -> claude-sonnet-4-6)
        stripped = model_id.replace("-thinking", "")
        matched_quota = live_quotas.get(stripped)

    if matched_quota is not None:
        if matched_quota <= 0:
            score -= 55.0  # Heavy penalty for 0% quota so working models are prioritized
        else:
            score += 10.0  # Bonus for verified active quota
    else:
        # Unknown model not present in accounts
        if "3.8-flash" not in mid and "2.5-flash" not in mid:
            score -= 30.0

    # Format human-friendly display label & theme color
    display_name = model_id
    color = "slate"
    if "opus" in mid:
        display_name = "Claude Opus 4.6 (Thinking)"
        color = "purple"
    elif "sonnet" in mid:
        display_name = "Claude Sonnet 4.6 (Thinking)"
        color = "indigo"
    elif "3.8-flash" in mid:
        display_name = "Gemini 3.8 Flash (Tiered)"
        color = "emerald"
    elif "3.1-pro-high" in mid:
        display_name = "Gemini 3.1 Pro (High Reasoning)"
        color = "blue"
    elif "3.1-pro" in mid:
        display_name = "Gemini 3.1 Pro"
        color = "cyan"
    elif "3.7-flash" in mid:
        display_name = "Gemini 3.7 Flash"
        color = "teal"
    elif "2.5-flash" in mid:
        display_name = "Gemini 2.5 Flash"
        color = "slate"

    return {
        "model_id": model_id,
        "display_name": display_name,
        "planning_score": round(score, 1),
        "is_thinking": is_thinking,
        "color": color,
        "quota_percent": matched_quota
    }

def get_dynamic_planning_cascade(force_refresh: bool = False) -> list[dict]:
    """
    Returns the live sorted planning cascade.
    Uses disk cache if fresh (< 1 hour) and not forced.
    """
    if not force_refresh and RANKINGS_CACHE_FILE.exists():
        try:
            cached = json.loads(RANKINGS_CACHE_FILE.read_text(encoding="utf-8"))
            updated_at = cached.get("timestamp", 0)
            if time.time() - updated_at < CACHE_TTL_HOURS * 3600 and cached.get("ranked_models"):
                return cached["ranked_models"]
        except Exception:
            pass

    available_models = fetch_available_router_models()
    if not available_models:
        available_models = ["gemini-3.8-flash-tiered", "gemini-2.5-flash"]

    openrouter_catalog = fetch_openrouter_catalog()
    live_quotas = get_live_account_quotas()
    
    scored_models = []
    for mid in available_models:
        # Filter out very low models
        if any(skip in mid for skip in ["lite", "low", "medium"]) and "pro-low" not in mid:
            continue
        scored_models.append(score_model_for_planning(mid, openrouter_catalog, live_quotas))

    # Sort descending by planning score
    scored_models.sort(key=lambda x: x["planning_score"], reverse=True)

    # Assign rank indices
    for idx, m in enumerate(scored_models, 1):
        m["rank"] = idx

    # Cache results
    cache_payload = {
        "timestamp": time.time(),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_available": len(available_models),
        "ranked_models": scored_models
    }
    try:
        DASHBOARDS_DIR.mkdir(parents=True, exist_ok=True)
        RANKINGS_CACHE_FILE.write_text(json.dumps(cache_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        print(f"[-] Cache write error: {e}")

    return scored_models

if __name__ == "__main__":
    cascade = get_dynamic_planning_cascade(force_refresh=True)
    print("\n" + "=" * 75)
    print(" 🏆 LIVE STRATEGIC PLANNING MODEL LEADERBOARD (DYNAMIC CASCADE)")
    print("=" * 75)
    for m in cascade:
        thinking_icon = "🧠 Thinking" if m["is_thinking"] else "⚡ Fast"
        q_str = f"Quota: {m['quota_percent']}%" if m['quota_percent'] is not None else "Quota: N/A"
        print(f"Rank #{m['rank']:2d} | Score: {m['planning_score']:4.1f} | {m['display_name']:<32} | {thinking_icon} | {q_str}")
    print("=" * 75)
