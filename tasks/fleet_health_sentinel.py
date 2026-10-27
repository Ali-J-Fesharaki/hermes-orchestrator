#!/usr/bin/env python3
"""
fleet_health_sentinel.py: Comprehensive Pre-Flight Health & Token Engine Verification
Probes:
1. LLM Router HTTP Gateway (Ports 8045 / 7836)
2. Cloud Account Quota & Captcha Status from cloud_accounts.db
3. Authentic End-to-End Hermes Agent Smoke Test (1-shot PONG execution)
Caches smoke test results to avoid unnecessary token consumption.
"""

import os
import sys
import json
import time
import sqlite3
import subprocess
import urllib.request
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent.parent
CACHE_FILE = BASE_DIR / "dashboards" / "fleet_health_cache.json"

def get_auth_tokens() -> list[str]:
    env_file = Path("/home/ajf/.hermes/.env")
    keys = []
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and any(k in line for k in ["KEY", "TOKEN"]):
                val = line.split("=", 1)[1].strip().strip("\"'")
                if val and val not in keys:
                    keys.append(val)
    return keys

def probe_gateway() -> dict:
    """Probes router HTTP ports (8045, 7836) for live models."""
    tokens = get_auth_tokens() + [""]
    for port in [8045, 7836]:
        for token in tokens:
            req = urllib.request.Request(f"http://127.0.0.1:{port}/v1/models")
            if token:
                req.add_header("Authorization", f"Bearer {token}")
            try:
                with urllib.request.urlopen(req, timeout=1.8) as resp:
                    data = json.loads(resp.read().decode())
                    models = [m.get("id") for m in data.get("data", [])]
                    return {
                        "gateway_online": True,
                        "port": port,
                        "models_count": len(models),
                        "models": models[:6]
                    }
            except Exception:
                pass
    return {
        "gateway_online": False,
        "port": 8045,
        "models_count": 0,
        "models": []
    }

def probe_cloud_accounts() -> dict:
    """Checks Google accounts in cloud_accounts.db for quota & captcha flags."""
    db_path = Path("/home/ajf/.antigravity-agent/cloud_accounts.db")
    if not db_path.exists():
        return {"accounts_healthy": True, "active_count": 0, "quarantined_count": 0}
    try:
        conn = sqlite3.connect(str(db_path), timeout=2.0)
        c = conn.cursor()
        c.execute("SELECT email, status, health_json, quota_json FROM accounts")
        rows = c.fetchall()
        conn.close()

        active = 0
        quarantined = 0
        details = []
        for email, status_val, h_json, q_json in rows:
            h = {}
            if h_json:
                try:
                    h = json.loads(h_json)
                except Exception:
                    pass
            is_quar = (h.get("validation", {}).get("status") == "requires_action" or 
                       h.get("status_reason") == "FORBIDDEN")
            is_healthy = (status_val == "active" and not is_quar)
            if is_quar:
                quarantined += 1
            elif is_healthy:
                active += 1
            details.append({"email": email, "active": is_healthy, "quarantined": is_quar})
        return {
            "accounts_healthy": active > 0,
            "active_count": active,
            "quarantined_count": quarantined,
            "details": details
        }
    except Exception as e:
        return {"accounts_healthy": True, "error": str(e), "active_count": 0}

def probe_hermes_agent_smoke_test(force: bool = False, model: str = "gemini-3.8-flash-tiered") -> dict:
    """
    Executes an actual 1-shot Hermes Agent ping probe to verify that
    Hermes Agent can converse, reach the model, and burn tokens cleanly.
    Caches result for 90 seconds.
    """
    now = time.time()
    if not force and CACHE_FILE.exists():
        try:
            cached = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
            if now - cached.get("timestamp", 0) < 90:
                return cached
        except Exception:
            pass

    t0 = time.time()
    try:
        cmd = [
            "/home/ajf/.local/bin/hermes",
            "-m", model,
            "--yolo",
            "-z", "Respond strictly with: PONG"
        ]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=18)
        latency = round(time.time() - t0, 2)
        stdout = res.stdout.strip()
        stderr = res.stderr.strip()

        is_success = (res.returncode == 0 and "PONG" in stdout)
        result = {
            "agent_smoke_test": "PASSED" if is_success else "FAILED",
            "model_tested": model,
            "latency_s": latency,
            "exit_code": res.returncode,
            "output": stdout[:100],
            "error": stderr[:150] if not is_success else "",
            "timestamp": now,
            "tested_at": datetime.now().strftime("%H:%M:%S")
        }
        CACHE_FILE.write_text(json.dumps(result, indent=2), encoding="utf-8")
        return result
    except subprocess.TimeoutExpired:
        result = {
            "agent_smoke_test": "TIMED_OUT",
            "model_tested": model,
            "latency_s": round(time.time() - t0, 2),
            "exit_code": -1,
            "output": "",
            "error": "Probe timed out after 18s",
            "timestamp": now,
            "tested_at": datetime.now().strftime("%H:%M:%S")
        }
        CACHE_FILE.write_text(json.dumps(result, indent=2), encoding="utf-8")
        return result
    except Exception as e:
        result = {
            "agent_smoke_test": "ERROR",
            "model_tested": model,
            "latency_s": round(time.time() - t0, 2),
            "exit_code": -1,
            "output": "",
            "error": str(e),
            "timestamp": now,
            "tested_at": datetime.now().strftime("%H:%M:%S")
        }
        return result

def get_fleet_health_snapshot(run_smoke_test: bool = False) -> dict:
    gw = probe_gateway()
    acc = probe_cloud_accounts()
    
    smoke = {}
    if run_smoke_test:
        smoke = probe_hermes_agent_smoke_test(force=True)
    elif CACHE_FILE.exists():
        try:
            smoke = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass

    overall_healthy = gw["gateway_online"] and acc["accounts_healthy"]
    if smoke and smoke.get("agent_smoke_test") in ("FAILED", "TIMED_OUT"):
        overall_healthy = False

    return {
        "overall_status": "HEALTHY" if overall_healthy else "WARNING",
        "gateway": gw,
        "accounts": acc,
        "smoke_test": smoke,
        "evaluated_at": datetime.now().strftime("%H:%M:%S")
    }

if __name__ == "__main__":
    print(json.dumps(get_fleet_health_snapshot(run_smoke_test=True), indent=2))
