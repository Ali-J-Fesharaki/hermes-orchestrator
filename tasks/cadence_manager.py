#!/usr/bin/env python3
"""
cadence_manager.py: Dynamic Cadence & Event-Driven Control Plane for Hermes Fleet.
Exposes an external, non-invasive interface for independent monitoring repositories,
supervisors, and governors to:
  1. Inspect live repository execution states, active workers, error rates, and metrics.
  2. Dynamically tune polling cadences, timeouts, and concurrency slots per-repository.
  3. Emit event signals (force_plan, force_dispatch, emergency_pause) without restarts.
"""

from __future__ import annotations

import json
import time
import threading
from pathlib import Path
from datetime import datetime

AUTO_DIR = Path(__file__).resolve().parent.parent
DASH_DIR = AUTO_DIR / "dashboards"
CADENCE_FILE = DASH_DIR / "cadence_state.json"

_CADENCE_LOCK = threading.RLock()

DEFAULT_CADENCE_CONFIG = {
    "version": "1.0.0",
    "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "global": {
        "watchdog_cadence_s": 10.0,
        "router_probe_cadence_s": 20.0,
        "gc_cadence_s": 30.0,
        "emergency_system_pause": False
    },
    "default_repo_policy": {
        "planner_cadence_s": 20.0,
        "executor_cadence_s": 2.0,
        "max_workers": 2,
        "timeout_s": 1500,
        "auto_planning_enabled": True,
        "auto_execution_enabled": True
    },
    "repositories": {},
    "runtime_telemetry": {},
    "external_triggers": {}
}

class CadenceManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._init_state()
            return cls._instance

    def _init_state(self):
        with _CADENCE_LOCK:
            if not CADENCE_FILE.exists():
                self._save_file(DEFAULT_CADENCE_CONFIG)

    def _load_file(self) -> dict:
        with _CADENCE_LOCK:
            if not CADENCE_FILE.exists():
                return dict(DEFAULT_CADENCE_CONFIG)
            try:
                with open(CADENCE_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return dict(DEFAULT_CADENCE_CONFIG)

    def _save_file(self, data: dict):
        with _CADENCE_LOCK:
            data["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            temp_f = CADENCE_FILE.with_suffix(".tmp")
            with open(temp_f, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                f.write("\n")
            temp_f.replace(CADENCE_FILE)

    def get_repo_cadence(self, repo_id: str) -> dict:
        """Returns the dynamic cadence parameters for a specific repository."""
        data = self._load_file()
        default_policy = data.get("default_repo_policy", DEFAULT_CADENCE_CONFIG["default_repo_policy"])
        repo_policy = data.get("repositories", {}).get(repo_id, {})
        
        merged = dict(default_policy)
        merged.update(repo_policy)
        merged["emergency_system_pause"] = data.get("global", {}).get("emergency_system_pause", False)
        return merged

    def record_repo_event(self, repo_id: str, state: str, active_workers: int = 0, last_action: str = ""):
        """Records an operational event/state snapshot for external supervisors to observe."""
        with _CADENCE_LOCK:
            data = self._load_file()
            if "runtime_telemetry" not in data:
                data["runtime_telemetry"] = {}
            data["runtime_telemetry"][repo_id] = {
                "state": state,  # IDLE, PLANNING, EXECUTING, PAUSED, ERROR_BACKOFF
                "active_workers": active_workers,
                "last_action": last_action,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "epoch_ts": time.time()
            }
            self._save_file(data)

    def tune_repo_cadence(self, repo_id: str, params: dict) -> dict:
        """Allows an external governor to dynamically update timeouts and intervals."""
        with _CADENCE_LOCK:
            data = self._load_file()
            if "repositories" not in data:
                data["repositories"] = {}
            if repo_id not in data["repositories"]:
                data["repositories"][repo_id] = dict(data.get("default_repo_policy", DEFAULT_CADENCE_CONFIG["default_repo_policy"]))
            
            for k in ["planner_cadence_s", "executor_cadence_s", "max_workers", "timeout_s", "auto_planning_enabled", "auto_execution_enabled"]:
                if k in params:
                    data["repositories"][repo_id][k] = params[k]
                    
            self._save_file(data)
            return data["repositories"][repo_id]

    def emit_trigger(self, repo_id: str, trigger_type: str = "wake") -> bool:
        """Emits an external trigger (e.g. 'wake_planner', 'wake_executor', 'force_dispatch')."""
        with _CADENCE_LOCK:
            data = self._load_file()
            if "external_triggers" not in data:
                data["external_triggers"] = {}
            data["external_triggers"][f"{repo_id}:{trigger_type}"] = {
                "ts": time.time(),
                "handled": False
            }
            self._save_file(data)
            return True

    def consume_trigger(self, repo_id: str, trigger_type: str = "wake") -> bool:
        """Checks and consumes an external trigger."""
        with _CADENCE_LOCK:
            data = self._load_file()
            key = f"{repo_id}:{trigger_type}"
            triggers = data.get("external_triggers", {})
            if key in triggers and not triggers[key].get("handled", False):
                triggers[key]["handled"] = True
                self._save_file(data)
                return True
            return False

    def get_full_control_state(self) -> dict:
        return self._load_file()

_MANAGER = CadenceManager()

def get_cadence_manager() -> CadenceManager:
    return _MANAGER

if __name__ == "__main__":
    cm = get_cadence_manager()
    print("Cadence Control Plane initialized. Current state:")
    print(json.dumps(cm.get_full_control_state(), indent=2))
