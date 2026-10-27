#!/usr/bin/env python3
"""
platform_supervisor_agent.py: Autonomous 24/7 Platform Supervisor & Health Sentinel
Audits all supervisory, tracing, routing, and execution modules every 10 minutes.
If any component fails:
  1. Captures root cause and full error diagnostic.
  2. Catalogs failure into failed_tasks_registry.json & FAILED_TASKS_REPORT.md.
  3. Attempts autonomous self-healing restarts for systemd services.
  4. Surfaces alert to Telemetry and Cockpit UI so recovery plans are synthesized.
"""

from __future__ import annotations

import os
import sys
import json
import time
import subprocess
import urllib.request
from pathlib import Path
from datetime import datetime

AUTO_DIR = Path(__file__).resolve().parent.parent
DASHBOARDS_DIR = AUTO_DIR / "dashboards"
TASKS_DIR = AUTO_DIR / "tasks"

for p in [str(AUTO_DIR), str(DASHBOARDS_DIR), str(TASKS_DIR)]:
    if p not in sys.path:
        sys.path.insert(0, p)

AUDIT_LOG_FILE = DASHBOARDS_DIR / "SUPERVISOR_AUDIT_REPORT.json"

def get_auth_token() -> str:
    env_file = Path("/home/ajf/.hermes/.env")
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and any(k in line for k in ["HERMES_CUSTOM_ANTIGRAVITY_TOOLS_API_KEY", "ANTIGRAVITY_API_KEY", "KEY"]):
                val = line.split("=", 1)[1].strip().strip("\"'")
                if val:
                    return val
    return ""

class PlatformSupervisorAgent:
    def __init__(self):
        self.results: dict[str, dict] = {}
        self.critical_failures: list[dict] = []

    def check_control_center(self) -> bool:
        """Verifies Cockpit Web UI (:5050) is alive and serving status API."""
        try:
            req = urllib.request.Request("http://127.0.0.1:5050/api/status")
            with urllib.request.urlopen(req, timeout=2.5) as r:
                data = json.loads(r.read().decode())
                if "repositories" in data:
                    self.results["control_center"] = {
                        "status": "HEALTHY",
                        "port": 5050,
                        "repos_count": len(data.get("repositories", [])),
                        "active_agents": len(data.get("telemetry", {}).get("active_agents", {}))
                    }
                    return True
        except Exception as e:
            self._handle_failure("control_center", f"Control Center (:5050) unreachable or returned invalid JSON: {e}", "control_center/server.py")
            self._attempt_service_restart("hermes-control-center.service")
            return False
        return False

    def check_llm_router(self) -> bool:
        """Verifies local LLM router (:8045 / :7836) is serving models."""
        try:
            from fleet_health_sentinel import probe_gateway
            gw = probe_gateway()
            if gw.get("gateway_online"):
                self.results["llm_router"] = {
                    "status": "HEALTHY",
                    "port": gw.get("port"),
                    "models_count": gw.get("models_count"),
                    "models_sample": gw.get("models", [])[:4]
                }
                return True
        except Exception as e:
            pass

        self._handle_failure("llm_router", "Local LLM Router (:8045 / :7836) is offline or rejecting bearer tokens.", "tasks/fleet_health_sentinel.py")
        return False

    def check_phoenix_tracing(self) -> bool:
        """Verifies Arize Phoenix observability server (:6006) is responsive."""
        try:
            req = urllib.request.Request("http://127.0.0.1:6006/")
            with urllib.request.urlopen(req, timeout=2.5) as r:
                if r.status in (200, 302, 301):
                    self.results["phoenix_tracing"] = {"status": "HEALTHY", "port": 6006}
                    return True
        except Exception as e:
            self._handle_failure("phoenix_tracing", f"Arize Phoenix (:6006) server is down: {e}", "phoenix_tracer.py")
            self._attempt_service_restart("arize-phoenix.service")
            return False
        return False

    def check_orchestrator_daemon(self) -> bool:
        """Verifies background autonomous-orchestrator.service is active."""
        try:
            res = subprocess.run(["systemctl", "--user", "is-active", "autonomous-orchestrator.service"], capture_output=True, text=True)
            state = res.stdout.strip()
            if state == "active":
                self.results["orchestrator_daemon"] = {"status": "HEALTHY", "service_state": "active"}
                return True
            else:
                self._handle_failure("orchestrator_daemon", f"autonomous-orchestrator.service is {state} (expected 'active').", "tasks/autonomous_orchestrator.py")
                self._attempt_service_restart("autonomous-orchestrator.service")
                return False
        except Exception as e:
            self._handle_failure("orchestrator_daemon", f"Failed checking systemd service state: {e}", "tasks/autonomous_orchestrator.py")
            return False

    def check_hermes_agent_smoke(self) -> bool:
        """Runs a fast 1-shot Hermes Agent smoke test (PONG probe)."""
        try:
            from fleet_health_sentinel import probe_hermes_agent_smoke_test
            smoke = probe_hermes_agent_smoke_test(force=False)
            if smoke.get("agent_smoke_test") == "PASSED":
                self.results["hermes_smoke_test"] = {
                    "status": "HEALTHY",
                    "latency_s": smoke.get("latency_s"),
                    "output": smoke.get("output")
                }
                return True
            else:
                self._handle_failure("hermes_smoke_test", f"Hermes Agent 1-shot smoke test failed: {smoke.get('error') or smoke.get('output')}", "tasks/fleet_health_sentinel.py")
                return False
        except Exception as e:
            self._handle_failure("hermes_smoke_test", f"Failed executing Hermes Agent smoke probe: {e}", "tasks/fleet_health_sentinel.py")
            return False

    def check_data_registries_integrity(self) -> bool:
        """Verifies that repositories.json, failed_tasks_registry.json, and telemetry.json have valid JSON."""
        all_ok = True
        files = {
            "repositories": DASHBOARDS_DIR / "repositories.json",
            "failed_tasks": DASHBOARDS_DIR / "failed_tasks_registry.json",
            "telemetry": DASHBOARDS_DIR / "telemetry.json"
        }
        for name, fpath in files.items():
            if not fpath.exists():
                self._handle_failure(f"registry_{name}", f"Registry file missing: {fpath}", str(fpath.relative_to(AUTO_DIR)))
                all_ok = False
                continue
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    json.load(f)
                self.results[f"registry_{name}"] = {"status": "HEALTHY", "size_bytes": fpath.stat().st_size}
            except Exception as e:
                self._handle_failure(f"registry_{name}", f"Syntax/JSON corruption in {name}: {e}", str(fpath.relative_to(AUTO_DIR)))
                all_ok = False
        return all_ok

    def _attempt_service_restart(self, service_name: str):
        """Attempts safe systemd restart for crashed user services."""
        try:
            print(f"[SUPERVISOR HEAL] Attempting auto-restart of {service_name}...")
            subprocess.run(["systemctl", "--user", "restart", service_name], timeout=5)
            time.sleep(1.5)
            res = subprocess.run(["systemctl", "--user", "is-active", service_name], capture_output=True, text=True)
            if res.stdout.strip() == "active":
                print(f"[SUPERVISOR HEAL OK] {service_name} restarted successfully!")
            else:
                print(f"[SUPERVISOR HEAL WARN] {service_name} failed to become active.")
        except Exception as e:
            print(f"[-] Auto-restart exception for {service_name}: {e}")

    def _handle_failure(self, component: str, error_msg: str, target_file: str):
        """Catalogs failure into failed_tasks_registry.json for autonomous resolution."""
        print(f"\n🚨 [SUPERVISOR DETECTED FAILURE] Component: {component}")
        print(f"• Diagnostic: {error_msg}")
        
        self.results[component] = {
            "status": "FAILED",
            "error": error_msg,
            "detected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        self.critical_failures.append({
            "component": component,
            "error": error_msg,
            "target_file": target_file
        })

        # 1. Record structured failure in FailedTaskRegistry
        try:
            from failed_task_registry import get_registry
            reg = get_registry()
            milestone_title = f"Fix: Supervisory Alert - Restore {component.replace('_', ' ').title()}"
            reg.record_failure(
                repo_id="hermes_orchestrator",
                repo_name="Hermes Multi-Repo Orchestrator",
                milestone_name=milestone_title,
                failure_reason=f"Platform Supervisor Watchdog Audit Failed:\n{error_msg}",
                target_file=target_file,
                model_used="platform-supervisor-agent"
            )
            print(f"[+] [SUPERVISOR] Registered structured bugfix task: '{milestone_title}'")
        except Exception as re:
            print(f"[-] Could not record failure in registry: {re}")

        # 2. Record failure event in telemetry
        try:
            from telemetry import load_telemetry, save_telemetry
            tdata = load_telemetry()
            event = f"[{datetime.now().strftime('%H:%M:%S')}] 🚨 SUPERVISOR ALERT: {component} failed: {error_msg[:80]}"
            tdata["recent_events"] = [event] + tdata.get("recent_events", [])[:15]
            save_telemetry(tdata)
        except Exception:
            pass

    def run_full_audit(self) -> dict:
        t0 = time.time()
        print(f"\n" + "=" * 75)
        print(f" 🛡️  PLATFORM SUPERVISOR AGENT AUDIT ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
        print("=" * 75)

        self.check_control_center()
        self.check_llm_router()
        self.check_phoenix_tracing()
        self.check_orchestrator_daemon()
        self.check_hermes_agent_smoke()
        self.check_data_registries_integrity()

        elapsed = round(time.time() - t0, 2)
        healthy_count = sum(1 for v in self.results.values() if v.get("status") == "HEALTHY")
        total_checks = len(self.results)
        is_all_healthy = len(self.critical_failures) == 0

        summary = {
            "supervisor_status": "ALL_HEALTHY" if is_all_healthy else "ISSUES_DETECTED",
            "healthy_components": f"{healthy_count}/{total_checks}",
            "elapsed_seconds": elapsed,
            "audit_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "critical_failures_count": len(self.critical_failures),
            "components": self.results
        }

        try:
            AUDIT_LOG_FILE.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        except Exception:
            pass

        print(f"\n[*] Audit Complete ({elapsed}s). Status: {summary['supervisor_status']} ({healthy_count}/{total_checks} Healthy)")
        if not is_all_healthy:
            print(f"🚨 Recorded {len(self.critical_failures)} bugfix tasks for autonomous remediation in next cycle.")
        print("=" * 75)
        return summary

if __name__ == "__main__":
    agent = PlatformSupervisorAgent()
    summary = agent.run_full_audit()
    sys.exit(0 if summary["supervisor_status"] == "ALL_HEALTHY" else 1)
