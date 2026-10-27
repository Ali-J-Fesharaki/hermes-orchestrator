#!/usr/bin/env python3
"""
server.py: Lightweight Local Backend for Hermes Agent & Multi-Repo Control Center.
Runs on http://127.0.0.1:5050
"""

import os
import sys
import re
import time
import json
import sqlite3
import threading
import subprocess
from datetime import datetime
from pathlib import Path
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import urllib.request

BASE_DIR = Path(__file__).resolve().parent.parent
DASH_DIR = BASE_DIR / "dashboards"
WEB_DIR = BASE_DIR / "control_center"
sys.path.insert(0, str(DASH_DIR))
sys.path.insert(0, str(BASE_DIR / "tasks"))

try:
    from telemetry import load_telemetry
except ImportError:
    def load_telemetry():
        return {}

try:
    import github_project_sync as gh_sync
except ImportError:
    gh_sync = None

try:
    import architect_steering as ast
except ImportError:
    ast = None

try:
    from autonomous_orchestrator import synthesize_steered_milestones
except ImportError:
    synthesize_steered_milestones = None

try:
    import token_quota_optimizer as tqo
except ImportError:
    tqo = None

try:
    from tasks.skill_matcher import infer_task_skills
except ImportError:
    try:
        from skill_matcher import infer_task_skills
    except ImportError:
        infer_task_skills = lambda m, r="", c="": ["systematic-debugging", "test-driven-development", "codebase-inspection"]

def is_orchestrator_running():
    my_pid = os.getpid()
    try:
        for pid_str in os.listdir("/proc"):
            if pid_str.isdigit() and int(pid_str) != my_pid:
                try:
                    with open(f"/proc/{pid_str}/cmdline", "rb") as f:
                        args = f.read().split(b"\x00")
                        if any(b"autonomous_orchestrator.py" in a for a in args) and not any(b"-c" == a for a in args):
                            return int(pid_str)
                except Exception:
                    pass
    except Exception:
        pass
    return None

PORT = 5050

class ControlCenterHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(WEB_DIR), **kwargs)

    def log_message(self, format, *args):
        # Mute routine logs
        pass

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/status":
            self.handle_status()
        elif parsed.path == "/api/models/rankings":
            self.handle_model_rankings()
        elif parsed.path == "/api/models":
            self.handle_get_models()
        elif parsed.path == "/api/steering/history":
            self.handle_steering_history()
        elif parsed.path == "/api/quota/optimizer":
            self.handle_quota_optimizer()
        elif parsed.path == "/api/orchestrator/status":
            self.handle_orchestrator_status()
        elif parsed.path == "/api/failed_tasks":
            self.handle_failed_tasks()
        elif parsed.path == "/api/fleet_health":
            self.handle_fleet_health()
        elif parsed.path == "/api/roadmap":
            self.handle_roadmap()
        elif parsed.path == "/api/capacity/forecast":
            self.handle_capacity_forecast()
        elif parsed.path == "/api/supervisor/report":
            self.handle_supervisor_report()
        elif parsed.path == "/api/performance/benchmark":
            self.handle_performance_benchmark()
        elif parsed.path == "/api/repository_evolution":
            self.handle_repository_evolution(parsed)
        elif parsed.path == "/api/cadence":
            self.handle_get_cadence()
        else:
            super().do_GET()

    def handle_get_cadence(self):
        try:
            try:
                from tasks.cadence_manager import get_cadence_manager
            except ImportError:
                from cadence_manager import get_cadence_manager  # type: ignore
            data = get_cadence_manager().get_full_control_state()
        except Exception as e:
            data = {"error": str(e)}
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))

    def handle_repository_evolution(self, parsed):
        from urllib.parse import parse_qs
        qs = parse_qs(parsed.query)
        repo_id = qs.get("repo", ["hermes_orchestrator"])[0]
        try:
            try:
                from tasks.repository_evolution_engine import calculate_repository_evolution_metrics
            except ImportError:
                from repository_evolution_engine import calculate_repository_evolution_metrics  # type: ignore
            data = calculate_repository_evolution_metrics(repo_id)
        except Exception as e:
            data = {"error": str(e), "repo_id": repo_id}
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))

    def handle_performance_benchmark(self):
        try:
            from performance_profiler import get_performance_report
            data = get_performance_report()
        except Exception as e:
            data = {"error": str(e)}
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))

    def handle_capacity_forecast(self):
        try:
            from capacity_forecaster import get_fleet_capacity_forecast
            data = get_fleet_capacity_forecast()
        except Exception as e:
            data = {"error": str(e), "recommended_worker_slots": 2}
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))

    def handle_supervisor_report(self):
        report_file = DASH_DIR / "SUPERVISOR_AUDIT_REPORT.json"
        data = {"supervisor_status": "UNKNOWN", "healthy_components": "0/0"}
        if report_file.exists():
            try:
                data = json.loads(report_file.read_text(encoding="utf-8"))
            except Exception:
                pass
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))

    def handle_roadmap(self):
        roadmap_path = BASE_DIR / "FUTURE_ROADMAP.md"
        content = ""
        if roadmap_path.exists():
            content = roadmap_path.read_text(encoding="utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({"content": content, "path": "FUTURE_ROADMAP.md"}).encode("utf-8"))

    def handle_fleet_health(self):
        try:
            from fleet_health_sentinel import get_fleet_health_snapshot
            data = get_fleet_health_snapshot(run_smoke_test=False)
        except Exception as e:
            data = {"overall_status": "ERROR", "error": str(e)}
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))

    def handle_failed_tasks(self):
        failed_tasks_file = DASH_DIR / "failed_tasks_registry.json"
        data = {"total_recorded_failures": 0, "pending_bugfix_tasks": 0, "failures": []}
        if failed_tasks_file.exists():
            try:
                data = json.loads(failed_tasks_file.read_text(encoding="utf-8"))
            except Exception as e:
                data["error"] = str(e)
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))

    def handle_quota_optimizer(self):
        try:
            from token_quota_optimizer import get_current_optimizer_state
            data = get_current_optimizer_state()
        except Exception as e:
            data = {"status": "ERROR", "can_run": False, "error": str(e)}
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))

    def handle_orchestrator_status(self):
        pid = is_orchestrator_running()
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({"running": bool(pid), "pid": pid}).encode("utf-8"))

    def handle_steering_history(self):
        hist = {}
        if ast:
            try:
                hist = ast.load_steering_history()
            except Exception:
                pass
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(hist, ensure_ascii=False).encode("utf-8"))

    def handle_model_rankings(self):
        rankings = []
        try:
            from dynamic_planner_ranker import get_dynamic_planning_cascade
            rankings = get_dynamic_planning_cascade()
        except Exception:
            pass
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(rankings, ensure_ascii=False).encode("utf-8"))

    def do_POST(self):
        parsed = urlparse(self.path)
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else "{}"
        try:
            payload = json.loads(body)
        except Exception:
            payload = {}

        # Any POST request is a state mutation — invalidate in-memory snapshot cache immediately!
        invalidate_snapshot_cache()

        if parsed.path == "/api/milestone/toggle":
            self.handle_toggle_milestone(payload)
        elif parsed.path == "/api/milestone/add":
            self.handle_add_milestone(payload)
        elif parsed.path == "/api/trigger_orchestrator":
            self.handle_trigger_orchestrator()
        elif parsed.path == "/api/orchestrator/toggle_auto":
            self.handle_toggle_auto_loop()
        elif parsed.path == "/api/github/sync":
            self.handle_github_sync()
        elif parsed.path == "/api/plan/steer":
            self.handle_steer_planner(payload)
        elif parsed.path in ["/api/card/ai_refine", "/api/milestone/ai_refine"]:
            self.handle_card_ai_refine(payload)
        elif parsed.path in ["/api/card/save", "/api/milestone/edit"]:
            self.handle_card_save(payload)
        elif parsed.path in ["/api/card/delete", "/api/milestone/delete"]:
            self.handle_card_delete(payload)
        elif parsed.path in ["/api/card/toggle_approval", "/api/milestone/toggle_approval"]:
            self.handle_card_toggle_approval(payload)
        elif parsed.path == "/api/repo/add":
            self.handle_add_repo(payload)
        elif parsed.path == "/api/repo/remove":
            self.handle_remove_repo(payload)
        elif parsed.path == "/api/repo/toggle_auto":
            self.handle_toggle_repo_auto(payload)
        elif parsed.path == "/api/repo/toggle_auto_approve":
            self.handle_toggle_repo_auto_approve(payload)
        elif parsed.path == "/api/repo/toggle_all_auto_approve":
            self.handle_toggle_all_auto_approve(payload)
        elif parsed.path == "/api/repo/update_models":
            self.handle_update_repo_models(payload)
        elif parsed.path == "/api/repo/update_cap":
            self.handle_update_repo_cap(payload)
        elif parsed.path == "/api/telemetry/clear_agents":
            self.handle_clear_agents()
        elif parsed.path == "/api/fleet/smoke_test":
            self.handle_fleet_smoke_test()
        elif parsed.path == "/api/supervisor/audit_now":
            self.handle_supervisor_audit_now()
        elif parsed.path == "/api/repository_evolution/steer":
            self.handle_repository_evolution_steer(payload)
        elif parsed.path == "/api/cadence/tune":
            self.handle_cadence_tune(payload)
        else:
            self.send_error(404, "Endpoint not found")

    def handle_cadence_tune(self, payload):
        repo_id = payload.get("repo_id", "")
        params = payload.get("parameters", {})
        trigger = payload.get("trigger", "")
        try:
            try:
                from tasks.cadence_manager import get_cadence_manager
            except ImportError:
                from cadence_manager import get_cadence_manager  # type: ignore
            cm = get_cadence_manager()
            res = {}
            if repo_id and params:
                res["tuned"] = cm.tune_repo_cadence(repo_id, params)
            if repo_id and trigger:
                cm.emit_trigger(repo_id, trigger)
                res["triggered"] = trigger
            res["status"] = "ok"
        except Exception as e:
            res = {"status": "error", "error": str(e)}

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(res, ensure_ascii=False).encode("utf-8"))

    def handle_repository_evolution_steer(self, payload):
        repo_id = payload.get("repo_id", "hermes_orchestrator")
        directive_type = payload.get("directive_type", "EXPEDITE_CORE_ROADMAP")
        custom_instructions = payload.get("custom_instructions", "")
        target_cap = int(payload.get("target_cap", 3))
        try:
            try:
                from tasks.repository_evolution_engine import apply_strategic_planning_directive
            except ImportError:
                from repository_evolution_engine import apply_strategic_planning_directive  # type: ignore
            res = apply_strategic_planning_directive(repo_id, directive_type, custom_instructions, target_cap)
        except Exception as e:
            res = {"status": "error", "message": str(e)}
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(res, ensure_ascii=False).encode("utf-8"))

    def handle_supervisor_audit_now(self):
        try:
            from platform_supervisor_agent import PlatformSupervisorAgent
            agent = PlatformSupervisorAgent()
            data = agent.run_full_audit()
        except Exception as e:
            data = {"supervisor_status": "ERROR", "error": str(e)}
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))

    def handle_fleet_smoke_test(self):
        try:
            from fleet_health_sentinel import probe_hermes_agent_smoke_test
            data = probe_hermes_agent_smoke_test(force=True)
        except Exception as e:
            data = {"agent_smoke_test": "ERROR", "error": str(e)}
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))

    def handle_clear_agents(self):
        pruned_count = 0
        try:
            from telemetry import clean_stale_agents
            # Safe pruning: Never touches processes actively running on the OS!
            pruned_count = clean_stale_agents(max_age_seconds=600, force=False)
        except Exception:
            pass
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "ok", "cleared": True, "pruned": pruned_count}).encode("utf-8"))

    def handle_card_ai_refine(self, payload):
        repo_id = payload.get("repo_id", "phd_proposal")
        repo_name = payload.get("repo_name", "General")
        curr_title = payload.get("title", "")
        curr_details = payload.get("details", "")
        curr_filepath = payload.get("relative_filepath", "")
        user_input = payload.get("user_input", "")

        refined = {}
        if ast and user_input:
            refined = ast.ai_refine_card(repo_id, repo_name, curr_title, curr_details, curr_filepath, user_input)

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(refined, ensure_ascii=False).encode("utf-8"))

    def handle_card_save(self, payload):
        repo_id = payload.get("repo_id")
        old_name = payload.get("old_name")
        new_name = payload.get("name")
        new_details = payload.get("details")
        new_filepath = payload.get("relative_filepath", "")
        user_speech = payload.get("user_speech", "")
        arch_principle = payload.get("architectural_principle", "")

        repos_file = DASH_DIR / "repositories.json"
        saved = False
        target_m = None
        old_m = {}

        if repos_file.exists():
            try:
                with open(repos_file, "r", encoding="utf-8") as f:
                    rdata = json.load(f)
                for r in rdata.get("repositories", []):
                    if r.get("id") == repo_id or r.get("name") == repo_id:
                        for m in r.get("milestones", []):
                            if m.get("name") == old_name:
                                old_m = dict(m)
                                m["name"] = new_name
                                m["details"] = new_details
                                if new_filepath is not None:
                                    m["relative_filepath"] = new_filepath
                                new_status = payload.get("status")
                                if new_status:
                                    m["status"] = new_status
                                    if new_status == "completed" and (not m.get("skills") or len(m.get("skills")) < 3):
                                        m["skills"] = infer_task_skills(m, repo_id, r.get("category", ""))
                                if "skills" in payload and isinstance(payload["skills"], list):
                                    m["skills"] = payload["skills"][:3]
                                m["last_edited_by_human"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                target_m = m
                                saved = True
                                break
                if saved:
                    with open(repos_file, "w", encoding="utf-8") as f:
                        json.dump(rdata, f, indent=2, ensure_ascii=False)
            except Exception as e:
                print(f"[-] Error saving card: {e}")

        # 1. Record in persistent Human Steering History
        if saved and ast and user_speech:
            try:
                ast.record_card_edit(repo_id, old_m, target_m, user_speech, arch_principle)
            except Exception as e:
                print(f"[-] Error recording steering: {e}")

        # 2. Update on GitHub Projects v2
        if target_m and target_m.get("github_item_id") and gh_sync:
            try:
                gh_title = f"[{repo_id}] {new_name}"
                gh_body = f"**Planner Model:** `Human Architect (Refined)`\n\n**Details:** {new_details}\n\n**File Target:** `{new_filepath or '—'}`\n\n*Updated via Hermes Cockpit Studio*"
                gh_sync.update_project_card_content(target_m["github_item_id"], gh_title, gh_body)
                
                # Also synchronize the status column to GitHub Projects v2
                st = target_m.get("status", "pending")
                target_gh_col = "Done" if st == "completed" else ("In progress" if st == "in_progress" else ("In review" if st == "in_review" else ("Ready" if st in ["pending", "todo"] else "Backlog")))
                gh_sync.set_milestone_github_status(new_name, target_gh_col)
            except Exception as e:
                print(f"[-] GitHub card sync error: {e}")

        # 3. Trigger board re-render
        try:
            subprocess.run(["python3", str(DASH_DIR / "note_converter.py")], capture_output=True, timeout=5)
        except Exception:
            pass

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "success" if saved else "not_found"}).encode("utf-8"))

    def handle_card_delete(self, payload):
        repo_id = payload.get("repo_id")
        m_name = payload.get("milestone_name")
        add_to_blacklist = bool(payload.get("blacklist", False))
        repos_file = DASH_DIR / "repositories.json"
        deleted = False
        deleted_m = {}

        if repos_file.exists():
            try:
                with open(repos_file, "r", encoding="utf-8") as f:
                    rdata = json.load(f)
                for r in rdata.get("repositories", []):
                    if r.get("id") == repo_id or r.get("name") == repo_id:
                        m_list = r.get("milestones", [])
                        new_list = []
                        for m in m_list:
                            if m.get("name") == m_name:
                                deleted_m = dict(m)
                                deleted = True
                            else:
                                new_list.append(m)
                        if deleted:
                            r["milestones"] = new_list
                if deleted:
                    with open(repos_file, "w", encoding="utf-8") as f:
                        json.dump(rdata, f, indent=2, ensure_ascii=False)
            except Exception as e:
                print(f"[-] Error deleting card: {e}")

        # 1. Record negative constraint into architect_steering_history.json ONLY IF user requested blacklisting
        if deleted and add_to_blacklist and ast:
            try:
                ast.record_discarded_milestone(repo_id, m_name, deleted_m.get("details", ""), "Explicitly blacklisted by human architect in Cockpit")
                print(f"🚫 [PLANNER BLACKLIST] Milestone '{m_name}' added to forbidden memory for '{repo_id}'.")
            except Exception as e:
                print(f"[-] Error recording negative steering: {e}")
        elif deleted:
            print(f"ℹ️ [CARD DELETED] Milestone '{m_name}' deleted without planner blacklisting.")

        # 2. Delete item from GitHub Projects v2
        if deleted and deleted_m.get("github_item_id") and gh_sync:
            try:
                pdata = gh_sync.fetch_project_v2_data()
                pid = pdata.get("project_id")
                if pid:
                    gh_sync.delete_project_v2_item(pid, deleted_m["github_item_id"])
            except Exception as e:
                print(f"[-] GitHub delete item error: {e}")

        # 3. Trigger board re-render
        try:
            subprocess.run(["python3", str(DASH_DIR / "note_converter.py")], capture_output=True, timeout=5)
        except Exception:
            pass

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({
            "status": "success" if deleted else "not_found",
            "blacklisted": add_to_blacklist,
            "milestone_name": m_name
        }, ensure_ascii=False).encode("utf-8"))

    def handle_card_toggle_approval(self, payload):
        repo_id = payload.get("repo_id")
        m_name = payload.get("name") or payload.get("milestone_name")
        repos_file = DASH_DIR / "repositories.json"
        updated = False
        new_approved_val = False

        if repos_file.exists():
            try:
                with open(repos_file, "r", encoding="utf-8") as f:
                    rdata = json.load(f)
                for r in rdata.get("repositories", []):
                    if r.get("id") == repo_id or r.get("name") == repo_id:
                        for m in r.get("milestones", []):
                            if m.get("name") == m_name:
                                curr = m.get("approved_for_auto_run", False)
                                new_approved_val = not curr
                                m["approved_for_auto_run"] = new_approved_val
                                m["is_ai_development"] = True
                                if new_approved_val:
                                    m["status"] = "ready"
                                else:
                                    m["status"] = "todo"
                                m["approval_updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                updated = True

                                # Live sync card to GitHub Projects v2 column (Ready vs Backlog)
                                if m.get("github_item_id") and gh_sync:
                                    try:
                                        target_col = "Ready" if new_approved_val else "Backlog"
                                        gh_sync.set_milestone_github_status(m_name, target_col)
                                    except Exception as ge:
                                        print(f"[-] GitHub approval sync notice: {ge}")
                                break
                if updated:
                    with open(repos_file, "w", encoding="utf-8") as f:
                        json.dump(rdata, f, indent=2, ensure_ascii=False)
            except Exception as e:
                print(f"[-] Error toggling auto-run approval: {e}")

        # Trigger board re-render
        try:
            subprocess.run(["python3", str(DASH_DIR / "note_converter.py")], capture_output=True, timeout=5)
        except Exception:
            pass

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({
            "status": "success" if updated else "not_found",
            "approved_for_auto_run": new_approved_val,
            "milestone_name": m_name
        }, ensure_ascii=False).encode("utf-8"))

    def handle_add_repo(self, payload):
        import time
        repo_name = payload.get("name", "").strip()
        repo_path_input = payload.get("path", "").strip()
        category = payload.get("category", "general-software").strip()
        priority = int(payload.get("priority", 2))
        auto_planning = bool(payload.get("auto_planning", True))
        milestone_cap = int(payload.get("milestone_cap", 3))
        milestone_cap = max(1, min(10, milestone_cap))
        token_budget = int(payload.get("daily_token_budget_est", 60000))

        if not repo_name or not repo_path_input:
            self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Repository name and path/URL are required"}).encode("utf-8"))
            return

        # Determine path & clone if it's a GitHub URL or owner/repo
        repo_path_str = repo_path_input.strip()
        if repo_path_str.startswith("~"):
            repo_path_str = str(Path.home() / repo_path_str[1:].lstrip("/"))
            
        repo_path = Path(repo_path_str)
        if repo_path_str.startswith("http://") or repo_path_str.startswith("https://") or ("github.com" in repo_path_str):
            dir_name = repo_path_str.rstrip("/").split("/")[-1].replace(".git", "")
            target_dir = Path.home() / dir_name
            if not target_dir.exists():
                try:
                    subprocess.run(["git", "clone", repo_path_str, str(target_dir)], check=True, timeout=120)
                except Exception as e:
                    print(f"[-] Git clone failed: {e}")
            repo_path = target_dir
        elif "/" in repo_path_str and not repo_path_str.startswith("/"):
            dir_name = repo_path_str.split("/")[-1]
            target_dir = Path.home() / dir_name
            if not target_dir.exists():
                clone_url = f"https://github.com/{repo_path_str}.git"
                try:
                    subprocess.run(["git", "clone", clone_url, str(target_dir)], check=True, timeout=120)
                except Exception as e:
                    print(f"[-] Git clone failed: {e}")
            repo_path = target_dir
        elif not repo_path.is_absolute():
            repo_path = Path.home() / repo_path

        # Intelligent fuzzy discovery if target path doesn't exist directly
        # E.g. user types 'Map-gz', 'map-gz', or 'mapgz' when on disk it is 'Map2Gz'
        if not repo_path.exists():
            clean_input = re.sub(r'[^a-zA-Z0-9]', '', repo_path.name.lower())
            for item in Path.home().iterdir():
                if item.is_dir() and (item / ".git").exists():
                    clean_item = re.sub(r'[^a-zA-Z0-9]', '', item.name.lower())
                    if (clean_item == clean_input or 
                        clean_item == clean_input.replace('gz', '2gz') or 
                        clean_item.replace('2', '') == clean_input.replace('-', '')):
                        repo_path = item
                        print(f"[+] Auto-discovered existing Git repository on disk: {repo_path}")
                        break

        # Generate slug id
        repo_id = repo_name.lower().replace(" ", "_").replace("-", "_")
        for ch in "!@#$%^&*()+=[]{}|\\:;\"'<>,.?/":
            repo_id = repo_id.replace(ch, "")
        if not repo_id:
            repo_id = f"repo_{int(time.time())}"

        # Initialize ROADMAP or goal file if doesn't exist
        goal_file = "ROADMAP.md" if (repo_path / "ROADMAP.md").exists() else "README.md"
        if repo_path.exists():
            if not (repo_path / goal_file).exists():
                try:
                    (repo_path / goal_file).write_text(f"# {repo_name}\n\nAutomated repository tracked by Hermes Multi-Repo Orchestrator.\n", encoding="utf-8")
                except Exception:
                    pass

        planner_model = payload.get("planner_model")
        executor_model = payload.get("executor_model")
        if planner_model == "auto": planner_model = None
        if executor_model == "auto": executor_model = None

        new_repo_entry = {
            "id": repo_id,
            "name": repo_name,
            "path": str(repo_path),
            "priority": priority,
            "primary_branch": "main",
            "active_branch": "main",
            "category": category,
            "auto_planning": auto_planning,
            "auto_approve_plans": False,
            "planner_model": planner_model,
            "executor_model": executor_model,
            "milestone_cap": milestone_cap,
            "daily_token_budget_est": token_budget,
            "goal_file": goal_file,
            "milestones": []
        }

        repos_file = DASH_DIR / "repositories.json"
        saved = False
        if repos_file.exists():
            try:
                with open(repos_file, "r", encoding="utf-8") as f:
                    rdata = json.load(f)
                existing = [r for r in rdata.get("repositories", []) if r.get("id") == repo_id]
                if not existing:
                    rdata.setdefault("repositories", []).append(new_repo_entry)
                    with open(repos_file, "w", encoding="utf-8") as f:
                        json.dump(rdata, f, indent=2, ensure_ascii=False)
                    saved = True
            except Exception as e:
                print(f"[-] Error adding repo: {e}")

        try:
            subprocess.run(["python3", str(DASH_DIR / "note_converter.py")], capture_output=True, timeout=5)
        except Exception:
            pass

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "success" if saved else "exists", "repo": new_repo_entry}).encode("utf-8"))

    def handle_remove_repo(self, payload):
        repo_id = payload.get("repo_id")
        repos_file = DASH_DIR / "repositories.json"
        removed = False
        target_repo = None
        if repos_file.exists():
            try:
                with open(repos_file, "r", encoding="utf-8") as f:
                    rdata = json.load(f)
                orig_len = len(rdata.get("repositories", []))
                for r in rdata.get("repositories", []):
                    if r.get("id") == repo_id or r.get("name") == repo_id:
                        target_repo = r
                        break
                rdata["repositories"] = [r for r in rdata.get("repositories", []) if r.get("id") != repo_id and r.get("name") != repo_id]
                if len(rdata["repositories"]) < orig_len:
                    removed = True
                    removed_list = rdata.setdefault("removed_repositories", [])
                    if repo_id not in removed_list:
                        removed_list.append(repo_id)
                    with open(repos_file, "w", encoding="utf-8") as f:
                        json.dump(rdata, f, indent=2, ensure_ascii=False)
            except Exception as e:
                print(f"[-] Error removing repo: {e}")

        # Delete all draft cards belonging to this repo from GitHub Projects v2!
        gh_deleted = 0
        if removed and gh_sync:
            try:
                repo_name = target_repo.get("name", "") if target_repo else ""
                gh_deleted = gh_sync.delete_repo_items_from_github(repo_id, repo_name)
                print(f"[+] Deleted {gh_deleted} draft items for repo '{repo_id}' from GitHub Projects v2.")
            except Exception as ge:
                print(f"[-] GitHub repo drafts cleanup error: {ge}")

        try:
            subprocess.run(["python3", str(DASH_DIR / "note_converter.py")], capture_output=True, timeout=5)
        except Exception:
            pass

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "success" if removed else "not_found", "github_deleted_items": gh_deleted}).encode("utf-8"))

    def handle_toggle_repo_auto(self, payload):
        repo_id = payload.get("repo_id")
        repos_file = DASH_DIR / "repositories.json"
        new_val = None
        if repos_file.exists():
            try:
                with open(repos_file, "r", encoding="utf-8") as f:
                    rdata = json.load(f)
                for r in rdata.get("repositories", []):
                    if r.get("id") == repo_id or r.get("name") == repo_id:
                        curr = r.get("auto_planning", True)
                        r["auto_planning"] = not curr
                        new_val = r["auto_planning"]
                        break
                if new_val is not None:
                    with open(repos_file, "w", encoding="utf-8") as f:
                        json.dump(rdata, f, indent=2, ensure_ascii=False)
            except Exception as e:
                print(f"[-] Error toggling auto-planning: {e}")

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "success", "auto_planning": new_val}).encode("utf-8"))

    def handle_toggle_repo_auto_approve(self, payload):
        repo_id = payload.get("repo_id")
        repos_file = DASH_DIR / "repositories.json"
        new_val = None
        if repos_file.exists():
            try:
                with open(repos_file, "r", encoding="utf-8") as f:
                    rdata = json.load(f)
                for r in rdata.get("repositories", []):
                    if r.get("id") == repo_id or r.get("name") == repo_id:
                        curr = r.get("auto_approve_plans", False)
                        r["auto_approve_plans"] = not curr
                        new_val = r["auto_approve_plans"]
                        # When enabled, promote all pending/todo/backlog AI milestones to Ready
                        if new_val:
                            for m in r.get("milestones", []):
                                if m.get("status") in ("todo", "backlog", "pending"):
                                    m["approved_for_auto_run"] = True
                                    m["status"] = "ready"
                                    m["github_column"] = "Ready"
                        else:
                            # When disabled, require human approval for all unexecuted AI development plans
                            for m in r.get("milestones", []):
                                if m.get("status") in ("todo", "backlog", "pending", "ready") and not m.get("is_recovery", False):
                                    m["approved_for_auto_run"] = False
                                    m["status"] = "todo"
                                    m["github_column"] = "Backlog"
                        break
                if new_val is not None:
                    with open(repos_file, "w", encoding="utf-8") as f:
                        json.dump(rdata, f, indent=2, ensure_ascii=False)
            except Exception as e:
                print(f"[-] Error toggling repo auto-approve: {e}")

        # Trigger board re-render & note sync
        try:
            subprocess.run(["python3", str(DASH_DIR / "board_engine.py")], capture_output=True, timeout=5)
            subprocess.run(["python3", str(DASH_DIR / "note_converter.py")], capture_output=True, timeout=5)
        except Exception:
            pass

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "success", "auto_approve_plans": new_val}).encode("utf-8"))

    def handle_toggle_all_auto_approve(self, payload):
        repos_file = DASH_DIR / "repositories.json"
        new_val = None
        if repos_file.exists():
            try:
                with open(repos_file, "r", encoding="utf-8") as f:
                    rdata = json.load(f)
                repos = rdata.get("repositories", [])
                any_off = any(not r.get("auto_approve_plans", False) for r in repos)
                new_val = any_off  # If any is OFF, turn all ON. If all ON, turn all OFF.
                for r in repos:
                    r["auto_approve_plans"] = new_val
                    if new_val:
                        for m in r.get("milestones", []):
                            if m.get("status") in ("todo", "backlog", "pending"):
                                m["approved_for_auto_run"] = True
                                m["status"] = "ready"
                                m["github_column"] = "Ready"
                    else:
                        for m in r.get("milestones", []):
                            if m.get("status") in ("todo", "backlog", "pending", "ready") and not m.get("is_recovery", False):
                                m["approved_for_auto_run"] = False
                                m["status"] = "todo"
                                m["github_column"] = "Backlog"
                with open(repos_file, "w", encoding="utf-8") as f:
                    json.dump(rdata, f, indent=2, ensure_ascii=False)
            except Exception as e:
                print(f"[-] Error toggling all auto-approve: {e}")

        try:
            subprocess.run(["python3", str(DASH_DIR / "board_engine.py")], capture_output=True, timeout=5)
            subprocess.run(["python3", str(DASH_DIR / "note_converter.py")], capture_output=True, timeout=5)
        except Exception:
            pass

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "success", "auto_approve_plans": new_val}).encode("utf-8"))

    def handle_get_models(self):
        try:
            from dynamic_model_provider import fetch_live_models
            data = fetch_live_models()
        except Exception as e:
            data = {"status": "error", "error": str(e), "models": []}

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache, no-store")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))

    def handle_update_repo_models(self, payload):
        repo_id = payload.get("repo_id")
        planner_model = payload.get("planner_model")
        executor_model = payload.get("executor_model")
        repos_file = DASH_DIR / "repositories.json"
        saved = False

        if repos_file.exists() and repo_id:
            try:
                with open(repos_file, "r", encoding="utf-8") as f:
                    rdata = json.load(f)
                for r in rdata.get("repositories", []):
                    if r.get("id") == repo_id or r.get("name") == repo_id:
                        if planner_model is not None:
                            r["planner_model"] = planner_model if planner_model != "auto" else None
                        if executor_model is not None:
                            r["executor_model"] = executor_model if executor_model != "auto" else None
                        saved = True
                        break
                if saved:
                    with open(repos_file, "w", encoding="utf-8") as f:
                        json.dump(rdata, f, indent=2, ensure_ascii=False)
                        f.write("\n")
            except Exception as e:
                print(f"[-] Error updating repo models: {e}")

        # Re-render boards
        try:
            subprocess.run(["python3", str(DASH_DIR / "board_engine.py")], capture_output=True, timeout=5)
            subprocess.run(["python3", str(DASH_DIR / "note_converter.py")], capture_output=True, timeout=5)
        except Exception:
            pass

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({
            "status": "success" if saved else "not_found",
            "repo_id": repo_id,
            "planner_model": planner_model,
            "executor_model": executor_model
        }).encode("utf-8"))

    def handle_update_repo_cap(self, payload):
        repo_id = payload.get("repo_id")
        cap = int(payload.get("milestone_cap", 3))
        cap = max(1, min(10, cap))
        repos_file = DASH_DIR / "repositories.json"
        updated = False
        if repos_file.exists():
            try:
                with open(repos_file, "r", encoding="utf-8") as f:
                    rdata = json.load(f)
                for r in rdata.get("repositories", []):
                    if r.get("id") == repo_id or r.get("name") == repo_id:
                        r["milestone_cap"] = cap
                        updated = True
                        break
                if updated:
                    with open(repos_file, "w", encoding="utf-8") as f:
                        json.dump(rdata, f, indent=2, ensure_ascii=False)
            except Exception as e:
                print(f"[-] Error updating repo milestone_cap: {e}")

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "success" if updated else "not_found", "milestone_cap": cap}).encode("utf-8"))

    def handle_github_sync(self):
        pull_res = {"status": "skipped"}
        push_res = {"status": "skipped"}
        if gh_sync:
            try:
                pull_res = gh_sync.sync_github_to_cockpit()
                push_res = gh_sync.push_milestones_to_github()
            except Exception as e:
                pull_res = {"error": str(e)}

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({"pull": pull_res, "push": push_res}).encode("utf-8"))

    def handle_steer_planner(self, payload):
        repo_id = payload.get("repo_id", "phd_proposal")
        intent = payload.get("steering_intent", "").strip()
        limit = int(payload.get("limit", 3))
        limit = max(1, min(10, limit))

        # Persist the specified cap so future autonomous replenishment uses this exact limit
        repos_file = DASH_DIR / "repositories.json"
        if repos_file.exists():
            try:
                with open(repos_file, "r", encoding="utf-8") as f:
                    rdata = json.load(f)
                for r in rdata.get("repositories", []):
                    if r.get("id") == repo_id or r.get("name") == repo_id:
                        r["milestone_cap"] = limit
                        break
                with open(repos_file, "w", encoding="utf-8") as f:
                    json.dump(rdata, f, indent=2, ensure_ascii=False)
            except Exception as e:
                print(f"[-] Error updating repo cap in steer: {e}")

        new_items = []
        err = None
        if synthesize_steered_milestones and intent:
            try:
                new_items = synthesize_steered_milestones(repo_id, intent, limit=limit)
            except Exception as e:
                err = str(e)
        else:
            err = "Missing intent or planner unavailable"

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({
            "status": "success" if new_items else "error",
            "error": err,
            "created_count": len(new_items),
            "milestones": new_items
        }, ensure_ascii=False).encode("utf-8"))

    def handle_status(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache, no-store")
        self.end_headers()

        data = get_cached_system_snapshot(max_age_s=2.5)
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))

    def handle_toggle_milestone(self, payload):
        repo_id = payload.get("repo_id")
        m_name = payload.get("milestone_name")
        new_status = payload.get("new_status", "completed")

        repos_file = DASH_DIR / "repositories.json"
        if repos_file.exists():
            try:
                with open(repos_file, "r") as f:
                    rdata = json.load(f)
                for r in rdata.get("repositories", []):
                    if r.get("id") == repo_id or r.get("name") == repo_id:
                        for m in r.get("milestones", []):
                            if m.get("name") == m_name:
                                m["status"] = new_status
                                m["github_column"] = "Done" if new_status == "completed" else "Ready"
                                if new_status == "completed" and (not m.get("skills") or len(m.get("skills")) < 3):
                                    m["skills"] = infer_task_skills(m, r.get("id"), r.get("category", ""))
                with open(repos_file, "w") as f:
                    json.dump(rdata, f, indent=2, ensure_ascii=False)
            except Exception:
                pass

        # Sync toggle to GitHub Projects v2
        if gh_sync:
            try:
                target_gh_col = "Done" if new_status == "completed" else "Ready"
                gh_sync.set_milestone_github_status(m_name, target_gh_col)
            except Exception as e:
                print(f"[-] Error syncing toggle to GitHub: {e}")

        # Trigger board re-render
        try:
            subprocess.run(["python3", str(DASH_DIR / "note_converter.py")], capture_output=True, timeout=5)
        except Exception:
            pass

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status": "ok"}')

    def handle_add_milestone(self, payload):
        repo_id = payload.get("repo_id", "phd_proposal")
        name = payload.get("name", "").strip()
        details = payload.get("details", "").strip()

        if name:
            repos_file = DASH_DIR / "repositories.json"
            if repos_file.exists():
                try:
                    with open(repos_file, "r") as f:
                        rdata = json.load(f)
                    for r in rdata.get("repositories", []):
                        if r.get("id") == repo_id:
                            new_item = {
                                "name": name,
                                "status": "pending",
                                "details": details,
                                "added_by": "User (Control Center)"
                            }
                            new_item["skills"] = infer_task_skills(new_item, repo_id, r.get("category", ""))
                            r.setdefault("milestones", []).append(new_item)
                    with open(repos_file, "w") as f:
                        json.dump(rdata, f, indent=2, ensure_ascii=False)
                except Exception:
                    pass

            # Trigger board re-render
            try:
                subprocess.run(["python3", str(DASH_DIR / "note_converter.py")], capture_output=True, timeout=5)
            except Exception:
                pass

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status": "ok"}')

    def handle_trigger_orchestrator(self):
        subprocess.run(["systemctl", "--user", "restart", "autonomous-orchestrator.service"])
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status": "triggered"}')

    def handle_toggle_auto_loop(self):
        res = subprocess.run(["systemctl", "--user", "is-active", "autonomous-orchestrator.service"], capture_output=True, text=True)
        is_active = res.stdout.strip() == "active"
        if is_active:
            subprocess.run(["systemctl", "--user", "stop", "autonomous-orchestrator.service"])
            running = False
        else:
            subprocess.run(["systemctl", "--user", "start", "autonomous-orchestrator.service"])
            running = True

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "ok", "running": running}).encode("utf-8"))

def get_recent_phoenix_traces(limit: int = 20) -> list:
    db_path = Path.home() / ".phoenix" / "phoenix.db"
    if not db_path.exists():
        return []
    traces = []
    try:
        con = sqlite3.connect(str(db_path), timeout=3)
        cur = con.cursor()
        cur.execute("""
            SELECT span_id, name, span_kind, start_time, end_time, status_code, status_message, attributes 
            FROM spans 
            ORDER BY start_time DESC 
            LIMIT ?
        """, (limit,))
        for span_id, name, kind, st, et, code, msg, attrs in cur.fetchall():
            meta = {}
            if attrs:
                try:
                    meta = json.loads(attrs)
                except Exception:
                    pass
            model = meta.get("llm", {}).get("model_name", "")
            role = meta.get("agent.role", "")
            repo = meta.get("repository.name", "")
            in_val = meta.get("input", {}).get("value", "")
            out_val = meta.get("output", {}).get("value", "")
            tokens = meta.get("llm", {}).get("token_count", {})
            latency = meta.get("latency.seconds", None)
            
            friendly_name = name
            if not role and ":" in name:
                role = name.split(":")[0].strip()
            if not model and ":" in name:
                model = name.split(":", 1)[1].strip()

            time_str = st
            if " " in str(time_str):
                time_str = str(time_str).split(" ")[1].split(".")[0]

            traces.append({
                "span_id": span_id,
                "name": friendly_name,
                "role": role or "Agent",
                "model": model or "LLM",
                "repo": repo or "General",
                "status": "ERROR" if (code == "ERROR" or msg) else "OK",
                "error_message": msg or "",
                "start_time": st,
                "time_display": time_str,
                "input_snippet": in_val[:600] if in_val else "",
                "output_snippet": out_val[:800] if out_val else "",
                "tokens": {
                    "prompt": tokens.get("prompt", 0),
                    "completion": tokens.get("completion", 0),
                    "total": tokens.get("total", 0)
                },
                "latency_s": latency
            })
        con.close()
    except Exception:
        pass
    return traces

def get_accounts_status():
    db_path = Path.home() / ".antigravity-agent" / "cloud_accounts.db"
    accounts = []
    if db_path.exists():
        try:
            con = sqlite3.connect(str(db_path))
            cur = con.cursor()
            cur.execute("SELECT email, status, quota_json, health_json FROM accounts")
            for email, status, quota_json, health_json in cur.fetchall():
                gemini_weekly = 100
                gemini_5h = 100
                claude_weekly = 0
                claude_opus = 0
                gemini_flash = 0
                if quota_json:
                    try:
                        qdata = json.loads(quota_json)
                        models = qdata.get("models", {})
                        claude_opus = models.get("claude-opus-4-6-thinking", {}).get("percentage", 0)
                        gemini_flash = models.get("gemini-3.8-flash-tiered", {}).get("percentage", 0)
                        for group in qdata.get("quota_groups", []):
                            for b in group.get("buckets", []):
                                if "gemini" in b.get("bucket_id", "").lower():
                                    if b.get("window") == "5h":
                                        gemini_5h = round(b.get("remaining_fraction", 1.0) * 100)
                                    elif b.get("window") == "weekly":
                                        gemini_weekly = round(b.get("remaining_fraction", 1.0) * 100)
                                elif "3p" in b.get("bucket_id", "").lower():
                                    if b.get("window") == "weekly":
                                        claude_weekly = round(b.get("remaining_fraction", 0.0) * 100)
                    except Exception:
                        pass

                health_warning = None
                if health_json:
                    try:
                        h = json.loads(health_json)
                        if h.get("validation", {}).get("status") == "requires_action":
                            health_warning = "Action Required: Google Web Verification"
                    except Exception:
                        pass

                accounts.append({
                    "email": email,
                    "status": status,
                    "gemini_5h_percent": gemini_5h,
                    "gemini_weekly_percent": gemini_weekly,
                    "claude_weekly_percent": claude_weekly,
                    "claude_opus_percent": claude_opus,
                    "gemini_flash_percent": gemini_flash,
                    "health_warning": health_warning
                })
            con.close()
        except Exception:
            pass
    return accounts

_ROUTER_HEALTH_CACHE = None
_ROUTER_HEALTH_TS = 0
_ROUTER_HEALTH_LOCK = threading.Lock()

def check_router_health(max_age_s: float = 20.0) -> dict:
    """Probes local router on port 8045/7836 with a 20-second in-memory cache to prevent socket hammering."""
    global _ROUTER_HEALTH_CACHE, _ROUTER_HEALTH_TS
    now = time.time()
    if _ROUTER_HEALTH_CACHE is not None and (now - _ROUTER_HEALTH_TS) < max_age_s:
        return _ROUTER_HEALTH_CACHE

    with _ROUTER_HEALTH_LOCK:
        if _ROUTER_HEALTH_CACHE is not None and (now - _ROUTER_HEALTH_TS) < max_age_s:
            return _ROUTER_HEALTH_CACHE

        env_file = Path("/home/ajf/.hermes/.env")
        keys = []
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if "=" in line and any(k in line for k in ["KEY", "TOKEN"]):
                    val = line.split("=", 1)[1].strip().strip("\"'")
                    if val and val not in keys:
                        keys.append(val)

        for port in [8045, 7836]:
            for key in keys + [""]:
                req = urllib.request.Request(f"http://127.0.0.1:{port}/v1/models")
                if key:
                    req.add_header("Authorization", f"Bearer {key}")
                try:
                    with urllib.request.urlopen(req, timeout=1.5) as r:
                        data = json.loads(r.read().decode())
                        models = data.get("data", [])
                        res = {
                            "status": "online",
                            "port": port,
                            "models_count": len(models),
                            "checked_at": datetime.now().strftime("%H:%M:%S")
                        }
                        _ROUTER_HEALTH_CACHE = res
                        _ROUTER_HEALTH_TS = time.time()
                        return res
                except Exception:
                    pass

        res = {
            "status": "offline",
            "port": 8045,
            "models_count": 0,
            "checked_at": datetime.now().strftime("%H:%M:%S")
        }
        _ROUTER_HEALTH_CACHE = res
        _ROUTER_HEALTH_TS = time.time()
        return res

_SNAPSHOT_CACHE = None
_SNAPSHOT_CACHE_TS = 0
_SNAPSHOT_LOCK = threading.Lock()

def invalidate_snapshot_cache():
    global _SNAPSHOT_CACHE, _SNAPSHOT_CACHE_TS
    with _SNAPSHOT_LOCK:
        _SNAPSHOT_CACHE = None
        _SNAPSHOT_CACHE_TS = 0

def get_cached_system_snapshot(max_age_s: float = 2.5) -> dict:
    global _SNAPSHOT_CACHE, _SNAPSHOT_CACHE_TS
    now = time.time()
    if _SNAPSHOT_CACHE is not None and (now - _SNAPSHOT_CACHE_TS) < max_age_s:
        return _SNAPSHOT_CACHE
    with _SNAPSHOT_LOCK:
        if _SNAPSHOT_CACHE is not None and (now - _SNAPSHOT_CACHE_TS) < max_age_s:
            return _SNAPSHOT_CACHE
        _SNAPSHOT_CACHE = get_system_snapshot()
        _SNAPSHOT_CACHE_TS = time.time()
        return _SNAPSHOT_CACHE

def compute_concurrency_diagnostics(repos, telemetry_data, governor_state, accounts):
    requested_max = 3
    safe_concurrent = governor_state.get("safe_concurrency", 2)
    gov_tier = governor_state.get("tier", "BALANCED_OPTIMAL")
    
    active_agents = telemetry_data.get("active_agents", {})
    active_count = len(active_agents)
    
    executable_count = 0
    seen_targets = set()
    file_collisions = []
    blocked_by_dependencies = 0
    
    for r in repos:
        completed_in_repo = {
            m.get("name", "").strip().lower()
            for m in r.get("milestones", [])
            if m.get("status") == "completed"
        }
        for m in r.get("milestones", []):
            st = m.get("status", "pending")
            if st in ("completed", "blocked"):
                continue
            is_approved = bool(m.get("approved_for_auto_run")) or bool(r.get("auto_approve_plans"))
            if not is_approved and not m.get("is_recovery"):
                continue
            
            # Check DAG dependencies
            deps = m.get("depends_on", []) or m.get("prerequisites", [])
            unmet = [d for d in deps if str(d).strip().lower() not in completed_in_repo]
            if unmet:
                blocked_by_dependencies += 1
                continue
                
            executable_count += 1
            target_f = m.get("relative_filepath", m.get("name"))
            if target_f in seen_targets:
                file_collisions.append(target_f)
            else:
                seen_targets.add(target_f)

    healthy_accounts = sum(1 for a in accounts if a.get("gemini_5h_percent", 0) > 0 and not a.get("health_warning"))
    
    bottleneck_reason = ""
    if healthy_accounts == 0:
        bottleneck_reason = "No healthy accounts with remaining quota in pool."
    elif safe_concurrent < requested_max:
        bottleneck_reason = f"Adaptive Rate Governor limited concurrency to {safe_concurrent} worker(s) (Policy: {gov_tier}) to prevent burst 429 rate limits."
    elif executable_count < safe_concurrent:
        bottleneck_reason = f"Only {executable_count} executable milestone(s) currently ready across active repositories."
    elif file_collisions:
        bottleneck_reason = f"File deconfliction active: multiple ready tasks target '{file_collisions[0]}'. Running sequentially to prevent Git merge collision."
    elif active_count >= safe_concurrent:
        bottleneck_reason = f"All {safe_concurrent} allocated worker slots are actively executing."
    else:
        bottleneck_reason = f"Stagger interval pacing ({governor_state.get('stagger_delay_sec', 0.95)}s delay between workers)."

    return {
        "requested_max_concurrency": requested_max,
        "safe_concurrency_allowed": safe_concurrent,
        "governor_tier": gov_tier,
        "active_workers_count": active_count,
        "executable_tasks_count": executable_count,
        "blocked_by_dependencies_count": blocked_by_dependencies,
        "file_collisions": file_collisions,
        "bottleneck_reason": bottleneck_reason
    }

def get_system_snapshot():
    # Auto clean ghost / abandoned agents
    try:
        from telemetry import clean_stale_agents
        clean_stale_agents(max_age_seconds=600)
    except Exception:
        pass

    # 1. Telemetry & Tokens
    telemetry = load_telemetry()
    
    # 2. Repositories
    repos = []
    repos_file = DASH_DIR / "repositories.json"
    if not repos_file.exists():
        example_file = DASH_DIR / "repositories.example.json"
        if example_file.exists():
            import shutil
            try:
                shutil.copyfile(example_file, repos_file)
            except Exception:
                pass

    if repos_file.exists():
        try:
            with open(repos_file, "r") as f:
                repos = json.load(f).get("repositories", [])
            # Self-healing reconciler for ghost in-progress tasks
            try:
                from autonomous_orchestrator import reconcile_ghost_in_progress_tasks
                reconcile_ghost_in_progress_tasks(repos)
            except Exception:
                pass
        except Exception:
            repos = []

    # 3. Accounts
    accounts = get_accounts_status()

    # 4. Human action blockers
    blockers = []
    action_file = DASH_DIR / "human_action_items.json"
    if action_file.exists():
        try:
            with open(action_file, "r") as f:
                blockers = json.load(f).get("active_blockers", [])
        except Exception:
            blockers = []

    # 5. LinkedIn Queue
    linkedin_text = ""
    linkedin_file = BASE_DIR / "LINKEDIN_QUEUE.md"
    if linkedin_file.exists():
        linkedin_text = linkedin_file.read_text(encoding="utf-8")

    # 6. Phoenix Live Agent Traces & Diagnostics
    phoenix_traces = get_recent_phoenix_traces(limit=20)

    # 7. Token Quota & Readiness Optimizer Engine
    quota_optimizer = {}
    try:
        from token_quota_optimizer import get_current_optimizer_state
        quota_optimizer = get_current_optimizer_state()
    except Exception as e:
        quota_optimizer = {"status": "UNKNOWN", "can_run": True, "error": str(e)}

    # 8. Adaptive Rate & Token Flow Governor
    rate_governor = {}
    governor_file = DASH_DIR / "rate_governor_state.json"
    if governor_file.exists():
        try:
            rate_governor = json.loads(governor_file.read_text(encoding="utf-8"))
        except Exception:
            pass

    # 9. Background Orchestrator Process Status
    orch_pid = is_orchestrator_running()

    # 10. Live Router Probe
    router_info = check_router_health()

    # 11. Failed Tasks Registry & Bugfix Queue
    failed_tasks = {"total_recorded_failures": 0, "pending_bugfix_tasks": 0, "failures": []}
    failed_tasks_file = DASH_DIR / "failed_tasks_registry.json"
    if failed_tasks_file.exists():
        try:
            failed_tasks = json.loads(failed_tasks_file.read_text(encoding="utf-8"))
        except Exception:
            pass

    # 12. Fleet Health Sentinel (Real Hermes Smoke Test & Token Readiness)
    fleet_health = {}
    try:
        from fleet_health_sentinel import get_fleet_health_snapshot
        fleet_health = get_fleet_health_snapshot(run_smoke_test=False)
    except Exception:
        pass

    # 13. Concurrency & Slot Allocation Diagnostics
    concurrency_diag = compute_concurrency_diagnostics(repos, telemetry, rate_governor, accounts)

    # 14. 10-Minute Platform Supervisor Audit Report
    supervisor_report = {}
    report_file = DASH_DIR / "SUPERVISOR_AUDIT_REPORT.json"
    if report_file.exists():
        try:
            supervisor_report = json.loads(report_file.read_text(encoding="utf-8"))
        except Exception:
            pass

    # 15. Predictive Token Capacity & Concurrency Forecast
    capacity_fc = {}
    try:
        from capacity_forecaster import get_fleet_capacity_forecast
        capacity_fc = get_fleet_capacity_forecast()
    except Exception:
        pass

    return {
        "telemetry": telemetry,
        "repositories": repos,
        "accounts": accounts,
        "blockers": blockers,
        "linkedin_queue": linkedin_text,
        "phoenix_traces": phoenix_traces,
        "phoenix_url": "http://127.0.0.1:6006",
        "router_port": router_info.get("port", 8045),
        "router_status": router_info.get("status", "offline"),
        "router_models_count": router_info.get("models_count", 0),
        "router_checked_at": router_info.get("checked_at", ""),
        "fleet_health": fleet_health,
        "quota_optimizer": quota_optimizer,
        "rate_governor": rate_governor,
        "failed_tasks": failed_tasks,
        "concurrency_diagnostics": concurrency_diag,
        "supervisor_report": supervisor_report,
        "capacity_forecast": capacity_fc,
        "orchestrator_running": bool(orch_pid),
        "orchestrator_pid": orch_pid,
        "github_bridge": {
            "status": "connected",
            "project_title": "Autonomous Multi-Repo Roadmap",
            "owner": "Ali-J-Fesharaki",
            "project_number": 1
        }
    }

def run_server():
    server = ThreadingHTTPServer(("127.0.0.1", PORT), ControlCenterHandler)
    print(f"[*] Hermes Control Center & Agent Monitor running on http://127.0.0.1:{PORT}")
    server.serve_forever()

if __name__ == "__main__":
    run_server()
