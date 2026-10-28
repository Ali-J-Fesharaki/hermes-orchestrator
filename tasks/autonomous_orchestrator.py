#!/usr/bin/env python3
"""
autonomous_orchestrator.py: Universal Multi-Repository Autonomous Orchestrator.

Architecture:
  1. Strategic Planner Daemon (Claude Opus 4.6 Thinking):
     - Roves across all registered repositories in dashboards/repositories.json.
     - Inspects Git state, recent commits, and goal completion status.
     - When a repository finishes its current milestones or encounters bottlenecks,
       Opus dynamically synthesizes the next concrete technical milestones.
  2. Prioritized Execution Engine (Gemini 3.8 Flash Tiered):
     - Dispatches execution agents in strict priority order (Priority 1, 2, 3...).
     - Implements concrete code, LaTeX chapters, algorithms, and tests.
     - Validates, compiles, and commits atomically to Git (one commit per milestone).
     - Live-syncs Control Tower dashboards, telemetry, and desktop sticky notes.
"""

import os
import sys
import time
import json
import re
import shutil
import tempfile
import sqlite3
import argparse
import subprocess
import threading
import concurrent.futures
import urllib.request
import urllib.error
import contextlib
import random
try:
    import fcntl
except ImportError:
    fcntl = None
from datetime import datetime
from pathlib import Path
from typing import Any, Union, Tuple, List, Dict, Optional

# Base Paths
AUTO_DIR = Path(__file__).resolve().parent.parent
DASHBOARDS_DIR = AUTO_DIR / "dashboards"
REPOS_FILE = DASHBOARDS_DIR / "repositories.json"
REPOS_LOCK_FILE = DASHBOARDS_DIR / ".repositories.json.lock"
ROUTER_URL = os.environ.get("ROUTER_URL", "http://127.0.0.1:8045/v1/chat/completions")
ENV_FILE = Path.home() / ".hermes/.env"
NOTIFIER = AUTO_DIR / "telegram_notifier.py"
BOARD_ENGINE = DASHBOARDS_DIR / "board_engine.py"
NOTE_CONVERTER = DASHBOARDS_DIR / "note_converter.py"
FIX_SCRIPT = Path.home() / "Latex_Persian_Tools/fix_latex_persian.py"
SELF_HEAL_SCRIPT = Path.home() / ".local/bin/check-antigravity-hermes.sh"
HERMES_BIN = shutil.which("hermes") or str(Path.home() / ".local/bin/hermes")

# Ensure automation paths are always available
for p in [str(AUTO_DIR), str(DASHBOARDS_DIR), str(AUTO_DIR / "tasks")]:
    if p not in sys.path:
        sys.path.insert(0, p)

# Import Telemetry & Integrations
import telemetry
try:
    from cadence_manager import get_cadence_manager
except ImportError:
    from tasks.cadence_manager import get_cadence_manager

# Arize Phoenix Tracing
try:
    from phoenix_tracer import init_phoenix_tracing, record_llm_span, record_error_span
    init_phoenix_tracing(project_name="default")
except Exception as e:
    print(f"[-] Phoenix tracing initialization note: {e}")
    def record_llm_span(*args, **kwargs):
        pass
    def record_error_span(*args, **kwargs):
        pass

# GitHub Projects Two-Way Bridge
try:
    import github_project_sync as gh_sync
except ImportError:
    sys.path.insert(0, str(AUTO_DIR / "tasks"))
    import github_project_sync as gh_sync

# Dynamic Strategic Planning Model Ranker (Live OpenRouter Benchmarks)
try:
    from dynamic_planner_ranker import get_dynamic_planning_cascade
except ImportError:
    sys.path.insert(0, str(AUTO_DIR / "tasks"))
    from dynamic_planner_ranker import get_dynamic_planning_cascade

# Persistent Human Architect Steering & Card Refinement History
try:
    from architect_steering import get_steering_context_for_repo
except ImportError:
    sys.path.insert(0, str(AUTO_DIR / "tasks"))
    from architect_steering import get_steering_context_for_repo

# Token Quota & Readiness Optimizer Engine
try:
    from token_quota_optimizer import evaluate_quota_readiness, STATE_FILE as QUOTA_STATE_FILE
except Exception:
    sys.path.insert(0, str(AUTO_DIR / "tasks"))
    try:
        from token_quota_optimizer import evaluate_quota_readiness, STATE_FILE as QUOTA_STATE_FILE
    except Exception:
        def evaluate_quota_readiness(*args, **kwargs):
            return {"can_run": True, "status": "READY"}
        QUOTA_STATE_FILE = None

# Adaptive Rate, 429 Prevention & Flow Control Governor
try:
    from adaptive_rate_governor import get_governor
    _GOVERNOR = get_governor()
except Exception:
    sys.path.insert(0, str(AUTO_DIR / "tasks"))
    try:
        from adaptive_rate_governor import get_governor
        _GOVERNOR = get_governor()
    except Exception:
        class _DummyGovernor:
            active_429_cooldown_until = 0.0
            def compute_safe_concurrency(self, requested_max=3, **kwargs): return min(3, requested_max)
            def compute_policy(self, **kwargs): return {"tier": "EXPEDITE_HIGH", "max_tokens": 6000, "directive": ""}
            def acquire_dispatch_slot(self, idx=0): time.sleep(idx * 1.5)
            def record_429_incident(self, *args, **kwargs): pass
            def record_success(self, *args, **kwargs): pass
        _GOVERNOR = _DummyGovernor()

# Failed Task Registry & Bugfix Queue
try:
    import failed_task_registry as ftr_mod
except ImportError:
    sys.path.insert(0, str(AUTO_DIR / "tasks"))
    import failed_task_registry as ftr_mod

PLANNER_MODEL_PRIMARY = "dynamic-openrouter-cascade"
PLANNER_MODEL_FALLBACK = "gemini-3.8-flash-tiered"
EXECUTOR_MODEL = "gemini-3.8-flash-tiered"

def parse_json_milestones(text: str) -> list:
    """Parses milestone array from LLM output with tolerant json_repair and deep extraction."""
    clean = text.strip()
    data = None
    try:
        import json_repair
        data = json_repair.loads(clean)
    except Exception:
        pass

    if data is None:
        for fence in ["```json", "```"]:
            if fence in clean:
                clean = clean.split(fence, 1)[1].split("```", 1)[0].strip()
                break
        try:
            data = json.loads(clean)
        except Exception:
            return []

    def extract_milestone_dicts(obj) -> list:
        results = []
        if isinstance(obj, dict):
            if "name" in obj:
                results.append(obj)
            elif "milestones" in obj and isinstance(obj["milestones"], list):
                results.extend(extract_milestone_dicts(obj["milestones"]))
        elif isinstance(obj, list):
            for item in obj:
                results.extend(extract_milestone_dicts(item))
        return results

    return extract_milestone_dicts(data)

def invoke_planner_cascade(
    system_prompt: str,
    user_prompt: str,
    repo_id: str,
    repo_name: str,
    temperature: float = 0.3,
    workdir: Path | None = None,
    preferred_model: str | None = None
) -> tuple[str, dict]:
    """
    Executes planning through independent Hermes Planner Agent sessions.
    If preferred_model is specified by the user, it is prioritized first.
    Picks the #1 ranked model live, and seamlessly cascades downward if quota expires.
    Returns (response_text, model_info).
    """
    cascade = get_dynamic_planning_cascade()
    if preferred_model and preferred_model != "auto":
        # Prepend user-chosen model to the top of the cascade
        pref_entry = {
            "model_id": preferred_model,
            "display_name": f"{preferred_model} (User Chosen)",
            "rank": 0,
            "color": "purple" if "opus" in preferred_model.lower() else ("blue" if "sonnet" in preferred_model.lower() else "emerald")
        }
        cascade = [pref_entry] + [m for m in cascade if m.get("model_id") != preferred_model]

    last_err = None
    for model_info in cascade:
        model_id = model_info["model_id"]
        model_label = model_info["display_name"]
        rank = model_info.get("rank", 1)
        try:
            print(f"[*] Dispatching Hermes Strategic Planner Agent via [Rank #{rank}] {model_label}...")
            resp_text, usage = run_hermes_agent(
                model=model_id,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                repo_id=repo_id,
                repo_name=repo_name,
                role=f"Hermes Strategic Planner ({model_label})",
                workdir=workdir,
                timeout_s=360
            )
            if resp_text and resp_text.strip():
                model_info["label"] = model_label
                model_info["session_id"] = usage.get("session_id")
                return resp_text, model_info
        except Exception as e:
            print(f"[-] Quota / error on [Rank #{rank}] {model_label}: {e}. Cascading to next rank...")
            last_err = e
            continue
    raise last_err or RuntimeError("All dynamically ranked planning models failed.")

def get_api_key() -> str:
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            if line.startswith("HERMES_CUSTOM_ANTIGRAVITY_MANAGER_API_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return os.environ.get("HERMES_CUSTOM_ANTIGRAVITY_MANAGER_API_KEY", "")

def notify(msg: str, force: bool = False):
    """Quiet Telegram notifications: only on milestone delivery or planner updates."""
    if force and NOTIFIER.exists():
        try:
            subprocess.run(["python3", str(NOTIFIER), msg], capture_output=True, timeout=10)
        except Exception:
            pass

def sync_dashboards():
    """Refreshes all 4 dashboards and sticky note summaries immediately."""
    if BOARD_ENGINE.exists():
        subprocess.run(["python3", str(BOARD_ENGINE)], capture_output=True)
    if NOTE_CONVERTER.exists():
        subprocess.run(["python3", str(NOTE_CONVERTER)], capture_output=True)

def run_hermes_agent(
    model: str,
    system_prompt: str,
    user_prompt: str,
    repo_id: str = "general",
    repo_name: str = "General",
    role: str = "Hermes Worker",
    workdir: Path | None = None,
    timeout_s: int = 1500,
    retries: int = 2
) -> tuple[str, dict]:
    """
    Spawns an authentic, independent Hermes Agent subprocess.
    Each execution creates a dedicated, tracked Hermes session in ~/.hermes/state.db,
    leverages Hermes skills and runtime, honors Adaptive Rate Governor pacing,
    and logs granular token telemetry and Arize Phoenix traces with the unique Hermes session_id.
    """
    full_prompt = (
        f"================================================================================\n"
        f"HERMES AUTONOMOUS MISSION DIRECTIVE ({role}):\n"
        f"{system_prompt.strip()}\n"
        f"================================================================================\n"
        f"TASK INSTRUCTIONS & SPECIFICATION:\n"
        f"{user_prompt.strip()}\n"
    )

    last_err = None
    target_dir = str(workdir or AUTO_DIR)

    for attempt in range(1, retries + 1):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tf:
            usage_file = tf.name

        cmd = [
            HERMES_BIN,
            "--in", target_dir,
            "--yolo",
            "--ignore-rules",
            "-m", model,
            "-z", full_prompt,
            "--usage-file", usage_file
        ]

        t0 = time.time()
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
            latency_s = time.time() - t0

            usage_data = {}
            if os.path.exists(usage_file):
                try:
                    with open(usage_file, "r", encoding="utf-8") as uf:
                        usage_data = json.load(uf)
                except Exception:
                    pass
                try:
                    os.unlink(usage_file)
                except Exception:
                    pass

            session_id = usage_data.get("session_id", "hermes_standalone")
            content = res.stdout.strip()
            err_text = res.stderr.strip()

            usage = {
                "prompt_tokens": usage_data.get("input_tokens", 0),
                "completion_tokens": usage_data.get("output_tokens", 0),
                "total_tokens": usage_data.get("total_tokens", 0),
                "reasoning_tokens": usage_data.get("reasoning_tokens", 0),
                "session_id": session_id
            }

            if res.returncode != 0:
                combined_err = f"{err_text} {content}"
                if "429" in combined_err or "resource_exhausted" in combined_err.lower() or "rate_limit" in combined_err.lower():
                    _GOVERNOR.record_429_incident(model, combined_err)
                    wait_time = max(5, int(_GOVERNOR.active_429_cooldown_until - time.time()))
                    print(f"[⏳ 429 RATE LIMIT (Hermes Agent)] Backing off {wait_time}s before attempt {attempt+1}...")
                    time.sleep(wait_time)
                    continue

                print(f"[-] Hermes Agent exit {res.returncode} on {model} [attempt {attempt}/{retries}]: {err_text[:200]}")
                last_err = RuntimeError(f"Hermes Agent error (code {res.returncode}): {err_text[:200]}")
                time.sleep(3 * attempt)
                continue

            # Record Granular Token Telemetry with Hermes Session ID
            telemetry.record_tokens(repo_id, repo_name, role, model, usage)

            # Report successful throughput to Rate Governor
            try:
                _GOVERNOR.record_success(tokens_used=usage["total_tokens"])
            except Exception:
                pass

            # Record to Arize Phoenix OpenTelemetry with Hermes Session ID
            try:
                record_llm_span(model, role, repo_name, user_prompt[:400], content, usage, latency_s, session_id=session_id)
            except Exception:
                pass

            print(f"[⚡ HERMES AGENT DELIVERED] Session: `{session_id}` | Tokens: {usage['total_tokens']:,} | Model: {model} | Latency: {latency_s:.1f}s")
            return content, usage

        except subprocess.TimeoutExpired:
            print(f"[-] Hermes Agent timed out after {timeout_s}s on {model} [attempt {attempt}/{retries}]")
            last_err = TimeoutError(f"Hermes Agent timed out after {timeout_s}s")
            time.sleep(5)
            continue
        except Exception as e:
            print(f"[-] Hermes Agent invocation error ({model}) [attempt {attempt}/{retries}]: {e}")
            last_err = e
            time.sleep(5)
            continue

    raise last_err or RuntimeError(f"Hermes Agent failed across {retries} attempts.")

def call_llm(model: str, messages: list, repo_id: str = "general", repo_name: str = "General",
             role: str = "Hermes Worker", temperature: float = 0.2, max_tokens: int = 4000, retries: int = 3,
             workdir: Path | None = None) -> str:
    """Invokes an independent Hermes Agent with fallback to direct router call."""
    system_prompt = ""
    user_prompt = ""
    for m in messages:
        if m.get("role") == "system":
            system_prompt += m.get("content", "") + "\n"
        else:
            user_prompt += m.get("content", "") + "\n"

    try:
        content, _ = run_hermes_agent(
            model=model,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            repo_id=repo_id,
            repo_name=repo_name,
            role=role,
            workdir=workdir,
            timeout_s=360,
            retries=retries
        )
        return content
    except Exception as e:
        print(f"[-] Hermes Agent runtime exception, falling back to direct router call: {e}")
        api_key = get_api_key()
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
        payload = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens
        }
        last_err = None
        for attempt in range(1, retries + 1):
            req = urllib.request.Request(ROUTER_URL, data=json.dumps(payload).encode("utf-8"), headers=headers)
            try:
                t0 = time.time()
                with urllib.request.urlopen(req, timeout=300) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                    latency_s = time.time() - t0
                    usage = data.get("usage", {})
                    telemetry.record_tokens(repo_id, repo_name, role, model, usage)
                    content = data["choices"][0]["message"]["content"]
                    try:
                        _GOVERNOR.record_success(tokens_used=usage.get("total_tokens", 0))
                    except Exception:
                        pass
                    try:
                        record_llm_span(model, role, repo_name, str(messages[-1].get("content", ""))[:400], content, usage, latency_s)
                    except Exception:
                        pass
                    return content
            except urllib.error.HTTPError as he:
                last_err = he
                err_msg = he.read().decode("utf-8") if he.fp else str(he)
                if he.code == 429 or "rate_limited" in err_msg.lower() or "RESOURCE_EXHAUSTED" in err_msg:
                    _GOVERNOR.record_429_incident(model, err_msg)
                    wait_time = max(5, int(_GOVERNOR.active_429_cooldown_until - time.time()))
                    time.sleep(wait_time)
                    continue
                time.sleep(2 * attempt)
            except Exception as ce:
                last_err = ce
                time.sleep(3 * attempt)
        raise last_err or RuntimeError(f"Direct fallback also failed for {model}")

_GIT_LOCK = threading.Lock()
_REPOS_GLOBAL_LOCK = threading.RLock()
_DISPATCH_TRIGGER = threading.Event()
_SHUTDOWN_EVENT = threading.Event()

@contextlib.contextmanager
def _repositories_file_lock(max_retries: int = 5, base_delay: float = 0.05, max_delay: float = 0.25):
    """
    Advisory file lock on .repositories.json.lock to prevent simultaneous interleaving writes
    across concurrent processes and threads.
    Retries up to max_retries with exponential backoff and randomized jitter.
    """
    if fcntl is None:
        yield
        return

    REPOS_LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    lock_fd = None
    acquired = False
    try:
        lock_fd = os.open(str(REPOS_LOCK_FILE), os.O_CREAT | os.O_RDWR, 0o666)
        for attempt in range(max_retries):
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
                break
            except (BlockingIOError, OSError):
                if attempt == max_retries - 1:
                    try:
                        fcntl.flock(lock_fd, fcntl.LOCK_EX)
                        acquired = True
                    except Exception as e:
                        print(f"[-] Warning: Advisory lock fallback failed for {REPOS_LOCK_FILE}: {e}")
                    break
                jitter = random.uniform(0.01, 0.05)
                backoff = min(max_delay, base_delay * (1.5 ** attempt)) + jitter
                time.sleep(backoff)
        yield
    finally:
        if lock_fd is not None:
            if acquired:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                except Exception:
                    pass
            try:
                os.close(lock_fd)
            except Exception:
                pass

def load_repositories() -> list:
    """Loads and returns all repositories sorted by priority (1, 2, 3...)."""
    with _REPOS_GLOBAL_LOCK:
        if not REPOS_FILE.exists():
            example_file = DASHBOARDS_DIR / "repositories.example.json"
            if example_file.exists():
                import shutil
                shutil.copyfile(example_file, REPOS_FILE)
            else:
                return []
        repos = []
        for attempt in range(3):
            try:
                with open(REPOS_FILE, "r", encoding="utf-8") as f:
                    repos = json.load(f).get("repositories", [])
                break
            except Exception:
                if attempt == 2:
                    return []
                time.sleep(0.05)
        repos.sort(key=lambda r: r.get("priority", 99))
        return repos

ARCHIVE_FILE = DASHBOARDS_DIR / "repositories_archive.json"

def archive_completed_milestones(repos: list, max_completed_keep: int = 5):
    """
    Safely archives completed milestones into repositories_archive.json
    so repositories.json stays lean (<100KB) and dashboards load instantly.
    """
    try:
        archive_data = {"repositories": {}, "last_archived_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        if ARCHIVE_FILE.exists():
            try:
                with open(ARCHIVE_FILE, "r", encoding="utf-8") as af:
                    archive_data = json.load(af)
            except Exception:
                pass
        
        arch_repos = archive_data.setdefault("repositories", {})
        
        for repo in repos:
            rid = repo.get("id")
            if not rid:
                continue
            ms = repo.get("milestones", [])
            completed = [m for m in ms if m.get("status") == "completed"]
            active = [m for m in ms if m.get("status") != "completed"]
            
            if len(completed) > max_completed_keep:
                keep = completed[-max_completed_keep:]
                to_archive = completed[:-max_completed_keep]
                
                arch_list = arch_repos.setdefault(rid, [])
                known_names = {m.get("name") for m in arch_list if m.get("name")}
                for m in to_archive:
                    if m.get("name") not in known_names:
                        arch_list.append(m)
                        known_names.add(m.get("name"))
                
                repo["milestones"] = active + keep
                repo["archived_count"] = len(arch_list)
            else:
                repo["archived_count"] = len(arch_repos.get(rid, []))
                
        temp_af = ARCHIVE_FILE.with_suffix(".tmp")
        with open(temp_af, "w", encoding="utf-8") as af:
            json.dump(archive_data, af, indent=2, ensure_ascii=False)
            af.write("\n")
        temp_af.replace(ARCHIVE_FILE)
    except Exception as e:
        print(f"[-] Warning: Failed to archive completed milestones: {e}")

def save_repositories(repos: list):
    """
    Atomically writes back the updated repositories registry preserving metadata,
    newly added repositories, and strictly RESPECTING human user configurations
    from disk (e.g. auto_planning, auto_approve_plans, milestone approvals).
    """
    with _REPOS_GLOBAL_LOCK:
        with _repositories_file_lock():
            repos_file = REPOS_FILE
            existing_data = {}
            if repos_file.exists():
                try:
                    with open(repos_file, "r", encoding="utf-8") as f:
                        existing_data = json.load(f)
                except Exception:
                    pass
                    
            # Filter out any repositories explicitly removed by the user in the Web UI
            removed_ids = set(existing_data.get("removed_repositories", []))
            repos[:] = [r for r in repos if r.get("id") not in removed_ids and r.get("name") not in removed_ids]

            existing_repos = existing_data.get("repositories", [])
            disk_repo_map = {r.get("id"): r for r in existing_repos if r.get("id")}
            in_memory_ids = {r.get("id"): r for r in repos if r.get("id")}

            # 1. Synchronize user-controlled configurations from disk into in-memory repos before saving
            for repo in repos:
                rid = repo.get("id")
                if rid in disk_repo_map:
                    dr = disk_repo_map[rid]
                    # Always respect user's manual toggles from disk:
                    repo["auto_planning"] = dr.get("auto_planning", repo.get("auto_planning", True))
                    repo["auto_approve_plans"] = dr.get("auto_approve_plans", repo.get("auto_approve_plans", False))
                    repo["planner_model"] = dr.get("planner_model", repo.get("planner_model"))
                    repo["executor_model"] = dr.get("executor_model", repo.get("executor_model"))
                    repo["milestone_cap"] = dr.get("milestone_cap", repo.get("milestone_cap", 3))
                    repo["priority"] = dr.get("priority", repo.get("priority", 99))
                    
                    # For milestones that are NOT actively executing in-progress, respect user approvals from disk
                    disk_milestones = {m.get("name"): m for m in dr.get("milestones", []) if m.get("name")}
                    for m in repo.get("milestones", []):
                        mname = m.get("name")
                        if mname in disk_milestones:
                            dm = disk_milestones[mname]
                            if dm.get("skills"):
                                m["skills"] = dm["skills"]
                            if m.get("status") not in ("in_progress", "completed"):
                                m["approved_for_auto_run"] = dm.get("approved_for_auto_run", False)
                                m["status"] = dm.get("status", m.get("status"))
                                m["github_column"] = dm.get("github_column", m.get("github_column"))

                    # Ensure all milestones have their top 3 authoritative skills assigned
                    try:
                        from tasks.skill_matcher import infer_task_skills
                        for m in repo.get("milestones", []):
                            curr_s = m.get("skills")
                            if not isinstance(curr_s, list) or len(curr_s) < 3:
                                m["skills"] = infer_task_skills(m, rid, repo.get("category", ""))
                    except Exception:
                        pass

            # 2. Merge newly added on-disk repositories that might have been added by user/API while orchestrator was running
            for er in existing_repos:
                er_id = er.get("id")
                if er_id and er_id not in in_memory_ids and er_id not in removed_ids:
                    repos.append(er)
                    in_memory_ids[er_id] = er
                    
            # Automatically archive older completed milestones to keep repositories.json lean and fast (<100KB)
            archive_completed_milestones(repos)
            existing_data["repositories"] = repos
            
            # Isolated Temporary File Protocol:
            # Construct unique, hidden temporary file in the same directory (ensuring atomic same-filesystem rename)
            repos_file.parent.mkdir(parents=True, exist_ok=True)
            tf = tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=repos_file.parent,
                prefix=f".{repos_file.name}.tmp_{os.getpid()}_{threading.get_ident()}_",
                suffix=".tmp",
                delete=False,
            )
            temp_path = Path(tf.name)
            try:
                json.dump(existing_data, tf, indent=2, ensure_ascii=False)
                tf.write("\n")
                tf.flush()
                os.fsync(tf.fileno())
                tf.close()

                # Retry guard around os.replace() catching OSError/FileNotFoundError with exponential backoff
                max_replace_retries = 5
                for rep_attempt in range(max_replace_retries):
                    try:
                        os.replace(temp_path, repos_file)
                        temp_path = None
                        break
                    except (OSError, FileNotFoundError) as e:
                        if rep_attempt == max_replace_retries - 1:
                            raise
                        time.sleep(0.05 * (2 ** rep_attempt) + random.uniform(0.01, 0.05))
            finally:
                if temp_path and temp_path.exists():
                    try:
                        temp_path.unlink()
                    except Exception:
                        pass

def get_repo_git_state(repo_path: Path) -> dict:
    if not repo_path.exists() or not (repo_path / ".git").exists():
        return {"branch": "unknown", "recent_commits": "none", "clean": True}
    
    branch = subprocess.run(["git", "branch", "--show-current"], cwd=repo_path, capture_output=True, text=True).stdout.strip()
    commits = subprocess.run(["git", "log", "-n", "3", "--oneline"], cwd=repo_path, capture_output=True, text=True).stdout.strip()
    status = subprocess.run(["git", "status", "--porcelain"], cwd=repo_path, capture_output=True, text=True).stdout.strip()
    return {
        "branch": branch,
        "recent_commits": commits,
        "clean": len(status) == 0
    }

def resolve_authoritative_goal_document(repo_path: Path, repo_entry: dict) -> Tuple[str, str]:
    """
    Resolves the authoritative roadmap/goal document for a repository.
    Default Priority Order:
    1. If a dedicated roadmap file exists (ROADMAP.md, roadmap.md, FUTURE_ROADMAP.md, 1_Roadmap_and_Progress.md),
       it ALWAYS takes precedence over generic README.md.
    2. Explicitly configured goal_file in repositories.json (e.g. DASHBOARD_STATE.md).
    3. Standard candidate filenames:
       - ROADMAP.md
       - roadmap.md
       - FUTURE_ROADMAP.md
       - 1_Roadmap_and_Progress.md
       - DASHBOARD_STATE.md
       - GOALS.md
       - README.md / readme.md
    Returns (goal_filename, goal_content).
    """
    configured = repo_entry.get("goal_file", "").strip()
    
    # Priority 1: Check standard primary roadmap files first
    roadmap_candidates = [
        "ROADMAP.md",
        "roadmap.md",
        "FUTURE_ROADMAP.md",
        "1_Roadmap_and_Progress.md",
        "DASHBOARD_STATE.md",
    ]
    
    # If explicitly configured to a dedicated non-README file, check it first
    if configured and configured.lower() not in ("readme.md", ""):
        cfg_path = repo_path / configured
        if cfg_path.exists() and cfg_path.is_file():
            try:
                return configured, cfg_path.read_text(encoding="utf-8")
            except Exception:
                pass

    # Check top roadmap candidate files in repository root
    for candidate in roadmap_candidates:
        cand_path = repo_path / candidate
        if cand_path.exists() and cand_path.is_file():
            try:
                return candidate, cand_path.read_text(encoding="utf-8")
            except Exception:
                pass

    # Fallback to configured or standard README.md
    for fb in [configured, "README.md", "readme.md", "GOALS.md"]:
        if not fb:
            continue
        fb_path = repo_path / fb
        if fb_path.exists() and fb_path.is_file():
            try:
                return fb, fb_path.read_text(encoding="utf-8")
            except Exception:
                pass
                
    return "ROADMAP.md", ""

# ==============================================================================
# 🧠 STRATEGIC PLANNER DAEMON (CLAUDE OPUS 4.6 THINKING)
# ==============================================================================
def run_planner_pass(repos: list) -> bool:
    """
    Roves across all repositories. If a repo has no pending milestones
    or its goals are accomplished, Opus 4.6 synthesizes new high-level milestones.
    """
    print("\n" + "=" * 75)
    print(" 🧠 STRATEGIC PLANNER AUDIT PASS (CLAUDE OPUS 4.6)")
    print("=" * 75)
    
    state_changed = False
    for repo in repos:
        repo_id = repo.get("id", "general")
        repo_name = repo.get("name", "Unknown")
        repo_path = Path(repo["path"])
        milestones = repo.get("milestones", [])
        # Continuous 3-Task Queue Buffer: Count unexecuted tasks waiting in queue (pending, todo, ready, backlog).
        # Any task claimed by an executor (moved to 'in_progress') or finished frees a slot in the queue,
        # so as soon as even ONE task leaves the 3-task queue, the planner synthesizes to refill it!
        queued_waiting = [m for m in milestones if m.get("status") in ("pending", "todo", "ready", "backlog")]
        pending = queued_waiting
        
        print(f"[*] Auditing Repo: {repo_name} (Priority {repo.get('priority', 99)})")
        print(f"    Total Milestones: {len(milestones)} | Queued Waiting Buffer: {len(queued_waiting)}")
        
        # Check if repo has autonomous planning enabled (dynamically re-check disk state)
        is_auto_planning = repo.get("auto_planning", True)
        if REPOS_FILE.exists():
            try:
                with open(REPOS_FILE, "r", encoding="utf-8") as f:
                    rdata = json.load(f)
                for r in rdata.get("repositories", []):
                    if r.get("id") == repo_id:
                        is_auto_planning = r.get("auto_planning", True)
                        repo["auto_planning"] = is_auto_planning
                        repo["auto_approve_plans"] = r.get("auto_approve_plans", False)
                        break
            except Exception:
                pass

        if not is_auto_planning:
            print(f"    [PAUSED] Autonomous planning disabled (OFF) for '{repo_name}'. Skipping.")
            continue

        target_cap = int(repo.get("milestone_cap", 3))
        
        if len(queued_waiting) >= target_cap:
            print(f"    [BUFFER FULL] '{repo_name}' has {len(queued_waiting)} waiting queued milestones (cap: {target_cap}).")
            continue

        missing = target_cap - len(queued_waiting)

        # Continuous Buffer Replenishment: As soon as queued_waiting < cap (even by 1 task!),
        # the Strategic Planner synthesizes the missing milestones to keep the pipeline buffer full!
        if missing > 0:
            agent_id = f"planner_opus_{repo_id}"
            task_desc = f"Replenish {missing} milestone(s) for {repo_name} (Buffer: {len(pending)}/{target_cap})"
            
            telemetry.register_agent(agent_id, repo_id, repo_name, "Strategic Planner", PLANNER_MODEL_PRIMARY, task_desc)
            sync_dashboards()
            
            try:
                git_state = get_repo_git_state(repo_path)
                goal_filename, goal_doc = resolve_authoritative_goal_document(repo_path, repo)
                    
                existing_pipeline_block = ""
                if pending:
                    existing_pipeline_block = (
                        f"\n--- EXISTING ACTIVE MILESTONES IN PIPELINE ({len(pending)} of {target_cap}) ---\n"
                        + "\n".join([f"• {m['name']} (Target: {m.get('relative_filepath', 'N/A')}): {m.get('details', '')}" for m in pending])
                        + f"\n\nCRITICAL BUFFER REPLENISHMENT DIRECTIVE:\n"
                        + f"The pipeline maintains a continuous buffer of {target_cap} tasks. Exactly {missing} slot(s) are currently open.\n"
                        + f"Synthesize exactly {missing} strictly NEW, subsequent milestone(s) that logically build upon the existing ones.\n"
                        + "DO NOT duplicate, overwrite, or repeat existing milestones.\n\n"
                    )

                system_prompt = (
                    "You are the Principal Strategic Research & Engineering Planner (Claude Opus 4.6 Thinking).\n"
                    "Your role is STRICTLY high-level architectural planning, goal prioritization, and milestone dispatching.\n"
                    "Base decisions SOLELY on the provided Repository Overview, Goal Dashboard, and existing active pipeline.\n"
                    f"The repository pipeline maintains a continuous active buffer of {target_cap} tasks.\n"
                    f"Formulate exactly {missing} concrete subsequent milestone(s) to refill the pipeline.\n"
                    "Return ONLY a clean JSON array of milestone objects, each with:\n"
                    "  'name': concise title,\n"
                    "  'status': 'pending',\n"
                    "  'details': detailed technical / mathematical description,\n"
                    "  'relative_filepath': target source/LaTeX file relative to repository root,\n"
                    "  'technical_specification': rigorous algorithmic formulation,\n"
                    "  'executor_prompt': detailed authoritative prompt for Gemini 3.8 to implement the artifact."
                )
                user_intent = repo.get("user_steering_intent", "").strip()
                intent_block = ""
                if user_intent:
                    intent_block = (
                        f"\n--- HUMAN ARCHITECT CURRENT INTENT (MAX PRIORITY) ---\n"
                        f"The human architect specifies the following target direction:\n"
                        f"\"{user_intent}\"\n"
                        f"Align all new milestones directly to advance this intent.\n\n"
                    )

                historical_steering = ""
                try:
                    historical_steering = get_steering_context_for_repo(repo_id)
                except Exception:
                    pass

                user_prompt = (
                    f"--- REPOSITORY OVERVIEW ---\n"
                    f"Repository: {repo_name}\n"
                    f"Category: {repo.get('category', 'general')}\n"
                    f"Active Branch: {git_state['branch']}\n"
                    f"Recent Commits:\n{git_state['recent_commits']}\n\n"
                    f"--- AUTHORITATIVE ROADMAP & GOAL SPECIFICATION ({goal_filename}) ---\n"
                    f"CRITICAL DIRECTIVE: Deriving your milestones directly from the phases and architecture outlined in this authoritative Roadmap is mandatory.\n\n"
                    f"{goal_doc[:12000]}\n\n"
                    f"{existing_pipeline_block}"
                    f"{historical_steering}\n"
                    f"{intent_block}"
                    f"Formulate exactly {missing} concrete milestone(s) to replenish the pipeline buffer."
                )
                
                chosen_planner = repo.get("planner_model")
                resp_text, planner_info = invoke_planner_cascade(
                    system_prompt, user_prompt, repo_id=repo_id, repo_name=repo_name, temperature=0.3, workdir=repo_path,
                    preferred_model=chosen_planner
                )
                planner_session_id = planner_info.get("session_id")
                if agent_id and planner_session_id:
                    telemetry.update_agent_session(agent_id, planner_session_id)
                    
                new_milestones = parse_json_milestones(resp_text)
                if new_milestones:
                    new_milestones = new_milestones[:missing]
                    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
                    auto_approve = bool(repo.get("auto_approve_plans", False))
                    for nm in new_milestones:
                        nm["status"] = "ready" if auto_approve else "todo"
                        nm["is_ai_development"] = True
                        nm["is_recovery"] = False
                        nm["approved_for_auto_run"] = auto_approve
                        nm["github_column"] = "Ready" if auto_approve else "Backlog"
                        nm["planned_by"] = planner_info["label"]
                        nm["planner_color"] = planner_info["color"]
                        nm["added_by"] = f"{planner_info['label']} (Buffer Replenish)" if pending else planner_info["label"]
                        nm["added_at"] = now_str
                        if planner_session_id:
                            nm["hermes_planner_session_id"] = planner_session_id
                    repo["milestones"].extend(new_milestones)
                    save_repositories(repos)
                    state_changed = True
                    print(f"✅ [{planner_info['label']}] Replenished {len(new_milestones)} new milestone(s) to maintain {target_cap}-task buffer in '{repo_name}'!")
                    notify(f"🎯 *Strategic Planner ({planner_info['label']})*\\nReplenished {len(new_milestones)} milestone(s) to maintain continuous {target_cap}-task buffer for *{repo_name}*.", force=True)
                    # Sync directly to GitHub Projects v2
                    try:
                        gh_sync.push_milestones_to_github()
                    except Exception as ge:
                        print(f"[-] GitHub sync push: {ge}")
            except Exception as pe:
                print(f"[-] Error in planner pass for '{repo_name}': {pe}")
            finally:
                telemetry.unregister_agent(agent_id)
                sync_dashboards()
        else:
            print(f"    [BUFFER FULL] {len(pending)}/{target_cap} pending. Next up: {pending[0]['name']}")
            
    return state_changed

def synthesize_steered_milestones(repo_id: str, steering_intent: str, limit: int = 3) -> list:
    """
    On-demand synthesis of up to `limit` milestones for a specific repository,
    strictly aligned with human steering intent, and auto-pushed to GitHub Projects.
    """
    repos = load_repositories()
    target_repo = None
    for r in repos:
        if r.get("id") == repo_id or r.get("name") == repo_id:
            target_repo = r
            break
    if not target_repo:
        raise ValueError(f"Repository '{repo_id}' not found in registry.")

    repo_name = target_repo.get("name", "Unknown")
    repo_path = Path(target_repo["path"])
    target_repo["user_steering_intent"] = steering_intent

    agent_id = f"steered_planner_opus_{repo_id}"
    task_desc = f"Steering {limit} milestones for {repo_name}: {steering_intent[:50]}"
    telemetry.register_agent(agent_id, repo_id, repo_name, "Strategic Planner (Steered)", PLANNER_MODEL_PRIMARY, task_desc)
    sync_dashboards()

    try:
        git_state = get_repo_git_state(repo_path)
        goal_filename, goal_doc = resolve_authoritative_goal_document(repo_path, target_repo)

        system_prompt = (
            "You are the Principal Strategic Research & Engineering Planner (Claude Opus 4.6 Thinking).\n"
            "Your role is STRICTLY high-level architectural planning and milestone dispatching.\n"
            f"The human architect has provided a specific steering direction. Synthesize exactly up to {limit} concrete milestones.\n"
            "Return ONLY a clean JSON array of milestone objects, each with:\n"
            "  'name': concise title,\n"
            "  'status': 'pending',\n"
            "  'details': detailed technical / mathematical description,\n"
            "  'relative_filepath': target source/LaTeX file relative to repository root,\n"
            "  'technical_specification': rigorous algorithmic formulation,\n"
            "  'executor_prompt': detailed authoritative prompt for Gemini 3.8 to implement the artifact."
        )
        historical_steering = ""
        try:
            historical_steering = get_steering_context_for_repo(repo_id)
        except Exception:
            pass

        user_prompt = (
            f"--- REPOSITORY OVERVIEW ---\n"
            f"Repository: {repo_name}\n"
            f"Category: {target_repo.get('category', 'general')}\n"
            f"Active Branch: {git_state['branch']}\n"
            f"Recent Commits:\n{git_state['recent_commits']}\n\n"
            f"--- AUTHORITATIVE ROADMAP & GOAL SPECIFICATION ({goal_filename}) ---\n"
            f"CRITICAL DIRECTIVE: Base milestone generation directly on the authoritative Roadmap phases.\n\n"
            f"{goal_doc[:12000]}\n\n"
            f"{historical_steering}\n"
            f"--- HUMAN ARCHITECT STEERING INTENT (MAX PRIORITY) ---\n"
            f"{steering_intent}\n\n"
            f"Synthesize exactly up to {limit} concrete milestones directly advancing this intent."
        )

        resp_text, planner_info = invoke_planner_cascade(
            system_prompt, user_prompt, repo_id=repo_id, repo_name=repo_name, temperature=0.3, workdir=repo_path
        )
        planner_session_id = planner_info.get("session_id")
        if agent_id and planner_session_id:
            telemetry.update_agent_session(agent_id, planner_session_id)

        new_milestones = parse_json_milestones(resp_text)
        if new_milestones:
            new_milestones = new_milestones[:limit]
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
            auto_approve = bool(target_repo.get("auto_approve_plans", False))
            for nm in new_milestones:
                nm["status"] = "ready" if auto_approve else "todo"
                nm["is_ai_development"] = True
                nm["is_recovery"] = False
                nm["approved_for_auto_run"] = auto_approve
                nm["github_column"] = "Ready" if auto_approve else "Backlog"
                nm["planned_by"] = f"{planner_info['label']} (Steered)"
                nm["planner_color"] = planner_info["color"]
                nm["added_by"] = f"{planner_info['label']} (Human Steered)"
                nm["added_at"] = now_str
                if planner_session_id:
                    nm["hermes_planner_session_id"] = planner_session_id
            target_repo.setdefault("milestones", []).extend(new_milestones)
            save_repositories(repos)
            notify(f"🎯 *Steered Planner ({planner_info['label']})*\nFormulated {len(new_milestones)} milestones for *{repo_name}* under direction: '{steering_intent[:60]}'", force=True)
            try:
                gh_sync.push_milestones_to_github()
            except Exception:
                pass
            return new_milestones
        return []
    finally:
        telemetry.unregister_agent(agent_id)
        sync_dashboards()

def reconcile_ghost_in_progress_tasks(repos: list, repos_lock=None) -> int:
    """
    Startup & Runtime Self-Healing Reconciler:
    Scans all milestones across all repositories. If any milestone is flagged as
    'in_progress' or has github_column == 'In progress', BUT there is no active agent
    executing it in telemetry, it was orphaned by a system reboot, service restart,
    or unexpected process termination.
    Auto-reconciles the ghost task back to 'ready' (if approved) or 'pending' (if unapproved)
    and syncs with GitHub Projects v2.
    """
    active_agent_tasks = set()
    try:
        tdata = telemetry.load_telemetry()
        for a in tdata.get("active_agents", {}).values():
            if "current_task" in a:
                active_agent_tasks.add(a["current_task"])
    except Exception:
        pass

    reconciled_count = 0
    state_changed = False
    
    def _do_reconcile():
        nonlocal state_changed, reconciled_count
        for repo in repos:
            repo_name = repo.get("name", "Unknown")
            for m in repo.get("milestones", []):
                st = m.get("status", "")
                gh_col = m.get("github_column", "")
                m_name = m.get("name", "")
                if st == "completed" or gh_col == "Done":
                    continue

                # If marked in_progress but no agent is actively executing it
                if st == "in_progress" or gh_col == "In progress":
                    if not any(m_name in at for at in active_agent_tasks):
                        # Verify whether a live hermes process is actively executing in this repository
                        repo_dir_str = str(repo.get("path", ""))
                        is_proc_alive = False
                        if repo_dir_str:
                            try:
                                chk = subprocess.run(["pgrep", "-a", "-f", f"hermes.*{repo_dir_str}"], capture_output=True, text=True)
                                if chk.returncode == 0 and chk.stdout.strip():
                                    is_proc_alive = True
                            except Exception:
                                pass
                        
                        if is_proc_alive:
                            # Hermes process is actively executing! Do NOT demote!
                            continue

                        is_appr = bool(m.get("approved_for_auto_run", False))
                        target_status = "ready" if is_appr else "pending"
                        target_gh = "Ready" if is_appr else "Backlog"
                        m["status"] = target_status
                        m["github_column"] = target_gh
                        state_changed = True
                        reconciled_count += 1
                        print(f"👻 [GHOST TASK RECONCILED] '{m_name}' in '{repo_name}' reset to {target_status} ({target_gh}) after service restart.")
                        try:
                            if gh_sync:
                                gh_sync.set_milestone_github_status(m_name, target_gh)
                        except Exception as ge:
                            print(f"[-] GitHub sync error in ghost reconciler: {ge}")

    if repos_lock:
        with repos_lock:
            _do_reconcile()
            if state_changed:
                save_repositories(repos)
                sync_dashboards()
    else:
        _do_reconcile()
        if state_changed:
            save_repositories(repos)
            sync_dashboards()

    return reconciled_count

RECOVERY_MODEL_PRIMARY = "gemini-3.8-flash-tiered"
RECOVERY_MODEL_FALLBACK = "gemini-2.5-flash"

def invoke_recovery_planner(system_prompt: str, user_prompt: str, repo_id: str, repo_name: str, workdir: Path | None = None) -> tuple[str, dict]:
    """
    Rapid, dedicated failure diagnosis & corrective re-planner using independent Hermes Recovery Agents.
    STRICTLY NEVER uses Opus to preserve Opus quotas and avoid slow reasoning latency.
    """
    recovery_models = [
        {"model_id": "gemini-3.8-flash-tiered", "display_name": "Gemini 3.8 Flash (Tiered)", "label": "Gemini 3.8 Flash (Recovery)"},
        {"model_id": "gemini-2.5-flash", "display_name": "Gemini 2.5 Flash", "label": "Gemini 2.5 Flash (Recovery)"}
    ]
    for minfo in recovery_models:
        mid = minfo["model_id"]
        mlabel = minfo["display_name"]
        try:
            print(f"[*] Dispatching Hermes Failure Recovery Planner Agent via {mlabel} (Non-Opus)...")
            resp, usage = run_hermes_agent(
                model=mid,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                repo_id=repo_id,
                repo_name=repo_name,
                role=f"Hermes Recovery Planner ({mlabel})",
                workdir=workdir,
                timeout_s=720
            )
            if resp and resp.strip():
                minfo["color"] = "amber"
                minfo["session_id"] = str(usage.get("session_id") or "")
                return resp, minfo
        except Exception as e:
            print(f"[-] Recovery model {mlabel} failed: {e}. Cascading...")
            continue
    raise RuntimeError("All recovery models failed.")

# ==============================================================================
# 🚨 FAILURE RECOVERY, RE-PRIORITIZATION & DEFENSIVE PROTOCOL (NO PURGE)
# ==============================================================================
def trigger_failure_replanning(target_repo: dict, failed_milestone: dict, failure_reason: str, repos: list, repos_lock: Any):
    """
    Called immediately when a milestone fails validation or execution.
    1. STRICTLY PRESERVES all downstream pending milestones (NO PURGE).
    2. Uses Gemini (NEVER Opus) to diagnose the issue and synthesize corrective milestones.
    3. RE-PRIORITIZES: Injects the corrective milestones at the FRONT of the queue so they execute first.
    4. STRICT QUEUE CAP: Total pending tasks never exceeds allowed_cap * 2 (e.g. 3 + 3 = 6).
    """
    repo_id = target_repo.get("id", "general")
    repo_name = target_repo.get("name", "Unknown")
    repo_path = Path(target_repo.get("path", AUTO_DIR))
    failed_name = failed_milestone.get("name", "Unknown")
    
    print("\n" + "!" * 75)
    print(f"🚨 FAILURE RECOVERY & RE-PRIORITIZATION ACTIVATED: {repo_name}")
    print(f"• Failed Milestone: {failed_name}")
    print(f"• Error Context:    {failure_reason[:200]}...")
    print("• Policy:           NO PURGE — Reprioritizing & queuing corrective fixes first")
    print("!" * 75)
    
    allowed_cap = int(target_repo.get("milestone_cap", 3))
    
    # Guard 0: Strictly check if Autonomous Planning is turned OFF for this repository
    is_auto_planning = target_repo.get("auto_planning", True)
    if REPOS_FILE.exists():
        try:
            with open(REPOS_FILE, "r", encoding="utf-8") as f:
                rdata = json.load(f)
            for r in rdata.get("repositories", []):
                if r.get("id") == repo_id:
                    is_auto_planning = r.get("auto_planning", True)
                    break
        except Exception:
            pass

    if not is_auto_planning:
        print(f"\n[PAUSED] Autonomous planning is DISABLED (OFF) for '{repo_name}'.")
        print(f"• Task '{failed_name}' failed, but NO new/corrective plans will be added to the board.")
        with repos_lock:
            failed_milestone["status"] = "failed"
            failed_milestone["failure_reason"] = failure_reason[:300]
            save_repositories(repos)
        notify(
            f"⚠️ *Task Execution Failed (Auto-Planning OFF)*\n"
            f"• *Repo:* {repo_name}\n"
            f"• *Failed Task:* {failed_name}\n"
            f"• *Error:* `{failure_reason[:150]}`\n"
            f"*(No new plans added — auto-planning is turned OFF)*",
            force=True
        )
        return

    # Guard 1: Stop cascading recovery storms (Never recursively recover a recovery milestone)
    if failed_milestone.get("is_recovery") or failed_name.startswith("Fix:") or failed_name.startswith("Scaffold:"):
        print(f"[-] Milestone '{failed_name}' is already a recovery task. Halting recursive recovery loop.")
        with repos_lock:
            failed_milestone["status"] = "failed"
            failed_milestone["failure_reason"] = failure_reason[:300]
            save_repositories(repos)
        return

    # Check current pending and failing counts
    current_milestones = target_repo.get("milestones", [])
    current_uncompleted = [m for m in current_milestones if m.get("status") != "completed" and m.get("name") != failed_name]
    existing_failed_count = sum(1 for m in current_uncompleted if m.get("status") in ("failed", "blocked"))
    max_total_pending = allowed_cap * 2  # e.g. 3 * 2 = 6 max

    # Guard 2: If failure backlog is piling up or queue is full, STOP inflating tasks!
    if len(current_uncompleted) >= max_total_pending or existing_failed_count >= 2:
        print(f"[-] Failure recovery paused for '{repo_name}': queue has {len(current_uncompleted)} tasks ({existing_failed_count} failing). Preserving existing milestones without adding more.")
        with repos_lock:
            failed_milestone["status"] = "failed"
            failed_milestone["failure_reason"] = failure_reason[:300]
            failed_milestone["failed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            save_repositories(repos)
        return

    max_corrective = max(1, min(allowed_cap, max_total_pending - len(current_uncompleted)))

    agent_id = f"recovery_planner_{repo_id}"
    task_desc = f"Emergency corrective re-planning for '{failed_name}' in {repo_name}"
    telemetry.register_agent(agent_id, repo_id, repo_name, "Recovery Planner (Gemini)", RECOVERY_MODEL_PRIMARY, task_desc)
    sync_dashboards()
    
    try:
        historical_steering = ""
        try:
            historical_steering = get_steering_context_for_repo(repo_id)
        except Exception:
            pass
            
        system_prompt = (
            "You are the Principal Technical Recovery & Debugging Engineer (Gemini 3.1 Pro).\n"
            "An autonomous execution step has encountered an error. Downstream tasks are preserved and paused.\n"
            f"Your objective: Diagnose the exact failure, evaluate the repository state, and synthesize up to {max_corrective}\n"
            "targeted, resilient corrective milestone(s) to fix the failure prerequisites and unblock progress.\n"
            "Return ONLY a clean JSON array of milestone objects, each with:\n"
            "  'name': concise title (e.g. 'Fix: ...' or 'Scaffold: ...'),\n"
            "  'status': 'pending',\n"
            "  'details': detailed technical / code description directly resolving the failure reason,\n"
            "  'relative_filepath': target source/LaTeX file relative to repository root,\n"
            "  'technical_specification': rigorous fix specifications with defensive guards,\n"
            "  'executor_prompt': detailed authoritative prompt for Gemini 3.8 to implement the fix.\n"
        )
        
        user_prompt = (
            f"--- REPOSITORY CONTEXT ---\n"
            f"Repository: {repo_name} ({repo_id})\n"
            f"Failed Milestone: {failed_name}\n"
            f"Target File: {failed_milestone.get('relative_filepath', 'N/A')}\n\n"
            f"--- CRITICAL FAILURE LOG / DIAGNOSTIC TRACE ---\n"
            f"{failure_reason}\n\n"
            f"--- ACCUMULATED HUMAN ARCHITECT PREFERENCES ---\n"
            f"{historical_steering}\n\n"
            f"Formulate exactly up to {max_corrective} concrete corrective milestone(s) to resolve this error."
        )
        
        # Invoke Hermes Recovery Agent (NEVER Opus)
        resp_text, planner_info = invoke_recovery_planner(
            system_prompt, user_prompt, repo_id=repo_id, repo_name=repo_name, workdir=repo_path
        )
        recovery_session_id = planner_info.get("session_id")
        if agent_id and recovery_session_id:
            telemetry.update_agent_session(agent_id, recovery_session_id)

        new_milestones = parse_json_milestones(resp_text)
        if new_milestones:
            new_milestones = new_milestones[:max_corrective]
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
            for nm in new_milestones:
                nm["status"] = "pending"
                nm["is_recovery"] = True
                nm["planned_by"] = planner_info["label"]
                nm["planner_color"] = "amber"
                nm["added_by"] = f"{planner_info['label']} (Corrective Fix)"
                nm["added_at"] = now_str
                if recovery_session_id:
                    nm["hermes_recovery_session_id"] = recovery_session_id
                
            with repos_lock:
                # RE-PRIORITIZE:
                # 1. Completed milestones stay completed at the beginning
                completed_list = [m for m in current_milestones if m.get("status") == "completed"]
                
                # 2. Update the failed milestone to 'blocked' (pending prerequisites)
                failed_milestone["status"] = "blocked"
                failed_milestone["failure_reason"] = failure_reason[:300]
                failed_milestone["failed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                # 3. Existing pending milestones are retained in full (NO PURGE)
                existing_other = [m for m in current_milestones if m.get("status") != "completed" and m.get("name") != failed_name]
                
                # 4. Corrective tasks are placed at the FRONT of the execution queue!
                combined = completed_list + new_milestones + [failed_milestone] + existing_other
                
                target_repo["milestones"] = combined
                save_repositories(repos)
                
            print(f"✅ [RECOVERY REPLANNING COMPLETE] Injected {len(new_milestones)} corrective milestones at the FRONT of queue (Non-Opus)!")
            notify(
                f"🔄 *Recovery Milestones Prioritized*\n"
                f"Injected {len(new_milestones)} corrective tasks at the FRONT of queue for *{repo_name}* via {planner_info['label']} (Preserved all existing tasks).",
                force=True
            )
            try:
                gh_sync.push_milestones_to_github()
            except Exception as ge:
                print(f"[-] GitHub sync push: {ge}")
    except Exception as e:
        print(f"[-] Error in dynamic failure recovery: {e}")
    finally:
        telemetry.unregister_agent(agent_id)
        sync_dashboards()

def extract_code_artifact(raw_text: str, target_rel: str) -> str:
    """
    Robustly extracts source code or LaTeX artifact from model response.
    Never truncates on internal docstring markdown fences.
    """
    if not raw_text or not isinstance(raw_text, str):
        return ""
    text = raw_text.strip()
    if "```" not in text:
        return text

    lines = text.splitlines()
    first_fence_idx = None
    last_fence_idx = None
    for idx, line in enumerate(lines):
        if line.strip().startswith("```"):
            if first_fence_idx is None:
                first_fence_idx = idx
            last_fence_idx = idx

    if first_fence_idx is not None and last_fence_idx is not None and last_fence_idx > first_fence_idx:
        extracted = "\n".join(lines[first_fence_idx + 1:last_fence_idx]).strip()
        if len(extracted) > 100:
            return extracted

    return text

# ==============================================================================
# ⚡ PRIORITIZED EXECUTOR ENGINE (GEMINI 3.8 FLASH TIERED - CONCURRENT WORKERS)
# ==============================================================================
def execute_single_milestone_worker(target_repo: dict, target_milestone: dict, repos: list, repos_lock: Any, worker_index: int = 0, timeout_s: int = 1500) -> bool:
    """Executes a single milestone for a repository with atomic Git commit."""
    # Stagger dispatch slot through Adaptive Rate Governor
    try:
        _GOVERNOR.acquire_dispatch_slot(worker_index)
    except Exception:
        pass
    repo_id = target_repo.get("id", "general")
    repo_name = target_repo.get("name", "Unknown")
    repo_path = Path(target_repo["path"])
    milestone_name = target_milestone["name"]
    raw_rel = target_milestone.get("relative_filepath")
    if not raw_rel or not str(raw_rel).strip():
        if repo_id == "phd_proposal":
            target_rel = "proposal/chapters/chapter1.tex"
        else:
            target_rel = "generated_artifact.txt"
    else:
        target_rel = str(raw_rel).strip()

    if repo_id == "phd_proposal":
        if target_rel.startswith("chapters/"):
            target_rel = "proposal/" + target_rel
        elif target_rel == "main.tex" or target_rel.endswith("/main.tex"):
            target_rel = "proposal/chapters/chapter3_supplementary.tex"
    
    m_clean = "".join(c for c in milestone_name if c.isalnum())[:12]
    agent_id = f"executor_{repo_id}_{m_clean}"
    task_desc = f"Implementing '{milestone_name}' ({target_rel})"
    hermes_session_id = None
    
    # Mark in-progress atomically in repositories.json and disk immediately
    with repos_lock:
        target_milestone["status"] = "in_progress"
        target_milestone["github_column"] = "In progress"
        save_repositories(repos)

    telemetry.register_agent(
        agent_id, repo_id, repo_name, "Hermes Task Executor", EXECUTOR_MODEL, task_desc,
        milestone_name=milestone_name
    )
    sync_dashboards()
    
    # Live sync card to 'In progress' column on GitHub Projects v2
    try:
        gh_sync.set_milestone_github_status(milestone_name, "In progress")
    except Exception:
        pass
    
    try:
        print(f"\n[🚀 DISPATCHING WORKER: {repo_name}]")
        print(f"• Priority:   {target_repo.get('priority')}")
        print(f"• Milestone:  {milestone_name}")
        print(f"• Target File: {target_rel}")
        
        category = target_repo.get("category", "general")
        is_persian_latex = (category == "academic-latex-research")
        
        system_prompt = (
            "You are an Elite Autonomous AI Engineer and Academic Writer.\n"
            "Execute the requested technical deliverable with publication-grade perfection.\n"
            "CRITICAL ROLE CONSTRAINT:\n"
            "1. You are a PURE CODE/TEXT GENERATOR. Output the exact contents of the requested file.\n"
            "2. DO NOT attempt to run bash commands, call external tools, or generate function calls.\n"
            "3. Output ONLY the raw artifact code or LaTeX document body directly in your response.\n"
        )
        if is_persian_latex:
            rule_file = repo_path / "proposal/prompt_for_latex_persian_AI_generation/PROMPT_v2.8.md"
            corpus_file = repo_path / "persian_academic_corpus/glossary/robotics_social_nav_lexicon.md"
            rule_content = rule_file.read_text(encoding="utf-8") if rule_file.exists() else ""
            corpus_content = corpus_file.read_text(encoding="utf-8") if corpus_file.exists() else ""
            system_prompt += (
                "CRITICAL XEPERSIAN RULES:\n"
                "1. Output ONLY the raw body of the LaTeX section without preamble.\n"
                "2. Strict Line Separation: Persian text lines and English/math/macro lines must be strictly separated.\n"
                "3. Enforce approved Persian robotics terminology.\n\n"
                f"--- RULES ---\n{rule_content[:1000]}\n\n"
                f"--- LEXICON ---\n{corpus_content[:1000]}"
            )
        elif category == "robotics-ros2":
            system_prompt += (
                "CRITICAL ROS 2 RULES:\n"
                "1. Output clean, modular ROS 2 code conforming to rclpy / rclcpp standards.\n"
                "2. Include robust logging, error handling, parameter declarations, and lifecycle management."
            )

        spec = target_milestone.get("technical_specification", target_milestone.get("details", ""))
        prompt_inst = target_milestone.get("executor_prompt", f"Implement {milestone_name} completely.")
        
        # Sanitize instructions to avoid triggering Google Cloud Code tool-calling traps
        sanitized_inst = re.sub(r"\bRun\s+(the\s+)?(xelatex|python|bash|compiler|git)\b", r"Ensure compatibility with \2", prompt_inst, flags=re.IGNORECASE)
        sanitized_inst = re.sub(r"\b(execute|run)\s+(`?[a-zA-Z0-9_\-\./]+\s+-+[a-zA-Z0-9_\-\./]+[^\n`]*`?)", r"Target compilation standard: \2", sanitized_inst, flags=re.IGNORECASE)

        user_prompt = (
            f"Repository: {repo_name}\n"
            f"File: {target_rel}\n"
            f"Milestone: {milestone_name}\n"
            f"Technical Specification: {spec}\n\n"
            f"Execution Instructions: {sanitized_inst}\n\n"
            "Generate the complete, fully implemented artifact code/content directly. Do not call tools or execute bash commands."
        )
        
        # Selected executor model per repository (or global fallback)
        chosen_executor_model = target_repo.get("executor_model") or EXECUTOR_MODEL

        # Compute dynamically adapted token policy (reward/penalty & quota aware)
        try:
            token_policy = _GOVERNOR.compute_policy(model_id=chosen_executor_model)
            alloc_tokens = token_policy.get("max_tokens", 3400)
            policy_tier = token_policy.get("tier", "BALANCED")
            directive = token_policy.get("directive", "")
            if directive:
                system_prompt += f"\n\nTOKEN FLOW GOVERNOR DIRECTIVE ({policy_tier}):\n{directive}\n"
        except Exception:
            alloc_tokens = 3400
            policy_tier = "BALANCED"

        print(f"[*] Spawning Hermes Executor Agent for '{target_rel}' in '{repo_name}' [{policy_tier} | max_tokens={alloc_tokens} | model={chosen_executor_model}]...")
        content, usage = run_hermes_agent(
            model=chosen_executor_model,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            repo_id=repo_id,
            repo_name=repo_name,
            role=f"Hermes Task Executor ({chosen_executor_model})",
            workdir=repo_path,
            timeout_s=timeout_s
        )
        hermes_session_id = usage.get("session_id")
        if hermes_session_id:
            telemetry.update_agent_session(agent_id, hermes_session_id)
        
        execution_failed = False
        failure_reason = ""
        clean_content = ""

        if not content or not isinstance(content, str):
            execution_failed = True
            failure_reason = "Model returned empty or non-string response."
        else:
            clean_content = extract_code_artifact(content, str(target_rel))

            # Guard against writing error messages or tool artifacts to source files
            error_keywords = [
                "could not complete a tool call",
                "switch to Gemini 3.7",
                "no longer available",
                "Internal Server Error",
                "Service Unavailable",
                "All accounts failed"
            ]
            if any(err.lower() in clean_content.lower() for err in error_keywords) or len(clean_content) < 20:
                execution_failed = True
                failure_reason = f"Model output is an error/notice message, not real code: {clean_content[:150]}"

        # Master Document Protection: Never overwrite main.tex directly
        if str(target_rel).rstrip("/").endswith("main.tex"):
            execution_failed = True
            failure_reason = "Milestone targets master main.tex. Direct overwrite forbidden to protect documentclass preamble."

        if not execution_failed:
            out_file = repo_path / target_rel
            out_file.parent.mkdir(parents=True, exist_ok=True)

            # Check if Hermes Agent already created/wrote a valid file on disk via its tools
            agent_already_wrote_valid_file = False
            if out_file.exists() and out_file.stat().st_size > 500:
                if out_file.suffix == ".py":
                    chk = subprocess.run(["python3", "-m", "py_compile", str(out_file)], capture_output=True)
                    if chk.returncode == 0:
                        agent_already_wrote_valid_file = True
                elif out_file.suffix == ".tex":
                    agent_already_wrote_valid_file = True

            if not agent_already_wrote_valid_file:
                out_file.write_text(clean_content, encoding="utf-8")
                print(f"[+] [{repo_name}] Written {len(clean_content)} chars to {out_file}")
            else:
                print(f"[+] [{repo_name}] Valid artifact already authored on disk ({out_file.stat().st_size} bytes). Preserving agent file.")
            
            # Post-Processing & Robust Validation
            if is_persian_latex and out_file.suffix == ".tex":
                if FIX_SCRIPT.exists():
                    subprocess.run(["python3", str(FIX_SCRIPT), str(out_file)], capture_output=True)
                main_tex = repo_path / "proposal/main.tex"
                if main_tex.exists():
                    tex_res = subprocess.run(["xelatex", "-interaction=nonstopmode", "main.tex"], cwd=main_tex.parent, capture_output=True, text=True)
                    if tex_res.returncode != 0:
                        execution_failed = True
                        tail = "\n".join(tex_res.stdout.splitlines()[-30:]) if tex_res.stdout else "XeLaTeX compilation failed."
                        failure_reason = f"XeLaTeX compilation failed (code {tex_res.returncode}):\n{tail}"
            elif out_file.suffix == ".py":
                py_res = subprocess.run(["python3", "-m", "py_compile", str(out_file)], capture_output=True, text=True)
                if py_res.returncode != 0:
                    execution_failed = True
                    failure_reason = f"Python syntax compilation failed:\n{py_res.stderr or py_res.stdout}"
                
        if execution_failed:
            print(f"❌ [VALIDATION FAILED] {milestone_name}:\n{failure_reason[:300]}")
            
            # Explicitly log error event to telemetry so it shows on Cockpit & Phoenix
            try:
                telemetry.record_failure_event(
                    agent_id=f"worker_{repo_id}_{target_milestone.get('name', 'task')[:15]}",
                    repo_name=repo_name,
                    role="Hermes Code Executor",
                    milestone_name=milestone_name,
                    error_msg=failure_reason
                )
            except Exception:
                pass

            try:
                record_error_span(
                    model=EXECUTOR_MODEL,
                    role="Hermes Code Executor",
                    repo_name=repo_name,
                    task_name=milestone_name,
                    error_message=failure_reason,
                    session_id=hermes_session_id
                )
            except Exception:
                pass

            failed_task_rec = None
            try:
                failed_task_rec = ftr_mod.get_registry().record_failure(
                    repo_id=repo_id,
                    repo_name=repo_name,
                    milestone_name=milestone_name,
                    failure_reason=failure_reason,
                    target_file=str(target_rel),
                    model_used=EXECUTOR_MODEL,
                    github_item_id=target_milestone.get("github_item_id"),
                    hermes_session_id=hermes_session_id
                )
            except Exception as fre:
                print(f"[-] Registry recording notice: {fre}")

            with repos_lock:
                target_milestone["status"] = "failed"
                target_milestone["failure_reason"] = failure_reason
                target_milestone["failed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if hermes_session_id:
                    target_milestone["hermes_session_id"] = hermes_session_id
                if failed_task_rec:
                    target_milestone["failure_task_id"] = failed_task_rec.get("task_id")
                try:
                    save_repositories(repos)
                except Exception as s_err:
                    print(f"[-] Warning: Failed to persist failed milestone state: {s_err}")
            try:
                gh_sync.set_milestone_github_status(milestone_name, "In review")
            except Exception:
                pass
            trigger_failure_replanning(target_repo, target_milestone, failure_reason, repos, repos_lock)
            return False
            
        # Thread-safe Atomic Git Commit
        commit_hash = "local"
        branch_name = "main"
        commit_url = ""
        with _GIT_LOCK:
            subprocess.run(["git", "add", target_rel], cwd=repo_path)
            if (repo_path / "proposal/main.tex").exists():
                subprocess.run(["git", "add", "proposal/main.tex"], cwd=repo_path)
            commit_msg = f"feat(milestone): {milestone_name} - implement {target_rel}"
            res = subprocess.run(["git", "commit", "-m", commit_msg], cwd=repo_path, capture_output=True, text=True)
            
            rev_out = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=repo_path, capture_output=True, text=True)
            if rev_out.returncode == 0 and rev_out.stdout.strip():
                commit_hash = rev_out.stdout.strip()

            branch_out = subprocess.run(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=repo_path, capture_output=True, text=True)
            if branch_out.returncode == 0 and branch_out.stdout.strip():
                branch_name = branch_out.stdout.strip()

            remote_out = subprocess.run(["git", "config", "--get", "remote.origin.url"], cwd=repo_path, capture_output=True, text=True)
            raw_remote = remote_out.stdout.strip()
            if raw_remote:
                http_remote = raw_remote.replace("git@github.com:", "https://github.com/").replace(".git", "").rstrip("/")
                if "github.com" in http_remote and commit_hash != "local":
                    commit_url = f"{http_remote}/commit/{commit_hash}"

            if "nothing to commit" not in res.stdout:
                print(f"✅ [ATOMIC COMMIT] {repo_name} ({commit_hash} on {branch_name}): {commit_msg}")
            else:
                print(f"ℹ️ [GIT UP-TO-DATE] {repo_name} ({commit_hash} on {branch_name}): {target_rel}")

        # Artifacts and Structured Research Preservation
        artifacts = target_milestone.get("artifacts") or {}
        artifacts["primary_file"] = str(target_rel)
        artifacts["branch"] = branch_name
        artifacts["commit_hash"] = commit_hash
        if commit_url:
            artifacts["commit_url"] = commit_url

        # Check if task is literature/research/survey/article oriented to attach structured papers
        text_context = f"{milestone_name} {target_milestone.get('details', '')} {target_milestone.get('executor_prompt', '')}".lower()
        if any(k in text_context for k in ["literature", "research", "survey", "paper", "article", "arxiv", "crowd framework", "sensor fusion", "review"]):
            if "articles" not in artifacts or not artifacts["articles"]:
                shards_dir = repo_path / "research_run_01/shards"
                collected_articles = []
                if shards_dir.exists():
                    for sfile in sorted(shards_dir.glob("*.json")):
                        try:
                            with open(sfile, "r", encoding="utf-8") as f:
                                sdata = json.load(f)
                                if isinstance(sdata, list):
                                    for item in sdata:
                                        if isinstance(item, dict) and item.get("title"):
                                            collected_articles.append({
                                                "title": item.get("title"),
                                                "authors": item.get("authors", "Unknown"),
                                                "year": str(item.get("year", "2024")),
                                                "venue": item.get("venue", "arXiv / IEEE"),
                                                "doi_or_url": item.get("doi_or_url", item.get("url", "")),
                                                "summary": item.get("summary", "")[:280],
                                                "sensors_fused": item.get("sensors_fused", [])
                                            })
                        except Exception:
                            pass
                if collected_articles:
                    artifacts["articles"] = collected_articles[:12]

        # Mark Milestone as Completed (Thread-Safe Save)
        with repos_lock:
            target_milestone["status"] = "completed"
            target_milestone["completed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            target_milestone["commit_hash"] = commit_hash
            target_milestone["branch"] = branch_name
            if commit_url:
                target_milestone["commit_url"] = commit_url
            target_milestone["output_file"] = str(target_rel)
            target_milestone["artifacts"] = artifacts
            if hermes_session_id:
                target_milestone["hermes_session_id"] = hermes_session_id

            try:
                from tasks.skill_matcher import infer_task_skills
                curr_skills = target_milestone.get("skills")
                if not isinstance(curr_skills, list) or len(curr_skills) < 3:
                    target_milestone["skills"] = infer_task_skills(target_milestone, repo_id, target_repo.get("category", ""))
            except Exception:
                pass

            # Defensive persistence retry to guard milestone completion against filesystem contention
            saved = False
            for save_attempt in range(5):
                try:
                    save_repositories(repos)
                    saved = True
                    break
                except Exception as ex:
                    print(f"[-] Warning: Transient failure in save_repositories during milestone completion (attempt {save_attempt + 1}/5): {ex}")
                    time.sleep(0.1 * (2 ** save_attempt) + random.uniform(0.02, 0.08))
            if not saved:
                print(f"[-] Error: Could not persist repositories.json after 5 attempts, but milestone artifact '{target_rel}' was generated.")

        # Mark in Failed Tasks Registry as resolved if previously logged
        try:
            tid = target_milestone.get("failure_task_id") or ftr_mod.generate_task_id(repo_id, milestone_name)
            ftr_mod.get_registry().mark_resolved(tid, fix_commit_hash=commit_hash, notes="Clean atomic completion by Hermes Agent")
        except Exception:
            pass
        
        # Live sync card to 'Done' column on GitHub Projects v2 with rich metadata
        try:
            gh_sync.set_milestone_github_status(
                milestone_name=milestone_name,
                target_status="Done",
                commit_hash=commit_hash,
                branch=branch_name,
                commit_url=commit_url,
                output_file=str(target_rel),
                articles=artifacts.get("articles"),
                skills=target_milestone.get("skills")
            )
        except Exception:
            pass
        
        notify(f"🎯 *Milestone Delivered*\\n• *Repo:* {repo_name}\\n• *Milestone:* {milestone_name}\\n• *File:* `{target_rel}`", force=True)
        return True
    except Exception as e:
        err_msg = f"Runtime error executing milestone '{milestone_name}': {e}"
        print(f"[-] {err_msg}")
        failed_task_rec = None
        current_sess_id = hermes_session_id if 'hermes_session_id' in locals() else None
        try:
            failed_task_rec = ftr_mod.get_registry().record_failure(
                repo_id=repo_id,
                repo_name=repo_name,
                milestone_name=milestone_name,
                failure_reason=err_msg,
                target_file=str(target_rel),
                model_used=EXECUTOR_MODEL,
                github_item_id=target_milestone.get("github_item_id"),
                hermes_session_id=current_sess_id
            )
        except Exception:
            pass

        with repos_lock:
            target_milestone["status"] = "failed"
            target_milestone["failure_reason"] = str(e)
            target_milestone["failed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if current_sess_id:
                target_milestone["hermes_session_id"] = current_sess_id
            if failed_task_rec:
                target_milestone["failure_task_id"] = failed_task_rec.get("task_id")
            try:
                save_repositories(repos)
            except Exception as s_err:
                print(f"[-] Warning: Failed to persist failed milestone state: {s_err}")
        trigger_failure_replanning(target_repo, target_milestone, err_msg, repos, repos_lock)
        return False
    finally:
        telemetry.unregister_agent(agent_id)
        sync_dashboards()

def is_milestone_executable(m: dict, repo: dict = None) -> bool:
    """
    Determines if a milestone is ready for automatic execution.
    - If status is 'completed' or 'blocked': False.
    - If repo has auto_approve_plans enabled: True (bypasses human approval).
    - If it's an AI-generated development milestone (not error recovery):
      STRICTLY REQUIRES human architect approval tick (approved_for_auto_run == True).
    - If it's an error recovery milestone (is_recovery == True), it can execute automatically to heal the build.
    - If user manually added it or manually edited it, it executes.
    """
    st = m.get("status", "pending")
    if st in ("completed", "blocked"):
        return False
    if st not in ("in_progress", "pending", "ready", "backlog", "todo"):
        return False

    # 0. Task Dependency DAG Check (Prerequisites must be completed before execution)
    deps = m.get("depends_on", []) or m.get("prerequisites", [])
    if deps and repo:
        completed_in_repo = {
            other.get("name", "").strip().lower()
            for other in repo.get("milestones", [])
            if other.get("status") == "completed"
        }
        for d in deps:
            d_clean = str(d).strip().lower()
            if d_clean and d_clean not in completed_in_repo:
                # Prerequisite is not completed yet! Hold execution.
                return False

    # Auto-Approve Mode: If repo has auto_approve_plans enabled, bypass manual approval requirement!
    if repo and repo.get("auto_approve_plans", False):
        return True

    # If milestone itself was already approved by user
    if m.get("approved_for_auto_run", False):
        return True

    is_ai_dev = m.get("is_ai_development", False) or (bool(m.get("planned_by")) and not m.get("is_recovery", False))
    if is_ai_dev and not m.get("is_recovery", False):
        # AI Development Plan: Human architect MUST check the tick!
        return bool(m.get("approved_for_auto_run", False))

    # If status is backlog, it's waiting in the unapproved queue
    if st == "backlog":
        return False

    return True

def run_executor_pass(repos: list, max_concurrent: int = 3) -> bool:
    """
    Dispatches concurrent execution agents across all repositories with pending milestones.
    Supports multi-repo parallelism AND intra-repo milestone parallelism!
    Dynamically paced by Adaptive Rate Governor to prevent HTTP 429 burst rate limits.
    """
    # Re-sync user configurations (auto_approve_plans, milestone approvals) from disk immediately
    if REPOS_FILE.exists():
        try:
            with open(REPOS_FILE, "r", encoding="utf-8") as f:
                disk_data = json.load(f)
            disk_repo_map = {r.get("id"): r for r in disk_data.get("repositories", []) if r.get("id")}
            for repo in repos:
                rid = repo.get("id")
                if rid in disk_repo_map:
                    dr = disk_repo_map[rid]
                    repo["auto_approve_plans"] = dr.get("auto_approve_plans", False)
                    repo["auto_planning"] = dr.get("auto_planning", True)
                    disk_milestones = {m.get("name"): m for m in dr.get("milestones", []) if m.get("name")}
                    for m in repo.get("milestones", []):
                        mname = m.get("name")
                        if mname in disk_milestones:
                            dm = disk_milestones[mname]
                            if m.get("status") not in ("in_progress", "completed"):
                                m["approved_for_auto_run"] = dm.get("approved_for_auto_run", False)
                                m["status"] = dm.get("status", m.get("status"))
                                m["github_column"] = dm.get("github_column", m.get("github_column"))
        except Exception:
            pass

    # Dynamic Capacity & Worker Headroom Forecasting (using feedback from telemetry & quota pools)
    try:
        from capacity_forecaster import get_fleet_capacity_forecast
        c_fc = get_fleet_capacity_forecast()
        recommended_slots = c_fc.get("recommended_worker_slots", 2)
        safe_concurrent = min(max_concurrent, recommended_slots)
        if safe_concurrent < max_concurrent:
            print(f"[*] Capacity Forecaster: Concurrency set to {safe_concurrent} worker(s). Reason: {c_fc.get('concurrency_allocation_reason')}")
    except Exception:
        try:
            safe_concurrent = _GOVERNOR.compute_safe_concurrency(requested_max=max_concurrent, model_id=EXECUTOR_MODEL)
        except Exception:
            safe_concurrent = min(2, max_concurrent)

    print("\n" + "=" * 75)
    print(f" ⚡ ADAPTIVE PRIORITIZED EXECUTOR DISPATCH (SAFE HEADROOM: {safe_concurrent} WORKERS)")
    print("=" * 75)
    
    tasks_to_run = []
    seen_files = set()
    
    # Pass 1: Select 1 executable task per repo (fair multi-repo balance)
    for repo in repos:
        executable = [m for m in repo.get("milestones", []) if is_milestone_executable(m, repo)]
        for m in executable:
            target_f = m.get("relative_filepath", m.get("name"))
            if target_f not in seen_files:
                tasks_to_run.append((repo, m))
                seen_files.add(target_f)
                break
        if len(tasks_to_run) >= safe_concurrent:
            break
            
    # Pass 2: If concurrent worker slots are still available, fill with remaining executable milestones!
    if len(tasks_to_run) < safe_concurrent:
        for repo in repos:
            executable = [m for m in repo.get("milestones", []) if is_milestone_executable(m, repo)]
            for m in executable:
                target_f = m.get("relative_filepath", m.get("name"))
                if (repo, m) not in tasks_to_run and target_f not in seen_files:
                    tasks_to_run.append((repo, m))
                    seen_files.add(target_f)
                    if len(tasks_to_run) >= safe_concurrent:
                        break
            if len(tasks_to_run) >= safe_concurrent:
                break

    # Pass 3: If slots are open and no pending tasks remain, retry uncompleted failed tasks!
    if len(tasks_to_run) < safe_concurrent:
        for repo in repos:
            failed_tasks = [m for m in repo.get("milestones", []) if m.get("status") == "failed"]
            for m in failed_tasks:
                target_f = m.get("relative_filepath", m.get("name"))
                if (repo, m) not in tasks_to_run and target_f not in seen_files:
                    print(f"[*] Auto-Retrying failed milestone: '{m.get('name')}'")
                    m["status"] = "pending"
                    tasks_to_run.append((repo, m))
                    seen_files.add(target_f)
                    if len(tasks_to_run) >= safe_concurrent:
                        break
            if len(tasks_to_run) >= safe_concurrent:
                break
            
    if not tasks_to_run:
        print("[*] No pending milestones found across any repository. All goals up to date!")
        return False
        
    print(f"[*] Found {len(tasks_to_run)} pending tasks across active repositories.")
    print(f"[*] Spawning {len(tasks_to_run)} concurrent Gemini 3.8 executor workers with staggered flow...")
    
    repos_lock = threading.Lock()
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks_to_run)) as pool:
        futures = [pool.submit(execute_single_milestone_worker, r, m, repos, repos_lock, idx) for idx, (r, m) in enumerate(tasks_to_run)]
        results = [f.result() for f in futures]
        
    return any(results)

# ==============================================================================
# 🌐 DECENTRALIZED PER-REPOSITORY AUTONOMOUS ACTOR PODS & FLEET SUPERVISOR
# ==============================================================================

class RepoAutonomousPod:
    """
    Decentralized Autonomous Actor Pod for an individual repository.
    Each pod independently manages:
      1. Dedicated Strategic Planner Thread (Claude Opus / Gemini)
      2. Dedicated Non-Blocking Worker Pool (Gemini 3.8 Workers)
      3. Isolated Local ThreadLock and Dispatch Trigger Event
      4. Dynamic Cadence integration with external CadenceManager
    """
    def __init__(self, repo_id: str):
        self.repo_id = repo_id
        self.lock = threading.RLock()
        self.dispatch_trigger = threading.Event()
        self.active_futures = {}  # future -> (milestone_name, target_file, worker_index, start_time)
        self.worker_pool = None
        self.planner_thread = None
        self.executor_thread = None
        self.running = False

    def start(self):
        self.running = True
        self.worker_pool = concurrent.futures.ThreadPoolExecutor(max_workers=3, thread_name_prefix=f"worker_{self.repo_id}")
        self.planner_thread = threading.Thread(target=self._planner_loop, name=f"PlannerPod_{self.repo_id}", daemon=True)
        self.executor_thread = threading.Thread(target=self._executor_loop, name=f"ExecutorPod_{self.repo_id}", daemon=True)
        self.planner_thread.start()
        self.executor_thread.start()
        print(f"[*] 🚀 [POD STARTED] Dedicated Actor Pod online for repository: '{self.repo_id}'")

    def stop(self):
        self.running = False
        self.dispatch_trigger.set()
        if self.worker_pool:
            self.worker_pool.shutdown(wait=False)

    def _planner_loop(self):
        cm = get_cadence_manager()
        while self.running and not _SHUTDOWN_EVENT.is_set():
            try:
                # Check external triggers (e.g. force_plan from external governor)
                external_wake = cm.consume_trigger(self.repo_id, "wake_planner") or cm.consume_trigger(self.repo_id, "force_plan")
                
                # Check dynamic cadence & policy
                cadence = cm.get_repo_cadence(self.repo_id)
                if cadence.get("emergency_system_pause") or not cadence.get("auto_planning_enabled", True):
                    cm.record_repo_event(self.repo_id, state="PAUSED", active_workers=len(self.active_futures), last_action="Planning paused by policy")
                    _SHUTDOWN_EVENT.wait(timeout=10.0)
                    continue

                repos = load_repositories()
                target_repo = next((r for r in repos if r.get("id") == self.repo_id), None)
                if target_repo and target_repo.get("auto_planning", True):
                    milestones = target_repo.get("milestones", [])
                    queued_waiting = [m for m in milestones if m.get("status") in ("pending", "todo", "ready", "backlog")]
                    cap = int(target_repo.get("milestone_cap", 3))

                    if len(queued_waiting) < cap or external_wake:
                        cm.record_repo_event(self.repo_id, state="PLANNING", active_workers=len(self.active_futures), last_action=f"Synthesizing plan (Buffer: {len(queued_waiting)}/{cap})")
                        state_changed = run_planner_pass([target_repo])
                        if state_changed:
                            self.dispatch_trigger.set()
                            cm.record_repo_event(self.repo_id, state="IDLE", active_workers=len(self.active_futures), last_action="New milestones generated")
                    else:
                        cm.record_repo_event(self.repo_id, state="IDLE", active_workers=len(self.active_futures), last_action=f"Buffer full ({len(queued_waiting)}/{cap})")
            except Exception as e:
                cm.record_repo_event(self.repo_id, state="ERROR_BACKOFF", last_action=str(e))
                print(f"[-] [POD: {self.repo_id}] Planner error: {e}")

            # Dynamic Cadence sleep (tunable from external supervisor)
            cadence = cm.get_repo_cadence(self.repo_id)
            poll_s = float(cadence.get("planner_cadence_s", 20.0))
            _SHUTDOWN_EVENT.wait(timeout=poll_s)

    def _executor_loop(self):
        cm = get_cadence_manager()
        while self.running and not _SHUTDOWN_EVENT.is_set():
            try:
                # Check external triggers (e.g. force_dispatch from external governor)
                cm.consume_trigger(self.repo_id, "wake_executor") or cm.consume_trigger(self.repo_id, "force_dispatch")
                
                cadence = cm.get_repo_cadence(self.repo_id)
                if cadence.get("emergency_system_pause") or not cadence.get("auto_execution_enabled", True):
                    cm.record_repo_event(self.repo_id, state="PAUSED", active_workers=len(self.active_futures), last_action="Execution paused by policy")
                    _SHUTDOWN_EVENT.wait(timeout=5.0)
                    continue

                # 1. Harvest finished worker futures
                finished = [f for f in list(self.active_futures.keys()) if f.done()]
                for f in finished:
                    m_name, t_file, w_idx, st = self.active_futures.pop(f)
                    elapsed = round(time.time() - st, 1)
                    try:
                        res = f.result()
                        res_str = "SUCCESS" if res else "FAILED"
                        print(f"[*] 🎯 [POD: {self.repo_id}] Worker Complete: '{m_name}' [{res_str}] in {elapsed}s.")
                    except Exception as ex:
                        print(f"[-] [POD: {self.repo_id}] Worker Exception: {ex}")
                    # Signal planner that a task slot was freed
                    self.dispatch_trigger.set()

                # 2. Check open concurrency slots for this repo
                max_w = int(cadence.get("max_workers", 2))
                open_slots = max_w - len(self.active_futures)

                if open_slots > 0:
                    repos = load_repositories()
                    target_repo = next((r for r in repos if r.get("id") == self.repo_id), None)
                    if target_repo:
                        executable = [m for m in target_repo.get("milestones", []) if is_milestone_executable(m, target_repo)]
                        running_files = {tf for (_, tf, _, _) in self.active_futures.values()}
                        running_names = {mn for (mn, _, _, _) in self.active_futures.values()}

                        tasks_to_launch = []
                        for m in executable:
                            target_f = m.get("relative_filepath", m.get("name"))
                            m_name = m.get("name")
                            if target_f not in running_files and m_name not in running_names:
                                tasks_to_launch.append(m)
                                running_files.add(target_f)
                                running_names.add(m_name)
                                if len(tasks_to_launch) >= open_slots:
                                    break

                        # Retry failed milestones if slots are open
                        if len(tasks_to_launch) < open_slots:
                            failed_tasks = [m for m in target_repo.get("milestones", []) if m.get("status") == "failed"]
                            for m in failed_tasks:
                                target_f = m.get("relative_filepath", m.get("name"))
                                m_name = m.get("name")
                                if target_f not in running_files and m_name not in running_names:
                                    m["status"] = "pending"
                                    tasks_to_launch.append(m)
                                    running_files.add(target_f)
                                    running_names.add(m_name)
                                    if len(tasks_to_launch) >= open_slots:
                                        break

                        for m in tasks_to_launch:
                            target_f = m.get("relative_filepath", m.get("name"))
                            w_idx = len(self.active_futures)
                            timeout_s = int(cadence.get("timeout_s", 1500))
                            fut = self.worker_pool.submit(execute_single_milestone_worker, target_repo, m, repos, self.lock, w_idx, timeout_s)
                            self.active_futures[fut] = (m.get("name"), target_f, w_idx, time.time())
                            cm.record_repo_event(self.repo_id, state="EXECUTING", active_workers=len(self.active_futures), last_action=f"Launched: {m.get('name')[:35]}")
                            print(f"[*] 🚀 [POD: {self.repo_id}] Launched task '{m.get('name')}' (Active: {len(self.active_futures)}/{max_w})")
            except Exception as ee:
                cm.record_repo_event(self.repo_id, state="ERROR_BACKOFF", last_action=str(ee))
                print(f"[-] [POD: {self.repo_id}] Executor error: {ee}")

            # Dynamic Cadence poll or wake on trigger
            cadence = cm.get_repo_cadence(self.repo_id)
            poll_s = float(cadence.get("executor_cadence_s", 2.0))
            self.dispatch_trigger.wait(timeout=poll_s)
            self.dispatch_trigger.clear()

ACTIVE_PODS: dict[str, RepoAutonomousPod] = {}

def _fleet_supervisor_loop():
    """
    Decentralized Fleet Supervisor Daemon:
    Spawns, maintains, and scales dedicated RepoAutonomousPod actors per repository.
    Runs global telemetry, quota evaluations, and self-healing ghost reconciler.
    """
    print("[*] 🌐 [FLEET SUPERVISOR] Decentralized Multi-Repo Fleet Supervisor online.")
    cm = get_cadence_manager()

    while not _SHUTDOWN_EVENT.is_set():
        try:
            repos = load_repositories()
            active_rids = set()

            for repo in repos:
                rid = repo.get("id")
                if not rid:
                    continue
                active_rids.add(rid)

                # Spawn dedicated actor pod for this repository if not already running
                if rid not in ACTIVE_PODS:
                    pod = RepoAutonomousPod(rid)
                    ACTIVE_PODS[rid] = pod
                    pod.start()

            # Clean up pods for removed repositories
            removed = [rid for rid in list(ACTIVE_PODS.keys()) if rid not in active_rids]
            for rid in removed:
                print(f"[*] [FLEET SUPERVISOR] Decommissioning pod for repository '{rid}'")
                ACTIVE_PODS[rid].stop()
                del ACTIVE_PODS[rid]

            # Global Ghost Task Self-Healing
            try:
                reconciled = reconcile_ghost_in_progress_tasks(repos, _REPOS_GLOBAL_LOCK)
                if reconciled > 0:
                    print(f"👻 [SELF-HEALING] Reconciled {reconciled} orphaned in-progress task(s).")
            except Exception:
                pass

            # Quota readiness check
            try:
                from token_quota_optimizer import evaluate_quota_readiness
                evaluate_quota_readiness(
                    executor_model=EXECUTOR_MODEL,
                    planner_model=PLANNER_MODEL_PRIMARY
                )
            except Exception:
                pass

            # Garbage Collection
            try:
                import gc
                gc.collect()
            except Exception:
                pass

        except Exception as fe:
            print(f"[-] Error in fleet supervisor loop: {fe}")

        _SHUTDOWN_EVENT.wait(timeout=10.0)
    
    # Clean shutdown of all pods
    for rid, pod in list(ACTIVE_PODS.items()):
        pod.stop()
    print("[*] Fleet Supervisor stopped.")

# ==============================================================================
# 🔄 MAIN MULTI-REPO COORDINATOR ENTRYPOINT
# ==============================================================================
def run_orchestrator(once: bool = False, planner_only: bool = False, executor_only: bool = False):
    print("=" * 80)
    print(" 🚀 UNIVERSAL MULTI-REPOSITORY AUTONOMOUS ORCHESTRATOR")
    print(f"   Architecture:      Decentralized Per-Repository Actor Pods")
    print(f"   Strategic Planner: {PLANNER_MODEL_PRIMARY} (Fallback: {PLANNER_MODEL_FALLBACK})")
    print(f"   Task Executor:     {EXECUTOR_MODEL}")
    print(f"   Registry:          {REPOS_FILE}")
    print(f"   Cadence Control:   {DASHBOARDS_DIR / 'cadence_state.json'}")
    print("=" * 80)

    # CLI one-off execution mode (--once, --planner, --executor)
    if once or planner_only or executor_only:
        repos = load_repositories()
        if not repos:
            print("[-] No repositories found in registry.")
            return
        if not executor_only:
            run_planner_pass(repos)
        if not planner_only:
            run_executor_pass(repos)
        print("\n[*] Single orchestrator pass complete. Exiting.")
        return

    # Production Decentralized Fleet Supervisor Mode
    supervisor_thread = threading.Thread(target=_fleet_supervisor_loop, name="FleetSupervisor", daemon=True)
    supervisor_thread.start()

    print("\n" + "=" * 80)
    print(" 🚀 DECENTRALIZED MULTI-REPO FLEET RUNTIME INITIALIZED:")
    print("   • Each active repository runs its own dedicated Planner & Executor Pod")
    print("   • Cloud Token Multiplexing handled concurrently via Antigravity Router (8045/7836)")
    print("   • Dynamic Cadence & External Control Plane active at dashboards/cadence_state.json")
    print("=" * 80 + "\n")

    try:
        while not _SHUTDOWN_EVENT.is_set():
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\n[-] Orchestrator stopping (Ctrl+C). Signaling fleet pods...")
        _SHUTDOWN_EVENT.set()
        supervisor_thread.join(timeout=3.0)
        print("[*] All fleet actor pods decommissioned cleanly.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Universal Multi-Repository Autonomous Orchestrator")
    parser.add_argument("--once", action="store_true", help="Run one pass across repos and exit")
    parser.add_argument("--planner", action="store_true", help="Run only the Opus strategic planner pass")
    parser.add_argument("--executor", action="store_true", help="Run only the Gemini prioritized executor pass")
    args = parser.parse_args()
    
    run_orchestrator(once=args.once, planner_only=args.planner, executor_only=args.executor)
