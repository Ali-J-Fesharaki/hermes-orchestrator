#!/usr/bin/env python3
"""
Transparent Task Execution State Reasoner & Cockpit Diagnostics
==============================================================
Module: tasks/execution_state_reasoner.py
Repository: hermes_orchestrator

Multi-Factor State Space Model:
  S_i = <m_i, r_j, H, G, C>
  where:
    - m_i : Milestone descriptor (id, title, status, relative_filepath, approved_for_auto_run, ...)
    - r_j : Repository configuration (name, path, auto_approve_plans, max_concurrency, ...)
    - H   : Fleet Health Sentinel snapshot (gateway router reachability, account statuses)
    - G   : Rate Governor dynamic state (token backoff, window cooldown, remaining seconds)
    - C   : Active worker concurrency table (active worker count, pool limits, locked files)

Diagnostic Decision Cascade:
  R(m_i) = first_match(Cascade):
    1. H[gateway][online] == False              -> GATEWAY_OFFLINE
    2. H[accounts][active_count] == 0           -> ACCOUNTS_QUARANTINED
    3. m_i[status] == in_progress && worker_alive -> EXECUTING
    4. !m_i[approved] && !r_j[auto_approve]     -> WAITING_HUMAN_APPROVAL
    5. m_i[relative_filepath] in C[locked_files]-> TARGET_FILE_COLLISION
    6. G[is_cooling_down]                       -> RATE_GOVERNOR_BACKOFF
    7. C[active_workers] >= C[max_concurrency]  -> WORKER_POOL_DEPLETED
    8. Default                                  -> READY_FOR_DISPATCH

Zero circular dependencies. Fully compliant with Python 3 standard library.
"""

from __future__ import annotations

import os
import sys
import json
import time
import socket
from dataclasses import dataclass, field, asdict
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any, Union


# ==============================================================================
# Reason Codes & Diagnostic Schema
# ==============================================================================

class TaskReasonCode(str, Enum):
    """Categorized diagnostic codes for milestone execution state reasoning."""
    GATEWAY_OFFLINE = "GATEWAY_OFFLINE"
    ACCOUNTS_QUARANTINED = "ACCOUNTS_QUARANTINED"
    EXECUTING = "EXECUTING"
    WAITING_HUMAN_APPROVAL = "WAITING_HUMAN_APPROVAL"
    TARGET_FILE_COLLISION = "TARGET_FILE_COLLISION"
    RATE_GOVERNOR_BACKOFF = "RATE_GOVERNOR_BACKOFF"
    WORKER_POOL_DEPLETED = "WORKER_POOL_DEPLETED"
    READY_FOR_DISPATCH = "READY_FOR_DISPATCH"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


# Metadata lookup table mapping reason codes to display properties and mitigation logic
REASON_METADATA: Dict[TaskReasonCode, Dict[str, Any]] = {
    TaskReasonCode.GATEWAY_OFFLINE: {
        "category": "INFRASTRUCTURE",
        "badge_text": "Gateway Offline",
        "badge_color": "#dc2626",  # Red-600
        "can_auto_resolve": False,
        "actionable_fix": "LLM Router unreachable on ports 8045/7836. Restart gateway daemon or check proxy routing.",
    },
    TaskReasonCode.ACCOUNTS_QUARANTINED: {
        "category": "SECURITY",
        "badge_text": "Accounts Quarantined",
        "badge_color": "#ea580c",  # Orange-600
        "can_auto_resolve": False,
        "actionable_fix": "Re-auth required for cloud accounts. Inspect ~/.antigravity-agent/cloud_accounts.db.",
    },
    TaskReasonCode.EXECUTING: {
        "category": "RUNTIME",
        "badge_text": "Executing",
        "badge_color": "#2563eb",  # Blue-600
        "can_auto_resolve": True,
        "actionable_fix": "Worker process is actively generating or testing artifacts.",
    },
    TaskReasonCode.WAITING_HUMAN_APPROVAL: {
        "category": "POLICY",
        "badge_text": "Waiting Human Approval",
        "badge_color": "#f59e0b",  # Amber-500
        "can_auto_resolve": False,
        "actionable_fix": "Click to authorize execution in Cockpit, or enable auto_approve_plans in repo config.",
    },
    TaskReasonCode.TARGET_FILE_COLLISION: {
        "category": "CONCURRENCY",
        "badge_text": "Target File Collision",
        "badge_color": "#7c3aed",  # Purple-600
        "can_auto_resolve": True,
        "actionable_fix": "Locked by concurrent worker. Task will dispatch automatically once the active checkout is released.",
    },
    TaskReasonCode.RATE_GOVERNOR_BACKOFF: {
        "category": "RATE_LIMIT",
        "badge_text": "Rate Governor Backoff",
        "badge_color": "#d97706",  # Amber-600
        "can_auto_resolve": True,
        "actionable_fix": "Rate governor cooldown window active. Throttling dispatches to prevent upstream HTTP 429.",
    },
    TaskReasonCode.WORKER_POOL_DEPLETED: {
        "category": "CONCURRENCY",
        "badge_text": "Worker Pool Depleted",
        "badge_color": "#64748b",  # Slate-500
        "can_auto_resolve": True,
        "actionable_fix": "Maximum concurrent agent workers active. Task is queued for the next available slot.",
    },
    TaskReasonCode.READY_FOR_DISPATCH: {
        "category": "SCHEDULING",
        "badge_text": "Ready for Dispatch",
        "badge_color": "#16a34a",  # Green-600
        "can_auto_resolve": True,
        "actionable_fix": "All preconditions satisfied. Task is eligible for immediate autonomous pick-up.",
    },
    TaskReasonCode.COMPLETED: {
        "category": "LIFECYCLE",
        "badge_text": "Completed",
        "badge_color": "#059669",  # Emerald-600
        "can_auto_resolve": True,
        "actionable_fix": "Milestone delivered, committed, and verified. No further action needed.",
    },
    TaskReasonCode.FAILED: {
        "category": "LIFECYCLE",
        "badge_text": "Execution Failed",
        "badge_color": "#b91c1c",  # Red-700
        "can_auto_resolve": False,
        "actionable_fix": "Execution failed verification tests or encountered runtime errors. Review logs to retry.",
    },
}


@dataclass
class TaskDiagnostic:
    """Structured diagnostic token schema for task execution state reasoner."""
    milestone_id: str
    milestone_title: str
    repo_name: str
    reason_code: str
    category: str
    badge_text: str
    badge_color: str
    can_auto_resolve: bool
    actionable_fix: str
    details: Dict[str, Any] = field(default_factory=dict)
    evaluated_at: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        """Convert diagnostic record into a JSON-serializable dictionary."""
        return asdict(self)


# ==============================================================================
# Hardware, Network, and Environment Sensor Hooks
# ==============================================================================

def check_socket_open(host: str, port: int, timeout: float = 0.35) -> bool:
    """Test TCP port availability with strict timeout."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, ConnectionRefusedError, socket.timeout):
        return False


def is_pid_alive(pid: Optional[int]) -> bool:
    """Check if a Unix process ID is alive and accessible."""
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def get_default_workspace_root() -> Path:
    """Locate current repository or automation workspace root."""
    cwd = Path(os.getcwd()).resolve()
    for candidate in [cwd, cwd.parent, Path("/home/ajf/automation")]:
        if (candidate / "dashboards").is_dir() or (candidate / "tasks").is_dir():
            return candidate
    return cwd


# ==============================================================================
# Multi-Factor State Snapshot Resolvers (H, G, C)
# ==============================================================================

def resolve_fleet_health_snapshot(explicit_snapshot: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Resolve Fleet Health Sentinel snapshot (H).
    Checks ports 8045 and 7836, and queries fleet_health_sentinel or fallback cache.
    """
    if explicit_snapshot is not None:
        return explicit_snapshot

    workspace = get_default_workspace_root()
    cache_file = workspace / "dashboards" / "fleet_health_state.json"

    # 1. Attempt cached file read
    if cache_file.is_file():
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict) and "gateway" in data and "accounts" in data:
                    return data
        except Exception:
            pass

    # 2. Real-time fallback inspection
    p8045 = check_socket_open("127.0.0.1", 8045)
    p7836 = check_socket_open("127.0.0.1", 7836)
    gateway_online = p8045 or p7836

    # Verify cloud accounts presence
    cloud_db = Path(os.path.expanduser("~/.antigravity-agent/cloud_accounts.db"))
    active_accounts = 4 if cloud_db.exists() else 0

    return {
        "gateway": {
            "online": gateway_online,
            "ports": [8045, 7836],
            "p8045_active": p8045,
            "p7836_active": p7836,
        },
        "accounts": {
            "active_count": active_accounts,
            "quarantined_count": 0 if active_accounts > 0 else 4,
            "total_count": 4,
        },
        "source": "live_fallback_probe",
    }


def resolve_rate_governor_state(explicit_state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Resolve Rate Governor dynamic state (G).
    Reads dashboards/rate_governor_state.json if available.
    """
    if explicit_state is not None:
        return explicit_state

    workspace = get_default_workspace_root()
    gov_file = workspace / "dashboards" / "rate_governor_state.json"

    if gov_file.is_file():
        try:
            with open(gov_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                now = time.time()
                cooldown_until = data.get("cooldown_until", 0.0)
                is_cooling = data.get("is_cooling_down", False) or (cooldown_until > now)
                remaining = max(0.0, round(cooldown_until - now, 1)) if is_cooling else 0.0
                return {
                    "is_cooling_down": is_cooling,
                    "remaining": remaining,
                    "model": data.get("current_model", "gemini-3.8-flash-tiered"),
                    "requests_per_minute": data.get("requests_per_minute", 0),
                }
        except Exception:
            pass

    return {
        "is_cooling_down": False,
        "remaining": 0.0,
        "model": "gemini-3.8-flash-tiered",
        "requests_per_minute": 0,
    }


def detect_active_file_locks(
    workspace_root: Optional[Union[str, Path]] = None,
    concurrency_state: Optional[Dict[str, Any]] = None,
) -> Set[str]:
    """
    Detect file collision locks across active git checkouts and running workers.
    Inspects:
      - concurrency_state["locked_files"]
      - workspace / .hermes / locks / *.lock
      - workspace / dashboards / active_workers.json
      - git index.lock files
    """
    locked: Set[str] = set()

    if concurrency_state and "locked_files" in concurrency_state:
        for item in concurrency_state["locked_files"]:
            locked.add(str(item).strip())

    root = Path(workspace_root) if workspace_root else get_default_workspace_root()

    # 1. Lock file directory
    lock_dir = root / ".hermes" / "locks"
    if lock_dir.is_dir():
        for lock_file in lock_dir.glob("*.lock"):
            rel_name = lock_file.stem.replace("__", "/")
            locked.add(rel_name)

    # 2. Active workers descriptor
    workers_file = root / "dashboards" / "active_workers.json"
    if workers_file.is_file():
        try:
            with open(workers_file, "r", encoding="utf-8") as f:
                worker_data = json.load(f)
                for w in worker_data.get("workers", []):
                    target = w.get("relative_filepath") or w.get("target_file")
                    pid = w.get("pid")
                    if target and (pid is None or is_pid_alive(pid)):
                        locked.add(str(target).strip())
        except Exception:
            pass

    # 3. Git index.lock detection
    for git_lock in root.glob("**/.git/index.lock"):
        locked.add(f"git:{git_lock.parent.parent.name}")

    return locked


def resolve_worker_concurrency(
    explicit_concurrency: Optional[Dict[str, Any]] = None,
    workspace_root: Optional[Union[str, Path]] = None,
) -> Dict[str, Any]:
    """
    Resolve active worker concurrency table (C).
    """
    if explicit_concurrency is not None:
        state = dict(explicit_concurrency)
        if "locked_files" in state and isinstance(state["locked_files"], list):
            state["locked_files"] = set(state["locked_files"])
        elif "locked_files" not in state:
            state["locked_files"] = set()
        return state

    root = Path(workspace_root) if workspace_root else get_default_workspace_root()
    active_locks = detect_active_file_locks(root)

    active_count = 0
    max_concurrency = 4
    running_milestones = {}

    workers_file = root / "dashboards" / "active_workers.json"
    if workers_file.is_file():
        try:
            with open(workers_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                max_concurrency = data.get("max_concurrency", 4)
                for w in data.get("workers", []):
                    pid = w.get("pid")
                    if pid is None or is_pid_alive(pid):
                        active_count += 1
                        m_id = w.get("milestone_id")
                        if m_id:
                            running_milestones[m_id] = w
        except Exception:
            pass

    return {
        "active_workers": active_count,
        "max_concurrency": max_concurrency,
        "locked_files": active_locks,
        "running_milestones": running_milestones,
    }


# ==============================================================================
# Decision Cascade Engine
# ==============================================================================

def evaluate_task_diagnostic(
    milestone: Dict[str, Any],
    repo: Dict[str, Any],
    active_locks: Optional[Set[str]] = None,
    fleet_health: Optional[Dict[str, Any]] = None,
    governor_state: Optional[Dict[str, Any]] = None,
    concurrency_state: Optional[Dict[str, Any]] = None,
) -> TaskDiagnostic:
    """
    Evaluate the diagnostic state for a single milestone m_i under repo r_j using the
    strict Diagnostic Decision Cascade:

      Cascade order:
        0. Lifecycle status check (COMPLETED / FAILED)
        1. Gateway offline?
        2. Accounts quarantined?
        3. Currently executing with live worker?
        4. Awaiting human approval?
        5. Target file collision / concurrent lock?
        6. Rate governor backoff active?
        7. Concurrency pool depleted?
        8. Default -> READY_FOR_DISPATCH
    """
    m_id = str(milestone.get("id") or milestone.get("milestone_id") or "unspecified_milestone")
    m_title = str(milestone.get("title") or milestone.get("name") or "Untitled Milestone")
    repo_name = str(repo.get("name") or repo.get("repo_name") or "default_repo")
    m_status = str(milestone.get("status", "pending")).lower().strip()

    # Pre-step: Terminal lifecycle states
    if m_status in ("completed", "done", "success"):
        meta = REASON_METADATA[TaskReasonCode.COMPLETED]
        return TaskDiagnostic(
            milestone_id=m_id,
            milestone_title=m_title,
            repo_name=repo_name,
            reason_code=TaskReasonCode.COMPLETED.value,
            category=meta["category"],
            badge_text=meta["badge_text"],
            badge_color=meta["badge_color"],
            can_auto_resolve=meta["can_auto_resolve"],
            actionable_fix=meta["actionable_fix"],
            details={"status": m_status},
        )

    if m_status in ("failed", "error", "aborted"):
        meta = REASON_METADATA[TaskReasonCode.FAILED]
        return TaskDiagnostic(
            milestone_id=m_id,
            milestone_title=m_title,
            repo_name=repo_name,
            reason_code=TaskReasonCode.FAILED.value,
            category=meta["category"],
            badge_text=meta["badge_text"],
            badge_color=meta["badge_color"],
            can_auto_resolve=meta["can_auto_resolve"],
            actionable_fix=meta["actionable_fix"],
            details={"status": m_status, "error": milestone.get("error_message")},
        )

    # Resolve State Space Model components
    H = resolve_fleet_health_snapshot(fleet_health)
    G = resolve_rate_governor_state(governor_state)
    C = resolve_worker_concurrency(concurrency_state)

    combined_locks = set(C.get("locked_files", set()))
    if active_locks:
        combined_locks.update(active_locks)

    # --------------------------------------------------------------------------
    # Step 1: Gateway Online Verification
    # --------------------------------------------------------------------------
    gateway_online = H.get("gateway", {}).get("online", True)
    if not gateway_online:
        meta = REASON_METADATA[TaskReasonCode.GATEWAY_OFFLINE]
        ports = H.get("gateway", {}).get("ports", [8045, 7836])
        return TaskDiagnostic(
            milestone_id=m_id,
            milestone_title=m_title,
            repo_name=repo_name,
            reason_code=TaskReasonCode.GATEWAY_OFFLINE.value,
            category=meta["category"],
            badge_text=meta["badge_text"],
            badge_color=meta["badge_color"],
            can_auto_resolve=meta["can_auto_resolve"],
            actionable_fix=f"LLM Router unreachable on ports {ports[0]}/{ports[1]}. Restart gateway daemon.",
            details={"gateway_snapshot": H.get("gateway")},
        )

    # --------------------------------------------------------------------------
    # Step 2: Account Health & Quarantined Check
    # --------------------------------------------------------------------------
    active_accounts = H.get("accounts", {}).get("active_count", 1)
    if active_accounts <= 0:
        meta = REASON_METADATA[TaskReasonCode.ACCOUNTS_QUARANTINED]
        return TaskDiagnostic(
            milestone_id=m_id,
            milestone_title=m_title,
            repo_name=repo_name,
            reason_code=TaskReasonCode.ACCOUNTS_QUARANTINED.value,
            category=meta["category"],
            badge_text=meta["badge_text"],
            badge_color=meta["badge_color"],
            can_auto_resolve=meta["can_auto_resolve"],
            actionable_fix="Re-auth required for cloud accounts in ~/.antigravity-agent/cloud_accounts.db.",
            details={"accounts_snapshot": H.get("accounts")},
        )

    # --------------------------------------------------------------------------
    # Step 3: Worker Active & Executing Check
    # --------------------------------------------------------------------------
    worker_pid = milestone.get("worker_pid") or milestone.get("pid")
    is_in_progress = m_status in ("in_progress", "executing", "running")

    # Also inspect C[running_milestones]
    running_info = C.get("running_milestones", {}).get(m_id)
    if running_info and not worker_pid:
        worker_pid = running_info.get("pid")
        is_in_progress = True

    if is_in_progress:
        # If PID is given, verify liveness; if no PID specified, assume active if flagged
        alive = is_pid_alive(worker_pid) if worker_pid else True
        if alive:
            meta = REASON_METADATA[TaskReasonCode.EXECUTING]
            pid_label = f"PID {worker_pid}" if worker_pid else "active process"
            return TaskDiagnostic(
                milestone_id=m_id,
                milestone_title=m_title,
                repo_name=repo_name,
                reason_code=TaskReasonCode.EXECUTING.value,
                category=meta["category"],
                badge_text=f"Worker active ({pid_label})",
                badge_color=meta["badge_color"],
                can_auto_resolve=meta["can_auto_resolve"],
                actionable_fix=f"Worker active on PID {worker_pid or 'N/A'}. Monitoring real-time telemetry.",
                details={"pid": worker_pid, "started_at": milestone.get("started_at")},
            )

    # --------------------------------------------------------------------------
    # Step 4: Policy & Human Approval Gate
    # --------------------------------------------------------------------------
    approved_for_auto_run = bool(milestone.get("approved_for_auto_run", False))
    auto_approve_plans = bool(repo.get("auto_approve_plans", False))

    if not approved_for_auto_run and not auto_approve_plans:
        meta = REASON_METADATA[TaskReasonCode.WAITING_HUMAN_APPROVAL]
        return TaskDiagnostic(
            milestone_id=m_id,
            milestone_title=m_title,
            repo_name=repo_name,
            reason_code=TaskReasonCode.WAITING_HUMAN_APPROVAL.value,
            category=meta["category"],
            badge_text=meta["badge_text"],
            badge_color=meta["badge_color"],
            can_auto_resolve=meta["can_auto_resolve"],
            actionable_fix="Click to authorize milestone plan in Cockpit to dispatch execution.",
            details={"approved_for_auto_run": False, "auto_approve_plans": False},
        )

    # --------------------------------------------------------------------------
    # Step 5: Target File Collision & Concurrency Lock
    # --------------------------------------------------------------------------
    target_file = (
        milestone.get("relative_filepath")
        or milestone.get("target_file")
        or milestone.get("file")
    )
    if target_file:
        target_norm = str(target_file).strip()
        repo_prefixed = f"{repo_name}/{target_norm}"
        if target_norm in combined_locks or repo_prefixed in combined_locks:
            meta = REASON_METADATA[TaskReasonCode.TARGET_FILE_COLLISION]
            return TaskDiagnostic(
                milestone_id=m_id,
                milestone_title=m_title,
                repo_name=repo_name,
                reason_code=TaskReasonCode.TARGET_FILE_COLLISION.value,
                category=meta["category"],
                badge_text=meta["badge_text"],
                badge_color=meta["badge_color"],
                can_auto_resolve=meta["can_auto_resolve"],
                actionable_fix=f"Target file '{target_norm}' is locked by a concurrent worker. Awaiting lock release.",
                details={"target_file": target_norm, "locked_files": list(combined_locks)},
            )

    # --------------------------------------------------------------------------
    # Step 6: Rate Governor Dynamic Backoff
    # --------------------------------------------------------------------------
    if G.get("is_cooling_down", False):
        remaining = G.get("remaining", 0.0)
        meta = REASON_METADATA[TaskReasonCode.RATE_GOVERNOR_BACKOFF]
        return TaskDiagnostic(
            milestone_id=m_id,
            milestone_title=m_title,
            repo_name=repo_name,
            reason_code=TaskReasonCode.RATE_GOVERNOR_BACKOFF.value,
            category=meta["category"],
            badge_text=f"Rate Backoff ({remaining:.0f}s)",
            badge_color=meta["badge_color"],
            can_auto_resolve=meta["can_auto_resolve"],
            actionable_fix=f"Cooldown active: {remaining:.1f}s remaining. Throttling dispatches to prevent rate limits.",
            details={"cooling_down": True, "remaining_seconds": remaining, "model": G.get("model")},
        )

    # --------------------------------------------------------------------------
    # Step 7: Worker Pool Concurrency Depletion
    # --------------------------------------------------------------------------
    active_workers = C.get("active_workers", 0)
    max_concurrency = C.get("max_concurrency", 4)
    if active_workers >= max_concurrency and max_concurrency > 0:
        meta = REASON_METADATA[TaskReasonCode.WORKER_POOL_DEPLETED]
        return TaskDiagnostic(
            milestone_id=m_id,
            milestone_title=m_title,
            repo_name=repo_name,
            reason_code=TaskReasonCode.WORKER_POOL_DEPLETED.value,
            category=meta["category"],
            badge_text=f"Pool Depleted ({active_workers}/{max_concurrency})",
            badge_color=meta["badge_color"],
            can_auto_resolve=meta["can_auto_resolve"],
            actionable_fix=f"All {max_concurrency} worker slots occupied. Queued for immediate pick-up upon slot release.",
            details={"active_workers": active_workers, "max_concurrency": max_concurrency},
        )

    # --------------------------------------------------------------------------
    # Step 8: Default Ready For Dispatch
    # --------------------------------------------------------------------------
    meta = REASON_METADATA[TaskReasonCode.READY_FOR_DISPATCH]
    return TaskDiagnostic(
        milestone_id=m_id,
        milestone_title=m_title,
        repo_name=repo_name,
        reason_code=TaskReasonCode.READY_FOR_DISPATCH.value,
        category=meta["category"],
        badge_text=meta["badge_text"],
        badge_color=meta["badge_color"],
        can_auto_resolve=meta["can_auto_resolve"],
        actionable_fix=meta["actionable_fix"],
        details={"active_workers": active_workers, "max_concurrency": max_concurrency},
    )


# ==============================================================================
# Multi-Repository Batch Reasoner & Cockpit API Adapter
# ==============================================================================

def diagnose_all_repositories(
    repos: List[Dict[str, Any]],
    fleet_health: Optional[Dict[str, Any]] = None,
    governor_state: Optional[Dict[str, Any]] = None,
    concurrency_state: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Evaluate diagnostic state reasons across all configured repositories and milestones.
    Provides complete state breakdown, reason aggregations, and badge metadata.
    """
    # Pre-resolve shared environment state once for efficiency
    H = resolve_fleet_health_snapshot(fleet_health)
    G = resolve_rate_governor_state(governor_state)
    C = resolve_worker_concurrency(concurrency_state)
    shared_locks = detect_active_file_locks(concurrency_state=C)

    summary_counts: Dict[str, int] = {code.value: 0 for code in TaskReasonCode}
    repo_results: Dict[str, Dict[str, Any]] = {}
    total_milestones = 0

    for repo in repos:
        repo_name = repo.get("name") or repo.get("repo_name") or "unnamed_repo"
        milestones = repo.get("milestones", [])
        milestone_diagnostics: List[Dict[str, Any]] = []

        for m in milestones:
            total_milestones += 1
            diag = evaluate_task_diagnostic(
                milestone=m,
                repo=repo,
                active_locks=shared_locks,
                fleet_health=H,
                governor_state=G,
                concurrency_state=C,
            )
            diag_dict = diag.to_dict()
            milestone_diagnostics.append(diag_dict)
            summary_counts[diag.reason_code] = summary_counts.get(diag.reason_code, 0) + 1

        repo_results[repo_name] = {
            "name": repo_name,
            "path": repo.get("path", ""),
            "auto_approve_plans": repo.get("auto_approve_plans", False),
            "milestone_count": len(milestones),
            "diagnostics": milestone_diagnostics,
        }

    return {
        "timestamp": time.time(),
        "total_repositories": len(repos),
        "total_milestones": total_milestones,
        "summary": summary_counts,
        "environment": {
            "gateway_online": H.get("gateway", {}).get("online", True),
            "active_accounts": H.get("accounts", {}).get("active_count", 0),
            "is_cooling_down": G.get("is_cooling_down", False),
            "rate_cooldown_remaining": G.get("remaining", 0.0),
            "active_workers": C.get("active_workers", 0),
            "max_concurrency": C.get("max_concurrency", 4),
            "locked_files_count": len(shared_locks),
        },
        "repositories": repo_results,
    }


def evaluate_task_state_reasons(repos: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Public entry point exposed for Hermes Cockpit API / UI consumption.
    Formats real-time diagnostic reasons for front-end badges and actionable dispatch controls.
    """
    return diagnose_all_repositories(repos)


# ==============================================================================
# Exhaustive Self-Testing Suite
# ==============================================================================

def _run_self_tests() -> None:
    """
    Comprehensive verification suite exercising all cascade transitions and edge cases:
      1. Gateway offline
      2. Quarantined accounts
      3. Active worker execution
      4. Unapproved task (human gate)
      5. Target file collision
      6. Rate governor backoff
      7. Worker pool depleted
      8. Healthy ready-for-dispatch
    """
    print("================================================================================")
    print("RUNNING EXHAUSTIVE TEST SUITE: Execution State Reasoner & Cockpit Diagnostics")
    print("================================================================================")

    # Baseline healthy environment
    healthy_H = {
        "gateway": {"online": True, "ports": [8045, 7836]},
        "accounts": {"active_count": 4, "quarantined_count": 0},
    }
    healthy_G = {"is_cooling_down": False, "remaining": 0.0}
    healthy_C = {"active_workers": 1, "max_concurrency": 4, "locked_files": set()}

    test_cases = [
        # 1. Gateway Offline
        {
            "name": "Cascade 1: Gateway Offline",
            "milestone": {"id": "m1", "title": "Implement parser", "approved_for_auto_run": True},
            "repo": {"name": "core", "auto_approve_plans": True},
            "H": {"gateway": {"online": False, "ports": [8045, 7836]}, "accounts": {"active_count": 4}},
            "G": healthy_G,
            "C": healthy_C,
            "expected_code": TaskReasonCode.GATEWAY_OFFLINE,
        },
        # 2. Accounts Quarantined
        {
            "name": "Cascade 2: Accounts Quarantined",
            "milestone": {"id": "m2", "title": "Run regression", "approved_for_auto_run": True},
            "repo": {"name": "core", "auto_approve_plans": True},
            "H": {"gateway": {"online": True}, "accounts": {"active_count": 0, "quarantined_count": 4}},
            "G": healthy_G,
            "C": healthy_C,
            "expected_code": TaskReasonCode.ACCOUNTS_QUARANTINED,
        },
        # 3. Executing with Alive Worker
        {
            "name": "Cascade 3: Worker Active & Executing",
            "milestone": {
                "id": "m3",
                "title": "Refactor router",
                "status": "in_progress",
                "pid": os.getpid(),  # Current process is definitely alive
                "approved_for_auto_run": True,
            },
            "repo": {"name": "core", "auto_approve_plans": True},
            "H": healthy_H,
            "G": healthy_G,
            "C": healthy_C,
            "expected_code": TaskReasonCode.EXECUTING,
        },
        # 4. Waiting Human Approval
        {
            "name": "Cascade 4: Waiting Human Approval",
            "milestone": {
                "id": "m4",
                "title": "Deploy schema",
                "approved_for_auto_run": False,
            },
            "repo": {"name": "core", "auto_approve_plans": False},
            "H": healthy_H,
            "G": healthy_G,
            "C": healthy_C,
            "expected_code": TaskReasonCode.WAITING_HUMAN_APPROVAL,
        },
        # 5. Target File Collision
        {
            "name": "Cascade 5: Target File Collision",
            "milestone": {
                "id": "m5",
                "title": "Update settings",
                "relative_filepath": "config/settings.py",
                "approved_for_auto_run": True,
            },
            "repo": {"name": "core", "auto_approve_plans": True},
            "H": healthy_H,
            "G": healthy_G,
            "C": {
                "active_workers": 1,
                "max_concurrency": 4,
                "locked_files": {"config/settings.py"},
            },
            "expected_code": TaskReasonCode.TARGET_FILE_COLLISION,
        },
        # 6. Rate Governor Backoff
        {
            "name": "Cascade 6: Rate Governor Backoff",
            "milestone": {"id": "m6", "title": "Bulk synthesize docs", "approved_for_auto_run": True},
            "repo": {"name": "core", "auto_approve_plans": True},
            "H": healthy_H,
            "G": {"is_cooling_down": True, "remaining": 28.5},
            "C": healthy_C,
            "expected_code": TaskReasonCode.RATE_GOVERNOR_BACKOFF,
        },
        # 7. Worker Pool Depleted
        {
            "name": "Cascade 7: Worker Pool Depleted",
            "milestone": {"id": "m7", "title": "Train fast heuristic", "approved_for_auto_run": True},
            "repo": {"name": "core", "auto_approve_plans": True},
            "H": healthy_H,
            "G": healthy_G,
            "C": {"active_workers": 4, "max_concurrency": 4, "locked_files": set()},
            "expected_code": TaskReasonCode.WORKER_POOL_DEPLETED,
        },
        # 8. Ready For Dispatch
        {
            "name": "Cascade 8: Ready For Dispatch",
            "milestone": {
                "id": "m8",
                "title": "Unit tests verification",
                "approved_for_auto_run": True,
                "relative_filepath": "tests/test_router.py",
            },
            "repo": {"name": "core", "auto_approve_plans": True},
            "H": healthy_H,
            "G": healthy_G,
            "C": healthy_C,
            "expected_code": TaskReasonCode.READY_FOR_DISPATCH,
        },
    ]

    all_passed = True

    for tc in test_cases:
        diag = evaluate_task_diagnostic(
            milestone=tc["milestone"],
            repo=tc["repo"],
            fleet_health=tc["H"],
            governor_state=tc["G"],
            concurrency_state=tc["C"],
        )
        passed = (diag.reason_code == tc["expected_code"].value)
        status_sym = "[PASS]" if passed else "[FAIL]"
        if not passed:
            all_passed = False
        print(f"{status_sym} {tc['name']}")
        print(f"       -> Expected: {tc['expected_code'].value}")
        print(f"       -> Actual:   {diag.reason_code}")
        print(f"       -> Badge:    [{diag.badge_text}] ({diag.badge_color})")
        print(f"       -> Fix:      {diag.actionable_fix}")
        print()

    # Verify Multi-Repository Batch API evaluation
    sample_repos = [
        {
            "name": "automation_hub",
            "auto_approve_plans": False,
            "milestones": [
                {"id": "ah-1", "title": "Unapproved Task", "approved_for_auto_run": False},
                {"id": "ah-2", "title": "Approved Task", "approved_for_auto_run": True},
            ],
        },
        {
            "name": "v2ray_tool",
            "auto_approve_plans": True,
            "milestones": [
                {"id": "vt-1", "title": "Auto-Approved Ready Task", "approved_for_auto_run": False},
            ],
        },
    ]

    batch_output = evaluate_task_state_reasons(sample_repos)
    assert batch_output["total_repositories"] == 2, "Batch repo count mismatch"
    assert batch_output["total_milestones"] == 3, "Batch milestone count mismatch"

    print("================================================================================")
    print(f"BATCH DIAGNOSTICS SUMMARY:")
    print(json.dumps(batch_output["summary"], indent=2))
    print("================================================================================")

    if all_passed:
        print("ALL DIAGNOSTIC REASONING TESTS PASSED PERFECTLY.")
    else:
        print("FAILURES DETECTED IN TEST SUITE.")
        sys.exit(1)


if __name__ == "__main__":
    _run_self_tests()