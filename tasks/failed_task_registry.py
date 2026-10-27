#!/usr/bin/env python3
"""
failed_task_registry.py: Structured Failure Registry & Bugfix Agent Dispatch Queue.

Stores failed execution tasks with:
  - Unique Task ID (e.g. 'fail_phd_a9f1b2')
  - Repo ID & Repo Name
  - Milestone Title & Target File
  - Structured Tags (e.g. ['latex', 'chapter4', 'xelatex_compilation_error', 'syntax_error'])
  - Failure Category & Exact Error Reason / Stack Trace
  - Model Used & Timestamp
  - Anti-Pattern ID & Taxonomic Label (from PostMortemPedagogyEngine)
  - Pedagogical Lesson (Broken Assumption, Countermeasure, Rule of Thumb)
  - Remediation Status ('pending_fix', 'in_progress', 'resolved')
  - Fix Commit Hash & Remediation Notes

Outputs:
  - JSON Registry: dashboards/failed_tasks_registry.json (Machine-readable for agents)
  - Markdown Report: dashboards/FAILED_TASKS_REPORT.md (Agent-readable high-density summary)
"""

from __future__ import annotations

import os
import sys
import json
import re
import hashlib
import threading
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

AUTO_DIR = Path(__file__).resolve().parent.parent
DASHBOARDS_DIR = AUTO_DIR / "dashboards"
FAILED_TASKS_FILE = DASHBOARDS_DIR / "failed_tasks_registry.json"
FAILED_REPORT_FILE = DASHBOARDS_DIR / "FAILED_TASKS_REPORT.md"
REPOS_FILE = DASHBOARDS_DIR / "repositories.json"

for _p in [str(AUTO_DIR), str(DASHBOARDS_DIR)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

# Defensive Import of PostMortemPedagogyEngine
try:
    from dashboards.post_mortem_pedagogy import PostMortemPedagogyEngine
except ImportError:
    try:
        from post_mortem_pedagogy import PostMortemPedagogyEngine  # type: ignore
    except ImportError:
        PostMortemPedagogyEngine = None

_REGISTRY_LOCK = threading.Lock()


def generate_task_id(repo_id: str, milestone_name: str) -> str:
    """Generates a stable, unique task ID based on repo and milestone name."""
    clean_repo = re.sub(r"[^a-zA-Z0-9]", "", repo_id.lower())[:8] or "task"
    h = hashlib.sha256(milestone_name.strip().encode("utf-8")).hexdigest()[:6]
    return f"fail_{clean_repo}_{h}"


def infer_tags(repo_id: str, milestone_name: str, target_file: str, failure_reason: str) -> list[str]:
    """
    Infers fine-grained categorical and contextual tags to assist bug-fixing agents:
      - Tech Stack / Domain: latex, python, ros2, drl, costmap, mechatronics, nav2
      - Component Target: chapter1, chapter2, chapter3, chapter4, main_tex, simulation
      - Failure Mode: xelatex_error, syntax_error, empty_response, tool_call_error, auth_error
    """
    tags = set()
    combined_text = f"{repo_id} {milestone_name} {target_file} {failure_reason}".lower()

    # 1. Tech Stack / Domain
    if any(k in combined_text for k in [".tex", "latex", "xelatex", "bibtex", "proposal"]):
        tags.add("latex")
    if any(k in combined_text for k in [".py", "python", "py_compile", "syntaxerror"]):
        tags.add("python")
    if any(k in combined_text for k in ["ros2", "nav2", "costmap", "sensor_msgs"]):
        tags.add("ros2")
    if any(k in combined_text for k in ["drl", "reinforcement", "policy", "reward", "q-learning"]):
        tags.add("drl")
    if any(k in combined_text for k in ["mechatronic", "wheelchair", "sensor ring", "ultrasonic", "lidar"]):
        tags.add("mechatronics")
    if any(k in combined_text for k in ["simulation", "gazebo", "stage", "benchmark", "pedestrian"]):
        tags.add("simulation")

    # 2. File / Structural Component
    for ch in ["chapter1", "chapter2", "chapter3", "chapter4", "chapter5", "chapter6"]:
        if ch in combined_text:
            tags.add(ch)
    if "main.tex" in combined_text:
        tags.add("main_tex")

    # 3. Failure Mode / Error Classification
    if "xelatex" in combined_text or "compilation failed" in combined_text:
        tags.add("xelatex_compilation_error")
    elif "python syntax" in combined_text or "syntaxerror" in combined_text:
        tags.add("python_syntax_error")
    elif "empty or non-string" in combined_text:
        tags.add("empty_llm_response")
    elif "could not complete a tool call" in combined_text:
        tags.add("tool_call_error")
    elif "403" in combined_text or "forbidden" in combined_text or "unauthorized" in combined_text:
        tags.add("auth_or_perm_error")
    elif "duplicate label" in combined_text:
        tags.add("duplicate_label_error")
    elif "timeout" in combined_text or "timed out" in combined_text:
        tags.add("timeout_error")
    else:
        tags.add("runtime_error")

    return sorted(list(tags))


def classify_failure_type(failure_reason: str) -> str:
    """Classifies failure into a standardized error type enum."""
    reason_lower = failure_reason.lower()
    if "xelatex compilation failed" in reason_lower:
        return "XELATEX_COMPILATION_ERROR"
    elif "python syntax" in reason_lower:
        return "PYTHON_SYNTAX_ERROR"
    elif "empty or non-string" in reason_lower:
        return "EMPTY_LLM_OUTPUT"
    elif "could not complete a tool call" in reason_lower:
        return "ROUTER_TOOL_CALL_REJECTED"
    elif "403" in reason_lower or "forbidden" in reason_lower:
        return "HTTP_403_FORBIDDEN"
    elif "429" in reason_lower or "resource_exhausted" in reason_lower:
        return "RATE_LIMIT_EXHAUSTED"
    elif "duplicate label" in reason_lower:
        return "DUPLICATE_LABEL_COLLISION"
    elif "timed out" in reason_lower or "timeout" in reason_lower:
        return "NETWORK_TIMEOUT"
    return "GENERAL_EXECUTION_FAILURE"


class FailedTaskRegistry:
    """Thread-safe registry managing recorded execution failures for bug-fixing agents."""

    def __init__(self):
        self.registry_file = FAILED_TASKS_FILE
        self.report_file = FAILED_REPORT_FILE
        DASHBOARDS_DIR.mkdir(parents=True, exist_ok=True)
        self.pedagogy_engine: Optional[Any] = None
        if PostMortemPedagogyEngine is not None:
            try:
                self.pedagogy_engine = PostMortemPedagogyEngine(
                    lessons_path=DASHBOARDS_DIR / "architectural_lessons.json",
                    digest_path=DASHBOARDS_DIR / "ARCHITECTURAL_LESSONS.md",
                )
            except Exception:
                self.pedagogy_engine = None
        self._ensure_init()

    def _ensure_init(self):
        if not self.registry_file.exists():
            initial_data = {
                "total_recorded_failures": 0,
                "pending_bugfix_tasks": 0,
                "resolved_bugfix_tasks": 0,
                "failures": [],
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            self.registry_file.write_text(json.dumps(initial_data, indent=2, ensure_ascii=False), encoding="utf-8")
        if not self.report_file.exists():
            self._export_markdown_report(self._load_data())

    def _load_data(self) -> dict:
        try:
            return json.loads(self.registry_file.read_text(encoding="utf-8"))
        except Exception:
            return {
                "total_recorded_failures": 0,
                "pending_bugfix_tasks": 0,
                "resolved_bugfix_tasks": 0,
                "failures": []
            }

    def _save_data(self, data: dict):
        # Update metrics
        failures = data.get("failures", [])
        data["total_recorded_failures"] = len(failures)
        data["pending_bugfix_tasks"] = sum(1 for f in failures if f.get("status") in ["pending_fix", "in_progress"])
        data["resolved_bugfix_tasks"] = sum(1 for f in failures if f.get("status") == "resolved")
        data["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        tmp = self.registry_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(self.registry_file)
        self._export_markdown_report(data)

    def record_failure(
        self,
        repo_id: str,
        repo_name: str,
        milestone_name: str,
        failure_reason: str,
        target_file: str = "",
        model_used: str = "gemini-3.8-flash-tiered",
        github_item_id: Optional[str] = None,
        hermes_session_id: Optional[str] = None
    ) -> dict:
        """
        Records or updates a failed execution task with structured ID, tags, anti-pattern taxonomy,
        pedagogical remediation lesson, and full error context.
        """
        task_id = generate_task_id(repo_id, milestone_name)
        tags = infer_tags(repo_id, milestone_name, target_file, failure_reason)
        fail_type = classify_failure_type(failure_reason)
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Architectural Post-Mortem & Anti-Pattern Analysis
        anti_pattern_id: Optional[str] = None
        anti_pattern_label: Optional[str] = None
        pedagogical_lesson: Optional[Dict[str, str]] = None

        task_payload = {
            "task_id": task_id,
            "repo_id": repo_id,
            "repo_name": repo_name,
            "milestone_name": milestone_name,
            "milestone_title": milestone_name,
            "target_file": target_file,
            "failure_reason": failure_reason,
            "failure_type": fail_type,
            "tags": tags,
            "model_used": model_used,
            "github_item_id": github_item_id,
            "hermes_session_id": hermes_session_id,
        }

        if self.pedagogy_engine is None and PostMortemPedagogyEngine is not None:
            try:
                self.pedagogy_engine = PostMortemPedagogyEngine(
                    lessons_path=DASHBOARDS_DIR / "architectural_lessons.json",
                    digest_path=DASHBOARDS_DIR / "ARCHITECTURAL_LESSONS.md",
                )
            except Exception:
                self.pedagogy_engine = None

        if self.pedagogy_engine is not None:
            try:
                pm_record = self.pedagogy_engine.analyze_failure(task_payload)
                if pm_record:
                    anti_pattern_id = getattr(pm_record, "anti_pattern_id", None)
                    anti_pattern_label = getattr(pm_record, "anti_pattern_label", None)
                    lesson_obj = getattr(pm_record, "lesson", None)
                    if lesson_obj is not None and hasattr(lesson_obj, "to_dict"):
                        pedagogical_lesson = lesson_obj.to_dict()
                    elif isinstance(lesson_obj, dict):
                        pedagogical_lesson = lesson_obj
                    elif lesson_obj is not None:
                        pedagogical_lesson = {
                            "assumption": str(getattr(lesson_obj, "assumption", "")),
                            "countermeasure": str(getattr(lesson_obj, "countermeasure", "")),
                            "rule_of_thumb": str(getattr(lesson_obj, "rule_of_thumb", "")),
                        }
                    try:
                        self.pedagogy_engine.save_lessons_json()
                        self.pedagogy_engine.render_markdown_digest()
                    except Exception:
                        pass
            except Exception:
                pass

        with _REGISTRY_LOCK:
            data = self._load_data()
            failures = data.setdefault("failures", [])

            # Check if this task already exists in registry
            existing = None
            for item in failures:
                if item.get("task_id") == task_id or item.get("milestone_name") == milestone_name:
                    existing = item
                    break

            if existing:
                existing["attempts"] = existing.get("attempts", 1) + 1
                existing["last_failed_at"] = now_str
                existing["failure_reason"] = failure_reason
                existing["failure_type"] = fail_type
                existing["tags"] = sorted(list(set(existing.get("tags", []) + tags)))
                existing["target_file"] = target_file or existing.get("target_file", "")
                existing["model_used"] = model_used
                if github_item_id:
                    existing["github_item_id"] = github_item_id
                if hermes_session_id:
                    existing["hermes_session_id"] = hermes_session_id
                if anti_pattern_id:
                    existing["anti_pattern_id"] = anti_pattern_id
                if anti_pattern_label:
                    existing["anti_pattern_label"] = anti_pattern_label
                if pedagogical_lesson:
                    existing["pedagogical_lesson"] = pedagogical_lesson
                if existing.get("status") == "resolved":
                    existing["status"] = "pending_fix"  # Regressed
                record = existing
            else:
                record = {
                    "task_id": task_id,
                    "repo_id": repo_id,
                    "repo_name": repo_name,
                    "milestone_name": milestone_name,
                    "tags": tags,
                    "target_file": target_file,
                    "failure_type": fail_type,
                    "failure_reason": failure_reason,
                    "anti_pattern_id": anti_pattern_id,
                    "anti_pattern_label": anti_pattern_label,
                    "pedagogical_lesson": pedagogical_lesson,
                    "model_used": model_used,
                    "github_item_id": github_item_id,
                    "hermes_session_id": hermes_session_id,
                    "status": "pending_fix",
                    "attempts": 1,
                    "first_failed_at": now_str,
                    "last_failed_at": now_str,
                    "remediation": {
                        "assigned_agent": None,
                        "fix_commit_hash": None,
                        "notes": None,
                        "resolved_at": None
                    }
                }
                failures.insert(0, record)

            self._save_data(data)
            ap_info = f" | Anti-Pattern: {anti_pattern_id}" if anti_pattern_id else ""
            print(f"[📌 FAILED TASK RECORDED] ID: `{task_id}` | Tags: {tags} | Reason: {fail_type}{ap_info}")
            return record

    def mark_resolved(self, task_id: str, fix_commit_hash: Optional[str] = None, notes: Optional[str] = None, agent_id: str = "bugfix_agent") -> bool:
        """Marks a previously failed task as resolved by a bug-fixing agent."""
        with _REGISTRY_LOCK:
            data = self._load_data()
            found = False
            for item in data.get("failures", []):
                if item.get("task_id") == task_id:
                    item["status"] = "resolved"
                    item["remediation"] = {
                        "assigned_agent": agent_id,
                        "fix_commit_hash": fix_commit_hash,
                        "notes": notes,
                        "resolved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    found = True
                    break

            if found:
                self._save_data(data)
                print(f"[✅ FAILED TASK RESOLVED] ID: `{task_id}` fixed in commit {fix_commit_hash}")
                return True
            return False

    def get_pending_tasks(self, tag: Optional[str] = None, repo_id: Optional[str] = None) -> list[dict]:
        """Retrieves pending tasks for consumption by a bug-fixing agent."""
        data = self._load_data()
        results = []
        for item in data.get("failures", []):
            if item.get("status") not in ["pending_fix", "in_progress"]:
                continue
            if repo_id and item.get("repo_id") != repo_id:
                continue
            if tag and tag not in item.get("tags", []):
                continue
            results.append(item)
        return results

    def _export_markdown_report(self, data: dict):
        """Generates an agent-readable Markdown report with pedagogical lessons for quick inspection."""
        failures = data.get("failures", [])
        pending = [f for f in failures if f.get("status") in ["pending_fix", "in_progress"]]
        resolved = [f for f in failures if f.get("status") == "resolved"]

        lines = [
            "# 🛠️ Execution Failures & Bugfix Agent Queue",
            f"**Last Updated:** `{data.get('updated_at', 'Now')}` | **Pending Fixes:** `{len(pending)}` | **Resolved:** `{len(resolved)}`",
            "",
            "This registry records all failed autonomous milestones with unique task IDs, domain tags, pedagogical anti-patterns, and failure traces.",
            "Autonomous bug-fixing agents can consume tasks from this board and apply atomic fixes informed by pedagogical lessons.",
            "",
            "## 🚨 Pending Remediation Tasks",
            ""
        ]

        if not pending:
            lines.append("*No pending failed tasks! All execution tasks have completed cleanly.*")
        else:
            for item in pending:
                tags_str = " ".join([f"`#{t}`" for t in item.get("tags", [])])
                item_lines = [
                    f"### ❌ `{item.get('task_id')}`: {item.get('milestone_name')}",
                    f"- **Repository:** `{item.get('repo_name')}` (`{item.get('repo_id')}`)",
                    f"- **Target File:** `{item.get('target_file') or '—'}`",
                    f"- **Tags:** {tags_str}",
                    f"- **Failure Type:** `{item.get('failure_type')}`",
                ]

                ap_label = item.get("anti_pattern_label")
                ap_id = item.get("anti_pattern_id")
                if ap_label or ap_id:
                    item_lines.append(f"- **Anti-Pattern Classification:** `{ap_label or ap_id}` (`{ap_id}`)")

                lesson = item.get("pedagogical_lesson")
                if lesson and isinstance(lesson, dict):
                    item_lines.extend([
                        "- **🎓 Pedagogical Lesson & Remediation:**",
                        f"  - **Broken Assumption:** {lesson.get('assumption', 'N/A')}",
                        f"  - **Countermeasure:** {lesson.get('countermeasure', 'N/A')}",
                        f"  - **Rule of Thumb:** {lesson.get('rule_of_thumb', 'N/A')}",
                    ])

                item_lines.extend([
                    f"- **Attempts:** `{item.get('attempts', 1)}` | **Last Failed:** `{item.get('last_failed_at')}`",
                    f"- **Error Trace / Failure Reason:**",
                    "```text",
                    item.get("failure_reason", "No details available")[:1200],
                    "```",
                    ""
                ])
                lines.extend(item_lines)

        if resolved:
            lines.extend([
                "---",
                "## ✅ Resolved Tasks Archive",
                ""
            ])
            for item in resolved[:10]:
                rem = item.get("remediation", {})
                ap_str = f" [{item.get('anti_pattern_id')}]" if item.get("anti_pattern_id") else ""
                lines.append(
                    f"- **`{item.get('task_id')}`** — *{item.get('milestone_name')}*{ap_str} "
                    f"(Resolved by `{rem.get('assigned_agent')}` in commit `{rem.get('fix_commit_hash')}` at `{rem.get('resolved_at')}`)"
                )

        try:
            self.report_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
        except Exception:
            pass


# Global Singleton Instance
_GLOBAL_REGISTRY = FailedTaskRegistry()

def get_registry() -> FailedTaskRegistry:
    return _GLOBAL_REGISTRY


def backfill_from_repositories():
    """Backfills any existing failed/blocked milestones from repositories.json."""
    if not REPOS_FILE.exists():
        return 0

    try:
        repos_data = json.loads(REPOS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return 0

    reg = get_registry()
    count = 0
    for r in repos_data.get("repositories", []):
        r_id = r.get("id", "repo")
        r_name = r.get("name", "General")
        for m in r.get("milestones", []):
            if m.get("status") in ["failed", "blocked"]:
                reason = m.get("failure_reason") or "Execution failure recorded in repositories.json"
                target_file = m.get("relative_filepath") or m.get("output_file") or ""
                gh_id = m.get("github_item_id")
                reg.record_failure(
                    repo_id=r_id,
                    repo_name=r_name,
                    milestone_name=m.get("name", ""),
                    failure_reason=reason,
                    target_file=target_file,
                    github_item_id=gh_id
                )
                count += 1
    return count


if __name__ == "__main__":
    count = backfill_from_repositories()
    reg = get_registry()
    data = reg._load_data()
    reg._export_markdown_report(data)
    print("=" * 75)
    print(" 🛠️ FAILED TASK REGISTRY & BUGFIX QUEUE")
    print("=" * 75)
    print(f"• Backfilled Existing Failures: {count}")
    print(f"• Total Recorded Failures:     {data.get('total_recorded_failures')}")
    print(f"• Pending Bugfix Tasks:        {data.get('pending_bugfix_tasks')}")
    print(f"• JSON Registry:               {FAILED_TASKS_FILE}")
    print(f"• Markdown Report:             {FAILED_REPORT_FILE}")
    print("=" * 75)
