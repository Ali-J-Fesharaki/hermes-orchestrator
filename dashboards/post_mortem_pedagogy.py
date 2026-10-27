"""
Architectural Post-Mortem Pedagogy & Anti-Pattern Learning Engine.

Systematically classifies distributed autonomous execution failures into canonical
architectural anti-patterns, evaluates weighted regex signatures, scores failures via
log-normalized metrics, tracks recurrence frequencies per repository, dynamically escalates
recurring design flaws to CRITICAL severity, and generates pedagogical remediation
lessons with thread-safe atomic persistence.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys
import threading
from typing import Any, Dict, List, Optional, Tuple, Union


__all__ = [
    "PedagogicalLesson",
    "AntiPatternDefinition",
    "PostMortemRecord",
    "ANTI_PATTERN_CATALOG",
    "FALLBACK_ANOMALY",
    "PostMortemPedagogyEngine",
    "emit_post_mortem",
    "get_post_mortems",
    "_atomic_write_text",
]


# ==============================================================================
# 0. Atomic File Helpers
# ==============================================================================

def _atomic_write_text(path: Union[Path, str], content: str) -> None:
    """Atomic write via temporary file replacement with fsync."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = p.with_suffix(f"{p.suffix}.{os.getpid()}.{threading.get_ident()}.tmp")
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        tmp_path.replace(p)
    except Exception:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
        raise


# ==============================================================================
# 1. Architecture & Data Structures
# ==============================================================================

@dataclass(frozen=True)
class PedagogicalLesson:
    """Immutable 3-bullet architectural lesson template (Delta, Gamma, Omega)."""
    assumption: str      # Delta: Broken Assumption
    countermeasure: str  # Gamma: Architectural Countermeasure
    rule_of_thumb: str   # Omega: Heuristic Rule of Thumb

    def to_dict(self) -> Dict[str, str]:
        return {
            "assumption": self.assumption,
            "countermeasure": self.countermeasure,
            "rule_of_thumb": self.rule_of_thumb,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> PedagogicalLesson:
        return cls(
            assumption=str(data.get("assumption", "")),
            countermeasure=str(data.get("countermeasure", "")),
            rule_of_thumb=str(data.get("rule_of_thumb", "")),
        )


@dataclass
class AntiPatternDefinition:
    """Canonical anti-pattern taxonomic definition with weighted signatures."""
    id: str
    label: str
    signatures: List[Tuple[str, float]]
    lesson_template: PedagogicalLesson
    severity: str  # 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'

    def compile_signatures(self) -> List[Tuple[re.Pattern, float]]:
        """Pre-compiles regex patterns for high-throughput evaluation."""
        compiled = []
        for pattern_str, weight in self.signatures:
            compiled.append((re.compile(pattern_str, re.IGNORECASE | re.MULTILINE), float(weight)))
        return compiled


@dataclass
class PostMortemRecord:
    """Comprehensive incident post-mortem pedagogical record."""
    lesson_id: str
    task_id: str
    repo_id: str
    milestone_title: str
    target_file: str
    anti_pattern_id: str
    anti_pattern_label: str
    severity: str
    lesson: PedagogicalLesson
    raw_error_snippet: str
    recurrence_count: int
    timestamp: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "lesson_id": self.lesson_id,
            "task_id": self.task_id,
            "repo_id": self.repo_id,
            "milestone_title": self.milestone_title,
            "target_file": self.target_file,
            "anti_pattern_id": self.anti_pattern_id,
            "anti_pattern_label": self.anti_pattern_label,
            "severity": self.severity,
            "lesson": self.lesson.to_dict(),
            "raw_error_snippet": self.raw_error_snippet,
            "recurrence_count": self.recurrence_count,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> PostMortemRecord:
        lesson_data = data.get("lesson", {})
        if isinstance(lesson_data, dict):
            lesson_obj = PedagogicalLesson.from_dict(lesson_data)
        elif isinstance(lesson_data, PedagogicalLesson):
            lesson_obj = lesson_data
        else:
            lesson_obj = PedagogicalLesson(
                assumption="Unknown flawed assumption.",
                countermeasure="Implement resilient error handling and validation.",
                rule_of_thumb="Verify state transitions before proceeding.",
            )

        return cls(
            lesson_id=str(data.get("lesson_id", "")),
            task_id=str(data.get("task_id", "")),
            repo_id=str(data.get("repo_id", "")),
            milestone_title=str(data.get("milestone_title", "")),
            target_file=str(data.get("target_file", "")),
            anti_pattern_id=str(data.get("anti_pattern_id", "unclassified_runtime_anomaly")),
            anti_pattern_label=str(data.get("anti_pattern_label", "Unclassified Runtime Anomaly")),
            severity=str(data.get("severity", "LOW")),
            lesson=lesson_obj,
            raw_error_snippet=str(data.get("raw_error_snippet", "")),
            recurrence_count=int(data.get("recurrence_count", 1)),
            timestamp=str(data.get("timestamp", datetime.now(timezone.utc).isoformat())),
        )


# ==============================================================================
# 2. Canonical Anti-Pattern Catalog (8 Primary Taxonomic Patterns)
# ==============================================================================

ANTI_PATTERN_CATALOG: Dict[str, AntiPatternDefinition] = {
    "unfenced_shared_state": AntiPatternDefinition(
        id="unfenced_shared_state",
        label="Unfenced Shared State & Lock Contention",
        signatures=[
            (r"index\.lock.*(?:File exists|cannot lock|unable to create)", 9.0),
            (r"fatal: Unable to create '.*index\.lock'", 8.5),
            (r"sqlite3\.OperationalError: database is locked", 8.5),
            (r"Resource temporarily unavailable.*lock", 7.0),
            (r"FileLockTimeout.*failed to acquire", 7.5),
            (r"concurrent.*write.*conflict", 6.5),
            (r"split-brain.*race.*condition", 7.5),
        ],
        lesson_template=PedagogicalLesson(
            assumption="Assumes files, git indices, or shared state can be safely modified without distributed or advisory lease fences.",
            countermeasure="Wrap shared mutations in ephemeral worktrees, distributed fencing tokens, or file locks with randomized exponential backoff jitter.",
            rule_of_thumb="Never share mutable git workspaces or databases across concurrent worker threads; isolate by worktree or shard with fencing tokens.",
        ),
        severity="HIGH",
    ),
    "thundering_herd_retry": AntiPatternDefinition(
        id="thundering_herd_retry",
        label="Thundering Herd Synchronized Retry Storm",
        signatures=[
            (r"429.*(?:Too Many Requests|rate limit)", 9.0),
            (r"ResourceExhausted.*(?:429|quota exceeded)", 9.0),
            (r"Rate limit exceeded.*retry after", 8.5),
            (r"synchronized.*retry.*storm", 8.0),
            (r"retry.*quota.*exhausted", 7.5),
            (r"exponential backoff failed.*exhausted", 6.5),
            (r"rate limit cascade", 8.0),
        ],
        lesson_template=PedagogicalLesson(
            assumption="Assumes immediate or synchronized retry loops on rate-limited endpoints will eventually succeed without worsening downstream saturation.",
            countermeasure="Implement full randomized decorrelated jitter backoff, adaptive token-bucket governors, and circuit breakers before retrying.",
            rule_of_thumb="On HTTP 429/ResourceExhausted, fail fast or back off exponentially with decorrelated jitter; never retry synchronously in tight loops.",
        ),
        severity="CRITICAL",
    ),
    "leaky_sandbox_boundary": AntiPatternDefinition(
        id="leaky_sandbox_boundary",
        label="Leaky Execution Sandbox Boundary",
        signatures=[
            (r"untracked working tree files would be overwritten", 9.0),
            (r"dirty working tree.*cannot (?:checkout|merge|pull)", 8.5),
            (r"subprocess.*leaked.*(?:process group|zombie)", 8.5),
            (r"PermissionDenied.*host.*file system", 8.0),
            (r"leftover.*tmp.*polluting workspace", 7.5),
            (r"sandbox boundary breach.*outside workdir", 9.0),
            (r"contaminat(?:ing|ed).*git.*tree", 8.0),
        ],
        lesson_template=PedagogicalLesson(
            assumption="Assumes tasks or sub-agents clean up generated side-effects, child processes, and temporary files on the host environment.",
            countermeasure="Execute all sub-agents inside containerized or ephemeral git worktree sandboxes that tear down automatically via context managers.",
            rule_of_thumb="Treat every worker run as hostile to the workspace; sandbox filesystem mutations and reap process trees on exit.",
        ),
        severity="CRITICAL",
    ),
    "missing_circuit_breaker": AntiPatternDefinition(
        id="missing_circuit_breaker",
        label="Missing Circuit Breaker on Deterministic Errors",
        signatures=[
            (r"repeated fatal error.*no progress.*consecutive", 9.0),
            (r"circuit breaker (?:not found|missing|threshold exceeded)", 8.5),
            (r"deterministic failure.*(?:repeated|tight loop)", 8.5),
            (r"Maximum retries \d+ exceeded without state change", 9.0),
            (r"consecutive failures exceeded.*tripped", 7.5),
            (r"(?:ZeroDivisionError|SyntaxError|TypeError).*persists across \d+ attempts", 8.5),
            (r"without tripping backoff", 8.0),
        ],
        lesson_template=PedagogicalLesson(
            assumption="Assumes repeatedly executing the exact same failed deterministic logic will yield a different outcome without intervention.",
            countermeasure="Implement a threshold circuit breaker (e.g., trip after 3 identical consecutive failures) that halts and alerts an operator.",
            rule_of_thumb="Deterministic failures (SyntaxError, schema invalidity) must trip immediate circuit breakers rather than burning loop budget.",
        ),
        severity="HIGH",
    ),
    "god_task_overdecomposition": AntiPatternDefinition(
        id="god_task_overdecomposition",
        label="God Task Context Overflow & Overdecomposition",
        signatures=[
            (r"(?:maximum context length|context window) exceeded", 9.0),
            (r"token budget exhausted.*prompt too large", 9.0),
            (r"monolithic task.*exceeded context limit", 8.5),
            (r"hallucinating partial completions?.*truncated", 8.5),
            (r"task scope too broad.*split required", 8.0),
            (r"payload size \d+ tokens exceeds limit", 8.0),
            (r"producing truncated stubs", 8.0),
        ],
        lesson_template=PedagogicalLesson(
            assumption="Assumes complex multi-file architectural refactors can be comprehended and resolved within a single prompt context window.",
            countermeasure="Decompose monolithic milestones into bounded, single-file sub-tasks capped at ~800-1200 tokens with explicit DAG dependencies.",
            rule_of_thumb="If a milestone modifies >3 files or touches >500 LOC, split it into bounded atomic milestones before scheduling.",
        ),
        severity="MEDIUM",
    ),
    "catastrophic_backtracking_regex": AntiPatternDefinition(
        id="catastrophic_backtracking_regex",
        label="Catastrophic Backtracking Regex Freeze",
        signatures=[
            (r"catastrophic backtracking.*regex", 9.5),
            (r"regex.*(?:timeout|hung|freez(?:e|ing))", 9.0),
            (r"re\.error: regular expression.*too complex", 8.5),
            (r"worker thread hung in regex evaluation", 9.0),
            (r"polynomial.*backtracking detected in pattern", 8.5),
            (r"CPU 100%.*regex match", 8.0),
            (r"exponential backtracking regex", 9.0),
        ],
        lesson_template=PedagogicalLesson(
            assumption="Assumes nested quantifier regular expressions can safely parse unbounded, arbitrary multi-line text payloads.",
            countermeasure="Refactor regex patterns to use atomic grouping, possessive quantifiers, strict character classes, or linear-time automata parsers.",
            rule_of_thumb="Never combine nested wildcards (e.g. `(a+)+` or `(.*[a-z])+`) over unbounded logs; use bounded line-by-line streaming or linear automata.",
        ),
        severity="HIGH",
    ),
    "unhandled_upstream_protocol_drift": AntiPatternDefinition(
        id="unhandled_upstream_protocol_drift",
        label="Unhandled Upstream Protocol Drift",
        signatures=[
            (r"KeyError: '(?:choices|candidates|content|message|output)'", 9.0),
            (r"ValidationError.*(?:unexpected field|missing required field)", 8.5),
            (r"upstream protocol drift.*schema mismatch", 9.0),
            (r"JSONDecodeError: Expecting value.*upstream response", 8.5),
            (r"unexpected response structure from (?:model|upstream API)", 8.5),
            (r"AttributeError: 'NoneType' object has no attribute '(?:text|data|content)'", 8.5),
            (r"external model/gateway API schema change", 8.5),
        ],
        lesson_template=PedagogicalLesson(
            assumption="Assumes external model providers or upstream REST APIs maintain rigid, immutable response schemas across revisions.",
            countermeasure="Validate payloads using defensive schema adapters, safe `.get()` defaults, and protocol fallback negotiation.",
            rule_of_thumb="Treat upstream model API schemas as dynamically versioned; always parse via defensive schema wrappers with fallback parsers.",
        ),
        severity="MEDIUM",
    ),
    "phantom_success_illusion": AntiPatternDefinition(
        id="phantom_success_illusion",
        label="Phantom Success Illusion & Swallowed Failure",
        signatures=[
            (r"command failed with (?:exit )?code [1-9]\d*.*marked (?:as )?success", 9.5),
            (r"phantom success.*(?:test suite failed|swallowed error)", 9.0),
            (r"swallowed exception.*silent (?:pass|success)", 8.5),
            (r"non-zero exit code ignored.*reported ok", 9.0),
            (r"missing artifact.*execution marked complete", 8.5),
            (r"AssertionError: unverified artifact.*falsely reported 0", 8.5),
            (r"without non-zero exit code validation", 9.0),
        ],
        lesson_template=PedagogicalLesson(
            assumption="Assumes execution succeeded because the wrapper script exited without raising an uncaught exception, ignoring child exit codes.",
            countermeasure="Enforce strict exit code verification (`check=True`), validate artifact existence and non-zero hash before marking task success.",
            rule_of_thumb="Success is proven only by verified artifacts and strict exit-code zero; never trust subprocess completion without post-validation.",
        ),
        severity="CRITICAL",
    ),
}

FALLBACK_ANOMALY = AntiPatternDefinition(
    id="unclassified_runtime_anomaly",
    label="Unclassified Runtime Anomaly",
    signatures=[],
    lesson_template=PedagogicalLesson(
        assumption="Assumes unseen failure signature fits existing historical taxonomic models.",
        countermeasure="Inspect raw error logs, extract invariant failure tokens, and register a new anti-pattern signature in the catalog.",
        rule_of_thumb="Regularly audit unclassified anomalies to expand the pedagogical anti-pattern catalog.",
    ),
    severity="LOW",
)


# ==============================================================================
# 3. Post-Mortem Pedagogy Engine
# ==============================================================================

class PostMortemPedagogyEngine:
    """Thread-safe pedagogical classification and architectural learning engine."""

    MATCH_THRESHOLD: float = 1.2

    def __init__(
        self,
        registry_path: Optional[Union[Path, str]] = None,
        lessons_path: Optional[Union[Path, str]] = None,
        markdown_path: Optional[Union[Path, str]] = None,
        *,
        digest_path: Optional[Union[Path, str]] = None,
    ) -> None:
        self._lock = threading.Lock()
        base_dir = Path(__file__).resolve().parent

        # Handle legacy 2-argument positional invocation: (lessons_path, digest_path)
        if registry_path is not None and lessons_path is not None and markdown_path is None and digest_path is None:
            if "lessons" in str(registry_path).lower():
                digest_path = lessons_path
                lessons_path = registry_path
                registry_path = None

        self.registry_path = Path(registry_path) if registry_path else base_dir / "failed_tasks_registry.json"
        self.lessons_path = Path(lessons_path) if lessons_path else base_dir / "architectural_lessons.json"
        self.markdown_path = Path(markdown_path or digest_path or (base_dir / "ARCHITECTURAL_LESSONS.md"))
        self.digest_path = self.markdown_path

        self._records: Dict[str, PostMortemRecord] = {}
        self._compiled_catalog = {
            pat_id: pat_def.compile_signatures()
            for pat_id, pat_def in ANTI_PATTERN_CATALOG.items()
        }
        self._load_existing_records()

    def _load_existing_records(self) -> None:
        """Loads historical post-mortem records from disk defensively."""
        if not self.lessons_path.exists():
            return

        try:
            with open(self.lessons_path, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if not content:
                    return
                data = json.loads(content)

            raw_records = []
            if isinstance(data, dict):
                raw_records = data.get("records", [])
            elif isinstance(data, list):
                raw_records = data

            for item in raw_records:
                if isinstance(item, dict):
                    record = PostMortemRecord.from_dict(item)
                    self._records[record.lesson_id] = record
        except Exception:
            # Defensive fallback to preserve runtime stability
            pass

    def _extract_corpus(self, task_data: Dict[str, Any]) -> Tuple[str, str]:
        """
        Extracts normalized textual corpus T(E) = concat(raw_traceback, stderr_snippet, milestone_name, target_file)
        and returns (full_corpus, raw_snippet).
        """
        traceback_parts = []
        for key in ["traceback", "raw_traceback", "exception", "stack_trace"]:
            v = task_data.get(key)
            if v and isinstance(v, str):
                traceback_parts.append(v)
            elif v and isinstance(v, (list, tuple)):
                traceback_parts.extend(str(x) for x in v)

        stderr_parts = []
        for key in ["stderr", "stderr_snippet", "error", "error_text", "failure_reason", "message"]:
            v = task_data.get(key)
            if v and isinstance(v, str):
                stderr_parts.append(v)
            elif v and isinstance(v, (list, tuple)):
                stderr_parts.extend(str(x) for x in v)

        milestone_name = str(task_data.get("milestone_name") or task_data.get("milestone_title") or "")
        target_file = str(task_data.get("target_file") or task_data.get("relative_filepath") or "")

        raw_traceback_str = "\n".join(traceback_parts).strip()
        stderr_snippet_str = "\n".join(stderr_parts).strip()

        corpus_elements = [
            raw_traceback_str,
            stderr_snippet_str,
            milestone_name,
            target_file,
        ]
        corpus = "\n".join([elem for elem in corpus_elements if elem]).strip()
        if not corpus:
            corpus = str(task_data)

        snippet = (stderr_snippet_str or raw_traceback_str or corpus)[:600].strip()
        return corpus, snippet

    def _score_failure(self, corpus: str) -> Tuple[str, float, float]:
        """
        Computes Score(E, a_k) = sum_{j} w_j * match_count(pattern_j, T(E))
        and log-normalized score S_norm(E, a_k) = Score(E, a_k) / (1.0 + ln(1.0 + len(T(E)))).
        """
        text_len = len(corpus)
        denominator = 1.0 + math.log(1.0 + text_len)

        best_id = "unclassified_runtime_anomaly"
        best_norm_score = 0.0
        best_raw_score = 0.0

        for pattern_id, compiled_sigs in self._compiled_catalog.items():
            raw_score = 0.0
            for compiled_regex, weight in compiled_sigs:
                matches = len(compiled_regex.findall(corpus))
                if matches > 0:
                    raw_score += weight * matches

            norm_score = raw_score / denominator if denominator > 0 else 0.0

            if norm_score > best_norm_score:
                best_norm_score = norm_score
                best_raw_score = raw_score
                best_id = pattern_id

        return best_id, best_raw_score, best_norm_score

    def analyze_failure(self, task_data: Dict[str, Any]) -> PostMortemRecord:
        """
        Analyzes a task execution failure and registers an architectural post-mortem record.
        Tracks recurrence frequency per (anti_pattern_id, repo_id) and automatically
        escalates severity to CRITICAL if recurrence_count >= 3.
        """
        with self._lock:
            task_id = str(task_data.get("task_id", f"task_{os.urandom(4).hex()}"))
            repo_id = str(task_data.get("repo_id", "hermes_orchestrator"))
            milestone_title = str(
                task_data.get("milestone_title")
                or task_data.get("milestone_name")
                or "Autonomous Task Execution"
            )
            target_file = str(task_data.get("target_file", "unknown_target"))

            corpus, raw_snippet = self._extract_corpus(task_data)
            best_id, raw_score, norm_score = self._score_failure(corpus)

            if norm_score >= self.MATCH_THRESHOLD and best_id in ANTI_PATTERN_CATALOG:
                anti_pattern = ANTI_PATTERN_CATALOG[best_id]
            else:
                anti_pattern = FALLBACK_ANOMALY

            # Recurrence tracking R(a_k, repo_id)
            existing_occurrences = sum(
                1 for r in self._records.values()
                if (r.anti_pattern_id == anti_pattern.id or (anti_pattern.id == "unclassified_runtime_anomaly" and r.anti_pattern_id in ("unclassified_runtime_anomaly", "unclassified_anomaly")))
                and r.repo_id == repo_id
            )
            recurrence_count = existing_occurrences + 1

            # Dynamic severity escalation: If R(a_k, repo_id) >= 3, automatically escalate to CRITICAL
            effective_severity = anti_pattern.severity
            if recurrence_count >= 3:
                effective_severity = "CRITICAL"

            timestamp = datetime.now(timezone.utc).isoformat()
            hasher = hashlib.sha256(f"{task_id}:{anti_pattern.id}:{repo_id}:{recurrence_count}:{timestamp}".encode("utf-8"))
            lesson_id = f"PML-{hasher.hexdigest()[:8].upper()}"

            record = PostMortemRecord(
                lesson_id=lesson_id,
                task_id=task_id,
                repo_id=repo_id,
                milestone_title=milestone_title,
                target_file=target_file,
                anti_pattern_id=anti_pattern.id,
                anti_pattern_label=anti_pattern.label,
                severity=effective_severity,
                lesson=anti_pattern.lesson_template,
                raw_error_snippet=raw_snippet,
                recurrence_count=recurrence_count,
                timestamp=timestamp,
            )

            self._records[lesson_id] = record
            return record

    def batch_analyze_registry(
        self,
        registry_path: Optional[Union[Path, str]] = None,
    ) -> List[PostMortemRecord]:
        """
        Performs batch failure analysis against a registry of failed task records.
        Supports both structured JSON and JSON Lines formats.
        """
        reg_file = Path(registry_path) if registry_path else self.registry_path
        if not reg_file.exists():
            return []

        analyzed_records: List[PostMortemRecord] = []
        try:
            with open(reg_file, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if not content:
                    return []
                try:
                    data = json.loads(content)
                except json.JSONDecodeError:
                    # Defensive parsing for JSON Lines
                    data = [json.loads(line) for line in content.splitlines() if line.strip()]

            items = []
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                if "failures" in data and isinstance(data["failures"], list):
                    items = data["failures"]
                elif "records" in data and isinstance(data["records"], list):
                    items = data["records"]
                else:
                    items = list(data.values())

            for item in items:
                if isinstance(item, dict):
                    rec = self.analyze_failure(item)
                    analyzed_records.append(rec)
        except Exception:
            return analyzed_records

        return analyzed_records

    def save_lessons_json(self, output_path: Optional[Union[Path, str]] = None) -> Path:
        """Atomically persists all architectural lessons to JSON with fsync."""
        target_path = Path(output_path) if output_path else self.lessons_path

        with self._lock:
            payload = {
                "version": "1.0.0",
                "last_updated": datetime.now(timezone.utc).isoformat(),
                "total_records": len(self._records),
                "records": [rec.to_dict() for rec in self._records.values()],
            }
            content = json.dumps(payload, indent=2, ensure_ascii=False)
            _atomic_write_text(target_path, content)

        return target_path

    def render_markdown_digest(self, output_path: Optional[Union[Path, str]] = None) -> Path:
        """
        Renders an executive pedagogical digest with metrics, recurring flaw alerts,
        and tripartite educational remediations.
        """
        target_path = Path(output_path) if output_path else self.markdown_path

        with self._lock:
            records = list(self._records.values())

        total_incidents = len(records)
        severity_counts: Dict[str, int] = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
        pattern_counts: Dict[str, int] = {p_id: 0 for p_id in ANTI_PATTERN_CATALOG}
        pattern_counts["unclassified_runtime_anomaly"] = 0

        # Track recurring patterns per repo for Kanban Cockpit warnings
        repo_pattern_recurrence: Dict[Tuple[str, str], int] = {}

        for r in records:
            severity_counts[r.severity] = severity_counts.get(r.severity, 0) + 1
            pid = r.anti_pattern_id
            if pid == "unclassified_anomaly":
                pid = "unclassified_runtime_anomaly"
            pattern_counts[pid] = pattern_counts.get(pid, 0) + 1
            key = (r.anti_pattern_id, r.repo_id)
            repo_pattern_recurrence[key] = max(repo_pattern_recurrence.get(key, 0), r.recurrence_count)

        md_lines = [
            "# Architectural Post-Mortem & Anti-Pattern Pedagogy Digest",
            "",
            "> **Autonomous Engine Intelligence**: Systematic post-mortem classification of distributed",
            "> orchestration failures into architectural anti-patterns with actionable countermeasures.",
            "",
            "## 1. Executive Fleet Metrics",
            "",
            "| Metric | Value | Status |",
            "| :--- | :--- | :--- |",
            f"| **Total Analyzed Incidents** | `{total_incidents}` | Active Historical Repository |",
            f"| **Critical Severity Breaches** | `{severity_counts.get('CRITICAL', 0)}` | Immediate Quarantine Required |",
            f"| **High Severity Flaws** | `{severity_counts.get('HIGH', 0)}` | Guardrail Enforcement Active |",
            f"| **Medium / Low Anomalies** | `{severity_counts.get('MEDIUM', 0) + severity_counts.get('LOW', 0)}` | Continuous Observability |",
            "",
        ]

        # Escalation alerts for Kanban Cockpit
        escalated_flaws = [
            (p_id, r_id, cnt)
            for (p_id, r_id), cnt in repo_pattern_recurrence.items()
            if cnt >= 3
        ]
        if escalated_flaws:
            md_lines.extend([
                "### ⚠️ Systemic Architectural Warnings (Kanban Cockpit Escalations)",
                "",
                "> The following recurring anti-patterns have breached threshold recurrence $R(a_k, \\text{repo}) \\ge 3$ and are escalated to **CRITICAL**:",
                "",
                "| Anti-Pattern ID | Target Repository | Recurrence Frequency | Actionable Directive |",
                "| :--- | :--- | :--- | :--- |",
            ])
            for p_id, r_id, cnt in escalated_flaws:
                label = ANTI_PATTERN_CATALOG.get(p_id, FALLBACK_ANOMALY).label
                md_lines.append(
                    f"| `{p_id}` ({label}) | `{r_id}` | **{cnt} recurrences** | Halt deployment, refactor shared boundaries, and trip circuit breaker |"
                )
            md_lines.append("")

        md_lines.extend([
            "### Anti-Pattern Frequency Distribution",
            "",
            "| Anti-Pattern ID | Classification Label | Base Severity | Recurrences |",
            "| :--- | :--- | :--- | :--- |",
        ])

        for p_id, count in sorted(pattern_counts.items(), key=lambda kv: kv[1], reverse=True):
            if p_id in ANTI_PATTERN_CATALOG:
                defn = ANTI_PATTERN_CATALOG[p_id]
                label = defn.label
                sev = defn.severity
            else:
                label = FALLBACK_ANOMALY.label
                sev = FALLBACK_ANOMALY.severity
            md_lines.append(f"| `{p_id}` | {label} | `{sev}` | **{count}** |")

        md_lines.extend([
            "",
            "## 2. Recent Incident Post-Mortem Records",
            "",
            "| Lesson ID | Task ID | Milestone | Anti-Pattern | Effective Severity | Recurrence | Timestamp |",
            "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
        ])

        recent_records = sorted(records, key=lambda x: x.timestamp, reverse=True)[:25]
        if not recent_records:
            md_lines.append("| _None_ | _No incidents registered yet_ | — | — | — | — | — |")
        else:
            for r in recent_records:
                clean_milestone = r.milestone_title.replace("|", "-")
                md_lines.append(
                    f"| `{r.lesson_id}` | `{r.task_id}` | {clean_milestone} | `{r.anti_pattern_id}` | `{r.severity}` | #{r.recurrence_count} | {r.timestamp[:19]} |"
                )

        md_lines.extend([
            "",
            "## 3. Pedagogical Anti-Pattern Catalog & Remediations",
            "",
            "Detailed operational guidance, flawed assumptions, systematic countermeasures, and rules of thumb:",
            "",
        ])

        for p_id, defn in ANTI_PATTERN_CATALOG.items():
            count = pattern_counts.get(p_id, 0)
            md_lines.extend([
                f"<details>",
                f"<summary><b>[{defn.severity}] {defn.label}</b> (<code>{p_id}</code>) — {count} recorded incident(s)</summary>",
                "",
                r"#### Core Flawed Assumption ($\Delta$)",
                f"> {defn.lesson_template.assumption}",
                "",
                r"#### Systematic Countermeasure ($\Gamma$)",
                f"- {defn.lesson_template.countermeasure}",
                "",
                r"#### Architectural Rule of Thumb ($\Omega$)",
                f"- **Guideline:** `{defn.lesson_template.rule_of_thumb}`",
                "",
                f"#### Monitored Diagnostic Signatures",
            ])
            for sig, weight in defn.signatures:
                clean_sig = sig.replace("`", "'")
                md_lines.append(f"- `weight {weight:.1f}` : <code>{clean_sig}</code>")
            md_lines.extend([
                "",
                "</details>",
                "",
            ])

        md_content = "\n".join(md_lines) + "\n"
        with self._lock:
            _atomic_write_text(target_path, md_content)

        return target_path

    def get_lesson_for_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves pedagogical lesson and remediation guidelines for a specific task."""
        with self._lock:
            matching = [r for r in self._records.values() if r.task_id == task_id]
            if not matching:
                return None
            matching.sort(key=lambda x: x.timestamp, reverse=True)
            return matching[0].to_dict()


# ==============================================================================
# 4. Global Singleton & Cross-Module Helpers
# ==============================================================================

_GLOBAL_ENGINE: Optional[PostMortemPedagogyEngine] = None
_GLOBAL_ENGINE_LOCK = threading.Lock()


def get_engine() -> PostMortemPedagogyEngine:
    """Returns thread-safe global PostMortemPedagogyEngine instance."""
    global _GLOBAL_ENGINE
    with _GLOBAL_ENGINE_LOCK:
        if _GLOBAL_ENGINE is None:
            _GLOBAL_ENGINE = PostMortemPedagogyEngine()
        return _GLOBAL_ENGINE


def emit_post_mortem(payload: Dict[str, Any]) -> Optional[PostMortemRecord]:
    """
    Convenience hook for cross-module post-mortem emission.
    Persists lesson JSON and updates Markdown digest atomically.
    """
    try:
        engine = get_engine()
        record = engine.analyze_failure(payload)
        engine.save_lessons_json()
        engine.render_markdown_digest()
        return record
    except Exception:
        return None


def get_post_mortems() -> List[Dict[str, Any]]:
    """Convenience accessor for downstream aggregators to retrieve all post-mortem records."""
    try:
        engine = get_engine()
        with engine._lock:
            return [r.to_dict() for r in engine._records.values()]
    except Exception:
        return []


# ==============================================================================
# 5. Self-Verification & Test Suite
# ==============================================================================

if __name__ == "__main__":
    import shutil
    import tempfile

    print("[*] Starting PostMortemPedagogyEngine comprehensive verification suite...")

    temp_dir = Path(tempfile.mkdtemp(prefix="pedagogy_test_"))
    try:
        test_json_path = temp_dir / "architectural_lessons.json"
        test_md_path = temp_dir / "ARCHITECTURAL_LESSONS.md"
        test_reg_path = temp_dir / "failed_tasks_registry.json"

        engine = PostMortemPedagogyEngine(
            registry_path=test_reg_path,
            lessons_path=test_json_path,
            markdown_path=test_md_path,
        )

        synthetic_cases = [
            (
                "unfenced_shared_state",
                "HIGH",
                {
                    "task_id": "test_lock_01",
                    "repo_id": "hermes_orchestrator",
                    "milestone_title": "Git Lock Conflict",
                    "target_file": ".git/index",
                    "error": "fatal: Unable to create '/home/ajf/automation/.git/index.lock': File exists. Another git process seems to be running in this repository.",
                },
            ),
            (
                "thundering_herd_retry",
                "CRITICAL",
                {
                    "task_id": "test_429_02",
                    "repo_id": "hermes_orchestrator",
                    "milestone_title": "API Quota Rate Limit",
                    "target_file": "clients/gemini.py",
                    "error": "HTTPError 429 Too Many Requests: ResourceExhausted quota exceeded. Synchronized retry storm detected across parallel workers.",
                },
            ),
            (
                "leaky_sandbox_boundary",
                "CRITICAL",
                {
                    "task_id": "test_sandbox_03",
                    "repo_id": "hermes_orchestrator",
                    "milestone_title": "Worktree Dirty State",
                    "target_file": "tasks/sandbox.py",
                    "error": "GitCommandError: error: untracked working tree files would be overwritten by checkout. Sandbox boundary breach detected outside workdir.",
                },
            ),
            (
                "missing_circuit_breaker",
                "HIGH",
                {
                    "task_id": "test_circuit_04",
                    "repo_id": "hermes_orchestrator",
                    "milestone_title": "Deterministic Loop Freeze",
                    "target_file": "core/loop.py",
                    "error": "ExecutionError: repeated fatal error with no progress across consecutive attempts. Maximum retries 5 exceeded without state change. Deterministic failure loop.",
                },
            ),
            (
                "god_task_overdecomposition",
                "MEDIUM",
                {
                    "task_id": "test_context_05",
                    "repo_id": "hermes_orchestrator",
                    "milestone_title": "Monolithic Prompt Overflow",
                    "target_file": "tasks/refactor.py",
                    "error": "APIError: maximum context length exceeded. Token budget exhausted with prompt too large. Monolithic task exceeded context limit.",
                },
            ),
            (
                "catastrophic_backtracking_regex",
                "HIGH",
                {
                    "task_id": "test_regex_06",
                    "repo_id": "hermes_orchestrator",
                    "milestone_title": "Parser Hang",
                    "target_file": "parsers/log_scanner.py",
                    "error": "WorkerTimeoutError: worker thread hung in regex evaluation. Catastrophic backtracking detected in pattern matching multiline text.",
                },
            ),
            (
                "unhandled_upstream_protocol_drift",
                "MEDIUM",
                {
                    "task_id": "test_schema_07",
                    "repo_id": "hermes_orchestrator",
                    "milestone_title": "LLM Response Parser",
                    "target_file": "adapters/model.py",
                    "error": "ProtocolError: KeyError: 'choices'. Upstream protocol drift and schema mismatch in unexpected response structure from model API.",
                },
            ),
            (
                "phantom_success_illusion",
                "CRITICAL",
                {
                    "task_id": "test_phantom_08",
                    "repo_id": "hermes_orchestrator",
                    "milestone_title": "Silent Process Pass",
                    "target_file": "runners/exec.py",
                    "error": "VerificationError: command failed with exit code 2 but task marked success. Swallowed exception led to phantom success illusion with unverified artifact.",
                },
            ),
            (
                "unclassified_runtime_anomaly",
                "LOW",
                {
                    "task_id": "test_unknown_09",
                    "repo_id": "hermes_orchestrator",
                    "milestone_title": "Exogenous Blip",
                    "target_file": "sys/network.py",
                    "error": "Machine rebooted gracefully by cloud hypervisor at 04:00 UTC.",
                },
            ),
        ]

        print("[*] 1. Testing synthetic failure classification across all 8 anti-patterns + anomaly...")
        for expected_id, expected_sev, payload in synthetic_cases:
            record = engine.analyze_failure(payload)
            assert record.anti_pattern_id in (expected_id, "unclassified_anomaly"), (
                f"Classification mismatch for {payload['task_id']}: expected {expected_id}, got {record.anti_pattern_id}"
            )
            assert record.severity == expected_sev, (
                f"Severity mismatch for {payload['task_id']}: expected {expected_sev}, got {record.severity}"
            )
            assert len(record.lesson.assumption) > 0
            assert len(record.lesson.countermeasure) > 0
            assert len(record.lesson.rule_of_thumb) > 0
            print(f"  [+] {expected_id:<36} -> {record.lesson_id} ({record.severity}) [MATCH]")

        print("[*] 2. Testing recurrence tracking & dynamic severity escalation...")
        rec2 = engine.analyze_failure({
            "task_id": "test_lock_01_retry",
            "repo_id": "hermes_orchestrator",
            "error": "fatal: Unable to create '.git/index.lock': File exists.",
        })
        assert rec2.anti_pattern_id == "unfenced_shared_state"
        assert rec2.recurrence_count == 2, f"Expected recurrence 2, got {rec2.recurrence_count}"
        assert rec2.severity == "HIGH", f"Expected severity HIGH at count 2, got {rec2.severity}"

        # 3rd occurrence must escalate severity to CRITICAL
        rec3 = engine.analyze_failure({
            "task_id": "test_lock_01_retry_3",
            "repo_id": "hermes_orchestrator",
            "error": "fatal: Unable to create '.git/index.lock': File exists.",
        })
        assert rec3.recurrence_count == 3, f"Expected recurrence 3, got {rec3.recurrence_count}"
        assert rec3.severity == "CRITICAL", f"Expected severity CRITICAL at count 3, got {rec3.severity}"
        print(f"  [+] Recurrence count verified: #{rec3.recurrence_count} escalated to CRITICAL [MATCH]")

        print("[*] 3. Testing get_lesson_for_task retrieval...")
        fetched = engine.get_lesson_for_task("test_429_02")
        assert fetched is not None, "Failed to retrieve lesson for test_429_02"
        assert fetched["anti_pattern_id"] == "thundering_herd_retry"
        assert "rule_of_thumb" in fetched["lesson"]
        assert engine.get_lesson_for_task("non_existent_task") is None
        print("  [+] Query by task_id verified.")

        print("[*] 4. Testing batch_analyze_registry on JSONL input...")
        registry_data = [
            {
                "task_id": "batch_task_01",
                "error": "KeyError: 'candidates' in upstream protocol drift schema mismatch",
            },
            {
                "task_id": "batch_task_02",
                "error": "Maximum retries 10 exceeded without state change. Deterministic failure loop.",
            },
        ]
        with open(test_reg_path, "w", encoding="utf-8") as f:
            for item in registry_data:
                f.write(json.dumps(item) + "\n")

        batch_results = engine.batch_analyze_registry(test_reg_path)
        assert len(batch_results) == 2, f"Expected 2 batch results, got {len(batch_results)}"
        assert batch_results[0].anti_pattern_id == "unhandled_upstream_protocol_drift"
        assert batch_results[1].anti_pattern_id == "missing_circuit_breaker"
        print(f"  [+] Batch analysis successfully processed {len(batch_results)} tasks.")

        print("[*] 5. Testing atomic JSON persistence and schema validity...")
        saved_json = engine.save_lessons_json()
        assert saved_json.exists(), "JSON file was not written"
        with open(saved_json, "r", encoding="utf-8") as f:
            persisted = json.load(f)
        assert persisted["version"] == "1.0.0"
        assert persisted["total_records"] >= 12
        assert len(persisted["records"]) >= 12
        print(f"  [+] JSON persistence verified ({persisted['total_records']} records stored).")

        print("[*] 6. Testing executive Markdown digest rendering & Kanban warnings...")
        saved_md = engine.render_markdown_digest()
        assert saved_md.exists(), "Markdown digest was not written"
        md_text = saved_md.read_text(encoding="utf-8")
        assert "# Architectural Post-Mortem & Anti-Pattern Pedagogy Digest" in md_text
        assert "Systemic Architectural Warnings" in md_text
        assert "<details>" in md_text
        assert "Unfenced Shared State" in md_text
        assert "Thundering Herd Synchronized Retry Storm" in md_text
        print(f"  [+] Markdown digest verified ({len(md_text.splitlines())} lines rendered).")

        print("[*] 7. Testing state reload on fresh engine instance...")
        engine_reloaded = PostMortemPedagogyEngine(lessons_path=test_json_path, markdown_path=test_md_path)
        assert len(engine_reloaded._records) == len(engine._records)
        print(f"  [+] Successfully reloaded {len(engine_reloaded._records)} records from disk.")

        print("\n[PASS] PostMortemPedagogyEngine verification passed")
        sys.exit(0)

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
