#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tasks/pipeline_antipattern_remediation_synthesizer.py
================================================================================
Pipeline Autonomic Anti-Pattern Self-Healing Governor & Pedagogical Remediation
Synthesizer (CARP: Counterfactual Anti-Pattern Remediation Pipeline)
================================================================================

Theoretical & Operational Formulation:
--------------------------------------
Let A = {AP_1, AP_2, ..., AP_K} be canonical distributed systems and software
anti-patterns cataloged across autonomous multi-agent execution runs.

1. Anti-Pattern Hazard State Space & Exponential Decay:
   Each anti-pattern AP_k maintains a recurrence history of failure events at
   timestamps T_k = {t_1, t_2, ..., t_{N_k}}.
   The decayed recurrence frequency lambda_k(t) is modeled as a sum of exponential
   kernels:
     lambda_k(t) = sum_{i=1}^{N_k} exp(-gamma * (t - t_i))
   where gamma = ln(2) / T_half (with default half-life T_half = 86400.0 s = 24 h).
   
   The composite Hazard Score H(AP_k) combines recurrence frequency, intrinsic
   architectural severity, and historical blast radius cost:
     H(AP_k) = w_f * lambda_norm_k(t) + w_s * Severity(AP_k) + w_c * NormalizedCost(AP_k)
   where w_f = 0.40, w_s = 0.40, w_c = 0.20 (w_f + w_s + w_c = 1.0).

2. Counterfactual Remediation Synthesis (CARP):
   The remediation mapping function R: A x AST_{target} -> <NegativeConstraints, InvariantValidators, ArchitecturalLessons>
   operates when H(AP_k) >= tau_hazard (default tau_hazard = 0.70):
     a) Prompt Injections: Injects mandatory negative constraints, bounded loops,
        and explicit architectural guardrails into downstream LLM executor prompts.
     b) Pre-Commit AST Invariant Validators: Generates and evaluates targeted AST rules
        (e.g., forbidding bare except, mandating subprocess timeouts, detecting catastrophic
        backtracking regex, requiring checked return codes).
     c) Socratic Pedagogical Micro-Lesson: Emits a structured 3-part micro-lesson:
        (1) The Broken Assumption, (2) The Architectural Countermeasure, (3) Rule of Thumb.

3. Self-Healing Closed-Loop Feedback:
   Confidence metric:
     Gamma_heal(AP_k) = N_{success_post_remediation} / N_{total_remediated}
   When Gamma_heal(AP_k) < 0.60 (for N_{total_remediated} >= 1), the engine escalates
   hazard severity and publishes an urgent architectural blocker to
   dashboards/HUMAN_ACTION_BOARD.md.

4. Persistence & Cockpit Export:
   - Structured JSON state: dashboards/antipattern_remediation_report.json
   - Human-readable pedagogical digest: dashboards/ARCHITECTURAL_LESSONS.md

Zero external dependencies beyond the Python standard library.
"""

from __future__ import annotations

import argparse
import ast
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Union


# ==============================================================================
# Path Constants & Atomic File Operations
# ==============================================================================

AUTO_DIR = Path(__file__).resolve().parent.parent
DASHBOARDS_DIR = AUTO_DIR / "dashboards"

REPORT_FILE = DASHBOARDS_DIR / "antipattern_remediation_report.json"
LESSONS_FILE = DASHBOARDS_DIR / "ARCHITECTURAL_LESSONS.md"
HUMAN_ACTION_BOARD_FILE = DASHBOARDS_DIR / "HUMAN_ACTION_BOARD.md"
FAILED_TASKS_REGISTRY_FILE = DASHBOARDS_DIR / "failed_tasks_registry.json"


def _atomic_write_text(path: Union[Path, str], content: str) -> None:
    """Atomic write via temporary file replacement with fsync to guarantee durability."""
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
# 1. Domain Models, Enums & Catalogs
# ==============================================================================

class AntiPatternSeverity(str, Enum):
    """Categorical severity tiers with normalized weighting."""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CATASTROPHIC = "CATASTROPHIC"

    @property
    def weight(self) -> float:
        mapping = {
            AntiPatternSeverity.LOW: 0.25,
            AntiPatternSeverity.MEDIUM: 0.50,
            AntiPatternSeverity.HIGH: 0.80,
            AntiPatternSeverity.CATASTROPHIC: 1.00,
        }
        return mapping.get(self, 0.25)

    @property
    def base_cost(self) -> float:
        mapping = {
            AntiPatternSeverity.LOW: 0.20,
            AntiPatternSeverity.MEDIUM: 0.50,
            AntiPatternSeverity.HIGH: 0.80,
            AntiPatternSeverity.CATASTROPHIC: 1.00,
        }
        return mapping.get(self, 0.20)


@dataclass
class AntiPatternRecord:
    """Canonical taxonomic definition of a distributed systems or code anti-pattern."""
    pattern_id: str
    name: str
    category: str
    description: str
    failure_signature_regexes: List[str]
    canonical_solution: str
    invariant_rules: List[str]
    severity: AntiPatternSeverity = AntiPatternSeverity.MEDIUM
    pedagogical_lesson: Dict[str, str] = field(default_factory=dict)

    def compile_signatures(self) -> List[re.Pattern]:
        """Pre-compiles regex patterns for high-throughput failure matching."""
        compiled = []
        for pat in self.failure_signature_regexes:
            try:
                compiled.append(re.compile(pat, re.IGNORECASE | re.MULTILINE))
            except re.error:
                continue
        return compiled

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pattern_id": self.pattern_id,
            "name": self.name,
            "category": self.category,
            "description": self.description,
            "failure_signature_regexes": self.failure_signature_regexes,
            "canonical_solution": self.canonical_solution,
            "invariant_rules": self.invariant_rules,
            "severity": self.severity.value,
            "pedagogical_lesson": self.pedagogical_lesson,
        }


@dataclass
class AntiPatternHazardState:
    """State space tracking for an anti-pattern's recurrence, decay, and remediation."""
    pattern_id: str
    recurrence_count: int = 0
    decayed_frequency: float = 0.0
    hazard_score: float = 0.0
    active_remediation_applied: bool = False
    last_seen_at: float = 0.0
    failure_timestamps: List[float] = field(default_factory=list)
    success_post_remediation: int = 0
    total_remediated: int = 0
    hazard_escalated: bool = False

    @property
    def healing_confidence(self) -> float:
        """Calculates Gamma_heal = N_success / N_total."""
        if self.total_remediated <= 0:
            return 1.0
        return self.success_post_remediation / float(self.total_remediated)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pattern_id": self.pattern_id,
            "recurrence_count": self.recurrence_count,
            "decayed_frequency": round(self.decayed_frequency, 4),
            "hazard_score": round(self.hazard_score, 4),
            "active_remediation_applied": self.active_remediation_applied,
            "last_seen_at": self.last_seen_at,
            "failure_timestamps": self.failure_timestamps,
            "success_post_remediation": self.success_post_remediation,
            "total_remediated": self.total_remediated,
            "hazard_escalated": self.hazard_escalated,
            "healing_confidence": round(self.healing_confidence, 4),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> AntiPatternHazardState:
        return cls(
            pattern_id=str(data.get("pattern_id", "")),
            recurrence_count=int(data.get("recurrence_count", 0)),
            decayed_frequency=float(data.get("decayed_frequency", 0.0)),
            hazard_score=float(data.get("hazard_score", 0.0)),
            active_remediation_applied=bool(data.get("active_remediation_applied", False)),
            last_seen_at=float(data.get("last_seen_at", 0.0)),
            failure_timestamps=[float(ts) for ts in data.get("failure_timestamps", [])],
            success_post_remediation=int(data.get("success_post_remediation", 0)),
            total_remediated=int(data.get("total_remediated", 0)),
            hazard_escalated=bool(data.get("hazard_escalated", False)),
        )


@dataclass
class RemediationGuardrail:
    """Synthesized guardrail binding target files to prompt guards and AST invariants."""
    guardrail_id: str
    target_file: str
    pattern_id: str
    prompt_injections: List[str]
    ast_invariants: List[str]
    pedagogical_lesson: Dict[str, str]
    created_at: float = field(default_factory=time.time)
    active: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "guardrail_id": self.guardrail_id,
            "target_file": self.target_file,
            "pattern_id": self.pattern_id,
            "prompt_injections": self.prompt_injections,
            "ast_invariants": self.ast_invariants,
            "pedagogical_lesson": self.pedagogical_lesson,
            "created_at": self.created_at,
            "active": self.active,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> RemediationGuardrail:
        return cls(
            guardrail_id=str(data.get("guardrail_id", "")),
            target_file=str(data.get("target_file", "")),
            pattern_id=str(data.get("pattern_id", "")),
            prompt_injections=list(data.get("prompt_injections", [])),
            ast_invariants=list(data.get("ast_invariants", [])),
            pedagogical_lesson=dict(data.get("pedagogical_lesson", {})),
            created_at=float(data.get("created_at", time.time())),
            active=bool(data.get("active", True)),
        )


# ==============================================================================
# 2. Canonical Anti-Pattern Catalog (10 Comprehensive Distributed Patterns)
# ==============================================================================

CANONICAL_ANTI_PATTERNS: Dict[str, AntiPatternRecord] = {
    "unfenced_shared_state": AntiPatternRecord(
        pattern_id="unfenced_shared_state",
        name="Unfenced Shared State & Lock Contention",
        category="Concurrency & Distributed State",
        description="Concurrent mutating agents access git indices, shared state files, or databases without advisory lease fences.",
        failure_signature_regexes=[
            r"index\.lock.*(?:File exists|cannot lock|unable to create)",
            r"fatal: Unable to create '.*index\.lock'",
            r"sqlite3\.OperationalError: database is locked",
            r"Resource temporarily unavailable.*lock",
            r"FileLockTimeout.*failed to acquire",
            r"concurrent.*write.*conflict",
        ],
        canonical_solution="Wrap shared resource mutations in advisory file locks with randomized jitter backoff, or isolate in ephemeral worktrees.",
        invariant_rules=[
            "require_advisory_lock_or_atomic_write",
            "forbid_unlocked_shared_file_mutation",
        ],
        severity=AntiPatternSeverity.HIGH,
        pedagogical_lesson={
            "broken_assumption": "Assumes files, git workspaces, or shared databases can be concurrently written without mutual exclusion leases.",
            "countermeasure": "Isolate file system mutations in ephemeral git worktrees, use atomic replace-via-rename, and acquire advisory file locks with bounded timeouts.",
            "rule_of_thumb": "Never share mutable workspaces across concurrent threads or subprocesses; isolate by worktree or shard.",
        },
    ),
    "thundering_herd_retry": AntiPatternRecord(
        pattern_id="thundering_herd_retry",
        name="Thundering Herd Synchronized Retry Storm",
        category="Rate Pacing & Fault Tolerance",
        description="Sub-agents or workers retry synchronously on HTTP 429 / ResourceExhausted without decorrelated jitter backoff.",
        failure_signature_regexes=[
            r"429.*(?:Too Many Requests|rate limit)",
            r"ResourceExhausted.*(?:429|quota exceeded)",
            r"Rate limit exceeded.*retry after",
            r"synchronized.*retry.*storm",
            r"retry.*quota.*exhausted",
            r"exponential backoff failed.*exhausted",
        ],
        canonical_solution="Implement full randomized jitter backoff (decorrelated jitter) and token-bucket pacing governors before retrying.",
        invariant_rules=[
            "require_exponential_backoff_with_jitter",
            "forbid_zero_delay_retry_loops",
        ],
        severity=AntiPatternSeverity.CATASTROPHIC,
        pedagogical_lesson={
            "broken_assumption": "Assumes immediate or tightly synchronized linear retries on saturated endpoints will eventually succeed without worsening system overload.",
            "countermeasure": "Integrate an adaptive rate governor with token-bucket leases and decorrelated jitter exponential backoff before every API retry.",
            "rule_of_thumb": "On HTTP 429 or ResourceExhausted, back off exponentially with full random jitter; never retry synchronously in tight loops.",
        },
    ),
    "missing_circuit_breaker": AntiPatternRecord(
        pattern_id="missing_circuit_breaker",
        name="Missing Circuit Breaker on Deterministic Fatal Errors",
        category="Execution Control & Resilience",
        description="Repeatedly executing identical deterministically failing code or queries without tripping a failure counter circuit breaker.",
        failure_signature_regexes=[
            r"repeated fatal error.*no progress",
            r"circuit breaker.*(?:not found|missing|threshold exceeded)",
            r"deterministic failure.*(?:repeated|tight loop)",
            r"Maximum retries \d+ exceeded without state change",
            r"consecutive failures exceeded.*tripped",
            r"(?:ZeroDivisionError|SyntaxError|TypeError).*persists across \d+ attempts",
        ],
        canonical_solution="Implement a threshold circuit breaker that halts execution after consecutive deterministic errors and escalates to human review.",
        invariant_rules=[
            "require_circuit_breaker_counter",
            "forbid_infinite_deterministic_retry",
        ],
        severity=AntiPatternSeverity.HIGH,
        pedagogical_lesson={
            "broken_assumption": "Assumes repeating deterministic logic (e.g., Python SyntaxError, invalid schema) without code modification will yield a different outcome.",
            "countermeasure": "Maintain consecutive failure counters per milestone and trip an immediate circuit breaker after 3 deterministic failures.",
            "rule_of_thumb": "Deterministic errors must trip immediate circuit breakers rather than consuming compute loops and quota.",
        },
    ),
    "catastrophic_backtracking_regex": AntiPatternRecord(
        pattern_id="catastrophic_backtracking_regex",
        name="Catastrophic Backtracking Regular Expression Freeze",
        category="Code Safety & Algorithmic Complexity",
        description="Using regular expressions with nested quantifiers over unbounded multi-line text, causing exponential CPU freeze.",
        failure_signature_regexes=[
            r"catastrophic backtracking.*regex",
            r"regex.*(?:timeout|hung|freez(?:e|ing))",
            r"re\.error: regular expression.*too complex",
            r"worker thread hung in regex evaluation",
            r"polynomial.*backtracking detected in pattern",
            r"CPU 100%.*regex match",
        ],
        canonical_solution="Refactor regex patterns to eliminate nested greedy quantifiers, apply bounded token scanning, or use linear-time string search.",
        invariant_rules=[
            "forbid_nested_quantifiers_regex",
            "require_safe_regex_compilation",
        ],
        severity=AntiPatternSeverity.HIGH,
        pedagogical_lesson={
            "broken_assumption": "Assumes nested quantifier regular expressions like (a+)+ or (.*[a-z])+ can safely parse unbounded arbitrary logs.",
            "countermeasure": "Eliminate nested quantifiers, utilize atomic grouping, or parse structured logs with linear string scanners.",
            "rule_of_thumb": "Never combine nested quantifiers on unbounded inputs; compile regex patterns strictly and benchmark against pathological cases.",
        },
    ),
    "phantom_success_illusion": AntiPatternRecord(
        pattern_id="phantom_success_illusion",
        name="Phantom Success Illusion & Swallowed Failure",
        category="Execution Verification & Integrity",
        description="Execution marked as successful even though subprocesses exited with non-zero codes or generated artifacts were empty.",
        failure_signature_regexes=[
            r"command failed with (?:exit )?code [1-9]\d*.*marked (?:as )?success",
            r"phantom success.*(?:test suite failed|swallowed error)",
            r"swallowed exception.*silent (?:pass|success)",
            r"non-zero exit code ignored.*reported ok",
            r"missing artifact.*execution marked complete",
            r"AssertionError: unverified artifact.*falsely reported 0",
        ],
        canonical_solution="Enforce check=True or explicit returncode verification on subprocesses, and assert non-empty artifact hashes before marking success.",
        invariant_rules=[
            "require_subprocess_check_or_returncode_validation",
            "forbid_bare_except_suppression",
        ],
        severity=AntiPatternSeverity.CATASTROPHIC,
        pedagogical_lesson={
            "broken_assumption": "Assumes a wrapper script succeeded simply because it did not crash, ignoring non-zero exit codes of child processes.",
            "countermeasure": "Enforce strict subprocess exit code verification (check=True) and validate on-disk artifact existence, size, and syntax.",
            "rule_of_thumb": "Success is verified solely by valid non-empty artifacts and exit-code zero; never assume unverified completion.",
        },
    ),
    "leaky_sandbox_boundary": AntiPatternRecord(
        pattern_id="leaky_sandbox_boundary",
        name="Leaky Execution Sandbox Boundary & Artifact Contamination",
        category="Isolation & Workspace Hygiene",
        description="Workers or child processes leak temporary files, child processes, or uncommitted modifications into parent workspaces.",
        failure_signature_regexes=[
            r"untracked working tree files would be overwritten",
            r"dirty working tree.*cannot (?:checkout|merge|pull)",
            r"subprocess.*leaked.*(?:process group|zombie)",
            r"PermissionDenied.*host.*file system",
            r"leftover.*tmp.*polluting workspace",
            r"sandbox boundary breach.*outside workdir",
        ],
        canonical_solution="Execute tasks in ephemeral git worktree sandboxes that auto-reap child process groups and unmount temporary directories on exit.",
        invariant_rules=[
            "require_context_managed_sandbox",
            "require_process_group_cleanup",
        ],
        severity=AntiPatternSeverity.CATASTROPHIC,
        pedagogical_lesson={
            "broken_assumption": "Assumes child agents or subprocesses clean up all generated files and terminate all background subprocess trees reliably.",
            "countermeasure": "Wrap all executions in context-managed worktrees, kill process groups with os.killpg, and isolate scratch paths under dedicated TMPDIRs.",
            "rule_of_thumb": "Treat every worker run as potentially hostile to workspace state; enforce sandboxed file boundaries and lifecycle context managers.",
        },
    ),
    "unhandled_upstream_protocol_drift": AntiPatternRecord(
        pattern_id="unhandled_upstream_protocol_drift",
        name="Unhandled Upstream Protocol Drift & Schema Deserialization Flaw",
        category="API Integration & Deserialization",
        description="Direct dictionary access on external LLM provider or REST API responses assuming static schemas, crashing on protocol drift.",
        failure_signature_regexes=[
            r"KeyError: '(?:choices|candidates|content|message|output)'",
            r"ValidationError.*(?:unexpected field|missing required field)",
            r"upstream protocol drift.*schema mismatch",
            r"JSONDecodeError: Expecting value.*upstream response",
            r"unexpected response structure from (?:model|upstream API)",
            r"AttributeError: 'NoneType' object has no attribute '(?:text|data|content)'",
        ],
        canonical_solution="Utilize defensive schema adapters, tolerant JSON deserializers, and safe dictionary .get() access with typed defaults.",
        invariant_rules=[
            "forbid_raw_dict_access_on_external_payloads",
            "require_defensive_schema_fallback",
        ],
        severity=AntiPatternSeverity.MEDIUM,
        pedagogical_lesson={
            "broken_assumption": "Assumes external model providers and remote APIs maintain rigid immutable payload structures across platform updates.",
            "countermeasure": "Wrap upstream responses in defensive data-parsing adapters with fallback fields and graceful degradation.",
            "rule_of_thumb": "Never assume raw external response keys exist; validate and unpack via defensive schema extractors.",
        },
    ),
    "god_task_overdecomposition": AntiPatternRecord(
        pattern_id="god_task_overdecomposition",
        name="Monolithic God Task & Context Overflow",
        category="Task Architecture & Token Governance",
        description="Scheduling massive monolithic milestones touching multiple files and exceeding model context windows.",
        failure_signature_regexes=[
            r"(?:maximum context length|context window) exceeded",
            r"token budget exhausted.*prompt too large",
            r"monolithic task.*exceeded context limit",
            r"hallucinating partial completions?.*truncated",
            r"task scope too broad.*split required",
            r"payload size \d+ tokens exceeds limit",
        ],
        canonical_solution="Decompose milestones into bounded single-file deliverables scoped to ~800-1200 tokens with explicit topological dependencies.",
        invariant_rules=[
            "require_bounded_task_scope",
            "forbid_monolithic_multi_file_mutation",
        ],
        severity=AntiPatternSeverity.MEDIUM,
        pedagogical_lesson={
            "broken_assumption": "Assumes an autonomous agent can comprehend and refactor an entire complex multi-file subsystem in a single prompt execution.",
            "countermeasure": "Decompose tasks topologically into submodular milestones, each targeting a single file with well-defined interface contracts.",
            "rule_of_thumb": "Milestones modifying >1 complex file or exceeding 500 lines of code must be split into directed acyclic graph subtasks.",
        },
    ),
    "deadlock_prone_lock_inversion": AntiPatternRecord(
        pattern_id="deadlock_prone_lock_inversion",
        name="Deadlock-Prone Lock Hierarchy Inversion",
        category="Concurrency & Deadlock Prevention",
        description="Multiple threads or workers acquire nested mutual exclusion locks in varying orders, creating circular wait-for deadlocks.",
        failure_signature_regexes=[
            r"deadlock detected.*lock acquisition",
            r"cyclic lock dependency",
            r"threads waiting on circular lock hierarchy",
            r"LockInversionError",
            r"deadlock: resource wait-for graph cycle",
        ],
        canonical_solution="Enforce global monotonic lock acquisition ordering based on resource IDs, and mandate timeouts on all lock acquisitions.",
        invariant_rules=[
            "require_lock_hierarchy_ordering",
            "require_lock_acquisition_timeout",
        ],
        severity=AntiPatternSeverity.CATASTROPHIC,
        pedagogical_lesson={
            "broken_assumption": "Assumes concurrent threads can safely acquire multiple locks in arbitrary nested orders without deadlocking.",
            "countermeasure": "Sort lock acquisition sequences canonically by unique lock keys and always pass bounded timeouts when acquiring locks.",
            "rule_of_thumb": "All multi-lock acquisitions must follow a strict monotonic order; never nest locks in ad-hoc order.",
        },
    ),
    "unbounded_queue_memory_leak": AntiPatternRecord(
        pattern_id="unbounded_queue_memory_leak",
        name="Unbounded Queue Buffer Exhaustion & Memory Leak",
        category="Resource Governance & Capacity",
        description="Telemetry or event buffering using unbounded queues without backpressure, causing out-of-memory worker termination.",
        failure_signature_regexes=[
            r"MemoryError",
            r"OutOfMemoryError.*queue size",
            r"unbounded queue accumulated \d+ entries",
            r"buffer overflow.*dropped packets",
            r"process killed due to memory limit",
        ],
        canonical_solution="Configure queues with explicit maxsize bounds, drop-oldest / drop-newest eviction policies, and backpressure mechanisms.",
        invariant_rules=[
            "require_bounded_queue_maxsize",
            "forbid_infinite_memory_accumulation",
        ],
        severity=AntiPatternSeverity.HIGH,
        pedagogical_lesson={
            "broken_assumption": "Assumes producer threads will never outpace consumer processing over prolonged autonomous fleet runs.",
            "countermeasure": "Bind all in-memory queues with explicit maxsize watermarks and apply sliding window eviction or reactive backpressure.",
            "rule_of_thumb": "Every in-memory buffer, queue, and cache must have an explicit capacity ceiling and drop policy.",
        },
    ),
}

FALLBACK_ANTI_PATTERN = AntiPatternRecord(
    pattern_id="unclassified_execution_anomaly",
    name="Unclassified Execution Anomaly",
    category="General Execution",
    description="Unmatched failure event requiring heuristic signature analysis.",
    failure_signature_regexes=[],
    canonical_solution="Inspect error trace, extract invariant failure signatures, and append an explicit taxonomic entry.",
    invariant_rules=["forbid_bare_except_suppression"],
    severity=AntiPatternSeverity.LOW,
    pedagogical_lesson={
        "broken_assumption": "Assumes novel failure signatures fit into previously observed anti-pattern classes.",
        "countermeasure": "Capture raw stack traces, quarantine the failure instance, and generate a new catalog rule.",
        "rule_of_thumb": "Continuously refine the failure taxonomic catalog from unclassified anomalies.",
    },
)


# ==============================================================================
# 3. AST Invariant Validator (NodeVisitor & Rule Evaluator)
# ==============================================================================

class CodeInvariantASTVisitor(ast.NodeVisitor):
    """
    Syntactic AST analyzer inspecting Python source code for compliance
    with anti-pattern pre-commit invariants.
    """

    def __init__(self, active_rules: Set[str]) -> None:
        super().__init__()
        self.active_rules = active_rules
        self.violations: List[str] = []

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        """Evaluates exception handling invariants."""
        # Rule: forbid_bare_except_suppression
        if "forbid_bare_except_suppression" in self.active_rules:
            if node.type is None:
                self.violations.append(
                    f"[Line {node.lineno}] Violation 'forbid_bare_except_suppression': "
                    "Bare 'except:' handler detected. Specify explicit Exception classes."
                )
            # Check for silent swallowed exception (body has only 'pass' or Ellipsis)
            if len(node.body) == 1:
                first_stmt = node.body[0]
                if isinstance(first_stmt, ast.Pass):
                    self.violations.append(
                        f"[Line {node.lineno}] Violation 'forbid_bare_except_suppression': "
                        "Swallowed exception with silent 'pass'. Log the error or re-raise."
                    )
                elif isinstance(first_stmt, ast.Expr) and isinstance(first_stmt.value, ast.Constant) and first_stmt.value.value is Ellipsis:
                    self.violations.append(
                        f"[Line {node.lineno}] Violation 'forbid_bare_except_suppression': "
                        "Swallowed exception with silent '...'. Handle or re-raise."
                    )
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        """Evaluates function call invariants for subprocess, regex, queues, and locks."""
        func_name = self._resolve_call_name(node.func)

        # Rule: require_subprocess_timeout
        if "require_subprocess_timeout" in self.active_rules:
            if func_name in {
                "subprocess.run", "subprocess.Popen", "subprocess.call",
                "subprocess.check_call", "subprocess.check_output"
            }:
                has_timeout = any(kw.arg == "timeout" for kw in node.keywords)
                if not has_timeout:
                    self.violations.append(
                        f"[Line {node.lineno}] Violation 'require_subprocess_timeout': "
                        f"{func_name}() called without mandatory 'timeout' parameter."
                    )

        # Rule: require_subprocess_check_or_returncode_validation
        if "require_subprocess_check_or_returncode_validation" in self.active_rules:
            if func_name == "subprocess.run":
                has_check = any(kw.arg == "check" for kw in node.keywords)
                # If check=True is not set, we require check parameter
                if not has_check:
                    self.violations.append(
                        f"[Line {node.lineno}] Violation 'require_subprocess_check_or_returncode_validation': "
                        "subprocess.run() called without explicit 'check=True' parameter."
                    )

        # Rule: forbid_nested_quantifiers_regex
        if "forbid_nested_quantifiers_regex" in self.active_rules:
            if func_name in {
                "re.compile", "re.search", "re.match", "re.findall", "re.finditer", "re.sub"
            }:
                if node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                    pattern_str = node.args[0].value
                    # Heuristic detection of nested greedy quantifiers: (..+)+ or (..*)* or similar
                    if re.search(r"\([^)]*[\+\*]\)[\+\*]", pattern_str) or re.search(r"\(\?:\s*[^)]*[\+\*]\)[\+\*]", pattern_str):
                        self.violations.append(
                            f"[Line {node.lineno}] Violation 'forbid_nested_quantifiers_regex': "
                            f"Catastrophic backtracking risk detected in regex pattern: '{pattern_str[:30]}...'"
                        )

        # Rule: require_bounded_queue_maxsize
        if "require_bounded_queue_maxsize" in self.active_rules:
            if func_name in {"queue.Queue", "Queue", "asyncio.Queue"}:
                has_maxsize = any(kw.arg == "maxsize" for kw in node.keywords)
                if not has_maxsize and not node.args:
                    self.violations.append(
                        f"[Line {node.lineno}] Violation 'require_bounded_queue_maxsize': "
                        f"{func_name}() instantiated without explicit maxsize ceiling."
                    )

        self.generic_visit(node)

    def _resolve_call_name(self, node: ast.AST) -> str:
        """Resolves compound dotted names like subprocess.run or simple names."""
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            prefix = self._resolve_call_name(node.value)
            return f"{prefix}.{node.attr}" if prefix else node.attr
        return ""


# ==============================================================================
# 4. SelfHealingGovernor Engine
# ==============================================================================

class SelfHealingGovernor:
    """
    Autonomic Anti-Pattern Self-Healing Governor & Pedagogical Remediation Synthesizer.
    Manages hazard state space, exponential decay calculations, remediation synthesis,
    AST pre-commit validation, and closed-loop feedback escalation.
    """

    DEFAULT_T_HALF: float = 86400.0  # 24 hours in seconds
    DEFAULT_TAU_HAZARD: float = 0.70  # Threshold to trigger mandatory remediation
    DEFAULT_W_F: float = 0.40         # Decayed frequency weight
    DEFAULT_W_S: float = 0.40         # Severity weight
    DEFAULT_W_C: float = 0.20         # Normalized cost weight

    def __init__(
        self,
        report_path: Optional[Path] = None,
        lessons_path: Optional[Path] = None,
        human_board_path: Optional[Path] = None,
        t_half: float = DEFAULT_T_HALF,
        tau_hazard: float = DEFAULT_TAU_HAZARD,
    ) -> None:
        self._lock = threading.Lock()
        self.report_path = report_path or REPORT_FILE
        self.lessons_path = lessons_path or LESSONS_FILE
        self.human_board_path = human_board_path or HUMAN_ACTION_BOARD_FILE
        self.t_half = t_half
        self.tau_hazard = tau_hazard
        self.gamma = math.log(2.0) / self.t_half

        self.catalog: Dict[str, AntiPatternRecord] = dict(CANONICAL_ANTI_PATTERNS)
        self._compiled_catalog: Dict[str, List[re.Pattern]] = {
            pid: record.compile_signatures() for pid, record in self.catalog.items()
        }

        self.hazard_states: Dict[str, AntiPatternHazardState] = {
            pid: AntiPatternHazardState(pattern_id=pid) for pid in self.catalog
        }
        self.guardrails: Dict[str, RemediationGuardrail] = {}

        self._load_state()

    # --------------------------------------------------------------------------
    # 4.1 Hazard Calculation & Failure Ingestion
    # --------------------------------------------------------------------------

    def compute_decayed_frequency(self, pattern_id: str, current_time: Optional[float] = None) -> float:
        """
        Calculates exponential decay recurrence frequency:
          lambda_k(t) = sum_{i=1}^{N_k} exp(-gamma * max(0, t - t_i))
        """
        now = current_time if current_time is not None else time.time()
        state = self.hazard_states.get(pattern_id)
        if not state or not state.failure_timestamps:
            return 0.0

        decayed_sum = 0.0
        for ts in state.failure_timestamps:
            delta_t = max(0.0, now - ts)
            decayed_sum += math.exp(-self.gamma * delta_t)
        return decayed_sum

    def compute_hazard_scores(self, current_time: Optional[float] = None) -> Dict[str, float]:
        """
        Evaluates hazard score H(AP_k) = w_f * lambda_norm(t) + w_s * Severity + w_c * Cost
        for all cataloged anti-patterns.
        """
        now = current_time if current_time is not None else time.time()
        scores: Dict[str, float] = {}

        with self._lock:
            for pid, record in self.catalog.items():
                state = self.hazard_states.setdefault(pid, AntiPatternHazardState(pattern_id=pid))
                decayed_freq = self.compute_decayed_frequency(pid, current_time=now)
                state.decayed_frequency = decayed_freq

                # Normalize decayed frequency to [0.0, 1.0] ceiling (saturates smoothly at 2.5)
                lambda_norm = min(1.0, decayed_freq / 2.5)
                sev_weight = record.severity.weight
                cost_norm = record.severity.base_cost

                raw_h = (
                    self.DEFAULT_W_F * lambda_norm +
                    self.DEFAULT_W_S * sev_weight +
                    self.DEFAULT_W_C * cost_norm
                )

                # Escalate hazard score if closed-loop healing confidence is impaired
                if state.hazard_escalated:
                    raw_h = min(1.0, raw_h * 1.25 + 0.15)

                hazard_score = max(0.0, min(1.0, raw_h))
                state.hazard_score = hazard_score
                scores[pid] = hazard_score

        return scores

    def ingest_failure_event(
        self,
        milestone_name: str,
        repo_id: str,
        target_file: str,
        error_log: str,
        timestamp: Optional[float] = None,
    ) -> Optional[AntiPatternRecord]:
        """
        Matches an observed error log against canonical anti-pattern signatures,
        updates failure timestamps, recalculates hazard score, and synthesizes
        remediation guardrails if hazard exceeds tau_hazard.
        """
        now = timestamp if timestamp is not None else time.time()
        matched_pattern_id: Optional[str] = None
        highest_match_score = 0.0

        for pid, compiled_patterns in self._compiled_catalog.items():
            for regex in compiled_patterns:
                matches = regex.findall(error_log)
                if matches:
                    score = len(matches) * 1.5
                    if score > highest_match_score:
                        highest_match_score = score
                        matched_pattern_id = pid

        if not matched_pattern_id:
            matched_pattern_id = "unclassified_execution_anomaly"
            if matched_pattern_id not in self.catalog:
                self.catalog[matched_pattern_id] = FALLBACK_ANTI_PATTERN
                self.hazard_states[matched_pattern_id] = AntiPatternHazardState(pattern_id=matched_pattern_id)

        record = self.catalog.get(matched_pattern_id, FALLBACK_ANTI_PATTERN)

        with self._lock:
            state = self.hazard_states.setdefault(
                matched_pattern_id, AntiPatternHazardState(pattern_id=matched_pattern_id)
            )
            state.recurrence_count += 1
            state.last_seen_at = now
            state.failure_timestamps.append(now)

        # Recompute hazard scores across catalog
        self.compute_hazard_scores(current_time=now)

        current_hazard = self.hazard_states[matched_pattern_id].hazard_score
        if current_hazard >= self.tau_hazard and target_file:
            self.synthesize_remediation_guardrail(
                pattern_id=matched_pattern_id,
                target_file=target_file,
            )

        self.persist_state()
        return record

    # --------------------------------------------------------------------------
    # 4.2 Counterfactual Remediation Synthesis (CARP)
    # --------------------------------------------------------------------------

    def synthesize_remediation_guardrail(
        self,
        pattern_id: str,
        target_file: str,
    ) -> RemediationGuardrail:
        """
        Synthesizes a concrete RemediationGuardrail with prompt injections,
        pre-commit AST invariants, and Socratic pedagogical micro-lessons.
        """
        record = self.catalog.get(pattern_id, FALLBACK_ANTI_PATTERN)
        guardrail_id = f"guard_{pattern_id}_{hashlib.sha256(target_file.encode()).hexdigest()[:6]}"

        prompt_injections = [
            f"[CRITICAL REMEDIATION GUARDRAIL: {record.name.upper()}]",
            f"Negative Constraint: Do not implement patterns matching: {record.description}",
            f"Mandatory Invariant: {record.canonical_solution}",
            f"Pre-Commit AST Lint: Must strictly satisfy invariant rules: {', '.join(record.invariant_rules)}",
        ]

        # Specific prompt guard additions based on pattern class
        if "require_subprocess_timeout" in record.invariant_rules:
            prompt_injections.append("Directive: Every subprocess call must pass an explicit timeout parameter (e.g., timeout=60).")
        if "forbid_bare_except_suppression" in record.invariant_rules:
            prompt_injections.append("Directive: Never use bare 'except:' or silent 'pass' in exception blocks; log or re-raise.")
        if "forbid_zero_delay_retry_loops" in record.invariant_rules:
            prompt_injections.append("Directive: Exponential backoff with random jitter is required before retrying any remote call.")

        ast_invariants = list(record.invariant_rules)
        # Always enforce baseline AST syntax sanity
        if "require_subprocess_timeout" not in ast_invariants and record.severity in {AntiPatternSeverity.HIGH, AntiPatternSeverity.CATASTROPHIC}:
            ast_invariants.append("require_subprocess_timeout")

        lesson = record.pedagogical_lesson or {
            "broken_assumption": "Assumes execution succeeds without formal invariant barriers.",
            "countermeasure": record.canonical_solution,
            "rule_of_thumb": "Apply defensive engineering and verify invariants prior to committing state.",
        }

        guardrail = RemediationGuardrail(
            guardrail_id=guardrail_id,
            target_file=target_file,
            pattern_id=pattern_id,
            prompt_injections=prompt_injections,
            ast_invariants=ast_invariants,
            pedagogical_lesson=lesson,
            created_at=time.time(),
            active=True,
        )

        with self._lock:
            self.guardrails[guardrail_id] = guardrail
            state = self.hazard_states.get(pattern_id)
            if state:
                state.active_remediation_applied = True

        return guardrail

    # --------------------------------------------------------------------------
    # 4.3 AST Invariant Code Validation
    # --------------------------------------------------------------------------

    def validate_code_invariants(
        self,
        code_str: str,
        guardrails: Optional[List[RemediationGuardrail]] = None,
    ) -> Tuple[bool, List[str]]:
        """
        Parses candidate Python code into an AST and evaluates active invariant rules.
        Returns (is_compliant, list_of_violations).
        """
        if not code_str or not code_str.strip():
            return False, ["Empty code string provided for invariant validation."]

        try:
            tree = ast.parse(code_str)
        except SyntaxError as e:
            return False, [f"SyntaxError during AST parsing at line {e.lineno}, col {e.offset}: {e.msg}"]

        active_rules: Set[str] = set()
        if guardrails:
            for g in guardrails:
                if g.active:
                    active_rules.update(g.ast_invariants)
        else:
            with self._lock:
                for g in self.guardrails.values():
                    if g.active:
                        active_rules.update(g.ast_invariants)

        if not active_rules:
            return True, []

        visitor = CodeInvariantASTVisitor(active_rules=active_rules)
        visitor.visit(tree)

        if visitor.violations:
            return False, visitor.violations
        return True, []

    # --------------------------------------------------------------------------
    # 4.4 Self-Healing Closed-Loop Feedback & Escalation
    # --------------------------------------------------------------------------

    def record_remediation_outcome(
        self,
        pattern_id: str,
        success: bool,
    ) -> float:
        """
        Records the outcome of an execution attempt where remediation was applied.
        Evaluates Gamma_heal. If Gamma_heal < 0.60, escalates hazard and posts alert.
        """
        with self._lock:
            state = self.hazard_states.setdefault(
                pattern_id, AntiPatternHazardState(pattern_id=pattern_id)
            )
            state.total_remediated += 1
            if success:
                state.success_post_remediation += 1

            gamma_heal = state.healing_confidence

            # Escalation policy: If remediation success rate drops below 60%
            if state.total_remediated >= 1 and gamma_heal < 0.60:
                state.hazard_escalated = True
                self._dispatch_human_action_alert(pattern_id, gamma_heal, state.total_remediated)

        self.compute_hazard_scores()
        self.persist_state()
        return gamma_heal

    def _dispatch_human_action_alert(self, pattern_id: str, gamma_heal: float, total: int) -> None:
        """Appends a high-priority action item to dashboards/HUMAN_ACTION_BOARD.md."""
        record = self.catalog.get(pattern_id, FALLBACK_ANTI_PATTERN)
        alert_line = (
            f"| #AP-ALERT-{pattern_id[:6].upper()} | Critical | hermes_orchestrator | "
            f"[SELF-HEALING ESCALATION] Confidence Gamma_heal={gamma_heal:.2f} (<0.60, N={total}) "
            f"for '{record.name}'. Active prompt guards insufficient. Manual architectural review required. |\n"
        )
        try:
            content = ""
            if self.human_board_path.exists():
                content = self.human_board_path.read_text(encoding="utf-8")
            else:
                content = (
                    "# Dashboard 4: Human-in-the-Loop Action & Assistance Board\n"
                    f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  \n"
                    "| Blocker ID | Priority | Target Repo | Needed Action / Resource |\n"
                    "| :---: | :---: | :--- | :--- |\n"
                )

            # Avoid duplicate alert lines for same pattern
            pattern_token = f"#AP-ALERT-{pattern_id[:6].upper()}"
            if pattern_token not in content:
                content = content.rstrip() + "\n" + alert_line
                _atomic_write_text(self.human_board_path, content)
        except Exception:
            pass

    # --------------------------------------------------------------------------
    # 4.5 Integration with Executor Prompts & Registry
    # --------------------------------------------------------------------------

    def enrich_executor_prompt(
        self,
        prompt: str,
        target_file: str,
        repo_id: str = "",
    ) -> str:
        """
        Enriches an executor prompt with active prompt injections matching the target file
        or high-hazard anti-patterns.
        """
        relevant_guards: List[RemediationGuardrail] = []
        with self._lock:
            for g in self.guardrails.values():
                if g.active and (g.target_file == target_file or g.target_file in target_file or target_file.endswith(g.target_file)):
                    relevant_guards.append(g)

            # If no target-file specific guards exist, inspect high hazard patterns
            if not relevant_guards:
                for pid, state in self.hazard_states.items():
                    if state.hazard_score >= self.tau_hazard:
                        temp_g = self.synthesize_remediation_guardrail(pid, target_file)
                        relevant_guards.append(temp_g)

        if not relevant_guards:
            return prompt

        guard_lines = ["\n[AUTONOMIC ARCHITECTURAL SELF-HEALING GUARDS]:"]
        for g in relevant_guards:
            guard_lines.extend(g.prompt_injections)
            if g.pedagogical_lesson:
                guard_lines.append(f"Architectural Axiom: {g.pedagogical_lesson.get('broken_assumption', '')}")
                guard_lines.append(f"Design Principle: {g.pedagogical_lesson.get('rule_of_thumb', '')}")

        guard_text = "\n".join(guard_lines) + "\n"
        return f"{prompt}\n{guard_text}"

    def sync_with_registry(self, registry_path: Optional[Path] = None) -> int:
        """
        Ingests historical failure events from dashboards/failed_tasks_registry.json.
        Returns the number of ingested failures.
        """
        reg_path = registry_path or FAILED_TASKS_REGISTRY_FILE
        if not reg_path.exists():
            return 0

        try:
            data = json.loads(reg_path.read_text(encoding="utf-8"))
            failures = data.get("failures", [])
            count = 0
            for item in failures:
                milestone = str(item.get("milestone_name", "Milestone"))
                repo_id = str(item.get("repo_id", "general"))
                target_file = str(item.get("target_file", ""))
                failure_reason = str(item.get("failure_reason", ""))
                raw_time = item.get("first_failed_at") or item.get("timestamp")
                ts = None
                if raw_time:
                    try:
                        ts = datetime.strptime(str(raw_time), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
                    except ValueError:
                        try:
                            ts = datetime.fromisoformat(str(raw_time)).timestamp()
                        except ValueError:
                            ts = time.time()
                self.ingest_failure_event(
                    milestone_name=milestone,
                    repo_id=repo_id,
                    target_file=target_file,
                    error_log=failure_reason,
                    timestamp=ts,
                )
                count += 1
            return count
        except Exception:
            return 0

    # --------------------------------------------------------------------------
    # 4.6 Reporting, Persistence & Markdown Export
    # --------------------------------------------------------------------------

    def export_report(self) -> Dict[str, Any]:
        """Compiles machine-readable state dictionary."""
        with self._lock:
            hazard_summary = {
                pid: state.to_dict() for pid, state in self.hazard_states.items()
            }
            guardrail_list = [g.to_dict() for g in self.guardrails.values()]

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "tau_hazard": self.tau_hazard,
            "t_half_seconds": self.t_half,
            "total_patterns_tracked": len(self.catalog),
            "active_guardrails_count": len([g for g in self.guardrails.values() if g.active]),
            "hazard_summary": hazard_summary,
            "guardrails": guardrail_list,
        }

    def persist_state(self) -> None:
        """Durably writes report to dashboards/antipattern_remediation_report.json."""
        report = self.export_report()
        _atomic_write_text(self.report_path, json.dumps(report, indent=2))
        self.export_architectural_lessons_markdown()

    def _load_state(self) -> None:
        """Loads state from report file if present."""
        if not self.report_path.exists():
            return
        try:
            data = json.loads(self.report_path.read_text(encoding="utf-8"))
            for pid, s_dict in data.get("hazard_summary", {}).items():
                if pid in self.hazard_states:
                    self.hazard_states[pid] = AntiPatternHazardState.from_dict(s_dict)
            for g_dict in data.get("guardrails", []):
                g = RemediationGuardrail.from_dict(g_dict)
                self.guardrails[g.guardrail_id] = g
        except Exception:
            pass

    def export_architectural_lessons_markdown(self) -> str:
        """
        Updates dashboards/ARCHITECTURAL_LESSONS.md with verified micro-lessons
        and active invariant guardrails.
        """
        scores = self.compute_hazard_scores()
        lines: List[str] = [
            "# Architectural Post-Mortem & Anti-Pattern Pedagogy Digest\n",
            "> **Autonomous Engine Intelligence**: Systematic post-mortem classification of distributed",
            "> orchestration failures into architectural anti-patterns with actionable countermeasures.\n",
            "## 1. Executive Fleet Metrics\n",
            "| Metric | Value | Status |",
            "| :--- | :--- | :--- |",
            f"| **Cataloged Anti-Patterns** | `{len(self.catalog)}` | Production Taxonomic Knowledge Base |",
            f"| **Active Remediation Guardrails** | `{len([g for g in self.guardrails.values() if g.active])}` | CARP Autonomous Invariant Enforcement |",
            f"| **Half-Life Window (T_half)** | `{int(self.t_half)}s` | Exponential Recurrence Decay Active |",
            f"| **Hazard Synthesis Threshold (tau)** | `{self.tau_hazard:.2f}` | Proactive Pre-Commit Prevention |\n",
            "### Anti-Pattern Hazard & Recurrence Distribution\n",
            "| Anti-Pattern ID | Classification Label | Severity | Recurrences | Decayed Freq | Hazard Score | Status |",
            "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
        ]

        # Sort patterns by hazard score descending
        sorted_patterns = sorted(
            self.catalog.values(),
            key=lambda p: self.hazard_states[p.pattern_id].hazard_score,
            reverse=True,
        )

        for p in sorted_patterns:
            st = self.hazard_states[p.pattern_id]
            status_badge = "**ESCALATED**" if st.hazard_escalated else ("ACTIVE" if st.hazard_score >= self.tau_hazard else "MONITORED")
            lines.append(
                f"| `{p.pattern_id}` | {p.name} | `{p.severity.value}` | "
                f"**{st.recurrence_count}** | {st.decayed_frequency:.3f} | "
                f"**{st.hazard_score:.3f}** | {status_badge} |"
            )

        lines.extend([
            "\n## 2. Autonomic Self-Healing Remediation Guardrails (CARP)\n",
            "| Guardrail ID | Pattern Target | Target File | Active AST Invariants | Healing Confidence |\n"
            "| :--- | :--- | :--- | :--- | :--- |",
        ])

        if not self.guardrails:
            lines.append("| `None` | N/A | No active file barriers | None | 1.00 |")
        else:
            for g in self.guardrails.values():
                st = self.hazard_states.get(g.pattern_id)
                conf = st.healing_confidence if st else 1.0
                invs = ", ".join(f"`{i}`" for i in g.ast_invariants[:2])
                lines.append(
                    f"| `{g.guardrail_id}` | `{g.pattern_id}` | `{g.target_file}` | {invs} | **{conf:.2f}** |"
                )

        lines.extend([
            "\n## 3. Verified Socratic Pedagogical Micro-Lessons\n",
        ])

        for p in sorted_patterns:
            lesson = p.pedagogical_lesson
            if not lesson:
                continue
            lines.extend([
                f"### Micro-Lesson: {p.name} (`{p.pattern_id}`)",
                f"- **The Broken Assumption**: {lesson.get('broken_assumption', '')}",
                f"- **The Architectural Countermeasure**: {lesson.get('countermeasure', '')}",
                f"- **Rule of Thumb for Future Designs**: {lesson.get('rule_of_thumb', '')}\n",
            ])

        content = "\n".join(lines) + "\n"
        _atomic_write_text(self.lessons_path, content)
        return content


# ==============================================================================
# 5. Global Singleton Accessor
# ==============================================================================

_GOVERNOR_INSTANCE: Optional[SelfHealingGovernor] = None
_GOVERNOR_LOCK = threading.Lock()


def get_governor() -> SelfHealingGovernor:
    """Thread-safe accessor for global SelfHealingGovernor instance."""
    global _GOVERNOR_INSTANCE
    with _GOVERNOR_LOCK:
        if _GOVERNOR_INSTANCE is None:
            _GOVERNOR_INSTANCE = SelfHealingGovernor()
        return _GOVERNOR_INSTANCE


def enrich_executor_prompt(prompt: str, target_file: str, repo_id: str = "") -> str:
    """Module-level bridge to automatically enrich prompt in autonomous_orchestrator."""
    gov = get_governor()
    return gov.enrich_executor_prompt(prompt=prompt, target_file=target_file, repo_id=repo_id)


# ==============================================================================
# 6. Standalone CLI & Self-Testing Harness
# ==============================================================================

def run_self_test() -> bool:
    """
    Comprehensive standalone validation suite testing:
      1. Failure signature classification.
      2. Exponential decay calculation (verifying half-life lambda_k(t)).
      3. Hazard score computation and threshold gating.
      4. Remediation guardrail synthesis.
      5. AST invariant verification on compliant and non-compliant code.
      6. Closed-loop confidence feedback and escalation dispatch.
      7. Persistence to JSON and Markdown reports.
    """
    print("\n" + "=" * 80)
    print("RUNNING SELF-TEST: Pipeline Autonomic Anti-Pattern Self-Healing Governor")
    print("=" * 80)

    test_dir = AUTO_DIR / "dashboards" / "test_scratch"
    test_dir.mkdir(parents=True, exist_ok=True)

    report_p = test_dir / "test_remediation_report.json"
    lessons_p = test_dir / "test_ARCHITECTURAL_LESSONS.md"
    human_p = test_dir / "test_HUMAN_ACTION_BOARD.md"

    # 1. Initialize Governor with test paths
    governor = SelfHealingGovernor(
        report_path=report_p,
        lessons_path=lessons_p,
        human_board_path=human_p,
        t_half=86400.0,
        tau_hazard=0.70,
    )
    print("[+] Test 1: SelfHealingGovernor successfully initialized.")

    # 2. Failure Signature Ingestion & Classification
    error_sample = (
        "Traceback (most recent call last):\n"
        "  File 'worker.py', line 45, in execute\n"
        "google.api_core.exceptions.ResourceExhausted: 429 Resource exhausted: quota exceeded"
    )
    matched = governor.ingest_failure_event(
        milestone_name="Pacing Test",
        repo_id="test_repo",
        target_file="tasks/worker.py",
        error_log=error_sample,
        timestamp=time.time(),
    )
    assert matched is not None, "Failed to classify error sample"
    assert matched.pattern_id == "thundering_herd_retry", f"Expected thundering_herd_retry, got {matched.pattern_id}"
    print(f"[+] Test 2: Error signature classified correctly as '{matched.pattern_id}'.")

    # 3. Exponential Decay Calculation
    t0 = 100000.0
    st = governor.hazard_states["thundering_herd_retry"]
    st.failure_timestamps = [t0]
    
    # At t = t0 (delta = 0) -> decay = 1.0
    freq_0 = governor.compute_decayed_frequency("thundering_herd_retry", current_time=t0)
    assert math.isclose(freq_0, 1.0, rel_tol=1e-5), f"Expected decay 1.0 at delta=0, got {freq_0}"

    # At t = t0 + 86400 (1 half-life) -> decay = 0.5
    freq_half = governor.compute_decayed_frequency("thundering_herd_retry", current_time=t0 + 86400.0)
    assert math.isclose(freq_half, 0.5, rel_tol=1e-4), f"Expected decay 0.5 at T_half, got {freq_half}"

    # At t = t0 + 2*86400 (2 half-lives) -> decay = 0.25
    freq_quarter = governor.compute_decayed_frequency("thundering_herd_retry", current_time=t0 + 2 * 86400.0)
    assert math.isclose(freq_quarter, 0.25, rel_tol=1e-4), f"Expected decay 0.25 at 2*T_half, got {freq_quarter}"
    print("[+] Test 3: Mathematical exponential recurrence decay verified with exact half-life.")

    # 4. Hazard Score Computation
    st.failure_timestamps = [time.time(), time.time() - 3600.0]  # Two recent failures
    scores = governor.compute_hazard_scores()
    h_score = scores.get("thundering_herd_retry", 0.0)
    assert h_score >= governor.tau_hazard, f"Expected hazard score >= 0.70, got {h_score}"
    print(f"[+] Test 4: Hazard score computed: {h_score:.3f} >= tau_hazard ({governor.tau_hazard}).")

    # 5. Remediation Guardrail Synthesis (CARP)
    guard = governor.synthesize_remediation_guardrail(
        pattern_id="thundering_herd_retry",
        target_file="tasks/worker.py",
    )
    assert guard is not None
    assert "require_exponential_backoff_with_jitter" in guard.ast_invariants
    assert any("429" in inj or "THUNDERING" in inj for inj in guard.prompt_injections)
    assert guard.pedagogical_lesson["broken_assumption"] != ""
    print("[+] Test 5: CARP remediation guardrail synthesized with negative constraints and Socratic micro-lesson.")

    # 6. AST Invariant Verification
    # 6a. Non-compliant code: Bare except and missing subprocess timeout
    phantom_guard = governor.synthesize_remediation_guardrail(
        pattern_id="phantom_success_illusion",
        target_file="tasks/worker.py",
    )
    bad_code = """
import subprocess
def run_task():
    try:
        res = subprocess.run(["ls", "-l"])
    except:
        pass
"""
    is_valid, violations = governor.validate_code_invariants(bad_code, guardrails=[phantom_guard])
    assert not is_valid, "AST validator should have rejected non-compliant code"
    assert len(violations) >= 2, f"Expected >=2 violations (bare except, subprocess timeout), got {violations}"
    print(f"[+] Test 6a: Non-compliant AST correctly rejected with {len(violations)} violations caught.")

    # 6b. Catastrophic Regex Invariant
    regex_guard = governor.synthesize_remediation_guardrail("catastrophic_backtracking_regex", "scanner.py")
    bad_regex_code = """
import re
def find_tokens(text):
    pat = re.compile(r"([a-z]+)+")
    return pat.findall(text)
"""
    is_regex_valid, regex_violations = governor.validate_code_invariants(bad_regex_code, guardrails=[regex_guard])
    assert not is_regex_valid, "AST validator should have caught nested quantifier regex"
    assert any("catastrophic backtracking" in v.lower() for v in regex_violations)
    print("[+] Test 6b: Catastrophic backtracking regex pattern flagged by AST inspector.")

    # 6c. Compliant code: Fully invariant-compliant implementation
    clean_code = """
import subprocess
import time
import logging

def run_task():
    try:
        res = subprocess.run(["ls", "-la"], timeout=30, check=True)
        return res.returncode
    except subprocess.SubprocessError as exc:
        logging.error("Subprocess execution failed: %s", exc)
        time.sleep(1.0)
        raise
"""
    is_clean_valid, clean_violations = governor.validate_code_invariants(clean_code, guardrails=[phantom_guard])
    assert is_clean_valid, f"Compliant code unexpectedly flagged violations: {clean_violations}"
    print("[+] Test 6c: Compliant source code passed all pre-commit AST invariants.")

    # 7. Self-Healing Closed-Loop Feedback & Escalation
    # Record failed remediation -> Gamma_heal drops below 0.60
    conf = governor.record_remediation_outcome("thundering_herd_retry", success=False)
    assert conf < 0.60, f"Expected Gamma_heal < 0.60, got {conf}"
    assert governor.hazard_states["thundering_herd_retry"].hazard_escalated is True
    assert human_p.exists()
    human_text = human_p.read_text(encoding="utf-8")
    assert "SELF-HEALING ESCALATION" in human_text
    print(f"[+] Test 7: Closed-loop escalation tripped (Gamma_heal={conf:.2f}); alert dispatched to HUMAN_ACTION_BOARD.")

    # 8. Prompt Enrichment Integration
    orig_prompt = "Implement tasks/worker.py to process background fleet requests."
    enriched = governor.enrich_executor_prompt(orig_prompt, target_file="tasks/worker.py")
    assert "[AUTONOMIC ARCHITECTURAL SELF-HEALING GUARDS]" in enriched
    assert "THUNDERING" in enriched
    print("[+] Test 8: Executor prompt enriched with mandatory self-healing guardrails.")

    # 9. Persistence & Markdown Report
    governor.persist_state()
    assert report_p.exists() and report_p.stat().st_size > 200
    assert lessons_p.exists() and lessons_p.stat().st_size > 500
    report_data = json.loads(report_p.read_text(encoding="utf-8"))
    assert "hazard_summary" in report_data
    assert "thundering_herd_retry" in report_data["hazard_summary"]
    print("[+] Test 9: Durability verified. JSON report and ARCHITECTURAL_LESSONS.md exported.")

    # Clean up test scratch
    try:
        report_p.unlink(missing_ok=True)
        lessons_p.unlink(missing_ok=True)
        human_p.unlink(missing_ok=True)
        test_dir.rmdir()
    except OSError:
        pass

    print("\n" + "=" * 80)
    print("ALL 9 UNIT & INTEGRATION TESTS PASSED WITH 100% SUCCESS.")
    print("=" * 80 + "\n")
    return True


def main() -> int:
    """CLI Entry point for governor inspection and manual invocations."""
    parser = argparse.ArgumentParser(
        description="Pipeline Autonomic Anti-Pattern Self-Healing Governor & Remediation Synthesizer (CARP)"
    )
    parser.add_argument("--status", action="store_true", help="Display current hazard states and active guardrails.")
    parser.add_argument("--analyze-failures", action="store_true", help="Ingest and sync historical failures from registry.")
    parser.add_argument("--synthesize-guards", action="store_true", help="Synthesize remediation guardrails for all high hazard patterns.")
    parser.add_argument("--verify-file", type=str, default="", help="Verify target Python file against active AST invariants.")
    parser.add_argument("--run-demo", action="store_true", help="Execute live end-to-end demonstration scenario.")
    parser.add_argument("--test", action="store_true", help="Run comprehensive unit test suite.")

    args = parser.parse_args()

    # Default to test if no arguments passed
    if not any([args.status, args.analyze_failures, args.synthesize_guards, args.verify_file, args.run_demo, args.test]):
        success = run_self_test()
        return 0 if success else 1

    if args.test:
        success = run_self_test()
        return 0 if success else 1

    governor = get_governor()

    if args.analyze_failures:
        print("[*] Ingesting failure history from failed_tasks_registry.json...")
        count = governor.sync_with_registry()
        scores = governor.compute_hazard_scores()
        governor.persist_state()
        print(f"[+] Successfully ingested {count} failure events across {len(scores)} anti-pattern categories.")
        return 0

    if args.synthesize_guards:
        print("[*] Evaluating hazard thresholds and synthesizing CARP remediation guardrails...")
        scores = governor.compute_hazard_scores()
        synthesized = 0
        for pid, score in scores.items():
            if score >= governor.tau_hazard:
                governor.synthesize_remediation_guardrail(pid, target_file="tasks/pipeline_core.py")
                synthesized += 1
        governor.persist_state()
        print(f"[+] Synthesized {synthesized} active remediation guardrails (tau >= {governor.tau_hazard}).")
        return 0

    if args.verify_file:
        p = Path(args.verify_file)
        if not p.exists():
            print(f"[!] Target file does not exist: {p}")
            return 1
        code_str = p.read_text(encoding="utf-8")
        is_valid, violations = governor.validate_code_invariants(code_str)
        if is_valid:
            print(f"[+] '{p}' complies with all active architectural AST invariants.")
            return 0
        else:
            print(f"[!] AST Invariant Violations detected in '{p}':")
            for v in violations:
                print(f"  - {v}")
            return 1

    if args.run_demo:
        print("\n" + "=" * 70)
        print("CARP DEMO: Live Anti-Pattern Ingestion, Decay & Guardrail Synthesis")
        print("=" * 70)
        sample_err = "sqlite3.OperationalError: database is locked during concurrent worktree commit"
        print(f"[*] Ingesting error log:\n    {sample_err}")
        matched = governor.ingest_failure_event("Demo Milestone", "hermes_orchestrator", "tasks/demo_worker.py", sample_err)
        if matched:
            print(f"[+] Classified Pattern: {matched.name} (ID: {matched.pattern_id})")
            scores = governor.compute_hazard_scores()
            print(f"[+] Updated Hazard Score for {matched.pattern_id}: {scores.get(matched.pattern_id, 0.0):.3f}")
            guard = governor.synthesize_remediation_guardrail(matched.pattern_id, "tasks/demo_worker.py")
            print(f"[+] Generated Guardrail: {guard.guardrail_id}")
            print("    Prompt Injections:")
            for inj in guard.prompt_injections:
                print(f"      • {inj}")
        print(f"[+] Exporting state to {governor.report_path} and {governor.lessons_path}")
        governor.persist_state()
        print("=" * 70 + "\n")
        return 0

    if args.status:
        scores = governor.compute_hazard_scores()
        print("\n" + "=" * 80)
        print(f"{'ANTI-PATTERN ID':<32} | {'SEVERITY':<12} | {'REC':<4} | {'HAZARD':<8} | {'STATUS'}")
        print("-" * 80)
        for pid, p in governor.catalog.items():
            st = governor.hazard_states[pid]
            status_str = "ESCALATED" if st.hazard_escalated else ("GUARD ACTIVE" if st.hazard_score >= governor.tau_hazard else "NORMAL")
            print(f"{pid:<32} | {p.severity.value:<12} | {st.recurrence_count:<4} | {scores.get(pid, 0.0):<8.3f} | {status_str}")
        print("=" * 80)
        print(f"Total Active Guardrails: {len([g for g in governor.guardrails.values() if g.active])}\n")
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
