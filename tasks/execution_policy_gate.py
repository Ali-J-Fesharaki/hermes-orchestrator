#!/usr/bin/env python3
"""
tasks/execution_policy_gate.py
================================================================================
Adaptive Execution Policy Gate & Anti-Regression Guardrail Engine
Repository: hermes_orchestrator

Provides pre-flight policy evaluation, blast-radius containment, regression
hazard detection, and adaptive constraint synthesis for autonomous agent
milestone execution.
================================================================================
"""

from __future__ import annotations

import os
import re
import json
import math
import time
import hashlib
import logging
from dataclasses import dataclass, field, asdict
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger("ExecutionPolicyGate")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [ExecutionPolicyGate] %(message)s"
    ))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


class GateStatus(str, Enum):
    APPROVED = "APPROVED"
    CONDITIONAL_APPROVAL = "CONDITIONAL_APPROVAL"
    REJECTED = "REJECTED"
    QUARANTINED = "QUARANTINED"
    MANUAL_REVIEW = "MANUAL_REVIEW"


class RiskLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


@dataclass
class AntiPatternMatch:
    pattern_id: str
    name: str
    category: str
    severity: RiskLevel
    similarity_score: float
    matched_tokens: List[str] = field(default_factory=list)
    description: str = ""
    remediation: str = ""

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["severity"] = self.severity.value
        return d


@dataclass
class GuardrailViolation:
    rule_id: str
    category: str
    severity: RiskLevel
    message: str
    context: Dict[str, Any] = field(default_factory=dict)
    remediation: str = ""

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["severity"] = self.severity.value
        return d


@dataclass
class PolicyVerdict:
    verdict: GateStatus
    allowed: bool
    risk_score: float
    risk_level: RiskLevel
    violations: List[GuardrailViolation] = field(default_factory=list)
    anti_patterns_detected: List[AntiPatternMatch] = field(default_factory=list)
    execution_constraints: Dict[str, Any] = field(default_factory=dict)
    remediation_guidance: List[str] = field(default_factory=list)
    evaluated_at: float = field(default_factory=time.time)
    milestone_id: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "verdict": self.verdict.value,
            "allowed": self.allowed,
            "risk_score": round(self.risk_score, 4),
            "risk_level": self.risk_level.value,
            "violations": [v.to_dict() for v in self.violations],
            "anti_patterns_detected": [p.to_dict() for p in self.anti_patterns_detected],
            "execution_constraints": self.execution_constraints,
            "remediation_guidance": self.remediation_guidance,
            "evaluated_at": self.evaluated_at,
            "milestone_id": self.milestone_id,
            "metadata": self.metadata,
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)


# Canonical Anti-Pattern & Regression Catalog
DEFAULT_ANTI_PATTERN_CATALOG: List[Dict[str, Any]] = [
    {
        "id": "AP-001",
        "name": "Destructive File Deletion",
        "category": "blast_radius",
        "severity": RiskLevel.CRITICAL,
        "keywords": ["rm -rf", "shutil.rmtree", "unlink", "drop database", "clean workspace", "delete all"],
        "regex": r"(?:rm\s+-(?:r|f|rf|fr)\s+[/~*]|shutil\.rmtree\s*\(|os\.remove\s*\(\s*['\"]\/)",
        "description": "Unbounded destructive filesystem or database deletion commands.",
        "remediation": "Utilize targeted file removal or safe staging / trash isolation."
    },
    {
        "id": "AP-002",
        "name": "Hardcoded Secrets and Plaintext Tokens",
        "category": "security",
        "severity": RiskLevel.CRITICAL,
        "keywords": ["bearer", "api_key", "secret_key", "password =", "ghp_", "sk-", "token ="],
        "regex": r"(?:['\"](?:ghp_[a-zA-Z0-9]{36}|sk-[a-zA-Z0-9]{20,}|AIza[0-9A-Za-z-_]{35})['\"]|(?:password|secret|token)\s*=\s*['\"][^'\"]{6,}['\"])",
        "description": "Embedding raw API keys, bearer tokens, or secrets directly in task spec.",
        "remediation": "Reference credentials via environment variables or encrypted secrets vaults."
    },
    {
        "id": "AP-003",
        "name": "Unbounded Recursive Execution Loop",
        "category": "stability",
        "severity": RiskLevel.HIGH,
        "keywords": ["while True", "recurse forever", "no timeout", "infinite retry", "retry indefinitely"],
        "regex": r"(?:while\s+True\s*:\s*(?!.*(?:sleep|break|timeout))|max_retries\s*=\s*(?:float\('inf'\)|-1|999999))",
        "description": "Execution patterns prone to runaway loops and resource exhaustion.",
        "remediation": "Enforce strict bounded iterations with maximum iteration caps and exponential backoff."
    },
    {
        "id": "AP-004",
        "name": "Global Context Monopolization",
        "category": "resource_governance",
        "severity": RiskLevel.MEDIUM,
        "keywords": ["read all repos", "dump full codebase", "unbounded prompt", "entire workspace context"],
        "regex": r"(?:read_file\s*\(\s*['\"](?:\.|\*).*['\"]\)|glob\('\*\*\/\*'\)|find\s+\.\s+-type\s+f)",
        "description": "Ingesting unbounded multi-repo codebases directly into single prompt turns.",
        "remediation": "Partition milestones into focused sub-tasks (< 1,200 words / chunked symbol slices)."
    },
    {
        "id": "AP-005",
        "name": "Silent Failure and Error Swallowing",
        "category": "resilience",
        "severity": RiskLevel.HIGH,
        "keywords": ["except: pass", "catch all ignore", "suppress error", "continue on error silent"],
        "regex": r"except(?:\s+Exception)?\s*:\s*(?:pass|return\s+None|logger\.debug)",
        "description": "Silent catch-all exception blocks causing unobservable execution drift.",
        "remediation": "Catch explicit domain exceptions, log tracebacks, and push records to FailedTaskRegistry."
    },
    {
        "id": "AP-006",
        "name": "Arbitrary Dynamic Code Execution",
        "category": "security",
        "severity": RiskLevel.CRITICAL,
        "keywords": ["eval(", "exec(", "__import__", "subprocess.call(shell=True)", "eval dynamic"],
        "regex": r"(?:eval\s*\(|exec\s*\(|subprocess\.(?:Popen|call|run)\s*\(.*shell\s*=\s*True)",
        "description": "Unsanitized dynamic code execution or arbitrary shell commands.",
        "remediation": "Use strongly typed dispatch vectors with validated parameter schemas."
    },
    {
        "id": "AP-007",
        "name": "Git History Mutation and Destructive Push",
        "category": "blast_radius",
        "severity": RiskLevel.CRITICAL,
        "keywords": ["git push --force", "push -f", "reset --hard HEAD~", "rebase -i root", "clean -fdx"],
        "regex": r"(?:git\s+push\s+.*(?:--force|-f)|git\s+reset\s+--hard|git\s+clean\s+-(?:f|fd|fdx))",
        "description": "Destructive VCS operations capable of wiping upstream repository commits.",
        "remediation": "Require atomic forward-only commits with fast-forward validation."
    },
    {
        "id": "AP-008",
        "name": "Unpinned Network Dependency Injection",
        "category": "reproducibility",
        "severity": RiskLevel.MEDIUM,
        "keywords": ["curl | bash", "wget | sh", "pip install latest", "npm install --force"],
        "regex": r"(?:curl\s+.*\|\s*(?:bash|sh)|wget\s+.*\|\s*(?:bash|sh)|pip\s+install\s+(?!.*==)[a-zA-Z0-9_\-]+)",
        "description": "Piping unverified remote scripts to shell or unpinned package installs.",
        "remediation": "Pin all dependencies in lockfiles and verify SHA256 checksums."
    }
]


class ExecutionPolicyGate:
    """
    Adaptive Execution Policy Gate & Anti-Regression Guardrail Engine.

    Evaluates proposed milestone specifications against regression catalogs,
    blast-radius limits, architectural invariants, and security compliance
    before allowing dispatch to execution backends.
    """

    def __init__(
        self,
        anti_pattern_catalog: Optional[List[Dict[str, Any]]] = None,
        failed_registry_path: Optional[str] = None,
        risk_threshold_reject: float = 0.65,
        risk_threshold_conditional: float = 0.35,
        max_allowed_file_scope: int = 12,
        strict_security_mode: bool = True
    ):
        self.catalog = anti_pattern_catalog or DEFAULT_ANTI_PATTERN_CATALOG
        self.failed_registry_path = Path(failed_registry_path) if failed_registry_path else None
        self.risk_threshold_reject = risk_threshold_reject
        self.risk_threshold_conditional = risk_threshold_conditional
        self.max_allowed_file_scope = max_allowed_file_scope
        self.strict_security_mode = strict_security_mode

        # Precompile regex rules in catalog
        self._compiled_rules: List[Tuple[Dict[str, Any], Optional[re.Pattern]]] = []
        for item in self.catalog:
            pattern = None
            if "regex" in item and item["regex"]:
                try:
                    pattern = re.compile(item["regex"], re.IGNORECASE | re.MULTILINE)
                except re.error as err:
                    logger.warning("Failed compiling regex for %s: %s", item.get("id"), err)
            self._compiled_rules.append((item, pattern))

    # -------------------------------------------------------------------------
    # Primary Pre-Flight Pipeline
    # -------------------------------------------------------------------------

    def evaluate(self, milestone_spec: Dict[str, Any]) -> PolicyVerdict:
        """
        Executes the end-to-end pre-flight policy evaluation pipeline on a
        milestone specification.

        Pipeline Stages:
        1. Spec Normalization & Schema Integrity Audit
        2. Anti-Pattern & Malicious Pattern Similarity Scan
        3. Blast Radius & Resource Footprint Analysis
        4. Historical Regression & Prior Failure Guardrail Audit
        5. Adaptive Execution Constraint Generation
        6. Final Risk Synthesis & Gate Verdict Determination
        """
        milestone_id = str(milestone_spec.get("id") or milestone_spec.get("milestone_id") or "M-UNKNOWN")
        spec_text = self._extract_full_text(milestone_spec)
        logger.info("Evaluating milestone: %s (length: %d chars)", milestone_id, len(spec_text))

        violations: List[GuardrailViolation] = []
        anti_patterns: List[AntiPatternMatch] = []
        remediation_guidance: List[str] = []

        # Stage 1: Schema Integrity & Required Directives Audit
        schema_violations = self._validate_schema_integrity(milestone_spec)
        violations.extend(schema_violations)

        # Stage 2: Anti-Pattern Similarity Scan
        detected_patterns = self._anti_pattern_similarity_scan(spec_text, self.catalog)
        anti_patterns.extend(detected_patterns)

        # Stage 3: Blast Radius & Structural Scope Assessment
        blast_violations, blast_score = self._evaluate_blast_radius(milestone_spec)
        violations.extend(blast_violations)

        # Stage 4: Historical Regression Audit
        regression_violations, regression_score = self._audit_regression_guardrails(milestone_spec, spec_text)
        violations.extend(regression_violations)

        # Stage 5: Compute Risk Score
        risk_score = self._compute_aggregate_risk(
            violations=violations,
            anti_patterns=anti_patterns,
            blast_score=blast_score,
            regression_score=regression_score
        )

        # Stage 6: Classify Risk Level & Verdict
        verdict, allowed, risk_level = self._determine_verdict(risk_score, violations, anti_patterns)

        # Stage 7: Synthesize Adaptive Execution Constraints
        constraints = self._synthesize_adaptive_constraints(
            verdict=verdict,
            risk_level=risk_level,
            milestone_spec=milestone_spec,
            anti_patterns=anti_patterns
        )

        # Collect consolidated remediation guidance
        for v in violations:
            if v.remediation and v.remediation not in remediation_guidance:
                remediation_guidance.append(v.remediation)
        for p in anti_patterns:
            if p.remediation and p.remediation not in remediation_guidance:
                remediation_guidance.append(p.remediation)

        return PolicyVerdict(
            verdict=verdict,
            allowed=allowed,
            risk_score=risk_score,
            risk_level=risk_level,
            violations=violations,
            anti_patterns_detected=anti_patterns,
            execution_constraints=constraints,
            remediation_guidance=remediation_guidance,
            milestone_id=milestone_id,
            metadata={
                "evaluator": "ExecutionPolicyGate_v2.0",
                "rules_evaluated": len(self.catalog),
                "strict_mode": self.strict_security_mode,
                "scope_files_count": len(milestone_spec.get("target_files", [])),
            }
        )

    # -------------------------------------------------------------------------
    # Anti-Pattern Similarity Scan
    # -------------------------------------------------------------------------

    def _anti_pattern_similarity_scan(
        self,
        spec_text: str,
        catalog: List[Dict[str, Any]]
    ) -> List[AntiPatternMatch]:
        """
        Scans specification text against known anti-patterns using hybrid
        structural regex matching and lexical token-set cosine/Jaccard similarity.
        """
        if not spec_text:
            return []

        matches: List[AntiPatternMatch] = []
        normalized_spec = spec_text.lower()
        spec_tokens = self._tokenize(normalized_spec)
        spec_token_set = set(spec_tokens)

        for rule, compiled_regex in self._compiled_rules:
            pattern_id = rule.get("id", "AP-UNKNOWN")
            name = rule.get("name", "Unknown Pattern")
            category = rule.get("category", "general")
            severity = rule.get("severity", RiskLevel.MEDIUM)
            description = rule.get("description", "")
            remediation = rule.get("remediation", "")

            matched_tokens: List[str] = []
            regex_matched = False

            # Check 1: Regex Precision Match
            if compiled_regex:
                regex_hits = compiled_regex.findall(spec_text)
                if regex_hits:
                    regex_matched = True
                    matched_tokens.extend([str(h)[:40] for h in regex_hits[:5]])

            # Check 2: Keyword Token-Set Overlap & Jaccard / Density Score
            keywords = [k.lower() for k in rule.get("keywords", [])]
            keyword_hits = 0
            for kw in keywords:
                if " " in kw:
                    if kw in normalized_spec:
                        keyword_hits += 1
                        matched_tokens.append(kw)
                else:
                    if kw in spec_token_set:
                        keyword_hits += 1
                        matched_tokens.append(kw)

            similarity_score = 0.0
            if keywords:
                lexical_ratio = keyword_hits / float(len(keywords))
                similarity_score = lexical_ratio

            if regex_matched:
                # Regex match elevates similarity score directly to high-confidence match
                similarity_score = max(similarity_score, 0.85 + (0.15 * min(1.0, len(matched_tokens) / 5.0)))

            # Threshold for registration
            if similarity_score >= 0.35 or regex_matched:
                matches.append(AntiPatternMatch(
                    pattern_id=pattern_id,
                    name=name,
                    category=category,
                    severity=severity,
                    similarity_score=round(min(1.0, similarity_score), 4),
                    matched_tokens=list(set(matched_tokens)),
                    description=description,
                    remediation=remediation
                ))

        # Sort matches by severity and similarity descending
        severity_order = {RiskLevel.CRITICAL: 4, RiskLevel.HIGH: 3, RiskLevel.MEDIUM: 2, RiskLevel.LOW: 1}
        matches.sort(
            key=lambda m: (severity_order.get(m.severity, 0), m.similarity_score),
            reverse=True
        )
        return matches

    # -------------------------------------------------------------------------
    # Guardrail & Subsystem Evaluators
    # -------------------------------------------------------------------------

    def _validate_schema_integrity(self, spec: Dict[str, Any]) -> List[GuardrailViolation]:
        """Validates fundamental milestone structural completeness."""
        violations = []
        required_fields = ["title", "description"]
        for field_name in required_fields:
            if not spec.get(field_name):
                violations.append(GuardrailViolation(
                    rule_id="SCHEMA-001",
                    category="spec_integrity",
                    severity=RiskLevel.HIGH,
                    message=f"Missing essential milestone specification field: '{field_name}'.",
                    context={"missing_field": field_name},
                    remediation=f"Ensure milestone defines a substantive '{field_name}' property."
                ))

        target_files = spec.get("target_files", [])
        if not target_files and not spec.get("deliverable") and not spec.get("file"):
            violations.append(GuardrailViolation(
                rule_id="SCHEMA-002",
                category="spec_integrity",
                severity=RiskLevel.MEDIUM,
                message="No explicit target files or deliverable scope specified.",
                context={},
                remediation="Declare explicit 'target_files' or target path to prevent unfocused repository drift."
            ))

        return violations

    def _evaluate_blast_radius(self, spec: Dict[str, Any]) -> Tuple[List[GuardrailViolation], float]:
        """Evaluates operational scope and containment boundaries."""
        violations = []
        score = 0.0

        target_files = spec.get("target_files", [])
        if isinstance(target_files, str):
            target_files = [target_files]

        # Scope overload check
        if len(target_files) > self.max_allowed_file_scope:
            excess = len(target_files) - self.max_allowed_file_scope
            score += min(0.4, excess * 0.05)
            violations.append(GuardrailViolation(
                rule_id="BLAST-001",
                category="blast_radius",
                severity=RiskLevel.HIGH,
                message=f"Scope violation: Milestone targets {len(target_files)} files (maximum allowed: {self.max_allowed_file_scope}).",
                context={"file_count": len(target_files), "limit": self.max_allowed_file_scope},
                remediation="Decompose milestone into atomic sub-tasks touching <= 5 files each."
            ))

        # Sensitive paths audit
        critical_system_patterns = [
            r"^\/?etc\/", r"^\/?var\/", r"^\/?usr\/", r"\.git\/", r"\.ssh\/",
            r"\.env(?:\..*)?$", r"id_rsa", r"sudoers", r"\/dev\/null"
        ]
        for f in target_files:
            for pattern in critical_system_patterns:
                if re.search(pattern, str(f)):
                    score += 0.5
                    violations.append(GuardrailViolation(
                        rule_id="BLAST-002",
                        category="security_isolation",
                        severity=RiskLevel.CRITICAL,
                        message=f"Target file '{f}' violates containment boundary ({pattern}).",
                        context={"violating_file": f, "pattern": pattern},
                        remediation="Do not alter repository root VCS files or OS system configurations directly."
                    ))

        return violations, min(1.0, score)

    def _audit_regression_guardrails(
        self,
        spec: Dict[str, Any],
        spec_text: str
    ) -> Tuple[List[GuardrailViolation], float]:
        """Checks for repeat regressions against previously failed task history."""
        violations = []
        score = 0.0

        if not self.failed_registry_path or not self.failed_registry_path.exists():
            return violations, score

        try:
            with open(self.failed_registry_path, "r", encoding="utf-8") as f:
                history = json.load(f)
                records = history.get("failed_tasks", []) if isinstance(history, dict) else history

            milestone_id = spec.get("id") or spec.get("milestone_id")
            spec_tokens = set(self._tokenize(spec_text.lower()))

            for rec in records[-20:]:  # Check recent 20 failures
                failed_id = rec.get("task_id") or rec.get("milestone_id")
                failed_reason = (rec.get("error_message") or rec.get("reason") or "").lower()
                failed_context = (rec.get("context_summary") or "").lower()

                if milestone_id and milestone_id == failed_id:
                    score += 0.35
                    violations.append(GuardrailViolation(
                        rule_id="REGR-001",
                        category="regression_hazard",
                        severity=RiskLevel.HIGH,
                        message=f"Milestone {milestone_id} previously failed without proven structural fix.",
                        context={"previous_error": failed_reason[:120]},
                        remediation="Incorporate explicit post-mortem remediation steps in milestone specification."
                    ))

                # Check lexical symptom repetition
                if failed_reason:
                    reason_tokens = set(self._tokenize(failed_reason))
                    if len(reason_tokens) >= 3 and len(reason_tokens.intersection(spec_tokens)) / len(reason_tokens) > 0.6:
                        score += 0.25
                        violations.append(GuardrailViolation(
                            rule_id="REGR-002",
                            category="regression_hazard",
                            severity=RiskLevel.MEDIUM,
                            message="Milestone text shows strong lexical overlap with previous runtime failure modes.",
                            context={"overlapping_symptom": failed_reason[:100]},
                            remediation="Review previous incident trace and verify safeguard presence."
                        ))

        except Exception as e:
            logger.warning("Failed to evaluate historical regression registry: %s", e)

        return violations, min(1.0, score)

    # -------------------------------------------------------------------------
    # Scoring & Verdict Synthesis
    # -------------------------------------------------------------------------

    def _compute_aggregate_risk(
        self,
        violations: List[GuardrailViolation],
        anti_patterns: List[AntiPatternMatch],
        blast_score: float,
        regression_score: float
    ) -> float:
        """Computes weighted aggregate risk index in normalized interval [0.0, 1.0]."""
        weight_map = {
            RiskLevel.CRITICAL: 0.40,
            RiskLevel.HIGH: 0.25,
            RiskLevel.MEDIUM: 0.12,
            RiskLevel.LOW: 0.04
        }

        violation_sum = sum(weight_map.get(v.severity, 0.05) for v in violations)
        anti_pattern_sum = sum(
            weight_map.get(p.severity, 0.05) * p.similarity_score
            for p in anti_patterns
        )

        raw_score = (
            (violation_sum * 0.40) +
            (anti_pattern_sum * 0.35) +
            (blast_score * 0.15) +
            (regression_score * 0.10)
        )

        # Non-linear logistic smoothing
        normalized = 1.0 / (1.0 + math.exp(-6.0 * (raw_score - 0.45)))
        return round(float(min(1.0, max(0.0, normalized))), 4)

    def _determine_verdict(
        self,
        risk_score: float,
        violations: List[GuardrailViolation],
        anti_patterns: List[AntiPatternMatch]
    ) -> Tuple[GateStatus, bool, RiskLevel]:
        """Derives conclusive execution gate status based on scores and critical barriers."""
        has_critical_violation = any(v.severity == RiskLevel.CRITICAL for v in violations)
        has_critical_anti_pattern = any(p.severity == RiskLevel.CRITICAL for p in anti_patterns)

        if has_critical_violation or has_critical_anti_pattern:
            return GateStatus.QUARANTINED, False, RiskLevel.CRITICAL

        if risk_score >= self.risk_threshold_reject:
            return GateStatus.REJECTED, False, RiskLevel.HIGH

        if risk_score >= self.risk_threshold_conditional:
            risk_lvl = RiskLevel.MEDIUM if risk_score < 0.55 else RiskLevel.HIGH
            return GateStatus.CONDITIONAL_APPROVAL, True, risk_lvl

        if risk_score >= 0.15:
            return GateStatus.APPROVED, True, RiskLevel.LOW

        return GateStatus.APPROVED, True, RiskLevel.LOW

    def _synthesize_adaptive_constraints(
        self,
        verdict: GateStatus,
        risk_level: RiskLevel,
        milestone_spec: Dict[str, Any],
        anti_patterns: List[AntiPatternMatch]
    ) -> Dict[str, Any]:
        """Synthesizes dynamic execution parameters and sandbox rules."""
        base_constraints = {
            "timeout_seconds": 180,
            "max_retries": 1,
            "token_budget": 8000,
            "require_preflight_ast_check": True,
            "atomic_commit_per_file": True,
            "allow_destructive_commands": False,
            "require_sandbox_isolation": False,
            "enforce_dry_run": False
        }

        if verdict == GateStatus.APPROVED:
            base_constraints.update({
                "timeout_seconds": 300,
                "max_retries": 2,
                "token_budget": 12000,
            })
        elif verdict == GateStatus.CONDITIONAL_APPROVAL:
            base_constraints.update({
                "timeout_seconds": 120,
                "max_retries": 1,
                "token_budget": 6000,
                "require_sandbox_isolation": True,
                "enforce_dry_run": (risk_level == RiskLevel.HIGH),
                "monitoring_heartbeat_interval": 15,
            })
        elif verdict in (GateStatus.REJECTED, GateStatus.QUARANTINED):
            base_constraints.update({
                "timeout_seconds": 0,
                "max_retries": 0,
                "token_budget": 0,
                "execution_blocked": True,
                "quarantine_reason": [p.name for p in anti_patterns] or "High risk score",
            })

        return base_constraints

    # -------------------------------------------------------------------------
    # Utilities
    # -------------------------------------------------------------------------

    @staticmethod
    def _extract_full_text(spec: Dict[str, Any]) -> str:
        """Aggregates all textual instructions from milestone dictionary."""
        parts = []
        for key in ["title", "description", "milestone", "spec", "instructions", "code", "deliverable"]:
            val = spec.get(key)
            if isinstance(val, str):
                parts.append(val)
            elif isinstance(val, list):
                parts.extend([str(item) for item in val if isinstance(item, (str, dict))])
            elif isinstance(val, dict):
                parts.append(json.dumps(val))
        return "\n".join(parts)

    @staticmethod
    def _tokenize(text: str) -> List[str]:
        """Splits text into lowercase alphanumerical tokens."""
        return re.findall(r"[a-zA-Z0-9_\-\.]{2,}", text)


# -----------------------------------------------------------------------------
# Module Self-Verification & Demonstration Entrypoint
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    gate = ExecutionPolicyGate()

    print("=" * 80)
    print("ExecutionPolicyGate: Pre-Flight Pipeline Test Suite")
    print("=" * 80)

    # Test Case 1: Benign, Well-Scoped Milestone
    safe_spec = {
        "id": "M-SAFE-001",
        "title": "Implement Prometheus Telemetry Exporter",
        "description": "Add standard metrics exporter for HTTP latency and memory residency using prometheus_client.",
        "target_files": ["tasks/telemetry_exporter.py", "tests/test_telemetry.py"],
        "token_limit": 4000
    }
    v1 = gate.evaluate(safe_spec)
    print(f"\n[Test 1] Safe Spec -> Verdict: {v1.verdict.value} | Risk: {v1.risk_score} ({v1.risk_level.value})")
    assert v1.allowed is True, "Safe milestone should be allowed"

    # Test Case 2: Dangerous Spec with Anti-Patterns
    dangerous_spec = {
        "id": "M-RISK-002",
        "title": "Clean Workspace & Force Push Build",
        "description": "Execute rm -rf / and git push --force origin main with hardcoded sk-12345678901234567890123456 token.",
        "target_files": ["/etc/passwd", ".git/config"],
    }
    v2 = gate.evaluate(dangerous_spec)
    print(f"[Test 2] Dangerous Spec -> Verdict: {v2.verdict.value} | Risk: {v2.risk_score} ({v2.risk_level.value})")
    print(f"         Violations: {len(v2.violations)}, Anti-patterns: {len(v2.anti_patterns_detected)}")
    assert v2.allowed is False, "Dangerous milestone must be blocked"
    assert v2.verdict == GateStatus.QUARANTINED, "Critical pattern must trigger Quarantine"

    # Test Case 3: Broad Spec Requiring Conditional Approval
    borderline_spec = {
        "id": "M-WARN-003",
        "title": "Large Monorepo Refactoring",
        "description": "Refactor all database query helpers across multiple legacy modules.",
        "target_files": [f"tasks/module_{i}.py" for i in range(15)]
    }
    v3 = gate.evaluate(borderline_spec)
    print(f"[Test 3] Borderline Spec -> Verdict: {v3.verdict.value} | Risk: {v3.risk_score} ({v3.risk_level.value})")
    print(f"         Execution Constraints: {v3.execution_constraints.get('require_sandbox_isolation')}")

    print("\nAll pre-flight policy evaluations passed verification successfully.")
    print("=" * 80)