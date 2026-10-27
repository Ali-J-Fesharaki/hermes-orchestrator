#!/usr/bin/env python3
"""
Pipeline Autonomic Multi-Agent Consensus Protocol & Distributed Quorum Decision Engine
======================================================================================
Arbitrates conflicting governance directives from concurrent autonomic subsystems:
  - Anomaly Gateway (High authority, defensive/critical overrides)
  - Capacity Governor (Resource allocation & throughput controls)
  - Learning Engine (Model adaptation & hyperparameter scaling)
  - Meta-Optimizer (Global parameter & policy coordination)

Implements priority-weighted median consensus, three-tier conflict classification
(BENIGN, CONTESTED, ADVERSARIAL), safety-first critical overrides, exponential recency
weighting via causal health score deltas, and append-only SHA-256 hash-chained ledger.
"""

from __future__ import annotations

import collections
from dataclasses import dataclass, field, asdict
from enum import Enum
import hashlib
import json
import math
import os
import shutil
import statistics
import tempfile
import threading
import time
from typing import Literal, Optional, Any, Dict, List, Tuple


class ConflictSeverity(str, Enum):
    """Classification of governance conflict severity."""
    BENIGN = "BENIGN"
    CONTESTED = "CONTESTED"
    ADVERSARIAL = "ADVERSARIAL"


# Severity span thresholds
BENIGN_SPAN_THRESHOLD: float = 0.15
ADVERSARIAL_SPAN_THRESHOLD: float = 0.50


@dataclass
class DirectiveProposal:
    """Governance directive proposed by an autonomic subsystem."""
    source_subsystem: str
    parameter_key: str
    proposed_value: float
    confidence: float  # Expected in [0.0, 1.0]
    urgency: Literal['advisory', 'defensive', 'critical']
    evidence_chain: List[str] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        if not (0.0 <= self.confidence <= 1.0):
            # Clamp confidence to standard probability bound
            object.__setattr__(self, 'confidence', max(0.0, min(1.0, float(self.confidence))))
        if self.urgency not in ('advisory', 'defensive', 'critical'):
            raise ValueError(f"Invalid urgency level: {self.urgency}")


@dataclass
class ProposalBatch:
    """Batch window aggregating directive proposals for a consensus cycle."""
    batch_id: str
    window_start: float
    window_end: float
    proposals: List[DirectiveProposal] = field(default_factory=list)


@dataclass
class ConsensusVerdict:
    """Consensus outcome reached by the distributed quorum engine."""
    parameter_key: str
    consensus_value: float
    conflict_severity: Literal['BENIGN', 'CONTESTED', 'ADVERSARIAL']
    contributing_proposals: List[DirectiveProposal]
    resolution_rationale: str
    dissenting_subsystems: List[str]
    verdict_timestamp: float = field(default_factory=time.time)


class ConsensusEngine:
    """
    Arbitrates multi-agent parameter disputes using authority-weighted median,
    safety-first critical preemption, and causal health delta recency weighting.
    """

    DEFAULT_AUTHORITY_WEIGHTS: Dict[str, float] = {
        "AnomalyGateway": 1.5,
        "CapacityGovernor": 1.2,
        "LearningEngine": 1.0,
        "MetaOptimizer": 0.8,
    }

    DEFAULT_URGENCY_MULTIPLIERS: Dict[str, float] = {
        "advisory": 1.0,
        "defensive": 2.0,
        "critical": 4.0,
    }

    def __init__(
        self,
        consensus_window_sec: float = 30.0,
        authority_weights: Optional[Dict[str, float]] = None,
        urgency_multipliers: Optional[Dict[str, float]] = None,
        ledger_path: str = "logs/consensus_ledger.jsonl",
        parameter_ranges: Optional[Dict[str, Tuple[float, float]]] = None,
    ) -> None:
        self.consensus_window_sec: float = float(consensus_window_sec)
        self.authority_weights: Dict[str, float] = (
            dict(authority_weights) if authority_weights is not None else dict(self.DEFAULT_AUTHORITY_WEIGHTS)
        )
        self.urgency_multipliers: Dict[str, float] = (
            dict(urgency_multipliers) if urgency_multipliers is not None else dict(self.DEFAULT_URGENCY_MULTIPLIERS)
        )
        self.ledger_path: str = os.path.abspath(ledger_path)
        self.parameter_ranges: Dict[str, Tuple[float, float]] = dict(parameter_ranges or {})

        self._lock: threading.Lock = threading.Lock()
        self._current_batch: List[DirectiveProposal] = []
        self._window_start_time: float = time.time()

        # Sliding window for post-application health score deltas (last 5 per subsystem)
        self._subsystem_health_deltas: Dict[str, collections.deque[float]] = collections.defaultdict(
            lambda: collections.deque(maxlen=5)
        )

        # In-memory ledger cache and hash-chain tracking
        self._last_hash: str = "0" * 64
        self._last_index: int = 0
        self._ledger_cache: List[Dict[str, Any]] = []

        self._init_ledger()

    def _init_ledger(self) -> None:
        """Initializes or recovers the append-only ledger and hash chain state."""
        ledger_dir = os.path.dirname(self.ledger_path)
        if ledger_dir:
            os.makedirs(ledger_dir, exist_ok=True)

        if os.path.exists(self.ledger_path) and os.path.getsize(self.ledger_path) > 0:
            with open(self.ledger_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                        self._ledger_cache.append(entry)
                        self._last_hash = entry.get("hash", self._last_hash)
                        self._last_index = entry.get("index", self._last_index) + 1
                    except json.JSONDecodeError:
                        continue

    def submit_proposal(self, proposal: DirectiveProposal) -> None:
        """Thread-safe proposal ingestion into the current batch window."""
        with self._lock:
            self._current_batch.append(proposal)

    def record_health_score_delta(self, subsystem: str, delta: float) -> None:
        """
        Records a post-application health score delta for causal audit tracking.
        Maintains a rolling window of the last 5 directives per subsystem.
        """
        with self._lock:
            self._subsystem_health_deltas[subsystem].append(float(delta))

    def _compute_recency_health_score(self, subsystem: str) -> float:
        """
        Computes exponential recency weighted health score delta for a subsystem.
        Weights recent directives exponentially: w_i = exp(0.5 * index).
        """
        deltas = list(self._subsystem_health_deltas.get(subsystem, []))
        if not deltas:
            return 0.0
        k = len(deltas)
        # Exponential recency weights: index 0 (oldest) to k-1 (newest)
        weights = [math.exp(0.5 * i) for i in range(k)]
        total_w = sum(weights)
        if total_w <= 0.0:
            return 0.0
        return sum(w * d for w, d in zip(weights, deltas)) / total_w

    def _weighted_median(self, values: List[float], weights: List[float]) -> float:
        """
        Computes the priority-weighted median in O(n log n) time.
        Finds the value where cumulative weight reaches half of total weight.
        """
        if not values:
            raise ValueError("Cannot compute weighted median of empty values.")
        if len(values) == 1:
            return float(values[0])

        combined = sorted(zip(values, weights), key=lambda x: x[0])
        total_weight = sum(w for _, w in combined)
        if total_weight <= 0.0:
            return float(statistics.median(values))

        half_weight = total_weight / 2.0
        cumulative_weight = 0.0

        for i, (val, weight) in enumerate(combined):
            cumulative_weight += weight
            if math.isclose(cumulative_weight, half_weight, rel_tol=1e-9, abs_tol=1e-9):
                if i + 1 < len(combined):
                    return float((val + combined[i + 1][0]) / 2.0)
                return float(val)
            elif cumulative_weight > half_weight:
                return float(val)

        return float(combined[-1][0])

    def _classify_conflict(self, proposals: List[DirectiveProposal]) -> ConflictSeverity:
        """
        Detects conflict severity:
          - BENIGN: All proposals within 15% of mean.
          - CONTESTED: Proposals span >15% but <50% of parameter range.
          - ADVERSARIAL: Proposals span >50% or have opposing sign deltas.
        """
        if len(proposals) <= 1:
            return ConflictSeverity.BENIGN

        values = [p.proposed_value for p in proposals]
        min_val = min(values)
        max_val = max(values)
        span = max_val - min_val

        # Opposing sign deltas: e.g., one subsystem proposes negative, another positive
        if min_val < 0.0 < max_val:
            return ConflictSeverity.ADVERSARIAL

        mean_val = statistics.mean(values)
        abs_mean = abs(mean_val)

        # Determine reference scale (parameter range if configured, else absolute mean)
        param_key = proposals[0].parameter_key
        if param_key in self.parameter_ranges:
            p_min, p_max = self.parameter_ranges[param_key]
            ref_scale = max(abs(p_max - p_min), 1e-9)
        else:
            ref_scale = abs_mean if abs_mean > 1e-9 else 1.0

        span_ratio = span / ref_scale
        max_dev_ratio = max(abs(v - mean_val) for v in values) / (abs_mean if abs_mean > 1e-9 else 1.0)

        if span_ratio > ADVERSARIAL_SPAN_THRESHOLD:
            return ConflictSeverity.ADVERSARIAL
        elif span_ratio > BENIGN_SPAN_THRESHOLD or max_dev_ratio > BENIGN_SPAN_THRESHOLD:
            return ConflictSeverity.CONTESTED
        else:
            return ConflictSeverity.BENIGN

    def resolve_batch(self) -> List[ConsensusVerdict]:
        """
        Processes all proposals in the current window, groups by parameter_key,
        arbitrates conflicts, records verdicts in the ledger, and returns verdicts.
        """
        with self._lock:
            proposals_to_process = list(self._current_batch)
            self._current_batch.clear()
            self._window_start_time = time.time()

        if not proposals_to_process:
            return []

        # Group proposals by parameter_key
        grouped: Dict[str, List[DirectiveProposal]] = collections.defaultdict(list)
        for p in proposals_to_process:
            grouped[p.parameter_key].append(p)

        verdicts: List[ConsensusVerdict] = []

        for param_key, param_proposals in grouped.items():
            verdict = self._arbitrate_parameter(param_key, param_proposals)
            self._record_verdict(verdict)
            verdicts.append(verdict)

        return verdicts

    def _arbitrate_parameter(
        self,
        param_key: str,
        proposals: List[DirectiveProposal],
    ) -> ConsensusVerdict:
        """Arbitrates proposals for a single parameter key."""
        now = time.time()

        # Case 1: Uncontested single-proposal pass-through
        if len(proposals) == 1:
            prop = proposals[0]
            return ConsensusVerdict(
                parameter_key=param_key,
                consensus_value=float(prop.proposed_value),
                conflict_severity=ConflictSeverity.BENIGN.value,
                contributing_proposals=proposals,
                resolution_rationale=f"Single directive proposal from {prop.source_subsystem} adopted directly.",
                dissenting_subsystems=[],
                verdict_timestamp=now,
            )

        severity = self._classify_conflict(proposals)

        # Compute standard priority weights for proposals
        # w_i = alpha_source(i) * confidence_i * urgency_multiplier(i)
        weights: List[float] = []
        for p in proposals:
            alpha = self.authority_weights.get(p.source_subsystem, 1.0)
            urgency_mult = self.urgency_multipliers.get(p.urgency.lower(), 1.0)
            conf = max(p.confidence, 1e-4)
            w = alpha * conf * urgency_mult
            weights.append(w)

        if severity == ConflictSeverity.BENIGN:
            values = [p.proposed_value for p in proposals]
            consensus_val = self._weighted_median(values, weights)
            dissenting = [
                p.source_subsystem
                for p in proposals
                if not math.isclose(p.proposed_value, consensus_val, rel_tol=1e-4, abs_tol=1e-4)
            ]
            return ConsensusVerdict(
                parameter_key=param_key,
                consensus_value=consensus_val,
                conflict_severity=ConflictSeverity.BENIGN.value,
                contributing_proposals=proposals,
                resolution_rationale="Benign conflict; resolved via authority-weighted median.",
                dissenting_subsystems=sorted(list(dict.fromkeys(dissenting))),
                verdict_timestamp=now,
            )

        elif severity == ConflictSeverity.CONTESTED:
            values = [p.proposed_value for p in proposals]
            consensus_val = self._weighted_median(values, weights)
            dissenting = [
                p.source_subsystem
                for p in proposals
                if not math.isclose(p.proposed_value, consensus_val, rel_tol=1e-4, abs_tol=1e-4)
            ]
            return ConsensusVerdict(
                parameter_key=param_key,
                consensus_value=consensus_val,
                conflict_severity=ConflictSeverity.CONTESTED.value,
                contributing_proposals=proposals,
                resolution_rationale="Contested dispute; arbitrated via priority-weighted median across subsystems.",
                dissenting_subsystems=sorted(list(dict.fromkeys(dissenting))),
                verdict_timestamp=now,
            )

        else:  # ADVERSARIAL
            # Strategy step 1: Safety-first critical override
            critical_proposals = [p for p in proposals if p.urgency.lower() == 'critical']
            if critical_proposals:
                # Select the critical proposal with highest authority * confidence
                winner = max(
                    critical_proposals,
                    key=lambda p: (self.authority_weights.get(p.source_subsystem, 1.0) * p.confidence),
                )
                consensus_val = float(winner.proposed_value)
                dissenting = [
                    p.source_subsystem for p in proposals if p.source_subsystem != winner.source_subsystem
                ]
                rationale = (
                    f"Adversarial conflict resolved via safety-first critical override; "
                    f"proposal from {winner.source_subsystem} selected."
                )
                return ConsensusVerdict(
                    parameter_key=param_key,
                    consensus_value=consensus_val,
                    conflict_severity=ConflictSeverity.ADVERSARIAL.value,
                    contributing_proposals=proposals,
                    resolution_rationale=rationale,
                    dissenting_subsystems=sorted(list(dict.fromkeys(dissenting))),
                    verdict_timestamp=now,
                )

            # Strategy step 2: Exponential recency health score delta weighting
            subsystem_scores: Dict[str, float] = {
                p.source_subsystem: self._compute_recency_health_score(p.source_subsystem)
                for p in proposals
            }

            winner_subsystem = max(
                subsystem_scores.keys(),
                key=lambda s: (
                    subsystem_scores[s],
                    self.authority_weights.get(s, 1.0),
                ),
            )
            winning_proposal = next(p for p in proposals if p.source_subsystem == winner_subsystem)
            consensus_val = float(winning_proposal.proposed_value)
            dissenting = [
                p.source_subsystem for p in proposals if p.source_subsystem != winner_subsystem
            ]
            rationale = (
                f"Adversarial conflict resolved via causal recency weighting favoring {winner_subsystem} "
                f"(historical health delta score: {subsystem_scores[winner_subsystem]:.4f})."
            )
            return ConsensusVerdict(
                parameter_key=param_key,
                consensus_value=consensus_val,
                conflict_severity=ConflictSeverity.ADVERSARIAL.value,
                contributing_proposals=proposals,
                resolution_rationale=rationale,
                dissenting_subsystems=sorted(list(dict.fromkeys(dissenting))),
                verdict_timestamp=now,
            )

    def _proposal_to_dict(self, proposal: DirectiveProposal) -> Dict[str, Any]:
        """Converts DirectiveProposal to JSON-serializable dictionary."""
        return {
            "source_subsystem": proposal.source_subsystem,
            "parameter_key": proposal.parameter_key,
            "proposed_value": float(proposal.proposed_value),
            "confidence": float(proposal.confidence),
            "urgency": str(proposal.urgency),
            "evidence_chain": list(proposal.evidence_chain),
            "timestamp": float(proposal.timestamp),
        }

    def _verdict_to_dict(self, verdict: ConsensusVerdict) -> Dict[str, Any]:
        """Converts ConsensusVerdict to JSON-serializable dictionary."""
        return {
            "parameter_key": verdict.parameter_key,
            "consensus_value": float(verdict.consensus_value),
            "conflict_severity": str(
                verdict.conflict_severity.value
                if isinstance(verdict.conflict_severity, ConflictSeverity)
                else verdict.conflict_severity
            ),
            "contributing_proposals": [self._proposal_to_dict(p) for p in verdict.contributing_proposals],
            "resolution_rationale": str(verdict.resolution_rationale),
            "dissenting_subsystems": list(verdict.dissenting_subsystems),
            "verdict_timestamp": float(verdict.verdict_timestamp),
        }

    def _record_verdict(self, verdict: ConsensusVerdict) -> None:
        """
        Records verdict in append-only JSONL ledger with SHA-256 hash chaining
        using atomic tempfile rename pattern.
        """
        payload = {
            "index": self._last_index,
            "prev_hash": self._last_hash,
            "timestamp": float(verdict.verdict_timestamp),
            "verdict": self._verdict_to_dict(verdict),
        }

        # SHA-256 hash over deterministic sorted JSON string
        serialized_payload = json.dumps(payload, sort_keys=True)
        current_hash = hashlib.sha256(serialized_payload.encode("utf-8")).hexdigest()

        entry = dict(payload)
        entry["hash"] = current_hash

        ledger_dir = os.path.dirname(self.ledger_path)
        if ledger_dir:
            os.makedirs(ledger_dir, exist_ok=True)

        # Atomic append pattern: write existing + new entry to tempfile and atomic replace
        with tempfile.NamedTemporaryFile("w", dir=ledger_dir, delete=False, encoding="utf-8") as tf:
            temp_path = tf.name
            if os.path.exists(self.ledger_path):
                with open(self.ledger_path, "r", encoding="utf-8") as orig:
                    shutil.copyfileobj(orig, tf)
            tf.write(json.dumps(entry, sort_keys=True) + "\n")
            tf.flush()
            os.fsync(tf.fileno())

        os.replace(temp_path, self.ledger_path)

        # Update in-memory state
        self._last_hash = current_hash
        self._last_index += 1
        self._ledger_cache.append(entry)

    def get_consensus_history(self, n_cycles: int = 10) -> List[Dict[str, Any]]:
        """Reads the last N verdict entries from the consensus ledger."""
        with self._lock:
            recent = self._ledger_cache[-n_cycles:] if n_cycles > 0 else []
            return [
                {
                    **item["verdict"],
                    "ledger_index": item["index"],
                    "ledger_hash": item["hash"],
                    "prev_hash": item["prev_hash"],
                }
                for item in recent
            ]

    def get_conflict_frequency(self) -> Dict[str, int]:
        """Counts conflicts by severity over the last 100 verdicts."""
        with self._lock:
            recent = self._ledger_cache[-100:]
            freq: Dict[str, int] = {
                ConflictSeverity.BENIGN.value: 0,
                ConflictSeverity.CONTESTED.value: 0,
                ConflictSeverity.ADVERSARIAL.value: 0,
            }
            for item in recent:
                v = item.get("verdict", {})
                sev = v.get("conflict_severity")
                if sev in freq:
                    freq[sev] += 1
                elif sev:
                    freq[str(sev)] = freq.get(str(sev), 0) + 1
            return freq

    def verify_ledger_integrity(self, path: Optional[str] = None) -> bool:
        """
        Verifies SHA-256 hash-chaining integrity across all records in the ledger.
        Returns True if uncorrupted, False if hash mismatch or sequence broken.
        """
        target_path = path or self.ledger_path
        if not os.path.exists(target_path):
            return True

        expected_prev_hash = "0" * 64
        with open(target_path, "r", encoding="utf-8") as f:
            for line_idx, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    return False

                if record.get("index") != line_idx:
                    return False

                if record.get("prev_hash") != expected_prev_hash:
                    return False

                payload = {
                    "index": record["index"],
                    "prev_hash": record["prev_hash"],
                    "timestamp": record["timestamp"],
                    "verdict": record["verdict"],
                }
                recomputed_hash = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
                if recomputed_hash != record.get("hash"):
                    return False

                expected_prev_hash = record["hash"]

        return True


# ==============================================================================
# SELF-TEST SUITE
# ==============================================================================
if __name__ == "__main__":
    import tempfile

    test_dir = tempfile.mkdtemp(prefix="consensus_engine_test_")
    test_ledger = os.path.join(test_dir, "test_consensus_ledger.jsonl")

    try:
        print("[*] Initializing ConsensusEngine self-test...")
        engine = ConsensusEngine(consensus_window_sec=5.0, ledger_path=test_ledger)

        # ----------------------------------------------------------------------
        # Test 1: Single-proposal pass-through
        # ----------------------------------------------------------------------
        print("\n--- Test 1: Single-proposal pass-through ---")
        p1 = DirectiveProposal(
            source_subsystem="CapacityGovernor",
            parameter_key="worker_threads",
            proposed_value=16.0,
            confidence=0.9,
            urgency="advisory",
            evidence_chain=["nominal_cpu_headroom"],
        )
        engine.submit_proposal(p1)
        verdicts = engine.resolve_batch()
        assert len(verdicts) == 1, f"Expected 1 verdict, got {len(verdicts)}"
        v1 = verdicts[0]
        assert v1.parameter_key == "worker_threads"
        assert math.isclose(v1.consensus_value, 16.0), f"Expected 16.0, got {v1.consensus_value}"
        assert v1.conflict_severity == ConflictSeverity.BENIGN.value
        assert len(v1.dissenting_subsystems) == 0
        print("  [✓] Pass-through successful:", v1.consensus_value)

        # ----------------------------------------------------------------------
        # Test 2: BENIGN multi-proposal consensus
        # ----------------------------------------------------------------------
        print("\n--- Test 2: BENIGN multi-proposal consensus ---")
        p2_a = DirectiveProposal(
            source_subsystem="CapacityGovernor",
            parameter_key="batch_size",
            proposed_value=100.0,
            confidence=0.9,
            urgency="advisory",
            evidence_chain=["memory_utilization_65%"],
        )
        p2_b = DirectiveProposal(
            source_subsystem="LearningEngine",
            parameter_key="batch_size",
            proposed_value=105.0,
            confidence=0.85,
            urgency="advisory",
            evidence_chain=["gradient_step_variance_low"],
        )
        engine.submit_proposal(p2_a)
        engine.submit_proposal(p2_b)
        verdicts = engine.resolve_batch()
        assert len(verdicts) == 1
        v2 = verdicts[0]
        assert v2.conflict_severity == ConflictSeverity.BENIGN.value
        assert 100.0 <= v2.consensus_value <= 105.0
        print("  [✓] Benign consensus reached:", v2.consensus_value)

        # ----------------------------------------------------------------------
        # Test 3: CONTESTED weighted median resolution
        # ----------------------------------------------------------------------
        print("\n--- Test 3: CONTESTED weighted median resolution ---")
        # Proposals span >15% and <50% of range
        p3_a = DirectiveProposal(
            source_subsystem="MetaOptimizer",  # alpha=0.8, conf=0.8, urg=advisory(1.0) => w=0.64
            parameter_key="prefetch_depth",
            proposed_value=20.0,
            confidence=0.8,
            urgency="advisory",
        )
        p3_b = DirectiveProposal(
            source_subsystem="LearningEngine",  # alpha=1.0, conf=0.9, urg=advisory(1.0) => w=0.9
            parameter_key="prefetch_depth",
            proposed_value=24.0,
            confidence=0.9,
            urgency="advisory",
        )
        p3_c = DirectiveProposal(
            source_subsystem="CapacityGovernor",  # alpha=1.2, conf=0.95, urg=defensive(2.0) => w=2.28
            parameter_key="prefetch_depth",
            proposed_value=28.0,
            confidence=0.95,
            urgency="defensive",
        )
        engine.submit_proposal(p3_a)
        engine.submit_proposal(p3_b)
        engine.submit_proposal(p3_c)
        verdicts = engine.resolve_batch()
        assert len(verdicts) == 1
        v3 = verdicts[0]
        assert v3.conflict_severity == ConflictSeverity.CONTESTED.value
        # CapacityGovernor weight (2.28) out of total (3.82) is > 50%, so median must equal 28.0
        assert math.isclose(v3.consensus_value, 28.0), f"Expected 28.0, got {v3.consensus_value}"
        assert "MetaOptimizer" in v3.dissenting_subsystems
        assert "LearningEngine" in v3.dissenting_subsystems
        print("  [✓] Contested resolution correctly weighted:", v3.consensus_value)

        # ----------------------------------------------------------------------
        # Test 4: ADVERSARIAL safety-first critical override
        # ----------------------------------------------------------------------
        print("\n--- Test 4: ADVERSARIAL safety-first critical override ---")
        p4_a = DirectiveProposal(
            source_subsystem="MetaOptimizer",
            parameter_key="max_concurrent_requests",
            proposed_value=100.0,
            confidence=0.9,
            urgency="advisory",
        )
        p4_b = DirectiveProposal(
            source_subsystem="AnomalyGateway",
            parameter_key="max_concurrent_requests",
            proposed_value=10.0,
            confidence=0.95,
            urgency="critical",
            evidence_chain=["upstream_endpoint_503_surge"],
        )
        engine.submit_proposal(p4_a)
        engine.submit_proposal(p4_b)
        verdicts = engine.resolve_batch()
        assert len(verdicts) == 1
        v4 = verdicts[0]
        assert v4.conflict_severity == ConflictSeverity.ADVERSARIAL.value
        assert math.isclose(v4.consensus_value, 10.0), f"Expected 10.0, got {v4.consensus_value}"
        assert "safety-first critical override" in v4.resolution_rationale
        assert "MetaOptimizer" in v4.dissenting_subsystems
        print("  [✓] Safety-first critical override verified:", v4.consensus_value)

        # ----------------------------------------------------------------------
        # Test 5: ADVERSARIAL recency-weighted resolution
        # ----------------------------------------------------------------------
        print("\n--- Test 5: ADVERSARIAL recency-weighted resolution ---")
        # Subsystems disagree widely (>50% span), neither is critical
        p5_a = DirectiveProposal(
            source_subsystem="LearningEngine",
            parameter_key="eviction_threshold",
            proposed_value=0.1,
            confidence=0.85,
            urgency="advisory",
        )
        p5_b = DirectiveProposal(
            source_subsystem="CapacityGovernor",
            parameter_key="eviction_threshold",
            proposed_value=0.9,
            confidence=0.85,
            urgency="defensive",
        )
        # Train history: LearningEngine had poor/negative deltas, CapacityGovernor had strong positive deltas
        for d in [-0.05, 0.0, -0.02, -0.01, 0.01]:
            engine.record_health_score_delta("LearningEngine", d)
        for d in [0.15, 0.20, 0.25, 0.30, 0.35]:
            engine.record_health_score_delta("CapacityGovernor", d)

        engine.submit_proposal(p5_a)
        engine.submit_proposal(p5_b)
        verdicts = engine.resolve_batch()
        assert len(verdicts) == 1
        v5 = verdicts[0]
        assert v5.conflict_severity == ConflictSeverity.ADVERSARIAL.value
        assert math.isclose(v5.consensus_value, 0.9), f"Expected CapacityGovernor's 0.9, got {v5.consensus_value}"
        assert "causal recency weighting" in v5.resolution_rationale
        assert "LearningEngine" in v5.dissenting_subsystems
        print("  [✓] Recency-weighted resolution verified:", v5.consensus_value)

        # ----------------------------------------------------------------------
        # Test 6: Hash-chain integrity verification
        # ----------------------------------------------------------------------
        print("\n--- Test 6: Ledger hash-chain integrity verification ---")
        assert engine.verify_ledger_integrity() is True
        print("  [✓] Authentic ledger passes hash-chain audit.")

        # Tampering test
        tampered_ledger = os.path.join(test_dir, "tampered_ledger.jsonl")
        shutil.copyfile(test_ledger, tampered_ledger)
        with open(tampered_ledger, "r", encoding="utf-8") as f:
            lines = f.readlines()
        # Tamper with the consensus value in the second record
        record_1 = json.loads(lines[1])
        record_1["verdict"]["consensus_value"] = 9999.0
        lines[1] = json.dumps(record_1) + "\n"
        with open(tampered_ledger, "w", encoding="utf-8") as f:
            f.writelines(lines)

        assert engine.verify_ledger_integrity(tampered_ledger) is False
        print("  [✓] Tampered ledger was successfully detected and rejected.")

        # ----------------------------------------------------------------------
        # Test 7: Conflict frequency statistics and history
        # ----------------------------------------------------------------------
        print("\n--- Test 7: Conflict frequency and history API ---")
        history = engine.get_consensus_history(n_cycles=5)
        assert len(history) == 5, f"Expected 5 history items, got {len(history)}"
        freq = engine.get_conflict_frequency()
        print("  [✓] Conflict frequency distribution:", freq)
        assert freq[ConflictSeverity.BENIGN.value] == 2
        assert freq[ConflictSeverity.CONTESTED.value] == 1
        assert freq[ConflictSeverity.ADVERSARIAL.value] == 2

        print("\n[SUCCESS] All 7 ConsensusEngine test suites passed with 0 errors.")

    finally:
        shutil.rmtree(test_dir, ignore_errors=True)