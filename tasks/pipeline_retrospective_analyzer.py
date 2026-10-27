"""
Pipeline Post-Execution Retrospective Analyzer & Closed-Loop Calibration Engine.

Hermes Orchestrator Task Subsystem.
Milestone: Pipeline Post-Execution Retrospective Analyzer & Closed-Loop Calibration Engine.

This module closes the outer feedback loop by analyzing completed milestone execution
outcomes and feeding calibration corrections back into the Capacity Planner,
Resource Allocation Engine, and Predictive Analytics subsystems. It ingests audit
logs, decomposes estimation variance, clusters recurring failure patterns, emits
adaptive calibration directives, and computes retrospective health indicators.
"""

from __future__ import annotations

import contextlib
import dataclasses
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import enum
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import sys
import threading
import time
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple, Union
import uuid

# ============================================================================
# OpenTelemetry Graceful Fallback Pattern
# ============================================================================

try:
    from opentelemetry import trace
    tracer = trace.get_tracer("hermes.pipeline.retrospective")
except ImportError:
    class DummySpan:
        """Fallback span providing a no-op context manager."""
        def __enter__(self) -> "DummySpan":
            return self

        def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            pass

        def set_attribute(self, key: str, value: Any) -> None:
            pass

        def record_exception(self, exception: Exception) -> None:
            pass

        def set_status(self, status: Any, description: Optional[str] = None) -> None:
            pass

    class DummyTracer:
        """Fallback tracer when OpenTelemetry is not installed."""
        def start_as_current_span(self, name: str, *args: Any, **kwargs: Any) -> DummySpan:
            return DummySpan()

        def start_span(self, name: str, *args: Any, **kwargs: Any) -> DummySpan:
            return DummySpan()

    tracer = DummyTracer()

# ============================================================================
# Threading & ReaderWriterLock Fallback Pattern
# ============================================================================

try:
    from tasks.runtime_execution_governor import ReaderWriterLock
except ImportError:
    class ReaderWriterLock:
        """
        Fair Reader-Writer Lock allowing concurrent readers or exclusive writers.
        Prevents writer starvation through a waiting writer counter.
        """
        def __init__(self) -> None:
            self._lock = threading.Lock()
            self._read_ready = threading.Condition(self._lock)
            self._readers = 0
            self._writers_waiting = 0
            self._writer_active = False

        @contextlib.contextmanager
        def read_locked(self) -> Iterator[None]:
            with self._lock:
                while self._writer_active or self._writers_waiting > 0:
                    self._read_ready.wait()
                self._readers += 1
            try:
                yield
            finally:
                with self._lock:
                    self._readers -= 1
                    if self._readers == 0:
                        self._read_ready.notify_all()

        @contextlib.contextmanager
        def write_locked(self) -> Iterator[None]:
            with self._lock:
                self._writers_waiting += 1
                while self._readers > 0 or self._writer_active:
                    self._read_ready.wait()
                self._writers_waiting -= 1
                self._writer_active = True
            try:
                yield
            finally:
                with self._lock:
                    self._writer_active = False
                    self._read_ready.notify_all()

# ============================================================================
# Module Logging
# ============================================================================

logger = logging.getLogger("hermes.pipeline.retrospective")
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(
        logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [hermes.pipeline.retrospective] %(message)s"
        )
    )
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)

# ============================================================================
# Enums and Constants
# ============================================================================

class ExecutionOutcomeStatus(str, enum.Enum):
    """Execution status outcomes for milestones."""
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILURE = "FAILURE"
    ROLLBACK = "ROLLBACK"


class FailureCategory(str, enum.Enum):
    """Categorical classification of milestone execution failures."""
    SYNTAX_ERROR = "SYNTAX_ERROR"
    TIMEOUT = "TIMEOUT"
    OOM_RESOURCE = "OOM_RESOURCE"
    RATE_LIMIT_429 = "RATE_LIMIT_429"
    CIRCUIT_BREAKER = "CIRCUIT_BREAKER"
    CONFORMANCE_BREACH = "CONFORMANCE_BREACH"
    DEPENDENCY_RESOLUTION = "DEPENDENCY_RESOLUTION"
    UNCLASSIFIED = "UNCLASSIFIED"


class MitigationStrategy(str, enum.Enum):
    """Actionable mitigation strategies emitted for recurring failure patterns."""
    SKIP = "skip"
    DECOMPOSE_SMALLER = "decompose_smaller"
    ASSIGN_DIFFERENT_MODEL_TIER = "assign_to_different_model_tier"
    INCREASE_CIRCUIT_BREAKER_THRESHOLD = "increase_circuit_breaker_threshold"
    QUARANTINE_MILESTONE = "quarantine_milestone"
    ADAPTIVE_TOKEN_THROTTLE = "adaptive_token_throttle"


# ============================================================================
# Dataclasses
# ============================================================================

@dataclass
class ExecutionOutcomeRecord:
    """
    Structured record capturing individual milestone execution results.
    Includes cost variance, execution duration, and reliability metrics.
    """
    milestone_id: str
    repository: str
    cycle_id: str
    planned_token_cost: float
    actual_token_cost: float
    cost_ratio: float = 1.0
    planned_duration_s: float = 0.0
    actual_duration_s: float = 0.0
    duration_ratio: float = 1.0
    outcome: str = ExecutionOutcomeStatus.SUCCESS.value
    breaker_trips: int = 0
    reschedule_events: int = 0
    complexity_class: str = "STANDARD"
    error_signature: Optional[str] = None
    completed_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Safely compute normalized cost and duration ratios."""
        p_cost = max(float(self.planned_token_cost), 1.0)
        a_cost = max(float(self.actual_token_cost), 0.0)
        self.cost_ratio = round(a_cost / p_cost, 4)

        p_dur = max(float(self.planned_duration_s), 0.001)
        a_dur = max(float(self.actual_duration_s), 0.0)
        self.duration_ratio = round(a_dur / p_dur, 4)

        if isinstance(self.outcome, enum.Enum):
            self.outcome = self.outcome.value

        if not self.error_signature and self.outcome in (
            ExecutionOutcomeStatus.FAILURE.value,
            ExecutionOutcomeStatus.ROLLBACK.value,
        ):
            # Synthesize deterministic error signature from available attributes
            raw_sig = f"{self.repository}:{self.complexity_class}:{self.outcome}:{self.breaker_trips}"
            self.error_signature = hashlib.sha256(raw_sig.encode("utf-8")).hexdigest()[:16]

    def to_dict(self) -> Dict[str, Any]:
        """Convert record to JSON-serializable dictionary."""
        return asdict(self)


@dataclass
class VarianceReport:
    """
    Comprehensive decomposition of estimation variance across repositories and cycles.
    """
    cycle_id: str
    timestamp: float
    sample_count: int
    per_repository_bias: Dict[str, Dict[str, float]]
    global_optimism_coefficient: float
    worst_estimations: List[Dict[str, Any]]
    mean_absolute_cost_error: float
    mean_absolute_duration_error: float
    cost_variance: float = 0.0
    duration_variance: float = 0.0
    underestimation_frequency: float = 0.0
    overestimation_frequency: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert report to JSON-serializable dictionary."""
        return asdict(self)


@dataclass
class FailureCluster:
    """
    Cluster of execution failures grouped by deterministic error fingerprint.
    """
    signature: str
    category: str
    occurrences: int
    first_seen: float
    last_seen: float
    is_chronic: bool
    affected_repositories: List[str]
    suggested_mitigation: str
    sample_milestones: List[str] = field(default_factory=list)
    cycle_ids: List[str] = field(default_factory=list)
    last_error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert failure cluster to JSON-serializable dictionary."""
        return asdict(self)


@dataclass
class CalibrationDirective:
    """
    Actionable closed-loop calibration adjustments emitted to downstream engines.
    """
    directive_id: str
    timestamp: float
    cycle_id: str
    per_repo_adjustments: Dict[str, Dict[str, float]]
    global_optimism_correction: float
    seasonal_updates: Dict[str, float]
    breaker_threshold_adjustments: Dict[str, float]
    rationale: str
    applied: bool = False
    applied_at: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert directive to JSON-serializable dictionary."""
        return asdict(self)


@dataclass
class RetrospectiveHealthScore:
    """
    Multivariate retrospective accuracy and health assessment.
    """
    score: float
    mean_absolute_cost_error: float
    mean_absolute_duration_error: float
    chronic_failure_ratio: float
    calibration_staleness_cycles: int
    cost_accuracy_component: float
    duration_accuracy_component: float
    chronic_failure_component: float
    staleness_component: float
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        """Convert health score to JSON-serializable dictionary."""
        return asdict(self)


# ============================================================================
# Component 2: Outcome Ingestion Engine
# ============================================================================

class OutcomeIngestionEngine:
    """
    Consumes JSONL audit logs from AllocationDispatcher, ExecutionMonitor,
    and ResilienceController; deduplicates by milestone_id; maintains a
    persistent watermark to ensure exactly-once ingestion across cycles.
    """
    def __init__(
        self,
        watermark_file: Optional[Union[str, Path]] = None,
        log_dir: Optional[Union[str, Path]] = None,
    ) -> None:
        self.log_dir = Path(log_dir) if log_dir else Path("logs")
        self.watermark_file = (
            Path(watermark_file)
            if watermark_file
            else self.log_dir / ".retrospective_watermark.json"
        )
        self._lock = threading.Lock()
        self._last_processed_timestamp: float = self._load_watermark()
        self._processed_milestone_ids: Set[str] = set()

    def _load_watermark(self) -> float:
        """Load persistent watermark timestamp from sidecar JSON."""
        if not self.watermark_file.exists():
            return 0.0
        try:
            with open(self.watermark_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                return float(data.get("last_watermark_timestamp", 0.0))
        except Exception as e:
            logger.warning("Failed to load watermark from %s: %s", self.watermark_file, e)
            return 0.0

    def _save_watermark(self, timestamp: float) -> None:
        """Persist watermark timestamp to sidecar JSON."""
        try:
            self.watermark_file.parent.mkdir(parents=True, exist_ok=True)
            temp_file = self.watermark_file.with_suffix(".tmp")
            payload = {
                "last_watermark_timestamp": timestamp,
                "saved_at_iso": datetime.now(timezone.utc).isoformat(),
                "dedup_count": len(self._processed_milestone_ids),
            }
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
            temp_file.replace(self.watermark_file)
            self._last_processed_timestamp = timestamp
        except Exception as e:
            logger.error("Failed to save watermark to %s: %s", self.watermark_file, e)

    def reset_watermark(self) -> None:
        """Reset watermark to process all available historical logs."""
        with self._lock:
            self._last_processed_timestamp = 0.0
            self._processed_milestone_ids.clear()
            if self.watermark_file.exists():
                try:
                    self.watermark_file.unlink()
                except OSError as e:
                    logger.warning("Failed to delete watermark file: %s", e)

    def ingest_from_jsonl(
        self,
        target_dir: Optional[Union[str, Path]] = None,
        cycle_id_override: Optional[str] = None,
    ) -> List[ExecutionOutcomeRecord]:
        """
        Scans directory for JSONL logs from AllocationDispatcher, ExecutionMonitor,
        and ResilienceController, deduplicating records by milestone_id.
        """
        scan_dir = Path(target_dir) if target_dir else self.log_dir
        if not scan_dir.exists():
            logger.debug("Ingestion target directory does not exist: %s", scan_dir)
            return []

        outcomes: Dict[str, ExecutionOutcomeRecord] = {}
        high_watermark = self._last_processed_timestamp
        candidate_files: List[Path] = []

        patterns = [
            "allocation_dispatcher*.jsonl",
            "execution_monitor*.jsonl",
            "resilience*.jsonl",
            "circuit_breaker*.jsonl",
            "milestone_outcomes*.jsonl",
            "retrospective_test*.jsonl",
        ]

        for pattern in patterns:
            candidate_files.extend(scan_dir.glob(pattern))

        # Also search immediate subdirectories if present
        for sub in scan_dir.iterdir():
            if sub.is_dir():
                for pattern in patterns:
                    candidate_files.extend(sub.glob(pattern))

        unique_files = sorted(list(set(candidate_files)), key=lambda p: p.stat().st_mtime)

        with self._lock:
            for file_path in unique_files:
                try:
                    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                        for line_num, line in enumerate(f, start=1):
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                entry = json.loads(line)
                            except json.JSONDecodeError:
                                continue

                            entry_time = float(
                                entry.get("timestamp")
                                or entry.get("completed_at")
                                or entry.get("created_at")
                                or 0.0
                            )

                            if entry_time > 0 and entry_time <= self._last_processed_timestamp:
                                continue

                            mid = str(
                                entry.get("milestone_id")
                                or entry.get("task_id")
                                or entry.get("id")
                                or ""
                            )
                            if not mid or mid in self._processed_milestone_ids:
                                continue

                            record = self._parse_entry(entry, cycle_id_override)
                            if record:
                                outcomes[mid] = record
                                self._processed_milestone_ids.add(mid)
                                if record.completed_at > high_watermark:
                                    high_watermark = record.completed_at

                except Exception as e:
                    logger.error("Error reading JSONL file %s: %s", file_path, e)

            if high_watermark > self._last_processed_timestamp:
                self._save_watermark(high_watermark)

        logger.info(
            "Ingested %d new execution outcome records (watermark: %.2f)",
            len(outcomes),
            high_watermark,
        )
        return list(outcomes.values())

    def _parse_entry(
        self,
        entry: Dict[str, Any],
        cycle_id_override: Optional[str] = None,
    ) -> Optional[ExecutionOutcomeRecord]:
        """Parse raw log entry dictionary into ExecutionOutcomeRecord."""
        mid = str(entry.get("milestone_id") or entry.get("task_id") or entry.get("id") or "")
        if not mid:
            return None

        repo = str(entry.get("repository") or entry.get("repo") or "unknown_repo")
        cycle_id = str(
            cycle_id_override
            or entry.get("cycle_id")
            or entry.get("planning_cycle_id")
            or f"cycle_{int(time.time() // 300)}"
        )

        planned_cost = float(
            entry.get("planned_token_cost")
            or entry.get("estimated_tokens")
            or entry.get("budget_tokens")
            or 1000.0
        )
        actual_cost = float(
            entry.get("actual_token_cost")
            or entry.get("consumed_tokens")
            or entry.get("actual_tokens")
            or planned_cost
        )

        planned_dur = float(
            entry.get("planned_duration_s")
            or entry.get("estimated_duration_s")
            or 60.0
        )
        actual_dur = float(
            entry.get("actual_duration_s")
            or entry.get("execution_time_s")
            or entry.get("duration_s")
            or planned_dur
        )

        raw_outcome = str(
            entry.get("outcome")
            or entry.get("status")
            or ExecutionOutcomeStatus.SUCCESS.value
        ).upper()

        if "SUCCESS" in raw_outcome or "PASS" in raw_outcome:
            outcome = ExecutionOutcomeStatus.SUCCESS.value
        elif "PARTIAL" in raw_outcome:
            outcome = ExecutionOutcomeStatus.PARTIAL.value
        elif "ROLLBACK" in raw_outcome or "REVERT" in raw_outcome:
            outcome = ExecutionOutcomeStatus.ROLLBACK.value
        else:
            outcome = ExecutionOutcomeStatus.FAILURE.value

        breaker_trips = int(entry.get("breaker_trips") or entry.get("circuit_breaker_trips") or 0)
        reschedule_events = int(entry.get("reschedule_events") or entry.get("retry_count") or 0)
        complexity = str(entry.get("complexity_class") or entry.get("complexity") or "STANDARD")

        error_sig = entry.get("error_signature") or entry.get("signature")
        if not error_sig and outcome in (ExecutionOutcomeStatus.FAILURE.value, ExecutionOutcomeStatus.ROLLBACK.value):
            err_msg = str(entry.get("error") or entry.get("error_message") or entry.get("exception") or "unknown")
            error_sig = hashlib.sha256(f"{repo}:{err_msg}".encode("utf-8")).hexdigest()[:16]

        completed_at = float(
            entry.get("completed_at")
            or entry.get("timestamp")
            or time.time()
        )

        return ExecutionOutcomeRecord(
            milestone_id=mid,
            repository=repo,
            cycle_id=cycle_id,
            planned_token_cost=planned_cost,
            actual_token_cost=actual_cost,
            planned_duration_s=planned_dur,
            actual_duration_s=actual_dur,
            outcome=outcome,
            breaker_trips=breaker_trips,
            reschedule_events=reschedule_events,
            complexity_class=complexity,
            error_signature=error_sig,
            completed_at=completed_at,
            metadata=entry.get("metadata", {}),
        )

    def ingest_records(self, records: List[ExecutionOutcomeRecord]) -> List[ExecutionOutcomeRecord]:
        """Ingest pre-constructed records directly, honoring deduplication."""
        deduped: List[ExecutionOutcomeRecord] = []
        with self._lock:
            for r in records:
                if r.milestone_id not in self._processed_milestone_ids:
                    self._processed_milestone_ids.add(r.milestone_id)
                    deduped.append(r)
        return deduped


# ============================================================================
# Component 3: Variance Decomposer
# ============================================================================

class VarianceDecomposer:
    """
    Computes estimation error metrics:
    - cost estimation error = (actual_tokens - planned_tokens) / planned_tokens
    - duration estimation error = (actual_duration - planned_duration) / planned_duration
    - maintains running EWMA bias correction factor per repository (alpha=0.15)
    - computes cross-repository systematic bias (global planning optimism/pessimism)
    """
    def __init__(self, alpha: float = 0.15) -> None:
        self.alpha = min(max(float(alpha), 0.01), 0.99)
        self._lock = threading.Lock()
        # Repositories map to EWMA accumulators: {"cost_bias": float, "duration_bias": float}
        self._historical_bias: Dict[str, Dict[str, float]] = {}
        self._global_bias_history: List[float] = []

    def decompose(
        self,
        outcomes: List[ExecutionOutcomeRecord],
        cycle_id: Optional[str] = None,
    ) -> VarianceReport:
        """
        Decomposes variance for a batch of execution outcomes and updates EWMA bias.
        """
        with tracer.start_as_current_span("retrospective.variance_decompose"):
            if not outcomes:
                current_cycle = cycle_id or f"cycle_{int(time.time() // 300)}"
                return VarianceReport(
                    cycle_id=current_cycle,
                    timestamp=time.time(),
                    sample_count=0,
                    per_repository_bias=self.get_current_bias(),
                    global_optimism_coefficient=1.0,
                    worst_estimations=[],
                    mean_absolute_cost_error=0.0,
                    mean_absolute_duration_error=0.0,
                )

            current_cycle = cycle_id or outcomes[0].cycle_id
            cost_errors: List[float] = []
            dur_errors: List[float] = []
            repo_cost_deltas: Dict[str, List[float]] = {}
            repo_dur_deltas: Dict[str, List[float]] = {}
            total_planned_tokens = 0.0
            total_actual_tokens = 0.0

            estimation_entries: List[Dict[str, Any]] = []

            for rec in outcomes:
                p_cost = max(rec.planned_token_cost, 1.0)
                a_cost = rec.actual_token_cost
                c_err = (a_cost - p_cost) / p_cost
                cost_errors.append(c_err)

                total_planned_tokens += p_cost
                total_actual_tokens += a_cost

                p_dur = max(rec.planned_duration_s, 0.001)
                a_dur = rec.actual_duration_s
                d_err = (a_dur - p_dur) / p_dur
                dur_errors.append(d_err)

                repo = rec.repository
                repo_cost_deltas.setdefault(repo, []).append(c_err)
                repo_dur_deltas.setdefault(repo, []).append(d_err)

                combined_abs_err = abs(c_err) + abs(d_err)
                estimation_entries.append({
                    "milestone_id": rec.milestone_id,
                    "repository": rec.repository,
                    "cost_error": round(c_err, 4),
                    "duration_error": round(d_err, 4),
                    "combined_abs_error": round(combined_abs_err, 4),
                    "planned_cost": rec.planned_token_cost,
                    "actual_cost": rec.actual_token_cost,
                    "planned_duration": rec.planned_duration_s,
                    "actual_duration": rec.actual_duration_s,
                    "outcome": rec.outcome,
                })

            # Update per-repository EWMA
            with self._lock:
                for repo, c_list in repo_cost_deltas.items():
                    d_list = repo_dur_deltas[repo]
                    mean_c_err = sum(c_list) / len(c_list)
                    mean_d_err = sum(d_list) / len(d_list)

                    if repo not in self._historical_bias:
                        self._historical_bias[repo] = {
                            "cost_bias": mean_c_err,
                            "duration_bias": mean_d_err,
                            "sample_count": len(c_list),
                        }
                    else:
                        prev_c = self._historical_bias[repo]["cost_bias"]
                        prev_d = self._historical_bias[repo]["duration_bias"]
                        new_c = (self.alpha * mean_c_err) + ((1.0 - self.alpha) * prev_c)
                        new_d = (self.alpha * mean_d_err) + ((1.0 - self.alpha) * prev_d)
                        self._historical_bias[repo]["cost_bias"] = round(new_c, 4)
                        self._historical_bias[repo]["duration_bias"] = round(new_d, 4)
                        self._historical_bias[repo]["sample_count"] += len(c_list)

            # Global systematic optimism coefficient: sum(actual) / sum(planned)
            # If > 1.0, planning was optimistic (cost exceeded plan)
            global_opt = (
                total_actual_tokens / max(total_planned_tokens, 1.0)
                if total_planned_tokens > 0
                else 1.0
            )
            with self._lock:
                self._global_bias_history.append(global_opt)
                if len(self._global_bias_history) > 100:
                    self._global_bias_history.pop(0)

            # Statistical variance
            n = len(outcomes)
            mean_abs_cost = sum(abs(e) for e in cost_errors) / n
            mean_abs_dur = sum(abs(e) for e in dur_errors) / n

            mean_c = sum(cost_errors) / n
            mean_d = sum(dur_errors) / n
            cost_var = sum((e - mean_c) ** 2 for e in cost_errors) / max(n - 1, 1)
            dur_var = sum((e - mean_d) ** 2 for e in dur_errors) / max(n - 1, 1)

            underest = sum(1 for e in cost_errors if e > 0.05) / n
            overest = sum(1 for e in cost_errors if e < -0.05) / n

            # Worst 5 estimations by combined absolute error
            estimation_entries.sort(key=lambda x: x["combined_abs_error"], reverse=True)
            worst_5 = estimation_entries[:5]

            report = VarianceReport(
                cycle_id=current_cycle,
                timestamp=time.time(),
                sample_count=n,
                per_repository_bias=self.get_current_bias(),
                global_optimism_coefficient=round(global_opt, 4),
                worst_estimations=worst_5,
                mean_absolute_cost_error=round(mean_abs_cost, 4),
                mean_absolute_duration_error=round(mean_abs_dur, 4),
                cost_variance=round(cost_var, 4),
                duration_variance=round(dur_var, 4),
                underestimation_frequency=round(underest, 4),
                overestimation_frequency=round(overest, 4),
            )
            return report

    def get_current_bias(self) -> Dict[str, Dict[str, float]]:
        """Return a defensive copy of current EWMA biases."""
        with self._lock:
            return {
                k: {
                    "cost_bias": v["cost_bias"],
                    "duration_bias": v["duration_bias"],
                    "sample_count": v["sample_count"],
                }
                for k, v in self._historical_bias.items()
            }


# ============================================================================
# Component 4: Failure Pattern Clusterer
# ============================================================================

class FailurePatternClusterer:
    """
    Groups failure outcomes by error signature fingerprint (SHA-256).
    Tracks occurrences over a sliding window of cycles (N=20).
    Identifies 'chronic' failures (>=3 occurrences) vs 'transient' (1 occurrence).
    Emits suggested mitigation strategies based on signature and repository context.
    """
    def __init__(self, window_size: int = 20, chronic_threshold: int = 3) -> None:
        self.window_size = window_size
        self.chronic_threshold = chronic_threshold
        self._lock = threading.Lock()
        # Sliding window of cycle failure batches: list of dicts mapping sig -> list[ExecutionOutcomeRecord]
        self._cycle_failure_window: List[Dict[str, List[ExecutionOutcomeRecord]]] = []

    def cluster(
        self,
        outcomes: List[ExecutionOutcomeRecord],
        window_size: Optional[int] = None,
    ) -> List[FailureCluster]:
        """
        Ingests cycle outcomes, updates the sliding window, and clusters active failure signatures.
        """
        with tracer.start_as_current_span("retrospective.failure_cluster"):
            eff_window = window_size or self.window_size
            failures = [
                rec for rec in outcomes
                if rec.outcome in (
                    ExecutionOutcomeStatus.FAILURE.value,
                    ExecutionOutcomeStatus.ROLLBACK.value,
                    ExecutionOutcomeStatus.PARTIAL.value,
                )
            ]

            cycle_batch: Dict[str, List[ExecutionOutcomeRecord]] = {}
            for f in failures:
                sig = f.error_signature or self._generate_fallback_signature(f)
                cycle_batch.setdefault(sig, []).append(f)

            with self._lock:
                self._cycle_failure_window.append(cycle_batch)
                while len(self._cycle_failure_window) > eff_window:
                    self._cycle_failure_window.pop(0)

                # Aggregate over current sliding window
                sig_records: Dict[str, List[ExecutionOutcomeRecord]] = {}
                for batch in self._cycle_failure_window:
                    for sig, r_list in batch.items():
                        sig_records.setdefault(sig, []).extend(r_list)

            clusters: List[FailureCluster] = []
            for sig, recs in sig_records.items():
                if not recs:
                    continue

                occurrences = len(recs)
                first_seen = min(r.completed_at for r in recs)
                last_seen = max(r.completed_at for r in recs)
                repos = sorted(list(set(r.repository for r in recs)))
                sample_mids = [r.milestone_id for r in recs[:5]]
                c_ids = sorted(list(set(r.cycle_id for r in recs)))

                is_chronic = occurrences >= self.chronic_threshold
                category = self._classify_category(recs)
                mitigation = self._suggest_mitigation(category, recs, is_chronic)

                last_error_msg = None
                for r in reversed(recs):
                    if r.metadata.get("error_message"):
                        last_error_msg = str(r.metadata["error_message"])
                        break
                    elif r.metadata.get("error"):
                        last_error_msg = str(r.metadata["error"])
                        break

                cluster = FailureCluster(
                    signature=sig,
                    category=category.value,
                    occurrences=occurrences,
                    first_seen=first_seen,
                    last_seen=last_seen,
                    is_chronic=is_chronic,
                    affected_repositories=repos,
                    suggested_mitigation=mitigation.value,
                    sample_milestones=sample_mids,
                    cycle_ids=c_ids,
                    last_error_message=last_error_msg,
                )
                clusters.append(cluster)

            clusters.sort(key=lambda c: (c.is_chronic, c.occurrences), reverse=True)
            return clusters

    def _generate_fallback_signature(self, rec: ExecutionOutcomeRecord) -> str:
        """Generate deterministic fallback fingerprint if error_signature was absent."""
        raw = f"{rec.repository}:{rec.complexity_class}:{rec.outcome}:{rec.breaker_trips}:{rec.reschedule_events}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    def _classify_category(self, recs: List[ExecutionOutcomeRecord]) -> FailureCategory:
        """Heuristically classify the failure category based on metadata and metrics."""
        text_corpus = " ".join(
            f"{r.metadata.get('error', '')} {r.metadata.get('error_message', '')} {r.metadata.get('exception', '')}"
            for r in recs
        ).lower()

        if any(w in text_corpus for w in ["timeout", "timed out", "deadline exceeded"]):
            return FailureCategory.TIMEOUT
        if any(w in text_corpus for w in ["429", "rate limit", "quota", "too many requests"]):
            return FailureCategory.RATE_LIMIT_429
        if any(w in text_corpus for w in ["oom", "out of memory", "memoryerror"]):
            return FailureCategory.OOM_RESOURCE
        if any(w in text_corpus for w in ["syntaxerror", "indentationerror", "parse error"]):
            return FailureCategory.SYNTAX_ERROR
        if any(w in text_corpus for w in ["breaker", "tripped", "circuit open"]):
            return FailureCategory.CIRCUIT_BREAKER
        if any(w in text_corpus for w in ["conformance", "assertion", "test failed", "verification failed"]):
            return FailureCategory.CONFORMANCE_BREACH
        if any(w in text_corpus for w in ["importerror", "modulenotfounderror", "unresolved dependency"]):
            return FailureCategory.DEPENDENCY_RESOLUTION

        # Check breaker trips directly
        if any(r.breaker_trips > 0 for r in recs):
            return FailureCategory.CIRCUIT_BREAKER

        return FailureCategory.UNCLASSIFIED

    def _suggest_mitigation(
        self,
        category: FailureCategory,
        recs: List[ExecutionOutcomeRecord],
        is_chronic: bool,
    ) -> MitigationStrategy:
        """Derive actionable mitigation strategy based on category and failure behavior."""
        if category == FailureCategory.TIMEOUT:
            return MitigationStrategy.DECOMPOSE_SMALLER
        if category == FailureCategory.RATE_LIMIT_429:
            return MitigationStrategy.ADAPTIVE_TOKEN_THROTTLE
        if category == FailureCategory.CIRCUIT_BREAKER:
            # Check if breaker trips happened on high-duration or normal-duration milestones
            avg_dur_ratio = sum(r.duration_ratio for r in recs) / len(recs)
            if avg_dur_ratio < 0.5:
                # Breaker tripped very early: threshold may be too sensitive
                return MitigationStrategy.INCREASE_CIRCUIT_BREAKER_THRESHOLD
            return MitigationStrategy.ASSIGN_DIFFERENT_MODEL_TIER
        if category in (FailureCategory.SYNTAX_ERROR, FailureCategory.CONFORMANCE_BREACH):
            if is_chronic:
                return MitigationStrategy.ASSIGN_DIFFERENT_MODEL_TIER
            return MitigationStrategy.DECOMPOSE_SMALLER
        if category == FailureCategory.DEPENDENCY_RESOLUTION:
            return MitigationStrategy.QUARANTINE_MILESTONE

        # Unclassified fallback
        if is_chronic:
            return MitigationStrategy.ASSIGN_DIFFERENT_MODEL_TIER
        return MitigationStrategy.DECOMPOSE_SMALLER


# ============================================================================
# Component 5: Calibration Directive Emitter
# ============================================================================

class CalibrationDirectiveEmitter:
    """
    Produces CalibrationDirective records containing:
    - per-repository cost_multiplier_adjustment and duration_multiplier_adjustment
      to calibrate CapacityAllocator's ResourceProfile EWMA estimates.
    - global_optimism_correction factor for Capacity Planner's LP constraint bounds.
    - seasonal_coefficient_updates for PredictiveAnalytics' Holt-Winters forecasting.
    - circuit_breaker_threshold_adjustments when breaker trips are overly sensitive or lax.
    """
    def __init__(
        self,
        min_multiplier: float = 0.5,
        max_multiplier: float = 3.0,
    ) -> None:
        self.min_multiplier = min_multiplier
        self.max_multiplier = max_multiplier

    def emit(
        self,
        variance: VarianceReport,
        clusters: List[FailureCluster],
        cycle_id_override: Optional[str] = None,
    ) -> CalibrationDirective:
        """
        Synthesizes variance analysis and failure clusters into a calibration directive.
        """
        with tracer.start_as_current_span("retrospective.calibration_emit"):
            directive_id = str(uuid.uuid4())
            now = time.time()
            cycle_id = cycle_id_override or variance.cycle_id

            per_repo_adj: Dict[str, Dict[str, float]] = {}
            for repo, biases in variance.per_repository_bias.items():
                c_bias = biases.get("cost_bias", 0.0)
                d_bias = biases.get("duration_bias", 0.0)

                # If bias is positive (underestimating), multiplier increases above 1.0
                c_mult = max(self.min_multiplier, min(self.max_multiplier, 1.0 + c_bias))
                d_mult = max(self.min_multiplier, min(self.max_multiplier, 1.0 + d_bias))

                per_repo_adj[repo] = {
                    "cost_multiplier": round(c_mult, 4),
                    "duration_multiplier": round(d_mult, 4),
                    "cost_bias_observed": round(c_bias, 4),
                    "duration_bias_observed": round(d_bias, 4),
                }

            # Global optimism correction:
            # If planner is systematically optimistic (actual > planned, coeff > 1.0),
            # we tighten LP constraint bounds by factor 1.0 / coeff.
            opt_coeff = max(variance.global_optimism_coefficient, 0.1)
            global_opt_correction = round(1.0 / opt_coeff, 4)

            # Seasonal updates for Holt-Winters model:
            # Shift seasonal baseline based on recent throughput variance
            seasonal_updates: Dict[str, float] = {
                "token_throughput_seasonal_delta": round((1.0 - opt_coeff) * 0.1, 4),
                "duration_seasonal_delta": round(variance.mean_absolute_duration_error * 0.05, 4),
            }

            # Circuit breaker threshold adjustments:
            # Analyze clusters for breaker sensitivity issues
            breaker_adjustments: Dict[str, float] = {}
            cb_clusters = [c for c in clusters if c.category == FailureCategory.CIRCUIT_BREAKER.value]

            if any(c.suggested_mitigation == MitigationStrategy.INCREASE_CIRCUIT_BREAKER_THRESHOLD.value for c in cb_clusters):
                # Thresholds tripped too early on valid execution -> relax by 20%
                breaker_adjustments["circuit_breaker_sensitivity_multiplier"] = 1.20
            elif len(cb_clusters) == 0 and any(c.is_chronic for c in clusters):
                # Chronic failures occurring without any breaker tripping -> tighten breaker by 15%
                breaker_adjustments["circuit_breaker_sensitivity_multiplier"] = 0.85
            else:
                breaker_adjustments["circuit_breaker_sensitivity_multiplier"] = 1.0

            # Construct human-readable rationale
            chronic_count = sum(1 for c in clusters if c.is_chronic)
            rationale_parts = [
                f"Cycle {cycle_id} retrospective:",
                f"Sample count: {variance.sample_count}.",
                f"Global optimism coefficient: {variance.global_optimism_coefficient:.2f} (correction: {global_opt_correction:.2f}).",
                f"Mean absolute cost error: {variance.mean_absolute_cost_error * 100:.1f}%.",
                f"Mean absolute duration error: {variance.mean_absolute_duration_error * 100:.1f}%.",
                f"Active chronic failure clusters: {chronic_count}.",
            ]
            if per_repo_adj:
                top_adj_repo = max(per_repo_adj.items(), key=lambda item: abs(item[1]["cost_multiplier"] - 1.0))
                rationale_parts.append(
                    f"Highest variance repo '{top_adj_repo[0]}' cost mult adjusted to {top_adj_repo[1]['cost_multiplier']:.2f}."
                )

            rationale = " ".join(rationale_parts)

            directive = CalibrationDirective(
                directive_id=directive_id,
                timestamp=now,
                cycle_id=cycle_id,
                per_repo_adjustments=per_repo_adj,
                global_optimism_correction=global_opt_correction,
                seasonal_updates=seasonal_updates,
                breaker_threshold_adjustments=breaker_adjustments,
                rationale=rationale,
            )
            return directive


# ============================================================================
# Component 6: Retrospective Health Contributor
# ============================================================================

class RetrospectiveHealthContributor:
    """
    Computes a retrospective_accuracy_score in [0, 1] as a weighted combination:
    - mean_absolute_cost_error (weight=0.35)
    - mean_absolute_duration_error (weight=0.25)
    - chronic_failure_ratio (weight=0.20)
    - calibration_staleness (weight=0.20), decaying as 1 / (1 + 0.1 * cycles_since_calibration)
    """
    def __init__(
        self,
        weight_cost: float = 0.35,
        weight_duration: float = 0.25,
        weight_chronic: float = 0.20,
        weight_staleness: float = 0.20,
    ) -> None:
        self.w_cost = weight_cost
        self.w_dur = weight_duration
        self.w_chronic = weight_chronic
        self.w_staleness = weight_staleness

    def compute_score(
        self,
        variance: VarianceReport,
        clusters: List[FailureCluster],
        cycles_since_calibration: int = 0,
    ) -> RetrospectiveHealthScore:
        """
        Evaluates estimation errors, failure chronicities, and staleness into a health score.
        """
        # Cost accuracy component in [0, 1] (penalty proportional to mean absolute cost error)
        cost_acc = max(0.0, 1.0 - min(variance.mean_absolute_cost_error, 1.0))

        # Duration accuracy component in [0, 1]
        dur_acc = max(0.0, 1.0 - min(variance.mean_absolute_duration_error, 1.0))

        # Chronic failure component in [0, 1]
        total_clusters = len(clusters)
        if total_clusters == 0:
            chronic_ratio = 0.0
            chronic_acc = 1.0
        else:
            chronic_count = sum(1 for c in clusters if c.is_chronic)
            chronic_ratio = chronic_count / total_clusters
            chronic_acc = max(0.0, 1.0 - chronic_ratio)

        # Staleness component decaying as 1 / (1 + 0.1 * staleness_cycles)
        stale_cycles = max(0, int(cycles_since_calibration))
        staleness_acc = 1.0 / (1.0 + 0.1 * stale_cycles)

        total_score = (
            (self.w_cost * cost_acc)
            + (self.w_dur * dur_acc)
            + (self.w_chronic * chronic_acc)
            + (self.w_staleness * staleness_acc)
        )
        total_score = max(0.0, min(1.0, round(total_score, 4)))

        return RetrospectiveHealthScore(
            score=total_score,
            mean_absolute_cost_error=variance.mean_absolute_cost_error,
            mean_absolute_duration_error=variance.mean_absolute_duration_error,
            chronic_failure_ratio=round(chronic_ratio, 4),
            calibration_staleness_cycles=stale_cycles,
            cost_accuracy_component=round(cost_acc, 4),
            duration_accuracy_component=round(dur_acc, 4),
            chronic_failure_component=round(chronic_acc, 4),
            staleness_component=round(staleness_acc, 4),
            timestamp=time.time(),
        )

    def get_health_contribution(
        self,
        variance: VarianceReport,
        clusters: List[FailureCluster],
        cycles_since_calibration: int = 0,
    ) -> Dict[str, Any]:
        """Format health contributor dictionary compatible with PipelineHealthScorer."""
        h_score = self.compute_score(variance, clusters, cycles_since_calibration)
        return {
            "name": "retrospective_accuracy_score",
            "score": h_score.score,
            "weight": 0.15,  # Pipeline-level weight
            "status": "HEALTHY" if h_score.score >= 0.75 else ("DEGRADED" if h_score.score >= 0.50 else "CRITICAL"),
            "details": h_score.to_dict(),
        }


# ============================================================================
# Component 7: Retrospective Analytics API Handler
# ============================================================================

class RetrospectiveAnalyticsAPIHandler:
    """
    REST API Handler providing diagnostic and analytical endpoints:
    - GET /api/retrospective/cycles: Paginated execution cycle summaries
    - GET /api/retrospective/cycles/<id>/variance: Detailed variance decomposition
    - GET /api/retrospective/calibration/latest: Most recent CalibrationDirective
    - GET /api/retrospective/failures/chronic: Active chronic failure clusters
    - GET /api/retrospective/health: Current retrospective health assessment
    """
    def __init__(self, engine: "RetrospectiveAnalyzerEngine") -> None:
        self.engine = engine

    def register_routes(self, app_or_dict: Any) -> None:
        """
        Registers routes onto a Flask app/Blueprint, FastAPI router, or dictionary routing table.
        """
        # Flask / Quart Blueprint or App
        if hasattr(app_or_dict, "add_url_rule"):
            app_or_dict.add_url_rule(
                "/api/retrospective/cycles",
                view_func=self.handle_get_cycles,
                methods=["GET"],
            )
            app_or_dict.add_url_rule(
                "/api/retrospective/cycles/<cycle_id>/variance",
                view_func=self.handle_get_cycle_variance,
                methods=["GET"],
            )
            app_or_dict.add_url_rule(
                "/api/retrospective/calibration/latest",
                view_func=self.handle_get_latest_calibration,
                methods=["GET"],
            )
            app_or_dict.add_url_rule(
                "/api/retrospective/failures/chronic",
                view_func=self.handle_get_chronic_failures,
                methods=["GET"],
            )
            app_or_dict.add_url_rule(
                "/api/retrospective/health",
                view_func=self.handle_get_health,
                methods=["GET"],
            )
            logger.info("Registered retrospective REST endpoints on Flask/Quart router")

        # FastAPI / Starlette router
        elif hasattr(app_or_dict, "get") and callable(getattr(app_or_dict, "get")):
            try:
                app_or_dict.get("/api/retrospective/cycles")(self.handle_get_cycles)
                app_or_dict.get("/api/retrospective/cycles/{cycle_id}/variance")(self.handle_get_cycle_variance)
                app_or_dict.get("/api/retrospective/calibration/latest")(self.handle_get_latest_calibration)
                app_or_dict.get("/api/retrospective/failures/chronic")(self.handle_get_chronic_failures)
                app_or_dict.get("/api/retrospective/health")(self.handle_get_health)
                logger.info("Registered retrospective REST endpoints on FastAPI router")
            except Exception:
                self._register_as_dict(app_or_dict)

        # Dictionary / Lookup registry fallback
        elif isinstance(app_or_dict, dict):
            self._register_as_dict(app_or_dict)

    def _register_as_dict(self, route_dict: Dict[str, Any]) -> None:
        """Register routes as dictionary mapping path to handler function."""
        route_dict["GET /api/retrospective/cycles"] = self.handle_get_cycles
        route_dict["GET /api/retrospective/cycles/{cycle_id}/variance"] = self.handle_get_cycle_variance
        route_dict["GET /api/retrospective/calibration/latest"] = self.handle_get_latest_calibration
        route_dict["GET /api/retrospective/failures/chronic"] = self.handle_get_chronic_failures
        route_dict["GET /api/retrospective/health"] = self.handle_get_health
        logger.info("Registered retrospective REST endpoints on route dictionary")

    def handle_get_cycles(self, request_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Handler for GET /api/retrospective/cycles."""
        params = request_params or {}
        limit = int(params.get("limit", 20))
        offset = int(params.get("offset", 0))
        history = self.engine.get_cycle_history(limit=limit, offset=offset)
        return {
            "status": "success",
            "count": len(history),
            "offset": offset,
            "limit": limit,
            "cycles": history,
        }

    def handle_get_cycle_variance(self, cycle_id: str) -> Dict[str, Any]:
        """Handler for GET /api/retrospective/cycles/<cycle_id>/variance."""
        variance = self.engine.get_cycle_variance(cycle_id)
        if not variance:
            return {
                "status": "not_found",
                "message": f"Variance report for cycle '{cycle_id}' not found.",
            }
        return {
            "status": "success",
            "cycle_id": cycle_id,
            "variance": variance.to_dict(),
        }

    def handle_get_latest_calibration(self) -> Dict[str, Any]:
        """Handler for GET /api/retrospective/calibration/latest."""
        directive = self.engine.get_latest_directive()
        if not directive:
            return {
                "status": "pending",
                "message": "No calibration directive emitted yet.",
            }
        return {
            "status": "success",
            "directive": directive.to_dict(),
        }

    def handle_get_chronic_failures(self) -> Dict[str, Any]:
        """Handler for GET /api/retrospective/failures/chronic."""
        chronic_clusters = self.engine.get_chronic_failures()
        return {
            "status": "success",
            "count": len(chronic_clusters),
            "chronic_failures": [c.to_dict() for c in chronic_clusters],
        }

    def handle_get_health(self) -> Dict[str, Any]:
        """Handler for GET /api/retrospective/health."""
        health = self.engine.get_health_score_record()
        return {
            "status": "success",
            "health": health.to_dict() if health else None,
            "score": self.engine.get_health_score(),
        }


# ============================================================================
# Component 8: Retrospective Analyzer Engine
# ============================================================================

class RetrospectiveAnalyzerEngine:
    """
    Singleton background engine executing closed-loop retrospective analysis.
    Periodically executes cycles: Ingest -> Decompose -> Cluster -> Emit -> Log -> Score.
    Provides thread-safe access to calibration state via ReaderWriterLock.
    """
    _instance: Optional["RetrospectiveAnalyzerEngine"] = None
    _singleton_lock = threading.Lock()

    @classmethod
    def get_instance(cls, config: Optional[Dict[str, Any]] = None) -> "RetrospectiveAnalyzerEngine":
        """Thread-safe singleton accessor."""
        with cls._singleton_lock:
            if cls._instance is None:
                cls._instance = cls(config=config)
            return cls._instance

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        cfg = config or {}
        self.cycle_interval_s = float(cfg.get("cycle_interval_s", 300.0))
        self.log_dir = Path(cfg.get("log_dir", "logs/retrospective"))
        self.max_history_cycles = int(cfg.get("max_history_cycles", 100))
        self.alpha = float(cfg.get("alpha", 0.15))
        self.window_size = int(cfg.get("window_size", 20))

        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.audit_file = self.log_dir / "retrospective_audit.jsonl"
        watermark_file = cfg.get("watermark_path") or (self.log_dir / ".retrospective_watermark.json")

        self.rw_lock = ReaderWriterLock()
        self.ingestion_engine = OutcomeIngestionEngine(
            watermark_file=watermark_file,
            log_dir=self.log_dir,
        )
        self.variance_decomposer = VarianceDecomposer(alpha=self.alpha)
        self.failure_clusterer = FailurePatternClusterer(window_size=self.window_size)
        self.directive_emitter = CalibrationDirectiveEmitter()
        self.health_contributor = RetrospectiveHealthContributor()
        self.api_handler = RetrospectiveAnalyticsAPIHandler(self)

        self._cycle_history: List[Dict[str, Any]] = []
        self._variance_reports: Dict[str, VarianceReport] = {}
        self._latest_directive: Optional[CalibrationDirective] = None
        self._latest_health_score: Optional[RetrospectiveHealthScore] = None
        self._active_clusters: List[FailureCluster] = []
        self._cycles_since_directive: int = 0

        self._running = False
        self._worker_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        """Start the background retrospective analysis loop."""
        with self.rw_lock.write_locked():
            if self._running:
                logger.warning("RetrospectiveAnalyzerEngine is already running.")
                return
            self._running = True
            self._stop_event.clear()
            self._worker_thread = threading.Thread(
                target=self._run_loop,
                name="RetrospectiveAnalyzerLoop",
                daemon=True,
            )
            self._worker_thread.start()
            logger.info(
                "Started RetrospectiveAnalyzerEngine background loop (interval: %.1fs)",
                self.cycle_interval_s,
            )

    def stop(self, timeout_s: float = 10.0) -> None:
        """Gracefully terminate the background loop."""
        with self.rw_lock.write_locked():
            if not self._running:
                return
            self._running = False
            self._stop_event.set()

        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=timeout_s)
            logger.info("Stopped RetrospectiveAnalyzerEngine background loop.")

    def _run_loop(self) -> None:
        """Continuous execution loop invoking run_cycle at scheduled intervals."""
        while not self._stop_event.is_set():
            try:
                self.run_cycle()
            except Exception as e:
                logger.error("Unhandled exception in retrospective cycle: %s", e, exc_info=True)

            self._stop_event.wait(timeout=self.cycle_interval_s)

    def run_cycle(
        self,
        injected_outcomes: Optional[List[ExecutionOutcomeRecord]] = None,
        cycle_id_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Executes one full closed-loop retrospective pass:
        1. Ingest completed outcomes from JSONL / injected sources.
        2. Decompose cost & duration variance, updating EWMA biases.
        3. Cluster failure signatures across the sliding window.
        4. Synthesize and emit CalibrationDirective.
        5. Write cycle audit record to retrospective JSONL trail.
        6. Compute retrospective accuracy health score.
        """
        with tracer.start_as_current_span("retrospective.run_cycle"):
            now = time.time()
            cycle_id = cycle_id_override or f"cycle_{int(now // self.cycle_interval_s)}"

            logger.info("Executing retrospective analysis for %s", cycle_id)

            # 1. Ingestion
            if injected_outcomes is not None:
                outcomes = self.ingestion_engine.ingest_records(injected_outcomes)
            else:
                outcomes = self.ingestion_engine.ingest_from_jsonl(cycle_id_override=cycle_id)

            # 2. Variance Decomposition
            variance = self.variance_decomposer.decompose(outcomes, cycle_id=cycle_id)

            # 3. Failure Pattern Clustering
            clusters = self.failure_clusterer.cluster(outcomes)

            # 4. Calibration Directive Emission
            directive = self.directive_emitter.emit(variance, clusters, cycle_id_override=cycle_id)

            # 5. Health Computation
            health_score = self.health_contributor.compute_score(
                variance=variance,
                clusters=clusters,
                cycles_since_calibration=self._cycles_since_directive,
            )

            # 6. Audit Logging to JSONL
            audit_entry = {
                "timestamp": now,
                "timestamp_iso": datetime.now(timezone.utc).isoformat(),
                "cycle_id": cycle_id,
                "sample_count": len(outcomes),
                "variance": variance.to_dict(),
                "clusters_count": len(clusters),
                "chronic_clusters_count": sum(1 for c in clusters if c.is_chronic),
                "directive": directive.to_dict(),
                "health_score": health_score.to_dict(),
            }
            self._write_audit_entry(audit_entry)

            # 7. State Update with Write Lock
            with self.rw_lock.write_locked():
                self._variance_reports[cycle_id] = variance
                self._latest_directive = directive
                self._latest_health_score = health_score
                self._active_clusters = clusters
                self._cycles_since_directive = 0  # Reset upon fresh directive emission

                self._cycle_history.append({
                    "cycle_id": cycle_id,
                    "timestamp": now,
                    "sample_count": len(outcomes),
                    "mean_cost_error": variance.mean_absolute_cost_error,
                    "mean_duration_error": variance.mean_absolute_duration_error,
                    "health_score": health_score.score,
                    "directive_id": directive.directive_id,
                })

                if len(self._cycle_history) > self.max_history_cycles:
                    self._cycle_history.pop(0)

            logger.info(
                "Completed retrospective cycle %s (score: %.3f, samples: %d)",
                cycle_id,
                health_score.score,
                len(outcomes),
            )
            return audit_entry

    def _write_audit_entry(self, entry: Dict[str, Any]) -> None:
        """Append audit entry safely to JSONL file."""
        try:
            self.audit_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.audit_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, default=str) + "\n")
        except Exception as e:
            logger.error("Failed writing retrospective audit entry: %s", e)

    def get_latest_directive(self) -> Optional[CalibrationDirective]:
        """Return the most recently emitted CalibrationDirective."""
        with self.rw_lock.read_locked():
            return self._latest_directive

    def get_health_score(self) -> float:
        """Return current retrospective accuracy score in [0, 1]."""
        with self.rw_lock.read_locked():
            if self._latest_health_score:
                return self._latest_health_score.score
            return 1.0

    def get_health_score_record(self) -> Optional[RetrospectiveHealthScore]:
        """Return full RetrospectiveHealthScore dataclass record."""
        with self.rw_lock.read_locked():
            return self._latest_health_score

    def get_health_contribution(self) -> Dict[str, Any]:
        """Return health contribution payload for PipelineHealthScorer."""
        with self.rw_lock.read_locked():
            if not self._latest_health_score or not self._cycle_history:
                return {
                    "name": "retrospective_accuracy_score",
                    "score": 1.0,
                    "weight": 0.15,
                    "status": "HEALTHY",
                    "details": {"message": "No cycles completed yet."},
                }
            last_cycle = self._cycle_history[-1]["cycle_id"]
            variance = self._variance_reports.get(last_cycle)
            if not variance:
                return {
                    "name": "retrospective_accuracy_score",
                    "score": self._latest_health_score.score,
                    "weight": 0.15,
                    "status": "HEALTHY",
                    "details": self._latest_health_score.to_dict(),
                }
            return self.health_contributor.get_health_contribution(
                variance=variance,
                clusters=self._active_clusters,
                cycles_since_calibration=self._cycles_since_directive,
            )

    def get_cycle_history(self, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """Return paginated cycle history summaries."""
        with self.rw_lock.read_locked():
            sliced = self._cycle_history[offset: offset + limit]
            return list(sliced)

    def get_cycle_variance(self, cycle_id: str) -> Optional[VarianceReport]:
        """Return detailed variance decomposition for a specific cycle."""
        with self.rw_lock.read_locked():
            return self._variance_reports.get(cycle_id)

    def get_chronic_failures(self) -> List[FailureCluster]:
        """Return active chronic failure clusters."""
        with self.rw_lock.read_locked():
            return [c for c in self._active_clusters if c.is_chronic]


# ============================================================================
# Public API Exports
# ============================================================================

__all__ = [
    "ExecutionOutcomeStatus",
    "FailureCategory",
    "MitigationStrategy",
    "ExecutionOutcomeRecord",
    "VarianceReport",
    "FailureCluster",
    "CalibrationDirective",
    "RetrospectiveHealthScore",
    "OutcomeIngestionEngine",
    "VarianceDecomposer",
    "FailurePatternClusterer",
    "CalibrationDirectiveEmitter",
    "RetrospectiveHealthContributor",
    "RetrospectiveAnalyticsAPIHandler",
    "RetrospectiveAnalyzerEngine",
]


# ============================================================================
# Unit Self-Test & Diagnostic Harness
# ============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("Hermes Pipeline Retrospective Analyzer — Self-Test & Verification")
    print("=" * 70)

    # Initialize Engine with ephemeral log dir
    test_dir = Path("/tmp/hermes_retrospective_test")
    test_dir.mkdir(parents=True, exist_ok=True)

    engine = RetrospectiveAnalyzerEngine({
        "cycle_interval_s": 5.0,
        "log_dir": test_dir,
        "max_history_cycles": 10,
        "alpha": 0.20,
        "window_size": 5,
    })

    # Synthesize simulated execution outcome records across 2 cycles
    now = time.time()
    sample_outcomes_cycle_1 = [
        ExecutionOutcomeRecord(
            milestone_id="m-101",
            repository="repo-core",
            cycle_id="cycle_1",
            planned_token_cost=1000.0,
            actual_token_cost=1400.0,  # 40% cost underestimation
            planned_duration_s=50.0,
            actual_duration_s=65.0,
            outcome=ExecutionOutcomeStatus.SUCCESS.value,
            complexity_class="STANDARD",
            completed_at=now - 200,
        ),
        ExecutionOutcomeRecord(
            milestone_id="m-102",
            repository="repo-core",
            cycle_id="cycle_1",
            planned_token_cost=2000.0,
            actual_token_cost=2200.0,  # 10% underestimation
            planned_duration_s=100.0,
            actual_duration_s=110.0,
            outcome=ExecutionOutcomeStatus.SUCCESS.value,
            complexity_class="HEAVY",
            completed_at=now - 190,
        ),
        ExecutionOutcomeRecord(
            milestone_id="m-103",
            repository="repo-client",
            cycle_id="cycle_1",
            planned_token_cost=800.0,
            actual_token_cost=900.0,
            planned_duration_s=30.0,
            actual_duration_s=120.0,  # Timeout failure
            outcome=ExecutionOutcomeStatus.FAILURE.value,
            breaker_trips=1,
            complexity_class="LIGHT",
            error_signature="sig-timeout-client-30s",
            completed_at=now - 180,
            metadata={"error": "deadline exceeded after 120s timeout"},
        ),
    ]

    print("\n--- Running Cycle 1 (Initial Ingestion & Calibration) ---")
    result_1 = engine.run_cycle(injected_outcomes=sample_outcomes_cycle_1, cycle_id_override="cycle_1")
    v1 = engine.get_cycle_variance("cycle_1")
    d1 = engine.get_latest_directive()
    h1 = engine.get_health_score()

    print(f"Cycle 1 Samples: {result_1['sample_count']}")
    if v1:
        print(f"Global Optimism Coeff: {v1.global_optimism_coefficient}")
        print(f"Per-Repo Bias: {v1.per_repository_bias}")
    if d1:
        print(f"Directive ID: {d1.directive_id}")
        print(f"Rationale: {d1.rationale}")
        print(f"Per-Repo Adjustments: {d1.per_repo_adjustments}")
    print(f"Health Score: {h1:.4f}")

    # Cycle 2 with recurring failure to trigger Chronic Cluster
    sample_outcomes_cycle_2 = [
        ExecutionOutcomeRecord(
            milestone_id="m-201",
            repository="repo-core",
            cycle_id="cycle_2",
            planned_token_cost=1500.0,
            actual_token_cost=1600.0,
            planned_duration_s=60.0,
            actual_duration_s=62.0,
            outcome=ExecutionOutcomeStatus.SUCCESS.value,
            completed_at=now - 100,
        ),
        ExecutionOutcomeRecord(
            milestone_id="m-202",
            repository="repo-client",
            cycle_id="cycle_2",
            planned_token_cost=800.0,
            actual_token_cost=850.0,
            planned_duration_s=30.0,
            actual_duration_s=125.0,
            outcome=ExecutionOutcomeStatus.FAILURE.value,
            breaker_trips=1,
            complexity_class="LIGHT",
            error_signature="sig-timeout-client-30s",  # Occurrence 2
            completed_at=now - 90,
            metadata={"error": "deadline exceeded after 125s timeout"},
        ),
        ExecutionOutcomeRecord(
            milestone_id="m-203",
            repository="repo-client",
            cycle_id="cycle_2",
            planned_token_cost=800.0,
            actual_token_cost=880.0,
            planned_duration_s=30.0,
            actual_duration_s=130.0,
            outcome=ExecutionOutcomeStatus.FAILURE.value,
            breaker_trips=2,
            complexity_class="LIGHT",
            error_signature="sig-timeout-client-30s",  # Occurrence 3 -> Chronic
            completed_at=now - 80,
            metadata={"error": "deadline exceeded after 130s timeout"},
        ),
    ]

    print("\n--- Running Cycle 2 (Trigger Chronic Failure Detection) ---")
    result_2 = engine.run_cycle(injected_outcomes=sample_outcomes_cycle_2, cycle_id_override="cycle_2")
    chronic_clusters = engine.get_chronic_failures()
    d2 = engine.get_latest_directive()
    h2 = engine.get_health_score()

    print(f"Cycle 2 Samples: {result_2['sample_count']}")
    print(f"Active Chronic Failure Clusters Detected: {len(chronic_clusters)}")
    for c in chronic_clusters:
        print(f"  - Sig: {c.signature} | Category: {c.category} | Occurrences: {c.occurrences} | Mitigation: {c.suggested_mitigation}")
    print(f"Health Score: {h2:.4f}")

    print("\n--- Testing REST API Handler Route Registration ---")
    route_table: Dict[str, Any] = {}
    engine.api_handler.register_routes(route_table)
    print(f"Registered {len(route_table)} routes in dictionary lookup.")
    cycles_resp = engine.api_handler.handle_get_cycles({"limit": 5})
    print(f"API GET /api/retrospective/cycles response count: {cycles_resp['count']}")
    chronic_resp = engine.api_handler.handle_get_chronic_failures()
    print(f"API GET /api/retrospective/failures/chronic count: {chronic_resp['count']}")

    print("\n[SUCCESS] Pipeline Post-Execution Retrospective Analyzer verified successfully.")
    print("=" * 70)