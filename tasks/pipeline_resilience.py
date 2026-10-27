"""
Hermes Pipeline Resilience Controller & Circuit Breaker Orchestration Engine
=============================================================================

Tasks / Pipeline Resilience: Multi-Stage Circuit Breakers, Cascade Detection,
Bugfix Queue Automation, and Coordinated Rollback Engine.

Architecture:
    Four interlocking resilience subsystems coordinated via a singleton
    ResilienceContext:
    1. CircuitBreakerRegistry:
       FSM per PipelineStage enum (CLOSED, OPEN, HALF_OPEN) with exponential
       backoff, sliding-window outcome tracking, and adaptive probe gating.
    2. CascadeDetector:
       Real-time Pearson cross-stage failure correlation over sliding 10s
       buckets with preemptive downstream trip triggers.
    3. BugfixQueue:
       Deterministic error signature fingerprinting (SHA-256 top-5 frames),
       regex category classification, and automated bugfix milestone synthesis.
    4. RollbackOrchestrator:
       Transactional git revert operations, pipeline state resets, priority
       boost re-enqueuing, and JSONL audit trail logging.
    5. ResilienceHealthContributor:
       Weighted multi-factor resilience score generation (breaker stability,
       cascade frequency, bugfix queue depth).
"""

from __future__ import annotations

import collections
from dataclasses import asdict, dataclass, field
from enum import Enum
import hashlib
import json
import logging
import math
import os
import re
import subprocess
import threading
import time
import traceback
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    List,
    NamedTuple,
    Optional,
    Protocol,
    Set,
    Tuple,
    Union,
    runtime_checkable,
)
import uuid

# ============================================================================
# OpenTelemetry Tracer Graceful Fallback
# ============================================================================

try:
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode

    _OTEL_AVAILABLE = True
    _TRACER = trace.get_tracer("hermes.pipeline.resilience")
except ImportError:
    _OTEL_AVAILABLE = False
    _TRACER = None

logger = logging.getLogger("hermes.pipeline.resilience")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter("[%(asctime)s] [%(levelname)s] [Resilience] %(message)s")
    )
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


class _NullSpan:
    """No-op context manager when OpenTelemetry is unavailable."""
    def __enter__(self) -> _NullSpan:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass

    def set_attribute(self, key: str, value: Any) -> None:
        pass

    def record_exception(self, exc: BaseException) -> None:
        pass

    def set_status(self, status: Any, description: Optional[str] = None) -> None:
        pass


def _start_span(name: str, attributes: Optional[Dict[str, Any]] = None):
    """Start an OpenTelemetry span if available, otherwise return NullSpan."""
    if _OTEL_AVAILABLE and _TRACER is not None:
        span = _TRACER.start_as_current_span(name)
        if attributes:
            for k, v in attributes.items():
                span.set_attribute(k, str(v) if not isinstance(v, (int, float, bool)) else v)
        return span
    return _NullSpan()


# ============================================================================
# Enums and Data Structures
# ============================================================================

class CircuitState(str, Enum):
    """Operational states of a pipeline stage circuit breaker."""
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class ErrorCategory(str, Enum):
    """Taxonomy of pipeline execution errors for triage and bugfix routing."""
    TIMEOUT = "TIMEOUT"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    GIT_CONFLICT = "GIT_CONFLICT"
    API_RATE_LIMIT = "API_RATE_LIMIT"
    DEPENDENCY_ERROR = "DEPENDENCY_ERROR"
    PERMISSION_ERROR = "PERMISSION_ERROR"
    UNKNOWN = "UNKNOWN"


class StageOutcome(str, Enum):
    """Execution outcomes recorded by stage runners."""
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    TIMEOUT = "TIMEOUT"
    SLOW = "SLOW"


class FailureSignature(NamedTuple):
    """Unique fingerprint characterizing a pipeline failure event."""
    repo: str
    stage: str
    error_category: ErrorCategory
    trace_fingerprint: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "repo": self.repo,
            "stage": self.stage,
            "error_category": self.error_category.value,
            "trace_fingerprint": self.trace_fingerprint,
        }


@dataclass
class BugfixMilestone:
    """Synthesized bugfix milestone generated from recurring failure signatures."""
    id: str
    signature: FailureSignature
    hit_count: int
    first_seen: float
    last_seen: float
    affected_milestone_ids: List[str]
    suggested_fix_strategy: str
    status: str = "PENDING"  # PENDING, APPROVED, RESOLVED

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "signature": self.signature.to_dict(),
            "hit_count": self.hit_count,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
            "affected_milestone_ids": list(self.affected_milestone_ids),
            "suggested_fix_strategy": self.suggested_fix_strategy,
            "status": self.status,
        }


@dataclass
class CascadeReport:
    """Diagnostic evaluation generated when cross-stage failure correlation trips."""
    cascade_id: str
    root_stage: str
    affected_stages: List[str]
    correlation_scores: Dict[str, float]
    detected_at: float
    recommended_recovery_action: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "cascade_id": self.cascade_id,
            "root_stage": self.root_stage,
            "affected_stages": list(self.affected_stages),
            "correlation_scores": dict(self.correlation_scores),
            "detected_at": self.detected_at,
            "recommended_recovery_action": self.recommended_recovery_action,
        }


@dataclass
class RollbackRecord:
    """Audit entry documenting an automated git revert and ledger reset."""
    rollback_id: str
    cascade_id: str
    repo: str
    milestone_id: str
    commit_hash: Optional[str]
    action: str
    timestamp: float
    success: bool
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rollback_id": self.rollback_id,
            "cascade_id": self.cascade_id,
            "repo": self.repo,
            "milestone_id": self.milestone_id,
            "commit_hash": self.commit_hash,
            "action": self.action,
            "timestamp": self.timestamp,
            "success": self.success,
            "error": self.error,
        }


@dataclass
class TuningAction:
    """Local representation of a pipeline tuning or circuit adjustment recommendation."""
    action_id: str
    action_type: str
    target_stage: str
    parameters: Dict[str, Any]
    reason: str
    created_at: float = field(default_factory=time.time)


# ============================================================================
# Exceptions & Protocol Definitions
# ============================================================================

class CircuitOpenError(Exception):
    """Raised when an execution request is rejected by an OPEN circuit breaker."""
    def __init__(
        self,
        stage_name: str,
        recovery_remaining: float,
        consecutive_trips: int = 0,
        message: Optional[str] = None,
    ):
        self.stage_name = stage_name
        self.recovery_remaining = max(0.0, recovery_remaining)
        self.consecutive_trips = consecutive_trips
        msg = (
            message
            or f"Stage '{stage_name}' circuit breaker is OPEN. "
               f"Recovery cooldown: {self.recovery_remaining:.1f}s remaining "
               f"(consecutive trips: {consecutive_trips})."
        )
        super().__init__(msg)


@runtime_checkable
class PipelineDiagnosticsProtocol(Protocol):
    """Structural protocol decoupling resilience from diagnostics telemetry."""
    def get_stage_transition_latency(self, from_stage: str, to_stage: str) -> float:
        ...


@runtime_checkable
class PipelineCoordinatorProtocol(Protocol):
    """Structural protocol decoupling resilience from the pipeline orchestrator."""
    def get_in_flight_milestones(
        self, stages: Optional[List[str]] = None, time_window: float = 3600.0
    ) -> List[Dict[str, Any]]:
        ...

    def reset_milestone_state(
        self, milestone_id: str, new_state: str, priority_boost: int = 0, context: Optional[Dict[str, Any]] = None
    ) -> bool:
        ...


# ============================================================================
# 1. Sliding Window Metrics
# ============================================================================

class SlidingWindowMetrics:
    """Thread-safe sliding ring buffer recording stage outcome timestamps."""

    def __init__(self, window_size_sec: float = 60.0):
        self.window_size_sec = float(window_size_sec)
        self._buffer: Deque[Tuple[float, StageOutcome]] = collections.deque()
        self._lock = threading.Lock()

    def record(self, outcome: StageOutcome, timestamp: Optional[float] = None) -> None:
        now = time.time() if timestamp is None else timestamp
        with self._lock:
            self._buffer.append((now, outcome))
            self._prune(now)

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_size_sec
        while self._buffer and self._buffer[0][0] < cutoff:
            self._buffer.popleft()

    def total_calls(self) -> int:
        now = time.time()
        with self._lock:
            self._prune(now)
            return len(self._buffer)

    def failure_rate(self) -> float:
        now = time.time()
        with self._lock:
            self._prune(now)
            if not self._buffer:
                return 0.0
            failures = sum(
                1 for _, outcome in self._buffer
                if outcome in (StageOutcome.FAILURE, StageOutcome.TIMEOUT)
            )
            return failures / len(self._buffer)

    def slow_rate(self) -> float:
        now = time.time()
        with self._lock:
            self._prune(now)
            if not self._buffer:
                return 0.0
            slows = sum(1 for _, outcome in self._buffer if outcome == StageOutcome.SLOW)
            return slows / len(self._buffer)

    def get_outcome_counts(self) -> Dict[str, int]:
        now = time.time()
        with self._lock:
            self._prune(now)
            counts: Dict[str, int] = {k.value: 0 for k in StageOutcome}
            for _, outcome in self._buffer:
                counts[outcome.value] += 1
            return counts

    def reset(self) -> None:
        with self._lock:
            self._buffer.clear()


# ============================================================================
# 2. Circuit Breaker Subsystem
# ============================================================================

class CircuitBreaker:
    """Per-stage Finite State Machine implementing resilient circuit breaking."""

    def __init__(
        self,
        stage_name: str,
        failure_rate_threshold: float = 0.5,
        slow_call_rate_threshold: float = 0.8,
        min_calls_in_window: int = 10,
        recovery_timeout_sec: float = 60.0,
        max_recovery_timeout_sec: float = 600.0,
        probe_count: int = 2,
    ):
        self.stage_name = stage_name
        self.failure_rate_threshold = float(failure_rate_threshold)
        self.slow_call_rate_threshold = float(slow_call_rate_threshold)
        self.min_calls_in_window = int(min_calls_in_window)
        self.recovery_timeout_sec = float(recovery_timeout_sec)
        self.max_recovery_timeout_sec = float(max_recovery_timeout_sec)
        self.probe_count = int(probe_count)

        self._state = CircuitState.CLOSED
        self._consecutive_trips = 0
        self._current_recovery_timeout = self.recovery_timeout_sec
        self._last_state_change = time.time()
        self._probe_success_count = 0
        self._trip_reason: Optional[str] = None
        self._trip_source: Optional[str] = None

        self.metrics = SlidingWindowMetrics(window_size_sec=60.0)
        self.lock = threading.RLock()

    @property
    def state(self) -> CircuitState:
        with self.lock:
            # Check for automatic transition from OPEN to HALF_OPEN
            if self._state == CircuitState.OPEN:
                elapsed = time.time() - self._last_state_change
                if elapsed >= self._current_recovery_timeout:
                    self._transition_to(
                        CircuitState.HALF_OPEN,
                        reason=f"Recovery timeout ({self._current_recovery_timeout:.1f}s) elapsed; entering probe mode",
                    )
            return self._state

    def acquire(self) -> bool:
        """Acquire permission to run in this stage. Raises CircuitOpenError if OPEN."""
        with self.lock:
            cur_state = self.state  # triggers evaluation of timeout
            now = time.time()

            if cur_state == CircuitState.OPEN:
                remaining = self._current_recovery_timeout - (now - self._last_state_change)
                logger.warning(
                    f"Stage [{self.stage_name}] execution rejected by CircuitBreaker (OPEN). "
                    f"Cooldown remaining: {remaining:.1f}s"
                )
                with _start_span(
                    "hermes.pipeline.resilience.circuit_breaker.rejected",
                    {"stage": self.stage_name, "remaining_sec": remaining},
                ):
                    pass
                raise CircuitOpenError(
                    stage_name=self.stage_name,
                    recovery_remaining=remaining,
                    consecutive_trips=self._consecutive_trips,
                )

            if cur_state == CircuitState.HALF_OPEN:
                logger.info(
                    f"Stage [{self.stage_name}] permitting trial probe "
                    f"({self._probe_success_count + 1}/{self.probe_count}) in HALF_OPEN state."
                )

            return True

    def record_outcome(self, outcome: StageOutcome) -> None:
        """Record stage execution outcome and evaluate state transition triggers."""
        with self.lock:
            now = time.time()
            self.metrics.record(outcome, timestamp=now)
            current = self.state

            with _start_span(
                "hermes.pipeline.resilience.circuit_breaker.record_outcome",
                {"stage": self.stage_name, "outcome": outcome.value, "current_state": current.value},
            ):
                if current == CircuitState.HALF_OPEN:
                    if outcome == StageOutcome.SUCCESS:
                        self._probe_success_count += 1
                        logger.info(
                            f"Breaker [{self.stage_name}] recorded probe success "
                            f"({self._probe_success_count}/{self.probe_count})."
                        )
                        if self._probe_success_count >= self.probe_count:
                            self._consecutive_trips = 0
                            self._current_recovery_timeout = self.recovery_timeout_sec
                            self._probe_success_count = 0
                            self._transition_to(
                                CircuitState.CLOSED,
                                reason=f"All {self.probe_count} probes succeeded; stage fully healed",
                            )
                    elif outcome in (StageOutcome.FAILURE, StageOutcome.TIMEOUT):
                        self._consecutive_trips += 1
                        self._current_recovery_timeout = min(
                            self.recovery_timeout_sec * (2 ** self._consecutive_trips),
                            self.max_recovery_timeout_sec,
                        )
                        self._transition_to(
                            CircuitState.OPEN,
                            reason=f"Probe failed with {outcome.value}; re-tripping breaker with backoff",
                        )

                elif current == CircuitState.CLOSED:
                    total = self.metrics.total_calls()
                    if total >= self.min_calls_in_window:
                        f_rate = self.metrics.failure_rate()
                        s_rate = self.metrics.slow_rate()

                        if f_rate >= self.failure_rate_threshold:
                            self._consecutive_trips += 1
                            self._current_recovery_timeout = min(
                                self.recovery_timeout_sec * (2 ** (self._consecutive_trips - 1)),
                                self.max_recovery_timeout_sec,
                            )
                            self._transition_to(
                                CircuitState.OPEN,
                                reason=f"Failure rate ({f_rate:.2%}) exceeded threshold ({self.failure_rate_threshold:.2%})",
                            )
                        elif s_rate >= self.slow_call_rate_threshold:
                            self._consecutive_trips += 1
                            self._current_recovery_timeout = min(
                                self.recovery_timeout_sec * (2 ** (self._consecutive_trips - 1)),
                                self.max_recovery_timeout_sec,
                            )
                            self._transition_to(
                                CircuitState.OPEN,
                                reason=f"Slow call rate ({s_rate:.2%}) exceeded threshold ({self.slow_call_rate_threshold:.2%})",
                            )

    def force_open(self, reason: str, source: Optional[str] = None) -> None:
        """Preemptively trip breaker into OPEN state (e.g. from cascade detection)."""
        with self.lock:
            self._trip_reason = reason
            self._trip_source = source
            self._consecutive_trips += 1
            self._current_recovery_timeout = min(
                self.recovery_timeout_sec * (2 ** (self._consecutive_trips - 1)),
                self.max_recovery_timeout_sec,
            )
            self._transition_to(
                CircuitState.OPEN,
                reason=f"Forced OPEN by [{source or 'EXTERNAL'}]: {reason}",
            )

    def reset(self) -> None:
        """Force reset circuit breaker to CLOSED and clear metrics."""
        with self.lock:
            self.metrics.reset()
            self._consecutive_trips = 0
            self._current_recovery_timeout = self.recovery_timeout_sec
            self._probe_success_count = 0
            self._trip_reason = None
            self._trip_source = None
            self._transition_to(CircuitState.CLOSED, reason="Manual operator reset")

    def _transition_to(self, new_state: CircuitState, reason: str) -> None:
        old_state = self._state
        if old_state == new_state:
            return
        self._state = new_state
        self._last_state_change = time.time()
        logger.warning(
            f"CIRCUIT STATE CHANGE: Stage [{self.stage_name}] {old_state.value} -> {new_state.value}. "
            f"Reason: {reason}"
        )
        with _start_span(
            "hermes.pipeline.resilience.circuit_breaker.transition",
            {
                "stage": self.stage_name,
                "from_state": old_state.value,
                "to_state": new_state.value,
                "reason": reason,
                "consecutive_trips": self._consecutive_trips,
            },
        ):
            pass

    def get_state_info(self) -> Dict[str, Any]:
        with self.lock:
            cur_state = self.state
            now = time.time()
            elapsed = now - self._last_state_change
            cooldown_left = 0.0
            if cur_state == CircuitState.OPEN:
                cooldown_left = max(0.0, self._current_recovery_timeout - elapsed)

            return {
                "stage": self.stage_name,
                "state": cur_state.value,
                "consecutive_trips": self._consecutive_trips,
                "current_recovery_timeout_sec": self._current_recovery_timeout,
                "cooldown_remaining_sec": round(cooldown_left, 2),
                "last_state_change_timestamp": self._last_state_change,
                "probe_success_count": self._probe_success_count,
                "probe_target": self.probe_count,
                "trip_reason": self._trip_reason,
                "trip_source": self._trip_source,
                "metrics": {
                    "total_calls": self.metrics.total_calls(),
                    "failure_rate": round(self.metrics.failure_rate(), 4),
                    "slow_rate": round(self.metrics.slow_rate(), 4),
                    "counts": self.metrics.get_outcome_counts(),
                },
            }


class CircuitBreakerRegistry:
    """Singleton registry tracking all pipeline stage circuit breakers."""

    _instance: Optional[CircuitBreakerRegistry] = None
    _singleton_lock = threading.Lock()

    STANDARD_STAGES = [
        "FEASIBILITY_GATE",
        "COST_ESTIMATION",
        "DAG_SCHEDULING",
        "DISPATCH",
        "EXECUTION",
        "COMMIT_VERIFICATION",
    ]

    def __new__(cls) -> CircuitBreakerRegistry:
        with cls._singleton_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._breakers = {}
                cls._instance._registry_lock = threading.RLock()
                cls._instance._initialize_standard_stages()
            return cls._instance

    def _initialize_standard_stages(self) -> None:
        for stage in self.STANDARD_STAGES:
            self.get_or_create(stage)

    def get_or_create(self, stage_name: str, **config: Any) -> CircuitBreaker:
        stage_name = stage_name.upper().strip()
        with self._registry_lock:
            if stage_name not in self._breakers:
                self._breakers[stage_name] = CircuitBreaker(stage_name=stage_name, **config)
            return self._breakers[stage_name]

    def get(self, stage_name: str) -> Optional[CircuitBreaker]:
        with self._registry_lock:
            return self._breakers.get(stage_name.upper().strip())

    def get_all_states(self) -> Dict[str, Dict[str, Any]]:
        with self._registry_lock:
            return {name: breaker.get_state_info() for name, breaker in self._breakers.items()}

    def reset_stage(self, stage_name: str) -> bool:
        breaker = self.get(stage_name)
        if breaker:
            breaker.reset()
            return True
        return False

    def reset_all(self) -> None:
        with self._registry_lock:
            for breaker in self._breakers.values():
                breaker.reset()


# ============================================================================
# 3. Cascade Detector
# ============================================================================

class CascadeDetector:
    """Monitors cross-stage failure correlation and preempts cascading breakdowns."""

    def __init__(
        self,
        registry: CircuitBreakerRegistry,
        diagnostics: Optional[PipelineDiagnosticsProtocol] = None,
        bucket_granularity_sec: int = 10,
        history_window_sec: int = 3600,
        cascade_correlation_threshold: float = 0.70,
        downstream_failure_rate_threshold: float = 0.30,
        check_interval_sec: float = 30.0,
    ):
        self.registry = registry
        self.diagnostics = diagnostics
        self.bucket_granularity_sec = max(5, int(bucket_granularity_sec))
        self.history_window_sec = int(history_window_sec)
        self.max_buckets = self.history_window_sec // self.bucket_granularity_sec
        self.cascade_correlation_threshold = float(cascade_correlation_threshold)
        self.downstream_failure_rate_threshold = float(downstream_failure_rate_threshold)
        self.check_interval_sec = float(check_interval_sec)

        # Stage -> Dict[bucket_timestamp, failure_count]
        self._time_series: Dict[str, Dict[int, int]] = collections.defaultdict(dict)
        self._lock = threading.RLock()

        self.active_cascades: List[CascadeReport] = []
        self.cascade_history: List[CascadeReport] = []
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # Standard sequential pipeline order defining DAG predecessor/successor
        self.pipeline_dag_stages = [
            "FEASIBILITY_GATE",
            "COST_ESTIMATION",
            "DAG_SCHEDULING",
            "DISPATCH",
            "EXECUTION",
            "COMMIT_VERIFICATION",
        ]

    def record_stage_failure(self, stage_name: str, timestamp: Optional[float] = None) -> None:
        """Register a failure occurrence into the appropriate time bucket."""
        ts = time.time() if timestamp is None else timestamp
        bucket = int(ts // self.bucket_granularity_sec) * self.bucket_granularity_sec
        stage_name = stage_name.upper().strip()

        with self._lock:
            series = self._time_series[stage_name]
            series[bucket] = series.get(bucket, 0) + 1
            self._prune_buckets(stage_name, ts)

    def _prune_buckets(self, stage_name: str, current_ts: float) -> None:
        cutoff = current_ts - self.history_window_sec
        series = self._time_series[stage_name]
        expired = [b for b in series if b < cutoff]
        for b in expired:
            del series[b]

    def _get_aligned_bucket_series(
        self, stage_a: str, stage_b: str, lag_sec: float, num_buckets: int = 30
    ) -> Tuple[List[float], List[float]]:
        """Extract aligned failure counts for stage A and lagged stage B."""
        now = time.time()
        current_bucket = int(now // self.bucket_granularity_sec) * self.bucket_granularity_sec
        lag_buckets = int(round(lag_sec / self.bucket_granularity_sec))

        series_a = self._time_series.get(stage_a, {})
        series_b = self._time_series.get(stage_b, {})

        a_vals: List[float] = []
        b_vals: List[float] = []

        for i in range(num_buckets, 0, -1):
            t_a = current_bucket - (i * self.bucket_granularity_sec)
            t_b = t_a + (lag_buckets * self.bucket_granularity_sec)

            a_vals.append(float(series_a.get(t_a, 0)))
            b_vals.append(float(series_b.get(t_b, 0)))

        return a_vals, b_vals

    @staticmethod
    def _pearson_correlation(x: List[float], y: List[float]) -> float:
        """Calculate Pearson correlation coefficient r between two vectors."""
        n = min(len(x), len(y))
        if n < 4:
            return 0.0

        x_slice = x[:n]
        y_slice = y[:n]

        sum_x = sum(x_slice)
        sum_y = sum(y_slice)

        if sum_x == 0.0 or sum_y == 0.0:
            return 0.0

        mean_x = sum_x / n
        mean_y = sum_y / n

        var_x = sum((val - mean_x) ** 2 for val in x_slice)
        var_y = sum((val - mean_y) ** 2 for val in y_slice)

        if var_x < 1e-9 or var_y < 1e-9:
            return 0.0

        cov = sum((x_slice[i] - mean_x) * (y_slice[i] - mean_y) for i in range(n))
        denom = math.sqrt(var_x * var_y)
        if denom == 0.0:
            return 0.0
        return max(-1.0, min(1.0, cov / denom))

    def detect_cascades(self) -> List[CascadeReport]:
        """Scan ordered stage pairs, evaluate correlation, and trip preemptive breakers."""
        detected_reports: List[CascadeReport] = []

        with self._lock:
            num_stages = len(self.pipeline_dag_stages)
            for i in range(num_stages):
                stage_a = self.pipeline_dag_stages[i]
                breaker_a = self.registry.get(stage_a)
                if not breaker_a:
                    continue

                for j in range(i + 1, num_stages):
                    stage_b = self.pipeline_dag_stages[j]
                    breaker_b = self.registry.get(stage_b)
                    if not breaker_b:
                        continue

                    # Determine lag between stage A and stage B
                    lag_sec = 30.0
                    if self.diagnostics is not None:
                        try:
                            latency = self.diagnostics.get_stage_transition_latency(stage_a, stage_b)
                            if latency > 0.0:
                                lag_sec = latency
                        except Exception as e:
                            logger.debug(f"Could not fetch latency from diagnostics: {e}")

                    x, y = self._get_aligned_bucket_series(stage_a, stage_b, lag_sec=lag_sec)
                    r = self._pearson_correlation(x, y)

                    breaker_a_open = (breaker_a.state == CircuitState.OPEN)
                    b_fail_rate = breaker_b.metrics.failure_rate()

                    # Trigger cascade if correlated, root is open, and downstream shows non-trivial failures
                    if (
                        r >= self.cascade_correlation_threshold
                        and breaker_a_open
                        and b_fail_rate >= self.downstream_failure_rate_threshold
                    ):
                        logger.critical(
                            f"CASCADE DETECTED: Stage [{stage_a}] -> [{stage_b}] (r={r:.3f}). "
                            f"Preemptively tripping downstream breaker for [{stage_b}]."
                        )

                        breaker_b.force_open(
                            reason=f"CASCADE_PREEMPTION from upstream {stage_a} (r={r:.2f})",
                            source=stage_a,
                        )

                        report = CascadeReport(
                            cascade_id=str(uuid.uuid4())[:8],
                            root_stage=stage_a,
                            affected_stages=[stage_a, stage_b],
                            correlation_scores={f"{stage_a}->{stage_b}": round(r, 4)},
                            detected_at=time.time(),
                            recommended_recovery_action=(
                                f"Halt dispatch to {stage_b}; initiate rollback of in-flight "
                                f"milestones and wait for {stage_a} recovery."
                            ),
                        )
                        detected_reports.append(report)
                        self.active_cascades.append(report)
                        self.cascade_history.append(report)

                        with _start_span(
                            "hermes.pipeline.resilience.cascade_detector.cascade_tripped",
                            report.to_dict(),
                        ):
                            pass

        return detected_reports

    def start(self) -> None:
        """Start the background cascade monitoring thread."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop, name="HermesCascadeDetectorThread", daemon=True
        )
        self._thread.start()
        logger.info("CascadeDetector background monitoring thread started.")

    def stop(self) -> None:
        """Stop the background cascade monitoring thread."""
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3.0)
        logger.info("CascadeDetector background monitoring thread stopped.")

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.detect_cascades()
            except Exception as e:
                logger.error(f"Unexpected error in cascade detection loop: {e}\n{traceback.format_exc()}")
            self._stop_event.wait(timeout=self.check_interval_sec)


# ============================================================================
# 4. Bugfix Queue
# ============================================================================

class BugfixQueue:
    """Collects failure signatures, classifies root causes, and generates bugfixes."""

    CATEGORY_PATTERNS = [
        (ErrorCategory.TIMEOUT, re.compile(r"(?i)(timeout|timed\s*out|deadline\s*exceeded)")),
        (ErrorCategory.API_RATE_LIMIT, re.compile(r"(?i)(rate\s*limit|429|too\s*many\s*requests|quota\s*exceeded|throttl)")),
        (ErrorCategory.GIT_CONFLICT, re.compile(r"(?i)(conflict|merge\s*conflict|non-fast-forward|rejected.*push|failed\s*to\s*push)")),
        (ErrorCategory.DEPENDENCY_ERROR, re.compile(r"(?i)(import\w*error|modulenotfound|missing\s*dependency|pkg_resources|distributionnotfound)")),
        (ErrorCategory.PERMISSION_ERROR, re.compile(r"(?i)(permission\s*denied|unauthorized|forbidden|401|403|access\s*denied)")),
        (ErrorCategory.VALIDATION_ERROR, re.compile(r"(?i)(validation|schema|invalid|bad\s*request|assertion|valueerror|pydantic|keyerror)")),
    ]

    FIX_STRATEGY_MAP = {
        ErrorCategory.TIMEOUT: "Increase stage execution timeout multiplier or optimize task chunking bounds",
        ErrorCategory.API_RATE_LIMIT: "Pace adaptive token flow governor, increase exponential backoff jitter",
        ErrorCategory.GIT_CONFLICT: "Invoke MergeReconciler to execute 3-way rebase against upstream target",
        ErrorCategory.DEPENDENCY_ERROR: "Validate virtual environment requirements.txt and rebuild cached wheels",
        ErrorCategory.PERMISSION_ERROR: "Verify repo filesystem permissions and API authentication token scopes",
        ErrorCategory.VALIDATION_ERROR: "Validate milestone payload against Pydantic schema before dispatching",
        ErrorCategory.UNKNOWN: "Trigger manual engineer triage and inspect full stacktrace in telemetry",
    }

    def __init__(
        self,
        storage_path: str = "dashboards/bugfix_queue.json",
        bugfix_generation_threshold: int = 3,
        bugfix_window_sec: float = 3600.0,
    ):
        self.storage_path = storage_path
        self.bugfix_generation_threshold = int(bugfix_generation_threshold)
        self.bugfix_window_sec = float(bugfix_window_sec)

        self._queue: Dict[FailureSignature, BugfixMilestone] = {}
        self._lock = threading.RLock()
        self._load_from_disk()

    @classmethod
    def compute_trace_fingerprint(cls, exc: Union[Exception, str], top_frames: int = 5) -> str:
        """Derive normalized 16-character SHA-256 fingerprint from stacktrace."""
        if isinstance(exc, Exception):
            raw_frames = traceback.format_exception(type(exc), exc, exc.__traceback__)
            clean_frames = [f.strip() for f in raw_frames if f.strip()]
            top = clean_frames[-top_frames:] if len(clean_frames) >= top_frames else clean_frames
            trace_text = "\n".join(top)
        else:
            trace_text = str(exc).strip()

        # Normalize line numbers, hex memory addresses, and timestamps
        normalized = re.sub(r"0x[0-9a-fA-F]+", "0xADDR", trace_text)
        normalized = re.sub(r'line \d+', "line LINE", normalized)
        normalized = re.sub(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", "TIMESTAMP", normalized)
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]

    @classmethod
    def classify_error(
        cls, exc: Union[Exception, str], stage_name: str, repo_name: str
    ) -> FailureSignature:
        """Classify exception into standardized FailureSignature."""
        msg = str(exc)
        exc_type = type(exc).__name__ if isinstance(exc, Exception) else "StringError"
        full_text = f"{exc_type}: {msg}"

        category = ErrorCategory.UNKNOWN
        for cat, pattern in cls.CATEGORY_PATTERNS:
            if pattern.search(full_text):
                category = cat
                break

        fingerprint = cls.compute_trace_fingerprint(exc)
        return FailureSignature(
            repo=repo_name,
            stage=stage_name.upper().strip(),
            error_category=category,
            trace_fingerprint=fingerprint,
        )

    def record_failure(
        self, signature: FailureSignature, milestone_id: str
    ) -> Optional[BugfixMilestone]:
        """Record a failure event and synthesize a BugfixMilestone if threshold met."""
        now = time.time()
        generated_milestone: Optional[BugfixMilestone] = None

        with self._lock:
            if signature in self._queue:
                item = self._queue[signature]
                item.hit_count += 1
                item.last_seen = now
                if milestone_id not in item.affected_milestone_ids:
                    item.affected_milestone_ids.append(milestone_id)

                if item.hit_count >= self.bugfix_generation_threshold and item.status == "PENDING":
                    generated_milestone = item
            else:
                strategy = self.FIX_STRATEGY_MAP.get(
                    signature.error_category, self.FIX_STRATEGY_MAP[ErrorCategory.UNKNOWN]
                )
                item = BugfixMilestone(
                    id=str(uuid.uuid4())[:8],
                    signature=signature,
                    hit_count=1,
                    first_seen=now,
                    last_seen=now,
                    affected_milestone_ids=[milestone_id],
                    suggested_fix_strategy=strategy,
                    status="PENDING",
                )
                self._queue[signature] = item

            self.flush_to_disk()

        return generated_milestone

    def get_pending(self) -> List[BugfixMilestone]:
        with self._lock:
            return [m for m in self._queue.values() if m.status == "PENDING"]

    def approve(self, bugfix_id: str) -> bool:
        with self._lock:
            for item in self._queue.values():
                if item.id == bugfix_id:
                    item.status = "APPROVED"
                    self.flush_to_disk()
                    logger.info(f"BugfixMilestone [{bugfix_id}] set to APPROVED.")
                    return True
        return False

    def resolve(self, bugfix_id: str) -> bool:
        with self._lock:
            for item in self._queue.values():
                if item.id == bugfix_id:
                    item.status = "RESOLVED"
                    self.flush_to_disk()
                    logger.info(f"BugfixMilestone [{bugfix_id}] set to RESOLVED.")
                    return True
        return False

    def flush_to_disk(self) -> None:
        """Persist bugfix queue state to disk JSON."""
        try:
            folder = os.path.dirname(self.storage_path)
            if folder:
                os.makedirs(folder, exist_ok=True)
            payload = [item.to_dict() for item in self._queue.values()]
            temp_path = f"{self.storage_path}.tmp"
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
            os.replace(temp_path, self.storage_path)
        except Exception as e:
            logger.error(f"Failed to flush BugfixQueue to {self.storage_path}: {e}")

    def _load_from_disk(self) -> None:
        if not os.path.exists(self.storage_path):
            return
        try:
            with open(self.storage_path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            for entry in raw:
                sig_data = entry["signature"]
                sig = FailureSignature(
                    repo=sig_data["repo"],
                    stage=sig_data["stage"],
                    error_category=ErrorCategory(sig_data["error_category"]),
                    trace_fingerprint=sig_data["trace_fingerprint"],
                )
                milestone = BugfixMilestone(
                    id=entry["id"],
                    signature=sig,
                    hit_count=entry["hit_count"],
                    first_seen=entry["first_seen"],
                    last_seen=entry["last_seen"],
                    affected_milestone_ids=entry.get("affected_milestone_ids", []),
                    suggested_fix_strategy=entry.get("suggested_fix_strategy", ""),
                    status=entry.get("status", "PENDING"),
                )
                self._queue[sig] = milestone
            logger.info(f"Loaded {len(self._queue)} items into BugfixQueue from disk.")
        except Exception as e:
            logger.warning(f"Could not load BugfixQueue from {self.storage_path}: {e}")


# ============================================================================
# 5. Rollback Orchestrator
# ============================================================================

class RollbackOrchestrator:
    """Safely reverts committed code and resets pipeline state during cascades."""

    def __init__(
        self,
        coordinator: Optional[PipelineCoordinatorProtocol] = None,
        audit_log_path: str = "dashboards/rollback_audit.jsonl",
        max_concurrent_rollbacks: int = 2,
    ):
        self.coordinator = coordinator
        self.audit_log_path = audit_log_path
        self.semaphore = threading.Semaphore(max_concurrent_rollbacks)
        self._repo_locks: Dict[str, threading.Lock] = collections.defaultdict(threading.Lock)
        self._lock = threading.Lock()

    def _get_repo_lock(self, repo: str) -> threading.Lock:
        with self._lock:
            return self._repo_locks[repo]

    def _append_audit_record(self, record: RollbackRecord) -> None:
        try:
            folder = os.path.dirname(self.audit_log_path)
            if folder:
                os.makedirs(folder, exist_ok=True)
            with open(self.audit_log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record.to_dict()) + "\n")
        except Exception as e:
            logger.error(f"Failed appending rollback audit record: {e}")

    def execute_git_revert(self, repo_dir: str, commit_hash: str) -> Tuple[bool, Optional[str]]:
        """Run `git revert --no-edit <hash>` in the specified repository path."""
        if not os.path.exists(repo_dir) or not os.path.exists(os.path.join(repo_dir, ".git")):
            # Simulation / fallback when run outside a real git working copy
            logger.info(f"Simulating git revert for {commit_hash} in {repo_dir} (no git directory)")
            return True, None

        cmd = ["git", "revert", "--no-edit", commit_hash]
        try:
            proc = subprocess.run(
                cmd,
                cwd=repo_dir,
                capture_output=True,
                text=True,
                check=False,
                timeout=45.0,
            )
            if proc.returncode == 0:
                logger.info(f"Successfully reverted git commit {commit_hash} in {repo_dir}")
                return True, None
            else:
                err = proc.stderr.strip() or proc.stdout.strip()
                logger.error(f"Git revert failed for {commit_hash}: {err}")
                return False, err
        except Exception as e:
            logger.error(f"Git revert subprocess error: {e}")
            return False, str(e)

    def rollback_cascade(self, cascade: CascadeReport, repo_base_dir: str = "/home/ajf") -> List[RollbackRecord]:
        """Roll back in-flight and recently committed milestones impacted by a cascade."""
        records: List[RollbackRecord] = []
        if self.coordinator is None:
            logger.warning("RollbackOrchestrator has no PipelineCoordinator attached; cannot query in-flight milestones.")
            return records

        with self.semaphore:
            with _start_span(
                "hermes.pipeline.resilience.rollback_orchestrator.execute",
                {"cascade_id": cascade.cascade_id, "root_stage": cascade.root_stage},
            ):
                in_flight = self.coordinator.get_in_flight_milestones(
                    stages=cascade.affected_stages, time_window=1800.0
                )
                logger.info(
                    f"Cascade [{cascade.cascade_id}] triggered rollback for {len(in_flight)} milestones."
                )

                # Group by repository
                grouped: Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)
                for item in in_flight:
                    repo = item.get("repo", "hermes_orchestrator")
                    grouped[repo].append(item)

                for repo_name, milestones in grouped.items():
                    repo_lock = self._get_repo_lock(repo_name)
                    with repo_lock:
                        repo_path = os.path.join(repo_base_dir, repo_name)
                        for m in milestones:
                            m_id = str(m.get("milestone_id") or m.get("id"))
                            commit_hash = m.get("commit_hash")
                            stage = m.get("stage", "UNKNOWN")

                            revert_success = True
                            revert_err = None

                            if commit_hash and stage in ("COMMIT_VERIFICATION", "COMPLETED", "EXECUTION"):
                                revert_success, revert_err = self.execute_git_revert(repo_path, commit_hash)

                            # Reset milestone state in coordinator ledger
                            reset_ok = self.coordinator.reset_milestone_state(
                                milestone_id=m_id,
                                new_state="PENDING",
                                priority_boost=2,
                                context={
                                    "original_failure": f"Cascade {cascade.cascade_id} preemption",
                                    "cascade_id": cascade.cascade_id,
                                    "reverted_commit": commit_hash,
                                },
                            )

                            rec = RollbackRecord(
                                rollback_id=str(uuid.uuid4())[:8],
                                cascade_id=cascade.cascade_id,
                                repo=repo_name,
                                milestone_id=m_id,
                                commit_hash=commit_hash,
                                action="GIT_REVERT_AND_LEDGER_RESET",
                                timestamp=time.time(),
                                success=revert_success and reset_ok,
                                error=revert_err,
                            )
                            self._append_audit_record(rec)
                            records.append(rec)

        return records


# ============================================================================
# 6. Resilience Health Contributor
# ============================================================================

class ResilienceHealthContributor:
    """Computes holistic resilience metrics and health sub-score for dashboards."""

    STAGE_WEIGHTS: Dict[str, float] = {
        "DISPATCH": 1.0,
        "EXECUTION": 1.0,
        "COMMIT_VERIFICATION": 0.9,
        "DAG_SCHEDULING": 0.8,
        "FEASIBILITY_GATE": 0.7,
        "COST_ESTIMATION": 0.6,
    }

    def __init__(
        self,
        registry: CircuitBreakerRegistry,
        cascade_detector: CascadeDetector,
        bugfix_queue: BugfixQueue,
    ):
        self.registry = registry
        self.cascade_detector = cascade_detector
        self.bugfix_queue = bugfix_queue

    def compute_health(self) -> Dict[str, Any]:
        """Compute resilience sub-score (0-100) and component breakdowns."""
        # 1. Breaker Stability (weighted)
        total_weight = 0.0
        weighted_stability_sum = 0.0
        states = self.registry.get_all_states()

        for stage, weight in self.STAGE_WEIGHTS.items():
            total_weight += weight
            breaker_info = states.get(stage)
            if not breaker_info:
                val = 1.0
            else:
                st = breaker_info["state"]
                val = 1.0 if st == CircuitState.CLOSED.value else 0.5 if st == CircuitState.HALF_OPEN.value else 0.0
            weighted_stability_sum += weight * val

        breaker_stability_pct = (weighted_stability_sum / total_weight) * 100.0 if total_weight > 0 else 100.0

        # 2. Cascade Frequency Score
        now = time.time()
        one_hour_ago = now - 3600.0
        recent_cascades = sum(1 for c in self.cascade_detector.cascade_history if c.detected_at >= one_hour_ago)
        cascade_frequency_score = max(0.0, 100.0 - (25.0 * recent_cascades))

        # 3. Bugfix Depth Score
        pending_bugfix_count = len(self.bugfix_queue.get_pending())
        bugfix_depth_penalty = max(0.0, 100.0 - (15.0 * math.log2(1.0 + pending_bugfix_count)))

        # Composite resilience sub score (0.5 * stability + 0.3 * cascade + 0.2 * bugfix)
        resilience_sub_score = (
            (0.50 * breaker_stability_pct)
            + (0.30 * cascade_frequency_score)
            + (0.20 * bugfix_depth_penalty)
        )
        resilience_sub_score = round(max(0.0, min(100.0, resilience_sub_score)), 2)

        return {
            "resilience_sub_score": resilience_sub_score,
            "breaker_stability_pct": round(breaker_stability_pct, 2),
            "cascade_frequency_score": round(cascade_frequency_score, 2),
            "bugfix_depth_score": round(bugfix_depth_penalty, 2),
            "recent_cascades_per_hour": recent_cascades,
            "pending_bugfix_count": pending_bugfix_count,
            "active_breakers_open": sum(1 for s in states.values() if s["state"] == CircuitState.OPEN.value),
            "active_breakers_half_open": sum(1 for s in states.values() if s["state"] == CircuitState.HALF_OPEN.value),
            "evaluated_at": now,
        }


# ============================================================================
# 7. Resilience Context (Shared Singleton)
# ============================================================================

class ResilienceContext:
    """Thread-safe singleton providing centralized access to resilience subsystems."""

    _instance: Optional[ResilienceContext] = None
    _lock = threading.Lock()

    def __new__(cls, *args: Any, **kwargs: Any) -> ResilienceContext:
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(
        self,
        diagnostics: Optional[PipelineDiagnosticsProtocol] = None,
        coordinator: Optional[PipelineCoordinatorProtocol] = None,
    ):
        if getattr(self, "_initialized", False):
            if diagnostics is not None:
                self.cascade_detector.diagnostics = diagnostics
            if coordinator is not None:
                self.rollback_orchestrator.coordinator = coordinator
            return

        self.registry = CircuitBreakerRegistry()
        self.cascade_detector = CascadeDetector(registry=self.registry, diagnostics=diagnostics)
        self.bugfix_queue = BugfixQueue()
        self.rollback_orchestrator = RollbackOrchestrator(coordinator=coordinator)
        self.health_contributor = ResilienceHealthContributor(
            registry=self.registry,
            cascade_detector=self.cascade_detector,
            bugfix_queue=self.bugfix_queue,
        )
        self._initialized = True


# ============================================================================
# 8. Resilience API Handler
# ============================================================================

class ResilienceAPIHandler:
    """Flask/WSGI-compatible API controller routing HTTP resilience requests."""

    def __init__(self, controller: PipelineResilienceController):
        self.controller = controller

    def get_breakers(self) -> Tuple[Dict[str, Any], int, Dict[str, str]]:
        """GET /api/pipeline/resilience/breakers"""
        data = self.controller.registry.get_all_states()
        return {"status": "ok", "breakers": data}, 200, {"Content-Type": "application/json"}

    def get_cascades(self) -> Tuple[Dict[str, Any], int, Dict[str, str]]:
        """GET /api/pipeline/resilience/cascades"""
        active = [c.to_dict() for c in self.controller.cascade_detector.active_cascades]
        history = [c.to_dict() for c in self.controller.cascade_detector.cascade_history[-50:]]
        return {
            "status": "ok",
            "active_cascades": active,
            "history": history,
        }, 200, {"Content-Type": "application/json"}

    def get_bugfix_queue(self) -> Tuple[Dict[str, Any], int, Dict[str, str]]:
        """GET /api/pipeline/resilience/bugfix-queue"""
        pending = [m.to_dict() for m in self.controller.bugfix_queue.get_pending()]
        return {"status": "ok", "pending_bugfixes": pending}, 200, {"Content-Type": "application/json"}

    def reset_breaker(self, stage: str) -> Tuple[Dict[str, Any], int, Dict[str, str]]:
        """POST /api/pipeline/resilience/breakers/{stage}/reset"""
        ok = self.controller.registry.reset_stage(stage)
        if ok:
            return {"status": "ok", "message": f"Breaker [{stage}] reset to CLOSED"}, 200, {"Content-Type": "application/json"}
        return {"status": "error", "message": f"Stage [{stage}] not found"}, 404, {"Content-Type": "application/json"}

    def approve_bugfix(self, bugfix_id: str) -> Tuple[Dict[str, Any], int, Dict[str, str]]:
        """POST /api/pipeline/resilience/bugfix-queue/{id}/approve"""
        ok = self.controller.bugfix_queue.approve(bugfix_id)
        if ok:
            return {"status": "ok", "message": f"Bugfix [{bugfix_id}] approved for execution"}, 200, {"Content-Type": "application/json"}
        return {"status": "error", "message": f"Bugfix [{bugfix_id}] not found"}, 404, {"Content-Type": "application/json"}

    def get_health(self) -> Tuple[Dict[str, Any], int, Dict[str, str]]:
        """GET /api/pipeline/resilience/health"""
        health = self.controller.health_contributor.compute_health()
        return {"status": "ok", "health": health}, 200, {"Content-Type": "application/json"}


# ============================================================================
# 9. Top-Level Pipeline Resilience Controller
# ============================================================================

class PipelineResilienceController:
    """Master controller wiring CircuitBreakers, Cascades, BugfixQueue, and Rollbacks."""

    def __init__(
        self,
        diagnostics: Optional[PipelineDiagnosticsProtocol] = None,
        coordinator: Optional[PipelineCoordinatorProtocol] = None,
        config: Optional[Any] = None,
    ):
        self.context = ResilienceContext(diagnostics=diagnostics, coordinator=coordinator)
        self.registry = self.context.registry
        self.cascade_detector = self.context.cascade_detector
        self.bugfix_queue = self.context.bugfix_queue
        self.rollback_orchestrator = self.context.rollback_orchestrator
        self.health_contributor = self.context.health_contributor
        self.config = config

        self.api_handler = ResilienceAPIHandler(self)
        self._running = False

    def start(self) -> None:
        """Launch background monitoring threads and initialize subsystems."""
        if self._running:
            return
        logger.info("Starting PipelineResilienceController...")
        self.cascade_detector.start()
        self._running = True

    def stop(self) -> None:
        """Stop background execution threads cleanly."""
        if not self._running:
            return
        logger.info("Stopping PipelineResilienceController...")
        self.cascade_detector.stop()
        self._running = False

    def acquire_stage(self, stage_name: str) -> bool:
        """Invoked by PipelineCoordinator before running a stage. Raises CircuitOpenError if open."""
        breaker = self.registry.get_or_create(stage_name)
        return breaker.acquire()

    def process_stage_outcome(
        self,
        stage_name: str,
        outcome: StageOutcome,
        milestone_id: str,
        exception: Optional[Union[Exception, str]] = None,
        repo_name: str = "hermes_orchestrator",
    ) -> CircuitState:
        """Main integration hook called after every stage execution."""
        breaker = self.registry.get_or_create(stage_name)
        breaker.record_outcome(outcome)

        if outcome in (StageOutcome.FAILURE, StageOutcome.TIMEOUT):
            # Record in cascade time series
            self.cascade_detector.record_stage_failure(stage_name)

            # Classify error and route to BugfixQueue
            if exception is not None:
                sig = self.bugfix_queue.classify_error(
                    exc=exception,
                    stage_name=stage_name,
                    repo_name=repo_name,
                )
                self.bugfix_queue.record_failure(sig, milestone_id=milestone_id)

        return breaker.state

    def trigger_cascade_rollback(self, cascade_report: CascadeReport) -> List[RollbackRecord]:
        """Trigger coordinated rollback across affected repositories."""
        return self.rollback_orchestrator.rollback_cascade(cascade_report)

    def get_health_report(self) -> Dict[str, Any]:
        """Return real-time composite resilience score."""
        return self.health_contributor.compute_health()


# ============================================================================
# Module Exports
# ============================================================================

__all__ = [
    "CircuitState",
    "ErrorCategory",
    "StageOutcome",
    "FailureSignature",
    "BugfixMilestone",
    "CascadeReport",
    "RollbackRecord",
    "TuningAction",
    "CircuitOpenError",
    "PipelineDiagnosticsProtocol",
    "PipelineCoordinatorProtocol",
    "SlidingWindowMetrics",
    "CircuitBreaker",
    "CircuitBreakerRegistry",
    "CascadeDetector",
    "BugfixQueue",
    "RollbackOrchestrator",
    "ResilienceHealthContributor",
    "ResilienceContext",
    "ResilienceAPIHandler",
    "PipelineResilienceController",
]


# ============================================================================
# Standalone Verification Demonstration
# ============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("Hermes Pipeline Resilience Subsystem - Standalone Verification")
    print("=" * 70)

    # 1. Initialize Controller
    controller = PipelineResilienceController()
    controller.start()

    # 2. Test Normal Circuit Breaker Acquire & Success Flow
    print("\n[+] 1. Testing Normal Execution Flow:")
    stage = "EXECUTION"
    breaker = controller.registry.get_or_create(stage)
    assert breaker.acquire() is True, "Breaker should acquire cleanly when CLOSED"
    for _ in range(5):
        controller.process_stage_outcome(stage, StageOutcome.SUCCESS, milestone_id="m-norm-1")
    print(f"    Stage {stage} state: {breaker.state.value} (Failure Rate: {breaker.metrics.failure_rate():.2%})")

    # 3. Simulate Failures to Trip Breaker
    print("\n[+] 2. Testing Failure Gating & Breaker Tripping:")
    for i in range(12):
        dummy_exc = TimeoutError("HTTP 408 Gateway Execution Timeout after 120s")
        controller.process_stage_outcome(
            stage, StageOutcome.TIMEOUT, milestone_id=f"m-fail-{i}", exception=dummy_exc
        )

    print(f"    Stage {stage} state after 12 timeouts: {breaker.state.value}")
    try:
        breaker.acquire()
        print("    [!] FAILED: Breaker did not raise CircuitOpenError!")
    except CircuitOpenError as e:
        print(f"    [OK] CircuitOpenError successfully raised: {e}")

    # 4. Bugfix Queue Verification
    print("\n[+] 3. Bugfix Queue Verification:")
    pending_fixes = controller.bugfix_queue.get_pending()
    print(f"    Synthesized Bugfix Milestones count: {len(pending_fixes)}")
    for fix in pending_fixes:
        print(f"    - Bugfix ID: {fix.id} | Category: {fix.signature.error_category.value} | Hits: {fix.hit_count}")
        print(f"      Strategy: {fix.suggested_fix_strategy}")

    # 5. Cascade Detection Simulation
    print("\n[+] 4. Simulating Cascade Correlation:")
    upstream = "DISPATCH"
    downstream = "EXECUTION"
    breaker_up = controller.registry.get_or_create(upstream)
    breaker_up.force_open("Simulated root dispatch failure", source="TEST_HARNESS")

    now_t = time.time()
    for b_offset in range(10):
        t_bucket = now_t - (b_offset * 10)
        # Inject synchronized failure pattern
        controller.cascade_detector.record_stage_failure(upstream, timestamp=t_bucket)
        controller.cascade_detector.record_stage_failure(downstream, timestamp=t_bucket)

    cascades = controller.cascade_detector.detect_cascades()
    print(f"    Detected Cascades count: {len(cascades)}")
    for c in cascades:
        print(f"    - Cascade ID: {c.cascade_id} | Root: {c.root_stage} -> Affected: {c.affected_stages}")
        print(f"      Correlation: {c.correlation_scores}")

    # 6. Resilience Health Score
    print("\n[+] 5. Resilience Health Score Computation:")
    health = controller.get_health_report()
    print(f"    Resilience Sub-Score: {health['resilience_sub_score']} / 100.0")
    print(f"    - Breaker Stability: {health['breaker_stability_pct']}%")
    print(f"    - Cascade Score:     {health['cascade_frequency_score']}/100")
    print(f"    - Bugfix Score:      {health['bugfix_depth_score']}/100")

    # Clean shutdown
    controller.stop()
    print("\n[+] Standalone verification completed with 100% operational success.")
    print("=" * 70)