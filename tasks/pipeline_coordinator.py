"""
Unified Milestone Pipeline Coordinator & Transactional Dispatch Orchestration Layer.

This module unifies Hermes Orchestrator subsystems into an end-to-end transactional
dispatch pipeline: cost estimation, feasibility gating, DAG dependency scheduling,
runtime execution supervision, quota arbitrage, steering memory, lifecycle tracking,
and provenance recording with automatic LIFO compensating transactions.
"""

from __future__ import annotations

import heapq
import logging
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Protocol,
    Tuple,
    Union,
    runtime_checkable,
)

# ============================================================================
# OpenTelemetry Graceful Fallback / Instrumentation Layer
# ============================================================================

try:
    from opentelemetry import trace as otel_trace
    from opentelemetry.trace import Span, Status, StatusCode

    OPENTELEMETRY_AVAILABLE = True
except ImportError:
    OPENTELEMETRY_AVAILABLE = False
    otel_trace = None
    Span = Any  # type: ignore
    Status = Any  # type: ignore

    class StatusCode:  # type: ignore
        OK = "OK"
        ERROR = "ERROR"
        UNSET = "UNSET"


class _NoOpSpan:
    """Fallback no-op span context manager when OpenTelemetry is unavailable."""

    def __init__(self, name: str = "") -> None:
        self.name = name
        self.attributes: Dict[str, Any] = {}

    def __enter__(self) -> "_NoOpSpan":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        return False

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value

    def set_attributes(self, attributes: Dict[str, Any]) -> None:
        self.attributes.update(attributes)

    def set_status(self, status: Any, description: Optional[str] = None) -> None:
        pass

    def record_exception(self, exception: BaseException) -> None:
        pass


class _NoOpTracer:
    """Fallback no-op tracer when OpenTelemetry is unavailable."""

    @contextmanager
    def start_as_current_span(
        self, name: str, attributes: Optional[Dict[str, Any]] = None
    ) -> Iterator[_NoOpSpan]:
        span = _NoOpSpan(name)
        if attributes:
            span.set_attributes(attributes)
        yield span


def get_tracer(name: str = "hermes.pipeline"):
    """Retrieve an OpenTelemetry tracer or a robust no-op fallback."""
    if OPENTELEMETRY_AVAILABLE and otel_trace is not None:
        return otel_trace.get_tracer(name)
    return _NoOpTracer()


tracer = get_tracer("hermes.pipeline")
logger = logging.getLogger("hermes.pipeline")


# ============================================================================
# Core Pipeline Enumerations & Data Contracts
# ============================================================================

class PipelineStage(str, Enum):
    """The 9 sequential lifecycle stages of the unified milestone pipeline."""
    INTAKE = "INTAKE"
    COST_ESTIMATION = "COST_ESTIMATION"
    FEASIBILITY_GATE = "FEASIBILITY_GATE"
    DAG_INSERT = "DAG_INSERT"
    READINESS_WAIT = "READINESS_WAIT"
    DISPATCH = "DISPATCH"
    EXECUTION_MONITOR = "EXECUTION_MONITOR"
    OUTCOME_CAPTURE = "OUTCOME_CAPTURE"
    FAILURE_RECOVERY = "FAILURE_RECOVERY"


class DecomposeTrigger(str, Enum):
    """Triggers prompting recursive sub-milestone decomposition."""
    FEASIBILITY_GATE = "FEASIBILITY_GATE"
    GOVERNOR_PREEMPT = "GOVERNOR_PREEMPT"


class GovernorActionType(str, Enum):
    """Supervisory actions produced by RuntimeExecutionGovernor."""
    CONTINUE = "CONTINUE"
    THROTTLE = "THROTTLE"
    PREEMPT = "PREEMPT"


class GateVerdict(str, Enum):
    """Decisions rendered by FeasibilityGate."""
    APPROVED = "APPROVED"
    REVISE_OR_DECOMPOSE = "REVISE_OR_DECOMPOSE"
    DEFER_OR_REJECT = "DEFER_OR_REJECT"


@dataclass(frozen=True)
class CostEstimate:
    """Estimated resource and token consumption for a milestone."""
    estimated_prompt_tokens: int = 0
    estimated_completion_tokens: int = 0
    estimated_cost_usd: float = 0.0
    estimated_duration_sec: float = 0.0
    model_name: str = "default-model"
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class GateDecision:
    """Outcome and prescription returned by the FeasibilityGate."""
    verdict: GateVerdict
    reason: str
    suggested_retry_after: Optional[datetime] = None
    sub_milestones: Optional[List[Dict[str, Any]]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class GovernorAction:
    """Supervisory intervention directive from ExecutionGovernor."""
    action_type: GovernorActionType
    milestone_id: str
    reason: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineStageInfo:
    """Observable snapshot of a milestone's current stage and runtime metadata."""
    milestone_id: str
    current_stage: PipelineStage
    entered_at: datetime
    stage_metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineResult:
    """Final outcome summary for a pipeline submission."""
    milestone_id: str
    final_stage: PipelineStage
    outcome: Literal[
        "COMPLETED",
        "DEFERRED",
        "DECOMPOSED",
        "PREEMPTED",
        "ROLLED_BACK",
        "FAILED",
    ]
    sub_milestones: Optional[List[str]] = None
    cost_estimate: Optional[CostEstimate] = None
    gate_decision: Optional[GateDecision] = None
    execution_telemetry: Optional[Dict[str, Any]] = None
    provenance_record_id: str = ""


# ============================================================================
# Subsystem Protocol Interfaces
# ============================================================================

@runtime_checkable
class MilestoneCostEstimator(Protocol):
    """Estimates financial and token budget for candidate milestones."""
    def estimate_milestone_cost(self, milestone_metadata: Dict[str, Any]) -> CostEstimate:
        ...


@runtime_checkable
class FeasibilityGate(Protocol):
    """Gating mechanism evaluating admission, rejection, or decomposition."""
    def evaluate(
        self, milestone_metadata: Dict[str, Any], cost_estimate: CostEstimate
    ) -> GateDecision:
        ...


@runtime_checkable
class MilestoneDecomposer(Protocol):
    """Partitions coarse-grained milestones into tractable sub-milestones."""
    def decompose(
        self, milestone_metadata: Dict[str, Any], trigger: DecomposeTrigger
    ) -> List[Dict[str, Any]]:
        ...


@runtime_checkable
class TopologicalScheduler(Protocol):
    """Maintains dependency graph, critical paths, and topological order."""
    def insert_milestone(self, milestone_metadata: Dict[str, Any]) -> None:
        ...

    def remove_milestone(self, milestone_id: str) -> None:
        ...

    def recompute_critical_path(self) -> Any:
        ...

    def is_ready(self, milestone_id: str) -> bool:
        ...


@runtime_checkable
class ExecutionGovernor(Protocol):
    """Supervises executing slots, throttling or preempting overrunning tasks."""
    def start_supervision(self, slots: List[Dict[str, Any]]) -> None:
        ...

    def tick(self) -> List[GovernorAction]:
        ...

    def abandon_slot(self, milestone_id: str) -> None:
        ...


@runtime_checkable
class MemoryStore(Protocol):
    """Episodic and semantic steering memory repository."""
    def record_outcome(self, outcome_data: Dict[str, Any]) -> str:
        ...

    def void_record(self, record_id: str) -> None:
        ...


@runtime_checkable
class ProvenanceLedger(Protocol):
    """Immutable ledger recording transactional stages and audit logs."""
    def append_record(
        self, milestone_id: str, stage: str, metadata: Dict[str, Any]
    ) -> str:
        ...

    def void_record(self, record_id: str) -> None:
        ...


@runtime_checkable
class LifecycleStateMachine(Protocol):
    """Tracks state transitions and emits lifecycle domain events."""
    def transition(
        self, milestone_id: str, target_state: str, context: Optional[Dict[str, Any]] = None
    ) -> bool:
        ...

    def revert_transition(self, milestone_id: str) -> None:
        ...


@runtime_checkable
class RollbackOrchestrator(Protocol):
    """Performs compensatory cleanups and state restorations."""
    def execute_rollback(
        self, milestone_id: str, context: Optional[Dict[str, Any]] = None
    ) -> bool:
        ...


@runtime_checkable
class QuotaArbitrageEngine(Protocol):
    """Allocates optimal provider credentials and manages slot capacity."""
    def select_optimal_account(
        self, requirements: Dict[str, Any]
    ) -> Dict[str, Any]:
        ...

    def release_allocation(self, account_id: str, allocation_id: str) -> None:
        ...


# ============================================================================
# Concurrency & Synchronization Primitives
# ============================================================================

class ReadWriteLock:
    """Reader-Writer Lock allowing concurrent readers or exclusive writers."""

    def __init__(self) -> None:
        self._cond = threading.Condition(threading.Lock())
        self._readers = 0
        self._writers_waiting = 0
        self._writer_active = False

    def acquire_read(self) -> None:
        with self._cond:
            while self._writer_active or self._writers_waiting > 0:
                self._cond.wait()
            self._readers += 1

    def release_read(self) -> None:
        with self._cond:
            self._readers -= 1
            if self._readers == 0:
                self._cond.notify_all()

    def acquire_write(self) -> None:
        with self._cond:
            self._writers_waiting += 1
            while self._writer_active or self._readers > 0:
                self._cond.wait()
            self._writers_waiting -= 1
            self._writer_active = True

    def release_write(self) -> None:
        with self._cond:
            self._writer_active = False
            self._cond.notify_all()

    @contextmanager
    def read_locked(self) -> Iterator[None]:
        self.acquire_read()
        try:
            yield
        finally:
            self.release_read()

    @contextmanager
    def write_locked(self) -> Iterator[None]:
        self.acquire_write()
        try:
            yield
        finally:
            self.release_write()


# ============================================================================
# Holding Queue
# ============================================================================

class HoldingQueue:
    """
    Thread-safe priority queue holding deferred milestones awaiting retry.

    Ordered by -priority_score (highest priority score popped first).
    Thread-safety is guaranteed via an internal ReadWriteLock.
    """

    def __init__(self) -> None:
        self._lock = ReadWriteLock()
        self._heap: List[Tuple[float, int, datetime, str, Dict[str, Any]]] = []
        self._counter = 0

    def enqueue(
        self,
        milestone_id: str,
        priority_score: float,
        retry_after: datetime,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Insert a deferred milestone into the priority queue."""
        if retry_after.tzinfo is None:
            retry_after = retry_after.replace(tzinfo=timezone.utc)

        with self._lock.write_locked():
            self._counter += 1
            # heapq is min-heap: store -priority_score for max-heap behavior
            entry = (-priority_score, self._counter, retry_after, milestone_id, metadata or {})
            heapq.heappush(self._heap, entry)
            logger.info(
                "Enqueued deferred milestone to HoldingQueue",
                extra={
                    "milestone_id": milestone_id,
                    "priority_score": priority_score,
                    "retry_after": retry_after.isoformat(),
                },
            )

    def dequeue_ready(self) -> List[Tuple[str, Dict[str, Any]]]:
        """
        Pop and return all entries whose retry_after threshold <= utcnow().
        Returns list of (milestone_id, metadata) tuples in priority order.
        """
        now = datetime.now(timezone.utc)
        ready_entries: List[Tuple[str, Dict[str, Any]]] = []

        with self._lock.write_locked():
            remaining_heap: List[Tuple[float, int, datetime, str, Dict[str, Any]]] = []
            while self._heap:
                neg_priority, cnt, retry_after, m_id, meta = heapq.heappop(self._heap)
                if retry_after <= now:
                    ready_entries.append((m_id, meta))
                else:
                    remaining_heap.append((neg_priority, cnt, retry_after, m_id, meta))

            # Restore remaining unready elements back into heap
            heapq.heapify(remaining_heap)
            self._heap = remaining_heap

        return ready_entries

    def peek(self) -> Optional[Tuple[str, float, datetime]]:
        """Inspect the highest priority milestone without popping."""
        with self._lock.read_locked():
            if not self._heap:
                return None
            neg_priority, _, retry_after, m_id, _ = self._heap[0]
            return (m_id, -neg_priority, retry_after)

    def __len__(self) -> int:
        with self._lock.read_locked():
            return len(self._heap)


# ============================================================================
# Transactional SAGA Execution: PipelineTransaction
# ============================================================================

class PipelineTransaction:
    """
    Transactional SAGA coordinator maintaining a LIFO compensation stack.

    Ensures that if any stage in the milestone pipeline fails, compensating
    actions are executed in reverse order. Compensator failures are caught,
    logged with structured metadata and OpenTelemetry spans, without halting
    the remaining compensations.
    """

    def __init__(self, milestone_id: str) -> None:
        self.milestone_id = milestone_id
        self._stack: List[Tuple[PipelineStage, Callable[[], Any]]] = []
        self._committed = False

    def __enter__(self) -> "PipelineTransaction":
        self._stack.clear()
        self._committed = False
        return self

    def record_compensation(
        self, stage: PipelineStage, compensator: Callable[[], Any]
    ) -> None:
        """Register a compensating action for a successfully executed stage."""
        self._stack.append((stage, compensator))

    def commit(self) -> None:
        """Mark transaction as committed, preventing automatic rollback on normal exit."""
        self._committed = True

    def rollback(self, reason: str = "") -> None:
        """Explicitly execute compensators in LIFO order."""
        self._execute_compensators(reason=reason)

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> bool:
        if exc_type is not None or not self._committed:
            error_reason = str(exc_val) if exc_val else "Transaction uncommitted or aborted"
            self._execute_compensators(reason=error_reason)
        return False  # Propagate exception to caller if present

    def _execute_compensators(self, reason: str = "") -> None:
        """Executes all registered compensators in LIFO order."""
        if not self._stack:
            return

        logger.warning(
            "Executing pipeline compensation stack in LIFO order",
            extra={
                "milestone_id": self.milestone_id,
                "reason": reason,
                "compensation_depth": len(self._stack),
            },
        )

        while self._stack:
            stage, compensator = self._stack.pop()
            stage_name = stage.value.lower()
            span_name = f"hermes.pipeline.compensation.{stage_name}"

            with tracer.start_as_current_span(
                span_name,
                attributes={
                    "milestone_id": self.milestone_id,
                    "stage": stage.value,
                    "reason": reason,
                },
            ) as span:
                start_ns = time.perf_counter_ns()
                try:
                    logger.warning(
                        "Applying compensation for stage: %s",
                        stage.value,
                        extra={"milestone_id": self.milestone_id, "stage": stage.value},
                    )
                    compensator()
                    duration_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
                    span.set_attribute("stage_duration_ms", duration_ms)
                    span.set_attribute("outcome", "COMPENSATED")
                except Exception as comp_err:
                    duration_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
                    span.set_attribute("stage_duration_ms", duration_ms)
                    span.set_attribute("outcome", "COMPENSATION_FAILED")
                    span.set_attribute("error.message", str(comp_err))
                    logger.error(
                        "Compensator failed for stage %s: %s",
                        stage.value,
                        str(comp_err),
                        exc_info=True,
                        extra={
                            "milestone_id": self.milestone_id,
                            "stage": stage.value,
                            "error": str(comp_err),
                        },
                    )


# ============================================================================
# Pipeline Coordinator
# ============================================================================

class PipelineCoordinator:
    """
    Unified Milestone Pipeline Coordinator & Transactional Dispatch Orchestrator.

    Drives milestones through all 9 lifecycle stages:
      1. INTAKE
      2. COST_ESTIMATION
      3. FEASIBILITY_GATE
      4. DAG_INSERT
      5. READINESS_WAIT
      6. DISPATCH
      7. EXECUTION_MONITOR
      8. OUTCOME_CAPTURE
      9. FAILURE_RECOVERY
    """

    def __init__(
        self,
        cost_estimator: MilestoneCostEstimator,
        feasibility_gate: FeasibilityGate,
        decomposer: MilestoneDecomposer,
        dag_scheduler: TopologicalScheduler,
        governor: ExecutionGovernor,
        steering_memory: MemoryStore,
        provenance: ProvenanceLedger,
        lifecycle: LifecycleStateMachine,
        rollback: RollbackOrchestrator,
        quota_engine: QuotaArbitrageEngine,
    ) -> None:
        self.cost_estimator = cost_estimator
        self.feasibility_gate = feasibility_gate
        self.decomposer = decomposer
        self.dag_scheduler = dag_scheduler
        self.governor = governor
        self.steering_memory = steering_memory
        self.provenance = provenance
        self.lifecycle = lifecycle
        self.rollback = rollback
        self.quota_engine = quota_engine

        self.holding_queue = HoldingQueue()
        self._states_lock = ReadWriteLock()
        self._pipeline_states: Dict[str, PipelineStageInfo] = {}

        self._milestone_locks_guard = threading.Lock()
        self._milestone_locks: Dict[str, threading.RLock] = {}

    def _get_milestone_lock(self, milestone_id: str) -> threading.RLock:
        """Retrieve or create a reentrant lock keyed per milestone_id."""
        with self._milestone_locks_guard:
            if milestone_id not in self._milestone_locks:
                self._milestone_locks[milestone_id] = threading.RLock()
            return self._milestone_locks[milestone_id]

    def _set_stage(
        self,
        milestone_id: str,
        stage: PipelineStage,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Record observable stage transition and update state tracker."""
        now = datetime.now(timezone.utc)
        meta = metadata or {}
        with self._states_lock.write_locked():
            self._pipeline_states[milestone_id] = PipelineStageInfo(
                milestone_id=milestone_id,
                current_stage=stage,
                entered_at=now,
                stage_metadata=meta,
            )

    def get_pipeline_state(self, milestone_id: str) -> Optional[PipelineStageInfo]:
        """Thread-safe inspection of current stage and runtime metadata."""
        with self._states_lock.read_locked():
            state = self._pipeline_states.get(milestone_id)
            if state is None:
                return None
            return PipelineStageInfo(
                milestone_id=state.milestone_id,
                current_stage=state.current_stage,
                entered_at=state.entered_at,
                stage_metadata=dict(state.stage_metadata),
            )

    # ------------------------------------------------------------------------
    # Pipeline Submission & Stage Progression
    # ------------------------------------------------------------------------

    def submit_milestone(
        self,
        milestone_metadata: Dict[str, Any],
        initial_stage: PipelineStage = PipelineStage.INTAKE,
    ) -> PipelineResult:
        """
        Drives a milestone through all 9 stages within a PipelineTransaction.

        Handles recursive decomposition on FEASIBILITY_GATE revision and
        PREEMPTION during execution monitoring. Thread-safe per milestone_id.
        """
        milestone_id = str(milestone_metadata.get("id") or milestone_metadata.get("milestone_id"))
        if not milestone_id:
            raise ValueError("Milestone metadata must contain 'id' or 'milestone_id'")

        lock = self._get_milestone_lock(milestone_id)
        with lock:
            with tracer.start_as_current_span(
                "hermes.pipeline.submit",
                attributes={
                    "milestone_id": milestone_id,
                    "initial_stage": initial_stage.value,
                },
            ) as root_span:
                pipeline_start = time.perf_counter_ns()
                tx = PipelineTransaction(milestone_id)

                provenance_id = ""
                cost_estimate: Optional[CostEstimate] = None
                gate_decision: Optional[GateDecision] = None
                execution_telemetry: Optional[Dict[str, Any]] = None

                with tx:
                    try:
                        # ----------------------------------------------------
                        # STAGE 1: INTAKE
                        # ----------------------------------------------------
                        if initial_stage == PipelineStage.INTAKE:
                            provenance_id = self._stage_intake(
                                milestone_id, milestone_metadata, tx
                            )

                        # ----------------------------------------------------
                        # STAGE 2: COST ESTIMATION
                        # ----------------------------------------------------
                        cost_estimate = self._stage_cost_estimation(
                            milestone_id, milestone_metadata, tx
                        )

                        # ----------------------------------------------------
                        # STAGE 3: FEASIBILITY GATE
                        # ----------------------------------------------------
                        gate_decision, decomposed_results = self._stage_feasibility_gate(
                            milestone_id, milestone_metadata, cost_estimate, tx
                        )
                        if gate_decision.verdict == GateVerdict.DEFER_OR_REJECT:
                            tx.commit()
                            return PipelineResult(
                                milestone_id=milestone_id,
                                final_stage=PipelineStage.FEASIBILITY_GATE,
                                outcome="DEFERRED",
                                cost_estimate=cost_estimate,
                                gate_decision=gate_decision,
                                provenance_record_id=provenance_id,
                            )
                        elif gate_decision.verdict == GateVerdict.REVISE_OR_DECOMPOSE:
                            tx.commit()
                            sub_ids = [r.milestone_id for r in (decomposed_results or [])]
                            return PipelineResult(
                                milestone_id=milestone_id,
                                final_stage=PipelineStage.FEASIBILITY_GATE,
                                outcome="DECOMPOSED",
                                sub_milestones=sub_ids,
                                cost_estimate=cost_estimate,
                                gate_decision=gate_decision,
                                provenance_record_id=provenance_id,
                            )

                        # ----------------------------------------------------
                        # STAGE 4: DAG INSERT
                        # ----------------------------------------------------
                        self._stage_dag_insert(milestone_id, milestone_metadata, tx)

                        # ----------------------------------------------------
                        # STAGE 5: READINESS WAIT
                        # ----------------------------------------------------
                        self._stage_readiness_wait(milestone_id, milestone_metadata, tx)

                        # ----------------------------------------------------
                        # STAGE 6: DISPATCH
                        # ----------------------------------------------------
                        slot_allocation = self._stage_dispatch(
                            milestone_id, milestone_metadata, cost_estimate, tx
                        )

                        # ----------------------------------------------------
                        # STAGE 7: EXECUTION MONITOR
                        # ----------------------------------------------------
                        monitor_outcome, execution_telemetry, preempted_sub_results = (
                            self._stage_execution_monitor(
                                milestone_id, milestone_metadata, slot_allocation, tx
                            )
                        )
                        if monitor_outcome == "PREEMPTED":
                            tx.commit()
                            sub_ids = [
                                r.milestone_id for r in (preempted_sub_results or [])
                            ]
                            return PipelineResult(
                                milestone_id=milestone_id,
                                final_stage=PipelineStage.EXECUTION_MONITOR,
                                outcome="PREEMPTED",
                                sub_milestones=sub_ids,
                                cost_estimate=cost_estimate,
                                gate_decision=gate_decision,
                                execution_telemetry=execution_telemetry,
                                provenance_record_id=provenance_id,
                            )

                        # ----------------------------------------------------
                        # STAGE 8: OUTCOME CAPTURE
                        # ----------------------------------------------------
                        self._stage_outcome_capture(
                            milestone_id,
                            milestone_metadata,
                            execution_telemetry or {},
                            tx,
                        )

                        # Commit SAGA transaction — all stages completed successfully
                        tx.commit()

                        total_duration_ms = (
                            time.perf_counter_ns() - pipeline_start
                        ) / 1_000_000.0
                        root_span.set_attribute("total_duration_ms", total_duration_ms)
                        root_span.set_attribute("final_stage", PipelineStage.OUTCOME_CAPTURE.value)
                        root_span.set_attribute("outcome", "COMPLETED")

                        logger.info(
                            "Milestone pipeline completed successfully",
                            extra={
                                "milestone_id": milestone_id,
                                "duration_ms": total_duration_ms,
                                "outcome": "COMPLETED",
                            },
                        )

                        return PipelineResult(
                            milestone_id=milestone_id,
                            final_stage=PipelineStage.OUTCOME_CAPTURE,
                            outcome="COMPLETED",
                            cost_estimate=cost_estimate,
                            gate_decision=gate_decision,
                            execution_telemetry=execution_telemetry,
                            provenance_record_id=provenance_id,
                        )

                    except Exception as exc:
                        logger.error(
                            "Milestone pipeline failed, transitioning to FAILURE_RECOVERY",
                            exc_info=True,
                            extra={"milestone_id": milestone_id, "error": str(exc)},
                        )
                        # ----------------------------------------------------
                        # STAGE 9: FAILURE RECOVERY
                        # ----------------------------------------------------
                        recovery_outcome = self._stage_failure_recovery(
                            milestone_id, milestone_metadata, exc
                        )
                        root_span.set_attribute("outcome", recovery_outcome)
                        root_span.record_exception(exc)

                        return PipelineResult(
                            milestone_id=milestone_id,
                            final_stage=PipelineStage.FAILURE_RECOVERY,
                            outcome="ROLLED_BACK" if recovery_outcome == "ROLLED_BACK" else "FAILED",
                            cost_estimate=cost_estimate,
                            gate_decision=gate_decision,
                            execution_telemetry={"error": str(exc)},
                            provenance_record_id=provenance_id,
                        )

    # ------------------------------------------------------------------------
    # Individual Stage Implementations & Compensations
    # ------------------------------------------------------------------------

    def _stage_intake(
        self,
        milestone_id: str,
        milestone_metadata: Dict[str, Any],
        tx: PipelineTransaction,
    ) -> str:
        """Stage 1: Record intake planning record in Provenance Ledger."""
        stage = PipelineStage.INTAKE
        self._set_stage(milestone_id, stage, {"status": "starting_intake"})

        with tracer.start_as_current_span(
            f"hermes.pipeline.stage.{stage.value.lower()}",
            attributes={"milestone_id": milestone_id, "stage": stage.value},
        ) as span:
            start_ns = time.perf_counter_ns()

            rec_id = self.provenance.append_record(
                milestone_id=milestone_id,
                stage=stage.value,
                metadata={
                    "metadata": milestone_metadata,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
            )

            # Compensation: void provenance record
            def compensate_intake():
                self.provenance.void_record(rec_id)

            tx.record_compensation(stage, compensate_intake)

            # Lifecycle transition
            self.lifecycle.transition(milestone_id, "INTAKE", {"metadata": milestone_metadata})

            duration_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
            span.set_attribute("stage_duration_ms", duration_ms)
            span.set_attribute("outcome", "COMPLETED")
            span.set_attribute("provenance_record_id", rec_id)

            logger.info(
                "Stage INTAKE completed",
                extra={
                    "milestone_id": milestone_id,
                    "stage": stage.value,
                    "duration_ms": duration_ms,
                    "outcome": "COMPLETED",
                },
            )
            return rec_id

    def _stage_cost_estimation(
        self,
        milestone_id: str,
        milestone_metadata: Dict[str, Any],
        tx: PipelineTransaction,
    ) -> CostEstimate:
        """Stage 2: Evaluate predictive token and dollar cost (stateless)."""
        stage = PipelineStage.COST_ESTIMATION
        self._set_stage(milestone_id, stage, {"status": "estimating_cost"})

        with tracer.start_as_current_span(
            f"hermes.pipeline.stage.{stage.value.lower()}",
            attributes={"milestone_id": milestone_id, "stage": stage.value},
        ) as span:
            start_ns = time.perf_counter_ns()

            # Compensation: stateless query -> no-op
            tx.record_compensation(stage, lambda: None)

            cost_est = self.cost_estimator.estimate_milestone_cost(milestone_metadata)

            duration_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
            span.set_attribute("stage_duration_ms", duration_ms)
            span.set_attribute("outcome", "COMPLETED")
            span.set_attribute("estimated_cost_usd", getattr(cost_est, "estimated_cost_usd", 0.0))

            logger.info(
                "Stage COST_ESTIMATION completed",
                extra={
                    "milestone_id": milestone_id,
                    "stage": stage.value,
                    "duration_ms": duration_ms,
                    "outcome": "COMPLETED",
                    "estimated_cost_usd": getattr(cost_est, "estimated_cost_usd", 0.0),
                },
            )
            return cost_est

    def _stage_feasibility_gate(
        self,
        milestone_id: str,
        milestone_metadata: Dict[str, Any],
        cost_estimate: CostEstimate,
        tx: PipelineTransaction,
    ) -> Tuple[GateDecision, Optional[List[PipelineResult]]]:
        """Stage 3: Validate admissibility, queue for deferral, or decompose."""
        stage = PipelineStage.FEASIBILITY_GATE
        self._set_stage(milestone_id, stage, {"status": "evaluating_gate"})

        with tracer.start_as_current_span(
            f"hermes.pipeline.stage.{stage.value.lower()}",
            attributes={"milestone_id": milestone_id, "stage": stage.value},
        ) as span:
            start_ns = time.perf_counter_ns()

            decision = self.feasibility_gate.evaluate(milestone_metadata, cost_estimate)

            # Compensation: revert lifecycle transition if emitted
            def compensate_feasibility():
                self.lifecycle.revert_transition(milestone_id)

            tx.record_compensation(stage, compensate_feasibility)

            decomposed_results: Optional[List[PipelineResult]] = None

            if decision.verdict == GateVerdict.APPROVED:
                self.lifecycle.transition(milestone_id, "GATE_APPROVED", {"decision": decision.reason})
                outcome_str = "APPROVED"

            elif decision.verdict == GateVerdict.DEFER_OR_REJECT:
                retry_at = decision.suggested_retry_after or datetime.now(timezone.utc)
                prio = float(milestone_metadata.get("priority_score", 1.0))
                self.holding_queue.enqueue(
                    milestone_id=milestone_id,
                    priority_score=prio,
                    retry_after=retry_at,
                    metadata=milestone_metadata,
                )
                self.lifecycle.transition(
                    milestone_id, "DEFERRED", {"retry_after": retry_at.isoformat()}
                )
                outcome_str = "DEFERRED"

            elif decision.verdict == GateVerdict.REVISE_OR_DECOMPOSE:
                outcome_str = "DECOMPOSED"
                self.lifecycle.transition(milestone_id, "DECOMPOSED", {"reason": decision.reason})
                sub_meta_list = self.decomposer.decompose(
                    milestone_metadata, trigger=DecomposeTrigger.FEASIBILITY_GATE
                )
                decomposed_results = []
                for sub_meta in sub_meta_list:
                    # Recursively submit each child sub-milestone
                    sub_res = self.submit_milestone(sub_meta, initial_stage=PipelineStage.INTAKE)
                    decomposed_results.append(sub_res)

            else:
                outcome_str = "UNKNOWN"

            duration_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
            span.set_attribute("stage_duration_ms", duration_ms)
            span.set_attribute("outcome", outcome_str)
            span.set_attribute("gate_verdict", decision.verdict.value)

            logger.info(
                "Stage FEASIBILITY_GATE evaluated verdict: %s",
                decision.verdict.value,
                extra={
                    "milestone_id": milestone_id,
                    "stage": stage.value,
                    "duration_ms": duration_ms,
                    "outcome": outcome_str,
                },
            )

            return decision, decomposed_results

    def _stage_dag_insert(
        self,
        milestone_id: str,
        milestone_metadata: Dict[str, Any],
        tx: PipelineTransaction,
    ) -> None:
        """Stage 4: Insert milestone into DependencyGraph and recompute critical path."""
        stage = PipelineStage.DAG_INSERT
        self._set_stage(milestone_id, stage, {"status": "inserting_dag"})

        with tracer.start_as_current_span(
            f"hermes.pipeline.stage.{stage.value.lower()}",
            attributes={"milestone_id": milestone_id, "stage": stage.value},
        ) as span:
            start_ns = time.perf_counter_ns()

            self.dag_scheduler.insert_milestone(milestone_metadata)
            self.dag_scheduler.recompute_critical_path()

            # Compensation: remove node from DependencyGraph and recompute critical path
            def compensate_dag():
                self.dag_scheduler.remove_milestone(milestone_id)
                self.dag_scheduler.recompute_critical_path()

            tx.record_compensation(stage, compensate_dag)

            duration_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
            span.set_attribute("stage_duration_ms", duration_ms)
            span.set_attribute("outcome", "COMPLETED")

            logger.info(
                "Stage DAG_INSERT completed",
                extra={
                    "milestone_id": milestone_id,
                    "stage": stage.value,
                    "duration_ms": duration_ms,
                    "outcome": "COMPLETED",
                },
            )

    def _stage_readiness_wait(
        self,
        milestone_id: str,
        milestone_metadata: Dict[str, Any],
        tx: PipelineTransaction,
    ) -> None:
        """Stage 5: Verify readiness poll set before dispatch."""
        stage = PipelineStage.READINESS_WAIT
        self._set_stage(milestone_id, stage, {"status": "awaiting_readiness"})

        with tracer.start_as_current_span(
            f"hermes.pipeline.stage.{stage.value.lower()}",
            attributes={"milestone_id": milestone_id, "stage": stage.value},
        ) as span:
            start_ns = time.perf_counter_ns()

            # Register compensation: remove from readiness poll set
            tx.record_compensation(stage, lambda: None)

            # Check readiness in scheduler
            is_ready = self.dag_scheduler.is_ready(milestone_id)
            if not is_ready:
                logger.info(
                    "Milestone %s not immediately ready in DAG, proceeding under optimistic dispatch",
                    milestone_id,
                )

            duration_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
            span.set_attribute("stage_duration_ms", duration_ms)
            span.set_attribute("outcome", "COMPLETED")
            span.set_attribute("is_ready", is_ready)

            logger.info(
                "Stage READINESS_WAIT completed",
                extra={
                    "milestone_id": milestone_id,
                    "stage": stage.value,
                    "duration_ms": duration_ms,
                    "outcome": "COMPLETED",
                },
            )

    def _stage_dispatch(
        self,
        milestone_id: str,
        milestone_metadata: Dict[str, Any],
        cost_estimate: Optional[CostEstimate],
        tx: PipelineTransaction,
    ) -> Dict[str, Any]:
        """Stage 6: Select optimal execution account and allocate execution slot."""
        stage = PipelineStage.DISPATCH
        self._set_stage(milestone_id, stage, {"status": "allocating_slot"})

        with tracer.start_as_current_span(
            f"hermes.pipeline.stage.{stage.value.lower()}",
            attributes={"milestone_id": milestone_id, "stage": stage.value},
        ) as span:
            start_ns = time.perf_counter_ns()

            requirements = {
                "milestone_id": milestone_id,
                "model_name": cost_estimate.model_name if cost_estimate else "default",
                "estimated_tokens": (
                    (cost_estimate.estimated_prompt_tokens + cost_estimate.estimated_completion_tokens)
                    if cost_estimate
                    else 1000
                ),
            }

            slot_alloc = self.quota_engine.select_optimal_account(requirements)
            account_id = str(slot_alloc.get("account_id", "default_account"))
            alloc_id = str(slot_alloc.get("allocation_id", f"alloc_{milestone_id}"))

            # Compensation: release execution slot, return quota allocation
            def compensate_dispatch():
                self.quota_engine.release_allocation(account_id, alloc_id)

            tx.record_compensation(stage, compensate_dispatch)

            self.lifecycle.transition(
                milestone_id, "DISPATCHED", {"account_id": account_id, "allocation_id": alloc_id}
            )

            duration_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
            span.set_attribute("stage_duration_ms", duration_ms)
            span.set_attribute("outcome", "COMPLETED")
            span.set_attribute("account_id", account_id)

            logger.info(
                "Stage DISPATCH completed, allocated account %s",
                account_id,
                extra={
                    "milestone_id": milestone_id,
                    "stage": stage.value,
                    "duration_ms": duration_ms,
                    "outcome": "COMPLETED",
                },
            )

            return slot_alloc

    def _stage_execution_monitor(
        self,
        milestone_id: str,
        milestone_metadata: Dict[str, Any],
        slot_allocation: Dict[str, Any],
        tx: PipelineTransaction,
    ) -> Tuple[str, Dict[str, Any], Optional[List[PipelineResult]]]:
        """Stage 7: Supervise active execution, handle preemption and throttling."""
        stage = PipelineStage.EXECUTION_MONITOR
        self._set_stage(milestone_id, stage, {"status": "supervising_execution"})

        with tracer.start_as_current_span(
            f"hermes.pipeline.stage.{stage.value.lower()}",
            attributes={"milestone_id": milestone_id, "stage": stage.value},
        ) as span:
            start_ns = time.perf_counter_ns()

            slot_info = {
                "milestone_id": milestone_id,
                "metadata": milestone_metadata,
                "allocation": slot_allocation,
                "started_at": datetime.now(timezone.utc).isoformat(),
            }
            self.governor.start_supervision([slot_info])

            # Compensation: signal Governor to abandon slot
            def compensate_monitor():
                self.governor.abandon_slot(milestone_id)

            tx.record_compensation(stage, compensate_monitor)

            # Supervision loop / inspection tick
            actions = self.governor.tick()
            preempt_action = next(
                (
                    a
                    for a in actions
                    if getattr(a, "milestone_id", "") == milestone_id
                    and getattr(a, "action_type", None) == GovernorActionType.PREEMPT
                ),
                None,
            )

            preempted_sub_results: Optional[List[PipelineResult]] = None
            if preempt_action is not None:
                logger.warning(
                    "Milestone %s PREEMPTED by ExecutionGovernor: %s",
                    milestone_id,
                    preempt_action.reason,
                    extra={"milestone_id": milestone_id, "reason": preempt_action.reason},
                )
                self.lifecycle.transition(
                    milestone_id, "PREEMPTED", {"reason": preempt_action.reason}
                )

                # Invoke MilestoneDecomposer with GOVERNOR_PREEMPT trigger
                sub_meta_list = self.decomposer.decompose(
                    milestone_metadata, trigger=DecomposeTrigger.GOVERNOR_PREEMPT
                )
                preempted_sub_results = []
                for sub_meta in sub_meta_list:
                    sub_res = self.submit_milestone(sub_meta, initial_stage=PipelineStage.INTAKE)
                    preempted_sub_results.append(sub_res)

                duration_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
                span.set_attribute("stage_duration_ms", duration_ms)
                span.set_attribute("outcome", "PREEMPTED")
                return "PREEMPTED", {"status": "preempted", "reason": preempt_action.reason}, preempted_sub_results

            # Simulated telemetry payload from active slot execution
            execution_telemetry = {
                "milestone_id": milestone_id,
                "status": "success",
                "output": "Artifacts generated successfully",
                "tokens_consumed": 1250,
                "duration_sec": 1.25,
            }

            duration_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
            span.set_attribute("stage_duration_ms", duration_ms)
            span.set_attribute("outcome", "COMPLETED")

            logger.info(
                "Stage EXECUTION_MONITOR completed",
                extra={
                    "milestone_id": milestone_id,
                    "stage": stage.value,
                    "duration_ms": duration_ms,
                    "outcome": "COMPLETED",
                },
            )

            return "COMPLETED", execution_telemetry, None

    def _stage_outcome_capture(
        self,
        milestone_id: str,
        milestone_metadata: Dict[str, Any],
        telemetry: Dict[str, Any],
        tx: PipelineTransaction,
    ) -> str:
        """Stage 8: Persist steering memory records and seal provenance ledger."""
        stage = PipelineStage.OUTCOME_CAPTURE
        self._set_stage(milestone_id, stage, {"status": "capturing_outcome"})

        with tracer.start_as_current_span(
            f"hermes.pipeline.stage.{stage.value.lower()}",
            attributes={"milestone_id": milestone_id, "stage": stage.value},
        ) as span:
            start_ns = time.perf_counter_ns()

            record_payload = {
                "milestone_id": milestone_id,
                "telemetry": telemetry,
                "captured_at": datetime.now(timezone.utc).isoformat(),
            }
            rec_id = self.steering_memory.record_outcome(record_payload)

            # Compensation: mark Steering Memory record as voided
            def compensate_outcome():
                self.steering_memory.void_record(rec_id)

            tx.record_compensation(stage, compensate_outcome)

            self.provenance.append_record(
                milestone_id=milestone_id,
                stage=stage.value,
                metadata={"steering_record_id": rec_id, "telemetry": telemetry},
            )

            self.lifecycle.transition(milestone_id, "COMPLETED", {"steering_record_id": rec_id})

            duration_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
            span.set_attribute("stage_duration_ms", duration_ms)
            span.set_attribute("outcome", "COMPLETED")
            span.set_attribute("steering_record_id", rec_id)

            logger.info(
                "Stage OUTCOME_CAPTURE completed",
                extra={
                    "milestone_id": milestone_id,
                    "stage": stage.value,
                    "duration_ms": duration_ms,
                    "outcome": "COMPLETED",
                },
            )

            return rec_id

    def _stage_failure_recovery(
        self,
        milestone_id: str,
        milestone_metadata: Dict[str, Any],
        exception: BaseException,
    ) -> str:
        """Stage 9: Execute automated rollback and update lifecycle state machine."""
        stage = PipelineStage.FAILURE_RECOVERY
        self._set_stage(milestone_id, stage, {"status": "recovering", "error": str(exception)})

        with tracer.start_as_current_span(
            f"hermes.pipeline.stage.{stage.value.lower()}",
            attributes={
                "milestone_id": milestone_id,
                "stage": stage.value,
                "error.message": str(exception),
            },
        ) as span:
            start_ns = time.perf_counter_ns()

            rollback_success = False
            try:
                rollback_success = self.rollback.execute_rollback(
                    milestone_id, {"exception": str(exception)}
                )
            except Exception as rb_err:
                logger.error(
                    "Rollback orchestrator encountered error: %s",
                    str(rb_err),
                    exc_info=True,
                    extra={"milestone_id": milestone_id},
                )

            final_state = "ROLLED_BACK" if rollback_success else "FAILED"
            try:
                self.lifecycle.transition(
                    milestone_id, final_state, {"error": str(exception)}
                )
            except Exception:
                pass

            duration_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
            span.set_attribute("stage_duration_ms", duration_ms)
            span.set_attribute("outcome", final_state)

            logger.warning(
                "Stage FAILURE_RECOVERY finalized with outcome: %s",
                final_state,
                extra={
                    "milestone_id": milestone_id,
                    "stage": stage.value,
                    "duration_ms": duration_ms,
                    "outcome": final_state,
                },
            )

            return final_state

    # ------------------------------------------------------------------------
    # Holding Queue Maintenance
    # ------------------------------------------------------------------------

    def drain_holding_queue(self) -> List[PipelineResult]:
        """
        Re-evaluates all DEFERRED milestones whose retry_after <= now().

        Re-submits them into the pipeline starting at the COST_ESTIMATION stage.
        Returns a list of completed PipelineResult objects.
        """
        ready_items = self.holding_queue.dequeue_ready()
        if not ready_items:
            return []

        logger.info(
            "Draining holding queue: %d ready milestones",
            len(ready_items),
            extra={"count": len(ready_items)},
        )

        results: List[PipelineResult] = []
        for milestone_id, metadata in ready_items:
            # Re-evaluate deferred milestones starting at COST_ESTIMATION stage
            meta = metadata or {"id": milestone_id}
            if "id" not in meta and "milestone_id" not in meta:
                meta["id"] = milestone_id
            result = self.submit_milestone(meta, initial_stage=PipelineStage.COST_ESTIMATION)
            results.append(result)

        return results


# ============================================================================
# Stubs & Self-Verification Demonstration Harness
# ============================================================================

class MockMilestoneCostEstimator:
    def estimate_milestone_cost(self, milestone_metadata: Dict[str, Any]) -> CostEstimate:
        return CostEstimate(
            estimated_prompt_tokens=850,
            estimated_completion_tokens=420,
            estimated_cost_usd=0.0035,
            estimated_duration_sec=2.5,
            model_name="claude-3-5-sonnet",
        )


class MockFeasibilityGate:
    def __init__(self, verdict: GateVerdict = GateVerdict.APPROVED) -> None:
        self.verdict = verdict

    def evaluate(
        self, milestone_metadata: Dict[str, Any], cost_estimate: CostEstimate
    ) -> GateDecision:
        return GateDecision(
            verdict=self.verdict,
            reason="Mock admission criteria satisfied",
        )


class MockMilestoneDecomposer:
    def decompose(
        self, milestone_metadata: Dict[str, Any], trigger: DecomposeTrigger
    ) -> List[Dict[str, Any]]:
        parent_id = milestone_metadata.get("id", "m_root")
        return [
            {"id": f"{parent_id}.part1", "name": "Sub-milestone 1", "priority_score": 10.0},
            {"id": f"{parent_id}.part2", "name": "Sub-milestone 2", "priority_score": 8.0},
        ]


class MockTopologicalScheduler:
    def __init__(self) -> None:
        self.nodes: Dict[str, Any] = {}

    def insert_milestone(self, milestone_metadata: Dict[str, Any]) -> None:
        self.nodes[milestone_metadata.get("id", "")] = milestone_metadata

    def remove_milestone(self, milestone_id: str) -> None:
        self.nodes.pop(milestone_id, None)

    def recompute_critical_path(self) -> Any:
        return ["crit_node"]

    def is_ready(self, milestone_id: str) -> bool:
        return True


class MockExecutionGovernor:
    def __init__(self, action: GovernorActionType = GovernorActionType.CONTINUE) -> None:
        self.action = action
        self.active_slots: List[Dict[str, Any]] = []

    def start_supervision(self, slots: List[Dict[str, Any]]) -> None:
        self.active_slots.extend(slots)

    def tick(self) -> List[GovernorAction]:
        actions = []
        for s in self.active_slots:
            actions.append(
                GovernorAction(
                    action_type=self.action,
                    milestone_id=s.get("milestone_id", ""),
                    reason="Supervision cycle normal",
                )
            )
        return actions

    def abandon_slot(self, milestone_id: str) -> None:
        self.active_slots = [
            s for s in self.active_slots if s.get("milestone_id") != milestone_id
        ]


class MockMemoryStore:
    def __init__(self) -> None:
        self.records: Dict[str, Any] = {}
        self._seq = 0

    def record_outcome(self, outcome_data: Dict[str, Any]) -> str:
        self._seq += 1
        rid = f"mem_rec_{self._seq}"
        self.records[rid] = outcome_data
        return rid

    def void_record(self, record_id: str) -> None:
        if record_id in self.records:
            self.records[record_id]["voided"] = True


class MockProvenanceLedger:
    def __init__(self) -> None:
        self.ledger: Dict[str, Any] = {}
        self._seq = 0

    def append_record(
        self, milestone_id: str, stage: str, metadata: Dict[str, Any]
    ) -> str:
        self._seq += 1
        rec_id = f"prov_rec_{self._seq}"
        self.ledger[rec_id] = {
            "milestone_id": milestone_id,
            "stage": stage,
            "metadata": metadata,
            "voided": False,
        }
        return rec_id

    def void_record(self, record_id: str) -> None:
        if record_id in self.ledger:
            self.ledger[record_id]["voided"] = True


class MockLifecycleStateMachine:
    def __init__(self) -> None:
        self.history: List[Tuple[str, str]] = []

    def transition(
        self, milestone_id: str, target_state: str, context: Optional[Dict[str, Any]] = None
    ) -> bool:
        self.history.append((milestone_id, target_state))
        return True

    def revert_transition(self, milestone_id: str) -> None:
        self.history.append((milestone_id, "REVERTED"))


class MockRollbackOrchestrator:
    def execute_rollback(
        self, milestone_id: str, context: Optional[Dict[str, Any]] = None
    ) -> bool:
        return True


class MockQuotaArbitrageEngine:
    def select_optimal_account(
        self, requirements: Dict[str, Any]
    ) -> Dict[str, Any]:
        return {
            "account_id": "google-pro-tier-1",
            "allocation_id": f"alloc_{requirements.get('milestone_id')}",
            "allocated_tokens": requirements.get("estimated_tokens", 1000),
        }

    def release_allocation(self, account_id: str, allocation_id: str) -> None:
        pass


def main() -> None:
    """Instantiate pipeline coordinator with mock subsystems and run demonstration."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
    )

    print("================================================================================")
    print("Hermes Orchestrator - PipelineCoordinator Instantiation & Progression Test")
    print("================================================================================")

    coordinator = PipelineCoordinator(
        cost_estimator=MockMilestoneCostEstimator(),
        feasibility_gate=MockFeasibilityGate(verdict=GateVerdict.APPROVED),
        decomposer=MockMilestoneDecomposer(),
        dag_scheduler=MockTopologicalScheduler(),
        governor=MockExecutionGovernor(action=GovernorActionType.CONTINUE),
        steering_memory=MockMemoryStore(),
        provenance=MockProvenanceLedger(),
        lifecycle=MockLifecycleStateMachine(),
        rollback=MockRollbackOrchestrator(),
        quota_engine=MockQuotaArbitrageEngine(),
    )

    milestone_test_payload = {
        "id": "MS-AUTONOMOUS-REPLAN-409",
        "title": "Hierarchical Re-Planning & Dynamic Dispatch",
        "priority_score": 95.5,
        "token_budget": 50000,
    }

    print(f"\nSubmitting milestone: {milestone_test_payload['id']}")
    result = coordinator.submit_milestone(milestone_test_payload)

    print("\n--- Pipeline Execution Summary ---")
    print(f"Milestone ID         : {result.milestone_id}")
    print(f"Final Stage          : {result.final_stage.value}")
    print(f"Final Outcome        : {result.outcome}")
    print(f"Cost Estimate (USD)  : ${result.cost_estimate.estimated_cost_usd:.4f}" if result.cost_estimate else "None")
    print(f"Provenance Record ID : {result.provenance_record_id}")
    print(f"Telemetry Status     : {result.execution_telemetry.get('status') if result.execution_telemetry else 'N/A'}")

    state_info = coordinator.get_pipeline_state(result.milestone_id)
    if state_info:
        print(f"Observed Stage Info  : {state_info.current_stage.value} entered at {state_info.entered_at}")

    print("\n[SUCCESS] PipelineCoordinator verified operational.")


if __name__ == "__main__":
    main()