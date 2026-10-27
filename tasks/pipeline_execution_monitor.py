"""
Pipeline Execution Monitor & Dynamic Re-Scheduling Feedback Controller.

Architecture Role:
    Part of the Hermes Orchestrator autonomous execution pipeline. Provides real-time
    execution monitoring, schedule conformance tracking, drift detection across temporal,
    quota, and topological dimensions, dynamic cascade re-scheduling orchestration,
    and feedback telemetry emission to the Predictive Analytics engine and Self-Tuner.

Components:
    1. ScheduleConformanceTracker: Tracks per-milestone duration deviation delta_i and
       computes global schedule conformance C weighted by priority.
    2. DriftDetector: Evaluates TemporalDrift (alpha threshold), QuotaDrift (beta threshold),
       and TopologicalDrift (dependency failure propagation) on periodic monitor ticks.
    3. ReSchedulingCascadeController: Orchestrates state snapshots (ScheduleDeviation),
       re-invokes Capacity Planning and Execution Scheduling, and atomically swaps active
       schedules without interrupting in-flight tasks.
    4. FeedbackTelemetryEmitter: Buffers and publishes FeedbackRecords comparing planned
       vs actual metrics for downstream model refinement.
    5. PipelineExecutionMonitor: Unified thread-safe coordinator exposing snapshot
       telemetry for the Web Cockpit UI and driving monitor ticks.
"""

from __future__ import annotations

import copy
from dataclasses import asdict, dataclass, field
from enum import Enum
import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Protocol, Tuple, runtime_checkable
import unittest

logger = logging.getLogger("hermes.pipeline_execution_monitor")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


class ExecutionStatus(str, Enum):
    """Execution status lifecycle for scheduled milestones."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CIRCUIT_BROKEN = "circuit_broken"


class DriftSeverity(str, Enum):
    """Severity classification for drift events."""
    WARNING = "warning"
    CRITICAL = "critical"


class DriftType(str, Enum):
    """Categorization of execution plan divergence."""
    TEMPORAL = "TemporalDrift"
    QUOTA = "QuotaDrift"
    TOPOLOGICAL = "TopologicalDrift"


@dataclass
class ScheduleEntry:
    """Planned dispatch specification for an individual milestone."""
    milestone_id: str
    repo_name: str
    planned_start_time: float
    planned_duration_s: float
    allocated_tokens: int
    assigned_account_id: str
    priority_weight: float = 1.0
    dependency_predecessors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ExecutionRecord:
    """Observed runtime execution record of a milestone."""
    milestone_id: str
    actual_start_time: float = 0.0
    elapsed_s: float = 0.0
    tokens_consumed: int = 0
    status: str = ExecutionStatus.PENDING.value
    error_context: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class DriftEvent:
    """Event emitted when execution behavior breaches expected schedule boundaries."""
    drift_type: str
    milestone_id: str
    severity: str
    observed_value: float
    threshold_value: float
    timestamp: float = field(default_factory=time.time)
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ScheduleDeviation:
    """State snapshot captured when critical drift triggers schedule recomputation."""
    completed: list[dict[str, Any]]
    in_flight: list[dict[str, Any]]
    failed: list[dict[str, Any]]
    remaining_undispatched: list[dict[str, Any]]
    token_budget_residuals: dict[str, int]
    revised_risk_profiles: dict[str, float]
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class FeedbackRecord:
    """Telemetry record correlating planned vs actual resources for model tuning."""
    milestone_id: str
    planned_duration: float
    actual_duration: float
    planned_tokens: int
    actual_tokens: int
    outcome: str
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@runtime_checkable
class CapacityPlannerProtocol(Protocol):
    """Interface for invoking capacity reallocation upon schedule deviation."""
    def recompute_allocation(self, deviation: ScheduleDeviation) -> dict[str, Any]:
        ...


@runtime_checkable
class ExecutionSchedulerProtocol(Protocol):
    """Interface for re-generating dispatch order from an updated allocation plan."""
    def reschedule(
        self,
        new_allocation: dict[str, Any],
        remaining_milestones: list[ScheduleEntry],
    ) -> dict[str, ScheduleEntry]:
        ...


class ScheduleConformanceTracker:
    """
    Tracks execution progression against planned milestone schedules.

    Calculates:
        - Per-milestone deviation: delta_i = |actual_elapsed - planned_duration| / planned_duration
        - Aggregate schedule conformance: C = 1 - (sum(w_i * delta_i) / sum(w_i)), clamped to [0, 1]
    """

    def __init__(self, schedule: Optional[dict[str, ScheduleEntry]] = None) -> None:
        self._lock = threading.Lock()
        self._schedule: dict[str, ScheduleEntry] = copy.deepcopy(schedule) if schedule else {}
        self._records: dict[str, ExecutionRecord] = {}

        if schedule:
            for m_id in schedule:
                self._records[m_id] = ExecutionRecord(milestone_id=m_id)

    def set_schedule(self, schedule: dict[str, ScheduleEntry]) -> None:
        """Atomically update the schedule mapping while preserving current execution progress."""
        with self._lock:
            self._schedule = copy.deepcopy(schedule)
            for m_id in schedule:
                if m_id not in self._records:
                    self._records[m_id] = ExecutionRecord(milestone_id=m_id)
            logger.debug("Schedule conformance tracker loaded %d schedule entries", len(schedule))

    def update_record(self, record: ExecutionRecord) -> None:
        """Update or insert an execution record for a milestone."""
        with self._lock:
            self._records[record.milestone_id] = copy.deepcopy(record)

    def get_schedule_entry(self, milestone_id: str) -> Optional[ScheduleEntry]:
        with self._lock:
            entry = self._schedule.get(milestone_id)
            return copy.deepcopy(entry) if entry else None

    def get_record(self, milestone_id: str) -> Optional[ExecutionRecord]:
        with self._lock:
            rec = self._records.get(milestone_id)
            return copy.deepcopy(rec) if rec else None

    def compute_milestone_deviation(self, milestone_id: str) -> float:
        """
        Computes delta_i = |actual_elapsed - planned_duration| / planned_duration.
        Guards against zero-division and uninitialized schedules.
        """
        with self._lock:
            entry = self._schedule.get(milestone_id)
            rec = self._records.get(milestone_id)

            if not entry or not rec:
                return 0.0

            planned = float(entry.planned_duration_s)
            if planned <= 0.0:
                return 0.0

            actual = float(rec.elapsed_s)
            return abs(actual - planned) / planned

    def compute_aggregate_conformance(self) -> float:
        """
        Computes aggregate schedule conformance:
            C = 1 - (sum(w_i * delta_i) / sum(w_i))
        Clamped to [0.0, 1.0]. Returns 1.0 if no evaluated milestones exist.
        """
        with self._lock:
            evaluated_milestones = [
                m_id for m_id, rec in self._records.items()
                if rec.status in (ExecutionStatus.RUNNING.value, ExecutionStatus.SUCCEEDED.value, ExecutionStatus.FAILED.value)
                and m_id in self._schedule
            ]

            if not evaluated_milestones:
                return 1.0

            weighted_deviation_sum = 0.0
            total_weight = 0.0

            for m_id in evaluated_milestones:
                entry = self._schedule[m_id]
                rec = self._records[m_id]
                weight = max(float(entry.priority_weight), 0.0)

                planned = float(entry.planned_duration_s)
                if planned > 0.0:
                    deviation = abs(float(rec.elapsed_s) - planned) / planned
                else:
                    deviation = 0.0

                weighted_deviation_sum += weight * deviation
                total_weight += weight

            if total_weight <= 0.0:
                return 1.0

            conformance = 1.0 - (weighted_deviation_sum / total_weight)
            return max(0.0, min(1.0, conformance))

    def get_cumulative_deviation_stats(self) -> dict[str, Any]:
        """Aggregate statistical summary of current schedule deviations."""
        with self._lock:
            deviations: list[float] = []
            for m_id, rec in self._records.items():
                if rec.status in (ExecutionStatus.RUNNING.value, ExecutionStatus.SUCCEEDED.value, ExecutionStatus.FAILED.value):
                    entry = self._schedule.get(m_id)
                    if entry and entry.planned_duration_s > 0:
                        deviations.append(abs(float(rec.elapsed_s) - entry.planned_duration_s) / entry.planned_duration_s)

            if not deviations:
                return {
                    "count": 0,
                    "mean_deviation": 0.0,
                    "max_deviation": 0.0,
                    "min_deviation": 0.0,
                }

            return {
                "count": len(deviations),
                "mean_deviation": sum(deviations) / len(deviations),
                "max_deviation": max(deviations),
                "min_deviation": min(deviations),
            }


class DriftDetector:
    """
    Evaluates execution divergence predicates across active milestones.

    Predicates evaluated:
        1. TemporalDrift: Active milestone elapsed_s > alpha * planned_duration_s (alpha default 1.5).
        2. QuotaDrift: Sum of active token burn rates exceeds beta * forecasted rate (beta default 1.3).
        3. TopologicalDrift: Predecessor milestone fails or is circuit-broken,
           invalidating downstream dependency paths.
    """

    def __init__(
        self,
        alpha: float = 1.5,
        beta: float = 1.3,
        tick_interval_s: float = 15.0,
    ) -> None:
        self.alpha = float(alpha)
        self.beta = float(beta)
        self.tick_interval_s = float(tick_interval_s)
        self._lock = threading.Lock()

    def evaluate_drift(
        self,
        schedule: dict[str, ScheduleEntry],
        records: dict[str, ExecutionRecord],
        current_timestamp: Optional[float] = None,
    ) -> list[DriftEvent]:
        """
        Runs full drift evaluation against current schedule and execution records.
        Returns a list of DriftEvent instances.
        """
        now = current_timestamp if current_timestamp is not None else time.time()
        drift_events: list[DriftEvent] = []

        with self._lock:
            # 1. Temporal Drift Check
            for m_id, rec in records.items():
                if rec.status == ExecutionStatus.RUNNING.value and m_id in schedule:
                    entry = schedule[m_id]
                    planned_duration = float(entry.planned_duration_s)
                    elapsed = float(rec.elapsed_s)

                    critical_threshold = self.alpha * planned_duration
                    warning_threshold = 1.15 * planned_duration

                    if planned_duration > 0.0 and elapsed > critical_threshold:
                        drift_events.append(
                            DriftEvent(
                                drift_type=DriftType.TEMPORAL.value,
                                milestone_id=m_id,
                                severity=DriftSeverity.CRITICAL.value,
                                observed_value=elapsed,
                                threshold_value=critical_threshold,
                                timestamp=now,
                                message=(
                                    f"Milestone {m_id} elapsed {elapsed:.2f}s exceeds critical "
                                    f"temporal threshold {critical_threshold:.2f}s (alpha={self.alpha})"
                                ),
                            )
                        )
                    elif planned_duration > 0.0 and elapsed > warning_threshold:
                        drift_events.append(
                            DriftEvent(
                                drift_type=DriftType.TEMPORAL.value,
                                milestone_id=m_id,
                                severity=DriftSeverity.WARNING.value,
                                observed_value=elapsed,
                                threshold_value=warning_threshold,
                                timestamp=now,
                                message=(
                                    f"Milestone {m_id} elapsed {elapsed:.2f}s exceeds warning "
                                    f"temporal threshold {warning_threshold:.2f}s"
                                ),
                            )
                        )

            # 2. Quota Drift Check
            running_milestones = [
                m_id for m_id, rec in records.items()
                if rec.status == ExecutionStatus.RUNNING.value and m_id in schedule
            ]

            if running_milestones:
                total_observed_burn_rate = 0.0
                total_forecasted_burn_rate = 0.0

                for m_id in running_milestones:
                    entry = schedule[m_id]
                    rec = records[m_id]

                    elapsed_safe = max(float(rec.elapsed_s), 1.0)
                    planned_safe = max(float(entry.planned_duration_s), 1.0)

                    observed_rate = float(rec.tokens_consumed) / elapsed_safe
                    forecasted_rate = float(entry.allocated_tokens) / planned_safe

                    total_observed_burn_rate += observed_rate
                    total_forecasted_burn_rate += forecasted_rate

                quota_threshold = self.beta * total_forecasted_burn_rate
                if total_forecasted_burn_rate > 0.0 and total_observed_burn_rate > quota_threshold:
                    drift_events.append(
                        DriftEvent(
                            drift_type=DriftType.QUOTA.value,
                            milestone_id=",".join(sorted(running_milestones)),
                            severity=DriftSeverity.CRITICAL.value,
                            observed_value=total_observed_burn_rate,
                            threshold_value=quota_threshold,
                            timestamp=now,
                            message=(
                                f"Active token burn rate {total_observed_burn_rate:.2f} tokens/s "
                                f"exceeds forecasted threshold {quota_threshold:.2f} tokens/s (beta={self.beta})"
                            ),
                        )
                    )

            # 3. Topological Drift Check
            for m_id, entry in schedule.items():
                rec = records.get(m_id)
                current_status = rec.status if rec else ExecutionStatus.PENDING.value

                # If milestone is waiting to dispatch or currently running, ensure predecessors succeeded
                if current_status in (ExecutionStatus.PENDING.value, ExecutionStatus.RUNNING.value):
                    for pred_id in entry.dependency_predecessors:
                        pred_rec = records.get(pred_id)
                        if pred_rec and pred_rec.status in (
                            ExecutionStatus.FAILED.value,
                            ExecutionStatus.CIRCUIT_BROKEN.value,
                        ):
                            drift_events.append(
                                DriftEvent(
                                    drift_type=DriftType.TOPOLOGICAL.value,
                                    milestone_id=m_id,
                                    severity=DriftSeverity.CRITICAL.value,
                                    observed_value=1.0,
                                    threshold_value=0.0,
                                    timestamp=now,
                                    message=(
                                        f"Milestone {m_id} depends on predecessor {pred_id} "
                                        f"which entered terminal failure state '{pred_rec.status}'"
                                    ),
                                )
                            )

        return drift_events


class ReSchedulingCascadeController:
    """
    Orchestrates the dynamic re-scheduling cascade when critical drift events fire.

    Cascade execution flow:
        (a) Snapshot execution state into a ScheduleDeviation report.
        (b) Invoke Capacity Planner's recompute interface with token residuals and risk profiles.
        (c) Invoke Execution Scheduler's reschedule interface for remaining undispatched milestones.
        (d) Atomically swap the active schedule under lock, ensuring in-flight milestones
            continue uninterrupted while new dispatches conform to the revised sequence.
    """

    def __init__(
        self,
        capacity_planner: Optional[Any] = None,
        execution_scheduler: Optional[Any] = None,
    ) -> None:
        self.capacity_planner = capacity_planner
        self.execution_scheduler = execution_scheduler
        self.reschedule_count: int = 0
        self._lock = threading.Lock()

    def snapshot_deviation(
        self,
        schedule: dict[str, ScheduleEntry],
        records: dict[str, ExecutionRecord],
    ) -> ScheduleDeviation:
        """Constructs an immutable ScheduleDeviation report from active state."""
        completed: list[dict[str, Any]] = []
        in_flight: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        remaining_undispatched: list[dict[str, Any]] = []

        token_residuals_by_account: dict[str, int] = {}
        failure_counts_by_repo: dict[str, int] = {}
        total_counts_by_repo: dict[str, int] = {}

        for m_id, entry in schedule.items():
            repo = entry.repo_name
            total_counts_by_repo[repo] = total_counts_by_repo.get(repo, 0) + 1

            rec = records.get(m_id)
            status = rec.status if rec else ExecutionStatus.PENDING.value
            consumed = rec.tokens_consumed if rec else 0
            account = entry.assigned_account_id

            residual = max(0, entry.allocated_tokens - consumed)
            token_residuals_by_account[account] = token_residuals_by_account.get(account, 0) + residual

            entry_dict = entry.to_dict()
            if rec:
                entry_dict["execution"] = rec.to_dict()

            if status == ExecutionStatus.SUCCEEDED.value:
                completed.append(entry_dict)
            elif status == ExecutionStatus.RUNNING.value:
                # Project remaining duration
                elapsed = rec.elapsed_s if rec else 0.0
                projected_remaining = max(0.0, entry.planned_duration_s - elapsed)
                entry_dict["projected_remaining_duration_s"] = projected_remaining
                in_flight.append(entry_dict)
            elif status in (ExecutionStatus.FAILED.value, ExecutionStatus.CIRCUIT_BROKEN.value):
                failed.append(entry_dict)
                failure_counts_by_repo[repo] = failure_counts_by_repo.get(repo, 0) + 1
            else:
                remaining_undispatched.append(entry_dict)

        # Revised risk profiles: base risk + failure penalty
        revised_risk_profiles: dict[str, float] = {}
        for repo, total in total_counts_by_repo.items():
            fails = failure_counts_by_repo.get(repo, 0)
            revised_risk_profiles[repo] = round(fails / max(1, total), 4)

        return ScheduleDeviation(
            completed=completed,
            in_flight=in_flight,
            failed=failed,
            remaining_undispatched=remaining_undispatched,
            token_budget_residuals=token_residuals_by_account,
            revised_risk_profiles=revised_risk_profiles,
            timestamp=time.time(),
        )

    def execute_cascade(
        self,
        current_schedule: dict[str, ScheduleEntry],
        records: dict[str, ExecutionRecord],
        drift_events: list[DriftEvent],
    ) -> Tuple[dict[str, ScheduleEntry], ScheduleDeviation]:
        """
        Executes cascade re-scheduling triggered by critical drift events.
        Guarantees thread-safe schedule synthesis and in-flight milestone preservation.
        """
        with self._lock:
            self.reschedule_count += 1
            cascade_id = self.reschedule_count
            logger.info("Executing Rescheduling Cascade #%d triggered by %d drift events", cascade_id, len(drift_events))

            # Step A: Snapshot execution state
            deviation = self.snapshot_deviation(current_schedule, records)

            # Step B: Recompute capacity allocation
            allocation_plan: dict[str, Any] = {}
            if self.capacity_planner:
                try:
                    if hasattr(self.capacity_planner, "recompute_allocation"):
                        allocation_plan = self.capacity_planner.recompute_allocation(deviation)
                    elif callable(self.capacity_planner):
                        allocation_plan = self.capacity_planner(deviation)
                except Exception as exc:
                    logger.error("Capacity planner recompute failed during cascade #%d: %s", cascade_id, exc)

            # Step C: Re-schedule remaining undispatched milestones
            remaining_entries: list[ScheduleEntry] = [
                current_schedule[item["milestone_id"]]
                for item in deviation.remaining_undispatched
                if item["milestone_id"] in current_schedule
            ]

            revised_remaining_schedule: dict[str, ScheduleEntry] = {}
            if self.execution_scheduler:
                try:
                    if hasattr(self.execution_scheduler, "reschedule"):
                        revised_remaining_schedule = self.execution_scheduler.reschedule(
                            allocation_plan, remaining_entries
                        )
                    elif callable(self.execution_scheduler):
                        revised_remaining_schedule = self.execution_scheduler(allocation_plan, remaining_entries)
                except Exception as exc:
                    logger.error("Execution scheduler reschedule failed during cascade #%d: %s", cascade_id, exc)
            else:
                # Fallback: retain undispatched entries with updated priorities
                revised_remaining_schedule = {e.milestone_id: copy.deepcopy(e) for e in remaining_entries}

            # Step D: Construct unified revised schedule preserving completed and in-flight entries
            unified_schedule: dict[str, ScheduleEntry] = {}

            # Preserve completed and in-flight
            for item in deviation.completed:
                m_id = item["milestone_id"]
                if m_id in current_schedule:
                    unified_schedule[m_id] = copy.deepcopy(current_schedule[m_id])

            for item in deviation.in_flight:
                m_id = item["milestone_id"]
                if m_id in current_schedule:
                    unified_schedule[m_id] = copy.deepcopy(current_schedule[m_id])

            # Preserve failed
            for item in deviation.failed:
                m_id = item["milestone_id"]
                if m_id in current_schedule:
                    unified_schedule[m_id] = copy.deepcopy(current_schedule[m_id])

            # Incorporate revised remaining schedule
            for m_id, entry in revised_remaining_schedule.items():
                unified_schedule[m_id] = copy.deepcopy(entry)

            logger.info(
                "Cascade #%d completed: active schedule re-anchored with %d milestones (%d in-flight preserved)",
                cascade_id,
                len(unified_schedule),
                len(deviation.in_flight),
            )

            return unified_schedule, deviation


class FeedbackTelemetryEmitter:
    """
    Maintains an atomic buffer of FeedbackRecord telemetry and dispatches to
    the Predictive Analytics engine and Self-Tuner.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._feedback_buffer: list[FeedbackRecord] = []

    def record_milestone_feedback(
        self,
        schedule_entry: ScheduleEntry,
        execution_record: ExecutionRecord,
        timestamp: Optional[float] = None,
    ) -> FeedbackRecord:
        """Constructs and buffers a FeedbackRecord upon milestone completion."""
        ts = timestamp if timestamp is not None else time.time()
        record = FeedbackRecord(
            milestone_id=schedule_entry.milestone_id,
            planned_duration=float(schedule_entry.planned_duration_s),
            actual_duration=float(execution_record.elapsed_s),
            planned_tokens=int(schedule_entry.allocated_tokens),
            actual_tokens=int(execution_record.tokens_consumed),
            outcome=str(execution_record.status),
            timestamp=ts,
        )

        with self._lock:
            self._feedback_buffer.append(record)

        logger.debug(
            "Emitted FeedbackRecord for %s (outcome=%s, delta_t=%.2fs, delta_tok=%d)",
            record.milestone_id,
            record.outcome,
            record.actual_duration - record.planned_duration,
            record.actual_tokens - record.planned_tokens,
        )
        return record

    def record_reschedule_feedback(
        self,
        drift_event: DriftEvent,
        deviation: ScheduleDeviation,
        timestamp: Optional[float] = None,
    ) -> FeedbackRecord:
        """Constructs feedback tracking for dynamic re-scheduling occurrences."""
        ts = timestamp if timestamp is not None else time.time()
        record = FeedbackRecord(
            milestone_id=f"cascade_reschedule_{drift_event.milestone_id}",
            planned_duration=0.0,
            actual_duration=drift_event.observed_value,
            planned_tokens=0,
            actual_tokens=0,
            outcome=f"rescheduled_{drift_event.drift_type}",
            timestamp=ts,
        )
        with self._lock:
            self._feedback_buffer.append(record)
        return record

    def flush_feedback(self) -> list[FeedbackRecord]:
        """Atomically extracts and flushes all buffered feedback telemetry."""
        with self._lock:
            flushed = self._feedback_buffer[:]
            self._feedback_buffer.clear()
            return flushed

    def buffer_size(self) -> int:
        with self._lock:
            return len(self._feedback_buffer)


class PipelineExecutionMonitor:
    """
    Main coordinator for the Pipeline Execution Monitor & Dynamic Re-Scheduling Feedback Controller.

    Thread-safe orchestrator managing:
        - Schedule Conformance Tracking
        - Multi-predicate Drift Detection
        - Dynamic Cascade Re-scheduling
        - Telemetry Ingestion and Feedback Generation
        - Web Cockpit Real-time State Snapshots
    """

    def __init__(
        self,
        capacity_planner: Optional[Any] = None,
        execution_scheduler: Optional[Any] = None,
        alpha_drift: float = 1.5,
        beta_drift: float = 1.3,
        tick_interval_s: float = 15.0,
    ) -> None:
        self._lock = threading.Lock()
        self.tracker = ScheduleConformanceTracker()
        self.drift_detector = DriftDetector(alpha=alpha_drift, beta=beta_drift, tick_interval_s=tick_interval_s)
        self.cascade_controller = ReSchedulingCascadeController(
            capacity_planner=capacity_planner,
            execution_scheduler=execution_scheduler,
        )
        self.feedback_emitter = FeedbackTelemetryEmitter()
        self._active_drift_events: list[DriftEvent] = []
        self._last_tick_time: float = 0.0

    def load_schedule(self, schedule_entries: list[ScheduleEntry] | dict[str, ScheduleEntry]) -> None:
        """Loads or updates the master execution schedule."""
        with self._lock:
            if isinstance(schedule_entries, list):
                schedule_map = {entry.milestone_id: entry for entry in schedule_entries}
            else:
                schedule_map = dict(schedule_entries)

            self.tracker.set_schedule(schedule_map)
            logger.info("PipelineExecutionMonitor loaded %d schedule milestones", len(schedule_map))

    def record_milestone_start(self, milestone_id: str, actual_start_time: Optional[float] = None) -> None:
        """Marks a milestone as started and transitions status to RUNNING."""
        start_ts = actual_start_time if actual_start_time is not None else time.time()
        with self._lock:
            rec = self.tracker.get_record(milestone_id)
            if not rec:
                rec = ExecutionRecord(milestone_id=milestone_id)

            rec.actual_start_time = start_ts
            rec.status = ExecutionStatus.RUNNING.value
            self.tracker.update_record(rec)
            logger.info("Milestone %s started at timestamp %.2f", milestone_id, start_ts)

    def record_milestone_update(self, milestone_id: str, elapsed_s: float, tokens_consumed: int) -> None:
        """Records an in-flight progress update for a milestone."""
        with self._lock:
            rec = self.tracker.get_record(milestone_id)
            if not rec:
                rec = ExecutionRecord(milestone_id=milestone_id)

            rec.elapsed_s = float(elapsed_s)
            rec.tokens_consumed = int(tokens_consumed)
            rec.status = ExecutionStatus.RUNNING.value
            self.tracker.update_record(rec)

    def record_milestone_completion(
        self,
        milestone_id: str,
        status: str,
        error_context: Optional[str] = None,
    ) -> Optional[FeedbackRecord]:
        """
        Marks milestone as completed, updates records, and buffers feedback telemetry.
        """
        with self._lock:
            rec = self.tracker.get_record(milestone_id)
            entry = self.tracker.get_schedule_entry(milestone_id)

            if not rec:
                rec = ExecutionRecord(milestone_id=milestone_id)

            rec.status = status
            rec.error_context = error_context
            self.tracker.update_record(rec)

            if entry:
                feedback = self.feedback_emitter.record_milestone_feedback(entry, rec)
                logger.info("Milestone %s completed with status '%s'", milestone_id, status)
                return feedback
            return None

    def run_monitor_tick(self, current_timestamp: Optional[float] = None) -> list[DriftEvent]:
        """
        Executes a monitor tick: evaluates drift predicates and triggers re-scheduling
        if any critical drift event is detected.
        """
        now = current_timestamp if current_timestamp is not None else time.time()

        with self._lock:
            schedule = self.tracker._schedule
            records = self.tracker._records

            # Evaluate drift conditions
            detected_events = self.drift_detector.evaluate_drift(schedule, records, current_timestamp=now)
            self._active_drift_events = copy.deepcopy(detected_events)
            self._last_tick_time = now

            critical_events = [
                e for e in detected_events if e.severity == DriftSeverity.CRITICAL.value
            ]

            if critical_events:
                logger.warning(
                    "Monitor tick detected %d critical drift events; initiating re-scheduling cascade",
                    len(critical_events),
                )
                revised_schedule, deviation = self.cascade_controller.execute_cascade(
                    current_schedule=schedule,
                    records=records,
                    drift_events=critical_events,
                )
                # Atomically apply revised schedule to tracker
                self.tracker.set_schedule(revised_schedule)

                # Emit cascade feedback
                for crit in critical_events:
                    self.feedback_emitter.record_reschedule_feedback(crit, deviation, timestamp=now)

            return detected_events

    def get_monitor_snapshot(self) -> dict[str, Any]:
        """
        Exposes a thread-safe snapshot of execution state for the Web Cockpit UI.
        """
        with self._lock:
            conformance = self.tracker.compute_aggregate_conformance()
            deviation_stats = self.tracker.get_cumulative_deviation_stats()

            statuses: dict[str, str] = {
                m_id: rec.status for m_id, rec in self.tracker._records.items()
            }

            active_events = [e.to_dict() for e in self._active_drift_events]

            return {
                "conformance_score": round(conformance, 4),
                "active_drift_events": active_events,
                "reschedule_count": self.cascade_controller.reschedule_count,
                "milestone_statuses": statuses,
                "cumulative_deviation_stats": deviation_stats,
                "last_tick_time": self._last_tick_time,
            }


# ==============================================================================
# Unit Test Suite
# ==============================================================================


class TestPipelineExecutionMonitor(unittest.TestCase):
    """Publication-grade unit verification suite for PipelineExecutionMonitor."""

    def setUp(self) -> None:
        self.mock_schedule = [
            ScheduleEntry(
                milestone_id="m1_db_migration",
                repo_name="core_service",
                planned_start_time=100.0,
                planned_duration_s=10.0,
                allocated_tokens=1000,
                assigned_account_id="acc_pro_1",
                priority_weight=2.0,
                dependency_predecessors=[],
            ),
            ScheduleEntry(
                milestone_id="m2_api_gateway",
                repo_name="core_service",
                planned_start_time=110.0,
                planned_duration_s=20.0,
                allocated_tokens=2000,
                assigned_account_id="acc_pro_2",
                priority_weight=1.5,
                dependency_predecessors=["m1_db_migration"],
            ),
            ScheduleEntry(
                milestone_id="m3_analytics_job",
                repo_name="analytics_service",
                planned_start_time=130.0,
                planned_duration_s=30.0,
                allocated_tokens=3000,
                assigned_account_id="acc_pro_3",
                priority_weight=1.0,
                dependency_predecessors=["m2_api_gateway"],
            ),
        ]

    def test_drift_detection_temporal(self) -> None:
        """Validates TemporalDrift fires correctly at threshold boundaries."""
        monitor = PipelineExecutionMonitor(alpha_drift=1.5)
        monitor.load_schedule(self.mock_schedule)

        # Milestone 1 running within acceptable alpha bounds
        monitor.record_milestone_start("m1_db_migration", actual_start_time=100.0)
        monitor.record_milestone_update("m1_db_migration", elapsed_s=12.0, tokens_consumed=500)
        events = monitor.run_monitor_tick(current_timestamp=112.0)
        temporal_events = [e for e in events if e.drift_type == DriftType.TEMPORAL.value]
        self.assertEqual(len(temporal_events), 0)

        # Milestone 1 exceeds warning threshold (1.15 * 10 = 11.5s), elapsed = 13s
        monitor.record_milestone_update("m1_db_migration", elapsed_s=13.0, tokens_consumed=500)
        events = monitor.run_monitor_tick(current_timestamp=113.0)
        temporal_events = [e for e in events if e.drift_type == DriftType.TEMPORAL.value]
        self.assertEqual(len(temporal_events), 1)
        self.assertEqual(temporal_events[0].severity, DriftSeverity.WARNING.value)

        # Milestone 1 breaches critical threshold (1.5 * 10 = 15.0s), elapsed = 16.0s
        monitor.record_milestone_update("m1_db_migration", elapsed_s=16.0, tokens_consumed=700)
        events = monitor.run_monitor_tick(current_timestamp=116.0)
        crit_events = [
            e for e in events
            if e.drift_type == DriftType.TEMPORAL.value and e.severity == DriftSeverity.CRITICAL.value
        ]
        self.assertEqual(len(crit_events), 1)
        self.assertEqual(crit_events[0].milestone_id, "m1_db_migration")
        self.assertEqual(crit_events[0].observed_value, 16.0)

    def test_drift_detection_quota(self) -> None:
        """Validates QuotaDrift fires when active token burn rate exceeds beta factor."""
        monitor = PipelineExecutionMonitor(beta_drift=1.3)
        monitor.load_schedule(self.mock_schedule)

        monitor.record_milestone_start("m1_db_migration", actual_start_time=100.0)
        # Planned burn rate: 1000 tokens / 10s = 100 tokens/s
        # Observed rate: 1500 tokens / 10s = 150 tokens/s (150 > 1.3 * 100 = 130)
        monitor.record_milestone_update("m1_db_migration", elapsed_s=10.0, tokens_consumed=1500)

        events = monitor.run_monitor_tick(current_timestamp=110.0)
        quota_events = [
            e for e in events
            if e.drift_type == DriftType.QUOTA.value and e.severity == DriftSeverity.CRITICAL.value
        ]
        self.assertEqual(len(quota_events), 1)
        self.assertAlmostEqual(quota_events[0].observed_value, 150.0, places=2)
        self.assertAlmostEqual(quota_events[0].threshold_value, 130.0, places=2)

    def test_drift_detection_topological(self) -> None:
        """Validates TopologicalDrift fires when a dependency predecessor fails."""
        monitor = PipelineExecutionMonitor()
        monitor.load_schedule(self.mock_schedule)

        # Mark predecessor m1 as failed
        monitor.record_milestone_completion("m1_db_migration", status=ExecutionStatus.FAILED.value)

        # Dependent milestone m2 is still pending
        events = monitor.run_monitor_tick(current_timestamp=110.0)
        topo_events = [e for e in events if e.drift_type == DriftType.TOPOLOGICAL.value]

        self.assertTrue(len(topo_events) >= 1)
        affected_ids = [e.milestone_id for e in topo_events]
        self.assertIn("m2_api_gateway", affected_ids)
        self.assertEqual(topo_events[0].severity, DriftSeverity.CRITICAL.value)

    def test_rescheduling_cascade(self) -> None:
        """Validates that critical drift cascades produce a revised schedule preserving in-flight tasks."""
        # Create mock capacity planner and execution scheduler
        capacity_recomputed = {"recomputed": True}

        def mock_capacity_planner(deviation: ScheduleDeviation) -> dict[str, Any]:
            self.assertEqual(len(deviation.in_flight), 1)
            self.assertEqual(len(deviation.failed), 1)
            return capacity_recomputed

        def mock_execution_scheduler(allocation: dict[str, Any], remaining: list[ScheduleEntry]) -> dict[str, ScheduleEntry]:
            rescheduled = {}
            for entry in remaining:
                updated = copy.deepcopy(entry)
                updated.planned_duration_s += 5.0  # modified allocation
                rescheduled[entry.milestone_id] = updated
            return rescheduled

        monitor = PipelineExecutionMonitor(
            capacity_planner=mock_capacity_planner,
            execution_scheduler=mock_execution_scheduler,
            alpha_drift=1.5,
        )
        monitor.load_schedule(self.mock_schedule)

        # Mark m1 as failed
        monitor.record_milestone_completion("m1_db_migration", status=ExecutionStatus.FAILED.value)

        # Start m2 (in-flight)
        monitor.record_milestone_start("m2_api_gateway", actual_start_time=110.0)
        monitor.record_milestone_update("m2_api_gateway", elapsed_s=5.0, tokens_consumed=400)

        # Monitor tick should detect topological drift on m2 or temporal on other and trigger cascade
        snapshot_before = monitor.get_monitor_snapshot()
        self.assertEqual(snapshot_before["reschedule_count"], 0)

        events = monitor.run_monitor_tick(current_timestamp=115.0)
        snapshot_after = monitor.get_monitor_snapshot()

        self.assertEqual(snapshot_after["reschedule_count"], 1)
        # Check that m2 is preserved in-flight
        self.assertEqual(snapshot_after["milestone_statuses"]["m2_api_gateway"], ExecutionStatus.RUNNING.value)
        # Check that m3 was rescheduled with modified duration
        m3_entry = monitor.tracker.get_schedule_entry("m3_analytics_job")
        self.assertIsNotNone(m3_entry)
        self.assertEqual(m3_entry.planned_duration_s, 35.0)

    def test_conformance_score_monotonicity(self) -> None:
        """Validates that aggregate conformance score degrades monotonically with increasing deviation."""
        tracker = ScheduleConformanceTracker()
        schedule_map = {e.milestone_id: e for e in self.mock_schedule}
        tracker.set_schedule(schedule_map)

        # Perfect execution: elapsed == planned_duration
        tracker.update_record(
            ExecutionRecord(
                milestone_id="m1_db_migration",
                elapsed_s=10.0,
                status=ExecutionStatus.SUCCEEDED.value,
            )
        )
        score_perfect = tracker.compute_aggregate_conformance()
        self.assertAlmostEqual(score_perfect, 1.0)

        # Small deviation: elapsed 12.0s (planned 10.0s -> delta = 0.2)
        tracker.update_record(
            ExecutionRecord(
                milestone_id="m1_db_migration",
                elapsed_s=12.0,
                status=ExecutionStatus.RUNNING.value,
            )
        )
        score_small_dev = tracker.compute_aggregate_conformance()
        self.assertLess(score_small_dev, score_perfect)

        # Large deviation: elapsed 20.0s (planned 10.0s -> delta = 1.0)
        tracker.update_record(
            ExecutionRecord(
                milestone_id="m1_db_migration",
                elapsed_s=20.0,
                status=ExecutionStatus.RUNNING.value,
            )
        )
        score_large_dev = tracker.compute_aggregate_conformance()
        self.assertLess(score_large_dev, score_small_dev)

        # Extreme deviation: clamped at 0.0
        tracker.update_record(
            ExecutionRecord(
                milestone_id="m1_db_migration",
                elapsed_s=100.0,
                status=ExecutionStatus.RUNNING.value,
            )
        )
        score_extreme = tracker.compute_aggregate_conformance()
        self.assertEqual(score_extreme, 0.0)

    def test_feedback_telemetry_emitter(self) -> None:
        """Validates accurate observed-vs-planned deltas in FeedbackRecords and buffer flush."""
        monitor = PipelineExecutionMonitor()
        monitor.load_schedule(self.mock_schedule)

        monitor.record_milestone_start("m1_db_migration", actual_start_time=100.0)
        monitor.record_milestone_update("m1_db_migration", elapsed_s=14.5, tokens_consumed=1250)
        feedback = monitor.record_milestone_completion(
            "m1_db_migration",
            status=ExecutionStatus.SUCCEEDED.value,
        )

        self.assertIsNotNone(feedback)
        self.assertEqual(feedback.milestone_id, "m1_db_migration")
        self.assertEqual(feedback.planned_duration, 10.0)
        self.assertEqual(feedback.actual_duration, 14.5)
        self.assertEqual(feedback.planned_tokens, 1000)
        self.assertEqual(feedback.actual_tokens, 1250)
        self.assertEqual(feedback.outcome, ExecutionStatus.SUCCEEDED.value)

        # Verify buffer and flush
        self.assertEqual(monitor.feedback_emitter.buffer_size(), 1)
        flushed = monitor.feedback_emitter.flush_feedback()
        self.assertEqual(len(flushed), 1)
        self.assertEqual(flushed[0].milestone_id, "m1_db_migration")
        self.assertEqual(monitor.feedback_emitter.buffer_size(), 0)


if __name__ == "__main__":
    unittest.main()