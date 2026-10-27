"""
Runtime Execution Governor & Adaptive Mid-Flight Re-Scheduling Engine
Module: tasks/runtime_execution_governor.py

Provides active supervisory control over in-flight milestone executions. Sits
between the pre-dispatch Feasibility Gate and the post-hoc Steering Memory,
continuously evaluating token burn rates, quota headroom, and dependency status
to execute dynamic adjustments: CONTINUE, THROTTLE, PREEMPT, or PROMOTE.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import Enum
import logging
import math
import threading
import time
from typing import Any, Dict, List, Optional

# Optional OpenTelemetry integration with fallback
try:
    from opentelemetry import trace
    tracer = trace.get_tracer("hermes.governor")
except ImportError:
    class _NoOpSpan:
        def __enter__(self) -> "_NoOpSpan":
            return self

        def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            pass

        def set_attribute(self, key: str, value: Any) -> None:
            pass

        def set_attributes(self, attributes: Dict[str, Any]) -> None:
            pass

        def record_exception(self, exception: Exception) -> None:
            pass

    class _NoOpTracer:
        def start_span(self, name: str, *args: Any, **kwargs: Any) -> _NoOpSpan:
            return _NoOpSpan()

        def start_as_current_span(self, name: str, *args: Any, **kwargs: Any) -> _NoOpSpan:
            return _NoOpSpan()

    tracer = _NoOpTracer()

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data Classes & Enums
# ---------------------------------------------------------------------------

class SlotStatus(str, Enum):
    ACTIVE = "ACTIVE"
    THROTTLED = "THROTTLED"
    PREEMPTED = "PREEMPTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class ActionType(str, Enum):
    CONTINUE = "CONTINUE"
    THROTTLE = "THROTTLE"
    PREEMPT = "PREEMPT"
    PROMOTE = "PROMOTE"


@dataclass
class CostEstimate:
    predicted_tokens: int
    confidence_upper: int
    predicted_duration: float = 60.0


@dataclass
class PartialBurn:
    observed_tokens: int
    observed_duration: float
    estimated_completion_ratio: float = 0.5


@dataclass
class TelemetrySnapshot:
    timestamp: float
    cumulative_tokens: int
    burn_rate: float
    anomaly_score: float
    circuit_breaker_state: str


@dataclass
class ExecutionSlot:
    milestone_id: str
    start_time: float
    predicted_cost: CostEstimate
    observed_tokens: int = 0
    observed_duration: float = 0.0
    status: SlotStatus = SlotStatus.ACTIVE
    telemetry_snapshots: List[TelemetrySnapshot] = field(default_factory=list)
    estimated_total_duration: float = 60.0


@dataclass
class GovernorAction:
    action_type: ActionType
    target_slot_id: str
    reason: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BurnRateMetrics:
    tokens_per_second: float
    cumulative_tokens: int
    projected_total: float
    overrun_ratio: float


@dataclass
class QuotaHeadroomResult:
    active_account: str
    remaining_tokens: int
    seconds_to_exhaustion: float
    next_rebalance_in: float
    sufficient: bool


# ---------------------------------------------------------------------------
# Concurrency Synchronization Helpers
# ---------------------------------------------------------------------------

class ReaderWriterLock:
    """Reader-writer lock using condition variables favoring waiting writers."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._readers = 0
        self._writers = 0
        self._writers_waiting = 0

    class _ReaderContext:
        def __init__(self, rwlock: "ReaderWriterLock") -> None:
            self.rwlock = rwlock

        def __enter__(self) -> "ReaderWriterLock._ReaderContext":
            self.rwlock.acquire_read()
            return self

        def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            self.rwlock.release_read()

    class _WriterContext:
        def __init__(self, rwlock: "ReaderWriterLock") -> None:
            self.rwlock = rwlock

        def __enter__(self) -> "ReaderWriterLock._WriterContext":
            self.rwlock.acquire_write()
            return self

        def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            self.rwlock.release_write()

    def read_lock(self) -> "_ReaderContext":
        return self._ReaderContext(self)

    def write_lock(self) -> "_WriterContext":
        return self._WriterContext(self)

    def acquire_read(self) -> None:
        with self._cond:
            while self._writers > 0 or self._writers_waiting > 0:
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
            while self._readers > 0 or self._writers > 0:
                self._cond.wait()
            self._writers_waiting -= 1
            self._writers += 1

    def release_write(self) -> None:
        with self._cond:
            self._writers -= 1
            self._cond.notify_all()


# ---------------------------------------------------------------------------
# Execution Governor Engine
# ---------------------------------------------------------------------------

class ExecutionGovernor:
    """
    Supervises live in-flight milestone executions. Calculates real-time token
    consumption trajectories, verifies quota headroom against dynamic token pools,
    updates milestone cost estimates via Bayesian conjugate updates, and acts
    mid-flight to throttle or preempt overrunning tasks.
    """

    def __init__(
        self,
        anomaly_detector: Any,
        quota_engine: Any,
        dep_scheduler: Any,
        cost_estimator: Any,
        memory_store: Any,
        tick_interval_s: float = 15.0,
        max_slots: int = 4,
    ) -> None:
        self.anomaly_detector = anomaly_detector
        self.quota_engine = quota_engine
        self.dep_scheduler = dep_scheduler
        self.cost_estimator = cost_estimator
        self.memory_store = memory_store
        self.tick_interval_s = tick_interval_s
        self.max_slots = max_slots

        self.slot_registry: Dict[str, ExecutionSlot] = {}
        self.holding_queue: List[ExecutionSlot] = []
        self.rwlock = ReaderWriterLock()
        self.tracer = tracer
        self._running = False

    async def start_supervision(self, active_slots: List[ExecutionSlot]) -> None:
        """Main supervisor loop running tick evaluation at tick_interval_s."""
        with self.rwlock.write_lock():
            for slot in active_slots:
                self.slot_registry[slot.milestone_id] = slot

        logger.info(
            "Starting execution supervision with %d active slots. Interval: %.1fs",
            len(active_slots),
            self.tick_interval_s,
        )
        self._running = True
        try:
            while self._running:
                actions = self.tick()
                for action in actions:
                    logger.info(
                        "[Governor Tick] %s -> Slot '%s': %s",
                        action.action_type.value,
                        action.target_slot_id,
                        action.reason,
                    )
                await asyncio.sleep(self.tick_interval_s)
        except asyncio.CancelledError:
            logger.info("Supervision loop cancelled.")
        finally:
            self._running = False

    def stop(self) -> None:
        """Stop the supervision loop."""
        self._running = False

    def record_telemetry(self, slot_id: str, snapshot: TelemetrySnapshot) -> None:
        """Thread-safe telemetry injection for in-flight slots."""
        with self.rwlock.write_lock():
            slot = self.slot_registry.get(slot_id)
            if slot:
                slot.telemetry_snapshots.append(snapshot)
                slot.observed_tokens = snapshot.cumulative_tokens
                slot.observed_duration = max(0.001, snapshot.timestamp - slot.start_time)
            else:
                logger.warning("Telemetry received for unknown slot ID '%s'", slot_id)

    def tick(self) -> List[GovernorAction]:
        """
        Evaluate all in-flight active slots under read lock, decide actions,
        and acquire write lock to mutate states, perform re-queuing and promotions.
        """
        with self.tracer.start_span("governor.tick") as span:
            # 1. Read Lock Phase: Read active slots and determine actions
            active_slots: List[ExecutionSlot] = []
            with self.rwlock.read_lock():
                for slot in self.slot_registry.values():
                    if slot.status in (SlotStatus.ACTIVE, SlotStatus.THROTTLED):
                        active_slots.append(slot)

            actions: List[GovernorAction] = []
            for slot in active_slots:
                with self.tracer.start_span("governor.evaluate_slot") as child_span:
                    child_span.set_attribute("slot.id", slot.milestone_id)
                    action = self._evaluate_slot(slot)
                    actions.append(action)

            # Check DAG dependencies for promotions into vacant slots
            promo_actions = self._check_dependency_promotions()
            actions.extend(promo_actions)

            # 2. Write Lock Phase: Apply mutations & enrich outcomes
            with self.rwlock.write_lock():
                for action in actions:
                    slot = self.slot_registry.get(action.target_slot_id)
                    if action.action_type == ActionType.PREEMPT and slot:
                        slot.status = SlotStatus.PREEMPTED
                        if "updated_cost_estimate" in action.metadata:
                            u_cost = action.metadata["updated_cost_estimate"]
                            slot.predicted_cost = CostEstimate(
                                predicted_tokens=u_cost["predicted_tokens"],
                                confidence_upper=u_cost["confidence_upper"],
                                predicted_duration=u_cost.get(
                                    "predicted_duration", slot.predicted_cost.predicted_duration
                                ),
                            )
                        self._emit_enriched_outcome(slot, action)
                        # Re-queue into holding queue with updated cost
                        if slot not in self.holding_queue:
                            self.holding_queue.append(slot)

                    elif action.action_type == ActionType.THROTTLE and slot:
                        slot.status = SlotStatus.THROTTLED
                        if self.anomaly_detector and hasattr(self.anomaly_detector, "signal_throttle"):
                            try:
                                self.anomaly_detector.signal_throttle(
                                    slot.milestone_id, action.metadata.get("backoff_factor", 1.5)
                                )
                            except Exception as ex:
                                logger.warning("Anomaly detector throttle signal failed: %s", ex)

                    elif action.action_type == ActionType.PROMOTE:
                        target_id = action.target_slot_id
                        for idx, h_slot in enumerate(self.holding_queue):
                            if h_slot.milestone_id == target_id:
                                h_slot.status = SlotStatus.ACTIVE
                                h_slot.start_time = time.time()
                                self.slot_registry[target_id] = h_slot
                                self.holding_queue.pop(idx)
                                break

            span.set_attribute("slot_count", len(active_slots))
            span.set_attribute("actions_taken", len(actions))
            return actions

    def _evaluate_slot(self, slot: ExecutionSlot) -> GovernorAction:
        """
        Evaluate individual slot burn metrics against predicted parameters and quota headroom.
        """
        burn = self._compute_burn_rate(slot)
        headroom = self._check_quota_headroom(slot)

        # Preempt: Overrun > 2.0x or insufficient token pool quota
        if burn.overrun_ratio > 2.0 or not headroom.sufficient:
            comp_ratio = min(max(slot.observed_duration / max(slot.estimated_total_duration, 1.0), 0.05), 0.95)
            partial = PartialBurn(
                observed_tokens=slot.observed_tokens,
                observed_duration=slot.observed_duration,
                estimated_completion_ratio=comp_ratio,
            )
            posterior_cost = self._bayesian_cost_update(slot.predicted_cost, partial)
            reasons = []
            if burn.overrun_ratio > 2.0:
                reasons.append(f"Overrun ratio {burn.overrun_ratio:.2f} > 2.0 threshold")
            if not headroom.sufficient:
                reasons.append(
                    f"Insufficient quota headroom on {headroom.active_account} "
                    f"({headroom.remaining_tokens} tokens remain, "
                    f"exhaustion in {headroom.seconds_to_exhaustion:.1f}s)"
                )
            return GovernorAction(
                action_type=ActionType.PREEMPT,
                target_slot_id=slot.milestone_id,
                reason="; ".join(reasons),
                metadata={
                    "burn_metrics": burn.__dict__,
                    "headroom": headroom.__dict__,
                    "updated_cost_estimate": posterior_cost.__dict__,
                },
            )

        # Throttle: Overrun > 1.5x
        if burn.overrun_ratio > 1.5:
            backoff_factor = min(burn.overrun_ratio, 3.0)
            return GovernorAction(
                action_type=ActionType.THROTTLE,
                target_slot_id=slot.milestone_id,
                reason=f"Overrun ratio {burn.overrun_ratio:.2f} > 1.5 backoff threshold",
                metadata={
                    "burn_metrics": burn.__dict__,
                    "headroom": headroom.__dict__,
                    "backoff_factor": backoff_factor,
                },
            )

        # Nominal execution
        return GovernorAction(
            action_type=ActionType.CONTINUE,
            target_slot_id=slot.milestone_id,
            reason="Execution trajectory within bounds",
            metadata={"burn_metrics": burn.__dict__, "headroom": headroom.__dict__},
        )

    def _compute_burn_rate(self, slot: ExecutionSlot) -> BurnRateMetrics:
        """Calculates tokens/sec, extrapolated completion, and predicted overrun ratio."""
        duration = max(slot.observed_duration, 0.001)
        tokens_per_second = slot.observed_tokens / duration
        total_duration = max(slot.estimated_total_duration, duration)
        projected_total = tokens_per_second * total_duration
        predicted_tokens = max(slot.predicted_cost.predicted_tokens, 1)
        overrun_ratio = projected_total / predicted_tokens

        return BurnRateMetrics(
            tokens_per_second=tokens_per_second,
            cumulative_tokens=slot.observed_tokens,
            projected_total=projected_total,
            overrun_ratio=overrun_ratio,
        )

    def _check_quota_headroom(self, slot: ExecutionSlot) -> QuotaHeadroomResult:
        """Calculates time-to-exhaustion and available quota against rebalance intervals."""
        active_account = "gemini-pool-primary"
        remaining_tokens = 250_000
        next_rebalance_in = 300.0

        if self.quota_engine and hasattr(self.quota_engine, "get_account_status"):
            try:
                status = self.quota_engine.get_account_status()
                if isinstance(status, dict):
                    active_account = status.get("active_account", active_account)
                    remaining_tokens = status.get("remaining_tokens", remaining_tokens)
                    next_rebalance_in = status.get("next_rebalance_in", next_rebalance_in)
            except Exception as ex:
                logger.warning("Error reading quota status from quota_engine: %s", ex)

        burn = self._compute_burn_rate(slot)
        if burn.tokens_per_second > 0:
            seconds_to_exhaustion = remaining_tokens / burn.tokens_per_second
        else:
            seconds_to_exhaustion = float("inf")

        remaining_milestone_time = max(0.0, slot.estimated_total_duration - slot.observed_duration)
        needed_tokens = max(0.0, burn.projected_total - slot.observed_tokens)

        sufficient = (remaining_tokens >= needed_tokens) or (
            seconds_to_exhaustion >= remaining_milestone_time
        )

        return QuotaHeadroomResult(
            active_account=active_account,
            remaining_tokens=remaining_tokens,
            seconds_to_exhaustion=seconds_to_exhaustion,
            next_rebalance_in=next_rebalance_in,
            sufficient=sufficient,
        )

    def _bayesian_cost_update(self, prior: CostEstimate, observed_partial: PartialBurn) -> CostEstimate:
        """
        Updates cost estimate using Gaussian conjugate updating:
        Prior ~ N(mu_prior, sigma_prior^2)
        Likelihood ~ N(mu_obs, sigma_obs^2)
        Posterior precision = 1/sigma_prior^2 + 1/sigma_obs^2
        """
        prior_mu = float(prior.predicted_tokens)
        prior_spread = float(prior.confidence_upper - prior.predicted_tokens)
        prior_sigma = (prior_spread / 1.96) if prior_spread > 0 else max(prior_mu * 0.2, 20.0)
        prior_sigma = max(prior_sigma, 1.0)

        completion_ratio = max(0.01, min(0.99, observed_partial.estimated_completion_ratio))
        observed_mu = float(observed_partial.observed_tokens) / completion_ratio

        # Early partial observations carry higher variance
        observed_sigma = max(
            observed_mu * math.sqrt((1.0 - completion_ratio) / completion_ratio),
            10.0,
        )

        prec_prior = 1.0 / (prior_sigma ** 2)
        prec_obs = 1.0 / (observed_sigma ** 2)
        prec_post = prec_prior + prec_obs

        post_mu = (prior_mu * prec_prior + observed_mu * prec_obs) / prec_post
        post_sigma = 1.0 / math.sqrt(prec_post)

        new_predicted_tokens = int(round(post_mu))
        new_confidence_upper = int(round(post_mu + 1.96 * post_sigma))

        return CostEstimate(
            predicted_tokens=new_predicted_tokens,
            confidence_upper=new_confidence_upper,
            predicted_duration=prior.predicted_duration,
        )

    def _check_dependency_promotions(self) -> List[GovernorAction]:
        """Scans holding queue for unblocked milestones when active slots have capacity."""
        actions: List[GovernorAction] = []
        with self.rwlock.read_lock():
            active_count = sum(
                1 for s in self.slot_registry.values() if s.status == SlotStatus.ACTIVE
            )
            available_capacity = max(0, self.max_slots - active_count)

        if available_capacity <= 0 or not self.holding_queue:
            return actions

        for candidate in list(self.holding_queue):
            target_id = candidate.milestone_id
            is_unblocked = True

            if self.dep_scheduler and hasattr(self.dep_scheduler, "is_dependency_resolved"):
                try:
                    is_unblocked = self.dep_scheduler.is_dependency_resolved(target_id)
                except Exception as ex:
                    logger.warning("Dependency check error for '%s': %s", target_id, ex)
                    is_unblocked = False

            if is_unblocked:
                actions.append(
                    GovernorAction(
                        action_type=ActionType.PROMOTE,
                        target_slot_id=target_id,
                        reason="Prerequisite dependencies resolved; execution slot available",
                        metadata={"queue_slot": target_id},
                    )
                )
                available_capacity -= 1
                if available_capacity <= 0:
                    break

        return actions

    def _emit_enriched_outcome(self, slot: ExecutionSlot, action: GovernorAction) -> None:
        """Packages runtime telemetry trajectory and stores outcome in Steering Memory."""
        trajectory = [
            {
                "timestamp": snap.timestamp,
                "cumulative_tokens": snap.cumulative_tokens,
                "burn_rate": snap.burn_rate,
                "anomaly_score": snap.anomaly_score,
                "circuit_breaker_state": snap.circuit_breaker_state,
            }
            for snap in slot.telemetry_snapshots
        ]
        outcome = {
            "milestone_id": slot.milestone_id,
            "action_type": action.action_type.value,
            "reason": action.reason,
            "status": slot.status.value,
            "observed_tokens": slot.observed_tokens,
            "observed_duration": slot.observed_duration,
            "predicted_cost": slot.predicted_cost.__dict__,
            "burn_trajectory": trajectory,
            "metadata": action.metadata,
        }
        if self.memory_store and hasattr(self.memory_store, "append_outcome"):
            try:
                self.memory_store.append_outcome(outcome)
                logger.debug("Emitted outcome for '%s' to memory store", slot.milestone_id)
            except Exception as ex:
                logger.error("Failed to append outcome to memory store: %s", ex)
        else:
            logger.debug("Memory store not configured; outcome prepared: %s", outcome["milestone_id"])


# ---------------------------------------------------------------------------
# Demonstration Simulation
# ---------------------------------------------------------------------------

async def main() -> None:
    """Demonstrates governor tick progression: CONTINUE -> THROTTLE -> PREEMPT."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    class MockMemoryStore:
        def __init__(self) -> None:
            self.outcomes: List[Dict[str, Any]] = []

        def append_outcome(self, outcome: Dict[str, Any]) -> None:
            self.outcomes.append(outcome)
            logger.info("-> [MemoryStore] Recorded outcome for %s", outcome["milestone_id"])

    class MockQuotaEngine:
        def get_account_status(self) -> Dict[str, Any]:
            return {
                "active_account": "gemini-3.8-flash-tier1",
                "remaining_tokens": 150_000,
                "next_rebalance_in": 120.0,
            }

    class MockDepScheduler:
        def is_dependency_resolved(self, milestone_id: str) -> bool:
            return True

    memory = MockMemoryStore()
    quota = MockQuotaEngine()
    scheduler = MockDepScheduler()

    governor = ExecutionGovernor(
        anomaly_detector=None,
        quota_engine=quota,
        dep_scheduler=scheduler,
        cost_estimator=None,
        memory_store=memory,
        tick_interval_s=1.0,
        max_slots=2,
    )

    # Initial Slot with predicted 2,000 tokens over 60s
    slot_id = "ms-optimization-v3"
    slot = ExecutionSlot(
        milestone_id=slot_id,
        start_time=time.time(),
        predicted_cost=CostEstimate(predicted_tokens=2000, confidence_upper=2400, predicted_duration=60.0),
        estimated_total_duration=60.0,
    )

    governor.slot_registry[slot_id] = slot

    print("\n=== RUNTIME EXECUTION GOVERNOR SIMULATION ===")
    print("Slot: ms-optimization-v3 (Predicted: 2000 tokens, 60s total)")

    # Tick 1: Nominal Burn (Overrun ~0.9x -> CONTINUE)
    print("\n--- TICK 1 (Simulating nominal consumption) ---")
    governor.record_telemetry(
        slot_id,
        TelemetrySnapshot(
            timestamp=slot.start_time + 15.0,
            cumulative_tokens=450,
            burn_rate=30.0,
            anomaly_score=0.05,
            circuit_breaker_state="CLOSED",
        ),
    )
    actions_t1 = governor.tick()
    for act in actions_t1:
        print(f"Result Action: {act.action_type.value} | Reason: {act.reason}")

    # Tick 2: Accelerated Burn (Overrun ~1.7x -> THROTTLE)
    print("\n--- TICK 2 (Simulating accelerated consumption spike) ---")
    governor.record_telemetry(
        slot_id,
        TelemetrySnapshot(
            timestamp=slot.start_time + 30.0,
            cumulative_tokens=1700,
            burn_rate=56.6,
            anomaly_score=0.45,
            circuit_breaker_state="CLOSED",
        ),
    )
    actions_t2 = governor.tick()
    for act in actions_t2:
        print(
            f"Result Action: {act.action_type.value} | "
            f"Backoff Factor: {act.metadata.get('backoff_factor', 'N/A')} | "
            f"Reason: {act.reason}"
        )

    # Tick 3: Runaway Overrun (Overrun ~2.6x -> PREEMPT & Bayesian Update)
    print("\n--- TICK 3 (Simulating severe runaway overrun) ---")
    governor.record_telemetry(
        slot_id,
        TelemetrySnapshot(
            timestamp=slot.start_time + 40.0,
            cumulative_tokens=3500,
            burn_rate=87.5,
            anomaly_score=0.89,
            circuit_breaker_state="HALF_OPEN",
        ),
    )
    actions_t3 = governor.tick()
    for act in actions_t3:
        print(f"Result Action: {act.action_type.value} | Reason: {act.reason}")
        if "updated_cost_estimate" in act.metadata:
            est = act.metadata["updated_cost_estimate"]
            print(
                f"Bayesian Posterior Cost Estimate: "
                f"predicted_tokens={est['predicted_tokens']}, "
                f"confidence_upper={est['confidence_upper']}"
            )

    print("\nSimulation complete. Holding Queue Count:", len(governor.holding_queue))
    print("Memory Store Outcomes Emitted:", len(memory.outcomes))


if __name__ == "__main__":
    asyncio.run(main())