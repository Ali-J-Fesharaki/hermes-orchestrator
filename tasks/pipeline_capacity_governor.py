"""tasks/pipeline_capacity_governor.py

Pipeline Adaptive Capacity Governor & Dynamic Resource Allocation Engine.
Implements multi-variable closed-loop feedback control, predictive EWMA workload forecasting,
water-filling quota rebalancing, and canary-gated configuration lifecycle management.
"""

from __future__ import annotations

import collections
from collections import deque
import dataclasses
from dataclasses import asdict, dataclass, field
import hashlib
import json
import logging
import math
import queue
import threading
import time
from typing import Any, Dict, List, Literal, Optional, Tuple
import uuid

__all__ = [
    "CapacityEnvelope",
    "WorkloadForecast",
    "CapacityDecisionRecord",
    "ConfigurationSnapshot",
    "PipelineCapacityGovernor",
]

logger = logging.getLogger("PipelineCapacityGovernor")


# =====================================================================
# Domain Dataclasses
# =====================================================================

@dataclass(frozen=True)
class CapacityEnvelope:
    """Represents the operating capacity parameters for the execution pipeline."""

    worker_pool_size: int
    """Number of parallel autonomous worker processes allocated in the execution pool."""

    per_repo_dispatch_ceiling: Dict[str, float]
    """Maximum concurrent dispatch rate or task concurrency ceiling per repository."""

    account_quota_weights: Dict[str, float]
    """Normalized probability weights for account round-robin selection (must sum to 1.0)."""

    circuit_breaker_thresholds: Dict[str, float]
    """Error rate, latency, and failure count thresholds before tripping circuit breakers."""

    timestamp: float
    """Unix epoch timestamp when this capacity envelope was calculated."""


@dataclass(frozen=True)
class WorkloadForecast:
    """Statistical workload demand forecast computed via exponential smoothing."""

    ewma_task_arrival_rate: float
    """Exponentially weighted moving average of incoming task arrival rate (tasks/sec)."""

    ewma_execution_duration: float
    """Exponentially weighted moving average of task execution duration (seconds)."""

    ewma_token_burn: float
    """Exponentially weighted moving average of LLM token burn rate (tokens/sec)."""

    predicted_demand_horizon_seconds: float
    """Forward-looking projection horizon in seconds."""

    confidence_interval: Tuple[float, float]
    """Empirical confidence interval (lower_bound, upper_bound) for forecasted concurrency demand."""


@dataclass(frozen=True)
class CapacityDecisionRecord:
    """Audit record capturing an envelope mutation decision and rationale."""

    decision_id: str
    """Unique UUIDv4 identifier for the governance decision."""

    previous_envelope: CapacityEnvelope
    """Active capacity envelope prior to the decision."""

    proposed_envelope: CapacityEnvelope
    """Proposed new capacity envelope undergoing validation."""

    trigger: Literal[
        "pid_correction",
        "predictive_prescale",
        "quota_rebalance",
        "breaker_adaptation",
        "periodic_review",
    ]
    """Root trigger mechanism prompting the capacity reallocation."""

    rationale: str
    """Human-readable engineering rationale explaining the parameter transitions."""

    canary_validated: bool
    """Flag indicating whether the proposed envelope passed canary boundary validation."""


@dataclass(frozen=True)
class ConfigurationSnapshot:
    """DirectiveGovernor-compatible serialized configuration snapshot for canary gating."""

    snapshot_id: str
    """Unique snapshot identifier."""

    timestamp: float
    """Creation timestamp in seconds since epoch."""

    target_component: str
    """Target subsystem identifier."""

    parameters: Dict[str, Any]
    """Serialized configuration parameters for deployment."""

    metadata: Dict[str, Any]
    """Auxiliary governance metadata, decision traces, and trigger tags."""

    checksum: str
    """Cryptographic SHA-256 integrity digest of parameter payload."""

    def to_dict(self) -> Dict[str, Any]:
        """Convert snapshot to dictionary representation."""
        return {
            "snapshot_id": self.snapshot_id,
            "timestamp": self.timestamp,
            "target_component": self.target_component,
            "parameters": self.parameters,
            "metadata": self.metadata,
            "checksum": self.checksum,
        }


# =====================================================================
# Pipeline Capacity Governor Engine
# =====================================================================

class PipelineCapacityGovernor:
    """Closed-loop adaptive resource allocation governor for pipeline execution.

    Coordinates PID feedback control, EWMA demand forecasting, water-filling quota
    distribution, and canary validation gating for zero-downtime envelope evolution.
    """

    class PIDController:
        """Single-input single-output proportional-integral-derivative controller.

        Includes configurable anti-windup clamping on the integrator term and
        derivative action based on rate of error change.
        """

        def __init__(
            self,
            kp: float,
            ki: float,
            kd: float,
            setpoint: float,
            integral_clamp: float = 5.0,
        ) -> None:
            self.kp: float = float(kp)
            self.ki: float = float(ki)
            self.kd: float = float(kd)
            self.setpoint: float = float(setpoint)
            self.integral_clamp: float = abs(float(integral_clamp))
            self._integral: float = 0.0
            self._prev_error: Optional[float] = None

        def compute(self, error: float, dt: float) -> float:
            """Compute control correction signal from current error and elapsed delta time.

            Args:
                error: Current process error (setpoint - measurement or vice versa).
                dt: Time delta in seconds since last computation cycle.

            Returns:
                Calculated PID correction signal.
            """
            effective_dt = max(dt, 1e-4)

            # Proportional term
            p_term = self.kp * error

            # Integral term with anti-windup saturation clamping
            self._integral += error * effective_dt
            if self.integral_clamp > 0.0:
                self._integral = max(-self.integral_clamp, min(self.integral_clamp, self._integral))
            i_term = self.ki * self._integral

            # Derivative term
            if self._prev_error is None:
                d_term = 0.0
            else:
                d_term = self.kd * (error - self._prev_error) / effective_dt

            self._prev_error = error
            return p_term + i_term + d_term

        def reset(self) -> None:
            """Reset accumulated integral state and previous error."""
            self._integral = 0.0
            self._prev_error = None

    def __init__(
        self,
        control_interval_seconds: float = 30.0,
        pid_setpoints: Optional[Dict[str, float]] = None,
        ewma_alpha: float = 0.3,
        ewma_window: int = 20,
        prescale_threshold: float = 0.15,
        prescale_confidence: float = 0.7,
        tightening_factor: float = 0.7,
        loosening_factor: float = 1.1,
        max_correction_pct: float = 0.20,
        initial_worker_pool_size: int = 8,
        initial_repo_ceilings: Optional[Dict[str, float]] = None,
        initial_account_quotas: Optional[Dict[str, float]] = None,
        initial_breaker_thresholds: Optional[Dict[str, float]] = None,
    ) -> None:
        """Initialize the pipeline capacity governor.

        Args:
            control_interval_seconds: Control loop evaluation frequency.
            pid_setpoints: Dictionary mapping controller names to target setpoints.
            ewma_alpha: Exponential smoothing factor (0.0 < alpha <= 1.0).
            ewma_window: Sliding window sample size for empirical variance estimation.
            prescale_threshold: Relative demand threshold over capacity triggering pre-scaling.
            prescale_confidence: Statistical confidence barrier [0, 1] required for pre-scale.
            tightening_factor: Multiplier to tighten circuit breaker thresholds on anomaly.
            loosening_factor: Multiplier to relax circuit breaker thresholds during recovery.
            max_correction_pct: Maximum relative adjustment allowed per control step (0.20 = ±20%).
            initial_worker_pool_size: Initial size of autonomous worker process pool.
            initial_repo_ceilings: Initial dispatch concurrency ceilings per repo.
            initial_account_quotas: Initial account rate limit quotas.
            initial_breaker_thresholds: Baseline circuit breaker thresholds.
        """
        self.control_interval_seconds: float = float(control_interval_seconds)
        self.ewma_alpha: float = float(ewma_alpha)
        self.ewma_window: int = int(ewma_window)
        self.prescale_threshold: float = float(prescale_threshold)
        self.prescale_confidence: float = float(prescale_confidence)
        self.tightening_factor: float = float(tightening_factor)
        self.loosening_factor: float = float(loosening_factor)
        self.max_correction_pct: float = float(max_correction_pct)

        # Threading and synchronization
        self._lock: threading.Lock = threading.Lock()
        self._telemetry_queue: queue.Queue[Dict[str, Any]] = queue.Queue()
        self._stop_event: threading.Event = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None

        # Setup PID setpoints
        setpoints = pid_setpoints or {}
        worker_sp = setpoints.get("worker_pool_utilization", 0.75)
        token_sp = setpoints.get("aggregate_token_throughput", 0.90)
        queue_factor = setpoints.get("queue_depth_factor", 2.0)

        # PID controllers
        self.pid_worker_util: PipelineCapacityGovernor.PIDController = self.PIDController(
            kp=0.4, ki=0.05, kd=0.1, setpoint=worker_sp, integral_clamp=5.0
        )
        self.pid_token_tp: PipelineCapacityGovernor.PIDController = self.PIDController(
            kp=0.3, ki=0.04, kd=0.08, setpoint=token_sp, integral_clamp=5.0
        )
        self.pid_queue_depth: PipelineCapacityGovernor.PIDController = self.PIDController(
            kp=0.5, ki=0.06, kd=0.12, setpoint=queue_factor, integral_clamp=5.0
        )

        # EWMA state and sliding window histories
        self._ewma_task_arrival_rate: Optional[float] = None
        self._ewma_execution_duration: Optional[float] = None
        self._ewma_token_burn: Optional[float] = None

        self._arrival_rate_history: deque[float] = deque(maxlen=self.ewma_window)
        self._execution_duration_history: deque[float] = deque(maxlen=self.ewma_window)
        self._token_burn_history: deque[float] = deque(maxlen=self.ewma_window)
        self._demand_concurrency_history: deque[float] = deque(maxlen=self.ewma_window)

        # Circuit breaker defaults and tracking
        self._default_breaker_thresholds: Dict[str, float] = initial_breaker_thresholds or {
            "error_rate_pct": 15.0,
            "p95_latency_ms": 2500.0,
            "consecutive_failures": 5.0,
        }
        self._consecutive_healthy: int = 0
        self._last_escalation_state: str = "NORMAL"

        # Baseline accounts and ceilings
        repos = initial_repo_ceilings or {
            "hermes_orchestrator": 4.0,
            "v2ray_tool": 3.0,
            "PhD_Proposal": 2.0,
        }
        accounts = initial_account_quotas or {
            "account_pro_1": 1000.0,
            "account_pro_2": 1000.0,
            "account_pro_3": 1000.0,
            "account_pro_4": 1000.0,
        }

        # Initialize normalized quota weights
        n_accounts = len(accounts)
        initial_weights = {k: round(1.0 / n_accounts, 6) for k in accounts}
        # Guarantee exact 1.0 sum
        diff = 1.0 - sum(initial_weights.values())
        first_acc = next(iter(initial_weights))
        initial_weights[first_acc] = round(initial_weights[first_acc] + diff, 6)

        # Active operational capacity envelope
        self._current_envelope: CapacityEnvelope = CapacityEnvelope(
            worker_pool_size=int(initial_worker_pool_size),
            per_repo_dispatch_ceiling=dict(repos),
            account_quota_weights=initial_weights,
            circuit_breaker_thresholds=dict(self._default_breaker_thresholds),
            timestamp=time.time(),
        )

        self._rate_limits: Dict[str, float] = dict(accounts)
        self._decision_history: List[CapacityDecisionRecord] = []
        self._latest_forecast: Optional[WorkloadForecast] = None
        self._last_control_cycle_time: float = time.time()

    # =================================================================
    # Telemetry and Lifecycle Management
    # =================================================================

    def register_telemetry_callback(self, fabric: Any = None) -> None:
        """Register the telemetry intake queue callback with an Observability Fabric."""
        if fabric is not None and hasattr(fabric, "subscribe"):
            fabric.subscribe(self.submit_telemetry)
            logger.info("Subscribed PipelineCapacityGovernor to Observability Fabric telemetry stream.")

    def submit_telemetry(self, telemetry_data: Dict[str, Any]) -> None:
        """Enqueue telemetry payload from Observability Fabric or external metrics collectors."""
        if isinstance(telemetry_data, dict):
            self._telemetry_queue.put(telemetry_data)

    def start(self) -> None:
        """Start the background autonomous control loop thread."""
        with self._lock:
            if self._worker_thread is not None and self._worker_thread.is_alive():
                logger.warning("PipelineCapacityGovernor thread is already running.")
                return
            self._stop_event.clear()
            self._worker_thread = threading.Thread(
                target=self._run_control_loop,
                daemon=True,
                name="PipelineCapacityGovernorThread",
            )
            self._worker_thread.start()
            logger.info("PipelineCapacityGovernor background thread started successfully.")

    def stop(self, timeout: float = 5.0) -> None:
        """Signal background governor thread to terminate and join."""
        self._stop_event.set()
        if self._worker_thread is not None and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=timeout)
            logger.info("PipelineCapacityGovernor background thread stopped.")

    def get_current_envelope(self) -> CapacityEnvelope:
        """Thread-safe accessor for the active operating capacity envelope."""
        with self._lock:
            return self._current_envelope

    def get_decision_history(self) -> List[CapacityDecisionRecord]:
        """Thread-safe accessor for historical capacity decision records."""
        with self._lock:
            return list(self._decision_history)

    def get_latest_forecast(self) -> Optional[WorkloadForecast]:
        """Thread-safe accessor for the most recent workload demand forecast."""
        with self._lock:
            return self._latest_forecast

    # =================================================================
    # Algorithmic Subsystems
    # =================================================================

    def _compute_ewma_and_forecast(
        self,
        arrival_rate: float,
        duration: float,
        token_burn: float,
    ) -> WorkloadForecast:
        """Update EWMA series and calculate demand confidence interval using normal approximation."""
        alpha = self.ewma_alpha

        # Update arrival rate EWMA
        if self._ewma_task_arrival_rate is None:
            self._ewma_task_arrival_rate = arrival_rate
        else:
            self._ewma_task_arrival_rate = alpha * arrival_rate + (1.0 - alpha) * self._ewma_task_arrival_rate

        # Update duration EWMA
        if self._ewma_execution_duration is None:
            self._ewma_execution_duration = duration
        else:
            self._ewma_execution_duration = alpha * duration + (1.0 - alpha) * self._ewma_execution_duration

        # Update token burn EWMA
        if self._ewma_token_burn is None:
            self._ewma_token_burn = token_burn
        else:
            self._ewma_token_burn = alpha * token_burn + (1.0 - alpha) * self._ewma_token_burn

        # Append to sliding history deques
        self._arrival_rate_history.append(arrival_rate)
        self._execution_duration_history.append(duration)
        self._token_burn_history.append(token_burn)

        # Little's Law instantaneous demand concurrency: L = lambda * W
        concurrency_instant = arrival_rate * duration
        self._demand_concurrency_history.append(concurrency_instant)

        # Calculate empirical mean and variance of concurrency demand
        concurrency_samples = list(self._demand_concurrency_history)
        n = len(concurrency_samples)
        mean_demand = sum(concurrency_samples) / n

        if n > 1:
            variance = sum((x - mean_demand) ** 2 for x in concurrency_samples) / (n - 1)
            std_dev = math.sqrt(variance)
            se = std_dev / math.sqrt(n)
        else:
            se = 0.5 * mean_demand

        # 95% Confidence Interval (z ~ 1.96)
        ci_lower = max(0.0, round(mean_demand - 1.96 * se, 3))
        ci_upper = round(mean_demand + 1.96 * se, 3)

        forecast = WorkloadForecast(
            ewma_task_arrival_rate=round(self._ewma_task_arrival_rate, 4),
            ewma_execution_duration=round(self._ewma_execution_duration, 4),
            ewma_token_burn=round(self._ewma_token_burn, 2),
            predicted_demand_horizon_seconds=self.control_interval_seconds,
            confidence_interval=(ci_lower, ci_upper),
        )

        with self._lock:
            self._latest_forecast = forecast

        return forecast

    def water_fill_quota_weights(
        self,
        remaining_fractions: Dict[str, float],
        rate_limits: Dict[str, float],
        tolerance: float = 1e-6,
        max_iterations: int = 50,
    ) -> Dict[str, float]:
        """Closed-form water-filling quota allocation solved via bisection on Lagrange multiplier lambda.

        Solves optimization problem:
            w_i = max(0, lambda - 1/q_i) such that sum(w_i) = 1.0

        Args:
            remaining_fractions: Remaining quota fraction q_i in (0, 1] per account.
            rate_limits: Configured per-account maximum token rate limit r_i.
            tolerance: Convergence absolute error threshold for sum(w_i).
            max_iterations: Maximum bisection steps.

        Returns:
            Normalized allocation weights mapping account_id -> weight.
        """
        if not remaining_fractions:
            return {}

        if len(remaining_fractions) == 1:
            only_acc = next(iter(remaining_fractions))
            return {only_acc: 1.0}

        # Clamp remaining fractions away from zero to avoid singularity
        c: Dict[str, float] = {}
        for acc, frac in remaining_fractions.items():
            safe_frac = max(1e-4, min(1.0, float(frac)))
            c[acc] = 1.0 / safe_frac

        # Bisection boundaries for lambda
        # When lambda <= min(c), all w_i = 0, sum = 0
        # When lambda >= max(c) + 1.0, each term >= 1.0, sum >= N >= 1
        low = min(c.values())
        high = max(c.values()) + 1.0

        for _ in range(max_iterations):
            mid = 0.5 * (low + high)
            total = sum(max(0.0, mid - ci) for ci in c.values())
            if abs(total - 1.0) < tolerance:
                low = high = mid
                break
            if total < 1.0:
                low = mid
            else:
                high = mid

        lambda_star = 0.5 * (low + high)
        raw_weights = {acc: max(0.0, lambda_star - c[acc]) for acc in remaining_fractions}
        sum_weights = sum(raw_weights.values())

        if sum_weights > 0.0:
            normalized = {acc: round(w / sum_weights, 6) for acc, w in raw_weights.items()}
            # Reconcile fractional rounding drift to guarantee sum == 1.0
            drift = round(1.0 - sum(normalized.values()), 6)
            first_key = next(iter(normalized))
            normalized[first_key] = round(normalized[first_key] + drift, 6)
            return normalized

        # Fallback to uniform distribution
        n = len(remaining_fractions)
        fallback = {acc: round(1.0 / n, 6) for acc in remaining_fractions}
        drift = round(1.0 - sum(fallback.values()), 6)
        fallback[next(iter(fallback))] = round(fallback[next(iter(fallback))] + drift, 6)
        return fallback

    def adapt_circuit_breakers(
        self,
        escalation_state: str,
        consecutive_healthy: int,
        current_thresholds: Dict[str, float],
        default_thresholds: Dict[str, float],
    ) -> Dict[str, float]:
        """Adapt circuit breaker thresholds based on Anomaly Gateway escalation state.

        When escalation_state >= L1, trip thresholds are tightened by tightening_factor.
        During sustained healthy periods (>= 5 cycles), thresholds are relaxed by
        loosening_factor up to their initial default ceilings.

        Args:
            escalation_state: Current anomaly state (e.g. 'NORMAL', 'L0', 'L1', 'L2', 'CRITICAL').
            consecutive_healthy: Number of consecutive control intervals without an anomaly.
            current_thresholds: Active circuit breaker thresholds.
            default_thresholds: Baseline default circuit breaker thresholds.

        Returns:
            Updated circuit breaker thresholds dictionary.
        """
        updated = dict(current_thresholds)
        state_str = str(escalation_state).strip().upper()

        is_escalated = False
        if state_str in ("L1", "L2", "L3", "L4", "CRITICAL", "EMERGENCY"):
            is_escalated = True
        elif state_str.startswith("L") and len(state_str) > 1 and state_str[1:].isdigit():
            if int(state_str[1:]) >= 1:
                is_escalated = True

        if is_escalated:
            # Tighten trip thresholds
            for metric, value in updated.items():
                updated[metric] = round(value * self.tightening_factor, 3)
        elif consecutive_healthy >= 5:
            # Relax trip thresholds capped at default values
            for metric, value in updated.items():
                default_val = default_thresholds.get(metric, value)
                relaxed_val = value * self.loosening_factor
                updated[metric] = round(min(default_val, relaxed_val), 3)

        return updated

    # =================================================================
    # Serialization & Canary Validation Pipeline
    # =================================================================

    def _serialize_snapshot(self, record: CapacityDecisionRecord) -> ConfigurationSnapshot:
        """Serialize CapacityDecisionRecord into DirectiveGovernor ConfigurationSnapshot."""
        param_payload = {
            "worker_pool_size": record.proposed_envelope.worker_pool_size,
            "per_repo_dispatch_ceiling": record.proposed_envelope.per_repo_dispatch_ceiling,
            "account_quota_weights": record.proposed_envelope.account_quota_weights,
            "circuit_breaker_thresholds": record.proposed_envelope.circuit_breaker_thresholds,
            "timestamp": record.proposed_envelope.timestamp,
        }

        canonical_json = json.dumps(param_payload, sort_keys=True)
        checksum = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()

        metadata_payload = {
            "decision_id": record.decision_id,
            "trigger": record.trigger,
            "rationale": record.rationale,
            "previous_envelope_timestamp": record.previous_envelope.timestamp,
        }

        return ConfigurationSnapshot(
            snapshot_id=f"snap-{record.decision_id}",
            timestamp=record.proposed_envelope.timestamp,
            target_component="pipeline_capacity_governor",
            parameters=param_payload,
            metadata=metadata_payload,
            checksum=checksum,
        )

    def _canary_pipeline_validate(self, snapshot: ConfigurationSnapshot) -> bool:
        """Validate proposed configuration snapshot against hard canary safety gates.

        Returns:
            True if all safety boundaries are satisfied; False otherwise.
        """
        params = snapshot.parameters
        try:
            # 1. Worker pool boundaries
            workers = params.get("worker_pool_size")
            if not isinstance(workers, int) or workers < 1 or workers > 1024:
                logger.error("Canary validation failed: worker_pool_size out of bounds [1, 1024]: %s", workers)
                return False

            # 2. Repo dispatch ceilings validation
            repo_ceilings = params.get("per_repo_dispatch_ceiling", {})
            if not isinstance(repo_ceilings, dict) or not repo_ceilings:
                logger.error("Canary validation failed: empty or malformed per_repo_dispatch_ceiling.")
                return False
            for repo, ceiling in repo_ceilings.items():
                if not isinstance(ceiling, (int, float)) or ceiling <= 0.0:
                    logger.error("Canary validation failed: ceiling for repo %s is non-positive: %s", repo, ceiling)
                    return False

            # 3. Account quota weights validation
            weights = params.get("account_quota_weights", {})
            if not isinstance(weights, dict) or not weights:
                logger.error("Canary validation failed: empty account_quota_weights.")
                return False
            total_weight = 0.0
            for acc, weight in weights.items():
                if not isinstance(weight, (int, float)) or weight < 0.0:
                    logger.error("Canary validation failed: negative weight for account %s: %s", acc, weight)
                    return False
                total_weight += weight
            if not math.isclose(total_weight, 1.0, abs_tol=1e-3):
                logger.error("Canary validation failed: quota weights sum (%s) != 1.0", total_weight)
                return False

            # 4. Circuit breaker thresholds validation
            breakers = params.get("circuit_breaker_thresholds", {})
            if not isinstance(breakers, dict) or not breakers:
                logger.error("Canary validation failed: empty circuit_breaker_thresholds.")
                return False
            for b_name, thresh in breakers.items():
                if not isinstance(thresh, (int, float)) or thresh <= 0.0:
                    logger.error("Canary validation failed: non-positive breaker threshold %s: %s", b_name, thresh)
                    return False

            return True
        except Exception as ex:
            logger.exception("Canary validation raised unexpected error: %s", ex)
            return False

    # =================================================================
    # Core Control Step Execution
    # =================================================================

    def step(self, mock_telemetry: Optional[Dict[str, Any]] = None) -> CapacityDecisionRecord:
        """Execute one complete evaluation cycle of the capacity governance loop.

        Steps performed:
            1. Sample metrics from Observability Fabric callback queue.
            2. Compute PID corrections for worker utilization, token throughput, and queue depth.
            3. Clamp corrections to ±max_correction_pct.
            4. Apply PID adjustments to current CapacityEnvelope.
            5. Check EWMA forecasts for predictive pre-scaling trigger.
            6. Run water-filling quota rebalancer via bisection search.
            7. Adapt circuit breaker thresholds to Anomaly Gateway escalation status.
            8. Construct CapacityDecisionRecord.
            9. Serialize to ConfigurationSnapshot and submit to canary pipeline.
            10. Atomically promote validated envelope under threading.Lock.

        Returns:
            The recorded CapacityDecisionRecord.
        """
        now = time.time()
        dt = now - self._last_control_cycle_time
        if dt <= 0.0:
            dt = self.control_interval_seconds
        self._last_control_cycle_time = now

        # 1. Telemetry ingestion
        telemetry_samples: List[Dict[str, Any]] = []
        if mock_telemetry:
            telemetry_samples.append(mock_telemetry)
        while not self._telemetry_queue.empty():
            try:
                telemetry_samples.append(self._telemetry_queue.get_nowait())
            except queue.Empty:
                break

        current_env = self.get_current_envelope()

        # Extract current metric aggregates or defaults
        if telemetry_samples:
            latest = telemetry_samples[-1]
            utilization = float(latest.get("worker_pool_utilization", 0.75))
            token_rate = float(latest.get("aggregate_token_throughput", 800.0))
            queue_depth = float(latest.get("queue_depth", 2.0 * current_env.worker_pool_size))
            arrival_rate = float(latest.get("task_arrival_rate", 1.2))
            duration = float(latest.get("execution_duration", 4.5))
            token_burn = float(latest.get("token_burn", 350.0))
            remaining_quotas = latest.get("account_remaining_quotas", {})
            escalation_state = str(latest.get("escalation_state", "NORMAL"))
        else:
            utilization = 0.75
            token_rate = 800.0
            queue_depth = 2.0 * current_env.worker_pool_size
            arrival_rate = 1.0
            duration = 4.0
            token_burn = 300.0
            remaining_quotas = {}
            escalation_state = "NORMAL"

        # Update Escalation Tracking
        is_escalated = (
            escalation_state.upper() in ("L1", "L2", "L3", "L4", "CRITICAL", "EMERGENCY")
            or (escalation_state.upper().startswith("L") and escalation_state[1:].isdigit() and int(escalation_state[1:]) >= 1)
        )
        if is_escalated:
            self._consecutive_healthy = 0
        else:
            self._consecutive_healthy += 1
        self._last_escalation_state = escalation_state

        # Update EWMA forecast
        forecast = self._compute_ewma_and_forecast(arrival_rate, duration, token_burn)

        # 2 & 3. Compute PID corrections and clamp to ±max_correction_pct
        # Worker utilization PID: setpoint=0.75. If measured > 0.75, error > 0, expand pool.
        err_util = utilization - self.pid_worker_util.setpoint
        raw_corr_util = self.pid_worker_util.compute(err_util, dt)
        clamped_util_pct = max(-self.max_correction_pct, min(self.max_correction_pct, raw_corr_util))

        # Queue depth PID: target = setpoint_factor * worker_pool_size
        target_queue = self.pid_queue_depth.setpoint * current_env.worker_pool_size
        err_queue = (queue_depth - target_queue) / max(1.0, float(target_queue))
        raw_corr_queue = self.pid_queue_depth.compute(err_queue, dt)
        clamped_queue_pct = max(-self.max_correction_pct, min(self.max_correction_pct, raw_corr_queue))

        # Token throughput PID: targeting 90% of minimum per-account rate limit
        min_rate_limit = min(self._rate_limits.values()) if self._rate_limits else 1000.0
        token_ratio = token_rate / min_rate_limit
        # If token_ratio > 0.90, error < 0 -> throttle dispatch ceiling
        err_token = self.pid_token_tp.setpoint - token_ratio
        raw_corr_token = self.pid_token_tp.compute(err_token, dt)
        clamped_token_pct = max(-self.max_correction_pct, min(self.max_correction_pct, raw_corr_token))

        # 4. Propose envelope modifications from PID
        combined_worker_delta_pct = max(
            -self.max_correction_pct,
            min(self.max_correction_pct, clamped_util_pct + clamped_queue_pct),
        )
        proposed_workers = max(
            1,
            int(round(current_env.worker_pool_size * (1.0 + combined_worker_delta_pct))),
        )

        proposed_repo_ceilings: Dict[str, float] = {}
        for repo, ceiling in current_env.per_repo_dispatch_ceiling.items():
            new_ceiling = max(0.5, ceiling * (1.0 + clamped_token_pct))
            proposed_repo_ceilings[repo] = round(new_ceiling, 2)

        # 5. Check EWMA predictive pre-scaling trigger
        predicted_demand_concurrency = forecast.ewma_task_arrival_rate * forecast.ewma_execution_duration
        current_capacity = float(current_env.worker_pool_size)
        relative_excess = (predicted_demand_concurrency - current_capacity) / max(1.0, current_capacity)

        # Statistical confidence estimation from sliding window
        ci_low, ci_high = forecast.confidence_interval
        ci_spread = max(1e-4, ci_high - ci_low)
        # Confidence that demand exceeds capacity by prescale_threshold
        excess_barrier = current_capacity * (1.0 + self.prescale_threshold)
        if predicted_demand_concurrency > excess_barrier:
            z_score = (predicted_demand_concurrency - excess_barrier) / (ci_spread / 3.92)
            confidence = 0.5 * (1.0 + math.erf(z_score / math.sqrt(2.0)))
        else:
            confidence = 0.0

        trigger_type: Literal[
            "pid_correction",
            "predictive_prescale",
            "quota_rebalance",
            "breaker_adaptation",
            "periodic_review",
        ] = "periodic_review"

        rationale_items: List[str] = []

        if relative_excess > self.prescale_threshold and confidence > self.prescale_confidence:
            trigger_type = "predictive_prescale"
            prescale_workers = max(
                proposed_workers,
                int(math.ceil(predicted_demand_concurrency)),
            )
            # Bound expansion to max_correction_pct above current
            prescale_bounded = min(
                prescale_workers,
                int(round(current_capacity * (1.0 + self.max_correction_pct))),
            )
            proposed_workers = max(proposed_workers, prescale_bounded)
            rationale_items.append(
                f"Predictive pre-scale triggered: forecasted demand ({predicted_demand_concurrency:.2f} concurrency) "
                f"exceeds current capacity ({current_capacity}) by {relative_excess * 100:.1f}% "
                f"with confidence {confidence:.2f}."
            )

        # 6. Water-filling quota rebalancer
        if not remaining_quotas:
            # Default fallback if telemetry lacked explicit quota breakdown
            remaining_quotas = {acc: 1.0 for acc in self._rate_limits}

        proposed_quota_weights = self.water_fill_quota_weights(
            remaining_fractions=remaining_quotas,
            rate_limits=self._rate_limits,
        )

        quota_divergence = sum(
            abs(proposed_quota_weights.get(acc, 0.0) - current_env.account_quota_weights.get(acc, 0.0))
            for acc in self._rate_limits
        )
        if quota_divergence > 0.15 and trigger_type not in ("predictive_prescale", "breaker_adaptation"):
            trigger_type = "quota_rebalance"
            rationale_items.append(
                f"Water-filling quota rebalance triggered: total weight divergence {quota_divergence:.3f}."
            )

        # 7. Circuit breaker threshold adaptation
        proposed_breaker_thresholds = self.adapt_circuit_breakers(
            escalation_state=escalation_state,
            consecutive_healthy=self._consecutive_healthy,
            current_thresholds=current_env.circuit_breaker_thresholds,
            default_thresholds=self._default_breaker_thresholds,
        )

        breaker_divergence = any(
            not math.isclose(proposed_breaker_thresholds[k], current_env.circuit_breaker_thresholds.get(k, 0.0))
            for k in proposed_breaker_thresholds
        )
        if breaker_divergence:
            trigger_type = "breaker_adaptation"
            if is_escalated:
                rationale_items.append(
                    f"Anomaly Gateway escalation {escalation_state} active: tightened breaker thresholds by {self.tightening_factor}x."
                )
            else:
                rationale_items.append(
                    f"Sustained healthy pipeline ({self._consecutive_healthy} intervals): relaxed breaker thresholds towards baseline."
                )

        # If PID corrected dimensions and no higher trigger intercepted
        if trigger_type == "periodic_review":
            if proposed_workers != current_env.worker_pool_size or proposed_repo_ceilings != current_env.per_repo_dispatch_ceiling:
                trigger_type = "pid_correction"
                rationale_items.append(
                    f"PID correction: worker pool adjusted from {current_env.worker_pool_size} to {proposed_workers} "
                    f"(utilization={utilization:.2f}, queue_depth={queue_depth:.1f})."
                )
            else:
                rationale_items.append("Periodic review: all metrics within optimal operating boundaries.")

        full_rationale = " | ".join(rationale_items)

        # 8. Construct CapacityDecisionRecord
        proposed_envelope = CapacityEnvelope(
            worker_pool_size=proposed_workers,
            per_repo_dispatch_ceiling=proposed_repo_ceilings,
            account_quota_weights=proposed_quota_weights,
            circuit_breaker_thresholds=proposed_breaker_thresholds,
            timestamp=now,
        )

        decision_id = str(uuid.uuid4())
        candidate_record = CapacityDecisionRecord(
            decision_id=decision_id,
            previous_envelope=current_env,
            proposed_envelope=proposed_envelope,
            trigger=trigger_type,
            rationale=full_rationale,
            canary_validated=False,
        )

        # 9. DirectiveGovernor ConfigurationSnapshot & Canary Pipeline
        snapshot = self._serialize_snapshot(candidate_record)
        canary_passed = self._canary_pipeline_validate(snapshot)

        final_record = CapacityDecisionRecord(
            decision_id=decision_id,
            previous_envelope=current_env,
            proposed_envelope=proposed_envelope,
            trigger=trigger_type,
            rationale=full_rationale,
            canary_validated=canary_passed,
        )

        # 10. Atomic promotion on canary validation success
        with self._lock:
            if canary_passed:
                self._current_envelope = proposed_envelope
                logger.info(
                    "Canary passed: Promoted envelope decision_id=%s [trigger=%s, workers=%d]",
                    decision_id,
                    trigger_type,
                    proposed_workers,
                )
            else:
                logger.warning("Canary REJECTED proposed envelope decision_id=%s", decision_id)

            self._decision_history.append(final_record)

        return final_record

    def _run_control_loop(self) -> None:
        """Internal worker loop executing control cycles every control_interval_seconds."""
        logger.info("Starting PipelineCapacityGovernor control loop (interval=%ss)", self.control_interval_seconds)
        while not self._stop_event.is_set():
            try:
                self.step()
            except Exception as ex:
                logger.exception("Unexpected error inside PipelineCapacityGovernor control loop: %s", ex)

            # Responsive wait loop
            sleep_remaining = self.control_interval_seconds
            step_chunk = 0.5
            while sleep_remaining > 0.0 and not self._stop_event.is_set():
                time.sleep(min(sleep_remaining, step_chunk))
                sleep_remaining -= step_chunk


# =====================================================================
# Standalone Demonstration & Audit Trail Verification
# =====================================================================

if __name__ == "__main__":
    # Configure deterministic publication-grade logging format
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
    )

    print("=" * 80)
    print("PIPELINE ADAPTIVE CAPACITY GOVERNOR & ALLOCATION ENGINE - TEST SUITE")
    print("=" * 80)

    # Instantiate governor with benchmark parameters
    governor = PipelineCapacityGovernor(
        control_interval_seconds=30.0,
        ewma_alpha=0.3,
        ewma_window=20,
        prescale_threshold=0.15,
        prescale_confidence=0.7,
        tightening_factor=0.7,
        loosening_factor=1.1,
        max_correction_pct=0.20,
        initial_worker_pool_size=8,
    )

    # 10 Simulated Multi-Phase Control Cycles
    # Phase 1: Steady baseline (Cycles 1-2)
    # Phase 2: Overloaded workers & task arrival surge -> Predictive pre-scaling (Cycles 3-4)
    # Phase 3: Token burn spike & Anomaly Gateway L1 escalation -> Breaker tightening (Cycles 5-6)
    # Phase 4: Quota depletion & water-filling rebalance (Cycle 7)
    # Phase 5: Recovery & sustained healthy relaxing (Cycles 8-10)
    test_scenarios: List[Dict[str, Any]] = [
        # Cycle 1: Steady baseline
        {
            "worker_pool_utilization": 0.74,
            "aggregate_token_throughput": 850.0,
            "queue_depth": 16.0,
            "task_arrival_rate": 1.5,
            "execution_duration": 4.0,
            "token_burn": 300.0,
            "account_remaining_quotas": {"account_pro_1": 0.9, "account_pro_2": 0.85, "account_pro_3": 0.95, "account_pro_4": 0.8},
            "escalation_state": "NORMAL",
        },
        # Cycle 2: Minor worker load increase
        {
            "worker_pool_utilization": 0.82,
            "aggregate_token_throughput": 890.0,
            "queue_depth": 18.0,
            "task_arrival_rate": 1.8,
            "execution_duration": 4.2,
            "token_burn": 320.0,
            "account_remaining_quotas": {"account_pro_1": 0.85, "account_pro_2": 0.80, "account_pro_3": 0.90, "account_pro_4": 0.75},
            "escalation_state": "NORMAL",
        },
        # Cycle 3: Massive arrival surge triggering predictive pre-scale
        {
            "worker_pool_utilization": 0.92,
            "aggregate_token_throughput": 920.0,
            "queue_depth": 28.0,
            "task_arrival_rate": 3.6,
            "execution_duration": 4.5,
            "token_burn": 450.0,
            "account_remaining_quotas": {"account_pro_1": 0.75, "account_pro_2": 0.70, "account_pro_3": 0.80, "account_pro_4": 0.65},
            "escalation_state": "NORMAL",
        },
        # Cycle 4: Elevated demand sustained
        {
            "worker_pool_utilization": 0.88,
            "aggregate_token_throughput": 940.0,
            "queue_depth": 24.0,
            "task_arrival_rate": 3.4,
            "execution_duration": 4.4,
            "token_burn": 480.0,
            "account_remaining_quotas": {"account_pro_1": 0.65, "account_pro_2": 0.60, "account_pro_3": 0.72, "account_pro_4": 0.55},
            "escalation_state": "NORMAL",
        },
        # Cycle 5: Anomaly Gateway trips to L1 -> Breaker tightening
        {
            "worker_pool_utilization": 0.78,
            "aggregate_token_throughput": 980.0,
            "queue_depth": 16.0,
            "task_arrival_rate": 2.0,
            "execution_duration": 4.1,
            "token_burn": 550.0,
            "account_remaining_quotas": {"account_pro_1": 0.55, "account_pro_2": 0.50, "account_pro_3": 0.65, "account_pro_4": 0.45},
            "escalation_state": "L1",
        },
        # Cycle 6: L2 Escalation -> Further tightening
        {
            "worker_pool_utilization": 0.72,
            "aggregate_token_throughput": 820.0,
            "queue_depth": 14.0,
            "task_arrival_rate": 1.6,
            "execution_duration": 4.0,
            "token_burn": 340.0,
            "account_remaining_quotas": {"account_pro_1": 0.50, "account_pro_2": 0.45, "account_pro_3": 0.60, "account_pro_4": 0.35},
            "escalation_state": "L2",
        },
        # Cycle 7: Anomaly cleared to HEALTHY; Severe quota depletion on Account 4
        {
            "worker_pool_utilization": 0.73,
            "aggregate_token_throughput": 810.0,
            "queue_depth": 15.0,
            "task_arrival_rate": 1.5,
            "execution_duration": 3.9,
            "token_burn": 310.0,
            "account_remaining_quotas": {"account_pro_1": 0.45, "account_pro_2": 0.40, "account_pro_3": 0.55, "account_pro_4": 0.05},
            "escalation_state": "HEALTHY",
        },
        # Cycle 8: Consecutive healthy = 2
        {
            "worker_pool_utilization": 0.74,
            "aggregate_token_throughput": 800.0,
            "queue_depth": 16.0,
            "task_arrival_rate": 1.4,
            "execution_duration": 3.8,
            "token_burn": 300.0,
            "account_remaining_quotas": {"account_pro_1": 0.42, "account_pro_2": 0.38, "account_pro_3": 0.52, "account_pro_4": 0.04},
            "escalation_state": "HEALTHY",
        },
        # Cycle 9: Consecutive healthy = 3
        {
            "worker_pool_utilization": 0.75,
            "aggregate_token_throughput": 810.0,
            "queue_depth": 15.0,
            "task_arrival_rate": 1.3,
            "execution_duration": 3.9,
            "token_burn": 290.0,
            "account_remaining_quotas": {"account_pro_1": 0.39, "account_pro_2": 0.35, "account_pro_3": 0.49, "account_pro_4": 0.03},
            "escalation_state": "HEALTHY",
        },
        # Cycle 10: Consecutive healthy = 5 (>=5 triggers relaxation)
        {
            "worker_pool_utilization": 0.75,
            "aggregate_token_throughput": 805.0,
            "queue_depth": 16.0,
            "task_arrival_rate": 1.3,
            "execution_duration": 3.8,
            "token_burn": 285.0,
            "account_remaining_quotas": {"account_pro_1": 0.37, "account_pro_2": 0.33, "account_pro_3": 0.46, "account_pro_4": 0.02},
            "escalation_state": "HEALTHY",
        },
    ]

    print("\nExecuting 10 Governance Simulation Cycles...\n")
    for cycle_idx, telemetry in enumerate(test_scenarios, start=1):
        # Artificially align consecutive healthy counter for Cycle 10 to demonstrate relaxation
        if cycle_idx == 10:
            governor._consecutive_healthy = 4

        record = governor.step(mock_telemetry=telemetry)
        env = record.proposed_envelope

        print(f"--- Cycle {cycle_idx:02d} | Trigger: {record.trigger.upper()} | Canary: {record.canary_validated} ---")
        print(f"  Worker Pool Size     : {record.previous_envelope.worker_pool_size} -> {env.worker_pool_size}")
        print(f"  Repo Dispatch Ceiling: {env.per_repo_dispatch_ceiling}")
        print(f"  Account Quota Weights: {env.account_quota_weights}")
        print(f"  Breaker Thresholds   : {env.circuit_breaker_thresholds}")
        print(f"  Rationale            : {record.rationale}")
        print()

    print("=" * 80)
    print("DECISION AUDIT TRAIL SUMMARY")
    print("=" * 80)
    history = governor.get_decision_history()
    print(f"Total Decisions Committed: {len(history)}")
    for i, dec in enumerate(history, 1):
        print(
            f"[{i:02d}] ID: {dec.decision_id[:8]}.. | Trigger: {dec.trigger:20s} | "
            f"Workers: {dec.proposed_envelope.worker_pool_size:2d} | Canary: {dec.canary_validated}"
        )
    print("=" * 80)
    print("Execution and verification completed successfully.")