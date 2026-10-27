"""
tasks/pipeline_self_tuner.py
Hermes Orchestrator - Adaptive Pipeline Self-Tuning Controller & Closed-Loop Reconfiguration Engine.

Provides an autonomous feedback control loop that senses pipeline diagnostics,
evaluates bottlenecks, and tunes shared execution parameters (concurrency, queue timeouts,
decomposition limits, cache TTLs) within safety bounds to optimize overall system throughput
and stability.
"""

from __future__ import annotations

import contextlib
from dataclasses import asdict, dataclass, field
import json
import logging
from pathlib import Path
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Union

# Configure module-level logging
logger = logging.getLogger("hermes.pipeline.tuner")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"))
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)

# OpenTelemetry Tracing Setup with Graceful Fallback
try:
    from opentelemetry import trace
    tracer = trace.get_tracer("hermes.pipeline.tuner")
except ImportError:
    tracer = None


# ============================================================================
# ReaderWriterLock Pattern (Multi-reader, Single-writer Synchronization)
# ============================================================================

try:
    from tasks.runtime_execution_governor import ReaderWriterLock
except (ImportError, AttributeError):
    class ReaderWriterLock:
        """A re-entrant reader-writer lock supporting concurrent readers or exclusive writer."""

        def __init__(self) -> None:
            self._lock = threading.Lock()
            self._readers_ok = threading.Condition(self._lock)
            self._writers_ok = threading.Condition(self._lock)
            self._readers: int = 0
            self._writers_waiting: int = 0
            self._writer_active: bool = False

        def acquire_read(self) -> None:
            with self._lock:
                while self._writer_active or self._writers_waiting > 0:
                    self._readers_ok.wait()
                self._readers += 1

        def release_read(self) -> None:
            with self._lock:
                self._readers -= 1
                if self._readers == 0:
                    self._writers_ok.notify()

        def acquire_write(self) -> None:
            with self._lock:
                self._writers_waiting += 1
                while self._writer_active or self._readers > 0:
                    self._writers_ok.wait()
                self._writers_waiting -= 1
                self._writer_active = True

        def release_write(self) -> None:
            with self._lock:
                self._writer_active = False
                if self._writers_waiting > 0:
                    self._writers_ok.notify()
                else:
                    self._readers_ok.notify_all()

        @contextlib.contextmanager
        def read_lock(self):
            self.acquire_read()
            try:
                yield
            finally:
                self.release_read()

        @contextlib.contextmanager
        def write_lock(self):
            self.acquire_write()
            try:
                yield
            finally:
                self.release_write()


# ============================================================================
# Data Models: TuningParameter & TuningAction
# ============================================================================

@dataclass
class TuningParameter:
    """Represents a tunable pipeline parameter governed by the controller."""
    name: str
    current_value: float
    min_value: float
    max_value: float
    step_size: float
    last_change_tick: int = 0
    consecutive_same_direction: int = 0
    _last_direction: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "current_value": self.current_value,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "step_size": self.step_size,
            "last_change_tick": self.last_change_tick,
            "consecutive_same_direction": self.consecutive_same_direction,
        }


@dataclass
class TuningAction:
    """Records an atomic adjustment applied to a pipeline configuration parameter."""
    parameter_name: str
    old_value: float
    new_value: float
    trigger_signal: str
    timestamp: float
    health_score_snapshot: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "parameter_name": self.parameter_name,
            "old_value": self.old_value,
            "new_value": self.new_value,
            "trigger_signal": self.trigger_signal,
            "timestamp": self.timestamp,
            "health_score_snapshot": self.health_score_snapshot,
        }


# ============================================================================
# SharedPipelineConfig: Thread-Safe Singleton
# ============================================================================

class SharedPipelineConfig:
    """Thread-safe configuration store using ReaderWriterLock.

    Maintains the canonical in-memory state of tunable pipeline parameters,
    providing non-blocking concurrent reads and serialized bounds-checked writes.
    """
    _instance: Optional["SharedPipelineConfig"] = None
    _singleton_lock = threading.Lock()

    def __new__(cls, *args, **kwargs) -> "SharedPipelineConfig":
        if cls._instance is None:
            with cls._singleton_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    @classmethod
    def get_instance(cls) -> "SharedPipelineConfig":
        return cls()

    def __init__(self) -> None:
        if getattr(self, "_initialized", False):
            return
        with self._singleton_lock:
            if getattr(self, "_initialized", False):
                return
            self._rw_lock = ReaderWriterLock()
            self._parameters: Dict[str, TuningParameter] = {}
            self._init_defaults()
            self._initialized = True

    def _init_defaults(self) -> None:
        defaults = [
            TuningParameter("CONCURRENCY_LIMIT", 3.0, 1.0, 6.0, 1.0),
            TuningParameter("FEASIBILITY_GATE_PASS_THRESHOLD", 0.6, 0.3, 0.9, 0.05),
            TuningParameter("DECOMPOSITION_MAX_DEPTH", 3.0, 1.0, 5.0, 1.0),
            TuningParameter("HOLDING_QUEUE_RETRY_INTERVAL_SEC", 120.0, 30.0, 600.0, 30.0),
            TuningParameter("GOVERNOR_THROTTLE_BURN_RATIO", 0.8, 0.5, 0.95, 0.05),
            TuningParameter("GOVERNOR_PREEMPT_TIMEOUT_MULTIPLIER", 2.5, 1.5, 4.0, 0.25),
            TuningParameter("COST_ESTIMATOR_CACHE_TTL_SEC", 300.0, 60.0, 1800.0, 60.0),
        ]
        for p in defaults:
            self._parameters[p.name] = p

    @contextlib.contextmanager
    def read_lock(self):
        with self._rw_lock.read_lock():
            yield

    @contextlib.contextmanager
    def write_lock(self):
        with self._rw_lock.write_lock():
            yield

    def get(self, param_name: str) -> float:
        """Retrieves the current numeric value of a parameter under a read lock."""
        with self._rw_lock.read_lock():
            if param_name not in self._parameters:
                raise KeyError(f"Parameter '{param_name}' not registered in SharedPipelineConfig")
            return self._parameters[param_name].current_value

    def set(self, param_name: str, value: float) -> None:
        """Sets a parameter value under a write lock, clamping within min/max bounds."""
        with self._rw_lock.write_lock():
            if param_name not in self._parameters:
                raise KeyError(f"Parameter '{param_name}' not registered in SharedPipelineConfig")
            param = self._parameters[param_name]
            clamped = max(param.min_value, min(param.max_value, float(value)))
            param.current_value = clamped

    def get_parameter(self, param_name: str) -> TuningParameter:
        """Returns a snapshot copy of the specified TuningParameter."""
        with self._rw_lock.read_lock():
            if param_name not in self._parameters:
                raise KeyError(f"Parameter '{param_name}' not registered in SharedPipelineConfig")
            p = self._parameters[param_name]
            return TuningParameter(
                name=p.name,
                current_value=p.current_value,
                min_value=p.min_value,
                max_value=p.max_value,
                step_size=p.step_size,
                last_change_tick=p.last_change_tick,
                consecutive_same_direction=p.consecutive_same_direction,
                _last_direction=p._last_direction,
            )

    def get_all(self) -> Dict[str, TuningParameter]:
        """Returns snapshot copies of all registered tuning parameters."""
        with self._rw_lock.read_lock():
            result = {}
            for k, p in self._parameters.items():
                result[k] = TuningParameter(
                    name=p.name,
                    current_value=p.current_value,
                    min_value=p.min_value,
                    max_value=p.max_value,
                    step_size=p.step_size,
                    last_change_tick=p.last_change_tick,
                    consecutive_same_direction=p.consecutive_same_direction,
                    _last_direction=p._last_direction,
                )
            return result


# ============================================================================
# SafetyGuard: Boundary, Override & Stability Dampening
# ============================================================================

class SafetyGuard:
    """Enforces safety constraints, prevents oscillation, and applies pinned overrides."""

    @staticmethod
    def check_stability(
        param: TuningParameter,
        proposed_direction: int,
        health_delta: float,
        stability_window: int = 3,
    ) -> bool:
        """Returns False if consecutive adjustments in the same direction reach stability_window

        and the resulting health delta is non-positive (<= 0), preventing runaway drift.
        """
        if proposed_direction == 0:
            return True
        if param.consecutive_same_direction >= stability_window and health_delta <= 0.0:
            return False
        return True

    @staticmethod
    def check_override(
        param_name: str,
        override_path: Optional[Union[str, Path]] = None,
    ) -> Optional[float]:
        """Reads dashboards/tuning_override.json for manually pinned parameter overrides."""
        if override_path is None:
            resolved_path = Path(__file__).resolve().parent.parent / "dashboards" / "tuning_override.json"
        else:
            resolved_path = Path(override_path)

        try:
            if resolved_path.is_file():
                with open(resolved_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict) and param_name in data:
                    return float(data[param_name])
        except Exception as e:
            logger.warning("Error reading tuning overrides from %s: %s", resolved_path, e)
        return None

    @staticmethod
    def clamp(param: TuningParameter, proposed_value: float) -> float:
        """Clamps proposed value within parameter bounds and rounds to step resolution."""
        bounded = max(param.min_value, min(param.max_value, proposed_value))
        step = param.step_size
        if step > 0:
            steps_count = round((bounded - param.min_value) / step)
            bounded = param.min_value + (steps_count * step)
        return float(round(max(param.min_value, min(param.max_value, bounded)), 6))


# ============================================================================
# TuningLedger: Append-Only Audit Trail
# ============================================================================

class TuningLedger:
    """Manages persistent append-only audit trail at dashboards/tuning_ledger.jsonl."""

    def __init__(self, ledger_path: Optional[Union[str, Path]] = None) -> None:
        if ledger_path is None:
            self.ledger_path = Path(__file__).resolve().parent.parent / "dashboards" / "tuning_ledger.jsonl"
        else:
            self.ledger_path = Path(ledger_path)
        self._lock = threading.Lock()
        self._ensure_dir()

    def _ensure_dir(self) -> None:
        try:
            self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.warning("Failed to verify/create directory for %s: %s", self.ledger_path, e)

    def append(self, action: TuningAction) -> None:
        """Appends an individual TuningAction JSON line to the ledger."""
        with self._lock:
            try:
                self._ensure_dir()
                with open(self.ledger_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(action.to_dict()) + "\n")
            except Exception as e:
                logger.error("Failed to append action to TuningLedger: %s", e)

    def read_recent(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Reads up to `limit` most recent tuning actions from the ledger."""
        with self._lock:
            if not self.ledger_path.is_file():
                return []
            try:
                with open(self.ledger_path, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                recent: List[Dict[str, Any]] = []
                for line in lines[-limit:]:
                    stripped = line.strip()
                    if stripped:
                        recent.append(json.loads(stripped))
                return recent
            except Exception as e:
                logger.error("Failed reading recent actions from TuningLedger: %s", e)
                return []


# ============================================================================
# ControlLoop: Autonomous Closed-Loop Self-Tuner
# ============================================================================

class ControlLoop:
    """Autonomous sense-compute-apply feedback controller for pipeline self-tuning."""

    def __init__(
        self,
        diagnostics: Any = None,
        config: Optional[SharedPipelineConfig] = None,
        tick_interval_sec: float = 120.0,
        target_health: float = 75.0,
        target_throughput: float = 2.0,
        target_stability: float = 70.0,
    ) -> None:
        self.diagnostics = diagnostics
        self.config = config or SharedPipelineConfig.get_instance()
        self.tick_interval_sec = tick_interval_sec
        self.target_health = target_health
        self.target_throughput = target_throughput
        self.target_stability = target_stability

        self.guard = SafetyGuard()
        self.ledger = TuningLedger()

        self.current_tick: int = 0
        self.last_health_score: Optional[float] = None
        self.last_health_delta: float = 0.0

        self.shutdown_event = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None
        self._history: List[TuningAction] = []
        self._history_lock = threading.Lock()

    def is_running(self) -> bool:
        return self._worker_thread is not None and self._worker_thread.is_alive()

    def sense(self) -> Dict[str, Any]:
        """Gathers runtime telemetry from the Diagnostics engine with robust fallbacks."""
        sensed: Dict[str, Any] = {
            "health": {},
            "bottleneck": {},
            "stage_metrics": {},
            "decomposition": {},
            "holding_queue": {},
            "timestamp": time.time(),
        }

        if self.diagnostics is None:
            return sensed

        # 1. Health score
        try:
            if hasattr(self.diagnostics, "get_health_score"):
                health_res = self.diagnostics.get_health_score()
                if isinstance(health_res, dict):
                    sensed["health"] = health_res
                elif isinstance(health_res, (int, float)):
                    sensed["health"] = {"overall_score": float(health_res)}
        except Exception as e:
            logger.warning("Error sensing health score: %s", e)

        # 2. Bottleneck identification
        try:
            if hasattr(self.diagnostics, "identify_bottleneck"):
                b_res = self.diagnostics.identify_bottleneck()
                if isinstance(b_res, dict):
                    sensed["bottleneck"] = b_res
                elif hasattr(b_res, "to_dict"):
                    sensed["bottleneck"] = b_res.to_dict()
        except Exception as e:
            logger.warning("Error sensing bottleneck: %s", e)

        # 3. Stage metrics
        try:
            if hasattr(self.diagnostics, "get_stage_metrics"):
                sm_res = self.diagnostics.get_stage_metrics()
                if isinstance(sm_res, dict):
                    sensed["stage_metrics"] = sm_res
        except Exception as e:
            logger.warning("Error sensing stage metrics: %s", e)

        # 4. Decomposition analytics
        try:
            if hasattr(self.diagnostics, "get_decomposition_analytics"):
                decomp_res = self.diagnostics.get_decomposition_analytics()
                if isinstance(decomp_res, dict):
                    sensed["decomposition"] = decomp_res
        except Exception as e:
            logger.warning("Error sensing decomposition analytics: %s", e)

        # 5. Holding queue metrics (if separate)
        try:
            if hasattr(self.diagnostics, "get_holding_queue_metrics"):
                hq_res = self.diagnostics.get_holding_queue_metrics()
                if isinstance(hq_res, dict):
                    sensed["holding_queue"] = hq_res
            elif "HOLDING_QUEUE" in sensed["stage_metrics"]:
                sensed["holding_queue"] = sensed["stage_metrics"]["HOLDING_QUEUE"]
        except Exception as e:
            logger.warning("Error sensing holding queue metrics: %s", e)

        return sensed

    def compute_actions(self, sensed: Dict[str, Any]) -> List[TuningAction]:
        """Evaluates mapping rules against sensed metrics and outputs dampened tuning actions."""
        actions: List[TuningAction] = []
        now = time.time()

        # Extract health snapshot
        health_info = sensed.get("health", {})
        overall_health = float(health_info.get("overall_score", health_info.get("health_score", 75.0)))
        throughput_score = float(health_info.get("throughput_score", 100.0))
        stability_score = float(health_info.get("stability_score", 100.0))

        # Helper to propose, validate and format an action
        def propose_tuning(param_name: str, direction: int, trigger: str) -> None:
            try:
                param = self.config.get_parameter(param_name)
            except KeyError:
                logger.error("Parameter %s not found in SharedPipelineConfig", param_name)
                return

            # Check override first
            override_val = self.guard.check_override(param_name)
            if override_val is not None:
                clamped_override = self.guard.clamp(param, override_val)
                if abs(clamped_override - param.current_value) > 1e-5:
                    actions.append(
                        TuningAction(
                            parameter_name=param_name,
                            old_value=param.current_value,
                            new_value=clamped_override,
                            trigger_signal=f"manual_override_pinned:{clamped_override}",
                            timestamp=now,
                            health_score_snapshot=overall_health,
                        )
                    )
                return

            if direction == 0:
                return

            # Check stability dampening
            if not self.guard.check_stability(param, direction, self.last_health_delta):
                logger.info(
                    "SafetyGuard: Suppressing adjustment for %s (direction=%+d, consecutive=%d, health_delta=%.2f)",
                    param_name,
                    direction,
                    param.consecutive_same_direction,
                    self.last_health_delta,
                )
                return

            proposed_val = param.current_value + (direction * param.step_size)
            clamped_val = self.guard.clamp(param, proposed_val)

            if abs(clamped_val - param.current_value) > 1e-5:
                actions.append(
                    TuningAction(
                        parameter_name=param_name,
                        old_value=param.current_value,
                        new_value=clamped_val,
                        trigger_signal=trigger,
                        timestamp=now,
                        health_score_snapshot=overall_health,
                    )
                )

        # --------------------------------------------------------------------
        # Rule 1: bottleneck_stage == 'COST_ESTIMATION' with utilization > 0.7
        # -> increase COST_ESTIMATOR_CACHE_TTL_SEC
        # --------------------------------------------------------------------
        bottleneck_info = sensed.get("bottleneck", {})
        bottleneck_stage = bottleneck_info.get("stage") or bottleneck_info.get("primary_bottleneck") or ""
        bottleneck_util = float(bottleneck_info.get("utilization", 0.0))

        if bottleneck_stage == "COST_ESTIMATION" and bottleneck_util > 0.7:
            propose_tuning(
                "COST_ESTIMATOR_CACHE_TTL_SEC",
                direction=+1,
                trigger=f"bottleneck:COST_ESTIMATION:utilization={bottleneck_util:.2f}",
            )

        # --------------------------------------------------------------------
        # Rule 2: bottleneck_stage == 'READINESS_WAIT' with fraction > 0.4
        # -> decrease DECOMPOSITION_MAX_DEPTH (reduce DAG depth)
        # --------------------------------------------------------------------
        readiness_fraction = float(bottleneck_info.get("fraction", 0.0))
        if bottleneck_stage == "READINESS_WAIT" and readiness_fraction > 0.4:
            propose_tuning(
                "DECOMPOSITION_MAX_DEPTH",
                direction=-1,
                trigger=f"bottleneck:READINESS_WAIT:fraction={readiness_fraction:.2f}",
            )

        # --------------------------------------------------------------------
        # Rule 3: health.throughput_score < target_throughput
        # -> increase CONCURRENCY_LIMIT
        # --------------------------------------------------------------------
        if throughput_score < self.target_throughput:
            propose_tuning(
                "CONCURRENCY_LIMIT",
                direction=+1,
                trigger=f"health:throughput_score={throughput_score:.2f}<target={self.target_throughput:.2f}",
            )

        # --------------------------------------------------------------------
        # Rule 4: health.stability_score < target_stability
        # -> increase GOVERNOR_PREEMPT_TIMEOUT_MULTIPLIER (less aggressive preemption)
        # --------------------------------------------------------------------
        if stability_score < self.target_stability:
            propose_tuning(
                "GOVERNOR_PREEMPT_TIMEOUT_MULTIPLIER",
                direction=+1,
                trigger=f"health:stability_score={stability_score:.2f}<target={self.target_stability:.2f}",
            )

        # --------------------------------------------------------------------
        # Rule 5: decomposition.avg_fan_out > 4.0
        # -> decrease FEASIBILITY_GATE_PASS_THRESHOLD
        # --------------------------------------------------------------------
        decomp_info = sensed.get("decomposition", {})
        avg_fan_out = float(decomp_info.get("avg_fan_out", 0.0))
        if avg_fan_out > 4.0:
            propose_tuning(
                "FEASIBILITY_GATE_PASS_THRESHOLD",
                direction=-1,
                trigger=f"decomposition:avg_fan_out={avg_fan_out:.2f}>4.0",
            )

        # --------------------------------------------------------------------
        # Rule 6: holding_queue.avg_dwell_time > 300s
        # -> decrease HOLDING_QUEUE_RETRY_INTERVAL_SEC
        # --------------------------------------------------------------------
        hq_info = sensed.get("holding_queue", {})
        avg_dwell_time = float(hq_info.get("avg_dwell_time", 0.0))
        if avg_dwell_time > 300.0:
            propose_tuning(
                "HOLDING_QUEUE_RETRY_INTERVAL_SEC",
                direction=-1,
                trigger=f"holding_queue:avg_dwell_time={avg_dwell_time:.1f}s>300s",
            )

        return actions

    def apply_actions(self, actions: List[TuningAction]) -> None:
        """Applies computed tuning actions atomically under an exclusive write-lock."""
        if not actions:
            return

        with self.config.write_lock():
            for action in actions:
                try:
                    param = self.config._parameters.get(action.parameter_name)
                    if not param:
                        continue

                    # Determine direction
                    if action.new_value > action.old_value:
                        cur_direction = 1
                    elif action.new_value < action.old_value:
                        cur_direction = -1
                    else:
                        cur_direction = 0

                    if cur_direction != 0 and cur_direction == param._last_direction:
                        param.consecutive_same_direction += 1
                    elif cur_direction != 0:
                        param.consecutive_same_direction = 1

                    param._last_direction = cur_direction
                    param.last_change_tick = self.current_tick
                    param.current_value = action.new_value

                    logger.info(
                        "[TICK %d] Tuned %s: %.3f -> %.3f (Trigger: %s)",
                        self.current_tick,
                        action.parameter_name,
                        action.old_value,
                        action.new_value,
                        action.trigger_signal,
                    )
                except Exception as e:
                    logger.error("Failed applying action %s: %s", action, e)

        # Audit recording outside the write-lock to minimize lock hold duration
        for action in actions:
            self.ledger.append(action)
            with self._history_lock:
                self._history.append(action)

    def tick(self) -> None:
        """Executes a single closed-loop sense-compute-apply-record iteration."""
        self.current_tick += 1
        span_cm = tracer.start_as_current_span("hermes.pipeline.tuner.tick") if tracer else contextlib.nullcontext()

        with span_cm as span:
            if span and hasattr(span, "set_attribute"):
                span.set_attribute("tuner.tick", self.current_tick)

            try:
                # 1. Sense
                sensed = self.sense()

                # Calculate health delta
                health_info = sensed.get("health", {})
                current_health = float(health_info.get("overall_score", health_info.get("health_score", 75.0)))
                if self.last_health_score is not None:
                    self.last_health_delta = current_health - self.last_health_score
                else:
                    self.last_health_delta = 0.0
                self.last_health_score = current_health

                if span and hasattr(span, "set_attribute"):
                    span.set_attribute("tuner.health_score", current_health)
                    span.set_attribute("tuner.health_delta", self.last_health_delta)

                # 2. Compute
                actions = self.compute_actions(sensed)

                if span and hasattr(span, "set_attribute"):
                    span.set_attribute("tuner.actions_count", len(actions))

                # 3. Apply & Record
                self.apply_actions(actions)

            except Exception as e:
                logger.error("Error during tuning control loop tick %d: %s", self.current_tick, e, exc_info=True)
                if span and hasattr(span, "record_exception"):
                    span.record_exception(e)

    def _run_loop(self) -> None:
        """Background daemon thread worker executing periodic ticks."""
        logger.info("Pipeline self-tuner control loop daemon started (interval=%.1fs)", self.tick_interval_sec)
        while not self.shutdown_event.is_set():
            start_time = time.time()
            try:
                self.tick()
            except Exception as e:
                logger.error("Unexpected exception in control loop daemon: %s", e)

            elapsed = time.time() - start_time
            sleep_time = max(0.1, self.tick_interval_sec - elapsed)
            if self.shutdown_event.wait(timeout=sleep_time):
                break
        logger.info("Pipeline self-tuner control loop daemon stopped.")

    def start(self) -> None:
        """Starts the autonomous self-tuning control loop in a background daemon thread."""
        if self.is_running():
            logger.warning("ControlLoop is already running.")
            return
        self.shutdown_event.clear()
        self._worker_thread = threading.Thread(target=self._run_loop, daemon=True, name="PipelineSelfTunerThread")
        self._worker_thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        """Signals shutdown to the background daemon thread and joins with timeout."""
        self.shutdown_event.set()
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=timeout)
        self._worker_thread = None


# ============================================================================
# TunerAPIHandler: Telemetry Inspection & Operator Setpoints
# ============================================================================

class TunerAPIHandler:
    """Provides programmatic API handlers for telemetry dashboards, setpoints, and overrides."""

    def __init__(
        self,
        tuner: Optional[ControlLoop] = None,
        config: Optional[SharedPipelineConfig] = None,
    ) -> None:
        self.tuner = tuner
        self.config = config or SharedPipelineConfig.get_instance()

    def get_state(self) -> Dict[str, Any]:
        """Returns the current state of the self-tuning controller and all parameters."""
        is_running = self.tuner.is_running() if self.tuner else False
        current_tick = self.tuner.current_tick if self.tuner else 0
        last_health = self.tuner.last_health_score if self.tuner else None

        parameters = {
            name: param.to_dict()
            for name, param in self.config.get_all().items()
        }

        setpoints = {
            "target_health": self.tuner.target_health if self.tuner else 75.0,
            "target_throughput": self.tuner.target_throughput if self.tuner else 2.0,
            "target_stability": self.tuner.target_stability if self.tuner else 70.0,
        }

        return {
            "status": "running" if is_running else "stopped",
            "current_tick": current_tick,
            "last_health_score": last_health,
            "setpoints": setpoints,
            "parameters": parameters,
        }

    def get_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Retrieves recent tuning actions from the persistent ledger."""
        if self.tuner:
            return self.tuner.ledger.read_recent(limit=limit)
        return TuningLedger().read_recent(limit=limit)

    def set_override(self, param_name: str, pinned_value: float) -> Dict[str, Any]:
        """Pins a parameter value manually and writes it to dashboards/tuning_override.json."""
        try:
            param = self.config.get_parameter(param_name)
        except KeyError:
            return {"status": "error", "message": f"Unknown parameter '{param_name}'"}

        clamped = SafetyGuard.clamp(param, float(pinned_value))
        override_file = Path(__file__).resolve().parent.parent / "dashboards" / "tuning_override.json"

        try:
            override_file.parent.mkdir(parents=True, exist_ok=True)
            existing_data: Dict[str, Any] = {}
            if override_file.is_file():
                with open(override_file, "r", encoding="utf-8") as f:
                    try:
                        existing_data = json.load(f)
                    except json.JSONDecodeError:
                        existing_data = {}

            existing_data[param_name] = clamped

            with open(override_file, "w", encoding="utf-8") as f:
                json.dump(existing_data, f, indent=2)

            # Apply directly into shared config
            self.config.set(param_name, clamped)

            return {
                "status": "success",
                "parameter": param_name,
                "pinned_value": clamped,
                "file": str(override_file),
            }
        except Exception as e:
            logger.error("Failed to persist override for %s: %s", param_name, e)
            return {"status": "error", "message": str(e)}

    def update_setpoints(self, targets: Dict[str, float]) -> Dict[str, Any]:
        """Updates controller setpoint targets (health, throughput, stability)."""
        if not self.tuner:
            return {"status": "error", "message": "No active ControlLoop attached"}

        updated = {}
        if "target_health" in targets:
            self.tuner.target_health = float(targets["target_health"])
            updated["target_health"] = self.tuner.target_health
        if "target_throughput" in targets:
            self.tuner.target_throughput = float(targets["target_throughput"])
            updated["target_throughput"] = self.tuner.target_throughput
        if "target_stability" in targets:
            self.tuner.target_stability = float(targets["target_stability"])
            updated["target_stability"] = self.tuner.target_stability

        return {
            "status": "success",
            "updated_setpoints": updated,
        }


# ============================================================================
# Public Module Exports
# ============================================================================

__all__ = [
    "TuningParameter",
    "TuningAction",
    "SharedPipelineConfig",
    "SafetyGuard",
    "TuningLedger",
    "ControlLoop",
    "TunerAPIHandler",
    "ReaderWriterLock",
]