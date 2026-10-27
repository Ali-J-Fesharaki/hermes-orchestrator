"""
tasks/pipeline_telemetry_bridge.py - Pipeline Autonomic Telemetry Federation & Phoenix Observability Bridge.

Federates structured tracing and metrics from all autonomic subsystems (Learning Engine,
Simulation Sandbox, Experiment Controller, Calibration Monitor, Loop Supervisor) into the
Arize Phoenix OpenTelemetry instance (default: localhost:6006).

Attributes:
    Namespace: 'autonomic.*'
"""

from __future__ import annotations

import enum
import functools
import json
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, Union

# OpenTelemetry Core API and SDK imports
from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.metrics import CallbackOptions, Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import Span, StatusCode

logger = logging.getLogger("autonomic.telemetry_bridge")


# ---------------------------------------------------------------------------
# Enums and Domain Constants
# ---------------------------------------------------------------------------

class AutonomicPhase(str, enum.Enum):
    PROPOSE = "PROPOSE"
    SIMULATE = "SIMULATE"
    EXPERIMENT = "EXPERIMENT"
    CALIBRATE = "CALIBRATE"
    FEEDBACK = "FEEDBACK"


class InterventionType(str, enum.Enum):
    LEARNING_RATE_DECAY = "LEARNING_RATE_DECAY"
    ENSEMBLE_RESIZE = "ENSEMBLE_RESIZE"
    GATE_WIDEN = "GATE_WIDEN"
    CIRCUIT_BREAK = "CIRCUIT_BREAK"


class ExperimentDecision(str, enum.Enum):
    PROMOTE = "PROMOTE"
    ROLLBACK = "ROLLBACK"
    EXTEND = "EXTEND"


class HealthGrade(str, enum.Enum):
    A = "A"
    B = "B"
    C = "C"
    D = "D"
    F = "F"


GRADE_NUMERIC_MAP = {
    HealthGrade.A: 4.0,
    HealthGrade.B: 3.0,
    HealthGrade.C: 2.0,
    HealthGrade.D: 1.0,
    HealthGrade.F: 0.0,
}


# ---------------------------------------------------------------------------
# Cockpit State Store
# ---------------------------------------------------------------------------

@dataclass
class CockpitState:
    total_cycles: int = 0
    last_cycle_duration_ms: int = 0
    active_phase: str = "IDLE"
    intervention_count_24h: int = 0
    drift_alert_active: bool = False
    token_burn_by_subsystem: Dict[str, int] = field(default_factory=dict)
    health_grade: str = "A"


# Global state singleton for runtime access and cockpit reporting
_GLOBAL_COCKPIT_STATE = CockpitState()


# ---------------------------------------------------------------------------
# AutonomicTelemetryBridge
# ---------------------------------------------------------------------------

class AutonomicTelemetryBridge:
    """
    Federated OpenTelemetry bridge for Arize Phoenix and autonomic pipeline monitoring.
    Configures TracerProvider and MeterProvider targeting OTLP/HTTP endpoints.
    """

    _instance: Optional[AutonomicTelemetryBridge] = None

    def __init__(
        self,
        endpoint: str = "http://localhost:6006",
        service_name: str = "hermes-autonomic-pipeline",
        enable_in_memory: bool = True,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.service_name = service_name
        self.enable_in_memory = enable_in_memory

        self.resource = Resource.create(
            {
                "service.name": self.service_name,
                "service.namespace": "autonomic",
                "instrumentation.provider": "phoenix_bridge",
            }
        )

        # Tracing Setup
        self.tracer_provider = TracerProvider(resource=self.resource)
        self.in_memory_span_exporter: Optional[InMemorySpanExporter] = None

        if self.enable_in_memory:
            self.in_memory_span_exporter = InMemorySpanExporter()
            self.tracer_provider.add_span_processor(
                SimpleSpanProcessor(self.in_memory_span_exporter)
            )

        try:
            otlp_trace_exporter = OTLPSpanExporter(
                endpoint=f"{self.endpoint}/v1/traces",
                timeout=5,
            )
            self.tracer_provider.add_span_processor(
                SimpleSpanProcessor(otlp_trace_exporter)
            )
        except Exception as e:
            logger.warning("Could not initialize OTLP HTTP trace exporter: %s", e)

        trace.set_tracer_provider(self.tracer_provider)
        self.tracer = trace.get_tracer("autonomic.telemetry", "1.0.0")

        # Metrics Setup
        self.in_memory_metric_reader: Optional[InMemoryMetricReader] = None
        readers = []

        if self.enable_in_memory:
            self.in_memory_metric_reader = InMemoryMetricReader()
            readers.append(self.in_memory_metric_reader)

        try:
            otlp_metric_exporter = OTLPMetricExporter(
                endpoint=f"{self.endpoint}/v1/metrics",
                timeout=5,
            )
            readers.append(
                PeriodicExportingMetricReader(
                    otlp_metric_exporter, export_interval_millis=1000
                )
            )
        except Exception as e:
            logger.warning("Could not initialize OTLP HTTP metric exporter: %s", e)

        self.meter_provider = MeterProvider(resource=self.resource, metric_readers=readers)
        metrics.set_meter_provider(self.meter_provider)
        self.meter = metrics.get_meter("autonomic.telemetry", "1.0.0")

        # Sub-components
        self.drift_exporter = DriftMetricsExporter(self.meter)
        self.token_collector = TokenAttributionCollector(self.tracer)
        self.event_emitter = MetaControllerEventEmitter()
        self.summary_endpoint = CockpitTelemetrySummaryEndpoint()

        AutonomicTelemetryBridge._instance = self

    @classmethod
    def get_instance(cls) -> AutonomicTelemetryBridge:
        if cls._instance is None:
            cls._instance = AutonomicTelemetryBridge()
        return cls._instance

    @contextmanager
    def span_autonomic_cycle(
        self,
        cycle_id: int,
        phase: Union[AutonomicPhase, str],
        loop_supervisor_state: str = "STABLE",
    ) -> Generator[Span, None, None]:
        """
        Creates a root span for a complete propose->simulate->experiment->calibrate->feedback cycle.
        """
        phase_str = phase.value if isinstance(phase, AutonomicPhase) else str(phase)
        span_name = f"autonomic.cycle.{phase_str.lower()}"
        start_time_ns = time.time_ns()

        _GLOBAL_COCKPIT_STATE.total_cycles = max(_GLOBAL_COCKPIT_STATE.total_cycles, cycle_id)
        _GLOBAL_COCKPIT_STATE.active_phase = phase_str

        attributes = {
            "autonomic.cycle_id": int(cycle_id),
            "autonomic.phase": phase_str,
            "autonomic.loop_supervisor_state": str(loop_supervisor_state),
            "autonomic.root_cycle": True,
        }

        with self.tracer.start_as_current_span(
            name=span_name,
            attributes=attributes,
            record_exception=True,
            set_status_on_exception=True,
        ) as root_span:
            try:
                yield root_span
                root_span.set_status(StatusCode.OK)
            except Exception as exc:
                root_span.set_status(StatusCode.ERROR, description=str(exc))
                root_span.record_exception(exc)
                raise
            finally:
                duration_ms = int((time.time_ns() - start_time_ns) / 1_000_000)
                root_span.set_attribute("autonomic.duration_ms", duration_ms)
                _GLOBAL_COCKPIT_STATE.last_cycle_duration_ms = duration_ms


# Module-level convenience context manager
def span_autonomic_cycle(
    cycle_id: int,
    phase: Union[AutonomicPhase, str],
    loop_supervisor_state: str = "STABLE",
) -> Any:
    bridge = AutonomicTelemetryBridge.get_instance()
    return bridge.span_autonomic_cycle(cycle_id, phase, loop_supervisor_state)


# ---------------------------------------------------------------------------
# Child Span Decorators
# ---------------------------------------------------------------------------

def trace_learning_engine(parent_span: Optional[Span] = None) -> Callable:
    """
    Decorator capturing learning engine posterior params, proposals, and gradients.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            bridge = AutonomicTelemetryBridge.get_instance()
            context_token = None
            if parent_span is not None:
                context_token = trace.set_span_in_context(parent_span)

            with bridge.tracer.start_as_current_span(
                "autonomic.learning_engine",
                record_exception=True,
                set_status_on_exception=True,
            ) as span:
                try:
                    result = func(*args, **kwargs)

                    payload = result if isinstance(result, dict) else getattr(result, "__dict__", {})
                    posterior = payload.get("posterior_params", {})
                    proposal = payload.get("proposal_vector", [])
                    grad_norm = float(payload.get("gradient_norm", 0.0))
                    convergence = float(payload.get("convergence_metric", 0.0))

                    span.set_attribute("autonomic.learning_engine.posterior_params", json.dumps(posterior))
                    span.set_attribute("autonomic.learning_engine.proposal_vector", json.dumps(proposal))
                    span.set_attribute("autonomic.learning_engine.gradient_norm", grad_norm)
                    span.set_attribute("autonomic.learning_engine.convergence_metric", convergence)

                    span.set_status(StatusCode.OK)
                    return result
                except Exception as exc:
                    span.set_status(StatusCode.ERROR, description=str(exc))
                    span.record_exception(exc)
                    raise
                finally:
                    if context_token is not None:
                        trace.context.detach(context_token)
        return wrapper
    return decorator


def trace_simulation_sandbox(parent_span: Optional[Span] = None) -> Callable:
    """
    Decorator capturing simulation ensemble attributes, counterfactual rewards, and hash.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            bridge = AutonomicTelemetryBridge.get_instance()
            context_token = None
            if parent_span is not None:
                context_token = trace.set_span_in_context(parent_span)

            with bridge.tracer.start_as_current_span(
                "autonomic.simulation_sandbox",
                record_exception=True,
                set_status_on_exception=True,
            ) as span:
                try:
                    result = func(*args, **kwargs)

                    payload = result if isinstance(result, dict) else getattr(result, "__dict__", {})
                    ensemble_size = int(payload.get("ensemble_size", 0))
                    cf_reward_mean = float(payload.get("counterfactual_reward_mean", 0.0))
                    ci_width = float(payload.get("confidence_interval_width", 0.0))
                    pred_hash = str(payload.get("prediction_hash", ""))

                    span.set_attribute("autonomic.simulation_sandbox.ensemble_size", ensemble_size)
                    span.set_attribute("autonomic.simulation_sandbox.counterfactual_reward_mean", cf_reward_mean)
                    span.set_attribute("autonomic.simulation_sandbox.confidence_interval_width", ci_width)
                    span.set_attribute("autonomic.simulation_sandbox.prediction_hash", pred_hash)

                    span.set_status(StatusCode.OK)
                    return result
                except Exception as exc:
                    span.set_status(StatusCode.ERROR, description=str(exc))
                    span.record_exception(exc)
                    raise
                finally:
                    if context_token is not None:
                        trace.context.detach(context_token)
        return wrapper
    return decorator


def trace_experiment_controller(parent_span: Optional[Span] = None) -> Callable:
    """
    Decorator capturing experiment canary metrics, effect sizes, and decisions.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            bridge = AutonomicTelemetryBridge.get_instance()
            context_token = None
            if parent_span is not None:
                context_token = trace.set_span_in_context(parent_span)

            with bridge.tracer.start_as_current_span(
                "autonomic.experiment_controller",
                record_exception=True,
                set_status_on_exception=True,
            ) as span:
                try:
                    result = func(*args, **kwargs)

                    payload = result if isinstance(result, dict) else getattr(result, "__dict__", {})
                    canary = float(payload.get("canary_fraction", 0.0))
                    p_val = float(payload.get("p_value", 1.0))
                    effect = float(payload.get("effect_size", 0.0))
                    raw_decision = payload.get("decision", ExperimentDecision.ROLLBACK)
                    decision = raw_decision.value if isinstance(raw_decision, ExperimentDecision) else str(raw_decision)

                    span.set_attribute("autonomic.experiment_controller.canary_fraction", canary)
                    span.set_attribute("autonomic.experiment_controller.p_value", p_val)
                    span.set_attribute("autonomic.experiment_controller.effect_size", effect)
                    span.set_attribute("autonomic.experiment_controller.decision", decision)

                    span.set_status(StatusCode.OK)
                    return result
                except Exception as exc:
                    span.set_status(StatusCode.ERROR, description=str(exc))
                    span.record_exception(exc)
                    raise
                finally:
                    if context_token is not None:
                        trace.context.detach(context_token)
        return wrapper
    return decorator


def trace_calibration_monitor(parent_span: Optional[Span] = None) -> Callable:
    """
    Decorator capturing calibration drift magnitude, trajectories, and health grades.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            bridge = AutonomicTelemetryBridge.get_instance()
            context_token = None
            if parent_span is not None:
                context_token = trace.set_span_in_context(parent_span)

            with bridge.tracer.start_as_current_span(
                "autonomic.calibration_monitor",
                record_exception=True,
                set_status_on_exception=True,
            ) as span:
                try:
                    result = func(*args, **kwargs)

                    payload = result if isinstance(result, dict) else getattr(result, "__dict__", {})
                    drift = float(payload.get("drift_magnitude", 0.0))
                    trajectory = payload.get("convergence_trajectory", [])
                    bias = float(payload.get("bias_score", 0.0))
                    raw_grade = payload.get("health_grade", HealthGrade.A)
                    grade = raw_grade.value if isinstance(raw_grade, HealthGrade) else str(raw_grade)

                    span.set_attribute("autonomic.calibration_monitor.drift_magnitude", drift)
                    span.set_attribute("autonomic.calibration_monitor.convergence_trajectory", json.dumps(trajectory))
                    span.set_attribute("autonomic.calibration_monitor.bias_score", bias)
                    span.set_attribute("autonomic.calibration_monitor.health_grade", grade)

                    # Update global cockpit state
                    _GLOBAL_COCKPIT_STATE.health_grade = grade
                    if drift > 0.35 or grade in ("D", "F"):
                        _GLOBAL_COCKPIT_STATE.drift_alert_active = True
                    else:
                        _GLOBAL_COCKPIT_STATE.drift_alert_active = False

                    span.set_status(StatusCode.OK)
                    return result
                except Exception as exc:
                    span.set_status(StatusCode.ERROR, description=str(exc))
                    span.record_exception(exc)
                    raise
                finally:
                    if context_token is not None:
                        trace.context.detach(context_token)
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# MetaControllerEventEmitter
# ---------------------------------------------------------------------------

class MetaControllerEventEmitter:
    """
    Translates Loop Supervisor interventions into OTel Span Events and aggregates 24h count.
    """

    def __init__(self) -> None:
        self.interventions_history: List[Dict[str, Any]] = []

    def emit_intervention(
        self,
        span: Optional[Span],
        intervention_type: Union[InterventionType, str],
        trigger_metric: str,
        old_value: float,
        new_value: float,
        rationale: str,
    ) -> Dict[str, Any]:
        """
        Emits an OpenTelemetry span event for an intervention action.
        """
        itype = intervention_type.value if isinstance(intervention_type, InterventionType) else str(intervention_type)
        payload = {
            "autonomic.intervention.type": itype,
            "autonomic.intervention.trigger_metric": str(trigger_metric),
            "autonomic.intervention.old_value": float(old_value),
            "autonomic.intervention.new_value": float(new_value),
            "autonomic.intervention.rationale": str(rationale),
            "timestamp": time.time(),
        }

        target_span = span or trace.get_current_span()
        if target_span and target_span.is_recording():
            target_span.add_event(
                name="autonomic.loop_supervisor.intervention",
                attributes={
                    "intervention_type": itype,
                    "trigger_metric": str(trigger_metric),
                    "old_value": float(old_value),
                    "new_value": float(new_value),
                    "rationale": str(rationale),
                },
            )

        self.interventions_history.append(payload)
        _GLOBAL_COCKPIT_STATE.intervention_count_24h += 1
        return payload


# ---------------------------------------------------------------------------
# TokenAttributionCollector
# ---------------------------------------------------------------------------

class TokenAttributionCollector:
    """
    Intercepts LLM API calls within autonomic subsystems and attributes prompt/completion tokens,
    model ID, and latency to the enclosing span for granular cost accounting.
    """

    def __init__(self, tracer: trace.Tracer) -> None:
        self.tracer = tracer
        self.subsystem_token_burn: Dict[str, int] = {}

    def record_usage(
        self,
        subsystem: str,
        prompt_tokens: int,
        completion_tokens: int,
        model_id: str,
        latency_ms: float,
        span: Optional[Span] = None,
    ) -> None:
        """
        Records token usage metrics onto current or specified span and updates cockpit aggregation.
        """
        total = int(prompt_tokens + completion_tokens)
        current_burn = self.subsystem_token_burn.get(subsystem, 0)
        self.subsystem_token_burn[subsystem] = current_burn + total

        # Sync to cockpit state
        _GLOBAL_COCKPIT_STATE.token_burn_by_subsystem[subsystem] = self.subsystem_token_burn[subsystem]

        target_span = span or trace.get_current_span()
        if target_span and target_span.is_recording():
            target_span.set_attribute(f"autonomic.token.{subsystem}.prompt_tokens", int(prompt_tokens))
            target_span.set_attribute(f"autonomic.token.{subsystem}.completion_tokens", int(completion_tokens))
            target_span.set_attribute(f"autonomic.token.{subsystem}.total_tokens", total)
            target_span.set_attribute(f"autonomic.token.{subsystem}.model_id", str(model_id))
            target_span.set_attribute(f"autonomic.token.{subsystem}.latency_ms", float(latency_ms))

    @contextmanager
    def capture_call(
        self,
        subsystem: str,
        model_id: str,
        span: Optional[Span] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Context manager to time and attribute an LLM invocation.
        Yields a dict that the caller can populate with prompt_tokens and completion_tokens.
        """
        usage_carrier: Dict[str, Any] = {"prompt_tokens": 0, "completion_tokens": 0}
        t0 = time.time()
        try:
            yield usage_carrier
        finally:
            dt_ms = (time.time() - t0) * 1000.0
            self.record_usage(
                subsystem=subsystem,
                prompt_tokens=int(usage_carrier.get("prompt_tokens", 0)),
                completion_tokens=int(usage_carrier.get("completion_tokens", 0)),
                model_id=model_id,
                latency_ms=dt_ms,
                span=span,
            )


# ---------------------------------------------------------------------------
# DriftMetricsExporter
# ---------------------------------------------------------------------------

class DriftMetricsExporter:
    """
    Exports Calibration Monitor drift signals using OpenTelemetry Metrics SDK Gauge instruments.
    """

    def __init__(self, meter: metrics.Meter) -> None:
        self.meter = meter

        # Latest gauge state stores
        self._mae_records: List[Tuple[float, Dict[str, Any]]] = []
        self._convergence_records: List[Tuple[float, Dict[str, Any]]] = []
        self._bias_records: List[Tuple[float, Dict[str, Any]]] = []
        self._grade_records: List[Tuple[float, Dict[str, Any]]] = []

        # Create observable gauges
        self.meter.create_observable_gauge(
            name="autonomic.drift.simulation_reality_mae",
            callbacks=[self._observe_mae],
            description="Mean Absolute Error between simulated predictions and reality",
            unit="error_rate",
        )

        self.meter.create_observable_gauge(
            name="autonomic.drift.convergence_rate",
            callbacks=[self._observe_convergence],
            description="Convergence rate of learning engine parameter trajectory",
            unit="rate",
        )

        self.meter.create_observable_gauge(
            name="autonomic.drift.promotion_bias",
            callbacks=[self._observe_bias],
            description="Systematic bias in canary promotion decisions",
            unit="bias",
        )

        self.meter.create_observable_gauge(
            name="autonomic.health.loop_grade",
            callbacks=[self._observe_grade],
            description="Overall autonomic loop health grade (A=4, B=3, C=2, D=1, F=0)",
            unit="grade_pts",
        )

    def _observe_mae(self, options: CallbackOptions) -> Iterable[Observation]:
        return [Observation(val, attrs) for val, attrs in self._mae_records]

    def _observe_convergence(self, options: CallbackOptions) -> Iterable[Observation]:
        return [Observation(val, attrs) for val, attrs in self._convergence_records]

    def _observe_bias(self, options: CallbackOptions) -> Iterable[Observation]:
        return [Observation(val, attrs) for val, attrs in self._bias_records]

    def _observe_grade(self, options: CallbackOptions) -> Iterable[Observation]:
        return [Observation(val, attrs) for val, attrs in self._grade_records]

    def record_drift_metrics(
        self,
        simulation_reality_mae: float,
        convergence_rate: float,
        promotion_bias: float,
        health_grade: Union[HealthGrade, str],
        subsystem: str = "calibration_monitor",
        cycle_id: int = 0,
        window_size: int = 10,
    ) -> None:
        """
        Updates the gauge readings with dimensions: {subsystem, cycle_id, window_size}.
        """
        attrs = {
            "subsystem": str(subsystem),
            "cycle_id": int(cycle_id),
            "window_size": int(window_size),
        }

        grade_enum = (
            health_grade
            if isinstance(health_grade, HealthGrade)
            else HealthGrade(str(health_grade).upper())
        )
        grade_num = GRADE_NUMERIC_MAP.get(grade_enum, 0.0)

        self._mae_records = [(float(simulation_reality_mae), attrs)]
        self._convergence_records = [(float(convergence_rate), attrs)]
        self._bias_records = [(float(promotion_bias), attrs)]
        self._grade_records = [(grade_num, attrs)]

        _GLOBAL_COCKPIT_STATE.health_grade = grade_enum.value
        _GLOBAL_COCKPIT_STATE.drift_alert_active = (
            simulation_reality_mae > 0.35 or grade_enum in (HealthGrade.D, HealthGrade.F)
        )


# ---------------------------------------------------------------------------
# CockpitTelemetrySummaryEndpoint
# ---------------------------------------------------------------------------

class CockpitTelemetrySummaryEndpoint:
    """
    Exposes GET /api/autonomic/telemetry/summary returning real-time health and token metrics.
    """

    def get_summary(self) -> Dict[str, Any]:
        """
        Returns JSON payload matching schema:
            total_cycles (int)
            last_cycle_duration_ms (int)
            active_phase (str)
            intervention_count_24h (int)
            drift_alert_active (bool)
            token_burn_by_subsystem (dict[str, int])
            health_grade (str)
        """
        return {
            "total_cycles": int(_GLOBAL_COCKPIT_STATE.total_cycles),
            "last_cycle_duration_ms": int(_GLOBAL_COCKPIT_STATE.last_cycle_duration_ms),
            "active_phase": str(_GLOBAL_COCKPIT_STATE.active_phase),
            "intervention_count_24h": int(_GLOBAL_COCKPIT_STATE.intervention_count_24h),
            "drift_alert_active": bool(_GLOBAL_COCKPIT_STATE.drift_alert_active),
            "token_burn_by_subsystem": dict(_GLOBAL_COCKPIT_STATE.token_burn_by_subsystem),
            "health_grade": str(_GLOBAL_COCKPIT_STATE.health_grade),
        }

    def handle_request(self, path: str, method: str = "GET") -> Tuple[int, Dict[str, Any]]:
        """
        Dispatches incoming API requests.
        """
        if method.upper() == "GET" and path == "/api/autonomic/telemetry/summary":
            return 200, self.get_summary()
        return 404, {"error": "Not Found", "path": path}


# ---------------------------------------------------------------------------
# Subsystem Hook Registration
# ---------------------------------------------------------------------------

def register_telemetry_hooks(
    learning_engine: Any,
    simulation_sandbox: Any,
    experiment_controller: Any,
    calibration_monitor: Any,
    loop_supervisor: Any,
) -> None:
    """
    Monkey-patches or wraps each autonomic subsystem's entry points with the corresponding trace decorators.
    """
    # 1. Learning Engine Hook
    if hasattr(learning_engine, "propose"):
        orig_propose = learning_engine.propose
        setattr(learning_engine, "propose", trace_learning_engine()(orig_propose))
    elif hasattr(learning_engine, "step"):
        orig_step = learning_engine.step
        setattr(learning_engine, "step", trace_learning_engine()(orig_step))

    # 2. Simulation Sandbox Hook
    if hasattr(simulation_sandbox, "simulate"):
        orig_sim = simulation_sandbox.simulate
        setattr(simulation_sandbox, "simulate", trace_simulation_sandbox()(orig_sim))
    elif hasattr(simulation_sandbox, "evaluate"):
        orig_eval = simulation_sandbox.evaluate
        setattr(simulation_sandbox, "evaluate", trace_simulation_sandbox()(orig_eval))

    # 3. Experiment Controller Hook
    if hasattr(experiment_controller, "evaluate_canary"):
        orig_canary = experiment_controller.evaluate_canary
        setattr(experiment_controller, "evaluate_canary", trace_experiment_controller()(orig_canary))
    elif hasattr(experiment_controller, "step"):
        orig_step_exp = experiment_controller.step
        setattr(experiment_controller, "step", trace_experiment_controller()(orig_step_exp))

    # 4. Calibration Monitor Hook
    if hasattr(calibration_monitor, "measure_drift"):
        orig_drift = calibration_monitor.measure_drift
        setattr(calibration_monitor, "measure_drift", trace_calibration_monitor()(orig_drift))
    elif hasattr(calibration_monitor, "step"):
        orig_step_cal = calibration_monitor.step
        setattr(calibration_monitor, "step", trace_calibration_monitor()(orig_step_cal))

    # 5. Loop Supervisor Interventions Hook
    if hasattr(loop_supervisor, "apply_intervention"):
        orig_intervene = loop_supervisor.apply_intervention
        bridge = AutonomicTelemetryBridge.get_instance()

        @functools.wraps(orig_intervene)
        def wrapped_intervene(*args: Any, **kwargs: Any) -> Any:
            res = orig_intervene(*args, **kwargs)
            itype = kwargs.get("intervention_type", getattr(res, "intervention_type", InterventionType.LEARNING_RATE_DECAY))
            tmetric = kwargs.get("trigger_metric", "loss_plateau")
            old_val = float(kwargs.get("old_value", 0.01))
            new_val = float(kwargs.get("new_value", 0.005))
            rationale = str(kwargs.get("rationale", "Autonomic meta-controller optimization"))

            bridge.event_emitter.emit_intervention(
                span=trace.get_current_span(),
                intervention_type=itype,
                trigger_metric=tmetric,
                old_value=old_val,
                new_value=new_val,
                rationale=rationale,
            )
            return res

        setattr(loop_supervisor, "apply_intervention", wrapped_intervene)


# ---------------------------------------------------------------------------
# Self-Test Verification Suite
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 80)
    print("RUNNING SELF-TEST: Pipeline Autonomic Telemetry Federation & Phoenix Bridge")
    print("=" * 80)

    # 1. Initialize Telemetry Bridge
    bridge = AutonomicTelemetryBridge(enable_in_memory=True)
    exporter = bridge.in_memory_span_exporter
    assert exporter is not None, "InMemorySpanExporter must be enabled for verification"
    exporter.clear()

    # 2. Synthetic Subsystem Mock Implementations
    class MockLearningEngine:
        def propose(self, cycle_id: int) -> Dict[str, Any]:
            return {
                "posterior_params": {"theta_mu": 0.42, "theta_sigma": 0.05},
                "proposal_vector": [0.12, 0.88, 0.45],
                "gradient_norm": 0.035,
                "convergence_metric": 0.982,
            }

    class MockSimulationSandbox:
        def simulate(self, cycle_id: int) -> Dict[str, Any]:
            return {
                "ensemble_size": 16,
                "counterfactual_reward_mean": 0.845,
                "confidence_interval_width": 0.042,
                "prediction_hash": f"sha256_mock_cycle_{cycle_id}",
            }

    class MockExperimentController:
        def evaluate_canary(self, cycle_id: int) -> Dict[str, Any]:
            return {
                "canary_fraction": 0.10,
                "p_value": 0.0041,
                "effect_size": 0.32,
                "decision": ExperimentDecision.PROMOTE,
            }

    class MockCalibrationMonitor:
        def measure_drift(self, cycle_id: int) -> Dict[str, Any]:
            return {
                "drift_magnitude": 0.082,
                "convergence_trajectory": [1.0, 0.45, 0.12, 0.082],
                "bias_score": 0.015,
                "health_grade": HealthGrade.A,
            }

    class MockLoopSupervisor:
        def apply_intervention(
            self,
            intervention_type: InterventionType,
            trigger_metric: str,
            old_value: float,
            new_value: float,
            rationale: str,
        ) -> Dict[str, Any]:
            return {"status": "applied", "intervention_type": intervention_type}

    le = MockLearningEngine()
    sim = MockSimulationSandbox()
    exp = MockExperimentController()
    cal = MockCalibrationMonitor()
    sup = MockLoopSupervisor()

    # Register hooks
    register_telemetry_hooks(le, sim, exp, cal, sup)

    # 3. Simulate 3 Complete Autonomic Cycles
    phases = [
        AutonomicPhase.PROPOSE,
        AutonomicPhase.SIMULATE,
        AutonomicPhase.EXPERIMENT,
        AutonomicPhase.CALIBRATE,
        AutonomicPhase.FEEDBACK,
    ]

    print("[*] Simulating 3 full autonomic loop cycles...")
    for cycle_num in range(1, 4):
        for phase in phases:
            with bridge.span_autonomic_cycle(
                cycle_id=cycle_num,
                phase=phase,
                loop_supervisor_state="OPTIMAL",
            ) as root_span:
                if phase == AutonomicPhase.PROPOSE:
                    le.propose(cycle_id=cycle_num)
                    # Simulate LLM attribution
                    with bridge.token_collector.capture_call(
                        subsystem="learning_engine",
                        model_id="gemini-3.8-flash-tiered",
                        span=root_span,
                    ) as carrier:
                        time.sleep(0.01)
                        carrier["prompt_tokens"] = 450
                        carrier["completion_tokens"] = 120

                elif phase == AutonomicPhase.SIMULATE:
                    sim.simulate(cycle_id=cycle_num)

                elif phase == AutonomicPhase.EXPERIMENT:
                    exp.evaluate_canary(cycle_id=cycle_num)

                elif phase == AutonomicPhase.CALIBRATE:
                    cal.measure_drift(cycle_id=cycle_num)
                    # Export drift metrics gauge
                    bridge.drift_exporter.record_drift_metrics(
                        simulation_reality_mae=0.082 + 0.01 * cycle_num,
                        convergence_rate=0.95 - 0.02 * cycle_num,
                        promotion_bias=0.012,
                        health_grade=HealthGrade.A,
                        subsystem="calibration_monitor",
                        cycle_id=cycle_num,
                        window_size=10,
                    )

                elif phase == AutonomicPhase.FEEDBACK:
                    # Trigger supervisor intervention on cycle 2
                    if cycle_num == 2:
                        sup.apply_intervention(
                            intervention_type=InterventionType.LEARNING_RATE_DECAY,
                            trigger_metric="drift.simulation_reality_mae",
                            old_value=0.01,
                            new_value=0.005,
                            rationale="Plateau dampening via autonomic meta-controller",
                        )
                    # Track Opus planning tokens
                    bridge.token_collector.record_usage(
                        subsystem="loop_supervisor",
                        prompt_tokens=800,
                        completion_tokens=250,
                        model_id="claude-opus-4-6-thinking",
                        latency_ms=185.0,
                        span=root_span,
                    )

    # 4. Verify Exported Spans
    finished_spans = exporter.get_finished_spans()
    span_names = [s.name for s in finished_spans]
    print(f"[*] Verified finished span count: {len(finished_spans)}")

    # 3 cycles * 5 phases = 15 root spans, plus 4 child subsystem spans per cycle = 12 child spans -> at least 27 spans
    assert len(finished_spans) >= 27, f"Expected at least 27 spans, got {len(finished_spans)}"

    # Check root attributes
    root_cycles = [s for s in finished_spans if s.attributes.get("autonomic.root_cycle")]
    assert len(root_cycles) == 15, f"Expected 15 root cycle spans, found {len(root_cycles)}"
    assert all("autonomic.cycle_id" in s.attributes for s in root_cycles)
    assert all("autonomic.phase" in s.attributes for s in root_cycles)

    # Check child span attributes
    le_spans = [s for s in finished_spans if s.name == "autonomic.learning_engine"]
    assert len(le_spans) == 3, f"Expected 3 learning engine spans, got {len(le_spans)}"
    assert "autonomic.learning_engine.gradient_norm" in le_spans[0].attributes
    assert "autonomic.learning_engine.posterior_params" in le_spans[0].attributes

    sim_spans = [s for s in finished_spans if s.name == "autonomic.simulation_sandbox"]
    assert len(sim_spans) == 3
    assert sim_spans[0].attributes["autonomic.simulation_sandbox.ensemble_size"] == 16

    exp_spans = [s for s in finished_spans if s.name == "autonomic.experiment_controller"]
    assert len(exp_spans) == 3
    assert exp_spans[0].attributes["autonomic.experiment_controller.decision"] == "PROMOTE"

    cal_spans = [s for s in finished_spans if s.name == "autonomic.calibration_monitor"]
    assert len(cal_spans) == 3
    assert cal_spans[0].attributes["autonomic.calibration_monitor.health_grade"] == "A"

    # Check supervisor intervention events
    feedback_spans = [s for s in root_cycles if s.attributes.get("autonomic.phase") == "FEEDBACK"]
    intervention_events = []
    for s in feedback_spans:
        intervention_events.extend(s.events)
    assert len(intervention_events) >= 1, "Expected at least 1 intervention event in span history"
    assert intervention_events[0].name == "autonomic.loop_supervisor.intervention"
    print(f"[*] Verified Intervention event attributes: {dict(intervention_events[0].attributes)}")

    # 5. Verify Metric Gauge Values
    assert bridge.drift_exporter._mae_records, "Drift MAE records must be populated"
    latest_mae, latest_mae_dims = bridge.drift_exporter._mae_records[0]
    assert latest_mae == 0.082 + 0.01 * 3
    assert latest_mae_dims["cycle_id"] == 3
    assert latest_mae_dims["subsystem"] == "calibration_monitor"
    print(f"[*] Verified Gauge: autonomic.drift.simulation_reality_mae = {latest_mae} with dims {latest_mae_dims}")

    # 6. Verify Cockpit Telemetry Summary Endpoint
    status_code, summary = bridge.summary_endpoint.handle_request("/api/autonomic/telemetry/summary")
    assert status_code == 200, f"Expected status code 200, got {status_code}"
    print("[*] Verified GET /api/autonomic/telemetry/summary:")
    print(json.dumps(summary, indent=2))

    # Validate Schema Types
    assert isinstance(summary["total_cycles"], int) and summary["total_cycles"] == 3
    assert isinstance(summary["last_cycle_duration_ms"], int) and summary["last_cycle_duration_ms"] >= 0
    assert summary["active_phase"] == "FEEDBACK"
    assert summary["intervention_count_24h"] >= 1
    assert summary["drift_alert_active"] is False
    assert summary["health_grade"] == "A"
    assert "learning_engine" in summary["token_burn_by_subsystem"]
    assert "loop_supervisor" in summary["token_burn_by_subsystem"]
    assert summary["token_burn_by_subsystem"]["learning_engine"] == (450 + 120) * 3
    assert summary["token_burn_by_subsystem"]["loop_supervisor"] == (800 + 250) * 3

    print("=" * 80)
    print("ALL TESTS PASSED: Autonomic Telemetry Bridge successfully validated.")
    print("=" * 80)