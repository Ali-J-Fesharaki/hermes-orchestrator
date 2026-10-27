"""
tasks/pipeline_predictive_analytics.py
Hermes Orchestrator - Predictive Pipeline Analytics & Anomaly Forecasting Engine
================================================================================
Implements time-series anomaly detection, Holt-Winters throughput forecasting,
preemptive parameter adjustment dispatching, and predictive pipeline health scoring.
"""

from __future__ import annotations

import collections
from dataclasses import asdict, dataclass, field
import itertools
import json
import logging
import math
import os
import threading
import time
from typing import Any
import uuid

# OpenTelemetry tracing integration (with transparent fallback)
try:
    from opentelemetry import trace
    _TRACER = trace.get_tracer("hermes.pipeline.predictive")
except ImportError:
    class _DummySpan:
        def __enter__(self) -> "_DummySpan":
            return self

        def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            pass

        def set_attribute(self, key: str, value: Any) -> None:
            pass

        def record_exception(self, exception: Exception) -> None:
            pass

    class _DummyTracer:
        def start_as_current_span(self, name: str, *args: Any, **kwargs: Any) -> _DummySpan:
            return _DummySpan()

    _TRACER = _DummyTracer()

logger = logging.getLogger("hermes.pipeline.predictive")

__all__ = [
    "MetricPoint",
    "TimeSeriesStore",
    "ZScoreDetector",
    "EWMADetector",
    "TrendDetector",
    "AnomalyVerdict",
    "AnomalyDetector",
    "HoltWintersModel",
    "ForecastReport",
    "ThroughputForecaster",
    "PreemptiveAction",
    "PreemptiveActionDispatcher",
    "PredictiveHealthContributor",
    "PredictiveAnalyticsAPIHandler",
    "PredictiveAnalyticsEngine",
]


# ==============================================================================
# 1. Metric Point Data Structure
# ==============================================================================

@dataclass
class MetricPoint:
    """Represents a single timestamped metric observation."""
    timestamp: float
    value: float
    labels: dict[str, str] = field(default_factory=dict)


# ==============================================================================
# 2. TimeSeriesStore (Ring Buffer Store)
# ==============================================================================

class TimeSeriesStore:
    """
    In-memory time-series store maintaining per-metric RingBuffers (collections.deque)
    keyed by (stage_name, metric_type) tuples with a default maxlen of 7200 samples.
    """

    def __init__(self, maxlen: int = 7200) -> None:
        self.maxlen = maxlen
        self._buffers: dict[tuple[str, str], collections.deque[MetricPoint]] = {}
        self._lock = threading.RLock()

    def ingest(
        self,
        stage_name: str,
        metric_type: str,
        value: float,
        labels: dict[str, str] | None = None,
    ) -> MetricPoint:
        """Appends a new observation with the current system timestamp."""
        key = (stage_name, metric_type)
        point = MetricPoint(
            timestamp=time.time(),
            value=float(value),
            labels=dict(labels) if labels is not None else {},
        )
        with self._lock:
            if key not in self._buffers:
                self._buffers[key] = collections.deque(maxlen=self.maxlen)
            self._buffers[key].append(point)
        return point

    def query(
        self,
        stage_name: str,
        metric_type: str,
        window: int | None = None,
    ) -> list[MetricPoint]:
        """
        Retrieves up to `window` most recent MetricPoints for the given key.
        Returns all buffered points if window is None.
        """
        key = (stage_name, metric_type)
        with self._lock:
            if key not in self._buffers:
                return []
            buf = self._buffers[key]
            if window is None or window >= len(buf):
                return list(buf)
            return list(itertools.islice(buf, len(buf) - window, len(buf)))

    def keys(self) -> list[tuple[str, str]]:
        """Returns all registered (stage_name, metric_type) metric keys."""
        with self._lock:
            return list(self._buffers.keys())


# ==============================================================================
# 3. Anomaly Detectors: ZScore, EWMA, Trend
# ==============================================================================

class ZScoreDetector:
    """
    Standard Score Anomaly Detector.
    μ = mean(buffer[-baseline_window:]), σ = std(buffer[-baseline_window:])
    Anomaly if |x - μ| > z_threshold * σ.
    """

    def __init__(self, baseline_window: int = 60, z_threshold: float = 2.5) -> None:
        self.baseline_window = baseline_window
        self.z_threshold = z_threshold

    def detect(self, samples: list[MetricPoint]) -> bool:
        if len(samples) < 2:
            return False

        window = samples[-self.baseline_window:]
        vals = [p.value for p in window]
        n = len(vals)
        mu = sum(vals) / n
        variance = sum((v - mu) ** 2 for v in vals) / n
        sigma = math.sqrt(variance)

        if sigma < 1e-9:
            return False

        latest_x = vals[-1]
        return abs(latest_x - mu) > (self.z_threshold * sigma)


class EWMADetector:
    """
    Exponentially Weighted Moving Average & Variance Detector.
    ewma_t = α * x_t + (1-α) * ewma_{t-1}
    ewma_var_t = α * (x_t - ewma_t)² + (1-α) * ewma_var_{t-1}
    Anomaly if |x_t - ewma_t| > ewma_band_width * √ewma_var_t.
    """

    def __init__(self, alpha: float = 0.3, band_width: float = 2.0) -> None:
        self.alpha = alpha
        self.band_width = band_width

    def detect(self, samples: list[MetricPoint]) -> bool:
        if len(samples) < 2:
            return False

        vals = [p.value for p in samples]
        ewma = vals[0]
        ewma_var = 0.0

        for x in vals[1:]:
            prev_ewma = ewma
            ewma = self.alpha * x + (1.0 - self.alpha) * prev_ewma
            diff = x - ewma
            ewma_var = self.alpha * (diff ** 2) + (1.0 - self.alpha) * ewma_var

        latest_x = vals[-1]
        sigma = math.sqrt(max(0.0, ewma_var))
        if sigma < 1e-9:
            return False

        return abs(latest_x - ewma) > (self.band_width * sigma)


class TrendDetector:
    """
    Least-squares linear trend projection detector (numpy-free).
    y = a + b*t over trend_window samples.
    projected = a + b * (t + forecast_horizon).
    Anomaly if projected > threshold_upper or projected < threshold_lower.
    """

    def __init__(self, trend_window: int = 30, forecast_horizon: int = 15) -> None:
        self.trend_window = trend_window
        self.forecast_horizon = forecast_horizon

    def detect(
        self,
        samples: list[MetricPoint],
        upper_threshold: float | None = None,
        lower_threshold: float | None = None,
    ) -> tuple[bool, float]:
        if not samples:
            return False, 0.0

        latest_val = samples[-1].value
        if len(samples) < 2:
            is_anom = False
            if upper_threshold is not None and latest_val > upper_threshold:
                is_anom = True
            if lower_threshold is not None and latest_val < lower_threshold:
                is_anom = True
            return is_anom, float(latest_val)

        window = samples[-self.trend_window:]
        n = len(window)
        y = [p.value for p in window]

        # Time coordinates t = 0, 1, ..., n-1
        sum_t = (n * (n - 1)) / 2.0
        sum_tt = (n * (n - 1) * (2 * n - 1)) / 6.0
        sum_y = sum(y)
        sum_ty = sum(i * val for i, val in enumerate(y))

        denom = (n * sum_tt) - (sum_t * sum_t)
        if abs(denom) < 1e-12:
            b = 0.0
            a = sum_y / n
        else:
            b = ((n * sum_ty) - (sum_t * sum_y)) / denom
            a = (sum_y - (b * sum_t)) / n

        t_projected = (n - 1) + self.forecast_horizon
        projected_value = a + (b * t_projected)

        is_anomaly = False
        if upper_threshold is not None and projected_value > upper_threshold:
            is_anomaly = True
        if lower_threshold is not None and projected_value < lower_threshold:
            is_anomaly = True

        return is_anomaly, float(projected_value)


# ==============================================================================
# 4. Anomaly Verdict & Multi-Strategy Ensemble Detector
# ==============================================================================

@dataclass
class AnomalyVerdict:
    """Ensemble verdict emitted when 2 or more anomaly detectors agree."""
    metric_key: tuple[str, str]
    current_value: float
    projected_value: float
    time_to_breach_ticks: int
    severity: str  # "WARNING" (2 detectors) or "CRITICAL" (3 detectors)
    contributing_detectors: list[str]
    recommended_preemptive_action: str
    detected_at: float = field(default_factory=time.time)
    verdict_id: str = field(default_factory=lambda: str(uuid.uuid4()))


class AnomalyDetector:
    """
    Orchestrates ZScoreDetector, EWMADetector, and TrendDetector over all registered metrics.
    Emits an AnomalyVerdict when sum(detector_flags) >= 2.
    """

    def __init__(
        self,
        z_score_detector: ZScoreDetector | None = None,
        ewma_detector: EWMADetector | None = None,
        trend_detector: TrendDetector | None = None,
    ) -> None:
        self.z_detector = z_score_detector or ZScoreDetector()
        self.ewma_detector = ewma_detector or EWMADetector()
        self.trend_detector = trend_detector or TrendDetector()

    def detect_anomalies(
        self,
        store: TimeSeriesStore,
        thresholds: dict[str | tuple[str, str], dict[str, float]],
    ) -> list[AnomalyVerdict]:
        verdicts: list[AnomalyVerdict] = []

        for key in store.keys():
            samples = store.query(key[0], key[1])
            if len(samples) < 3:
                continue

            stage_name, metric_type = key
            # Locate threshold configuration
            thresh = (
                thresholds.get(key)
                or thresholds.get(metric_type)
                or thresholds.get(f"{stage_name}.{metric_type}")
                or {}
            )
            upper = thresh.get("upper", thresh.get("threshold_upper"))
            lower = thresh.get("lower", thresh.get("threshold_lower"))

            # Execute the 3 strategies
            flag_z = self.z_detector.detect(samples)
            flag_ewma = self.ewma_detector.detect(samples)
            flag_trend, projected_val = self.trend_detector.detect(samples, upper, lower)

            contributing: list[str] = []
            if flag_z:
                contributing.append("ZScoreDetector")
            if flag_ewma:
                contributing.append("EWMADetector")
            if flag_trend:
                contributing.append("TrendDetector")

            vote_count = len(contributing)
            if vote_count >= 2:
                severity = "CRITICAL" if vote_count >= 3 else "WARNING"
                current_val = samples[-1].value

                # Estimate ticks to breach threshold
                time_to_breach = self.trend_detector.forecast_horizon
                if flag_trend:
                    time_to_breach = max(1, self.trend_detector.forecast_horizon // 2)

                action_hint = self._derive_action_hint(metric_type, current_val, projected_val)

                verdict = AnomalyVerdict(
                    metric_key=key,
                    current_value=float(current_val),
                    projected_value=float(projected_val),
                    time_to_breach_ticks=time_to_breach,
                    severity=severity,
                    contributing_detectors=contributing,
                    recommended_preemptive_action=action_hint,
                    detected_at=time.time(),
                )
                verdicts.append(verdict)

        return verdicts

    @staticmethod
    def _derive_action_hint(metric_type: str, current_val: float, projected_val: float) -> str:
        increasing = projected_val >= current_val
        m = metric_type.lower()
        if "latency" in m:
            return "dampen_concurrency" if increasing else "relax_concurrency"
        elif "error" in m:
            return "increase_retry_backoff" if increasing else "reset_retry_backoff"
        elif "queue" in m:
            return "scale_worker_pool" if increasing else "coalesce_worker_pool"
        elif "cpu" in m or "memory" in m:
            return "throttle_batch_size" if increasing else "expand_batch_size"
        elif "throughput" in m:
            return "boost_stage_capacity" if not increasing else "maintain_throughput"
        return "preemptive_stabilization"


# ==============================================================================
# 5. Holt-Winters Additive Forecaster & Throughput Model
# ==============================================================================

class HoltWintersModel:
    """
    Additive Holt-Winters Double Exponential Smoothing (Level + Trend).
    level_t = α * y_t + (1-α) * (level_{t-1} + trend_{t-1})
    trend_t = β * (level_t - level_{t-1}) + (1-β) * trend_{t-1}
    forecast_{t+h} = level_t + h * trend_t
    Confidence intervals: CI_p = forecast ± z_p * residual_std * √h
    """

    def __init__(self, alpha: float = 0.3, beta: float = 0.1) -> None:
        self.alpha = alpha
        self.beta = beta
        self.level: float = 0.0
        self.trend: float = 0.0
        self.residuals: list[float] = []
        self.residual_std: float = 0.0

    def fit(self, values: list[float]) -> "HoltWintersModel":
        if not values:
            self.level = 0.0
            self.trend = 0.0
            self.residual_std = 0.0
            return self

        if len(values) == 1:
            self.level = values[0]
            self.trend = 0.0
            self.residual_std = 0.0
            return self

        # Initialize level and trend
        self.level = values[0]
        self.trend = values[1] - values[0]
        self.residuals = [0.0]

        for t in range(1, len(values)):
            y_t = values[t]
            fitted_t = self.level + self.trend
            prev_level = self.level

            # Update level and trend equations
            self.level = self.alpha * y_t + (1.0 - self.alpha) * (prev_level + self.trend)
            self.trend = self.beta * (self.level - prev_level) + (1.0 - self.beta) * self.trend

            res = y_t - fitted_t
            self.residuals.append(res)

        # Compute standard deviation over last 60 residuals
        recent_residuals = self.residuals[-60:]
        n = len(recent_residuals)
        if n > 1:
            mean_res = sum(recent_residuals) / n
            var_res = sum((r - mean_res) ** 2 for r in recent_residuals) / (n - 1)
            self.residual_std = math.sqrt(max(0.0, var_res))
        else:
            self.residual_std = 0.0

        return self

    def forecast(self, horizon: int) -> list[float]:
        """Generates point forecasts for horizons 1 through `horizon`."""
        return [self.level + (h * self.trend) for h in range(1, horizon + 1)]

    def confidence_intervals(self, horizon: int, confidence: float = 0.8) -> tuple[float, float]:
        """Computes confidence interval bounds for a specific horizon step."""
        z_p = 1.28 if confidence <= 0.85 else 1.96
        point = self.level + (horizon * self.trend)
        margin = z_p * self.residual_std * math.sqrt(horizon)
        return (point - margin, point + margin)


@dataclass
class ForecastReport:
    """Consolidated throughput forecast output."""
    point_estimates: list[float]
    ci_80_lower: list[float]
    ci_80_upper: list[float]
    ci_95_lower: list[float]
    ci_95_upper: list[float]
    estimated_queue_completion_sec: float


class ThroughputForecaster:
    """Predicts pipeline throughput and estimated backlog clearance time."""

    def __init__(self, alpha: float = 0.3, beta: float = 0.1) -> None:
        self.model = HoltWintersModel(alpha=alpha, beta=beta)

    def forecast(self, store: TimeSeriesStore, forecast_window: int = 30) -> ForecastReport:
        # Locate throughput samples across registered metrics
        throughput_samples: list[MetricPoint] = []
        queue_samples: list[MetricPoint] = []

        for stage, mtype in store.keys():
            if "throughput" in mtype.lower():
                throughput_samples = store.query(stage, mtype, window=120)
            elif "queue" in mtype.lower() or "backlog" in mtype.lower():
                queue_samples = store.query(stage, mtype, window=60)

        # Fallback if no throughput series is explicitly named
        if not throughput_samples:
            for stage, mtype in store.keys():
                throughput_samples = store.query(stage, mtype, window=120)
                if throughput_samples:
                    break

        y_values = [p.value for p in throughput_samples] if throughput_samples else [10.0] * 10
        self.model.fit(y_values)
        points = self.model.forecast(forecast_window)

        ci_80_l: list[float] = []
        ci_80_u: list[float] = []
        ci_95_l: list[float] = []
        ci_95_u: list[float] = []

        for h in range(1, forecast_window + 1):
            pt = points[h - 1]
            m80 = 1.28 * self.model.residual_std * math.sqrt(h)
            m95 = 1.96 * self.model.residual_std * math.sqrt(h)
            ci_80_l.append(pt - m80)
            ci_80_u.append(pt + m80)
            ci_95_l.append(pt - m95)
            ci_95_u.append(pt + m95)

        # Estimate queue completion time
        current_queue = queue_samples[-1].value if queue_samples else 0.0
        avg_forecast = max(0.1, sum(points) / len(points)) if points else 1.0
        completion_sec = (current_queue / avg_forecast) * 60.0 if current_queue > 0 else 0.0

        return ForecastReport(
            point_estimates=points,
            ci_80_lower=ci_80_l,
            ci_80_upper=ci_80_u,
            ci_95_lower=ci_95_l,
            ci_95_upper=ci_95_u,
            estimated_queue_completion_sec=round(completion_sec, 2),
        )


# ==============================================================================
# 6. Preemptive Action Dispatcher & Efficacy Tracking
# ==============================================================================

@dataclass
class PreemptiveAction:
    """Preemptive operational control parameter adjustment."""
    action_id: str
    target_parameter: str
    current_value: float
    suggested_value: float
    preemptive_confidence: float
    source_verdict_id: str
    dispatched_at: float
    outcome_after_applied: str = ""


class PreemptiveActionDispatcher:
    """
    Translates AnomalyVerdicts into proactive configuration adjustments
    using static parameter mappings and dampening heuristics.
    Tracks efficacy to evaluate preemptive defense success.
    """

    ACTION_MAPPINGS: dict[str, tuple[str, str]] = {
        "latency": ("concurrency_limit", "decrease"),
        "error_rate": ("retry_backoff_sec", "increase"),
        "queue_depth": ("worker_pool_size", "increase"),
        "cpu_utilization": ("batch_size", "decrease"),
        "memory_rss": ("cache_ttl_sec", "decrease"),
        "throughput": ("batch_size", "increase"),
    }

    BASE_STEPS: dict[str, float] = {
        "concurrency_limit": 4.0,
        "retry_backoff_sec": 1.5,
        "worker_pool_size": 2.0,
        "batch_size": 10.0,
        "cache_ttl_sec": 60.0,
    }

    def __init__(self, preemptive_dampening_factor: float = 0.5) -> None:
        self.preemptive_dampening_factor = preemptive_dampening_factor
        self.dispatched_history: list[dict[str, Any]] = []
        self.reactive_triggers: list[dict[str, Any]] = []
        self._lock = threading.RLock()

    def dispatch(
        self,
        verdicts: list[AnomalyVerdict],
        current_config: dict[str, Any],
    ) -> list[PreemptiveAction]:
        actions: list[PreemptiveAction] = []

        with self._lock:
            for v in verdicts:
                _, metric_type = v.metric_key
                mapping = self._find_mapping(metric_type)
                if not mapping:
                    continue

                target_param, direction = mapping
                curr_val = float(current_config.get(target_param, 50.0))
                base_step = self.BASE_STEPS.get(target_param, max(1.0, curr_val * 0.15))
                effective_step = base_step * self.preemptive_dampening_factor

                if direction == "decrease":
                    suggested = max(1.0, curr_val - effective_step)
                else:
                    suggested = curr_val + effective_step

                confidence = 0.95 if v.severity == "CRITICAL" else 0.75
                action_obj = PreemptiveAction(
                    action_id=str(uuid.uuid4()),
                    target_parameter=target_param,
                    current_value=curr_val,
                    suggested_value=round(suggested, 2),
                    preemptive_confidence=confidence,
                    source_verdict_id=v.verdict_id,
                    dispatched_at=time.time(),
                )
                actions.append(action_obj)
                self.dispatched_history.append({
                    "action": action_obj,
                    "metric_key": v.metric_key,
                    "dispatched_at": action_obj.dispatched_at,
                    "evaluated": False,
                    "efficacy_score": 0,
                })

        return actions

    def _find_mapping(self, metric_type: str) -> tuple[str, str] | None:
        m = metric_type.lower()
        for key, mapping in self.ACTION_MAPPINGS.items():
            if key in m:
                return mapping
        return None

    def record_reactive_trigger(
        self,
        stage_name: str,
        metric_type: str,
        trigger_type: str = "breaker_trip",
    ) -> None:
        """Records reactive safety trips for post-action efficacy correlation."""
        with self._lock:
            self.reactive_triggers.append({
                "stage_name": stage_name,
                "metric_type": metric_type,
                "trigger_type": trigger_type,
                "timestamp": time.time(),
            })

    def evaluate_efficacy(self, forecast_horizon_sec: float = 300.0) -> float:
        """
        Calculates ratio of preemptive actions that averted reactive trips.
        If no reactive trigger occurred within forecast_horizon_sec, efficacy += 1.
        """
        now = time.time()
        with self._lock:
            total_evaluated = 0
            successful = 0

            for entry in self.dispatched_history:
                if entry["evaluated"]:
                    total_evaluated += 1
                    successful += entry["efficacy_score"]
                    continue

                age = now - entry["dispatched_at"]
                if age >= forecast_horizon_sec:
                    entry["evaluated"] = True
                    total_evaluated += 1
                    act = entry["action"]
                    stage, mtype = entry["metric_key"]

                    # Check for correlating reactive triggers
                    had_reactive = any(
                        trig["stage_name"] == stage
                        and trig["metric_type"] == mtype
                        and (entry["dispatched_at"] <= trig["timestamp"] <= entry["dispatched_at"] + forecast_horizon_sec)
                        for trig in self.reactive_triggers
                    )

                    if not had_reactive:
                        entry["efficacy_score"] = 1
                        act.outcome_after_applied = "Averted Reactive Incident"
                        successful += 1
                    else:
                        entry["efficacy_score"] = 0
                        act.outcome_after_applied = "Reactive Incident Occurred"

            return (successful / total_evaluated) if total_evaluated > 0 else 1.0

    def log_actions(
        self,
        actions: list[PreemptiveAction],
        filepath: str = "dashboards/predictive_actions.jsonl",
    ) -> None:
        """
        Appends actions to JSON-lines log file using an atomic write pattern
        (writes full updated stream to .tmp then os.replace).
        """
        if not actions:
            return

        os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
        with self._lock:
            existing_lines: list[str] = []
            if os.path.exists(filepath):
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        existing_lines = f.readlines()
                except OSError as err:
                    logger.warning("Could not read existing actions log: %s", err)

            new_lines = [json.dumps(asdict(act)) + "\n" for act in actions]
            combined = existing_lines + new_lines

            tmp_path = f"{filepath}.tmp.{uuid.uuid4().hex}"
            try:
                with open(tmp_path, "w", encoding="utf-8") as f:
                    f.writelines(combined)
                os.replace(tmp_path, filepath)
            except Exception as err:
                logger.error("Failed atomic replace for actions log %s: %s", filepath, err)
                if os.path.exists(tmp_path):
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        pass


# ==============================================================================
# 7. Predictive Health Contributor
# ==============================================================================

class PredictiveHealthContributor:
    """
    Computes unified composite predictive health score (0 - 100).
    predictive_sub_score = 0.4 * anomaly_load + 0.3 * forecast_confidence + 0.3 * efficacy
    """

    def compute_score(
        self,
        active_anomalies: int,
        forecast_confidence_width: float,
        preemptive_efficacy_ratio: float,
    ) -> float:
        # 1. Anomaly Load Score (100 is pristine, penalize 20 pts per active anomaly)
        anomaly_load_score = max(0.0, 100.0 - (float(active_anomalies) * 20.0))

        # 2. Forecast Confidence Score (tighter CI -> higher confidence)
        if forecast_confidence_width <= 1.0:
            forecast_confidence_score = (1.0 - max(0.0, forecast_confidence_width)) * 100.0
        else:
            forecast_confidence_score = max(0.0, min(100.0, 100.0 - forecast_confidence_width))

        # 3. Preemptive Efficacy Score
        efficacy_score = min(100.0, max(0.0, preemptive_efficacy_ratio * 100.0))

        # Weighted aggregate
        sub_score = (
            (0.4 * anomaly_load_score)
            + (0.3 * forecast_confidence_score)
            + (0.3 * efficacy_score)
        )
        return round(max(0.0, min(100.0, sub_score)), 2)


# ==============================================================================
# 8. API Handler
# ==============================================================================

class PredictiveAnalyticsAPIHandler:
    """Provides REST / RPC control interface for analytics inspection and reconfiguration."""

    def __init__(
        self,
        detector: AnomalyDetector,
        forecaster: ThroughputForecaster,
        dispatcher: PreemptiveActionDispatcher,
        store: TimeSeriesStore,
        engine: "PredictiveAnalyticsEngine | None" = None,
    ) -> None:
        self.detector = detector
        self.forecaster = forecaster
        self.dispatcher = dispatcher
        self.store = store
        self.engine = engine

    def handle_get_anomalies(self, query_params: dict[str, Any] | None = None) -> dict[str, Any]:
        params = query_params or {}
        thresholds = self.engine.thresholds if self.engine else {}
        verdicts = self.detector.detect_anomalies(self.store, thresholds)

        severity_filter = params.get("severity")
        if severity_filter:
            verdicts = [v for v in verdicts if v.severity.upper() == severity_filter.upper()]

        return {
            "status": "success",
            "count": len(verdicts),
            "anomalies": [asdict(v) for v in verdicts],
        }

    def handle_get_forecast(self, query_params: dict[str, Any] | None = None) -> dict[str, Any]:
        params = query_params or {}
        window = int(params.get("forecast_window", 30))
        report = self.forecaster.forecast(self.store, forecast_window=window)
        return {
            "status": "success",
            "forecast": asdict(report),
        }

    def handle_get_actions(self, query_params: dict[str, Any] | None = None) -> dict[str, Any]:
        with self.dispatcher._lock:
            actions = [asdict(entry["action"]) for entry in self.dispatcher.dispatched_history]
        return {
            "status": "success",
            "count": len(actions),
            "actions": actions,
        }

    def handle_post_config(self, body: dict[str, Any]) -> dict[str, Any]:
        if not body or not isinstance(body, dict):
            return {"status": "error", "message": "Invalid JSON configuration payload"}

        if self.engine:
            if "tick_interval" in body:
                self.engine.tick_interval = int(body["tick_interval"])
            if "thresholds" in body and isinstance(body["thresholds"], dict):
                self.engine.thresholds.update(body["thresholds"])
            if "current_config" in body and isinstance(body["current_config"], dict):
                self.engine.current_config.update(body["current_config"])

        if "preemptive_dampening_factor" in body:
            self.dispatcher.preemptive_dampening_factor = float(body["preemptive_dampening_factor"])

        return {
            "status": "success",
            "message": "Predictive analytics configuration updated",
        }


# ==============================================================================
# 9. Predictive Analytics Engine (Sense-Detect-Forecast-Dispatch Cycle)
# ==============================================================================

class PredictiveAnalyticsEngine:
    """
    Core orchestrating engine executing autonomous sense-detect-forecast-dispatch cycles
    with background thread scheduling and OpenTelemetry tracing.
    """

    def __init__(
        self,
        tick_interval: int = 120,
        config: dict[str, Any] | None = None,
    ) -> None:
        self.tick_interval = tick_interval
        self.config = config or {}

        # Subsystems
        self.store = TimeSeriesStore(maxlen=self.config.get("buffer_maxlen", 7200))
        self.detector = AnomalyDetector()
        self.forecaster = ThroughputForecaster()
        self.dispatcher = PreemptiveActionDispatcher(
            preemptive_dampening_factor=self.config.get("preemptive_dampening_factor", 0.5)
        )
        self.health_contributor = PredictiveHealthContributor()
        self.api_handler = PredictiveAnalyticsAPIHandler(
            detector=self.detector,
            forecaster=self.forecaster,
            dispatcher=self.dispatcher,
            store=self.store,
            engine=self,
        )

        # Configurable setpoints and current pipeline controls
        self.thresholds: dict[str | tuple[str, str], dict[str, float]] = self.config.get(
            "thresholds",
            {
                "latency": {"upper": 50.0, "lower": 0.0},
                "error_rate": {"upper": 0.05, "lower": 0.0},
                "queue_depth": {"upper": 100.0, "lower": 0.0},
                "cpu_utilization": {"upper": 85.0, "lower": 0.0},
            },
        )
        self.current_config: dict[str, Any] = self.config.get(
            "current_config",
            {
                "concurrency_limit": 16.0,
                "retry_backoff_sec": 2.0,
                "worker_pool_size": 8.0,
                "batch_size": 50.0,
                "cache_ttl_sec": 300.0,
            },
        )

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._latest_health_score: float = 100.0

    def start(self) -> None:
        """Starts the autonomous predictive analytics background thread."""
        if self._thread and self._thread.is_alive():
            logger.warning("PredictiveAnalyticsEngine is already running.")
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="HermesPredictiveAnalyticsThread",
            daemon=True,
        )
        self._thread.start()
        logger.info("PredictiveAnalyticsEngine background thread started (interval=%ds).", self.tick_interval)

    def stop(self) -> None:
        """Stops the autonomous predictive analytics background thread gracefully."""
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)
        logger.info("PredictiveAnalyticsEngine background thread stopped.")

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._tick()
            except Exception as e:
                logger.error("Error encountered during analytics tick: %s", e, exc_info=True)
            self._stop_event.wait(self.tick_interval)

    def _tick(self) -> dict[str, Any]:
        """Executes a single sense-detect-forecast-dispatch cycle wrapped in an OTel span."""
        with _TRACER.start_as_current_span("hermes.pipeline.predictive.tick") as span:
            cycle_start = time.time()
            span.set_attribute("cycle.start_timestamp", cycle_start)

            # 1. Sense & Detect Anomalies
            verdicts = self.detector.detect_anomalies(self.store, self.thresholds)
            span.set_attribute("anomalies.detected_count", len(verdicts))

            # 2. Forecast Pipeline Throughput
            forecast_report = self.forecaster.forecast(self.store, forecast_window=30)
            avg_throughput = (
                sum(forecast_report.point_estimates) / len(forecast_report.point_estimates)
                if forecast_report.point_estimates
                else 0.0
            )
            span.set_attribute("forecast.projected_avg_throughput", avg_throughput)

            # 3. Dispatch Preemptive Actions
            actions = self.dispatcher.dispatch(verdicts, self.current_config)
            span.set_attribute("actions.dispatched_count", len(actions))

            # Log actions using atomic JSON-lines writer
            self.dispatcher.log_actions(actions)

            # 4. Efficacy & Health Contribution
            efficacy_ratio = self.dispatcher.evaluate_efficacy(forecast_horizon_sec=120.0)

            # Compute normalized confidence width (CI_95 band width relative to mean)
            if forecast_report.point_estimates and avg_throughput > 0:
                h_last = len(forecast_report.point_estimates) - 1
                ci_width = (
                    forecast_report.ci_95_upper[h_last] - forecast_report.ci_95_lower[h_last]
                )
                norm_ci_width = min(100.0, (ci_width / avg_throughput) * 20.0)
            else:
                norm_ci_width = 10.0

            health_score = self.health_contributor.compute_score(
                active_anomalies=len(verdicts),
                forecast_confidence_width=norm_ci_width,
                preemptive_efficacy_ratio=efficacy_ratio,
            )
            self._latest_health_score = health_score
            span.set_attribute("pipeline.predictive_health_score", health_score)

            duration = time.time() - cycle_start
            logger.info(
                "Predictive tick completed in %.3fs | Anomalies: %d | Actions: %d | "
                "Efficacy: %.2f | Health Score: %.2f",
                duration,
                len(verdicts),
                len(actions),
                efficacy_ratio,
                health_score,
            )

            return {
                "anomalies": verdicts,
                "forecast": forecast_report,
                "actions": actions,
                "health_score": health_score,
                "duration_sec": duration,
            }


# ==============================================================================
# Self-Execution & Demonstration Harness
# ==============================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
    )

    print("=" * 80)
    print("Hermes Orchestrator - Predictive Pipeline Analytics Engine Harness")
    print("=" * 80)

    # Instantiate engine with high-frequency tick interval for harness verification
    engine = PredictiveAnalyticsEngine(tick_interval=1)

    # Seed baseline time series to exercise anomaly detection and forecasting
    now = time.time()
    stages = ["intake", "transformation", "dispatch"]
    print("\n[+] Ingesting historical baseline time-series data...")

    for stage in stages:
        for idx in range(120):
            ts = now - (120 - idx) * 2.0
            # Steady baseline throughput with mild positive trend
            tp_value = 50.0 + (idx * 0.25) + ((idx % 4) * 0.5)
            engine.store.ingest(stage, "throughput", tp_value, labels={"stage": stage})

            # Queue depth: rising backlog towards tail of series
            backlog_value = 15.0 + (idx * 0.4 if idx > 70 else 2.0)
            engine.store.ingest(stage, "queue_depth", backlog_value, labels={"stage": stage})

            # Latency: inject sustained spike on transformation stage to trigger multi-detector consensus
            lat_value = 22.0 + (idx % 3)
            if stage == "transformation" and idx >= 112:
                lat_value += 65.0  # Deviation exceeding Z-Score and EWMA thresholds
            engine.store.ingest(stage, "latency", lat_value, labels={"stage": stage})

    print("[+] Historical ingestion complete. Executing 3 consecutive analytics ticks...")

    for tick_number in range(1, 4):
        print(f"\n>>> Running Predictive Analytics Tick {tick_number} of 3 <<<")
        tick_summary = engine._tick()

        print(f"  • Active Anomaly Verdicts: {len(tick_summary['anomalies'])}")
        for verdict in tick_summary["anomalies"]:
            print(
                f"    - Key: {verdict.metric_key} | Severity: {verdict.severity} | "
                f"Current: {verdict.current_value:.2f} | Projected: {verdict.projected_value:.2f} | "
                f"Detectors: {verdict.contributing_detectors}"
            )

        print(f"  • Preemptive Actions Dispatched: {len(tick_summary['actions'])}")
        for action in tick_summary["actions"]:
            print(
                f"    - Parameter: {action.target_parameter} | Current: {action.current_value} "
                f"-> Suggested: {action.suggested_value} | Confidence: {action.preemptive_confidence * 100:.0f}%"
            )

        fc = tick_summary["forecast"]
        p_est = fc.point_estimates[:5]
        print(f"  • Throughput Forecast (Next 5 steps): {[round(x, 2) for x in p_est]}")
        print(f"  • Backlog Clearance Estimate: {fc.estimated_queue_completion_sec:.1f}s")
        print(f"  • Unified Predictive Health Sub-Score: {tick_summary['health_score']:.2f}/100.0")

        time.sleep(0.5)

    engine.stop()
    print("\n[+] Verification successful: 3 predictive analytics ticks completed cleanly.")
    print("=" * 80)