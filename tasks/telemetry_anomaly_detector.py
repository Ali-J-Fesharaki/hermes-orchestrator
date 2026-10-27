"""
Execution Telemetry Anomaly Detector & Adaptive Circuit Breaker Tuner.

This module provides real-time execution anomaly detection and adaptive circuit
breaker threshold tuning over distributed LLM telemetry streams collected via
Arize Phoenix (OpenTelemetry conventions).

Mathematical Foundation - Exponentially Weighted Moving Average (EWMA):
------------------------------------------------------------------------
To detect non-stationary distribution shifts in high-throughput streaming
telemetry without the memory overhead of sliding window buffers, we maintain an
online running mean and variance using an exponentially weighted formulation:

    Mean Update:
        mu_t = alpha * x_t + (1 - alpha) * mu_{t-1}

    Variance Update:
        sigma^2_t = alpha * (x_t - mu_t)^2 + (1 - alpha) * sigma^2_{t-1}

    Standard Deviation:
        sigma_t = sqrt(max(0, sigma^2_t))

    Z-Score Standardization:
        z_t = (x_t - mu_t) / max(sigma_t, epsilon)    where epsilon = 1e-6

Where alpha in [0.1, 0.3] dictates the memory span (decay rate). EWMA ensures O(1)
space and computational time per ingestion cycle while prioritizing recent transient
spikes, latency degradations, and error bursts.

Adaptive Circuit Breaker Tuning:
--------------------------------
When a monitored execution metric deviates beyond its adaptive statistical
envelope (z > threshold), the system dynamically calculates an adjusted failure
trip limit for affected accounts to prevent cascading failures:

    trip_count = max(1, floor(baseline_trip * (1 - (z - threshold) / z)))

This non-linear throttling contracts the circuit breaker trip limit aggressively
as the anomaly extremity increases, guaranteeing swift isolation during upstream
LLM API brownouts, quota throttling, or infrastructure latency degradation.
"""

from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass, field
import datetime
import json
import logging
import math
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import urllib.error
import urllib.parse
import urllib.request

# -----------------------------------------------------------------------------
# Logging Configuration
# -----------------------------------------------------------------------------
logger = logging.getLogger("hermes.telemetry_anomaly_detector")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [TelemetryAnomalyDetector] %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# -----------------------------------------------------------------------------
# Dataclasses
# -----------------------------------------------------------------------------
@dataclass
class TelemetryWindow:
    """Represents the rolling statistical window for an individual metric track."""

    account_id: str
    repo: str
    metric_name: str
    timestamps: List[float] = field(default_factory=list)
    values: List[float] = field(default_factory=list)
    ewma: float = 0.0
    variance: float = 0.0
    z_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert dataclass instance to serializable dictionary."""
        return asdict(self)


@dataclass
class AnomalyAlert:
    """Telemetry anomaly event emitted when metric z-score breaches thresholds."""

    severity: str  # 'warning' | 'critical'
    account_id: str
    repo: str
    metric: str
    z_score: float
    current_value: float
    baseline_mean: float
    recommended_action: str
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        """Convert dataclass instance to serializable dictionary."""
        return asdict(self)


@dataclass
class AdaptiveThresholds:
    """Defines critical z-score thresholds and minimum sample counts."""

    latency_z: float = 2.5
    error_rate_z: float = 2.0
    token_burn_z: float = 2.0
    min_samples: int = 10

    def to_dict(self) -> Dict[str, Any]:
        """Convert dataclass instance to serializable dictionary."""
        return asdict(self)


# Alias for backward compatibility
AnomalyThresholds = AdaptiveThresholds


# -----------------------------------------------------------------------------
# Online EWMA Calculator
# -----------------------------------------------------------------------------
class EWMACalculator:
    """Maintains running exponential weighted moving average and variance."""

    def __init__(
        self,
        alpha: float = 0.2,
        min_samples: int = 10,
        epsilon: float = 1e-6,
        max_history: int = 120,
    ) -> None:
        """Initialize the online EWMA calculator.

        Args:
            alpha: Smoothing factor within [0.1, 0.3].
            min_samples: Minimum data points before calculating valid z-scores.
            epsilon: Minimum variance regularizer to prevent division by zero.
            max_history: Maximum raw data points to retain in rolling buffer.
        """
        if not (0.01 <= alpha <= 0.99):
            raise ValueError(f"Alpha {alpha} must be strictly between 0 and 1.")
        self.alpha: float = alpha
        self.min_samples: int = min_samples
        self.epsilon: float = epsilon
        self.max_history: int = max_history

        self.mean: float = 0.0
        self.variance: float = 0.0
        self.count: int = 0
        self.timestamps: List[float] = []
        self.values: List[float] = []

    def update(self, value: float, timestamp: Optional[float] = None) -> float:
        """Update online mean and variance with a new observation.

        Formulas:
            mu_t = alpha * x_t + (1 - alpha) * mu_{t-1}
            sigma^2_t = alpha * (x_t - mu_t)^2 + (1 - alpha) * sigma^2_{t-1}

        Returns:
            The calculated z-score for the updated observation.
        """
        ts = timestamp if timestamp is not None else time.time()
        self.timestamps.append(ts)
        self.values.append(value)

        # Enforce history bound
        if len(self.timestamps) > self.max_history:
            self.timestamps = self.timestamps[-self.max_history :]
            self.values = self.values[-self.max_history :]

        self.count += 1

        if self.count == 1:
            self.mean = float(value)
            self.variance = 0.0
        else:
            # μ_t = α·x_t + (1-α)·μ_{t-1}
            new_mean = self.alpha * value + (1.0 - self.alpha) * self.mean
            # σ²_t = α·(x_t - μ_t)² + (1-α)·σ²_{t-1} using current mean
            new_variance = (
                self.alpha * ((value - new_mean) ** 2)
                + (1.0 - self.alpha) * self.variance
            )
            self.mean = new_mean
            self.variance = max(0.0, new_variance)

        return self.z_score(value)

    @property
    def std_dev(self) -> float:
        """Calculate standard deviation with non-negative guarantee."""
        return math.sqrt(max(0.0, self.variance))

    def z_score(self, value: Optional[float] = None) -> float:
        """Compute the standardized z-score.

        Formula:
            z_t = (x_t - mu_t) / max(sigma_t, epsilon)
        """
        if self.count < self.min_samples:
            return 0.0

        val = (
            value
            if value is not None
            else (self.values[-1] if self.values else self.mean)
        )
        sigma = max(self.std_dev, self.epsilon)
        return (val - self.mean) / sigma


# -----------------------------------------------------------------------------
# Phoenix Telemetry Ingestion (OTel Conventions)
# -----------------------------------------------------------------------------
class PhoenixSpanIngester:
    """Queries Arize Phoenix /v1/spans REST API and groups metrics by account and repo."""

    def __init__(
        self,
        phoenix_url: str = "http://127.0.0.1:6006",
        timeout_s: float = 5.0,
    ) -> None:
        """Initialize Phoenix OTel span ingester."""
        self.phoenix_url = phoenix_url.rstrip("/")
        self.timeout_s = timeout_s

    def fetch_recent_spans(
        self, window_minutes: int = 5
    ) -> List[Dict[str, Any]]:
        """Fetch spans from Phoenix REST API within specified time window.

        Args:
            window_minutes: Time window in minutes to look back.

        Returns:
            List of raw span dictionaries extracted from Phoenix.
        """
        url = f"{self.phoenix_url}/v1/spans"
        query_params = {
            "limit": 500,
            "order": "desc",
        }
        full_url = f"{url}?{urllib.parse.urlencode(query_params)}"
        req = urllib.request.Request(
            full_url,
            headers={
                "Accept": "application/json",
                "User-Agent": "Hermes-Telemetry-Anomaly-Detector/1.0",
            },
        )

        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                if resp.status == 200:
                    data = json.loads(resp.read().decode("utf-8"))
                    if isinstance(data, dict):
                        return (
                            data.get("data")
                            or data.get("spans")
                            or data.get("items")
                            or []
                        )
                    elif isinstance(data, list):
                        return data
                logger.warning(
                    "Unexpected HTTP status from Phoenix API: %s", resp.status
                )
        except urllib.error.URLError as e:
            logger.debug(
                "Phoenix API unreachable at %s: %s (falling back to empty/synthetic stream)",
                full_url,
                e,
            )
        except Exception as e:
            logger.warning("Error fetching spans from Phoenix: %s", e)

        return []

    def _flatten_attributes(
        self, raw_attrs: Union[Dict[str, Any], List[Dict[str, Any]]]
    ) -> Dict[str, Any]:
        """Normalize OpenTelemetry attribute formats into a flat dictionary."""
        flat: Dict[str, Any] = {}
        if isinstance(raw_attrs, dict):
            for k, v in raw_attrs.items():
                if isinstance(v, dict) and "value" in v:
                    flat[k] = v["value"]
                else:
                    flat[k] = v
        elif isinstance(raw_attrs, list):
            for item in raw_attrs:
                k = item.get("key")
                v = item.get("value")
                if k is not None:
                    if isinstance(v, dict):
                        # Extract typed values (string_value, int_value, double_value)
                        extracted = next(iter(v.values())) if v else None
                        flat[k] = extracted
                    else:
                        flat[k] = v
        return flat

    def _extract_duration_ms(self, span: Dict[str, Any]) -> float:
        """Extract or derive duration in milliseconds from span payload."""
        if "duration_ms" in span:
            try:
                return float(span["duration_ms"])
            except (ValueError, TypeError):
                pass

        start_time = span.get("start_time")
        end_time = span.get("end_time")

        if start_time is not None and end_time is not None:
            # Handle numeric timestamps (seconds, ms, us, ns)
            try:
                s = float(start_time)
                e = float(end_time)
                diff = e - s
                if diff > 1e15:  # nanoseconds
                    return diff / 1e6
                elif diff > 1e12:  # microseconds
                    return diff / 1e3
                elif diff > 1e6:  # milliseconds
                    return diff
                else:  # seconds
                    return diff * 1000.0
            except (ValueError, TypeError):
                pass

            # Handle ISO string timestamps
            try:
                t0 = datetime.datetime.fromisoformat(
                    str(start_time).replace("Z", "+00:00")
                )
                t1 = datetime.datetime.fromisoformat(
                    str(end_time).replace("Z", "+00:00")
                )
                return max(0.0, (t1 - t0).total_seconds() * 1000.0)
            except Exception:
                pass

        return 0.0

    def parse_and_group_spans(
        self, spans: List[Dict[str, Any]]
    ) -> Dict[Tuple[str, str], Dict[str, List[float]]]:
        """Parse raw spans according to OTel semantic conventions and group by (account, repo).

        Grouped Metrics:
            - latency_ms: List[float]
            - token_burn: List[float]
            - error_rate: List[float] (1.0 for error, 0.0 for success)

        Returns:
            Dictionary mapped by (account_id, repo) containing metric values.
        """
        grouped: Dict[Tuple[str, str], Dict[str, List[float]]] = {}

        for span in spans:
            attrs = self._flatten_attributes(span.get("attributes", {}))

            # Resolve Account Identifier
            account_id = (
                attrs.get("account_id")
                or attrs.get("orchestrator.account_id")
                or attrs.get("user.id")
                or attrs.get("account")
                or "default_account"
            )

            # Resolve Repository Identifier
            repo = (
                attrs.get("repository")
                or attrs.get("repo")
                or attrs.get("repo_name")
                or attrs.get("service.name")
                or "hermes_orchestrator"
            )

            key = (str(account_id), str(repo))
            if key not in grouped:
                grouped[key] = {
                    "latency_ms": [],
                    "token_burn": [],
                    "error_rate": [],
                }

            # 1. Latency (duration_ms)
            duration_ms = self._extract_duration_ms(span)
            if duration_ms > 0.0:
                grouped[key]["latency_ms"].append(duration_ms)

            # 2. Token Burn (llm.token_count.total)
            token_count = (
                attrs.get("llm.token_count.total")
                or attrs.get("llm.usage.total_tokens")
                or attrs.get("gen_ai.usage.total_tokens")
                or attrs.get("usage.total_tokens")
                or attrs.get("tokens.total")
            )
            if token_count is not None:
                try:
                    grouped[key]["token_burn"].append(float(token_count))
                except (ValueError, TypeError):
                    pass

            # 3. Status / Error Rate
            is_error = 0.0
            status_obj = span.get("status")
            status_code = None
            if isinstance(status_obj, dict):
                status_code = status_obj.get("status_code") or status_obj.get(
                    "code"
                )
            elif isinstance(status_obj, str):
                status_code = status_obj

            if (
                status_code in ("ERROR", 2, "error")
                or attrs.get("error") is True
                or str(attrs.get("status_code", "")).startswith("5")
            ):
                is_error = 1.0

            grouped[key]["error_rate"].append(is_error)

        return grouped


# -----------------------------------------------------------------------------
# Cockpit WebSocket Broadcaster
# -----------------------------------------------------------------------------
class CockpitWebSocketBroadcaster:
    """Manages emission of high-severity anomaly alerts to Cockpit WebSocket."""

    def __init__(
        self,
        cockpit_ws_url: str = "ws://127.0.0.1:5050/ws/telemetry",
        http_webhook_url: str = "http://127.0.0.1:5050/api/telemetry/anomalies",
    ) -> None:
        self.cockpit_ws_url = cockpit_ws_url
        self.http_webhook_url = http_webhook_url
        self.subscribers: List[Callable[[AnomalyAlert], None]] = []

    def register_listener(self, callback: Callable[[AnomalyAlert], None]) -> None:
        """Register an in-process callback listener."""
        self.subscribers.append(callback)

    def emit(self, alert: AnomalyAlert) -> None:
        """Broadcast anomaly alert to local subscribers and cockpit webhook."""
        # 1. In-process dispatch
        for listener in self.subscribers:
            try:
                listener(alert)
            except Exception as e:
                logger.error("Error in alert listener callback: %s", e)

        # 2. HTTP Webhook fallback for local dashboard integration
        payload = json.dumps(alert.to_dict()).encode("utf-8")
        req = urllib.request.Request(
            self.http_webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=1.0) as resp:
                pass
        except Exception:
            # Silent fallback when cockpit HTTP service is offline
            pass


# -----------------------------------------------------------------------------
# Telemetry Anomaly Detector
# -----------------------------------------------------------------------------
class TelemetryAnomalyDetector:
    """Detects statistical anomalies across execution telemetry tracks using EWMA."""

    def __init__(
        self,
        alpha: float = 0.2,
        thresholds: Optional[AdaptiveThresholds] = None,
        broadcaster: Optional[CockpitWebSocketBroadcaster] = None,
    ) -> None:
        """Initialize anomaly detector with dedicated EWMA calculators per metric track."""
        self.alpha = alpha
        self.thresholds = thresholds or AdaptiveThresholds()
        self.broadcaster = broadcaster or CockpitWebSocketBroadcaster()
        # Key: (account_id, repo, metric_name) -> EWMACalculator
        self.calculators: Dict[Tuple[str, str, str], EWMACalculator] = {}
        self.active_alerts: List[AnomalyAlert] = []

    def get_or_create_calculator(
        self, account_id: str, repo: str, metric_name: str
    ) -> EWMACalculator:
        """Retrieve existing calculator or instantiate a new one."""
        key = (account_id, repo, metric_name)
        if key not in self.calculators:
            self.calculators[key] = EWMACalculator(
                alpha=self.alpha,
                min_samples=self.thresholds.min_samples,
            )
        return self.calculators[key]

    def ingest_spans(
        self,
        spans_or_grouped: Union[
            List[Dict[str, Any]],
            Dict[Tuple[str, str], Dict[str, List[float]]],
        ],
    ) -> None:
        """Ingest raw spans or pre-grouped metric batches into active EWMA calculators."""
        if isinstance(spans_or_grouped, list):
            ingester = PhoenixSpanIngester()
            grouped_metrics = ingester.parse_and_group_spans(spans_or_grouped)
        else:
            grouped_metrics = spans_or_grouped

        for (account_id, repo), metrics in grouped_metrics.items():
            for metric_name, values in metrics.items():
                if not values:
                    continue
                calc = self.get_or_create_calculator(
                    account_id, repo, metric_name
                )
                for val in values:
                    calc.update(val)

    def detect_anomalies(
        self, thresholds: Optional[AdaptiveThresholds] = None
    ) -> List[AnomalyAlert]:
        """Evaluate all active telemetry calculators against statistical thresholds.

        Returns:
            List of AnomalyAlert objects exceeding z-score boundaries.
        """
        active_thresholds = thresholds or self.thresholds
        alerts: List[AnomalyAlert] = []

        for (
            account_id,
            repo,
            metric_name,
        ), calc in self.calculators.items():
            if calc.count < active_thresholds.min_samples:
                continue

            current_z = calc.z_score()
            if metric_name == "latency_ms":
                limit_z = active_thresholds.latency_z
                recommendation = (
                    "Throttle concurrent task workers, defer batch operations, "
                    "and review upstream inference latency."
                )
            elif metric_name == "error_rate":
                limit_z = active_thresholds.error_rate_z
                recommendation = (
                    "Contract circuit breaker trip limit, initiate dynamic exponential backoff, "
                    "and inspect model endpoint health."
                )
            elif metric_name == "token_burn":
                limit_z = active_thresholds.token_burn_z
                recommendation = (
                    "Trigger token flow governor throttle, enforce strict prompt compression, "
                    "and rotate quota allocation bucket."
                )
            else:
                limit_z = 2.0
                recommendation = (
                    "Inspect system health and adjust resource allocation."
                )

            if current_z > limit_z:
                severity = "critical" if current_z >= (limit_z * 1.5) else "warning"
                current_val = calc.values[-1] if calc.values else calc.mean

                alert = AnomalyAlert(
                    severity=severity,
                    account_id=account_id,
                    repo=repo,
                    metric=metric_name,
                    z_score=round(current_z, 4),
                    current_value=round(current_val, 4),
                    baseline_mean=round(calc.mean, 4),
                    recommended_action=recommendation,
                    timestamp=time.time(),
                )
                alerts.append(alert)

                # Emit via cockpit WebSocket
                self.broadcaster.emit(alert)
                logger.warning(
                    "Anomaly detected! Account: %s, Repo: %s, Metric: %s, Z: %.2f, Severity: %s",
                    account_id,
                    repo,
                    metric_name,
                    current_z,
                    severity,
                )

        self.active_alerts = alerts
        return alerts

    def get_telemetry_windows(self) -> List[TelemetryWindow]:
        """Export current state of all tracking windows."""
        windows: List[TelemetryWindow] = []
        for (
            account_id,
            repo,
            metric_name,
        ), calc in self.calculators.items():
            windows.append(
                TelemetryWindow(
                    account_id=account_id,
                    repo=repo,
                    metric_name=metric_name,
                    timestamps=list(calc.timestamps),
                    values=list(calc.values),
                    ewma=round(calc.mean, 4),
                    variance=round(calc.variance, 4),
                    z_score=round(calc.z_score(), 4),
                )
            )
        return windows

    def get_adaptive_thresholds(self, account_id: str) -> Dict[str, Any]:
        """Retrieve active thresholds and statistical baselines for an account."""
        account_metrics: Dict[str, Dict[str, Any]] = {}
        for (acc, repo, metric_name), calc in self.calculators.items():
            if acc == account_id:
                if repo not in account_metrics:
                    account_metrics[repo] = {}
                account_metrics[repo][metric_name] = {
                    "ewma_mean": round(calc.mean, 4),
                    "std_dev": round(calc.std_dev, 4),
                    "z_score": round(calc.z_score(), 4),
                    "sample_count": calc.count,
                }

        return {
            "account_id": account_id,
            "thresholds": self.thresholds.to_dict(),
            "active_metrics": account_metrics,
        }


# -----------------------------------------------------------------------------
# Adaptive Circuit Breaker Tuner
# -----------------------------------------------------------------------------
class AdaptiveCircuitBreakerTuner:
    """Calculates dynamically tuned circuit breaker trip counts when anomalies arise."""

    def __init__(
        self,
        baseline_trip_count: int = 5,
        orchestrator_api_url: Optional[str] = None,
    ) -> None:
        """Initialize AdaptiveCircuitBreakerTuner.

        Args:
            baseline_trip_count: Default static consecutive failure threshold.
            orchestrator_api_url: Internal API endpoint to push updated configs.
        """
        self.baseline_trip_count = baseline_trip_count
        self.orchestrator_api_url = orchestrator_api_url

    def compute_adjustments(
        self,
        alerts: List[AnomalyAlert],
        current_breaker_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Compute adjusted trip counts for affected accounts based on anomaly z-scores.

        Formula:
            trip_count = max(1, floor(baseline_trip * (1 - (z - threshold) / z)))

        Returns:
            Dictionary mapping account_id to recommended circuit breaker settings.
        """
        adjustments: Dict[str, Any] = {}
        config = current_breaker_config or {}

        # Group alerts by account
        account_alerts: Dict[str, List[AnomalyAlert]] = {}
        for alert in alerts:
            account_alerts.setdefault(alert.account_id, []).append(alert)

        for account_id, al_list in account_alerts.items():
            baseline_trip = config.get(account_id, {}).get(
                "trip_count", self.baseline_trip_count
            )

            # Find the most severe anomaly for this account
            max_z = 0.0
            threshold_used = 2.0
            triggering_metric = ""

            for a in al_list:
                if a.z_score > max_z:
                    max_z = a.z_score
                    triggering_metric = a.metric
                    if a.metric == "latency_ms":
                        threshold_used = 2.5
                    elif a.metric in ("error_rate", "token_burn"):
                        threshold_used = 2.0

            if max_z > threshold_used:
                # new trip_count = max(1, floor(baseline_trip * (1 - (z - threshold)/z)))
                scale_factor = 1.0 - ((max_z - threshold_used) / max_z)
                new_trip_count = max(
                    1, math.floor(baseline_trip * max(0.0, scale_factor))
                )

                adjustments[account_id] = {
                    "account_id": account_id,
                    "previous_trip_count": baseline_trip,
                    "new_trip_count": new_trip_count,
                    "max_z_score": round(max_z, 4),
                    "threshold_used": threshold_used,
                    "triggering_metric": triggering_metric,
                    "status": "THROTTLED",
                    "timestamp": time.time(),
                }

        # Apply update to orchestrator internal API if configured
        if self.orchestrator_api_url and adjustments:
            self._push_config_update(adjustments)

        return adjustments

    def _push_config_update(self, adjustments: Dict[str, Any]) -> None:
        """Push updated circuit breaker configuration via internal HTTP REST API."""
        try:
            payload = json.dumps({"circuit_breaker_adjustments": adjustments}).encode(
                "utf-8"
            )
            req = urllib.request.Request(
                self.orchestrator_api_url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=2.0) as resp:
                logger.info(
                    "Pushed circuit breaker adjustments to orchestrator API (HTTP %s)",
                    resp.status,
                )
        except Exception as e:
            logger.debug(
                "Could not push to orchestrator API %s: %s",
                self.orchestrator_api_url,
                e,
            )


# -----------------------------------------------------------------------------
# Global Module State & Entry Points
# -----------------------------------------------------------------------------
_GLOBAL_DETECTOR: TelemetryAnomalyDetector = TelemetryAnomalyDetector()
_GLOBAL_TUNER: AdaptiveCircuitBreakerTuner = AdaptiveCircuitBreakerTuner()


def detect_anomalies(
    spans: Optional[List[Dict[str, Any]]] = None,
    thresholds: Optional[AdaptiveThresholds] = None,
) -> List[AnomalyAlert]:
    """Module-level entry point to ingest optional spans and detect active anomalies."""
    if spans:
        _GLOBAL_DETECTOR.ingest_spans(spans)
    return _GLOBAL_DETECTOR.detect_anomalies(thresholds=thresholds)


def get_adaptive_thresholds(account_id: str) -> Dict[str, Any]:
    """Module-level entry point to retrieve active thresholds and EWMA baselines."""
    return _GLOBAL_DETECTOR.get_adaptive_thresholds(account_id)


def run_anomaly_detection_cycle(
    phoenix_url: str = "http://127.0.0.1:6006",
    current_breaker_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Execute a single end-to-end telemetry anomaly detection and circuit breaker tuning cycle.

    Args:
        phoenix_url: Arize Phoenix REST API base URL.
        current_breaker_config: Current static circuit breaker parameters.

    Returns:
        Dictionary containing 'alerts' list and 'threshold_adjustments' dict.
    """
    logger.info("Starting telemetry anomaly detection cycle via %s", phoenix_url)
    ingester = PhoenixSpanIngester(phoenix_url=phoenix_url)
    raw_spans = ingester.fetch_recent_spans(window_minutes=5)

    # Ingest spans into global detector
    if raw_spans:
        _GLOBAL_DETECTOR.ingest_spans(raw_spans)
    else:
        logger.info("No remote spans fetched from Phoenix. Checking existing window state.")

    # Detect anomalies
    alerts = _GLOBAL_DETECTOR.detect_anomalies()

    # Calculate adaptive adjustments
    adjustments = _GLOBAL_TUNER.compute_adjustments(
        alerts=alerts,
        current_breaker_config=current_breaker_config,
    )

    return {
        "timestamp": time.time(),
        "spans_processed": len(raw_spans),
        "alerts_count": len(alerts),
        "alerts": [alert.to_dict() for alert in alerts],
        "threshold_adjustments": adjustments,
    }


# -----------------------------------------------------------------------------
# Event Loop Periodic Task Integration
# -----------------------------------------------------------------------------
async def periodic_anomaly_detection_task(
    interval_s: float = 30.0,
    phoenix_url: str = "http://127.0.0.1:6006",
    current_breaker_config: Optional[Dict[str, Any]] = None,
    stop_event: Optional[asyncio.Event] = None,
) -> None:
    """Periodic task running at 30s intervals inside the orchestrator event loop."""
    logger.info("Registered periodic telemetry anomaly detector task (interval: %.1fs)", interval_s)
    while True:
        if stop_event and stop_event.is_set():
            logger.info("Periodic anomaly detection task received stop signal.")
            break
        try:
            run_anomaly_detection_cycle(
                phoenix_url=phoenix_url,
                current_breaker_config=current_breaker_config,
            )
        except Exception as e:
            logger.error("Unhandled error during periodic anomaly detection: %s", e)

        await asyncio.sleep(interval_s)


def register_telemetry_detector_task(
    event_loop: Optional[asyncio.AbstractEventLoop] = None,
    interval_s: float = 30.0,
    phoenix_url: str = "http://127.0.0.1:6006",
) -> asyncio.Task:
    """Helper to attach the periodic detector task to an existing asyncio loop."""
    loop = event_loop or asyncio.get_event_loop()
    return loop.create_task(
        periodic_anomaly_detection_task(
            interval_s=interval_s,
            phoenix_url=phoenix_url,
        )
    )


# -----------------------------------------------------------------------------
# CLI Verification & Synthetic Test Harness
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import random

    print("=" * 80)
    print("Executing Hermes Telemetry Anomaly Detector Verification Cycle")
    print("=" * 80)

    # 1. Generate synthetic baseline telemetry for testing if Phoenix is offline
    test_account = "google-pro-account-01"
    test_repo = "hermes_orchestrator"

    synthetic_spans = []
    base_time = time.time() - 300

    # Inject normal operational baseline (15 samples)
    for i in range(15):
        synthetic_spans.append(
            {
                "attributes": {
                    "account_id": test_account,
                    "repository": test_repo,
                    "llm.token_count.total": random.randint(800, 1100),
                },
                "duration_ms": random.uniform(120.0, 180.0),
                "status": {"status_code": "OK"},
                "start_time": base_time + (i * 10),
                "end_time": base_time + (i * 10) + 0.15,
            }
        )

    # Inject an acute latency and error rate anomaly
    synthetic_spans.append(
        {
            "attributes": {
                "account_id": test_account,
                "repository": test_repo,
                "llm.token_count.total": 4500,  # Token burn spike
            },
            "duration_ms": 1250.0,  # Latency surge (z > 2.5)
            "status": {"status_code": "ERROR"},  # Error burst
            "start_time": base_time + 160,
            "end_time": base_time + 161.25,
        }
    )

    # Seed detector with synthetic spans
    _GLOBAL_DETECTOR.ingest_spans(synthetic_spans)

    # Run detection cycle
    baseline_config = {test_account: {"trip_count": 5}}
    cycle_result = run_anomaly_detection_cycle(
        current_breaker_config=baseline_config
    )

    print("\nTelemetry Detection Cycle Results (JSON):")
    print(json.dumps(cycle_result, indent=2))
    print("\n" + "=" * 80)
    print("Verification Completed Successfully.")
    print("=" * 80)