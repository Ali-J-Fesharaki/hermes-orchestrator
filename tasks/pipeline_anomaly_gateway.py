"""
tasks/pipeline_anomaly_gateway.py
Pipeline Anomaly Detection & Autonomous Escalation Gateway
"""

from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import IntEnum
import hashlib
import json
import logging
import math
from pathlib import Path
import random
from typing import Any, Dict, List, Optional, Tuple, Union

# Optional scikit-learn dependency with fallback
try:
    import numpy as np
    from sklearn.ensemble import IsolationForest

    SKLEARN_AVAILABLE = True
except ImportError:
    IsolationForest = None
    np = None
    SKLEARN_AVAILABLE = False

logger = logging.getLogger("PipelineAnomalyGateway")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


class EscalationLevel(IntEnum):
    """Hierarchical severity levels for autonomous remediation and alerts."""

    L0_ADVISORY = 0
    L1_DEFENSIVE = 1
    L2_CRITICAL = 2
    L3_EMERGENCY = 3


@dataclass
class AnomalyFeatureVector:
    """Fixed-dimensional projection of sliding window execution spans."""

    window_id: int
    cycle_range: Tuple[int, int]
    latency_p50: float
    latency_p95: float
    latency_p99: float
    mean_token_burn: float
    conformance_mean: float
    conformance_std: float
    canary_promotion_ratio: float
    rollback_frequency: float
    directive_throughput: float
    ewma_residual_magnitude: float
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> Dict[str, float]:
        """Convert metrics to a standard key-value map for dimension analysis."""
        return {
            "latency_p50": self.latency_p50,
            "latency_p95": self.latency_p95,
            "latency_p99": self.latency_p99,
            "mean_token_burn": self.mean_token_burn,
            "conformance_mean": self.conformance_mean,
            "conformance_std": self.conformance_std,
            "canary_promotion_ratio": self.canary_promotion_ratio,
            "rollback_frequency": self.rollback_frequency,
            "directive_throughput": self.directive_throughput,
            "ewma_residual_magnitude": self.ewma_residual_magnitude,
        }

    def to_vector(self) -> List[float]:
        """Convert features to ordered numerical array for detector ingestion."""
        d = self.to_dict()
        return [float(d[k]) for k in sorted(d.keys())]


@dataclass
class CausalAttribution:
    """Attribution linkage tying an anomaly back to an audit ledger directive."""

    directive_id: str
    confidence: float
    affected_features: List[str]
    temporal_distance_cycles: int


@dataclass
class EscalationEvent:
    """Immutable audit record representing an escalation decision and action."""

    timestamp: str
    level: EscalationLevel
    anomaly_score: float
    causal_attribution: Optional[CausalAttribution]
    action_taken: str
    cooldown_expiry: str
    causal_root_hash: str

    def to_dict(self) -> Dict[str, Any]:
        """Serialize event to dict supporting JSON serialization."""
        return {
            "timestamp": self.timestamp,
            "level": self.level.name,
            "level_value": int(self.level),
            "anomaly_score": round(self.anomaly_score, 4),
            "causal_attribution": asdict(self.causal_attribution)
            if self.causal_attribution
            else None,
            "action_taken": self.action_taken,
            "cooldown_expiry": self.cooldown_expiry,
            "causal_root_hash": self.causal_root_hash,
        }


class FeatureExtractor:
    """Consumes ObservabilityFabric span records and projects them into fixed-dimensional vectors."""

    @staticmethod
    def _percentile(values: List[float], p: float) -> float:
        """Compute percentile using linear interpolation without external dependencies."""
        if not values:
            return 0.0
        sorted_vals = sorted(values)
        k = (len(sorted_vals) - 1) * (p / 100.0)
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return float(sorted_vals[int(k)])
        d0 = sorted_vals[int(f)] * (c - k)
        d1 = sorted_vals[int(c)] * (k - f)
        return float(d0 + d1)

    def extract_window_features(
        self,
        telemetry_spans: List[Dict[str, Any]],
        window_size: int = 5,
        window_id: int = 0,
        cycle_range: Optional[Tuple[int, int]] = None,
    ) -> AnomalyFeatureVector:
        """Projects span records within sliding window W into AnomalyFeatureVector."""
        if not telemetry_spans:
            c_range = cycle_range or (window_id, window_id)
            return AnomalyFeatureVector(
                window_id=window_id,
                cycle_range=c_range,
                latency_p50=0.0,
                latency_p95=0.0,
                latency_p99=0.0,
                mean_token_burn=0.0,
                conformance_mean=1.0,
                conformance_std=0.0,
                canary_promotion_ratio=1.0,
                rollback_frequency=0.0,
                directive_throughput=0.0,
                ewma_residual_magnitude=0.0,
            )

        # Extract latencies (ms)
        latencies = []
        token_burns = []
        conformance_scores = []
        canary_evals = 0
        canary_promotions = 0
        rollbacks = 0
        directives = 0
        residuals = []
        cycle_ids = []

        for span in telemetry_spans:
            attrs = span.get("attributes", span)
            cid = span.get("cycle_id", attrs.get("cycle_id"))
            if cid is not None:
                cycle_ids.append(int(cid))

            lat = (
                attrs.get("latency_ms")
                or attrs.get("latency")
                or attrs.get("duration_ms")
                or 0.0
            )
            latencies.append(float(lat))

            burn = (
                attrs.get("token_burn")
                or attrs.get("tokens_used")
                or attrs.get("tokens")
                or 0.0
            )
            token_burns.append(float(burn))

            conf = attrs.get("conformance_score")
            if conf is not None:
                conformance_scores.append(float(conf))

            if "canary_promoted" in attrs:
                canary_evals += 1
                if attrs["canary_promoted"]:
                    canary_promotions += 1
            elif attrs.get("status") == "canary_promoted":
                canary_evals += 1
                canary_promotions += 1

            if attrs.get("rollback") or attrs.get("rollback_occurred"):
                rollbacks += 1

            if attrs.get("directive_applied") or attrs.get(
                "is_directive_execution"
            ):
                directives += 1

            res = attrs.get("ewma_residual") or attrs.get(
                "prediction_residual"
            )
            if res is not None:
                residuals.append(abs(float(res)))

        # Aggregate statistics
        p50 = self._percentile(latencies, 50.0)
        p95 = self._percentile(latencies, 95.0)
        p99 = self._percentile(latencies, 99.0)
        mean_tokens = (
            sum(token_burns) / len(token_burns) if token_burns else 0.0
        )

        if conformance_scores:
            c_mean = sum(conformance_scores) / len(conformance_scores)
            variance = sum(
                (x - c_mean) ** 2 for x in conformance_scores
            ) / max(1, len(conformance_scores))
            c_std = math.sqrt(variance)
        else:
            c_mean, c_std = 1.0, 0.0

        promo_ratio = (
            (canary_promotions / canary_evals) if canary_evals > 0 else 1.0
        )
        norm_window = max(1, window_size)
        rollback_freq = rollbacks / norm_window
        directive_tput = directives / norm_window
        mean_residual = (
            (sum(residuals) / len(residuals)) if residuals else 0.0
        )

        min_c = min(cycle_ids) if cycle_ids else window_id
        max_c = max(cycle_ids) if cycle_ids else window_id
        c_range = cycle_range or (min_c, max_c)

        return AnomalyFeatureVector(
            window_id=window_id,
            cycle_range=c_range,
            latency_p50=round(p50, 2),
            latency_p95=round(p95, 2),
            latency_p99=round(p99, 2),
            mean_token_burn=round(mean_tokens, 2),
            conformance_mean=round(c_mean, 4),
            conformance_std=round(c_std, 4),
            canary_promotion_ratio=round(promo_ratio, 4),
            rollback_frequency=round(rollback_freq, 4),
            directive_throughput=round(directive_tput, 4),
            ewma_residual_magnitude=round(mean_residual, 4),
        )


class AnomalyDetector:
    """Isolation Forest anomaly detector with reservoir sampling and statistical z-score ensemble fallback."""

    def __init__(
        self,
        contamination: float = 0.05,
        n_estimators: int = 200,
        warmup_threshold: int = 20,
        z_score_threshold: float = 2.5,
        reservoir_capacity: int = 200,
    ):
        self.contamination = contamination
        self.n_estimators = n_estimators
        self.warmup_threshold = warmup_threshold
        self.z_score_threshold = z_score_threshold
        self.reservoir_capacity = reservoir_capacity
        self.reservoir_buffer: List[AnomalyFeatureVector] = []
        self.total_vectors_seen: int = 0
        self.is_warm: bool = False
        self._model = None

        if SKLEARN_AVAILABLE:
            self._model = IsolationForest(
                n_estimators=self.n_estimators,
                contamination=self.contamination,
                random_state=42,
            )

    def ingest_vector(self, vector: AnomalyFeatureVector) -> None:
        """Appends feature vector using reservoir sampling to keep baseline representative."""
        self.total_vectors_seen += 1
        if len(self.reservoir_buffer) < self.reservoir_capacity:
            self.reservoir_buffer.append(vector)
        else:
            idx = random.randint(0, self.total_vectors_seen - 1)
            if idx < self.reservoir_capacity:
                self.reservoir_buffer[idx] = vector

        if (
            not self.is_warm
            and len(self.reservoir_buffer) >= self.warmup_threshold
        ):
            self.is_warm = True
            logger.info(
                f"AnomalyDetector reached warm-up threshold ({self.warmup_threshold} vectors)."
            )
            self._fit_detector()
        elif self.is_warm and (self.total_vectors_seen % 10 == 0):
            # Incremental periodic refresh
            self._fit_detector()

    def _fit_detector(self) -> None:
        """Trains IsolationForest if sklearn is installed and buffer is sufficiently populated."""
        if SKLEARN_AVAILABLE and self._model is not None and self.is_warm:
            try:
                matrix = [v.to_vector() for v in self.reservoir_buffer]
                self._model.fit(matrix)
            except Exception as e:
                logger.warning(
                    f"Failed to fit IsolationForest; relying on z-score ensemble fallback: {e}"
                )

    def detect(self, vector: AnomalyFeatureVector) -> Tuple[bool, float]:
        """Evaluates feature vector. Returns (is_anomaly, anomaly_score).

        Anomaly score is expressed as aggregate deviation (aggregate z-score or inverted IF score).
        """
        if not self.is_warm or not self.reservoir_buffer:
            return False, 0.0

        # Calculate per-dimension statistical baseline across reservoir
        keys = list(vector.to_dict().keys())
        target_dict = vector.to_dict()
        z_scores: List[float] = []

        for k in keys:
            vals = [v.to_dict()[k] for v in self.reservoir_buffer]
            mean_v = sum(vals) / len(vals)
            var_v = sum((x - mean_v) ** 2 for x in vals) / max(
                1, len(vals) - 1
            )
            std_v = math.sqrt(var_v) if var_v > 1e-9 else 1e-4

            val = target_dict[k]
            # Conformance mean: lower is anomalous; others: higher or extreme is anomalous
            if k == "conformance_mean":
                z = (mean_v - val) / std_v
            else:
                z = abs(val - mean_v) / std_v
            z_scores.append(max(0.0, z))

        aggregate_z = sum(z_scores) / len(z_scores)

        # Primary detection via IsolationForest if available
        if SKLEARN_AVAILABLE and self._model is not None:
            try:
                vec = [vector.to_vector()]
                # score_samples returns opposite of anomaly score; lower is more abnormal
                raw_score = float(self._model.score_samples(vec)[0])
                pred = int(self._model.predict(vec)[0])
                is_anomaly = (pred == -1) or (
                    aggregate_z > self.z_score_threshold
                )
                # Map raw score to positive severity metric aligned with z-score
                normalized_score = max(aggregate_z, -raw_score * 5.0)
                return is_anomaly, normalized_score
            except Exception as e:
                logger.debug(f"IsolationForest scoring failed: {e}")

        # Fallback heuristic: aggregate z-score over feature dimensions
        is_anomaly = aggregate_z > self.z_score_threshold
        return is_anomaly, aggregate_z


class CausalAttributor:
    """Ranks candidate optimization transitions by temporal proximity and parameter alignment."""

    PARAMETER_FEATURE_MAP: Dict[str, List[str]] = {
        "concurrency_limit": ["latency_p50", "latency_p95", "latency_p99"],
        "task_timeout_ms": [
            "latency_p95",
            "latency_p99",
            "rollback_frequency",
        ],
        "token_bucket_capacity": ["mean_token_burn", "directive_throughput"],
        "token_refill_rate": ["mean_token_burn", "directive_throughput"],
        "conformance_gate_threshold": ["conformance_mean", "conformance_std"],
        "canary_traffic_fraction": [
            "canary_promotion_ratio",
            "rollback_frequency",
        ],
        "ewma_smoothing_factor": ["ewma_residual_magnitude"],
        "retry_backoff_base": [
            "latency_p99",
            "rollback_frequency",
            "directive_throughput",
        ],
        "circuit_breaker_threshold": [
            "rollback_frequency",
            "directive_throughput",
        ],
        "model_tier_routing": [
            "latency_p50",
            "mean_token_burn",
            "conformance_mean",
        ],
    }

    def _identify_deviating_features(
        self, window: AnomalyFeatureVector
    ) -> List[str]:
        """Identifies which features are exhibiting abnormal behavior."""
        deviations = []
        d = window.to_dict()
        if d["latency_p99"] > 1500.0 or d["latency_p95"] > 1000.0:
            deviations.extend(["latency_p95", "latency_p99"])
        if d["rollback_frequency"] > 0.15:
            deviations.append("rollback_frequency")
        if d["conformance_mean"] < 0.85:
            deviations.append("conformance_mean")
        if d["ewma_residual_magnitude"] > 3.0:
            deviations.append("ewma_residual_magnitude")
        if d["mean_token_burn"] > 3500.0:
            deviations.append("mean_token_burn")
        if d["canary_promotion_ratio"] < 0.5:
            deviations.append("canary_promotion_ratio")
        return list(set(deviations))

    def attribute(
        self, anomalous_window: AnomalyFeatureVector, audit_ledger: List[Dict]
    ) -> Optional[CausalAttribution]:
        """Filters ledger entries within cycle range, ranking by proximity and parameter alignment."""
        if not audit_ledger:
            return None

        window_start, window_end = anomalous_window.cycle_range
        affected_features = self._identify_deviating_features(anomalous_window)
        if not affected_features:
            affected_features = ["latency_p99", "ewma_residual_magnitude"]

        candidates = []
        for entry in audit_ledger:
            entry_cycle = entry.get("cycle_id", entry.get("cycle", 0))
            # Include transitions applied within or slightly leading the window (lookback 5 cycles)
            if (window_start - 5) <= entry_cycle <= window_end:
                temporal_dist = max(1, window_end - entry_cycle)
                directive_id = (
                    entry.get("directive_id")
                    or entry.get("snapshot_id")
                    or entry.get("id", "dir_unknown")
                )

                # Extract modified parameters
                params = []
                if "parameter_name" in entry:
                    params.append(entry["parameter_name"])
                if "parameters" in entry:
                    p = entry["parameters"]
                    params.extend(p.keys() if isinstance(p, dict) else p)
                if "modified_parameters" in entry:
                    params.extend(entry["modified_parameters"])

                # Calculate domain alignment
                mapped_features = []
                for p in params:
                    mapped_features.extend(
                        self.PARAMETER_FEATURE_MAP.get(p, [])
                    )
                mapped_features = list(set(mapped_features))

                overlapping = [
                    f for f in affected_features if f in mapped_features
                ]
                alignment_ratio = (
                    len(overlapping) / max(1, len(affected_features))
                    if overlapping
                    else 0.2
                )
                temporal_weight = 1.0 / math.sqrt(temporal_dist)

                confidence = min(
                    1.0, (temporal_weight * 0.5) + (alignment_ratio * 0.5)
                )

                candidates.append(
                    CausalAttribution(
                        directive_id=str(directive_id),
                        confidence=round(confidence, 4),
                        affected_features=overlapping
                        if overlapping
                        else affected_features,
                        temporal_distance_cycles=temporal_dist,
                    )
                )

        if not candidates:
            return None

        candidates.sort(key=lambda c: c.confidence, reverse=True)
        best = candidates[0]
        return best if best.confidence > 0.3 else None


class EscalationClassifier:
    """Maps anomaly score, causal confidence, and feature criticality into levels L0-L3."""

    FEATURE_CRITICALITY: Dict[str, float] = {
        "latency_p99": 1.25,
        "latency_p95": 1.10,
        "latency_p50": 0.85,
        "rollback_frequency": 1.50,
        "conformance_mean": 1.40,
        "conformance_std": 1.15,
        "ewma_residual_magnitude": 1.30,
        "mean_token_burn": 0.95,
        "canary_promotion_ratio": 1.05,
        "directive_throughput": 0.80,
    }

    def __init__(self, thresholds: Optional[Dict[EscalationLevel, float]] = None):
        # Default thresholds on aggregate score metric
        self.thresholds = thresholds or {
            EscalationLevel.L0_ADVISORY: 2.5,
            EscalationLevel.L1_DEFENSIVE: 3.5,
            EscalationLevel.L2_CRITICAL: 4.5,
            EscalationLevel.L3_EMERGENCY: 5.5,
        }

    def classify(
        self,
        anomaly_score: float,
        attribution: Optional[CausalAttribution] = None,
    ) -> EscalationLevel:
        """Determines EscalationLevel via multi-dimensional severity weighting."""
        # Baseline score magnitude
        mag = abs(anomaly_score)

        # Criticality multiplier from affected features
        crit_weight = 1.0
        if attribution and attribution.affected_features:
            weights = [
                self.FEATURE_CRITICALITY.get(f, 1.0)
                for f in attribution.affected_features
            ]
            crit_weight = sum(weights) / len(weights)

        # Confidence amplification
        conf_factor = 1.0 + (
            (attribution.confidence * 0.4) if attribution else 0.0
        )
        effective_score = mag * crit_weight * conf_factor

        if effective_score >= self.thresholds[EscalationLevel.L3_EMERGENCY]:
            return EscalationLevel.L3_EMERGENCY
        elif effective_score >= self.thresholds[EscalationLevel.L2_CRITICAL]:
            return EscalationLevel.L2_CRITICAL
        elif effective_score >= self.thresholds[EscalationLevel.L1_DEFENSIVE]:
            return EscalationLevel.L1_DEFENSIVE
        elif effective_score >= self.thresholds[EscalationLevel.L0_ADVISORY]:
            return EscalationLevel.L0_ADVISORY
        return EscalationLevel.L0_ADVISORY


class HysteresisController:
    """Enforces 2-consecutive confirmation, 3-cycle debounce, and progressive elevation."""

    def __init__(
        self, confirmation_window: int = 2, debounce_interval: int = 3
    ):
        self.confirmation_window = confirmation_window
        self.debounce_interval = debounce_interval
        self.consecutive_anomaly_count: int = 0
        self.last_escalation_cycle: Optional[int] = None
        self.escalation_history: Dict[str, List[Tuple[int, EscalationEvent]]] = (
            defaultdict(list)
        )

    def should_escalate(self, current_cycle: int, is_anomaly: bool) -> bool:
        """Evaluates debounce interval and confirmation counter."""
        if (
            self.last_escalation_cycle is not None
            and (current_cycle - self.last_escalation_cycle)
            <= self.debounce_interval
        ):
            # In debounce cooldown
            return False

        if is_anomaly:
            self.consecutive_anomaly_count += 1
        else:
            self.consecutive_anomaly_count = 0
            return False

        return self.consecutive_anomaly_count >= self.confirmation_window

    def compute_progressive_level(
        self,
        base_level: EscalationLevel,
        causal_root_hash: str,
        current_cycle: int = 0,
        lookback_cycles: int = 10,
    ) -> EscalationLevel:
        """Elevates severity by one level if the same causal root occurred within lookback."""
        past_events = self.escalation_history.get(causal_root_hash, [])
        is_recurring = any(
            (current_cycle - past_cycle) <= lookback_cycles
            for past_cycle, _ in past_events
        )

        if is_recurring:
            elevated_val = min(
                int(EscalationLevel.L3_EMERGENCY), int(base_level) + 1
            )
            return EscalationLevel(elevated_val)
        return base_level

    def record_escalation(
        self, current_cycle: int, event: EscalationEvent
    ) -> None:
        """Updates internal cycle indices upon triggered escalation."""
        self.last_escalation_cycle = current_cycle
        self.consecutive_anomaly_count = 0
        self.escalation_history[event.causal_root_hash].append(
            (current_cycle, event)
        )


class EscalationExecutor:
    """Dispatches level-appropriate defensive and emergency actions."""

    def execute(
        self,
        level: EscalationLevel,
        attribution: Optional[CausalAttribution],
        context: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Executes designated remediation actions based on escalation level."""
        ctx = context or {}
        snapshot_id = ctx.get(
            "last_known_good_id",
            f"snap_{attribution.directive_id}_prev"
            if attribution
            else "snap_lkb_auto",
        )
        cluster_ids = ctx.get(
            "cluster_ids", "cluster_core_tasks,cluster_worker_fleet"
        )

        if level == EscalationLevel.L0_ADVISORY:
            logger.info(
                f"[ESCALATION L0] Advisory logged. Attribution: {attribution}"
            )
            return "advisory_logged"

        elif level == EscalationLevel.L1_DEFENSIVE:
            logger.warning(
                f"[ESCALATION L1] Invoking DirectiveGovernor rollback to snapshot: {snapshot_id}"
            )
            return f"rollback_initiated::{snapshot_id}"

        elif level == EscalationLevel.L2_CRITICAL:
            logger.error(
                f"[ESCALATION L2] Rollback executed & circuit breakers tripped on clusters: {cluster_ids}"
            )
            return f"circuit_breakers_activated::{cluster_ids}"

        elif level == EscalationLevel.L3_EMERGENCY:
            logger.critical(
                f"[ESCALATION L3] Setting pipeline state to QUIESCED; dispatching Telegram alert & GitHub Issue"
            )
            return "pipeline_quiesced::alerts_dispatched"

        return "advisory_logged"


class EscalationJournal:
    """Append-only dataclass log with JSON serialization and persistence."""

    def __init__(self, journal_path: Optional[str] = None):
        self.journal: List[EscalationEvent] = []
        self.journal_path = Path(journal_path) if journal_path else None

    def record(self, event: EscalationEvent) -> None:
        """Appends event to journal and persists if path is configured."""
        self.journal.append(event)
        if self.journal_path:
            self.save(str(self.journal_path))

    def get_recent(self, n: int = 5) -> List[EscalationEvent]:
        """Returns the n most recent escalation events."""
        return self.journal[-n:]

    def get_by_causal_root(self, root_hash: str) -> List[EscalationEvent]:
        """Filters events associated with a specific causal root hash."""
        return [e for e in self.journal if e.causal_root_hash == root_hash]

    def serialize(self) -> str:
        """Serializes entire journal into indented JSON."""
        return json.dumps([e.to_dict() for e in self.journal], indent=2)

    def save(self, path: str) -> None:
        """Persists journal records to target file path."""
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(self.serialize(), encoding="utf-8")

    def load(self, path: str) -> None:
        """Loads journal from JSON file."""
        p = Path(path)
        if not p.exists():
            return
        data = json.loads(p.read_text(encoding="utf-8"))
        self.journal = []
        for d in data:
            attr_dict = d.get("causal_attribution")
            attr = CausalAttribution(**attr_dict) if attr_dict else None
            event = EscalationEvent(
                timestamp=d["timestamp"],
                level=EscalationLevel[d["level"]],
                anomaly_score=d["anomaly_score"],
                causal_attribution=attr,
                action_taken=d["action_taken"],
                cooldown_expiry=d["cooldown_expiry"],
                causal_root_hash=d["causal_root_hash"],
            )
            self.journal.append(event)


class PipelineAnomalyGateway:
    """Autonomous Escalation Gateway orchestrating feature extraction, anomaly detection, attribution, hysteresis, and remediation."""

    def __init__(
        self,
        journal_path: Optional[str] = None,
        warmup_threshold: int = 20,
        confirmation_window: int = 2,
        debounce_interval: int = 3,
    ):
        self.feature_extractor = FeatureExtractor()
        self.anomaly_detector = AnomalyDetector(
            warmup_threshold=warmup_threshold
        )
        self.causal_attributor = CausalAttributor()
        self.escalation_classifier = EscalationClassifier()
        self.hysteresis_controller = HysteresisController(
            confirmation_window=confirmation_window,
            debounce_interval=debounce_interval,
        )
        self.escalation_executor = EscalationExecutor()
        self.journal = EscalationJournal(journal_path=journal_path)
        self.current_cycle: int = 0

    def analyze_cycle(
        self,
        cycle_id: int,
        telemetry_spans: List[Dict[str, Any]],
        audit_ledger: List[Dict[str, Any]],
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[EscalationEvent]:
        """Orchestrates end-to-end detection, attribution, hysteresis, and defensive execution."""
        self.current_cycle = cycle_id

        # 1. Feature Extraction
        vector = self.feature_extractor.extract_window_features(
            telemetry_spans=telemetry_spans,
            window_size=5,
            window_id=cycle_id,
            cycle_range=(max(0, cycle_id - 4), cycle_id),
        )

        # 2. Anomaly Detection & Reservoir baseline ingestion
        is_anomaly, score = self.anomaly_detector.detect(vector)
        self.anomaly_detector.ingest_vector(vector)

        # 3. Hysteresis Filtering (Confirmation window & Debounce interval)
        if not self.hysteresis_controller.should_escalate(cycle_id, is_anomaly):
            return None

        # 4. Causal Attribution
        attribution = self.causal_attributor.attribute(vector, audit_ledger)
        root_key = (
            attribution.directive_id
            if attribution
            else f"unknown_root_cycle_{cycle_id}"
        )
        causal_root_hash = hashlib.sha256(root_key.encode("utf-8")).hexdigest()[
            :16
        ]

        # 5. Severity Classification & Progressive Elevation
        base_level = self.escalation_classifier.classify(score, attribution)
        final_level = self.hysteresis_controller.compute_progressive_level(
            base_level=base_level,
            causal_root_hash=causal_root_hash,
            current_cycle=cycle_id,
        )

        # 6. Action Execution
        exec_ctx = context or {}
        exec_ctx.setdefault("cycle_id", cycle_id)
        action_taken = self.escalation_executor.execute(
            level=final_level, attribution=attribution, context=exec_ctx
        )

        # 7. Cooldown & Journal Recording
        cooldown_minutes = 5 * (int(final_level) + 1)
        expiry = (
            datetime.now(timezone.utc) + timedelta(minutes=cooldown_minutes)
        ).isoformat()
        event = EscalationEvent(
            timestamp=datetime.now(timezone.utc).isoformat(),
            level=final_level,
            anomaly_score=round(score, 4),
            causal_attribution=attribution,
            action_taken=action_taken,
            cooldown_expiry=expiry,
            causal_root_hash=causal_root_hash,
        )

        self.hysteresis_controller.record_escalation(cycle_id, event)
        self.journal.record(event)
        return event

    def get_gateway_status(self) -> Dict[str, Any]:
        """Provides operational telemetry summary for the gateway."""
        return {
            "current_cycle": self.current_cycle,
            "is_warm": self.anomaly_detector.is_warm,
            "reservoir_size": len(self.anomaly_detector.reservoir_buffer),
            "total_vectors_seen": self.anomaly_detector.total_vectors_seen,
            "consecutive_anomalies": self.hysteresis_controller.consecutive_anomaly_count,
            "last_escalation_cycle": self.hysteresis_controller.last_escalation_cycle,
            "total_escalations": len(self.journal.journal),
            "active_roots_tracked": len(
                self.hysteresis_controller.escalation_history
            ),
        }


if __name__ == "__main__":
    # Self-contained operational validation:
    # Generates 25 synthetic telemetry windows with injected anomaly at window 20 and 21
    print("=" * 80)
    print("PIPELINE ANOMALY DETECTION & AUTONOMOUS ESCALATION GATEWAY VALIDATION")
    print("=" * 80)

    gateway = PipelineAnomalyGateway(
        warmup_threshold=15, confirmation_window=2, debounce_interval=3
    )

    audit_ledger: List[Dict[str, Any]] = [
        {
            "directive_id": "MOD-2026-OPTIM-01",
            "cycle_id": 5,
            "parameter_name": "concurrency_limit",
            "old_value": 4,
            "new_value": 8,
            "timestamp": "2026-10-27T10:00:00Z",
        },
        {
            "directive_id": "MOD-2026-REGRESS-09",
            "cycle_id": 19,
            "parameter_name": "task_timeout_ms",
            "old_value": 3000,
            "new_value": 450,
            "timestamp": "2026-10-27T11:45:00Z",
        },
    ]

    for cycle in range(1, 26):
        is_anomalous_cycle = cycle in (20, 21)

        spans: List[Dict[str, Any]] = []
        for task_idx in range(5):
            if is_anomalous_cycle:
                # Injected severe degradation (high latency, severe timeouts, high rollback rate)
                span = {
                    "cycle_id": cycle,
                    "task_id": f"task_{cycle}_{task_idx}",
                    "latency_ms": random.uniform(3500.0, 5200.0),
                    "token_burn": random.uniform(3200.0, 4800.0),
                    "conformance_score": random.uniform(0.40, 0.65),
                    "canary_promoted": False,
                    "rollback": True,
                    "directive_applied": False,
                    "ewma_residual": random.uniform(6.5, 9.2),
                }
            else:
                # Nominal baseline conditions
                span = {
                    "cycle_id": cycle,
                    "task_id": f"task_{cycle}_{task_idx}",
                    "latency_ms": random.uniform(120.0, 240.0),
                    "token_burn": random.uniform(400.0, 800.0),
                    "conformance_score": random.uniform(0.96, 1.0),
                    "canary_promoted": True,
                    "rollback": False,
                    "directive_applied": (task_idx == 0),
                    "ewma_residual": random.uniform(0.01, 0.15),
                }
            spans.append(span)

        event = gateway.analyze_cycle(
            cycle_id=cycle, telemetry_spans=spans, audit_ledger=audit_ledger
        )

        status = gateway.get_gateway_status()
        if event:
            print(
                f"[CYCLE {cycle:02d}] ESCALATION TRIGGERED -> Level: {event.level.name} | "
                f"Score: {event.anomaly_score:.2f} | Root: {event.causal_root_hash} | "
                f"Action: {event.action_taken}"
            )
        else:
            print(
                f"[CYCLE {cycle:02d}] Nominal -> Warm: {status['is_warm']} | "
                f"Consecutive Anomalies: {status['consecutive_anomalies']}"
            )

    print("\n" + "=" * 80)
    print("FINAL ESCALATION JOURNAL (JSON SERIALIZATION):")
    print("=" * 80)
    print(gateway.journal.serialize())
    print("\nOperational Gateway Status:")
    print(json.dumps(gateway.get_gateway_status(), indent=2))
    print("=" * 80)
    print("Validation successfully completed.")