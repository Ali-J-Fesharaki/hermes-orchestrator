"""
Pipeline Diagnostics & Bottleneck Analysis Engine
=================================================
File: tasks/pipeline_diagnostics.py
Milestone: End-to-End Pipeline Observability Dashboard & Bottleneck Diagnosis Engine

Provides a comprehensive real-time observability and bottleneck diagnosis layer
for the Hermes Orchestrator pipeline. Implements Kingman/VUT queueing delay
approximation, composite pipeline health scoring, decomposition lineage tracking,
and holding queue dynamics.
"""

from __future__ import annotations

import os
import sys
import time
import json
import math
import threading
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any, Tuple, Union
from pathlib import Path

import numpy as np

# Optional OpenTelemetry integration with self-contained NoOp fallback
try:
    from opentelemetry import trace
    TRACER = trace.get_tracer("hermes.diagnostics")
except ImportError:
    class _NoOpSpan:
        """No-Op fallback context manager for environments without OpenTelemetry."""
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
        """No-Op fallback tracer."""
        def start_as_current_span(self, name: str, *args: Any, **kwargs: Any) -> _NoOpSpan:
            return _NoOpSpan()

        def start_span(self, name: str, *args: Any, **kwargs: Any) -> _NoOpSpan:
            return _NoOpSpan()

    TRACER = _NoOpTracer()  # type: ignore[assignment]

# Optional Flask Blueprint support with dictionary fallback
try:
    from flask import Blueprint, jsonify
    HAS_FLASK = True
except ImportError:
    HAS_FLASK = False


# Canonical pipeline stages recognized by the diagnostics engine
RECOGNIZED_STAGES = (
    "INTAKE",
    "COST_ESTIMATION",
    "FEASIBILITY_GATE",
    "DAG_INSERT",
    "READINESS_WAIT",
    "DISPATCH",
    "EXECUTION_MONITOR",
    "OUTCOME_CAPTURE",
    "FAILURE_RECOVERY",
)


# ============================================================================
# Dataclasses
# ============================================================================

@dataclass
class StageMetrics:
    """Aggregated latency and throughput metrics for a single pipeline stage."""
    stage_name: str
    count: int = 0
    total_latency_ms: float = 0.0
    failures: int = 0
    max_latency_ms: float = 0.0
    mean_latency_ms: float = 0.0
    std_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    latencies: List[float] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize stage metrics to a JSON-compatible dictionary."""
        return {
            "stage_name": self.stage_name,
            "count": self.count,
            "total_latency_ms": round(self.total_latency_ms, 2),
            "failures": self.failures,
            "max_latency_ms": round(self.max_latency_ms, 2),
            "mean_latency_ms": round(self.mean_latency_ms, 2),
            "std_latency_ms": round(self.std_latency_ms, 2),
            "p95_latency_ms": round(self.p95_latency_ms, 2),
            "sample_size": len(self.latencies),
        }


@dataclass
class BottleneckReport:
    """VUT Kingman approximation analysis identifying pipeline bottlenecks."""
    stage_name: str
    utilization: float
    cv: float
    vut_score: float
    recommendation: str

    def to_dict(self) -> Dict[str, Any]:
        """Serialize bottleneck report to a JSON-compatible dictionary."""
        return {
            "stage_name": self.stage_name,
            "utilization": round(self.utilization, 4),
            "cv": round(self.cv, 4),
            "vut_score": round(self.vut_score, 4),
            "recommendation": self.recommendation,
        }


@dataclass
class HealthScore:
    """Composite pipeline health metrics and SLA compliance status."""
    total_score: int
    throughput_score: float
    efficiency_score: float
    latency_score: float
    stability_score: float
    status: str

    def to_dict(self) -> Dict[str, Any]:
        """Serialize health score report to a JSON-compatible dictionary."""
        return {
            "total_score": self.total_score,
            "throughput_score": round(self.throughput_score, 4),
            "efficiency_score": round(self.efficiency_score, 4),
            "latency_score": round(self.latency_score, 4),
            "stability_score": round(self.stability_score, 4),
            "status": self.status,
        }


@dataclass
class DecompositionReport:
    """Lineage, fan-out, and recursive depth analytics for task decomposition."""
    avg_fan_out: float
    max_recursive_depth: int
    success_rate_lift: float
    overhead_ratio: float
    total_decompositions: int

    def to_dict(self) -> Dict[str, Any]:
        """Serialize decomposition analytics to a JSON-compatible dictionary."""
        return {
            "avg_fan_out": round(self.avg_fan_out, 2),
            "max_recursive_depth": self.max_recursive_depth,
            "success_rate_lift": round(self.success_rate_lift, 4),
            "overhead_ratio": round(self.overhead_ratio, 4),
            "total_decompositions": self.total_decompositions,
        }


@dataclass
class HoldingQueueReport:
    """Analytics on deferred milestones, queue depth trends, and dwell latency."""
    avg_dwell_time_ms: float
    retry_success_rate: float
    queue_depth: int
    depth_trend_slope: float
    priority_quartiles: List[float]

    def to_dict(self) -> Dict[str, Any]:
        """Serialize holding queue report to a JSON-compatible dictionary."""
        return {
            "avg_dwell_time_ms": round(self.avg_dwell_time_ms, 2),
            "retry_success_rate": round(self.retry_success_rate, 4),
            "queue_depth": self.queue_depth,
            "depth_trend_slope": round(self.depth_trend_slope, 4),
            "priority_quartiles": [round(float(q), 4) for q in self.priority_quartiles],
        }


# ============================================================================
# 1. TimeSeriesStore
# ============================================================================

class TimeSeriesStore:
    """
    Sliding-window circular buffer storage with 1-second granularity buckets.
    Maintains active stage execution metrics, decomposition events, and holding queue
    snapshots with thread-safe atomic access and background checkpointing.
    """

    def __init__(
        self,
        window_minutes: int = 60,
        checkpoint_path: str = "dashboards/pipeline_diagnostics.json",
    ) -> None:
        self.window_minutes = max(1, window_minutes)
        self.window_seconds = self.window_minutes * 60
        self.checkpoint_path = checkpoint_path
        self._lock = threading.Lock()

        # Circular buffer: each entry holds {"timestamp": int, "stages": {stage_name: {...}}}
        self._num_buckets = self.window_seconds
        self._buckets: List[Dict[str, Any]] = [
            {"timestamp": 0, "stages": {}} for _ in range(self._num_buckets)
        ]

        # Additional analytical event logs (bounded by sliding window)
        self._decomposition_events: List[Dict[str, Any]] = []
        self._parent_child_map: Dict[str, List[str]] = {}
        self._holding_queue_snapshots: List[Dict[str, Any]] = []
        self._dwell_times_ms: List[float] = []
        self._retry_results: List[Dict[str, Any]] = []
        self._priority_scores: List[float] = []
        self._governor_interventions: int = 0
        self._completions_count: int = 0

        self._last_checkpoint_time = time.time()

    def ingest_span(
        self,
        span_name: str,
        duration_ms: float,
        success: bool = True,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Thread-safely ingests OpenTelemetry span data and associates metrics
        with circular buffer buckets and analytical sub-stores.
        """
        attrs = attributes or {}
        now = time.time()
        now_sec = int(now)

        stage_identity = self._extract_stage_identity(span_name)

        with self._lock:
            # 1. Update circular buffer if recognized or general pipeline stage
            if stage_identity:
                bucket_idx = now_sec % self._num_buckets
                bucket = self._buckets[bucket_idx]

                if bucket["timestamp"] != now_sec:
                    bucket["timestamp"] = now_sec
                    bucket["stages"] = {}

                stages = bucket["stages"]
                if stage_identity not in stages:
                    stages[stage_identity] = {
                        "count": 0,
                        "total_latency_ms": 0.0,
                        "failures": 0,
                        "max_latency_ms": 0.0,
                        "latencies": [],
                    }

                stage_data = stages[stage_identity]
                stage_data["count"] += 1
                stage_data["total_latency_ms"] += duration_ms
                if not success:
                    stage_data["failures"] += 1
                if duration_ms > stage_data["max_latency_ms"]:
                    stage_data["max_latency_ms"] = duration_ms
                stage_data["latencies"].append(duration_ms)

                if stage_identity == "OUTCOME_CAPTURE" and success:
                    self._completions_count += 1

            # 2. Decomposition lineage span parsing
            if span_name.startswith("hermes.decomposer") or "parent_id" in attrs:
                parent_id = attrs.get("parent_id") or attrs.get("milestone_id")
                child_ids = attrs.get("child_ids") or attrs.get("sub_milestone_ids", [])
                if isinstance(child_ids, str):
                    child_ids = [c.strip() for c in child_ids.split(",") if c.strip()]

                if parent_id and child_ids:
                    self._parent_child_map[parent_id] = list(child_ids)

                decomp_event = {
                    "timestamp": now,
                    "parent_id": parent_id,
                    "child_ids": child_ids,
                    "parent_predicted_success_rate": float(attrs.get("parent_predicted_success_rate", 0.5)),
                    "child_success_rate": float(attrs.get("child_success_rate", 1.0 if success else 0.0)),
                    "parent_estimated_tokens": float(attrs.get("parent_estimated_tokens", 0.0)),
                    "sub_milestone_tokens": float(attrs.get("sub_milestone_tokens", attrs.get("tokens", 0.0))),
                }
                self._decomposition_events.append(decomp_event)

            # 3. Holding queue metrics ingestion
            if span_name.startswith("hermes.holding_queue") or "queue_depth" in attrs or "dwell_time_ms" in attrs:
                if "queue_depth" in attrs:
                    self._holding_queue_snapshots.append({
                        "timestamp": now,
                        "queue_depth": int(attrs["queue_depth"]),
                    })

                if "dwell_time_ms" in attrs:
                    self._dwell_times_ms.append(float(attrs["dwell_time_ms"]))

                if "retry_success" in attrs or "retry_verdict" in attrs:
                    self._retry_results.append({
                        "timestamp": now,
                        "success": bool(attrs.get("retry_success", success)),
                        "reason": str(attrs.get("reason", attrs.get("gate_verdict_reason", "UNKNOWN"))),
                    })

                if "quota_headroom" in attrs and "success_probability" in attrs:
                    priority = float(attrs["quota_headroom"]) * float(attrs["success_probability"])
                    self._priority_scores.append(priority)
                elif "priority_score" in attrs:
                    self._priority_scores.append(float(attrs["priority_score"]))

            # 4. Runtime governor intervention tracking
            if attrs.get("governor_intervention") or span_name.startswith("hermes.governor"):
                self._governor_interventions += 1

            # Auto-checkpoint every 5 minutes (300 seconds)
            if now - self._last_checkpoint_time >= 300.0:
                self._checkpoint_locked()

    def get_window_snapshot(self) -> Dict[str, StageMetrics]:
        """
        Extracts a thread-safe, frozen snapshot of all stage metrics aggregated
        over the sliding observation window.
        """
        now = time.time()
        now_sec = int(now)
        cutoff_sec = now_sec - self.window_seconds

        with self._lock:
            # Aggregator buckets across stages
            aggregated: Dict[str, Dict[str, Any]] = {
                stage: {
                    "count": 0,
                    "total_latency_ms": 0.0,
                    "failures": 0,
                    "max_latency_ms": 0.0,
                    "latencies": [],
                }
                for stage in RECOGNIZED_STAGES
            }

            for bucket in self._buckets:
                ts = bucket["timestamp"]
                if cutoff_sec <= ts <= now_sec:
                    for stage, data in bucket["stages"].items():
                        if stage not in aggregated:
                            aggregated[stage] = {
                                "count": 0,
                                "total_latency_ms": 0.0,
                                "failures": 0,
                                "max_latency_ms": 0.0,
                                "latencies": [],
                            }
                        agg = aggregated[stage]
                        agg["count"] += data["count"]
                        agg["total_latency_ms"] += data["total_latency_ms"]
                        agg["failures"] += data["failures"]
                        agg["max_latency_ms"] = max(agg["max_latency_ms"], data["max_latency_ms"])
                        agg["latencies"].extend(data["latencies"])

            snapshot: Dict[str, StageMetrics] = {}
            for stage, agg in aggregated.items():
                cnt = agg["count"]
                lats = agg["latencies"]
                mean_lat = float(np.mean(lats)) if lats else 0.0
                std_lat = float(np.std(lats)) if len(lats) > 1 else 0.0
                p95_lat = float(np.percentile(lats, 95)) if lats else 0.0

                snapshot[stage] = StageMetrics(
                    stage_name=stage,
                    count=cnt,
                    total_latency_ms=agg["total_latency_ms"],
                    failures=agg["failures"],
                    max_latency_ms=agg["max_latency_ms"],
                    mean_latency_ms=mean_lat,
                    std_latency_ms=std_lat,
                    p95_latency_ms=p95_lat,
                    latencies=list(lats),
                )

            return snapshot

    def checkpoint(self) -> None:
        """Explicitly checkpoints time-series metrics to disk via an atomic write."""
        with self._lock:
            self._checkpoint_locked()

    def _checkpoint_locked(self) -> None:
        """Internal atomic checkpoint write executed under lock."""
        now = time.time()
        self._last_checkpoint_time = now

        # Build export dictionary
        snapshot = self._export_summary_dict(now)

        try:
            target_path = Path(self.checkpoint_path).resolve()
            target_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = target_path.with_suffix(f".tmp.{os.getpid()}")

            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(snapshot, f, indent=2)
                f.flush()
                os.fsync(f.fileno())

            os.replace(tmp_path, target_path)
        except Exception as e:
            sys.stderr.write(f"[TimeSeriesStore] Checkpoint failed: {e}\n")

    def _export_summary_dict(self, now: float) -> Dict[str, Any]:
        """Constructs serializable dictionary snapshot for disk checkpoint."""
        now_sec = int(now)
        cutoff_sec = now_sec - self.window_seconds

        stage_summaries = {}
        for bucket in self._buckets:
            if cutoff_sec <= bucket["timestamp"] <= now_sec:
                for stage, data in bucket["stages"].items():
                    if stage not in stage_summaries:
                        stage_summaries[stage] = {
                            "count": 0,
                            "total_latency_ms": 0.0,
                            "failures": 0,
                            "max_latency_ms": 0.0,
                        }
                    summary = stage_summaries[stage]
                    summary["count"] += data["count"]
                    summary["total_latency_ms"] += data["total_latency_ms"]
                    summary["failures"] += data["failures"]
                    summary["max_latency_ms"] = max(summary["max_latency_ms"], data["max_latency_ms"])

        return {
            "checkpoint_timestamp": now,
            "window_minutes": self.window_minutes,
            "stage_metrics": stage_summaries,
            "holding_queue_depth": (
                self._holding_queue_snapshots[-1]["queue_depth"]
                if self._holding_queue_snapshots
                else 0
            ),
            "governor_interventions": self._governor_interventions,
            "total_completions": self._completions_count,
        }

    def _extract_stage_identity(self, span_name: str) -> Optional[str]:
        """Maps incoming span names into recognized pipeline stages."""
        upper_name = span_name.upper()

        if span_name.startswith("hermes.pipeline."):
            suffix = span_name[len("hermes.pipeline."):].upper()
            if suffix in RECOGNIZED_STAGES:
                return suffix

        for stage in RECOGNIZED_STAGES:
            if stage in upper_name:
                return stage

        return None

    def get_decomposition_records(self) -> Tuple[List[Dict[str, Any]], Dict[str, List[str]]]:
        """Thread-safe retrieval of decomposition events and DAG parent-child relationships."""
        now = time.time()
        cutoff = now - self.window_seconds
        with self._lock:
            active_events = [e for e in self._decomposition_events if e["timestamp"] >= cutoff]
            return list(active_events), dict(self._parent_child_map)

    def get_holding_queue_data(self) -> Tuple[List[Dict[str, Any]], List[float], List[Dict[str, Any]], List[float]]:
        """Thread-safe retrieval of holding queue snapshots and dwell time records."""
        now = time.time()
        cutoff = now - self.window_seconds
        with self._lock:
            active_snapshots = [s for s in self._holding_queue_snapshots if s["timestamp"] >= cutoff]
            return (
                active_snapshots,
                list(self._dwell_times_ms),
                list(self._retry_results),
                list(self._priority_scores),
            )

    def get_stability_counters(self) -> Tuple[int, int]:
        """Returns (governor_interventions, completions_count)."""
        with self._lock:
            return self._governor_interventions, self._completions_count


# ============================================================================
# 2. BottleneckAnalyzer
# ============================================================================

class BottleneckAnalyzer:
    """
    Computes Kingman/VUT queueing delay approximations across all pipeline stages.
    Identifies dominant bottleneck stages and generates operational remediation
    recommendations based on stage utilization and variance patterns.
    """

    def __init__(self, store: TimeSeriesStore) -> None:
        self.store = store

    def identify(self, concurrency_slots: int = 3) -> BottleneckReport:
        """
        Executes Kingman/VUT bottleneck identification over active sliding window metrics.
        Kingman wait formula proxy: Bottleneck = argmax_S ( U(S) * (1 + CV(S)^2) )
        """
        snapshot = self.store.get_window_snapshot()
        window_duration_ms = float(self.store.window_minutes * 60 * 1000)
        slots = max(1, concurrency_slots)

        candidates: List[Tuple[str, float, float, float]] = []

        for stage_name, metrics in snapshot.items():
            if metrics.count == 0:
                continue

            # U(S) = active processing time / (wall-clock duration * concurrency_slots)
            utilization = metrics.total_latency_ms / (window_duration_ms * slots)
            utilization = min(1.0, max(0.0, utilization))

            # CV(S) = std(latency) / mean(latency)
            if metrics.mean_latency_ms > 0:
                cv = metrics.std_latency_ms / metrics.mean_latency_ms
            else:
                cv = 0.0

            # VUT queue delay score
            vut_score = utilization * (1.0 + (cv ** 2))
            candidates.append((stage_name, utilization, cv, vut_score))

        if not candidates:
            return BottleneckReport(
                stage_name="NONE",
                utilization=0.0,
                cv=0.0,
                vut_score=0.0,
                recommendation="Nominal execution: no active pipeline processing detected in window.",
            )

        # argmax_S ( VUT_score )
        candidates.sort(key=lambda x: x[3], reverse=True)
        bottleneck_stage, u, cv, vut = candidates[0]

        recommendation = self._generate_recommendation(bottleneck_stage, u, cv)

        return BottleneckReport(
            stage_name=bottleneck_stage,
            utilization=u,
            cv=cv,
            vut_score=vut,
            recommendation=recommendation,
        )

    def _generate_recommendation(self, stage: str, utilization: float, cv: float) -> str:
        """Maps utilization level and coefficient of variation to actionable remediations."""
        is_high_cv = cv > 0.75

        if utilization > 0.90:
            return (
                f"Stage '{stage}' exhibits extreme utilization ({utilization:.1%}). "
                "Immediate remediation: scale concurrency slots or horizontally partition workers."
            )
        elif utilization >= 0.60:
            if is_high_cv:
                return (
                    f"Stage '{stage}' exhibits moderate-high utilization ({utilization:.1%}) with high variance (CV={cv:.2f}). "
                    "Remediation: reduce variance via caching, request batching, or input normalization."
                )
            else:
                return (
                    f"Stage '{stage}' exhibits deterministic high utilization ({utilization:.1%}). "
                    "Remediation: optimize stage execution critical path or increase concurrency ceiling."
                )
        else:
            if is_high_cv:
                return (
                    f"Stage '{stage}' exhibits low utilization ({utilization:.1%}) with elevated variance (CV={cv:.2f}). "
                    "Remediation: investigate bimodal workload distribution, intermittent network I/O stalls, or retry cascades."
                )
            else:
                return (
                    f"Stage '{stage}' operates within nominal margins ({utilization:.1%} util, CV={cv:.2f}). "
                    "No immediate capacity intervention required."
                )


# ============================================================================
# 3. PipelineHealthScorer
# ============================================================================

class PipelineHealthScorer:
    """
    Synthesizes multi-dimensional pipeline performance into a unified 0-100 composite health score:
    H = 0.30*T (Throughput) + 0.25*E (Efficiency) + 0.25*L (Latency) + 0.20*S (Stability)
    """

    def __init__(
        self,
        store: TimeSeriesStore,
        baseline_throughput: float = 2.0,
        slo_target_ms: float = 300000.0,
    ) -> None:
        self.store = store
        self.baseline_throughput = max(0.1, baseline_throughput)  # completions per hour
        self.slo_target_ms = max(1.0, slo_target_ms)

    def compute(self) -> HealthScore:
        """Computes current composite pipeline health score."""
        snapshot = self.store.get_window_snapshot()
        interventions, completions = self.store.get_stability_counters()
        decomp_records, _ = self.store.get_decomposition_records()

        # 1. Throughput (T): min(1.0, actual_completions_per_hour / baseline_target)
        window_hours = max(0.0167, self.store.window_minutes / 60.0)
        actual_completions_per_hour = completions / window_hours
        t_score = min(1.0, max(0.0, actual_completions_per_hour / self.baseline_throughput))

        # 2. Efficiency (E): 1.0 - rollback_rate - (decomposition_overhead_tokens / total_tokens)
        total_spans = sum(m.count for m in snapshot.values())
        total_failures = sum(m.failures for m in snapshot.values())
        rollback_rate = (total_failures / total_spans) if total_spans > 0 else 0.0

        total_tokens = sum(r.get("parent_estimated_tokens", 0.0) for r in decomp_records)
        sub_tokens = sum(r.get("sub_milestone_tokens", 0.0) for r in decomp_records)
        token_overhead_ratio = (
            max(0.0, sub_tokens - total_tokens) / total_tokens if total_tokens > 0 else 0.0
        )

        e_score = max(0.0, min(1.0, 1.0 - rollback_rate - token_overhead_ratio))

        # 3. Latency (L): max(0, 1.0 - (p95_pipeline_latency - slo_target) / slo_target)
        all_latencies = []
        for m in snapshot.values():
            all_latencies.extend(m.latencies)

        if all_latencies:
            p95_latency = float(np.percentile(all_latencies, 95))
        else:
            p95_latency = 0.0

        if p95_latency <= self.slo_target_ms:
            l_score = 1.0
        else:
            l_score = max(0.0, 1.0 - ((p95_latency - self.slo_target_ms) / self.slo_target_ms))

        # 4. Stability (S): 1.0 / (1.0 + governor_interventions_per_milestone)
        effective_milestones = max(1, completions)
        interventions_per_milestone = interventions / effective_milestones
        s_score = 1.0 / (1.0 + interventions_per_milestone)

        # Composite score calculation (0.30, 0.25, 0.25, 0.20)
        composite = (0.30 * t_score) + (0.25 * e_score) + (0.25 * l_score) + (0.20 * s_score)
        total_score = int(round(composite * 100))
        total_score = max(0, min(100, total_score))

        # Traffic light assignment
        if total_score >= 70:
            status = "green"
        elif total_score >= 40:
            status = "yellow"
        else:
            status = "red"

        return HealthScore(
            total_score=total_score,
            throughput_score=t_score,
            efficiency_score=e_score,
            latency_score=l_score,
            stability_score=s_score,
            status=status,
        )


# ============================================================================
# 4. DecompositionAnalytics
# ============================================================================

class DecompositionAnalytics:
    """
    Computes hierarchical decomposition performance, fan-out distributions,
    DAG recursion depths, and token overhead inflation.
    """

    def __init__(self, store: TimeSeriesStore) -> None:
        self.store = store

    def analyze(self) -> DecompositionReport:
        """Evaluates decomposition tree metrics over the active sliding window."""
        records, parent_child_map = self.store.get_decomposition_records()

        if not records:
            return DecompositionReport(
                avg_fan_out=0.0,
                max_recursive_depth=0,
                success_rate_lift=0.0,
                overhead_ratio=0.0,
                total_decompositions=0,
            )

        # 1. Average fan-out distribution
        fan_outs = [len(r.get("child_ids", [])) or 1 for r in records]
        avg_fan_out = float(np.mean(fan_outs)) if fan_outs else 0.0

        # 2. Maximum recursive DAG depth
        max_depth = self._calculate_max_dag_depth(parent_child_map)

        # 3. Success rate lift: mean(child_success_rate - parent_predicted_success_rate)
        lifts = [
            r["child_success_rate"] - r["parent_predicted_success_rate"]
            for r in records
        ]
        avg_lift = float(np.mean(lifts)) if lifts else 0.0

        # 4. Decomposition token overhead ratio: total sub-milestone tokens / parent estimated tokens
        parent_tokens = sum(r.get("parent_estimated_tokens", 0.0) for r in records)
        sub_tokens = sum(r.get("sub_milestone_tokens", 0.0) for r in records)
        if parent_tokens > 0:
            overhead_ratio = sub_tokens / parent_tokens
        else:
            overhead_ratio = 1.0 if sub_tokens > 0 else 0.0

        return DecompositionReport(
            avg_fan_out=avg_fan_out,
            max_recursive_depth=max_depth,
            success_rate_lift=avg_lift,
            overhead_ratio=overhead_ratio,
            total_decompositions=len(records),
        )

    def _calculate_max_dag_depth(self, parent_child_map: Dict[str, List[str]]) -> int:
        """Traverses the parent-to-children adjacency map to find maximum tree depth."""
        if not parent_child_map:
            return 0

        # Identify root nodes (nodes that are never children)
        all_children = set()
        for children in parent_child_map.values():
            all_children.update(children)

        roots = [p for p in parent_child_map.keys() if p not in all_children]
        if not roots:
            roots = list(parent_child_map.keys())

        def get_depth(node: str, visited: set) -> int:
            if node in visited:
                return 0
            visited.add(node)
            children = parent_child_map.get(node, [])
            if not children:
                return 1
            return 1 + max((get_depth(c, visited.copy()) for c in children), default=0)

        max_depth = 0
        for root in roots:
            max_depth = max(max_depth, get_depth(root, set()))

        return max(0, max_depth - 1) if max_depth > 0 else 0


# ============================================================================
# 5. HoldingQueueAnalytics
# ============================================================================

class HoldingQueueAnalytics:
    """
    Computes holding queue operational dynamics including dwell times,
    retry success rates, priority distributions, and queue growth linear regression slopes.
    """

    def __init__(self, store: TimeSeriesStore) -> None:
        self.store = store

    def analyze(self) -> HoldingQueueReport:
        """Analyzes holding queue state snapshots and queue trend dynamics."""
        snapshots, dwell_times, retries, priority_scores = self.store.get_holding_queue_data()

        # 1. Average dwell latency in milliseconds
        avg_dwell_time = float(np.mean(dwell_times)) if dwell_times else 0.0

        # 2. Retry success rate
        if retries:
            successes = sum(1 for r in retries if r.get("success"))
            retry_success_rate = successes / len(retries)
        else:
            retry_success_rate = 1.0

        # 3. Current queue depth
        current_depth = snapshots[-1]["queue_depth"] if snapshots else 0

        # 4. Linear regression slope of queue depth over time (growth trend)
        slope = self._compute_depth_trend_slope(snapshots)

        # 5. Priority score distribution quartiles [25%, 50%, 75%]
        if priority_scores:
            quartiles = [float(q) for q in np.percentile(priority_scores, [25, 50, 75])]
        else:
            quartiles = [0.0, 0.0, 0.0]

        return HoldingQueueReport(
            avg_dwell_time_ms=avg_dwell_time,
            retry_success_rate=retry_success_rate,
            queue_depth=current_depth,
            depth_trend_slope=slope,
            priority_quartiles=quartiles,
        )

    def _compute_depth_trend_slope(self, snapshots: List[Dict[str, Any]]) -> float:
        """Calculates queue depth trend slope via least-squares linear regression."""
        if len(snapshots) < 2:
            return 0.0

        times = np.array([s["timestamp"] for s in snapshots], dtype=np.float64)
        depths = np.array([s["queue_depth"] for s in snapshots], dtype=np.float64)

        # Normalize time origin to minutes to avoid numerical instability
        t_min = (times - times[0]) / 60.0

        if np.all(t_min == t_min[0]) or np.all(depths == depths[0]):
            return 0.0

        try:
            poly = np.polyfit(t_min, depths, deg=1)
            return float(poly[0])
        except Exception:
            return 0.0


# ============================================================================
# 6. PipelineDiagnostics (Main Facade)
# ============================================================================

class PipelineDiagnostics:
    """
    Main orchestration facade for Hermes pipeline observability, telemetry ingestion,
    VUT bottleneck analysis, and health scoring.
    """

    def __init__(
        self,
        window_minutes: int = 60,
        concurrency_slots: int = 3,
        checkpoint_path: str = "dashboards/pipeline_diagnostics.json",
    ) -> None:
        self.window_minutes = window_minutes
        self.concurrency_slots = concurrency_slots
        self.store = TimeSeriesStore(
            window_minutes=window_minutes,
            checkpoint_path=checkpoint_path,
        )
        self.bottleneck_analyzer = BottleneckAnalyzer(self.store)
        self.health_scorer = PipelineHealthScorer(self.store)
        self.decomposition_analytics = DecompositionAnalytics(self.store)
        self.holding_queue_analytics = HoldingQueueAnalytics(self.store)

    def ingest_span(
        self,
        span_name: str,
        duration_ms: float,
        success: bool = True,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Ingests pipeline span execution data into the time-series store."""
        self.store.ingest_span(
            span_name=span_name,
            duration_ms=duration_ms,
            success=success,
            attributes=attributes,
        )

    def get_stage_metrics(self, window_minutes: Optional[int] = None) -> Dict[str, Any]:
        """Returns aggregated metrics for all recognized pipeline stages."""
        with TRACER.start_as_current_span("hermes.diagnostics.stages") as span:
            span.set_attribute("diagnostics.window_minutes", window_minutes or self.window_minutes)
            snapshot = self.store.get_window_snapshot()
            return {stage: metrics.to_dict() for stage, metrics in snapshot.items()}

    def identify_bottleneck(self) -> Dict[str, Any]:
        """Identifies primary pipeline bottleneck via Kingman/VUT queueing delay analysis."""
        with TRACER.start_as_current_span("hermes.diagnostics.bottleneck") as span:
            report = self.bottleneck_analyzer.identify(self.concurrency_slots)
            span.set_attribute("diagnostics.bottleneck.stage", report.stage_name)
            span.set_attribute("diagnostics.bottleneck.utilization", report.utilization)
            span.set_attribute("diagnostics.bottleneck.vut_score", report.vut_score)
            return report.to_dict()

    def get_health_score(self) -> Dict[str, Any]:
        """Computes comprehensive composite pipeline health score."""
        with TRACER.start_as_current_span("hermes.diagnostics.health") as span:
            health = self.health_scorer.compute()
            span.set_attribute("diagnostics.health.score", health.total_score)
            span.set_attribute("diagnostics.health.status", health.status)
            return health.to_dict()

    def get_decomposition_analytics(self) -> Dict[str, Any]:
        """Computes hierarchical milestone decomposition performance and overhead."""
        with TRACER.start_as_current_span("hermes.diagnostics.decomposition") as span:
            report = self.decomposition_analytics.analyze()
            span.set_attribute("diagnostics.decomposition.count", report.total_decompositions)
            return report.to_dict()

    def get_holding_queue_analytics(self) -> Dict[str, Any]:
        """Computes holding queue depth, retry success rate, and dwell time dynamics."""
        with TRACER.start_as_current_span("hermes.diagnostics.holding_queue") as span:
            report = self.holding_queue_analytics.analyze()
            span.set_attribute("diagnostics.holding_queue.depth", report.queue_depth)
            return report.to_dict()


# ============================================================================
# 7. DiagnosticsAPIHandler
# ============================================================================

class DiagnosticsAPIHandler:
    """
    HTTP API exposure handler providing REST endpoints for pipeline diagnostics.
    Returns a Flask Blueprint when Flask is installed, or a dictionary routes map
    for standalone and testing environments.
    """

    def __init__(self, diagnostics: PipelineDiagnostics) -> None:
        self.diagnostics = diagnostics

    def create_blueprint(self) -> Any:
        """
        Creates and configures the diagnostic REST endpoints.
        Endpoints:
          - GET /api/pipeline/health
          - GET /api/pipeline/bottleneck
          - GET /api/pipeline/stages
          - GET /api/pipeline/decomposition
          - GET /api/pipeline/holding-queue
        """
        if HAS_FLASK:
            bp = Blueprint("pipeline_diagnostics", __name__)

            @bp.route("/api/pipeline/health", methods=["GET"])
            def health_endpoint() -> Any:
                return jsonify(self.diagnostics.get_health_score())

            @bp.route("/api/pipeline/bottleneck", methods=["GET"])
            def bottleneck_endpoint() -> Any:
                return jsonify(self.diagnostics.identify_bottleneck())

            @bp.route("/api/pipeline/stages", methods=["GET"])
            def stages_endpoint() -> Any:
                return jsonify(self.diagnostics.get_stage_metrics())

            @bp.route("/api/pipeline/decomposition", methods=["GET"])
            def decomposition_endpoint() -> Any:
                return jsonify(self.diagnostics.get_decomposition_analytics())

            @bp.route("/api/pipeline/holding-queue", methods=["GET"])
            def holding_queue_endpoint() -> Any:
                return jsonify(self.diagnostics.get_holding_queue_analytics())

            return bp
        else:
            class _RoutesMap(dict):
                """Fallback dictionary acting as route registry."""
                def __init__(self, routes: Dict[str, Any]) -> None:
                    super().__init__(routes)
                    self.routes = routes
                    self.name = "pipeline_diagnostics"

            return _RoutesMap({
                "/api/pipeline/health": self.diagnostics.get_health_score,
                "/api/pipeline/bottleneck": self.diagnostics.identify_bottleneck,
                "/api/pipeline/stages": self.diagnostics.get_stage_metrics,
                "/api/pipeline/decomposition": self.diagnostics.get_decomposition_analytics,
                "/api/pipeline/holding-queue": self.diagnostics.get_holding_queue_analytics,
            })


# ============================================================================
# Module Convenience Factory
# ============================================================================

def create_diagnostics(
    window_minutes: int = 60,
    concurrency_slots: int = 3,
) -> PipelineDiagnostics:
    """Convenience factory function for instantiating PipelineDiagnostics."""
    return PipelineDiagnostics(
        window_minutes=window_minutes,
        concurrency_slots=concurrency_slots,
    )


# ============================================================================
# Standalone Demonstration Execution
# ============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print(" Hermes Pipeline Diagnostics & Bottleneck Diagnosis Engine")
    print("=" * 70)

    # Instantiate diagnostics engine
    diag = create_diagnostics(window_minutes=60, concurrency_slots=3)

    # Ingest 20 synthetic spans across various pipeline stages
    synthetic_spans = [
        ("hermes.pipeline.intake", 45.0, True, {"milestone_id": "M-1"}),
        ("hermes.pipeline.cost_estimation", 120.0, True, {"parent_estimated_tokens": 12000}),
        ("hermes.pipeline.feasibility_gate", 80.0, True, {"gate_verdict": "APPROVED"}),
        ("hermes.pipeline.dag_insert", 35.0, True, {}),
        ("hermes.pipeline.readiness_wait", 210.0, True, {}),
        ("hermes.pipeline.dispatch", 150.0, True, {"slot": 1}),
        ("hermes.pipeline.execution_monitor", 8500.0, True, {"governor_intervention": False}),
        ("hermes.pipeline.outcome_capture", 95.0, True, {"completions": 1}),
        ("hermes.pipeline.intake", 50.0, True, {"milestone_id": "M-2"}),
        ("hermes.pipeline.cost_estimation", 130.0, True, {"parent_estimated_tokens": 8000}),
        ("hermes.pipeline.feasibility_gate", 90.0, True, {"gate_verdict": "DEFERRED"}),
        ("hermes.holding_queue.deferred", 15.0, True, {
            "queue_depth": 3,
            "dwell_time_ms": 1450.0,
            "quota_headroom": 0.85,
            "success_probability": 0.92,
        }),
        ("hermes.holding_queue.retry", 25.0, True, {
            "retry_success": True,
            "gate_verdict_reason": "QUOTA_EXHAUSTED",
        }),
        ("hermes.pipeline.dispatch", 140.0, True, {"slot": 2}),
        ("hermes.pipeline.execution_monitor", 9200.0, False, {"error": "SubprocessTimeout"}),
        ("hermes.pipeline.failure_recovery", 450.0, True, {"recovered": True}),
        ("hermes.decomposer.evaluate", 310.0, True, {
            "parent_id": "M-2",
            "child_ids": ["M-2.1", "M-2.2"],
            "parent_predicted_success_rate": 0.55,
            "child_success_rate": 0.95,
            "parent_estimated_tokens": 8000,
            "sub_milestone_tokens": 8600,
        }),
        ("hermes.pipeline.dispatch", 160.0, True, {"slot": 3}),
        ("hermes.pipeline.execution_monitor", 12500.0, True, {}),
        ("hermes.pipeline.outcome_capture", 110.0, True, {"completions": 1}),
    ]

    print(f"\nIngesting {len(synthetic_spans)} synthetic telemetry spans...")
    for span_name, duration, success, attrs in synthetic_spans:
        diag.ingest_span(span_name, duration, success, attrs)

    # Compute and display analytical reports
    health = diag.get_health_score()
    bottleneck = diag.identify_bottleneck()
    stages = diag.get_stage_metrics()
    decomp = diag.get_decomposition_analytics()
    queue = diag.get_holding_queue_analytics()

    print("\n--- Pipeline Health Score ---")
    print(json.dumps(health, indent=2))

    print("\n--- Bottleneck Identification (Kingman VUT) ---")
    print(json.dumps(bottleneck, indent=2))

    print("\n--- Decomposition Lineage Analytics ---")
    print(json.dumps(decomp, indent=2))

    print("\n--- Holding Queue Analytics ---")
    print(json.dumps(queue, indent=2))

    print("\n--- Stage Latencies & Counts (Sample) ---")
    sample_stages = {k: stages[k] for k in ["EXECUTION_MONITOR", "DISPATCH", "FEASIBILITY_GATE"]}
    print(json.dumps(sample_stages, indent=2))

    # Demonstrate API Blueprint generation
    api_handler = DiagnosticsAPIHandler(diag)
    blueprint = api_handler.create_blueprint()
    print(f"\nAPI Blueprint Handler Initialized: {type(blueprint).__name__}")
    print("=" * 70)
    print(" Pipeline Diagnostics Demonstration Completed Successfully.")
    print("=" * 70)