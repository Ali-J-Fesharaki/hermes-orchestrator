"""Execution Telemetry Correlation & Resource Anomaly Detection Engine.

Tasks / Telemetry Anomaly Engine for the Hermes Orchestrator.
Correlates execution telemetry with provenance ledgers to detect resource
anomalies, track moving baseline distributions, calculate adaptive risk
modifiers, and publish system telemetry health reports.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import json
import math
import os
from pathlib import Path
import sqlite3
import tempfile
from typing import Any, Dict, List, Optional, Tuple, Union

try:
    from flask import Blueprint, jsonify, request
    HAS_FLASK = True
except ImportError:
    HAS_FLASK = False
    Blueprint = None
    jsonify = None
    request = None


# =====================================================================
# 1. Metric Vector Dataclass
# =====================================================================

@dataclass
class MetricVector:
    """Represents a single execution's resource profile across core dimensions."""
    total_tokens: int = 0
    wall_time_s: float = 0.0
    error_count: int = 0
    retry_count: int = 0
    span_depth: int = 0
    llm_calls: int = 0

    def as_dict(self) -> Dict[str, Any]:
        """Convert metric vector to a dictionary."""
        return {
            "total_tokens": int(self.total_tokens),
            "wall_time_s": float(self.wall_time_s),
            "error_count": int(self.error_count),
            "retry_count": int(self.retry_count),
            "span_depth": int(self.span_depth),
            "llm_calls": int(self.llm_calls),
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "MetricVector":
        """Instantiate MetricVector from a dictionary with type safety."""
        return cls(
            total_tokens=int(d.get("total_tokens", 0)),
            wall_time_s=float(d.get("wall_time_s", 0.0)),
            error_count=int(d.get("error_count", 0)),
            retry_count=int(d.get("retry_count", 0)),
            span_depth=int(d.get("span_depth", 0)),
            llm_calls=int(d.get("llm_calls", 0)),
        )


# =====================================================================
# 2. Baseline State Dataclass
# =====================================================================

@dataclass
class BaselineState:
    """Tracks exponential moving average and online variance for a metric dimension."""
    ema: float = 0.0
    ema_variance: float = 0.0
    sample_count: int = 0

    def update(self, value: float, alpha: float = 0.2) -> None:
        """Update EMA and EMA-adapted Welford variance with a new observation.

        ema = alpha * value + (1 - alpha) * ema_old
        diff = value - ema_old
        ema_variance = (1 - alpha) * (ema_variance + alpha * diff * diff)
        """
        val = float(value)
        if self.sample_count == 0:
            self.ema = val
            self.ema_variance = 0.0
            self.sample_count = 1
        else:
            ema_old = self.ema
            self.ema = alpha * val + (1.0 - alpha) * ema_old
            diff = val - ema_old
            self.ema_variance = (1.0 - alpha) * (self.ema_variance + alpha * diff * diff)
            self.sample_count += 1

    def as_dict(self) -> Dict[str, Any]:
        """Convert baseline state to a dictionary."""
        return {
            "ema": self.ema,
            "ema_variance": self.ema_variance,
            "sample_count": self.sample_count,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "BaselineState":
        """Instantiate BaselineState from a dictionary."""
        return cls(
            ema=float(d.get("ema", 0.0)),
            ema_variance=float(d.get("ema_variance", 0.0)),
            sample_count=int(d.get("sample_count", 0)),
        )


# =====================================================================
# 3. Anomaly Classification Enum
# =====================================================================

class AnomalyClassification(str, Enum):
    """Categorization of metric divergence relative to empirical baseline."""
    NOMINAL = "NOMINAL"
    ELEVATED = "ELEVATED"
    ANOMALOUS = "ANOMALOUS"


# =====================================================================
# 4. Anomaly Record Dataclass
# =====================================================================

@dataclass
class AnomalyRecord:
    """Individual anomaly observation captured during execution telemetry ingestion."""
    milestone_id: str
    repo: str
    timestamp: str
    metric_name: str
    value: float
    z_score: float
    classification: AnomalyClassification
    provenance_hash: Optional[str] = None
    correlated_outcome: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        """Convert anomaly record to a JSON-serializable dictionary."""
        return {
            "milestone_id": self.milestone_id,
            "repo": self.repo,
            "timestamp": self.timestamp,
            "metric_name": self.metric_name,
            "value": float(self.value),
            "z_score": float(self.z_score),
            "classification": self.classification.value if isinstance(self.classification, AnomalyClassification) else str(self.classification),
            "provenance_hash": self.provenance_hash,
            "correlated_outcome": self.correlated_outcome,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "AnomalyRecord":
        """Reconstruct AnomalyRecord from a dictionary."""
        raw_class = str(d.get("classification", "NOMINAL"))
        try:
            classification = AnomalyClassification(raw_class)
        except ValueError:
            classification = AnomalyClassification.NOMINAL

        return cls(
            milestone_id=str(d.get("milestone_id", "")),
            repo=str(d.get("repo", "")),
            timestamp=str(d.get("timestamp", "")),
            metric_name=str(d.get("metric_name", "")),
            value=float(d.get("value", 0.0)),
            z_score=float(d.get("z_score", 0.0)),
            classification=classification,
            provenance_hash=d.get("provenance_hash"),
            correlated_outcome=d.get("correlated_outcome"),
        )


# =====================================================================
# File System & Atomic Write Helpers
# =====================================================================

def _atomic_write_text(filepath: Path, content: str) -> None:
    """Atomically write text content to filepath using NamedTemporaryFile and os.replace."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    temp_dir = filepath.parent
    with tempfile.NamedTemporaryFile("w", dir=temp_dir, delete=False, encoding="utf-8") as tf:
        tf.write(content)
        tf.flush()
        os.fsync(tf.fileno())
        temp_name = tf.name
    os.replace(temp_name, filepath)


def _atomic_write_json(filepath: Path, data: Any, indent: int = 2) -> None:
    """Atomically serialize data to JSON at filepath."""
    content = json.dumps(data, indent=indent, default=str)
    _atomic_write_text(filepath, content)


# =====================================================================
# 5. Telemetry Anomaly Engine
# =====================================================================

class TelemetryAnomalyEngine:
    """Production-grade telemetry correlation, online anomaly detection, and risk modifier engine."""

    def __init__(
        self,
        baselines_path: str = "dashboards/telemetry_baselines.json",
        registry_path: str = "dashboards/anomaly_registry.json",
        health_path: str = "dashboards/TELEMETRY_HEALTH.md",
        executions_path: str = "dashboards/telemetry_executions.json",
        z_anomalous: float = 2.5,
        z_elevated: float = 1.5,
    ) -> None:
        """Initialize engine, load persistent baselines and historical anomaly registries."""
        self.baselines_path = Path(baselines_path)
        self.registry_path = Path(registry_path)
        self.health_path = Path(health_path)
        self.executions_path = Path(executions_path)
        self.z_anomalous = float(z_anomalous)
        self.z_elevated = float(z_elevated)

        self._baselines: Dict[Tuple[str, str], BaselineState] = {}
        self._registry: List[AnomalyRecord] = []
        self._executions: List[Dict[str, Any]] = []

        self._load_baselines()
        self._load_registry()
        self._load_executions()

    def _load_baselines(self) -> None:
        """Load baseline states from JSON if available."""
        if not self.baselines_path.exists():
            return
        try:
            with open(self.baselines_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                for key_str, state_dict in data.items():
                    if "::" in key_str:
                        repo, metric_name = key_str.split("::", 1)
                        self._baselines[(repo, metric_name)] = BaselineState.from_dict(state_dict)
        except Exception:
            # Fallback cleanly on missing or malformed baseline file
            pass

    def _load_registry(self) -> None:
        """Load anomaly registry from JSON if available."""
        if not self.registry_path.exists():
            return
        try:
            with open(self.registry_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                self._registry = [AnomalyRecord.from_dict(item) for item in data if isinstance(item, dict)]
        except Exception:
            pass

    def _load_executions(self) -> None:
        """Load execution logs from JSON if available."""
        if not self.executions_path.exists():
            return
        try:
            with open(self.executions_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                self._executions = data
        except Exception:
            pass

    def _persist_baselines(self) -> None:
        """Atomic JSON write of baselines converting tuple keys to 'repo::metric' strings."""
        serialized = {
            f"{repo}::{metric}": state.as_dict()
            for (repo, metric), state in self._baselines.items()
        }
        _atomic_write_json(self.baselines_path, serialized)

    def _persist_registry(self) -> None:
        """Atomic JSON write of anomaly registry."""
        serialized = [rec.as_dict() for rec in self._registry]
        _atomic_write_json(self.registry_path, serialized)

    def _persist_executions(self) -> None:
        """Atomic JSON write of recent execution history."""
        # Retain last 200 executions for performance and bounded footprint
        _atomic_write_json(self.executions_path, self._executions[-200:])

    def ingest_execution(
        self,
        milestone_id: str,
        repo: str,
        metric_vector: MetricVector,
        provenance_hash: Optional[str] = None,
    ) -> Dict[str, AnomalyClassification]:
        """Ingest execution telemetry, compute modified Z-scores, update baselines, and persist anomalies."""
        metrics_dict = metric_vector.as_dict()
        now_iso = datetime.now(timezone.utc).isoformat()
        results: Dict[str, AnomalyClassification] = {}
        has_anomaly = False
        anomaly_count = 0

        for metric_name, val_raw in metrics_dict.items():
            val = float(val_raw)
            key = (repo, metric_name)

            if key not in self._baselines:
                self._baselines[key] = BaselineState(ema=val, ema_variance=0.0, sample_count=0)

            baseline = self._baselines[key]
            stdev = math.sqrt(max(0.0, baseline.ema_variance))
            denom = max(stdev, 1e-6)
            z_score = (val - baseline.ema) / denom

            abs_z = abs(z_score)
            if abs_z >= self.z_anomalous:
                classification = AnomalyClassification.ANOMALOUS
            elif abs_z >= self.z_elevated:
                classification = AnomalyClassification.ELEVATED
            else:
                classification = AnomalyClassification.NOMINAL

            # Update moving baseline distribution
            baseline.update(val)
            results[metric_name] = classification

            if classification != AnomalyClassification.NOMINAL:
                has_anomaly = True
                anomaly_count += 1
                record = AnomalyRecord(
                    milestone_id=milestone_id,
                    repo=repo,
                    timestamp=now_iso,
                    metric_name=metric_name,
                    value=val,
                    z_score=z_score,
                    classification=classification,
                    provenance_hash=provenance_hash,
                    correlated_outcome=None,
                )
                self._registry.append(record)

        # Log execution for rolling health tracking
        exec_record = {
            "milestone_id": milestone_id,
            "repo": repo,
            "timestamp": now_iso,
            "metrics": metrics_dict,
            "provenance_hash": provenance_hash,
            "has_anomaly": has_anomaly,
            "anomaly_count": anomaly_count,
            "classifications": {k: v.value for k, v in results.items()},
        }
        self._executions.append(exec_record)

        self._persist_baselines()
        if has_anomaly:
            self._persist_registry()
        self._persist_executions()

        return results

    def correlate_with_provenance(self, ledger_path: str = "tasks/provenance_ledger.json") -> int:
        """Read provenance ledger JSON, match by milestone_id or provenance_hash, annotate outcome."""
        path = Path(ledger_path)
        if not path.exists():
            return 0

        try:
            with open(path, "r", encoding="utf-8") as f:
                ledger_data = json.load(f)
        except Exception:
            return 0

        entries: List[Dict[str, Any]] = []
        if isinstance(ledger_data, list):
            entries = [e for e in ledger_data if isinstance(e, dict)]
        elif isinstance(ledger_data, dict):
            if "records" in ledger_data and isinstance(ledger_data["records"], list):
                entries = [e for e in ledger_data["records"] if isinstance(e, dict)]
            elif "ledger" in ledger_data and isinstance(ledger_data["ledger"], list):
                entries = [e for e in ledger_data["ledger"] if isinstance(e, dict)]
            else:
                entries = [v for v in ledger_data.values() if isinstance(v, dict)]

        lookup_by_milestone: Dict[str, str] = {}
        lookup_by_hash: Dict[str, str] = {}

        for item in entries:
            outcome = item.get("outcome") or item.get("status") or item.get("result") or "success"
            mid = item.get("milestone_id") or item.get("id")
            phash = item.get("provenance_hash") or item.get("hash")
            if mid:
                lookup_by_milestone[str(mid)] = str(outcome)
            if phash:
                lookup_by_hash[str(phash)] = str(outcome)

        updated_count = 0
        for record in self._registry:
            if record.correlated_outcome is None:
                matched_outcome = None
                if record.provenance_hash and record.provenance_hash in lookup_by_hash:
                    matched_outcome = lookup_by_hash[record.provenance_hash]
                elif record.milestone_id and record.milestone_id in lookup_by_milestone:
                    matched_outcome = lookup_by_milestone[record.milestone_id]

                if matched_outcome:
                    record.correlated_outcome = matched_outcome
                    updated_count += 1

        if updated_count > 0:
            self._persist_registry()

        return updated_count

    def compute_risk_modifier(self, filepath: str, repo: str) -> float:
        """Compute additive risk modifier (0.0 to 0.3) for target filepath's repo using 7-day half-life decay."""
        now = datetime.now(timezone.utc)
        half_life_seconds = 7.0 * 86400.0  # 7 days
        decay_lambda = math.log(2.0) / half_life_seconds

        total_risk = 0.0

        for rec in self._registry:
            if rec.repo != repo:
                continue

            weight = 0.0
            if rec.classification == AnomalyClassification.ANOMALOUS:
                weight = 0.05
            elif rec.classification == AnomalyClassification.ELEVATED:
                weight = 0.02

            if weight <= 0.0:
                continue

            try:
                rec_dt = datetime.fromisoformat(rec.timestamp)
                if rec_dt.tzinfo is None:
                    rec_dt = rec_dt.replace(tzinfo=timezone.utc)
                dt_seconds = max(0.0, (now - rec_dt).total_seconds())
            except Exception:
                dt_seconds = 0.0

            decay = math.exp(-decay_lambda * dt_seconds)
            total_risk += weight * decay

        capped_risk = min(0.3, max(0.0, total_risk))
        return round(capped_risk, 4)

    def get_health_summary(self) -> Dict[str, Any]:
        """Compute rolling telemetry health metrics over the last 50 executions."""
        last_50_execs = self._executions[-50:] if self._executions else []
        total_eval = len(last_50_execs)

        if total_eval > 0:
            anom_eval = sum(1 for e in last_50_execs if e.get("has_anomaly", False))
            overall_anomaly_rate = anom_eval / total_eval
        else:
            overall_anomaly_rate = 0.0

        # Health Grading: A (< 0.05), B (< 0.10), C (< 0.20), D (< 0.35), F (>= 0.35)
        if overall_anomaly_rate < 0.05:
            overall_grade = "A"
        elif overall_anomaly_rate < 0.10:
            overall_grade = "B"
        elif overall_anomaly_rate < 0.20:
            overall_grade = "C"
        elif overall_anomaly_rate < 0.35:
            overall_grade = "D"
        else:
            overall_grade = "F"

        all_repos = sorted(list({repo for (repo, _) in self._baselines.keys()} | {e["repo"] for e in self._executions}))
        repo_summaries: Dict[str, Any] = {}

        for repo in all_repos:
            repo_execs = [e for e in self._executions if e.get("repo") == repo][-50:]
            count = len(repo_execs)
            if count == 0:
                continue

            anom_count = sum(1 for e in repo_execs if e.get("has_anomaly", False))
            rate = anom_count / count
            avg_tok = sum(e["metrics"]["total_tokens"] for e in repo_execs) / count
            avg_wall = sum(e["metrics"]["wall_time_s"] for e in repo_execs) / count

            # Trend analysis comparing last 10 vs previous 10 executions
            token_trend = "→"
            latency_trend = "→"
            if count >= 2:
                last_10 = repo_execs[-10:]
                prev_10 = repo_execs[-20:-10] if count >= 20 else repo_execs[:-10] if count > 10 else repo_execs[:max(1, count // 2)]
                
                curr_avg_tok = sum(e["metrics"]["total_tokens"] for e in last_10) / len(last_10)
                prev_avg_tok = sum(e["metrics"]["total_tokens"] for e in prev_10) / len(prev_10) if prev_10 else curr_avg_tok
                
                curr_avg_wall = sum(e["metrics"]["wall_time_s"] for e in last_10) / len(last_10)
                prev_avg_wall = sum(e["metrics"]["wall_time_s"] for e in prev_10) / len(prev_10) if prev_10 else curr_avg_wall

                delta_tok = (curr_avg_tok - prev_avg_tok) / max(prev_avg_tok, 1.0)
                delta_wall = (curr_avg_wall - prev_avg_wall) / max(prev_avg_wall, 0.001)

                def _pct_to_arrow(pct: float) -> str:
                    if pct > 0.20:
                        return "↑"
                    elif pct > 0.05:
                        return "↗"
                    elif pct >= -0.05:
                        return "→"
                    elif pct >= -0.20:
                        return "↘"
                    else:
                        return "↓"

                token_trend = _pct_to_arrow(delta_tok)
                latency_trend = _pct_to_arrow(delta_wall)

            repo_summaries[repo] = {
                "execution_count": count,
                "anomaly_rate": round(rate, 4),
                "avg_tokens": round(avg_tok, 1),
                "token_trend": token_trend,
                "avg_wall_time_s": round(avg_wall, 3),
                "latency_trend": latency_trend,
            }

        return {
            "overall_grade": overall_grade,
            "overall_anomaly_rate": round(overall_anomaly_rate, 4),
            "total_executions_evaluated": total_eval,
            "total_anomalies_recorded": len(self._registry),
            "repos": repo_summaries,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def generate_health_dashboard(self) -> None:
        """Write dashboards/TELEMETRY_HEALTH.md atomically with metric tables, trends, and top anomalies."""
        summary = self.get_health_summary()
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        # Top 5 most severe anomalies by absolute Z-score
        top_5 = sorted(self._registry, key=lambda r: abs(r.z_score), reverse=True)[:5]

        lines = [
            "# Execution Telemetry & Resource Anomaly Health Dashboard",
            "",
            f"**Last Updated:** `{now_str}`  ",
            f"**System Telemetry Health Grade:** `{summary['overall_grade']}`  ",
            f"**Overall Anomaly Rate (Last 50 Executions):** `{summary['overall_anomaly_rate'] * 100.0:.1f}%`  ",
            f"**Total Historical Anomalies Tracked:** `{summary['total_anomalies_recorded']}`",
            "",
            "---",
            "",
            "## 1. Per-Repository Health & Resource Trends (Last 20-50 Executions)",
            "",
            "| Repository | Window Size | Anomaly Rate | Avg Tokens | Token Trend | Avg Wall Time (s) | Latency Trend | Status |",
            "| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :--- |",
        ]

        if not summary["repos"]:
            lines.append("| *No executions recorded* | - | - | - | - | - | - | `INITIALIZING` |")
        else:
            for repo, r_data in summary["repos"].items():
                rate_pct = f"{r_data['anomaly_rate'] * 100.0:.1f}%"
                status_badge = "`NOMINAL`" if r_data["anomaly_rate"] < 0.10 else ("`ELEVATED`" if r_data["anomaly_rate"] < 0.25 else "`DEGRADED`")
                lines.append(
                    f"| `{repo}` | {r_data['execution_count']} | {rate_pct} | "
                    f"{r_data['avg_tokens']:,.0f} | **{r_data['token_trend']}** | "
                    f"{r_data['avg_wall_time_s']:.2f}s | **{r_data['latency_trend']}** | {status_badge} |"
                )

        lines.extend([
            "",
            "---",
            "",
            "## 2. Top-5 Most Severe Telemetry Anomalies",
            "",
            "| Milestone ID | Repository | Timestamp | Metric | Observed | Z-Score | Classification | Correlated Outcome |",
            "| :--- | :--- | :--- | :--- | :---: | :---: | :--- | :--- |",
        ])

        if not top_5:
            lines.append("| *None* | - | - | - | - | - | `NOMINAL` | *N/A* |")
        else:
            for a in top_5:
                outcome_str = f"`{a.correlated_outcome}`" if a.correlated_outcome else "*Uncorrelated*"
                lines.append(
                    f"| `{a.milestone_id}` | `{a.repo}` | `{a.timestamp[:19]}` | "
                    f"`{a.metric_name}` | {a.value:,.1f} | `{a.z_score:+.2f}σ` | "
                    f"`{a.classification.value}` | {outcome_str} |"
                )

        lines.extend([
            "",
            "---",
            "",
            "## 3. Active Telemetry Baselines Snapshot",
            "",
            "| Repository | Metric Dimension | EMA Baseline | Stdev (σ) | Sample Count |",
            "| :--- | :--- | :---: | :---: | :---: |",
        ])

        if not self._baselines:
            lines.append("| *None* | - | - | - | - |")
        else:
            for (repo, metric), state in sorted(self._baselines.items()):
                stdev = math.sqrt(max(0.0, state.ema_variance))
                lines.append(
                    f"| `{repo}` | `{metric}` | {state.ema:,.2f} | {stdev:,.2f} | {state.sample_count} |"
                )

        lines.extend([
            "",
            "---",
            "",
            "## 4. Operational Risk & Policy Gate Remediations",
            "",
        ])

        if summary["overall_grade"] in ("A", "B"):
            lines.extend([
                "- **Nominal Stability:** Execution resource profiles remain within empirical boundary limits.",
                "- **Policy Action:** Standard dispatch concurrency permitted; no rate throttling required.",
            ])
        elif summary["overall_grade"] == "C":
            lines.extend([
                "- **Elevated Variance Detected:** Moderate token or latency spikes observed in recent milestones.",
                "- **Policy Action:** Enable preemptive token governor pacing; monitor elevated repositories.",
            ])
        else:
            lines.extend([
                "- **Critical Resource Divergence:** Frequent severe anomalies (>2.5σ) detected.",
                "- **Policy Action:** Enforce conservative rate gating, limit parallel agent workers, and review recent rollbacks.",
            ])

        lines.append("")
        content = "\n".join(lines)
        _atomic_write_text(self.health_path, content)


# =====================================================================
# 6. Flask Blueprint Registration
# =====================================================================

_global_engine: Optional[TelemetryAnomalyEngine] = None


def get_engine() -> TelemetryAnomalyEngine:
    """Retrieve or initialize singleton TelemetryAnomalyEngine instance."""
    global _global_engine
    if _global_engine is None:
        _global_engine = TelemetryAnomalyEngine()
    return _global_engine


def set_engine(engine: TelemetryAnomalyEngine) -> None:
    """Assign explicit TelemetryAnomalyEngine instance."""
    global _global_engine
    _global_engine = engine


if HAS_FLASK:
    telemetry_bp = Blueprint("telemetry_bp", __name__)

    @telemetry_bp.route("/api/telemetry/health", methods=["GET"])
    @telemetry_bp.route("/health", methods=["GET"])
    def telemetry_health():
        """Return JSON telemetry health summary."""
        engine = get_engine()
        return jsonify(engine.get_health_summary())

    @telemetry_bp.route("/api/telemetry/anomalies", methods=["GET"])
    @telemetry_bp.route("/anomalies", methods=["GET"])
    def telemetry_anomalies():
        """Return filtered anomaly records from registry."""
        engine = get_engine()
        repo_filter = request.args.get("repo")
        limit = int(request.args.get("limit", 50))
        classification_filter = request.args.get("classification")

        records = engine._registry
        if repo_filter:
            records = [r for r in records if r.repo == repo_filter]
        if classification_filter:
            records = [
                r for r in records
                if (r.classification.value if isinstance(r.classification, AnomalyClassification) else str(r.classification)).upper()
                == classification_filter.upper()
            ]

        sorted_records = sorted(records, key=lambda r: r.timestamp, reverse=True)[:limit]
        return jsonify([r.as_dict() for r in sorted_records])

    @telemetry_bp.route("/api/telemetry/risk-modifier/<path:filepath>", methods=["GET"])
    @telemetry_bp.route("/risk-modifier/<path:filepath>", methods=["GET"])
    def telemetry_risk_modifier(filepath: str):
        """Calculate and return risk modifier float for target filepath and repo."""
        engine = get_engine()
        repo = request.args.get("repo", "")
        risk = engine.compute_risk_modifier(filepath, repo)
        return jsonify({
            "filepath": filepath,
            "repo": repo,
            "risk_modifier": risk,
        })
else:
    telemetry_bp = None


# =====================================================================
# 7. Phoenix Telemetry Ingestion Integration
# =====================================================================

def ingest_from_phoenix(phoenix_db_path: str, engine: TelemetryAnomalyEngine) -> int:
    """Read Arize Phoenix SQLite spans table, aggregate metrics per trace/milestone, and ingest."""
    db_path = Path(phoenix_db_path)
    if not db_path.exists():
        return 0

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='spans';")
        if not cursor.fetchone():
            conn.close()
            return 0

        cursor.execute("SELECT * FROM spans;")
        rows = cursor.fetchall()
        conn.close()
    except Exception:
        return 0

    traces: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        r = dict(row)
        trace_id = str(r.get("trace_id") or r.get("id") or "default_trace")
        if trace_id not in traces:
            traces[trace_id] = []
        traces[trace_id].append(r)

    ingested_count = 0
    for trace_id, span_list in traces.items():
        total_tokens = 0
        error_count = 0
        llm_calls = 0
        retry_count = 0
        min_start = float("inf")
        max_end = float("-inf")
        repo = "hermes_orchestrator"
        milestone_id = f"phoenix_{trace_id[:8]}"

        for s in span_list:
            attrs_raw = s.get("attributes") or "{}"
            attrs: Dict[str, Any] = {}
            if isinstance(attrs_raw, str):
                try:
                    attrs = json.loads(attrs_raw)
                except Exception:
                    pass
            elif isinstance(attrs_raw, dict):
                attrs = attrs_raw

            tok = attrs.get("llm.token_count.total") or attrs.get("total_tokens") or s.get("total_tokens") or 0
            total_tokens += int(tok)

            status_str = str(s.get("status_code", "")).upper()
            if status_str == "ERROR" or "error" in str(s.get("status", "")).lower():
                error_count += 1

            name = str(s.get("name", "")).lower()
            if "retry" in name:
                retry_count += 1
            if "llm" in name or attrs.get("span_kind") == "LLM":
                llm_calls += 1

            if "repo" in attrs:
                repo = str(attrs["repo"])
            if "milestone_id" in attrs:
                milestone_id = str(attrs["milestone_id"])

            start_t = s.get("start_time")
            end_t = s.get("end_time")
            if isinstance(start_t, (int, float)):
                min_start = min(min_start, float(start_t))
            if isinstance(end_t, (int, float)):
                max_end = max(max_end, float(end_t))

        wall_time_s = 0.0
        if min_start < float("inf") and max_end > float("-inf") and max_end >= min_start:
            diff = max_end - min_start
            if diff > 1e12:  # Nanoseconds
                wall_time_s = diff / 1e9
            elif diff > 1e6:  # Microseconds
                wall_time_s = diff / 1e6
            else:
                wall_time_s = diff

        mv = MetricVector(
            total_tokens=total_tokens,
            wall_time_s=round(wall_time_s, 3),
            error_count=error_count,
            retry_count=retry_count,
            span_depth=len(span_list),
            llm_calls=llm_calls,
        )

        engine.ingest_execution(
            milestone_id=milestone_id,
            repo=repo,
            metric_vector=mv,
            provenance_hash=trace_id,
        )
        ingested_count += 1

    return ingested_count


# =====================================================================
# Demo Execution & Self-Test
# =====================================================================

if __name__ == "__main__":
    demo_dir = Path("dashboards")
    demo_dir.mkdir(parents=True, exist_ok=True)

    engine = TelemetryAnomalyEngine(
        baselines_path="dashboards/telemetry_baselines.json",
        registry_path="dashboards/anomaly_registry.json",
        health_path="dashboards/TELEMETRY_HEALTH.md",
        executions_path="dashboards/telemetry_executions.json",
        z_anomalous=2.5,
        z_elevated=1.5,
    )
    set_engine(engine)

    print("==================================================================")
    print("Starting Telemetry Anomaly Engine Demo Execution...")
    print("==================================================================")

    # Ingest 10 synthetic execution profiles across 2 repos
    synthetic_runs = [
        ("M-101", "hermes_orchestrator", MetricVector(total_tokens=1500, wall_time_s=2.5, error_count=0, retry_count=0, span_depth=4, llm_calls=2)),
        ("M-102", "hermes_orchestrator", MetricVector(total_tokens=1620, wall_time_s=2.8, error_count=0, retry_count=0, span_depth=4, llm_calls=2)),
        ("M-103", "hermes_orchestrator", MetricVector(total_tokens=1580, wall_time_s=2.4, error_count=0, retry_count=0, span_depth=5, llm_calls=2)),
        ("M-104", "v2ray_tool", MetricVector(total_tokens=800, wall_time_s=1.2, error_count=0, retry_count=0, span_depth=2, llm_calls=1)),
        ("M-105", "v2ray_tool", MetricVector(total_tokens=850, wall_time_s=1.3, error_count=0, retry_count=0, span_depth=2, llm_calls=1)),
        ("M-106", "hermes_orchestrator", MetricVector(total_tokens=1550, wall_time_s=2.6, error_count=0, retry_count=0, span_depth=4, llm_calls=2)),
        ("M-107", "hermes_orchestrator", MetricVector(total_tokens=1600, wall_time_s=2.7, error_count=0, retry_count=0, span_depth=4, llm_calls=2)),
        # Elevated run
        ("M-108", "hermes_orchestrator", MetricVector(total_tokens=3200, wall_time_s=6.8, error_count=1, retry_count=1, span_depth=8, llm_calls=4)),
        # Anomalous run (severe token + latency explosion)
        ("M-109", "hermes_orchestrator", MetricVector(total_tokens=9400, wall_time_s=22.5, error_count=4, retry_count=3, span_depth=14, llm_calls=9)),
        # Return to nominal
        ("M-110", "hermes_orchestrator", MetricVector(total_tokens=1650, wall_time_s=2.9, error_count=0, retry_count=0, span_depth=4, llm_calls=2)),
    ]

    for mid, r_name, mv in synthetic_runs:
        classes = engine.ingest_execution(
            milestone_id=mid,
            repo=r_name,
            metric_vector=mv,
            provenance_hash=f"hash_{mid.lower()}",
        )
        non_nominal = {k: v.value for k, v in classes.items() if v != AnomalyClassification.NOMINAL}
        status_flag = f"Anomalies: {non_nominal}" if non_nominal else "NOMINAL"
        print(f"Ingested {mid} ({r_name}): {status_flag}")

    # Generate health dashboard
    engine.generate_health_dashboard()

    # Calculate risk modifier for policy gate
    risk_mod = engine.compute_risk_modifier("tasks/telemetry_anomaly_engine.py", "hermes_orchestrator")
    summary = engine.get_health_summary()

    print("\n--- Telemetry Health Summary ---")
    print(f"Health Grade: {summary['overall_grade']}")
    print(f"Overall Anomaly Rate: {summary['overall_anomaly_rate'] * 100.0:.1f}%")
    print(f"Risk Modifier for 'hermes_orchestrator': +{risk_mod:.4f}")
    print(f"Dashboard generated at: {engine.health_path.resolve()}")
    print("==================================================================")
    print("Execution complete.")