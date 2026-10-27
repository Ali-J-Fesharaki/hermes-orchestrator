"""tasks/steering_memory_engine.py

Steering Memory & Planner Feedback Loop with Execution Outcome Distillation.
Maintains persistent, bounded historical telemetry and outcome distillation
to steer autonomous milestone scheduling and strategic planning loops.
"""

from __future__ import annotations

import collections
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
import json
import logging
import math
import os
from pathlib import Path
import statistics
import threading
from typing import Any, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


def _normalize_dt(dt: datetime) -> datetime:
    """Normalize datetime to timezone-aware UTC datetime."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _atomic_write_text(filepath: Path, content: str) -> None:
    """Safely write text content to a file using an atomic temporary swap."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    temp_file = filepath.with_name(
        f"{filepath.name}.tmp.{os.getpid()}.{threading.get_ident()}"
    )
    try:
        with open(temp_file, "w", encoding="utf-8") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_file, filepath)
    except Exception as exc:
        if temp_file.exists():
            try:
                temp_file.unlink()
            except OSError:
                pass
        logger.error("Failed atomic write to %s: %s", filepath, exc)
        raise


@dataclass
class OutcomeRecord:
    """Granular execution telemetry record for a completed or terminated milestone."""

    milestone_id: str
    repo: str
    status: str
    planned_at: datetime
    started_at: datetime
    completed_at: datetime
    estimated_complexity: float
    actual_token_burn: int
    budget_token_limit: int
    retry_count: int
    error_classifications: List[str] = field(default_factory=list)
    dependency_wait_seconds: float = 0.0
    files_touched: int = 0
    anomaly_signals: List[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Serialize outcome record to a JSON-compatible dictionary."""
        return {
            "milestone_id": self.milestone_id,
            "repo": self.repo,
            "status": self.status,
            "planned_at": self.planned_at.isoformat(),
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat(),
            "estimated_complexity": float(self.estimated_complexity),
            "actual_token_burn": int(self.actual_token_burn),
            "budget_token_limit": int(self.budget_token_limit),
            "retry_count": int(self.retry_count),
            "error_classifications": list(self.error_classifications),
            "dependency_wait_seconds": float(self.dependency_wait_seconds),
            "files_touched": int(self.files_touched),
            "anomaly_signals": list(self.anomaly_signals),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "OutcomeRecord":
        """Deserialize outcome record from a dictionary."""
        return cls(
            milestone_id=str(data["milestone_id"]),
            repo=str(data["repo"]),
            status=str(data["status"]),
            planned_at=datetime.fromisoformat(data["planned_at"]),
            started_at=datetime.fromisoformat(data["started_at"]),
            completed_at=datetime.fromisoformat(data["completed_at"]),
            estimated_complexity=float(data.get("estimated_complexity", 1.0)),
            actual_token_burn=int(data.get("actual_token_burn", 0)),
            budget_token_limit=int(data.get("budget_token_limit", 100000)),
            retry_count=int(data.get("retry_count", 0)),
            error_classifications=list(data.get("error_classifications", [])),
            dependency_wait_seconds=float(data.get("dependency_wait_seconds", 0.0)),
            files_touched=int(data.get("files_touched", 0)),
            anomaly_signals=list(data.get("anomaly_signals", [])),
        )


@dataclass
class RepoCapabilityProfile:
    """Distilled repository capability profile using exponential weighted moving averages."""

    repo: str
    mean_token_ratio: float
    mean_duration_ratio: float
    failure_rate: float
    dominant_error_classes: List[str]
    sample_count: int
    last_updated: datetime

    def to_dict(self) -> dict:
        """Serialize repository profile to dictionary."""
        return {
            "repo": self.repo,
            "mean_token_ratio": round(self.mean_token_ratio, 4),
            "mean_duration_ratio": round(self.mean_duration_ratio, 4),
            "failure_rate": round(self.failure_rate, 4),
            "dominant_error_classes": list(self.dominant_error_classes),
            "sample_count": self.sample_count,
            "last_updated": self.last_updated.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "RepoCapabilityProfile":
        """Deserialize repository profile from dictionary."""
        return cls(
            repo=str(data["repo"]),
            mean_token_ratio=float(data["mean_token_ratio"]),
            mean_duration_ratio=float(data["mean_duration_ratio"]),
            failure_rate=float(data["failure_rate"]),
            dominant_error_classes=list(data.get("dominant_error_classes", [])),
            sample_count=int(data.get("sample_count", 0)),
            last_updated=datetime.fromisoformat(data["last_updated"]),
        )


@dataclass
class CrossCuttingHeuristic:
    """Mined cross-cutting associative heuristic identifying failure-correlated patterns."""

    rule: str
    confidence: float
    supporting_evidence_count: int
    feature_correlations: dict

    def to_dict(self) -> dict:
        """Serialize heuristic to dictionary."""
        return {
            "rule": self.rule,
            "confidence": round(self.confidence, 4),
            "supporting_evidence_count": self.supporting_evidence_count,
            "feature_correlations": self.feature_correlations,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CrossCuttingHeuristic":
        """Deserialize heuristic from dictionary."""
        return cls(
            rule=str(data["rule"]),
            confidence=float(data["confidence"]),
            supporting_evidence_count=int(data["supporting_evidence_count"]),
            feature_correlations=dict(data.get("feature_correlations", {})),
        )


@dataclass
class DistilledMemory:
    """Condensed historical execution knowledge for strategic prompt steering."""

    repo_profiles: Dict[str, RepoCapabilityProfile]
    heuristics: List[CrossCuttingHeuristic]
    total_outcomes_processed: int
    last_distillation: datetime

    def to_dict(self) -> dict:
        """Serialize distilled memory container to dictionary."""
        return {
            "repo_profiles": {k: v.to_dict() for k, v in self.repo_profiles.items()},
            "heuristics": [h.to_dict() for h in self.heuristics],
            "total_outcomes_processed": self.total_outcomes_processed,
            "last_distillation": self.last_distillation.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "DistilledMemory":
        """Deserialize distilled memory container from dictionary."""
        profiles = {
            k: RepoCapabilityProfile.from_dict(v)
            for k, v in data.get("repo_profiles", {}).items()
        }
        heuristics = [
            CrossCuttingHeuristic.from_dict(h)
            for h in data.get("heuristics", [])
        ]
        return cls(
            repo_profiles=profiles,
            heuristics=heuristics,
            total_outcomes_processed=int(data.get("total_outcomes_processed", 0)),
            last_distillation=datetime.fromisoformat(data["last_distillation"]),
        )

    def to_markdown(self, max_tokens: int = 800) -> str:
        """Render a compact steering context string capped within max_tokens.

        Sections are formatted with prioritized retention:
        Priority 1: Critical warnings (high failure rate > 0.3 or token ratio > 2.0).
        Priority 2: Per-repo capability profiles as a structured markdown table.
        Priority 3: General cross-cutting heuristics.
        Truncates lowest-priority sections first based on ~4 characters per token.
        """
        max_chars = max(100, int(max_tokens * 4))

        # 1. Critical Warnings (Priority 1)
        critical_alerts: List[str] = []
        for repo_name, profile in sorted(self.repo_profiles.items()):
            if profile.failure_rate > 0.3:
                err_str = (
                    ", ".join(profile.dominant_error_classes)
                    if profile.dominant_error_classes
                    else "General Failures"
                )
                critical_alerts.append(
                    f"- ⚠️ **{repo_name}**: Critical failure rate ({profile.failure_rate:.1%}). Dominant errors: {err_str}."
                )
            if profile.mean_token_ratio > 2.0:
                critical_alerts.append(
                    f"- ⚠️ **{repo_name}**: Severe token overburn ({profile.mean_token_ratio:.2f}x budget limit)."
                )

        if critical_alerts:
            sec_warnings = "### Critical Operational Warnings\n" + "\n".join(critical_alerts)
        else:
            sec_warnings = "### Critical Operational Warnings\n- None. All repositories within nominal operational tolerances."

        # 2. Repo Profiles Table (Priority 2)
        if self.repo_profiles:
            table_lines = [
                "### Repository Capability Profiles",
                "| Repository | Token Ratio | Duration Ratio | Failure Rate | Samples | Dominant Errors |",
                "| :--- | :---: | :---: | :---: | :---: | :--- |",
            ]
            for repo_name, profile in sorted(self.repo_profiles.items()):
                errs = ", ".join(profile.dominant_error_classes) if profile.dominant_error_classes else "None"
                table_lines.append(
                    f"| {repo_name} | {profile.mean_token_ratio:.2f}x | {profile.mean_duration_ratio:.2f}x | "
                    f"{profile.failure_rate:.1%} | {profile.sample_count} | {errs} |"
                )
            sec_profiles = "\n".join(table_lines)
        else:
            sec_profiles = "### Repository Capability Profiles\n- No historical repository profiles recorded."

        # 3. Cross-Cutting Heuristics (Priority 3)
        if self.heuristics:
            h_lines = ["### Cross-Cutting Steering Heuristics"]
            for h in self.heuristics:
                h_lines.append(
                    f"- **Rule**: {h.rule} (Confidence: {h.confidence:.1%}, N={h.supporting_evidence_count})"
                )
            sec_heuristics = "\n".join(h_lines)
        else:
            sec_heuristics = "### Cross-Cutting Steering Heuristics\n- No statistically significant heuristics discovered."

        # Assemble with graceful progressive degradation
        full_text = f"{sec_warnings}\n\n{sec_profiles}\n\n{sec_heuristics}"
        if len(full_text) <= max_chars:
            return full_text

        # Budget exceeded: Attempt to trim heuristics lines from lowest priority
        heuristics_header = "### Cross-Cutting Steering Heuristics\n"
        budget_for_h = max_chars - len(sec_warnings) - len(sec_profiles) - 4
        if self.heuristics and budget_for_h > len(heuristics_header) + 40:
            truncated_h_lines = [heuristics_header.strip()]
            cur_len = len(heuristics_header)
            for h in self.heuristics:
                line = f"- **Rule**: {h.rule} (Confidence: {h.confidence:.1%}, N={h.supporting_evidence_count})"
                if cur_len + len(line) + 50 <= budget_for_h:
                    truncated_h_lines.append(line)
                    cur_len += len(line) + 1
                else:
                    truncated_h_lines.append("- *(Additional heuristics truncated for budget)*")
                    break
            sec_heuristics_trimmed = "\n".join(truncated_h_lines)
            composed = f"{sec_warnings}\n\n{sec_profiles}\n\n{sec_heuristics_trimmed}"
            if len(composed) <= max_chars:
                return composed

        # Drop heuristics completely
        composed_no_h = f"{sec_warnings}\n\n{sec_profiles}"
        if len(composed_no_h) <= max_chars:
            return composed_no_h

        # If even profiles exceed budget, truncate table rows
        budget_for_p = max_chars - len(sec_warnings) - 4
        if budget_for_p > 180:
            p_lines = [
                "### Repository Capability Profiles",
                "| Repository | Token Ratio | Duration Ratio | Failure Rate | Samples | Dominant Errors |",
                "| :--- | :---: | :---: | :---: | :---: | :--- |",
            ]
            cur_p_len = sum(len(line) + 1 for line in p_lines)
            for repo_name, profile in sorted(self.repo_profiles.items()):
                errs = ", ".join(profile.dominant_error_classes) if profile.dominant_error_classes else "None"
                row = (
                    f"| {repo_name} | {profile.mean_token_ratio:.2f}x | {profile.mean_duration_ratio:.2f}x | "
                    f"{profile.failure_rate:.1%} | {profile.sample_count} | {errs} |"
                )
                if cur_p_len + len(row) + 1 <= budget_for_p:
                    p_lines.append(row)
                    cur_p_len += len(row) + 1
                else:
                    break
            sec_profiles_trimmed = "\n".join(p_lines)
            composed_p = f"{sec_warnings}\n\n{sec_profiles_trimmed}"
            if len(composed_p) <= max_chars:
                return composed_p

        # Last resort: truncate warnings to fit hard character cap
        if len(sec_warnings) <= max_chars:
            return sec_warnings
        return sec_warnings[: max_chars - 30] + "\n- *(Output truncated for token budget)*"


class MemoryStore:
    """Thread-safe persistent storage and periodic distillation manager for steering memory."""

    MAX_FILE_BYTES: int = 10 * 1024 * 1024  # 10MB rotation threshold
    EWMA_ALPHA: float = 0.15
    HISTORICAL_CUTOFF_HOURS: int = 72

    def __init__(
        self,
        storage_path: Union[str, Path] = "data/steering_memory.jsonl",
        distill_interval_hours: int = 6,
        distill_outcome_threshold: int = 20,
    ) -> None:
        """Initialize MemoryStore with path configurations and concurrency locks."""
        self.storage_path = Path(storage_path)
        self.distilled_path = self.storage_path.parent / (
            self.storage_path.stem + "_distilled.json"
        )
        self.distill_interval_hours = distill_interval_hours
        self.distill_outcome_threshold = distill_outcome_threshold

        self._lock = threading.Lock()
        self._outcomes_since_distill: int = 0
        self._last_distill_time: datetime = datetime.now(timezone.utc)
        self._cached_distilled_memory: Optional[DistilledMemory] = None

        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        self._load_cached_distillation()

    def _load_cached_distillation(self) -> None:
        """Attempt to load previously persisted DistilledMemory from disk."""
        if self.distilled_path.exists():
            try:
                with open(self.distilled_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self._cached_distilled_memory = DistilledMemory.from_dict(data)
                    self._last_distill_time = self._cached_distilled_memory.last_distillation
            except Exception as exc:
                logger.warning("Could not read distilled memory cache: %s", exc)

    def _rotate_if_needed(self) -> None:
        """Rotate JSONL storage file to archive .jsonl.1 if size exceeds 10MB."""
        if not self.storage_path.exists():
            return

        try:
            current_size = self.storage_path.stat().st_size
            if current_size >= self.MAX_FILE_BYTES:
                archive_path = self.storage_path.with_name(
                    f"{self.storage_path.name}.1"
                )
                if archive_path.exists():
                    archive_path.unlink()
                self.storage_path.rename(archive_path)
                logger.info(
                    "Rotated steering memory log (%d bytes) to %s",
                    current_size,
                    archive_path,
                )
        except OSError as exc:
            logger.error("Error during log rotation: %s", exc)

    def append_outcome(self, record: OutcomeRecord) -> None:
        """Validate, thread-safely append an outcome record, and trigger distillation if due."""
        if not isinstance(record, OutcomeRecord):
            raise TypeError(f"Expected OutcomeRecord instance, got {type(record)}")

        with self._lock:
            self._rotate_if_needed()
            serialized_line = json.dumps(record.to_dict()) + "\n"

            with open(self.storage_path, "a", encoding="utf-8") as f:
                f.write(serialized_line)
                f.flush()

            self._outcomes_since_distill += 1

            now = datetime.now(timezone.utc)
            elapsed_hours = (now - self._last_distill_time).total_seconds() / 3600.0
            time_due = elapsed_hours >= self.distill_interval_hours
            count_due = self._outcomes_since_distill >= self.distill_outcome_threshold

            if time_due or count_due:
                logger.info(
                    "Distillation triggered (count_due=%s, time_due=%s)",
                    count_due,
                    time_due,
                )
                self._distill_locked()

    def distill(self, force_all: bool = False) -> DistilledMemory:
        """Public thread-safe entrypoint to execute historical outcome distillation."""
        with self._lock:
            return self._distill_locked(force_all=force_all)

    def _distill_locked(self, force_all: bool = False) -> DistilledMemory:
        """Internal distillation routine executed under acquired lock."""
        now = datetime.now(timezone.utc)
        records: List[OutcomeRecord] = []

        if self.storage_path.exists():
            with open(self.storage_path, "r", encoding="utf-8") as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record_data = json.loads(line)
                        records.append(OutcomeRecord.from_dict(record_data))
                    except Exception as err:
                        logger.warning("Malformed record at line %d: %s", line_num, err)

        if not records:
            empty_memory = self._cached_distilled_memory or DistilledMemory(
                repo_profiles={},
                heuristics=[],
                total_outcomes_processed=0,
                last_distillation=now,
            )
            self._cached_distilled_memory = empty_memory
            return empty_memory

        cutoff = now - timedelta(hours=self.HISTORICAL_CUTOFF_HOURS)
        historical: List[OutcomeRecord] = []
        recent: List[OutcomeRecord] = []

        for r in records:
            if _normalize_dt(r.completed_at) <= cutoff:
                historical.append(r)
            else:
                recent.append(r)

        # Distill historical records, or fall back to all records if forced or bootstrapping
        if historical or force_all:
            records_to_distill = historical if not force_all else records
            records_to_retain = recent if not force_all else []
        else:
            # Bootstrap fallback: distill all records if no historical records exist yet
            records_to_distill = records
            records_to_retain = recent

        # Compute per-repo capability profiles via EWMA (alpha=0.15)
        repo_groups: Dict[str, List[OutcomeRecord]] = collections.defaultdict(list)
        for r in records_to_distill:
            repo_groups[r.repo].append(r)

        existing_profiles = (
            self._cached_distilled_memory.repo_profiles
            if self._cached_distilled_memory
            else {}
        )
        updated_profiles: Dict[str, RepoCapabilityProfile] = dict(existing_profiles)

        for repo_name, repo_records in repo_groups.items():
            repo_records.sort(key=lambda x: _normalize_dt(x.completed_at))
            profile = self._calculate_repo_ewma(
                repo_name, repo_records, updated_profiles.get(repo_name), now
            )
            updated_profiles[repo_name] = profile

        # Generate cross-cutting heuristics via Pearson correlation
        heuristics = self._mine_association_heuristics(records_to_distill)

        # Formulate DistilledMemory
        total_processed = (
            (self._cached_distilled_memory.total_outcomes_processed if self._cached_distilled_memory else 0)
            + len(records_to_distill)
        )
        distilled = DistilledMemory(
            repo_profiles=updated_profiles,
            heuristics=heuristics,
            total_outcomes_processed=total_processed,
            last_distillation=now,
        )

        # Persist distilled output atomically
        _atomic_write_text(
            self.distilled_path, json.dumps(distilled.to_dict(), indent=2)
        )

        # Prune raw storage: retain only recent records
        retained_payload = "".join(json.dumps(r.to_dict()) + "\n" for r in records_to_retain)
        _atomic_write_text(self.storage_path, retained_payload)

        self._cached_distilled_memory = distilled
        self._outcomes_since_distill = 0
        self._last_distill_time = now

        logger.info(
            "Distillation completed: processed %d records, pruned storage to %d recent records",
            len(records_to_distill),
            len(records_to_retain),
        )
        return distilled

    def _calculate_repo_ewma(
        self,
        repo_name: str,
        records: List[OutcomeRecord],
        previous_profile: Optional[RepoCapabilityProfile],
        timestamp: datetime,
    ) -> RepoCapabilityProfile:
        """Compute EWMA capability profile over chronologically sorted outcomes."""
        alpha = self.EWMA_ALPHA
        error_counts = collections.Counter()

        if previous_profile:
            token_ratio = previous_profile.mean_token_ratio
            duration_ratio = previous_profile.mean_duration_ratio
            failure_rate = previous_profile.failure_rate
            sample_count = previous_profile.sample_count
            for err in previous_profile.dominant_error_classes:
                error_counts[err] += 1
            start_idx = 0
        else:
            first = records[0]
            token_ratio = first.actual_token_burn / max(1, first.budget_token_limit)
            duration_sec = max(0.0, (first.completed_at - first.started_at).total_seconds())
            duration_ratio = duration_sec / max(1.0, first.estimated_complexity)
            failure_rate = 1.0 if self._is_failure(first.status) else 0.0
            sample_count = 1
            for err in first.error_classifications:
                error_counts[err] += 1
            start_idx = 1

        for r in records[start_idx:]:
            curr_token_ratio = r.actual_token_burn / max(1, r.budget_token_limit)
            duration_sec = max(0.0, (r.completed_at - r.started_at).total_seconds())
            curr_duration_ratio = duration_sec / max(1.0, r.estimated_complexity)
            curr_failure = 1.0 if self._is_failure(r.status) else 0.0

            token_ratio = alpha * curr_token_ratio + (1.0 - alpha) * token_ratio
            duration_ratio = alpha * curr_duration_ratio + (1.0 - alpha) * duration_ratio
            failure_rate = alpha * curr_failure + (1.0 - alpha) * failure_rate
            sample_count += 1

            for err in r.error_classifications:
                error_counts[err] += 1

        top_errors = [err for err, _ in error_counts.most_common(3)]
        return RepoCapabilityProfile(
            repo=repo_name,
            mean_token_ratio=token_ratio,
            mean_duration_ratio=duration_ratio,
            failure_rate=failure_rate,
            dominant_error_classes=top_errors,
            sample_count=sample_count,
            last_updated=timestamp,
        )

    def _mine_association_heuristics(
        self, records: List[OutcomeRecord]
    ) -> List[CrossCuttingHeuristic]:
        """Compute Pearson correlation between binary features and milestone failures."""
        n_samples = len(records)
        if n_samples < 5:
            return []

        # Target outcome vector: 1.0 if failure, 0.0 if success
        y = [1.0 if self._is_failure(r.status) else 0.0 for r in records]
        y_mean = statistics.mean(y)
        var_y = sum((val - y_mean) ** 2 for val in y)

        if var_y == 0.0:
            # All outcomes had the exact same status; correlation undefined
            return []

        # Collect candidate binary features
        all_errors = set()
        for r in records:
            all_errors.update(r.error_classifications)

        features: Dict[str, List[float]] = {
            "files_touched > 3": [1.0 if r.files_touched > 3 else 0.0 for r in records],
            "retry_count > 0": [1.0 if r.retry_count > 0 else 0.0 for r in records],
            "dependency_wait > 30s": [
                1.0 if r.dependency_wait_seconds > 30.0 else 0.0 for r in records
            ],
            "burn_exceeds_budget": [
                1.0 if r.actual_token_burn > r.budget_token_limit else 0.0
                for r in records
            ],
        }

        for err in all_errors:
            features[f"error:{err}"] = [
                1.0 if err in r.error_classifications else 0.0 for r in records
            ]

        heuristics: List[CrossCuttingHeuristic] = []

        for feat_name, x in features.items():
            x_mean = statistics.mean(x)
            var_x = sum((val - x_mean) ** 2 for val in x)
            if var_x == 0.0:
                continue

            cov_xy = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, y))
            r_corr = cov_xy / math.sqrt(var_x * var_y)

            if abs(r_corr) > 0.3:
                # Calculate support and rule confidence
                feat_count = sum(1 for val in x if val == 1.0)
                joint_fail_count = sum(
                    1 for xi, yi in zip(x, y) if xi == 1.0 and yi == 1.0
                )
                confidence = (
                    (joint_fail_count / feat_count) if feat_count > 0 else 0.0
                )

                rule_text = self._format_rule_description(feat_name, r_corr, confidence)
                heuristics.append(
                    CrossCuttingHeuristic(
                        rule=rule_text,
                        confidence=confidence,
                        supporting_evidence_count=joint_fail_count if r_corr > 0 else feat_count,
                        feature_correlations={
                            "pearson_r": round(r_corr, 4),
                            "feature_prevalence": round(feat_count / n_samples, 4),
                            "joint_failures": joint_fail_count,
                            "sample_size": n_samples,
                        },
                    )
                )

        # Sort heuristics by absolute correlation strength descending
        heuristics.sort(
            key=lambda h: abs(h.feature_correlations.get("pearson_r", 0.0)),
            reverse=True,
        )
        return heuristics

    @staticmethod
    def _is_failure(status: str) -> bool:
        """Determine whether status string denotes an unrecovered terminal failure."""
        return status.strip().upper() in (
            "FAILED",
            "FAILURE",
            "ERROR",
            "TIMEOUT",
            "CANCELLED",
            "ABORTED",
            "CIRCUIT_BROKEN",
        )

    @staticmethod
    def _format_rule_description(feature: str, r: float, conf: float) -> str:
        """Formulate human-readable planner directive from feature correlation."""
        if feature == "files_touched > 3":
            return (
                f"Modifying >3 files strongly predicts failure (r={r:+.2f}, fail_prob={conf:.1%}). "
                "Decompose milestones into atomic single-component units."
            )
        elif feature == "retry_count > 0":
            return (
                f"Milestones requiring retries have high terminal failure persistence (r={r:+.2f}). "
                "Escalate to fallback planner upon first retry."
            )
        elif feature == "dependency_wait > 30s":
            return (
                f"Excessive dependency wait (>30s) correlates with execution deadlock (r={r:+.2f}). "
                "Validate dependency graph topologically before dispatch."
            )
        elif feature.startswith("error:"):
            err_name = feature.split(":", 1)[1]
            return (
                f"Recurring error '{err_name}' indicates systemic fragility (r={r:+.2f}). "
                "Apply targeted defensive preconditions."
            )
        return f"Condition '{feature}' exhibits significant correlation with failure (r={r:+.2f})."

    def get_steering_context(self, max_tokens: int = 800) -> str:
        """Retrieve and format distilled execution memory as markdown capped at max_tokens."""
        with self._lock:
            if self._cached_distilled_memory is None:
                self._load_cached_distillation()

            if self._cached_distilled_memory is None:
                distilled = self._distill_locked(force_all=True)
            else:
                distilled = self._cached_distilled_memory

        return distilled.to_markdown(max_tokens=max_tokens)


class SteeringMemoryEngine:
    """Closes the feedback loop between milestone outcomes and the strategic planner."""

    def __init__(
        self,
        memory_store: MemoryStore,
        event_bus: Optional[Any] = None,
    ) -> None:
        """Initialize steering engine and attach listener to event bus if present."""
        self.memory_store = memory_store
        self.event_bus = event_bus

        if self.event_bus is not None:
            self._register_event_bus_listener()

    def _register_event_bus_listener(self) -> None:
        """Duck-typed registration on TransitionEventBus for lifecycle events."""
        for hook_name in ("register_listener", "register", "subscribe", "add_listener"):
            hook = getattr(self.event_bus, hook_name, None)
            if callable(hook):
                hook(self._on_terminal_transition)
                logger.info(
                    "SteeringMemoryEngine registered transition listener via %s",
                    hook_name,
                )
                return
        logger.warning(
            "Provided event_bus does not support standard listener registration methods."
        )

    def _on_terminal_transition(self, event: Union[dict, Any]) -> None:
        """Extract milestone metadata and telemetry from lifecycle transition events."""
        try:

            def _get(key: str, default: Any = None) -> Any:
                if isinstance(event, dict):
                    return event.get(key, default)
                return getattr(event, key, default)

            status = str(_get("to_state", _get("status", _get("state", "")))).strip()
            if not self._is_terminal_state(status):
                return

            now = datetime.now(timezone.utc)
            planned_at = self._parse_datetime(_get("planned_at"), default=now)
            started_at = self._parse_datetime(_get("started_at"), default=now)
            completed_at = self._parse_datetime(_get("completed_at"), default=now)

            record = OutcomeRecord(
                milestone_id=str(_get("milestone_id", "unknown_milestone")),
                repo=str(_get("repo", "unknown_repo")),
                status=status,
                planned_at=planned_at,
                started_at=started_at,
                completed_at=completed_at,
                estimated_complexity=float(_get("estimated_complexity", 1.0)),
                actual_token_burn=int(_get("actual_token_burn", 0)),
                budget_token_limit=int(_get("budget_token_limit", 100000)),
                retry_count=int(_get("retry_count", 0)),
                error_classifications=list(_get("error_classifications", [])),
                dependency_wait_seconds=float(_get("dependency_wait_seconds", 0.0)),
                files_touched=int(_get("files_touched", 0)),
                anomaly_signals=list(_get("anomaly_signals", [])),
            )

            self.memory_store.append_outcome(record)
            logger.debug(
                "Appended terminal transition record for %s [%s]",
                record.milestone_id,
                record.status,
            )
        except Exception as exc:
            logger.error(
                "Error processing lifecycle event in SteeringMemoryEngine: %s", exc
            )

    @staticmethod
    def _is_terminal_state(state: str) -> bool:
        """Determine if a lifecycle state is terminal."""
        terminal_states = {
            "COMPLETED",
            "FAILED",
            "CANCELLED",
            "TIMEOUT",
            "ABORTED",
            "SUCCESS",
            "ERROR",
            "CIRCUIT_BROKEN",
        }
        return state.upper() in terminal_states

    @staticmethod
    def _parse_datetime(val: Any, default: datetime) -> datetime:
        """Parse datetime from str or return default."""
        if isinstance(val, datetime):
            return _normalize_dt(val)
        if isinstance(val, str):
            try:
                return _normalize_dt(datetime.fromisoformat(val))
            except Exception:
                pass
        return default

    def get_planner_injection(self, max_tokens: int = 800) -> str:
        """Return formatted steering memory block for injection into planner system prompts."""
        context = self.memory_store.get_steering_context(max_tokens=max_tokens)
        return f"## Execution Steering Memory\n{context}"


if __name__ == "__main__":
    import shutil
    import tempfile

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    print("=" * 80)
    print("SteeringMemoryEngine: Verification & Demonstration")
    print("=" * 80)

    # 1. Setup isolated temporary directory for MemoryStore
    temp_dir = Path(tempfile.mkdtemp(prefix="steering_engine_demo_"))
    storage_file = temp_dir / "steering_memory.jsonl"

    try:
        # Initialize MemoryStore with tight thresholds for immediate verification
        store = MemoryStore(
            storage_path=storage_file,
            distill_interval_hours=1,
            distill_outcome_threshold=10,
        )
        engine = SteeringMemoryEngine(memory_store=store)

        base_time = datetime.now(timezone.utc) - timedelta(days=5)

        # 2. Synthesize 15 outcome records across 3 repositories with distinct failure patterns
        # Repo A: autonomous_orchestrator (Reliable, within budget)
        # Repo B: quota_arbitrage_engine (Token intensive, occasional rate-limiting)
        # Repo C: campus_network_routing (Prone to network timeout, high retry, multi-file)
        synthetic_records: List[OutcomeRecord] = [
            # autonomous_orchestrator (5 records: 5 COMPLETED, 0 FAILED)
            OutcomeRecord(
                milestone_id="orch-001",
                repo="autonomous_orchestrator",
                status="COMPLETED",
                planned_at=base_time + timedelta(hours=1),
                started_at=base_time + timedelta(hours=1, minutes=2),
                completed_at=base_time + timedelta(hours=1, minutes=10),
                estimated_complexity=600.0,
                actual_token_burn=45000,
                budget_token_limit=50000,
                retry_count=0,
                files_touched=1,
            ),
            OutcomeRecord(
                milestone_id="orch-002",
                repo="autonomous_orchestrator",
                status="COMPLETED",
                planned_at=base_time + timedelta(hours=5),
                started_at=base_time + timedelta(hours=5, minutes=1),
                completed_at=base_time + timedelta(hours=5, minutes=8),
                estimated_complexity=500.0,
                actual_token_burn=42000,
                budget_token_limit=50000,
                retry_count=0,
                files_touched=2,
            ),
            OutcomeRecord(
                milestone_id="orch-003",
                repo="autonomous_orchestrator",
                status="COMPLETED",
                planned_at=base_time + timedelta(hours=9),
                started_at=base_time + timedelta(hours=9, minutes=3),
                completed_at=base_time + timedelta(hours=9, minutes=12),
                estimated_complexity=600.0,
                actual_token_burn=48000,
                budget_token_limit=50000,
                retry_count=0,
                files_touched=1,
            ),
            OutcomeRecord(
                milestone_id="orch-004",
                repo="autonomous_orchestrator",
                status="COMPLETED",
                planned_at=base_time + timedelta(hours=14),
                started_at=base_time + timedelta(hours=14, minutes=1),
                completed_at=base_time + timedelta(hours=14, minutes=9),
                estimated_complexity=550.0,
                actual_token_burn=44000,
                budget_token_limit=50000,
                retry_count=0,
                files_touched=2,
            ),
            OutcomeRecord(
                milestone_id="orch-005",
                repo="autonomous_orchestrator",
                status="COMPLETED",
                planned_at=base_time + timedelta(hours=18),
                started_at=base_time + timedelta(hours=18, minutes=2),
                completed_at=base_time + timedelta(hours=18, minutes=11),
                estimated_complexity=600.0,
                actual_token_burn=46000,
                budget_token_limit=50000,
                retry_count=0,
                files_touched=1,
            ),
            # quota_arbitrage_engine (5 records: 3 COMPLETED, 2 FAILED due to RateLimitError)
            OutcomeRecord(
                milestone_id="quota-101",
                repo="quota_arbitrage_engine",
                status="COMPLETED",
                planned_at=base_time + timedelta(hours=22),
                started_at=base_time + timedelta(hours=22, minutes=5),
                completed_at=base_time + timedelta(hours=22, minutes=45),
                estimated_complexity=1200.0,
                actual_token_burn=180000,
                budget_token_limit=80000,
                retry_count=1,
                files_touched=3,
            ),
            OutcomeRecord(
                milestone_id="quota-102",
                repo="quota_arbitrage_engine",
                status="FAILED",
                planned_at=base_time + timedelta(hours=26),
                started_at=base_time + timedelta(hours=26, minutes=2),
                completed_at=base_time + timedelta(hours=26, minutes=50),
                estimated_complexity=1200.0,
                actual_token_burn=210000,
                budget_token_limit=80000,
                retry_count=2,
                error_classifications=["RateLimitError"],
                files_touched=4,
            ),
            OutcomeRecord(
                milestone_id="quota-103",
                repo="quota_arbitrage_engine",
                status="COMPLETED",
                planned_at=base_time + timedelta(hours=30),
                started_at=base_time + timedelta(hours=30, minutes=4),
                completed_at=base_time + timedelta(hours=30, minutes=38),
                estimated_complexity=1200.0,
                actual_token_burn=175000,
                budget_token_limit=80000,
                retry_count=1,
                files_touched=3,
            ),
            OutcomeRecord(
                milestone_id="quota-104",
                repo="quota_arbitrage_engine",
                status="FAILED",
                planned_at=base_time + timedelta(hours=34),
                started_at=base_time + timedelta(hours=34, minutes=1),
                completed_at=base_time + timedelta(hours=34, minutes=55),
                estimated_complexity=1200.0,
                actual_token_burn=225000,
                budget_token_limit=80000,
                retry_count=3,
                error_classifications=["RateLimitError"],
                files_touched=5,
            ),
            OutcomeRecord(
                milestone_id="quota-105",
                repo="quota_arbitrage_engine",
                status="COMPLETED",
                planned_at=base_time + timedelta(hours=38),
                started_at=base_time + timedelta(hours=38, minutes=5),
                completed_at=base_time + timedelta(hours=38, minutes=40),
                estimated_complexity=1200.0,
                actual_token_burn=190000,
                budget_token_limit=80000,
                retry_count=1,
                files_touched=3,
            ),
            # campus_network_routing (5 records: 1 COMPLETED, 4 FAILED with network timeouts)
            OutcomeRecord(
                milestone_id="net-201",
                repo="campus_network_routing",
                status="FAILED",
                planned_at=base_time + timedelta(hours=42),
                started_at=base_time + timedelta(hours=42, minutes=2),
                completed_at=base_time + timedelta(hours=42, minutes=25),
                estimated_complexity=800.0,
                actual_token_burn=60000,
                budget_token_limit=60000,
                retry_count=3,
                error_classifications=["PortalAuthTimeout", "CaptiveDNSFailure"],
                dependency_wait_seconds=45.0,
                files_touched=5,
            ),
            OutcomeRecord(
                milestone_id="net-202",
                repo="campus_network_routing",
                status="FAILED",
                planned_at=base_time + timedelta(hours=46),
                started_at=base_time + timedelta(hours=46, minutes=1),
                completed_at=base_time + timedelta(hours=46, minutes=30),
                estimated_complexity=800.0,
                actual_token_burn=65000,
                budget_token_limit=60000,
                retry_count=2,
                error_classifications=["CaptiveDNSFailure"],
                dependency_wait_seconds=50.0,
                files_touched=4,
            ),
            OutcomeRecord(
                milestone_id="net-203",
                repo="campus_network_routing",
                status="COMPLETED",
                planned_at=base_time + timedelta(hours=50),
                started_at=base_time + timedelta(hours=50, minutes=3),
                completed_at=base_time + timedelta(hours=50, minutes=15),
                estimated_complexity=800.0,
                actual_token_burn=55000,
                budget_token_limit=60000,
                retry_count=0,
                dependency_wait_seconds=5.0,
                files_touched=1,
            ),
            OutcomeRecord(
                milestone_id="net-204",
                repo="campus_network_routing",
                status="FAILED",
                planned_at=base_time + timedelta(hours=54),
                started_at=base_time + timedelta(hours=54, minutes=2),
                completed_at=base_time + timedelta(hours=54, minutes=28),
                estimated_complexity=800.0,
                actual_token_burn=70000,
                budget_token_limit=60000,
                retry_count=3,
                error_classifications=["PortalAuthTimeout"],
                dependency_wait_seconds=60.0,
                files_touched=5,
            ),
            OutcomeRecord(
                milestone_id="net-205",
                repo="campus_network_routing",
                status="FAILED",
                planned_at=base_time + timedelta(hours=58),
                started_at=base_time + timedelta(hours=58, minutes=1),
                completed_at=base_time + timedelta(hours=58, minutes=35),
                estimated_complexity=800.0,
                actual_token_burn=62000,
                budget_token_limit=60000,
                retry_count=2,
                error_classifications=["CaptiveDNSFailure"],
                dependency_wait_seconds=55.0,
                files_touched=4,
            ),
        ]

        print(f"[*] Appending {len(synthetic_records)} outcome records to MemoryStore...")
        for r in synthetic_records:
            store.append_outcome(r)

        print("[*] Triggering outcome distillation...")
        distilled = store.distill()

        print(f"[*] Distillation complete.")
        print(f"    - Total processed: {distilled.total_outcomes_processed}")
        print(f"    - Repos profiled: {len(distilled.repo_profiles)}")
        print(f"    - Heuristics mined: {len(distilled.heuristics)}")

        print("\n" + "=" * 80)
        print("GENERATED PLANNER INJECTION CONTEXT (max_tokens=800):")
        print("=" * 80)
        injection_text = engine.get_planner_injection(max_tokens=800)
        print(injection_text)
        print("=" * 80)

        # 3. Verify event bus mock integration
        class MockEventBus:
            def __init__(self):
                self.listener = None

            def register_listener(self, fn):
                self.listener = fn

            def emit(self, evt):
                if self.listener:
                    self.listener(evt)

        mock_bus = MockEventBus()
        bus_engine = SteeringMemoryEngine(memory_store=store, event_bus=mock_bus)
        mock_bus.emit(
            {
                "milestone_id": "test-live-01",
                "repo": "autonomous_orchestrator",
                "to_state": "COMPLETED",
                "planned_at": datetime.now(timezone.utc).isoformat(),
                "started_at": datetime.now(timezone.utc).isoformat(),
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "actual_token_burn": 35000,
                "budget_token_limit": 50000,
                "estimated_complexity": 300.0,
                "retry_count": 0,
                "files_touched": 1,
            }
        )
        print("[*] Successfully verified live TransitionEventBus listener integration.")

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
        print("[*] Cleanup complete.")