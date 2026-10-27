"""
Pipeline Directive Governance & Safe Configuration Application Controller.

This module provides an enterprise-grade governance gatekeeper for autonomous
pipeline parameter tuning. It enforces safety invariants, manages canary deployments,
evaluates promotion criteria based on statistical telemetry, and provides atomic
rollback guarantees to prevent oscillatory or catastrophic configuration drift.
"""

from __future__ import annotations

import collections
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
import logging
import math
from pathlib import Path
from typing import Any, Optional, Union
import uuid

# Configure module-level logger
logger = logging.getLogger("hermes.pipeline_directive_governor")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# =====================================================================
# 1. Parameter Types and Configuration Snapshot
# =====================================================================

@dataclass(frozen=True)
class HWTriple:
    """Holt-Winters triple for level (alpha), trend (beta), and seasonality (gamma)."""
    alpha: float
    beta: float
    gamma: float

    def to_dict(self) -> dict[str, float]:
        return {"alpha": self.alpha, "beta": self.beta, "gamma": self.gamma}

    @classmethod
    def from_dict(cls, data: Union[dict[str, Any], list, tuple]) -> HWTriple:
        if isinstance(data, (list, tuple)):
            return cls(alpha=float(data[0]), beta=float(data[1]), gamma=float(data[2]))
        return cls(
            alpha=float(data["alpha"]),
            beta=float(data["beta"]),
            gamma=float(data["gamma"]),
        )


@dataclass(frozen=True)
class CBParams:
    """Circuit breaker configuration parameters."""
    failure_threshold: int
    cooldown_seconds: float
    half_open_max: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "failure_threshold": self.failure_threshold,
            "cooldown_seconds": self.cooldown_seconds,
            "half_open_max": self.half_open_max,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CBParams:
        return cls(
            failure_threshold=int(data["failure_threshold"]),
            cooldown_seconds=float(data["cooldown_seconds"]),
            half_open_max=int(data["half_open_max"]),
        )


class PacingMode(str, Enum):
    """Pacing mode for autonomous pipeline scheduling."""
    AGGRESSIVE = "aggressive"
    MODERATE = "moderate"
    CONSERVATIVE = "conservative"


@dataclass(frozen=True)
class SchedulingPolicy:
    """Scheduling policy assigned to repository clusters."""
    pacing_mode: PacingMode
    model_tier_preference: tuple[str, ...]
    buffer_priority_weight: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "pacing_mode": (
                self.pacing_mode.value
                if isinstance(self.pacing_mode, PacingMode)
                else str(self.pacing_mode)
            ),
            "model_tier_preference": list(self.model_tier_preference),
            "buffer_priority_weight": self.buffer_priority_weight,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SchedulingPolicy:
        pm = data["pacing_mode"]
        if isinstance(pm, str):
            pm = PacingMode(pm.lower())
        return cls(
            pacing_mode=pm,
            model_tier_preference=tuple(data.get("model_tier_preference", [])),
            buffer_priority_weight=float(data.get("buffer_priority_weight", 1.0)),
        )


@dataclass(frozen=True, eq=False)
class ConfigurationSnapshot:
    """
    Frozen dataclass capturing the comprehensive parameter state of the pipeline.
    Each snapshot is assigned a monotonic version_id and canonical SHA-256 hash.
    """
    version_id: int
    content_hash: str
    timestamp: str
    buffer_depth: int
    max_concurrency: int
    ewma_alpha: dict[str, float]
    holt_winters_params: dict[str, HWTriple]
    conformance_thresholds: dict[str, float]
    circuit_breaker_config: dict[str, CBParams]
    model_tier_ordering: dict[str, list[str]]
    cluster_policies: dict[str, SchedulingPolicy]

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ConfigurationSnapshot):
            return False
        return self.content_hash == other.content_hash

    def __hash__(self) -> int:
        return hash(self.content_hash)

    @staticmethod
    def compute_content_hash(
        buffer_depth: int,
        max_concurrency: int,
        ewma_alpha: dict[str, float],
        holt_winters_params: dict[str, Any],
        conformance_thresholds: dict[str, float],
        circuit_breaker_config: dict[str, Any],
        model_tier_ordering: dict[str, list[str]],
        cluster_policies: dict[str, Any],
    ) -> str:
        """Computes a deterministic SHA-256 hash over canonical JSON representation."""
        payload = {
            "buffer_depth": buffer_depth,
            "max_concurrency": max_concurrency,
            "ewma_alpha": {k: round(float(v), 6) for k, v in sorted(ewma_alpha.items())},
            "holt_winters_params": {
                k: (v.to_dict() if hasattr(v, "to_dict") else dict(v))
                for k, v in sorted(holt_winters_params.items())
            },
            "conformance_thresholds": {
                k: round(float(v), 6) for k, v in sorted(conformance_thresholds.items())
            },
            "circuit_breaker_config": {
                k: (v.to_dict() if hasattr(v, "to_dict") else dict(v))
                for k, v in sorted(circuit_breaker_config.items())
            },
            "model_tier_ordering": {k: list(v) for k, v in sorted(model_tier_ordering.items())},
            "cluster_policies": {
                k: (v.to_dict() if hasattr(v, "to_dict") else dict(v))
                for k, v in sorted(cluster_policies.items())
            },
        }
        canonical_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()

    @classmethod
    def create(
        cls,
        version_id: int,
        buffer_depth: int,
        max_concurrency: int,
        ewma_alpha: dict[str, float],
        holt_winters_params: dict[str, Any],
        conformance_thresholds: dict[str, float],
        circuit_breaker_config: dict[str, Any],
        model_tier_ordering: dict[str, list[str]],
        cluster_policies: dict[str, Any],
        timestamp: Optional[str] = None,
    ) -> ConfigurationSnapshot:
        """Factory method to construct a validated, hashed ConfigurationSnapshot."""
        ts = timestamp or datetime.now(timezone.utc).isoformat()
        parsed_hw = {
            k: (v if isinstance(v, HWTriple) else HWTriple.from_dict(v))
            for k, v in holt_winters_params.items()
        }
        parsed_cb = {
            k: (v if isinstance(v, CBParams) else CBParams.from_dict(v))
            for k, v in circuit_breaker_config.items()
        }
        parsed_cluster = {
            k: (v if isinstance(v, SchedulingPolicy) else SchedulingPolicy.from_dict(v))
            for k, v in cluster_policies.items()
        }
        content_hash = cls.compute_content_hash(
            buffer_depth=buffer_depth,
            max_concurrency=max_concurrency,
            ewma_alpha=ewma_alpha,
            holt_winters_params=parsed_hw,
            conformance_thresholds=conformance_thresholds,
            circuit_breaker_config=parsed_cb,
            model_tier_ordering=model_tier_ordering,
            cluster_policies=parsed_cluster,
        )
        return cls(
            version_id=version_id,
            content_hash=content_hash,
            timestamp=ts,
            buffer_depth=buffer_depth,
            max_concurrency=max_concurrency,
            ewma_alpha=ewma_alpha,
            holt_winters_params=parsed_hw,
            conformance_thresholds=conformance_thresholds,
            circuit_breaker_config=parsed_cb,
            model_tier_ordering=model_tier_ordering,
            cluster_policies=parsed_cluster,
        )

    def apply_changes(
        self,
        new_version_id: int,
        changes: dict[str, Any],
        timestamp: Optional[str] = None,
    ) -> ConfigurationSnapshot:
        """Derives a new ConfigurationSnapshot by overlaying verified changes."""
        ts = timestamp or datetime.now(timezone.utc).isoformat()
        new_buffer_depth = int(changes.get("buffer_depth", self.buffer_depth))
        new_max_concurrency = int(changes.get("max_concurrency", self.max_concurrency))

        new_ewma_alpha = dict(self.ewma_alpha)
        if "ewma_alpha" in changes:
            new_ewma_alpha.update(changes["ewma_alpha"])

        new_hw = dict(self.holt_winters_params)
        if "holt_winters_params" in changes:
            for k, v in changes["holt_winters_params"].items():
                new_hw[k] = v if isinstance(v, HWTriple) else HWTriple.from_dict(v)

        new_conf = dict(self.conformance_thresholds)
        if "conformance_thresholds" in changes:
            new_conf.update(changes["conformance_thresholds"])

        new_cb = dict(self.circuit_breaker_config)
        if "circuit_breaker_config" in changes:
            for k, v in changes["circuit_breaker_config"].items():
                new_cb[k] = v if isinstance(v, CBParams) else CBParams.from_dict(v)

        new_tiers = {k: list(v) for k, v in self.model_tier_ordering.items()}
        if "model_tier_ordering" in changes:
            for k, v in changes["model_tier_ordering"].items():
                new_tiers[k] = list(v)

        new_clusters = dict(self.cluster_policies)
        if "cluster_policies" in changes:
            for k, v in changes["cluster_policies"].items():
                new_clusters[k] = (
                    v if isinstance(v, SchedulingPolicy) else SchedulingPolicy.from_dict(v)
                )

        content_hash = self.compute_content_hash(
            buffer_depth=new_buffer_depth,
            max_concurrency=new_max_concurrency,
            ewma_alpha=new_ewma_alpha,
            holt_winters_params=new_hw,
            conformance_thresholds=new_conf,
            circuit_breaker_config=new_cb,
            model_tier_ordering=new_tiers,
            cluster_policies=new_clusters,
        )

        return ConfigurationSnapshot(
            version_id=new_version_id,
            content_hash=content_hash,
            timestamp=ts,
            buffer_depth=new_buffer_depth,
            max_concurrency=new_max_concurrency,
            ewma_alpha=new_ewma_alpha,
            holt_winters_params=new_hw,
            conformance_thresholds=new_conf,
            circuit_breaker_config=new_cb,
            model_tier_ordering=new_tiers,
            cluster_policies=new_clusters,
        )

    def to_dict(self) -> dict[str, Any]:
        """Serializes snapshot to JSON-compatible dictionary."""
        return {
            "version_id": self.version_id,
            "content_hash": self.content_hash,
            "timestamp": self.timestamp,
            "buffer_depth": self.buffer_depth,
            "max_concurrency": self.max_concurrency,
            "ewma_alpha": self.ewma_alpha,
            "holt_winters_params": {
                k: v.to_dict() for k, v in self.holt_winters_params.items()
            },
            "conformance_thresholds": self.conformance_thresholds,
            "circuit_breaker_config": {
                k: v.to_dict() for k, v in self.circuit_breaker_config.items()
            },
            "model_tier_ordering": self.model_tier_ordering,
            "cluster_policies": {
                k: v.to_dict() for k, v in self.cluster_policies.items()
            },
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ConfigurationSnapshot:
        """Reconstructs ConfigurationSnapshot from dictionary."""
        hw = {
            k: HWTriple.from_dict(v)
            for k, v in data.get("holt_winters_params", {}).items()
        }
        cb = {
            k: CBParams.from_dict(v)
            for k, v in data.get("circuit_breaker_config", {}).items()
        }
        policies = {
            k: SchedulingPolicy.from_dict(v)
            for k, v in data.get("cluster_policies", {}).items()
        }
        return cls(
            version_id=int(data["version_id"]),
            content_hash=str(data["content_hash"]),
            timestamp=str(data["timestamp"]),
            buffer_depth=int(data["buffer_depth"]),
            max_concurrency=int(data["max_concurrency"]),
            ewma_alpha={k: float(v) for k, v in data.get("ewma_alpha", {}).items()},
            holt_winters_params=hw,
            conformance_thresholds={
                k: float(v) for k, v in data.get("conformance_thresholds", {}).items()
            },
            circuit_breaker_config=cb,
            model_tier_ordering={
                k: list(v) for k, v in data.get("model_tier_ordering", {}).items()
            },
            cluster_policies=policies,
        )

    def save_to_jsonl(self, filepath: Union[str, Path] = "data/config_snapshots.jsonl") -> None:
        """Appends the snapshot to append-only JSONL storage if not already present."""
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        # Avoid duplicate appending of exact same hash
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        try:
                            record = json.loads(line)
                            if record.get("content_hash") == self.content_hash:
                                return
                        except json.JSONDecodeError:
                            continue

        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(self.to_dict()) + "\n")
        logger.info(
            "Saved ConfigurationSnapshot v%s (%s) to %s",
            self.version_id,
            self.content_hash[:8],
            path,
        )

    @classmethod
    def load_all_from_jsonl(
        cls, filepath: Union[str, Path] = "data/config_snapshots.jsonl"
    ) -> list[ConfigurationSnapshot]:
        """Loads full snapshot lineage from JSONL file."""
        path = Path(filepath)
        if not path.exists():
            return []
        snapshots = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    snapshots.append(cls.from_dict(json.loads(line)))
        return snapshots


# =====================================================================
# 2. Safety Invariant Registry and Directive Validator
# =====================================================================

@dataclass
class ValidatedDirective:
    """Outcome of parameter validation with field-level annotations."""
    directive_id: str
    source: str
    accepted_changes: dict[str, Any]
    clamped_changes: dict[str, dict[str, Any]]
    rejected_changes: dict[str, str]
    is_applicable: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "directive_id": self.directive_id,
            "source": self.source,
            "accepted_changes": self.accepted_changes,
            "clamped_changes": self.clamped_changes,
            "rejected_changes": self.rejected_changes,
            "is_applicable": self.is_applicable,
        }


class SafetyInvariantRegistry:
    """
    Authoritative registry of immutable safety bounds and oscillation guards.
    """
    BUFFER_DEPTH_RANGE: tuple[int, int] = (2, 8)
    EWMA_ALPHA_RANGE: tuple[float, float] = (0.01, 0.99)
    CB_COOLDOWN_RANGE: tuple[float, float] = (30.0, 600.0)
    MAX_CONCURRENCY_RANGE: tuple[int, int] = (1, 8)
    MAX_PARAM_CHANGE_RATE: float = 0.40  # 40% maximum shift per cycle

    # Whitelist of approved model identifiers
    PROVIDER_REGISTRY: set[str] = {
        "gemini-3.8-flash-tiered",
        "gemini-3.8-flash-high",
        "gemini-3.8-pro",
        "claude-opus-4-6-thinking",
        "claude-sonnet-4-6",
        "claude-haiku-4-5",
        "gpt-4o",
        "o1",
        "o3-mini",
        "local-qwen-coder",
    }

    @classmethod
    def validate_directive(
        cls,
        current_snapshot: ConfigurationSnapshot,
        proposed_changes: dict[str, Any],
        known_repo_ids: Optional[list[str]] = None,
        directive_id: Optional[str] = None,
        source: str = "meta_optimizer",
    ) -> ValidatedDirective:
        """Direct delegation to DirectiveValidator logic."""
        validator = DirectiveValidator(registry=cls)
        return validator.validate_directive(
            current_snapshot=current_snapshot,
            proposed_changes=proposed_changes,
            known_repo_ids=known_repo_ids,
            directive_id=directive_id,
            source=source,
        )


class DirectiveValidator:
    """
    Validates every parameter change proposal against safety invariants,
    rate-of-change constraints, and structural integrity.
    """

    def __init__(self, registry: type[SafetyInvariantRegistry] = SafetyInvariantRegistry):
        self.registry = registry

    def validate_directive(
        self,
        current_snapshot: ConfigurationSnapshot,
        proposed_changes: dict[str, Any],
        known_repo_ids: Optional[list[str]] = None,
        directive_id: Optional[str] = None,
        source: str = "meta_optimizer",
    ) -> ValidatedDirective:
        did = directive_id or f"dir_{uuid.uuid4().hex[:8]}"
        accepted: dict[str, Any] = {}
        clamped: dict[str, dict[str, Any]] = {}
        rejected: dict[str, str] = {}

        # 1. buffer_depth validation
        if "buffer_depth" in proposed_changes:
            raw_val = proposed_changes["buffer_depth"]
            try:
                val = int(raw_val)
                min_b, max_b = self.registry.BUFFER_DEPTH_RANGE
                cur_val = current_snapshot.buffer_depth
                clamped_val = max(min_b, min(max_b, val))
                was_clamped = clamped_val != val

                # Check rate of change
                rel_delta = abs(clamped_val - cur_val) / max(cur_val, 1)
                if rel_delta > self.registry.MAX_PARAM_CHANGE_RATE:
                    rejected["buffer_depth"] = (
                        f"Shift {rel_delta:.1%} exceeds rate-of-change limit 40% "
                        f"(current: {cur_val}, proposed: {clamped_val})"
                    )
                else:
                    if was_clamped:
                        clamped["buffer_depth"] = {
                            "original": val,
                            "clamped": clamped_val,
                            "reason": f"Clamped to hard range [{min_b}, {max_b}]",
                        }
                    accepted["buffer_depth"] = clamped_val
            except (ValueError, TypeError) as e:
                rejected["buffer_depth"] = f"Invalid buffer_depth integer value: {e}"

        # 2. max_concurrency validation
        if "max_concurrency" in proposed_changes:
            raw_val = proposed_changes["max_concurrency"]
            try:
                val = int(raw_val)
                min_c, max_c = self.registry.MAX_CONCURRENCY_RANGE
                cur_val = current_snapshot.max_concurrency
                clamped_val = max(min_c, min(max_c, val))
                was_clamped = clamped_val != val

                rel_delta = abs(clamped_val - cur_val) / max(cur_val, 1)
                if rel_delta > self.registry.MAX_PARAM_CHANGE_RATE:
                    rejected["max_concurrency"] = (
                        f"Shift {rel_delta:.1%} exceeds rate-of-change limit 40% "
                        f"(current: {cur_val}, proposed: {clamped_val})"
                    )
                else:
                    if was_clamped:
                        clamped["max_concurrency"] = {
                            "original": val,
                            "clamped": clamped_val,
                            "reason": f"Clamped to hard range [{min_c}, {max_c}]",
                        }
                    accepted["max_concurrency"] = clamped_val
            except (ValueError, TypeError) as e:
                rejected["max_concurrency"] = f"Invalid max_concurrency integer value: {e}"

        # 3. ewma_alpha validation (dict per repo)
        if "ewma_alpha" in proposed_changes:
            ewma_dict = proposed_changes["ewma_alpha"]
            if not isinstance(ewma_dict, dict):
                rejected["ewma_alpha"] = "ewma_alpha must be a mapping of repo_id -> float"
            else:
                accepted_ewma = {}
                min_a, max_a = self.registry.EWMA_ALPHA_RANGE
                for repo_id, raw_alpha in ewma_dict.items():
                    if not isinstance(repo_id, str) or not repo_id.strip():
                        rejected[f"ewma_alpha.{repo_id}"] = "repo_id must be non-empty string"
                        continue
                    try:
                        alpha = float(raw_alpha)
                        clamped_alpha = max(min_a, min(max_a, alpha))
                        was_clamped = abs(clamped_alpha - alpha) > 1e-6
                        cur_alpha = current_snapshot.ewma_alpha.get(repo_id)

                        if cur_alpha is not None:
                            rel_delta = abs(clamped_alpha - cur_alpha) / max(cur_alpha, 1e-4)
                            if rel_delta > self.registry.MAX_PARAM_CHANGE_RATE:
                                rejected[f"ewma_alpha.{repo_id}"] = (
                                    f"Shift {rel_delta:.1%} exceeds rate-of-change limit 40% "
                                    f"(current: {cur_alpha:.3f}, proposed: {clamped_alpha:.3f})"
                                )
                                continue

                        if was_clamped:
                            clamped[f"ewma_alpha.{repo_id}"] = {
                                "original": alpha,
                                "clamped": clamped_alpha,
                                "reason": f"Clamped to hard range ({min_a}, {max_a})",
                            }
                        accepted_ewma[repo_id] = clamped_alpha
                    except (ValueError, TypeError) as e:
                        rejected[f"ewma_alpha.{repo_id}"] = f"Invalid alpha float value: {e}"
                if accepted_ewma:
                    accepted["ewma_alpha"] = accepted_ewma

        # 4. circuit_breaker_config validation
        if "circuit_breaker_config" in proposed_changes:
            cb_dict = proposed_changes["circuit_breaker_config"]
            if not isinstance(cb_dict, dict):
                rejected["circuit_breaker_config"] = "circuit_breaker_config must be a dictionary"
            else:
                accepted_cb = {}
                min_cd, max_cd = self.registry.CB_COOLDOWN_RANGE
                for key, params in cb_dict.items():
                    if not isinstance(key, str) or not key.strip():
                        rejected[f"circuit_breaker_config.{key}"] = "key must be non-empty string"
                        continue
                    try:
                        p_dict = params.to_dict() if hasattr(params, "to_dict") else dict(params)
                        cooldown = float(p_dict.get("cooldown_seconds", 60.0))
                        fail_th = int(p_dict.get("failure_threshold", 5))
                        ho_max = int(p_dict.get("half_open_max", 2))

                        clamped_cd = max(min_cd, min(max_cd, cooldown))
                        was_clamped = abs(clamped_cd - cooldown) > 1e-6

                        cur_cb = current_snapshot.circuit_breaker_config.get(key)
                        if cur_cb is not None:
                            cur_cd = cur_cb.cooldown_seconds
                            rel_delta = abs(clamped_cd - cur_cd) / max(cur_cd, 1.0)
                            if rel_delta > self.registry.MAX_PARAM_CHANGE_RATE:
                                rejected[f"circuit_breaker_config.{key}.cooldown"] = (
                                    f"Shift {rel_delta:.1%} exceeds rate limit 40% "
                                    f"(current: {cur_cd}s, proposed: {clamped_cd}s)"
                                )
                                continue

                        if was_clamped:
                            clamped[f"circuit_breaker_config.{key}.cooldown_seconds"] = {
                                "original": cooldown,
                                "clamped": clamped_cd,
                                "reason": f"Clamped to hard range [{min_cd}, {max_cd}]s",
                            }
                        accepted_cb[key] = CBParams(
                            failure_threshold=max(1, fail_th),
                            cooldown_seconds=clamped_cd,
                            half_open_max=max(1, ho_max),
                        )
                    except Exception as e:
                        rejected[f"circuit_breaker_config.{key}"] = f"Malformed CBParams: {e}"
                if accepted_cb:
                    accepted["circuit_breaker_config"] = accepted_cb

        # 5. model_tier_ordering validation (structural whitelist check)
        if "model_tier_ordering" in proposed_changes:
            mto = proposed_changes["model_tier_ordering"]
            if not isinstance(mto, dict):
                rejected["model_tier_ordering"] = "model_tier_ordering must be a dict"
            else:
                accepted_mto = {}
                for repo_id, models in mto.items():
                    if not isinstance(repo_id, str) or not repo_id.strip():
                        rejected[f"model_tier_ordering.{repo_id}"] = "repo_id must be non-empty"
                        continue
                    if not isinstance(models, (list, tuple)) or not models:
                        rejected[f"model_tier_ordering.{repo_id}"] = "model list cannot be empty"
                        continue
                    invalid_models = [m for m in models if m not in self.registry.PROVIDER_REGISTRY]
                    if invalid_models:
                        rejected[f"model_tier_ordering.{repo_id}"] = (
                            f"Models {invalid_models} not recognized in SafetyInvariantRegistry"
                        )
                    else:
                        accepted_mto[repo_id] = list(models)
                if accepted_mto:
                    accepted["model_tier_ordering"] = accepted_mto

        # 6. cluster_policies validation
        if "cluster_policies" in proposed_changes:
            cp = proposed_changes["cluster_policies"]
            if not isinstance(cp, dict):
                rejected["cluster_policies"] = "cluster_policies must be a dictionary"
            else:
                accepted_cp = {}
                for cluster_id, pol in cp.items():
                    if not isinstance(cluster_id, str) or not cluster_id.strip():
                        rejected[f"cluster_policies.{cluster_id}"] = "cluster_id must be non-empty"
                        continue
                    try:
                        pol_obj = (
                            pol
                            if isinstance(pol, SchedulingPolicy)
                            else SchedulingPolicy.from_dict(pol)
                        )
                        # Check models in preference
                        invalid_m = [
                            m
                            for m in pol_obj.model_tier_preference
                            if m not in self.registry.PROVIDER_REGISTRY
                        ]
                        if invalid_m:
                            rejected[f"cluster_policies.{cluster_id}"] = (
                                f"Unapproved models in cluster preference: {invalid_m}"
                            )
                        else:
                            accepted_cp[cluster_id] = pol_obj
                    except Exception as e:
                        rejected[f"cluster_policies.{cluster_id}"] = (
                            f"Malformed SchedulingPolicy: {e}"
                        )
                if accepted_cp:
                    accepted["cluster_policies"] = accepted_cp

        # 7. conformance_thresholds validation
        if "conformance_thresholds" in proposed_changes:
            ct = proposed_changes["conformance_thresholds"]
            if isinstance(ct, dict):
                accepted_ct = {}
                for metric, raw_val in ct.items():
                    try:
                        val = float(raw_val)
                        clamped_val = max(0.01, min(1.0, val))
                        if clamped_val != val:
                            clamped[f"conformance_thresholds.{metric}"] = {
                                "original": val,
                                "clamped": clamped_val,
                                "reason": "Clamped to [0.01, 1.0]",
                            }
                        accepted_ct[metric] = clamped_val
                    except Exception as e:
                        rejected[f"conformance_thresholds.{metric}"] = str(e)
                if accepted_ct:
                    accepted["conformance_thresholds"] = accepted_ct

        # 8. holt_winters_params validation
        if "holt_winters_params" in proposed_changes:
            hwp = proposed_changes["holt_winters_params"]
            if isinstance(hwp, dict):
                accepted_hwp = {}
                for repo_id, triple in hwp.items():
                    try:
                        obj = triple if isinstance(triple, HWTriple) else HWTriple.from_dict(triple)
                        # Ensure bounds (0.001, 0.999)
                        c_alpha = max(0.001, min(0.999, obj.alpha))
                        c_beta = max(0.001, min(0.999, obj.beta))
                        c_gamma = max(0.001, min(0.999, obj.gamma))
                        accepted_hwp[repo_id] = HWTriple(c_alpha, c_beta, c_gamma)
                    except Exception as e:
                        rejected[f"holt_winters_params.{repo_id}"] = str(e)
                if accepted_hwp:
                    accepted["holt_winters_params"] = accepted_hwp

        is_applicable = bool(accepted or clamped)
        return ValidatedDirective(
            directive_id=did,
            source=source,
            accepted_changes=accepted,
            clamped_changes=clamped,
            rejected_changes=rejected,
            is_applicable=is_applicable,
        )


# =====================================================================
# 3. Canary Controller
# =====================================================================

@dataclass
class CanaryState:
    """State of an in-flight canary observation cohort."""
    directive_id: str
    canary_set: list[str]
    baseline_set: list[str]
    experimental_params: dict[str, Any]
    baseline_snapshot_version: int
    window_cycles: int = 2
    current_cycle: int = 0
    is_active: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "directive_id": self.directive_id,
            "canary_set": self.canary_set,
            "baseline_set": self.baseline_set,
            "experimental_params": self.experimental_params,
            "baseline_snapshot_version": self.baseline_snapshot_version,
            "window_cycles": self.window_cycles,
            "current_cycle": self.current_cycle,
            "is_active": self.is_active,
        }


@dataclass
class CanaryMetrics:
    """Paired empirical telemetry metrics collected across canary and baseline sets."""
    canary_metrics: dict[str, float]
    baseline_metrics: dict[str, float]
    sample_counts: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        return {
            "canary_metrics": {k: round(v, 4) for k, v in self.canary_metrics.items()},
            "baseline_metrics": {k: round(v, 4) for k, v in self.baseline_metrics.items()},
            "sample_counts": self.sample_counts,
        }


class CanaryController:
    """
    Partitions active repositories into canary and baseline sets,
    routing experimental directives to high-variance repositories.
    """

    def partition_repositories(
        self, repo_ids: list[str], historical_variance: dict[str, float]
    ) -> tuple[list[str], list[str]]:
        """
        Partitions repos into canary_set (ceil(N * 0.3) highest variance repos)
        and baseline_set (remainder).
        """
        if not repo_ids:
            return [], []
        n = len(repo_ids)
        canary_count = math.ceil(n * 0.3)
        sorted_repos = sorted(
            repo_ids, key=lambda r: historical_variance.get(r, 0.0), reverse=True
        )
        canary_set = sorted_repos[:canary_count]
        baseline_set = sorted_repos[canary_count:]
        logger.info(
            "Partitioned %d repos into Canary (N=%d: %s) and Baseline (N=%d: %s)",
            n,
            len(canary_set),
            canary_set,
            len(baseline_set),
            baseline_set,
        )
        return canary_set, baseline_set

    def apply_to_canary(
        self,
        validated_directive: ValidatedDirective,
        canary_set: list[str],
        baseline_set: list[str],
        current_snapshot: ConfigurationSnapshot,
        window_cycles: int = 2,
    ) -> CanaryState:
        """Initializes canary trial tracking state."""
        # Merge accepted and clamped changes
        combined_params = dict(validated_directive.accepted_changes)
        return CanaryState(
            directive_id=validated_directive.directive_id,
            canary_set=canary_set,
            baseline_set=baseline_set,
            experimental_params=combined_params,
            baseline_snapshot_version=current_snapshot.version_id,
            window_cycles=window_cycles,
            current_cycle=0,
            is_active=True,
        )

    def collect_canary_metrics(
        self,
        canary_set: list[str],
        baseline_set: list[str],
        execution_records: list[dict[str, Any]],
    ) -> CanaryMetrics:
        """Computes paired telemetry vectors across canary and baseline sets."""
        canary_records = [
            r for r in execution_records if r.get("repo_id") in canary_set
        ]
        baseline_records = [
            r for r in execution_records if r.get("repo_id") in baseline_set
        ]

        def _aggregate(records: list[dict[str, Any]]) -> dict[str, float]:
            if not records:
                return {
                    "success_rate": 1.0,
                    "mean_token_cost": 0.0,
                    "mean_latency": 0.0,
                    "conformance_score": 1.0,
                }
            n = len(records)
            success_count = sum(1 for r in records if bool(r.get("success", False)))
            total_tokens = sum(
                float(r.get("token_cost", r.get("tokens", 0.0))) for r in records
            )
            total_latency = sum(
                float(r.get("latency", r.get("duration", 0.0))) for r in records
            )
            total_conformance = sum(
                float(r.get("conformance_score", r.get("conformance", 1.0)))
                for r in records
            )
            return {
                "success_rate": success_count / n,
                "mean_token_cost": total_tokens / n,
                "mean_latency": total_latency / n,
                "conformance_score": total_conformance / n,
            }

        return CanaryMetrics(
            canary_metrics=_aggregate(canary_records),
            baseline_metrics=_aggregate(baseline_records),
            sample_counts={"canary": len(canary_records), "baseline": len(baseline_records)},
        )


# =====================================================================
# 4. Promotion Evaluator
# =====================================================================

@dataclass
class PromotionDecision:
    """Decision package from comparative statistical evaluation."""
    promoted: bool
    promotion_score: float
    component_scores: dict[str, float]
    rejection_reasons: list[str]
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "promoted": self.promoted,
            "promotion_score": round(self.promotion_score, 5),
            "component_scores": {k: round(v, 5) for k, v in self.component_scores.items()},
            "rejection_reasons": self.rejection_reasons,
            "timestamp": self.timestamp,
        }


class PromotionEvaluator:
    """
    Evaluates empirical metrics against weighted objective thresholds:
    promotion_score = Σ_i w_i × (canary_metric_i - baseline_metric_i) / baseline_metric_i
    """
    WEIGHTS: dict[str, float] = {
        "success_rate": 0.40,
        "token_cost": -0.25,
        "latency": -0.15,
        "conformance": 0.20,
    }
    PROMOTION_THRESHOLD: float = -0.05  # Max allowable 5% aggregate degradation
    COOLDOWN_CYCLES: int = 3

    def evaluate(self, metrics: CanaryMetrics) -> PromotionDecision:
        """Computes weighted promotion score with robust division-by-zero guards."""
        c = metrics.canary_metrics
        b = metrics.baseline_metrics

        component_scores: dict[str, float] = {}
        rejection_reasons: list[str] = []

        # 1. success_rate (w = 0.4)
        c_sr = c.get("success_rate", 1.0)
        b_sr = b.get("success_rate", 1.0)
        delta_sr = (c_sr - b_sr) / max(b_sr, 1e-4) if b_sr > 0 else 0.0
        component_scores["success_rate"] = self.WEIGHTS["success_rate"] * delta_sr

        # 2. token_cost (w = -0.25, negative penalizes increase)
        c_tc = c.get("mean_token_cost", 0.0)
        b_tc = b.get("mean_token_cost", 0.0)
        delta_tc = (c_tc - b_tc) / max(b_tc, 1.0) if b_tc > 0 else 0.0
        component_scores["token_cost"] = self.WEIGHTS["token_cost"] * delta_tc

        # 3. latency (w = -0.15, negative penalizes increase)
        c_lat = c.get("mean_latency", 0.0)
        b_lat = b.get("mean_latency", 0.0)
        delta_lat = (c_lat - b_lat) / max(b_lat, 1e-3) if b_lat > 0 else 0.0
        component_scores["latency"] = self.WEIGHTS["latency"] * delta_lat

        # 4. conformance (w = 0.2)
        c_cf = c.get("conformance_score", 1.0)
        b_cf = b.get("conformance_score", 1.0)
        delta_cf = (c_cf - b_cf) / max(b_cf, 1e-4) if b_cf > 0 else 0.0
        component_scores["conformance"] = self.WEIGHTS["conformance"] * delta_cf

        promotion_score = sum(component_scores.values())
        promoted = promotion_score >= self.PROMOTION_THRESHOLD

        if not promoted:
            rejection_reasons.append(
                f"Promotion score {promotion_score:.4f} is below threshold {self.PROMOTION_THRESHOLD:.2f}"
            )
            for metric, score in component_scores.items():
                if score < -0.01:
                    rejection_reasons.append(
                        f"Metric '{metric}' penalized overall score by {score:.4f}"
                    )

        logger.info(
            "Evaluated Canary: score=%.4f (promoted=%s), components=%s",
            promotion_score,
            promoted,
            {k: round(v, 4) for k, v in component_scores.items()},
        )
        return PromotionDecision(
            promoted=promoted,
            promotion_score=promotion_score,
            component_scores=component_scores,
            rejection_reasons=rejection_reasons,
        )


# =====================================================================
# 5. Rollback Controller
# =====================================================================

class RollbackController:
    """
    Maintains a ring buffer of recent ConfigurationSnapshots and detects
    emergency conditions requiring instantaneous parameter recovery.
    """

    def __init__(self, max_snapshots: int = 5):
        self._buffer: collections.deque[ConfigurationSnapshot] = collections.deque(
            maxlen=max_snapshots
        )

    def checkpoint(self, snapshot: ConfigurationSnapshot) -> None:
        """Inserts a verified snapshot into the ring buffer."""
        # Avoid duplicate consecutive entries
        if not self._buffer or self._buffer[-1].content_hash != snapshot.content_hash:
            self._buffer.append(snapshot)
            logger.info(
                "Checkpoint buffer updated: v%s (%s), buffer depth=%d",
                snapshot.version_id,
                snapshot.content_hash[:8],
                len(self._buffer),
            )

    def rollback(self, target_version_id: int) -> ConfigurationSnapshot:
        """Retrieves and restores a snapshot by version_id from the ring buffer."""
        for snap in reversed(self._buffer):
            if snap.version_id == target_version_id:
                logger.warning(
                    "Rollback executed: restoring to snapshot v%s (%s)",
                    snap.version_id,
                    snap.content_hash[:8],
                )
                return snap
        available = [s.version_id for s in self._buffer]
        raise ValueError(
            f"Snapshot v{target_version_id} not found in rollback ring buffer {available}"
        )

    def check_emergency_rollback(
        self, canary_execution_records: list[dict[str, Any]]
    ) -> bool:
        """
        Triggers emergency rollback if:
        1. Canary set experiences >=2 consecutive milestone failures.
        2. Any conformance_score drops below 0.5.
        """
        if not canary_execution_records:
            return False

        consecutive_failures = 0
        for rec in canary_execution_records:
            # Check conformance threshold
            conformance = float(rec.get("conformance_score", rec.get("conformance", 1.0)))
            if conformance < 0.5:
                logger.error(
                    "EMERGENCY ROLLBACK TRIGGERED: Conformance score %.3f < 0.5 threshold",
                    conformance,
                )
                return True

            # Check consecutive milestone failures
            success = bool(rec.get("success", False))
            if not success:
                consecutive_failures += 1
                if consecutive_failures >= 2:
                    logger.error(
                        "EMERGENCY ROLLBACK TRIGGERED: %d consecutive canary milestone failures",
                        consecutive_failures,
                    )
                    return True
            else:
                consecutive_failures = 0

        return False

    def get_buffered_versions(self) -> list[int]:
        return [s.version_id for s in self._buffer]


# =====================================================================
# 6. Governance Audit Log
# =====================================================================

class GovernanceAuditLog:
    """
    Append-only audit trail logging every parameter proposal, validation,
    canary outcome, promotion decision, and rollback event with causal links.
    """

    def __init__(self, log_path: Union[str, Path] = "data/governance_audit.jsonl"):
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._in_memory_recent: collections.deque[dict[str, Any]] = collections.deque(
            maxlen=25
        )

    def _append(self, record: dict[str, Any]) -> None:
        record["timestamp"] = datetime.now(timezone.utc).isoformat()
        self._in_memory_recent.append(record)
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    def log_directive_received(
        self,
        directive_id: str,
        source: str,
        directive_summary: dict[str, Any],
        cycle_id: Optional[str] = None,
    ) -> None:
        self._append({
            "event_type": "DIRECTIVE_RECEIVED",
            "directive_id": directive_id,
            "source": source,
            "cycle_id": cycle_id,
            "summary": directive_summary,
        })

    def log_validation_outcome(
        self,
        directive_id: str,
        validated_directive: ValidatedDirective,
        cycle_id: Optional[str] = None,
    ) -> None:
        self._append({
            "event_type": "VALIDATION_OUTCOME",
            "directive_id": directive_id,
            "cycle_id": cycle_id,
            "is_applicable": validated_directive.is_applicable,
            "accepted": validated_directive.accepted_changes,
            "clamped": validated_directive.clamped_changes,
            "rejected": validated_directive.rejected_changes,
        })

    def log_canary_partition(
        self,
        directive_id: str,
        canary_set: list[str],
        baseline_set: list[str],
        cycle_id: Optional[str] = None,
    ) -> None:
        self._append({
            "event_type": "CANARY_PARTITIONED",
            "directive_id": directive_id,
            "cycle_id": cycle_id,
            "canary_set": canary_set,
            "baseline_set": baseline_set,
        })

    def log_promotion_decision(
        self,
        directive_id: str,
        promotion_decision: PromotionDecision,
        cycle_id: Optional[str] = None,
    ) -> None:
        self._append({
            "event_type": "PROMOTION_DECISION",
            "directive_id": directive_id,
            "cycle_id": cycle_id,
            "decision": promotion_decision.to_dict(),
        })

    def log_rollback(
        self,
        directive_id: str,
        from_version: int,
        to_version: int,
        reason: str,
        cycle_id: Optional[str] = None,
    ) -> None:
        self._append({
            "event_type": "ROLLBACK_APPLIED",
            "directive_id": directive_id,
            "cycle_id": cycle_id,
            "from_version": from_version,
            "to_version": to_version,
            "reason": reason,
        })

    def log_governance_veto(
        self,
        directive_id: str,
        veto_reasons: list[str],
        cooldown_cycles: int,
        cycle_id: Optional[str] = None,
    ) -> None:
        self._append({
            "event_type": "GOVERNANCE_VETO",
            "directive_id": directive_id,
            "cycle_id": cycle_id,
            "veto_reasons": veto_reasons,
            "cooldown_cycles": cooldown_cycles,
        })

    def get_recent_entries(self, limit: int = 10) -> list[dict[str, Any]]:
        return list(self._in_memory_recent)[-limit:]


# =====================================================================
# 7. Main Directive Governor Orchestrator
# =====================================================================

class DirectiveGovernor:
    """
    Main orchestrator for Pipeline Directive Governance & Safe Configuration.
    Manages the lifecycle: directive validation -> canary isolation -> observation ->
    promotion/rollback -> audit logging.
    """

    def __init__(
        self,
        initial_snapshot: ConfigurationSnapshot,
        snapshots_file: Union[str, Path] = "data/config_snapshots.jsonl",
        audit_file: Union[str, Path] = "data/governance_audit.jsonl",
    ):
        self.snapshots_file = Path(snapshots_file)
        self.audit_log = GovernanceAuditLog(log_path=audit_file)
        self.validator = DirectiveValidator()
        self.canary_controller = CanaryController()
        self.promotion_evaluator = PromotionEvaluator()
        self.rollback_controller = RollbackController(max_snapshots=5)

        self.current_snapshot = initial_snapshot
        self.rollback_controller.checkpoint(self.current_snapshot)
        self.current_snapshot.save_to_jsonl(self.snapshots_file)

        self.active_canary: Optional[CanaryState] = None
        self.pending_directives: collections.deque[dict[str, Any]] = collections.deque()
        self.veto_cooldowns: dict[str, int] = {}  # directive signature -> cycles remaining

    def get_current_config(self) -> ConfigurationSnapshot:
        """Returns the authoritative pipeline configuration snapshot."""
        return self.current_snapshot

    def rollback_to_version(
        self, target_version_id: int, reason: str = "Administrative rollback"
    ) -> ConfigurationSnapshot:
        """Restores a previous configuration from the ring buffer."""
        restored = self.rollback_controller.rollback(target_version_id)
        from_v = self.current_snapshot.version_id
        self.current_snapshot = restored
        self.current_snapshot.save_to_jsonl(self.snapshots_file)
        self.audit_log.log_rollback(
            directive_id=f"rb_{uuid.uuid4().hex[:6]}",
            from_version=from_v,
            to_version=target_version_id,
            reason=reason,
        )
        # Abort any active canary on rollback
        self.active_canary = None
        return self.current_snapshot

    def step_veto_cooldowns(self) -> None:
        """Decrements cooldown counters on vetoed directives."""
        to_delete = []
        for key in self.veto_cooldowns:
            self.veto_cooldowns[key] -= 1
            if self.veto_cooldowns[key] <= 0:
                to_delete.append(key)
        for key in to_delete:
            del self.veto_cooldowns[key]

    def process_directive(
        self,
        directive: dict[str, Any],
        repo_ids: list[str],
        historical_variance: dict[str, float],
        execution_records: Optional[list[dict[str, Any]]] = None,
        cycle_id: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Orchestrates full directive application lifecycle.
        Validates changes -> partitions canary -> manages observation -> evaluates promotion.
        """
        self.step_veto_cooldowns()
        did = directive.get("directive_id", f"dir_{uuid.uuid4().hex[:8]}")
        source = directive.get("source", "meta_optimizer")
        proposed_changes = directive.get(
            "proposed_changes", directive.get("parameters", {})
        )

        # Log directive receipt
        self.audit_log.log_directive_received(
            directive_id=did,
            source=source,
            directive_summary=proposed_changes,
            cycle_id=cycle_id,
        )

        # Check for active veto
        directive_signature = hashlib.md5(
            json.dumps(proposed_changes, sort_keys=True).encode()
        ).hexdigest()
        if directive_signature in self.veto_cooldowns:
            rem = self.veto_cooldowns[directive_signature]
            msg = f"Directive {did} matches vetoed signature; cooldown active ({rem} cycles remaining)"
            logger.warning(msg)
            return {"status": "VETOED", "directive_id": did, "message": msg}

        # Handle active canary state progression
        if self.active_canary is not None:
            logger.info(
                "Active canary in progress for directive '%s' (cycle %d/%d). Queuing new directive '%s'",
                self.active_canary.directive_id,
                self.active_canary.current_cycle,
                self.active_canary.window_cycles,
                did,
            )
            self.pending_directives.append(directive)

            # Process execution records if provided
            if execution_records:
                canary_records = [
                    r
                    for r in execution_records
                    if r.get("repo_id") in self.active_canary.canary_set
                ]
                if self.rollback_controller.check_emergency_rollback(canary_records):
                    self.audit_log.log_rollback(
                        directive_id=self.active_canary.directive_id,
                        from_version=self.current_snapshot.version_id,
                        to_version=self.active_canary.baseline_snapshot_version,
                        reason="Emergency rollback: Canary failure or low conformance",
                        cycle_id=cycle_id,
                    )
                    self.active_canary = None
                    return {
                        "status": "EMERGENCY_ROLLBACK",
                        "directive_id": did,
                        "message": "Active canary aborted due to emergency failure",
                    }

            return {
                "status": "QUEUED",
                "directive_id": did,
                "active_canary_id": self.active_canary.directive_id,
            }

        # Validate proposed directive against current snapshot
        validated = self.validator.validate_directive(
            current_snapshot=self.current_snapshot,
            proposed_changes=proposed_changes,
            known_repo_ids=repo_ids,
            directive_id=did,
            source=source,
        )
        self.audit_log.log_validation_outcome(
            directive_id=did, validated_directive=validated, cycle_id=cycle_id
        )

        if not validated.is_applicable:
            logger.warning("Directive %s rejected in full: %s", did, validated.rejected_changes)
            return {
                "status": "REJECTED",
                "directive_id": did,
                "reasons": validated.rejected_changes,
            }

        # Partition repositories for canary trial
        canary_set, baseline_set = self.canary_controller.partition_repositories(
            repo_ids=repo_ids, historical_variance=historical_variance
        )
        self.audit_log.log_canary_partition(
            directive_id=did,
            canary_set=canary_set,
            baseline_set=baseline_set,
            cycle_id=cycle_id,
        )

        observation_window = int(directive.get("window_cycles", 2))
        self.active_canary = self.canary_controller.apply_to_canary(
            validated_directive=validated,
            canary_set=canary_set,
            baseline_set=baseline_set,
            current_snapshot=self.current_snapshot,
            window_cycles=observation_window,
        )

        # If execution records are supplied in this invocation, evaluate canary immediately
        if execution_records:
            return self._evaluate_and_settle_canary(
                records=execution_records,
                directive_sig=directive_signature,
                cycle_id=cycle_id,
            )

        return {
            "status": "CANARY_ACTIVE",
            "directive_id": did,
            "canary_set": canary_set,
            "baseline_set": baseline_set,
            "accepted_changes": validated.accepted_changes,
            "clamped_changes": validated.clamped_changes,
            "rejected_changes": validated.rejected_changes,
        }

    def _evaluate_and_settle_canary(
        self,
        records: list[dict[str, Any]],
        directive_sig: str,
        cycle_id: Optional[str] = None,
    ) -> dict[str, Any]:
        """Settles an active canary trial with comparative metric evaluation."""
        if self.active_canary is None:
            return {"status": "ERROR", "message": "No active canary to settle"}

        canary_state = self.active_canary
        canary_records = [
            r for r in records if r.get("repo_id") in canary_state.canary_set
        ]

        # 1. Emergency safety check
        if self.rollback_controller.check_emergency_rollback(canary_records):
            self.audit_log.log_rollback(
                directive_id=canary_state.directive_id,
                from_version=self.current_snapshot.version_id,
                to_version=canary_state.baseline_snapshot_version,
                reason="Emergency threshold breached during canary window",
                cycle_id=cycle_id,
            )
            self.veto_cooldowns[directive_sig] = self.promotion_evaluator.COOLDOWN_CYCLES
            self.audit_log.log_governance_veto(
                directive_id=canary_state.directive_id,
                veto_reasons=["Emergency threshold breached in canary"],
                cooldown_cycles=self.promotion_evaluator.COOLDOWN_CYCLES,
                cycle_id=cycle_id,
            )
            self.active_canary = None
            return {
                "status": "EMERGENCY_ROLLBACK",
                "directive_id": canary_state.directive_id,
            }

        # 2. Collect paired metrics and evaluate
        metrics = self.canary_controller.collect_canary_metrics(
            canary_set=canary_state.canary_set,
            baseline_set=canary_state.baseline_set,
            execution_records=records,
        )
        decision = self.promotion_evaluator.evaluate(metrics)
        self.audit_log.log_promotion_decision(
            directive_id=canary_state.directive_id,
            promotion_decision=decision,
            cycle_id=cycle_id,
        )

        if decision.promoted:
            # Promote parameter set globally
            new_v = self.current_snapshot.version_id + 1
            new_snap = self.current_snapshot.apply_changes(
                new_version_id=new_v, changes=canary_state.experimental_params
            )
            self.current_snapshot = new_snap
            self.rollback_controller.checkpoint(new_snap)
            new_snap.save_to_jsonl(self.snapshots_file)
            self.active_canary = None
            logger.info("PROMOTED directive '%s' -> Snapshot v%d", canary_state.directive_id, new_v)
            return {
                "status": "PROMOTED",
                "directive_id": canary_state.directive_id,
                "new_version": new_v,
                "content_hash": new_snap.content_hash,
                "promotion_score": decision.promotion_score,
            }
        else:
            # Rollback canary to baseline and issue Governance Veto
            self.veto_cooldowns[directive_sig] = self.promotion_evaluator.COOLDOWN_CYCLES
            self.audit_log.log_governance_veto(
                directive_id=canary_state.directive_id,
                veto_reasons=decision.rejection_reasons,
                cooldown_cycles=self.promotion_evaluator.COOLDOWN_CYCLES,
                cycle_id=cycle_id,
            )
            self.active_canary = None
            logger.warning(
                "REJECTED directive '%s': score=%.4f, vetoed for %d cycles",
                canary_state.directive_id,
                decision.promotion_score,
                self.promotion_evaluator.COOLDOWN_CYCLES,
            )
            return {
                "status": "REJECTED_AT_CANARY",
                "directive_id": canary_state.directive_id,
                "reasons": decision.rejection_reasons,
                "promotion_score": decision.promotion_score,
            }

    def get_governance_summary(self) -> dict[str, Any]:
        """Provides high-level health and status telemetry of the governance gatekeeper."""
        return {
            "current_config_version": self.current_snapshot.version_id,
            "current_content_hash": self.current_snapshot.content_hash,
            "buffer_depth": self.current_snapshot.buffer_depth,
            "max_concurrency": self.current_snapshot.max_concurrency,
            "pending_directives_count": len(self.pending_directives),
            "active_canary": self.active_canary.to_dict() if self.active_canary else None,
            "active_vetoes_count": len(self.veto_cooldowns),
            "buffered_versions": self.rollback_controller.get_buffered_versions(),
            "recent_audit_entries": self.audit_log.get_recent_entries(limit=5),
        }


# =====================================================================
# Main Demonstration Block
# =====================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("PIPELINE DIRECTIVE GOVERNOR & CONFIGURATION CONTROLLER DEMONSTRATION")
    print("=" * 80)

    # Clean temporary files for clean run
    demo_snapshots_path = Path("data/demo_config_snapshots.jsonl")
    demo_audit_path = Path("data/demo_governance_audit.jsonl")
    if demo_snapshots_path.exists():
        demo_snapshots_path.unlink()
    if demo_audit_path.exists():
        demo_audit_path.unlink()

    # 1. Construct baseline ConfigurationSnapshot v1
    repo_list = ["repo_alpha", "repo_beta", "repo_gamma", "repo_delta", "repo_epsilon"]
    initial_snap = ConfigurationSnapshot.create(
        version_id=1,
        buffer_depth=4,
        max_concurrency=4,
        ewma_alpha={r: 0.50 for r in repo_list},
        holt_winters_params={r: HWTriple(alpha=0.2, beta=0.1, gamma=0.05) for r in repo_list},
        conformance_thresholds={"milestone_pass_rate": 0.85, "lint_clean": 0.90},
        circuit_breaker_config={
            "default": CBParams(failure_threshold=5, cooldown_seconds=60.0, half_open_max=2)
        },
        model_tier_ordering={
            r: ["gemini-3.8-flash-tiered", "gemini-3.8-pro", "claude-opus-4-6-thinking"]
            for r in repo_list
        },
        cluster_policies={
            "primary_cluster": SchedulingPolicy(
                pacing_mode=PacingMode.MODERATE,
                model_tier_preference=("gemini-3.8-flash-tiered",),
                buffer_priority_weight=1.0,
            )
        },
    )

    governor = DirectiveGovernor(
        initial_snapshot=initial_snap,
        snapshots_file=demo_snapshots_path,
        audit_file=demo_audit_path,
    )
    print("\n[STEP 1] Initial Snapshot v1 Created:")
    print(f"  Version: {initial_snap.version_id}")
    print(f"  Content Hash: {initial_snap.content_hash}")
    print(f"  Buffer Depth: {initial_snap.buffer_depth}, Max Concurrency: {initial_snap.max_concurrency}")

    # 2. Process Directive with mixed Valid, Clamped, and Rejected parameters
    sample_directive = {
        "directive_id": "dir_opt_cycle_42",
        "source": "MetaOptimizer_Cycle_42",
        "proposed_changes": {
            "buffer_depth": 5,  # Valid: delta 1/4 = 25% (<= 40%)
            "max_concurrency": 8,  # Rejected: delta 4/4 = 100% (> 40% rate limit)
            "ewma_alpha": {
                "repo_alpha": 0.999,  # Clamped to 0.99, but delta (0.99-0.5)/0.5 = 98% > 40% -> Rejected
                "repo_beta": 0.60,  # Valid: delta (0.60-0.5)/0.5 = 20% (<= 40%)
            },
            "circuit_breaker_config": {
                "default": {"cooldown_seconds": 20.0}  # Clamped to 30.0s hard bound
            },
            "model_tier_ordering": {
                "repo_gamma": ["unsupported-alien-model-v9"]  # Rejected: unapproved model
            },
        },
    }

    variances = {
        "repo_alpha": 0.95,
        "repo_beta": 0.40,
        "repo_gamma": 0.85,
        "repo_delta": 0.10,
        "repo_epsilon": 0.05,
    }

    print("\n[STEP 2] Processing Directive with Mixed Invariant Compliance:")
    result_phase1 = governor.process_directive(
        directive=sample_directive,
        repo_ids=repo_list,
        historical_variance=variances,
        cycle_id="cycle_meta_042",
    )
    print(f"  Status: {result_phase1['status']}")
    print(f"  Accepted Changes: {list(result_phase1['accepted_changes'].keys())}")
    print(f"  Clamped Changes: {list(result_phase1['clamped_changes'].keys())}")
    print(f"  Rejected Changes: {list(result_phase1['rejected_changes'].keys())}")
    print(f"  Canary Cohort (Top Variance): {result_phase1['canary_set']}")
    print(f"  Baseline Cohort: {result_phase1['baseline_set']}")

    # 3. Simulate Canary Telemetry Observation & Promotion
    synthetic_records = [
        # Canary set repos (repo_alpha, repo_gamma) showing lower latency and lower token cost
        {
            "repo_id": "repo_alpha",
            "success": True,
            "token_cost": 820.0,
            "latency": 3.1,
            "conformance_score": 0.98,
        },
        {
            "repo_id": "repo_gamma",
            "success": True,
            "token_cost": 850.0,
            "latency": 3.2,
            "conformance_score": 0.96,
        },
        # Baseline set repos (repo_beta, repo_delta, repo_epsilon)
        {
            "repo_id": "repo_beta",
            "success": True,
            "token_cost": 1050.0,
            "latency": 4.1,
            "conformance_score": 0.92,
        },
        {
            "repo_id": "repo_delta",
            "success": True,
            "token_cost": 990.0,
            "latency": 4.0,
            "conformance_score": 0.93,
        },
        {
            "repo_id": "repo_epsilon",
            "success": True,
            "token_cost": 1020.0,
            "latency": 4.2,
            "conformance_score": 0.91,
        },
    ]

    print("\n[STEP 3] Running Canary Metric Evaluation & Promotion:")
    promotion_result = governor._evaluate_and_settle_canary(
        records=synthetic_records,
        directive_sig="dummy_sig_42",
        cycle_id="cycle_meta_042",
    )
    print(f"  Evaluation Status: {promotion_result['status']}")
    print(f"  Promotion Score: {promotion_result.get('promotion_score')}")
    print(f"  New Pipeline Snapshot Version: {governor.get_current_config().version_id}")
    print(f"  New Buffer Depth: {governor.get_current_config().buffer_depth}")

    # 4. Demonstrate Atomic Rollback to Version 1
    print("\n[STEP 4] Executing Rollback to Baseline Snapshot v1:")
    restored_snap = governor.rollback_to_version(1, reason="Test regression recovery verification")
    print(f"  Active Snapshot Version: {restored_snap.version_id}")
    print(f"  Restored Buffer Depth: {restored_snap.buffer_depth}")
    print(f"  Restored Hash matches Initial: {restored_snap.content_hash == initial_snap.content_hash}")

    # 5. Display Comprehensive Governance Summary
    summary = governor.get_governance_summary()
    print("\n[STEP 5] Final Governance Controller Telemetry:")
    print(f"  Current Config Version: {summary['current_config_version']}")
    print(f"  Buffered Snapshot History: {summary['buffered_versions']}")
    print(f"  Active Canary: {summary['active_canary']}")
    print(f"  Recent Audit Entries Count: {len(summary['recent_audit_entries'])}")

    print("\n" + "=" * 80)
    print("DEMONSTRATION COMPLETED SUCCESSFULLY")
    print("=" * 80)
    # Clean up test artifacts
    if demo_snapshots_path.exists():
        demo_snapshots_path.unlink()
    if demo_audit_path.exists():
        demo_audit_path.unlink()
    if demo_snapshots_path.parent.exists() and not any(demo_snapshots_path.parent.iterdir()):
        demo_snapshots_path.parent.rmdir()
