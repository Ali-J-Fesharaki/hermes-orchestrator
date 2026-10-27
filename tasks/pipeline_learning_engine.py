#!/usr/bin/env python3
"""
Pipeline Retrospective Learning Engine & Policy Gradient Optimizer
==================================================================
Tasks/Pipeline Learning Engine for the Hermes Orchestrator.

Closes the autonomous control loop by learning from historical pipeline execution
data to continuously optimize governance parameters. Implements:
  1. EpisodeTrajectoryConstructor: Formulates state-action-reward trajectories.
  2. PolicyGradientEstimator: REINFORCE with learned EMA baseline variance reduction.
  3. PolicyParameterRegistry: Per-dimension Gaussian distributions with Thompson sampling
     and Bayesian updates persisted via JSON sidecar.
  4. StalenessDetector: Multidimensional KL-divergence tracker to prevent policy drift
     and trigger adaptive exploration widening.
  5. PolicyEvolutionReporter: Rich Markdown diagnostic generator with ASCII sparklines.
  6. DirectiveGovernorIntegration: Submits canary validation proposals with confidence metrics.

Pure Python stdlib implementation with zero external dependencies (no NumPy/SciPy).
"""

from __future__ import annotations

import os
import json
import math
import random
import logging
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("hermes.pipeline_learning_engine")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# ============================================================================
# 1. Episode Trajectory and Buffer
# ============================================================================

@dataclass
class EpisodeTrajectory:
    """Represents a discrete cycle execution tuple (State, Action, Reward)."""
    state_vector: List[float]
    action_vector: List[float]
    reward: float
    cycle_id: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        """Convert trajectory to JSON-serializable dictionary."""
        return {
            "cycle_id": self.cycle_id,
            "timestamp": self.timestamp.isoformat(),
            "state_vector": self.state_vector,
            "action_vector": self.action_vector,
            "reward": round(self.reward, 6),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> EpisodeTrajectory:
        """Construct trajectory instance from dictionary."""
        ts = datetime.fromisoformat(data["timestamp"]) if isinstance(data["timestamp"], str) else data["timestamp"]
        return cls(
            state_vector=data["state_vector"],
            action_vector=data["action_vector"],
            reward=float(data["reward"]),
            cycle_id=str(data["cycle_id"]),
            timestamp=ts,
        )


class EpisodeBuffer:
    """Bounded circular buffer of orchestration episode trajectories."""

    def __init__(self, maxlen: int = 500) -> None:
        self.maxlen: int = maxlen
        self._buffer: deque[EpisodeTrajectory] = deque(maxlen=maxlen)

    def append(self, trajectory: EpisodeTrajectory) -> None:
        """Append an episode trajectory to the bounded queue."""
        self._buffer.append(trajectory)

    def get_recent(self, n: int) -> List[EpisodeTrajectory]:
        """Retrieve up to n most recent trajectories in chronological order."""
        if n <= 0:
            return []
        items = list(self._buffer)
        return items[-n:]

    def get_all(self) -> List[EpisodeTrajectory]:
        """Retrieve all trajectories currently buffered."""
        return list(self._buffer)

    def to_serializable(self) -> List[Dict[str, Any]]:
        """Return list of dictionary representations for all stored episodes."""
        return [ep.to_dict() for ep in self._buffer]

    def __len__(self) -> int:
        return len(self._buffer)


# ============================================================================
# 2. Reward Computer
# ============================================================================

class RewardComputer:
    """
    Calculates multi-objective composite scalar reward from pipeline cycle telemetry:
      R_t = alpha * Norm(Throughput) + beta * (1 - Norm(TokenCost)) + gamma * Norm(AnomalyFreeDuration)
    where alpha + beta + gamma = 1.0.
    """

    def __init__(
        self,
        alpha: float = 0.40,
        beta: float = 0.35,
        gamma: float = 0.25,
        max_throughput: float = 20.0,
        max_token_cost: float = 1.0,
        max_anomaly_free_duration: float = 60.0,
    ) -> None:
        total = alpha + beta + gamma
        if abs(total - 1.0) > 1e-6:
            # Normalize weights if not strictly summing to 1
            alpha /= total
            beta /= total
            gamma /= total
        self.alpha: float = alpha
        self.beta: float = beta
        self.gamma: float = gamma
        self.max_throughput: float = max(max_throughput, 1.0)
        self.max_token_cost: float = max(max_token_cost, 1e-4)
        self.max_anomaly_free_duration: float = max(max_anomaly_free_duration, 1.0)

    def compute(self, cycle_metrics: Dict[str, Any]) -> float:
        """
        Calculates composite scalar reward clamped to [0.0, 1.0].
        Expected metrics keys:
          - throughput_milestones_per_hour (float)
          - normalized_token_cost (float, 0.0 - 1.0) or token_cost
          - anomaly_free_duration_minutes (float)
        """
        throughput = float(cycle_metrics.get("throughput_milestones_per_hour", 0.0))
        norm_tp = min(max(throughput / self.max_throughput, 0.0), 1.0)

        raw_cost = float(cycle_metrics.get("normalized_token_cost", cycle_metrics.get("token_cost", 0.0)))
        norm_cost = min(max(raw_cost / self.max_token_cost, 0.0), 1.0)
        cost_efficiency = 1.0 - norm_cost

        anomaly_free = float(cycle_metrics.get("anomaly_free_duration_minutes", 0.0))
        norm_af = min(max(anomaly_free / self.max_anomaly_free_duration, 0.0), 1.0)

        composite = (self.alpha * norm_tp) + (self.beta * cost_efficiency) + (self.gamma * norm_af)
        return min(max(composite, 0.0), 1.0)


# ============================================================================
# 3. Policy Parameter Registry
# ============================================================================

@dataclass
class ParameterDimension:
    """Per-dimension distribution parameters and safety boundaries."""
    name: str
    mu: float
    sigma: float
    min_val: float
    max_val: float
    sigma_initial: float

    def to_dict(self) -> Dict[str, float]:
        return {
            "mu": self.mu,
            "sigma": self.sigma,
            "min_val": self.min_val,
            "max_val": self.max_val,
            "sigma_initial": self.sigma_initial,
        }

    @classmethod
    def from_dict(cls, name: str, data: Dict[str, Any]) -> ParameterDimension:
        return cls(
            name=name,
            mu=float(data["mu"]),
            sigma=float(data["sigma"]),
            min_val=float(data["min_val"]),
            max_val=float(data["max_val"]),
            sigma_initial=float(data.get("sigma_initial", data["sigma"])),
        )


class PolicyParameterRegistry:
    """
    Maintains per-governance-dimension Gaussian distributions N(mu_d, sigma_d^2).
    Performs Thompson sampling, Bayesian updates, and disk persistence.
    """

    DIMENSIONS: List[str] = [
        "worker_pool_size",
        "slot_spacing_ms",
        "quota_weight_0",
        "quota_weight_1",
        "quota_weight_2",
        "quota_weight_3",
        "cb_trip_threshold",
        "rate_ceiling",
    ]

    DEFAULT_CONFIGS: Dict[str, Dict[str, float]] = {
        "worker_pool_size": {"mu": 4.0, "sigma": 1.0, "min_val": 1.0, "max_val": 16.0},
        "slot_spacing_ms": {"mu": 250.0, "sigma": 40.0, "min_val": 50.0, "max_val": 2000.0},
        "quota_weight_0": {"mu": 0.25, "sigma": 0.05, "min_val": 0.05, "max_val": 1.0},
        "quota_weight_1": {"mu": 0.25, "sigma": 0.05, "min_val": 0.05, "max_val": 1.0},
        "quota_weight_2": {"mu": 0.25, "sigma": 0.05, "min_val": 0.05, "max_val": 1.0},
        "quota_weight_3": {"mu": 0.25, "sigma": 0.05, "min_val": 0.05, "max_val": 1.0},
        "cb_trip_threshold": {"mu": 5.0, "sigma": 1.0, "min_val": 1.0, "max_val": 20.0},
        "rate_ceiling": {"mu": 60.0, "sigma": 8.0, "min_val": 10.0, "max_val": 200.0},
    }

    def __init__(self, persistence_path: Optional[str] = None) -> None:
        self.persistence_path: str = persistence_path or "pipeline_policy_registry.json"
        self.dimensions: Dict[str, ParameterDimension] = {}
        self._initialize_defaults()
        if os.path.exists(self.persistence_path):
            self.load(self.persistence_path)

    def _initialize_defaults(self) -> None:
        """Populate initial registry dimensions from default specs."""
        for name in self.DIMENSIONS:
            cfg = self.DEFAULT_CONFIGS[name]
            self.dimensions[name] = ParameterDimension(
                name=name,
                mu=cfg["mu"],
                sigma=cfg["sigma"],
                min_val=cfg["min_val"],
                max_val=cfg["max_val"],
                sigma_initial=cfg["sigma"],
            )

    def sample_candidate(self) -> Dict[str, float]:
        """
        Thompson sampling: sample candidate_d ~ N(mu_d, sigma_d^2),
        clamped strictly to [min_d, max_d] safety bounds.
        """
        sampled = {}
        for name, dim in self.dimensions.items():
            val = random.gauss(dim.mu, dim.sigma)
            clamped_val = min(max(val, dim.min_val), dim.max_val)
            sampled[name] = clamped_val
        return sampled

    def bayesian_update(
        self,
        dimension: str,
        observed_target: float,
        sigma_obs: Optional[float] = None,
    ) -> None:
        """
        Bayesian update of Gaussian distribution:
          mu_d' = (sigma_prior^2 * target_d + sigma_obs^2 * mu_d) / (sigma_prior^2 + sigma_obs^2)
        """
        if dimension not in self.dimensions:
            logger.warning("Dimension %s not found in registry.", dimension)
            return

        dim = self.dimensions[dimension]
        sigma_prior_sq = dim.sigma ** 2
        obs_variance = (sigma_obs ** 2) if sigma_obs is not None else (dim.sigma_initial * 0.5) ** 2

        denominator = sigma_prior_sq + obs_variance
        if denominator <= 1e-9:
            return

        new_mu = (sigma_prior_sq * observed_target + obs_variance * dim.mu) / denominator
        dim.mu = min(max(new_mu, dim.min_val), dim.max_val)

        # Posterior variance: (sigma_prior^2 * obs_variance) / denominator
        post_variance = (sigma_prior_sq * obs_variance) / denominator
        # Bound sigma to avoid premature variance collapse
        min_sigma = dim.sigma_initial * 0.15
        dim.sigma = max(math.sqrt(post_variance), min_sigma)

    def get_exploration_ratio(self) -> float:
        """Compute aggregate exploration ratio = mean(sigma) / mean(sigma_initial)."""
        if not self.dimensions:
            return 1.0
        cur_sigma_sum = sum(d.sigma for d in self.dimensions.values())
        init_sigma_sum = sum(d.sigma_initial for d in self.dimensions.values())
        if init_sigma_sum <= 1e-9:
            return 1.0
        return cur_sigma_sum / init_sigma_sum

    def save(self, path: Optional[str] = None) -> None:
        """Persist registry to JSON sidecar file."""
        target_path = path or self.persistence_path
        dir_name = os.path.dirname(target_path)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)

        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "exploration_ratio": round(self.get_exploration_ratio(), 4),
            "dimensions": {name: dim.to_dict() for name, dim in self.dimensions.items()},
        }
        with open(target_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        logger.debug("Policy registry successfully saved to %s", target_path)

    def load(self, path: Optional[str] = None) -> bool:
        """Load registry from JSON sidecar file."""
        target_path = path or self.persistence_path
        if not os.path.exists(target_path):
            return False
        try:
            with open(target_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            dims_data = payload.get("dimensions", {})
            for name, ddata in dims_data.items():
                if name in self.DIMENSIONS:
                    self.dimensions[name] = ParameterDimension.from_dict(name, ddata)
            logger.info("Loaded policy parameter registry from %s", target_path)
            return True
        except Exception as e:
            logger.error("Failed to load policy registry from %s: %s", target_path, e)
            return False


# ============================================================================
# 4. Policy Gradient Estimator (REINFORCE with EMA Baseline)
# ============================================================================

class PolicyGradientEstimator:
    """
    Implements REINFORCE policy gradient optimization with an exponential
    moving average (EMA) reward baseline for variance reduction:
      nabla_J = (1/N) * sum_t [ (R_t - b_t) * nabla_log_pi(A_t | S_t) ]
    Clipping applied at max_norm=1.0.
    """

    def __init__(
        self,
        learning_rate: float = 0.01,
        max_grad_norm: float = 1.0,
        update_every_k: int = 10,
        baseline_ema_alpha: float = 0.1,
    ) -> None:
        self.learning_rate: float = learning_rate
        self.max_grad_norm: float = max_grad_norm
        self.update_every_k: int = update_every_k
        self.baseline_ema_alpha: float = baseline_ema_alpha
        self.baseline: Optional[float] = None

    def update_baseline(self, reward: float) -> float:
        """Update EMA reward baseline."""
        if self.baseline is None:
            self.baseline = reward
        else:
            self.baseline = (1.0 - self.baseline_ema_alpha) * self.baseline + self.baseline_ema_alpha * reward
        return self.baseline

    def estimate_gradient(
        self,
        episodes: List[EpisodeTrajectory],
        registry: PolicyParameterRegistry,
    ) -> Dict[str, float]:
        """
        Estimate policy gradient for distribution means (mu_d) over the given batch of episodes.
        For a Gaussian policy pi(a_d) ~ N(mu_d, sigma_d^2),
        nabla_{mu_d} log pi(a_d) = (a_d - mu_d) / (sigma_d^2).
        """
        if not episodes:
            return {d: 0.0 for d in registry.DIMENSIONS}

        n = len(episodes)
        grads: Dict[str, float] = {d: 0.0 for d in registry.DIMENSIONS}

        # Track rolling baseline across episodes in order
        for ep in episodes:
            b_t = self.update_baseline(ep.reward)
            advantage = ep.reward - b_t

            for idx, d_name in enumerate(registry.DIMENSIONS):
                if idx >= len(ep.action_vector):
                    continue
                a_d = ep.action_vector[idx]
                dim = registry.dimensions[d_name]
                var_d = max(dim.sigma ** 2, 1e-6)

                grad_log_pi = (a_d - dim.mu) / var_d
                grads[d_name] += advantage * grad_log_pi

        for d_name in grads:
            grads[d_name] /= float(n)

        return grads

    def apply_gradient(
        self,
        registry: PolicyParameterRegistry,
        gradient: Dict[str, float],
    ) -> Dict[str, float]:
        """
        Clip gradient L2 norm to max_grad_norm and apply gradient step:
          mu_d <- clamp(mu_d + lr * grad_d, min_d, max_d)
        Returns the clipped gradient dictionary.
        """
        sq_sum = sum(g ** 2 for g in gradient.values())
        norm = math.sqrt(sq_sum)

        scale = 1.0
        if norm > self.max_grad_norm and norm > 1e-9:
            scale = self.max_grad_norm / norm

        clipped_grad: Dict[str, float] = {}
        for d_name, g in gradient.items():
            cg = g * scale
            clipped_grad[d_name] = cg
            dim = registry.dimensions.get(d_name)
            if dim is not None:
                step = self.learning_rate * cg
                dim.mu = min(max(dim.mu + step, dim.min_val), dim.max_val)

        return clipped_grad


# ============================================================================
# 5. Staleness Detector
# ============================================================================

class StalenessDetector:
    """
    Computes KL divergence KL(pi_current || pi_empirical) against empirical distribution
    of top-performing episodes. Widens exploration variance if policy enters staleness.
    """

    def __init__(self, kl_threshold: float = 0.5) -> None:
        self.kl_threshold: float = kl_threshold

    def check_staleness(
        self,
        registry: PolicyParameterRegistry,
        recent_episodes: List[EpisodeTrajectory],
    ) -> Tuple[bool, float, Dict[str, float]]:
        """
        Compute KL divergence:
          KL(pi_current || pi_empirical) = sum_d [ ln(sigma_emp / sigma_cur) +
            (sigma_cur^2 + (mu_cur - mu_emp)^2)/(2 * sigma_emp^2) - 0.5 ]
        Uses top 25% highest reward episodes as empirical target.
        """
        if len(recent_episodes) < 4:
            return False, 0.0, {}

        # Select top 25% reward trajectories
        sorted_eps = sorted(recent_episodes, key=lambda ep: ep.reward, reverse=True)
        top_k_count = max(2, len(sorted_eps) // 4)
        top_eps = sorted_eps[:top_k_count]

        total_kl = 0.0
        per_dim_kl: Dict[str, float] = {}

        for idx, d_name in enumerate(registry.DIMENSIONS):
            dim = registry.dimensions[d_name]
            actions = [ep.action_vector[idx] for ep in top_eps if idx < len(ep.action_vector)]
            if not actions:
                continue

            # Compute empirical mean and standard deviation
            mu_emp = sum(actions) / len(actions)
            variance_emp = sum((x - mu_emp) ** 2 for x in actions) / len(actions)
            sigma_emp = max(math.sqrt(variance_emp), dim.sigma_initial * 0.1)

            mu_cur = dim.mu
            sigma_cur = max(dim.sigma, 1e-4)

            # KL( N(mu_cur, sigma_cur^2) || N(mu_emp, sigma_emp^2) )
            term1 = math.log(sigma_emp / sigma_cur)
            term2 = (sigma_cur ** 2 + (mu_cur - mu_emp) ** 2) / (2.0 * (sigma_emp ** 2))
            kl_d = max(0.0, term1 + term2 - 0.5)

            per_dim_kl[d_name] = kl_d
            total_kl += kl_d

        is_stale = total_kl > self.kl_threshold
        return is_stale, total_kl, per_dim_kl

    def trigger_reestimation(self, registry: PolicyParameterRegistry) -> None:
        """Widen exploration: reset sigma to max(sigma, sigma_initial * 0.5)."""
        logger.warning("Policy staleness detected. Triggering parameter re-estimation / variance widening.")
        for dim in registry.dimensions.values():
            floor_sigma = dim.sigma_initial * 0.5
            if dim.sigma < floor_sigma:
                dim.sigma = floor_sigma


# ============================================================================
# 6. Policy Evolution Reporter
# ============================================================================

class PolicyEvolutionReporter:
    """
    Produces publication-grade Markdown reports detailing policy optimization trajectories:
      - Reward time-series & ASCII sparkline
      - Parameter drift
      - Exploration ratio
      - Sensitivity rankings
    """

    SPARK_CHARS = [" ", "▂", "▃", "▄", "▅", "▆", "▇", "█"]

    @classmethod
    def make_sparkline(cls, values: List[float], width: int = 40) -> str:
        """Render compact ASCII sparkline for a numeric sequence."""
        if not values:
            return ""
        if len(values) > width:
            # Resample / downsample down to width
            step = len(values) / width
            values = [values[int(i * step)] for i in range(width)]

        min_v = min(values)
        max_v = max(values)
        if abs(max_v - min_v) < 1e-7:
            return cls.SPARK_CHARS[len(cls.SPARK_CHARS) // 2] * len(values)

        spark = []
        for v in values:
            norm = (v - min_v) / (max_v - min_v)
            idx = int(norm * (len(cls.SPARK_CHARS) - 1))
            idx = min(max(idx, 0), len(cls.SPARK_CHARS) - 1)
            spark.append(cls.SPARK_CHARS[idx])
        return "".join(spark)

    def calculate_sensitivities(
        self,
        episodes: List[EpisodeTrajectory],
        registry: PolicyParameterRegistry,
    ) -> List[Tuple[str, float]]:
        """
        Estimate parameter sensitivities |partial R / partial d| using normalized
        covariance / sample gradient approximation over recent history.
        """
        if len(episodes) < 3:
            return [(d, 0.0) for d in registry.DIMENSIONS[:3]]

        mean_reward = sum(ep.reward for ep in episodes) / len(episodes)
        sensitivities: List[Tuple[str, float]] = []

        for idx, d_name in enumerate(registry.DIMENSIONS):
            actions = [ep.action_vector[idx] for ep in episodes if idx < len(ep.action_vector)]
            if len(actions) != len(episodes):
                continue
            mean_action = sum(actions) / len(actions)
            cov = sum((ep.reward - mean_reward) * (actions[i] - mean_action) for i, ep in enumerate(episodes)) / len(episodes)
            act_var = sum((a - mean_action) ** 2 for a in actions) / len(actions)
            grad_approx = abs(cov / act_var) if act_var > 1e-7 else 0.0
            sensitivities.append((d_name, grad_approx))

        sensitivities.sort(key=lambda item: item[1], reverse=True)
        return sensitivities

    def generate_report(
        self,
        episode_buffer: EpisodeBuffer,
        registry: PolicyParameterRegistry,
        cycle_id: str,
        output_path: str = "reports/policy_evolution_latest.md",
    ) -> str:
        """Generate comprehensive markdown report and save to output_path."""
        recent_50 = episode_buffer.get_recent(50)
        rewards = [ep.reward for ep in recent_50]
        avg_reward = (sum(rewards) / len(rewards)) if rewards else 0.0
        sparkline = self.make_sparkline(rewards, width=min(50, len(rewards))) if rewards else "N/A"
        exploration_ratio = registry.get_exploration_ratio()
        sensitivities = self.calculate_sensitivities(recent_50, registry)

        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        lines = [
            "# Policy Evolution & Retrospective Learning Report",
            f"**Cycle ID**: `{cycle_id}` | **Generated**: `{now_str}`",
            "",
            "## 1. Executive Summary",
            f"- **Buffered Episodes**: {len(episode_buffer)} / {episode_buffer.maxlen}",
            f"- **Recent Mean Reward (Last 50)**: `{avg_reward:.4f}`",
            f"- **Current Exploration Ratio**: `{exploration_ratio:.4f}` (Mean Sigma / Initial Sigma)",
            f"- **Reward Trajectory Sparkline**: `{sparkline}`",
            "",
            "## 2. Parameter Distribution Drift & Safety Bounds",
            "| Dimension | Current μ | Current σ | Safety Bounds [Min, Max] | Initial σ | Drift (μ - μ_0) |",
            "| :--- | :--- | :--- | :--- | :--- | :--- |",
        ]

        for d_name in registry.DIMENSIONS:
            dim = registry.dimensions[d_name]
            default_mu = registry.DEFAULT_CONFIGS[d_name]["mu"]
            drift = dim.mu - default_mu
            lines.append(
                f"| `{d_name}` | `{dim.mu:.3f}` | `{dim.sigma:.3f}` | "
                f"[{dim.min_val:.1f}, {dim.max_val:.1f}] | `{dim.sigma_initial:.3f}` | `{drift:+.3f}` |"
            )

        lines.extend([
            "",
            "## 3. Parameter Sensitivity Ranking (|∂R / ∂θ_d|)",
            "Top-3 dimensions governing pipeline throughput & stability:",
            "",
        ])

        for rank, (name, sens) in enumerate(sensitivities[:3], start=1):
            lines.append(f"{rank}. **`{name}`**: Sensitivity = `{sens:.4f}`")

        lines.extend([
            "",
            "## 4. Recent Episode Window (Last 10 Trajectories)",
            "| Cycle ID | Reward | Sampled Worker Pool | Slot Spacing (ms) | Rate Ceiling |",
            "| :--- | :--- | :--- | :--- | :--- |",
        ])

        for ep in recent_50[-10:]:
            act = ep.action_vector
            w_pool = f"{act[0]:.1f}" if len(act) > 0 else "-"
            spacing = f"{act[1]:.0f}" if len(act) > 1 else "-"
            rate_c = f"{act[7]:.1f}" if len(act) > 7 else "-"
            lines.append(f"| `{ep.cycle_id}` | `{ep.reward:.4f}` | {w_pool} | {spacing} | {rate_c} |")

        lines.append("")
        content = "\n".join(lines)

        try:
            dir_name = os.path.dirname(output_path)
            if dir_name:
                os.makedirs(dir_name, exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(content)
            logger.info("Saved policy evolution report to %s", output_path)
        except Exception as e:
            logger.error("Failed to write report to %s: %s", output_path, e)

        return content


# ============================================================================
# 7. Directive Governor Integration Helper
# ============================================================================

@dataclass
class PolicyUpdateDirective:
    """Wraps policy proposals for DirectiveGovernor canary validation."""
    source: str
    params: Dict[str, float]
    confidence: float
    confirmation_window_cycles: int = 2
    cycle_id: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "params": self.params,
            "confidence": round(self.confidence, 4),
            "confirmation_window_cycles": self.confirmation_window_cycles,
            "cycle_id": self.cycle_id,
        }


# ============================================================================
# 8. Main Pipeline Learning Engine Orchestrator
# ============================================================================

class PipelineLearningEngine:
    """
    Main orchestrator closing the loop between pipeline observation,
    reinforcement learning, and autonomous governance directives.
    """

    def __init__(
        self,
        registry_path: str = "pipeline_policy_registry.json",
        report_path: str = "reports/policy_evolution_latest.md",
        alpha: float = 0.40,
        beta: float = 0.35,
        gamma: float = 0.25,
        learning_rate: float = 0.01,
        max_grad_norm: float = 1.0,
        update_every_k: int = 10,
        kl_threshold: float = 0.5,
    ) -> None:
        self.registry_path: str = registry_path
        self.report_path: str = report_path
        self.episode_buffer: EpisodeBuffer = EpisodeBuffer(maxlen=500)
        self.reward_computer: RewardComputer = RewardComputer(alpha=alpha, beta=beta, gamma=gamma)
        self.registry: PolicyParameterRegistry = PolicyParameterRegistry(persistence_path=registry_path)
        self.gradient_estimator: PolicyGradientEstimator = PolicyGradientEstimator(
            learning_rate=learning_rate,
            max_grad_norm=max_grad_norm,
            update_every_k=update_every_k,
        )
        self.staleness_detector: StalenessDetector = StalenessDetector(kl_threshold=kl_threshold)
        self.reporter: PolicyEvolutionReporter = PolicyEvolutionReporter()
        self.cycle_counter: int = 0
        logger.info("PipelineLearningEngine initialized successfully.")

    def construct_state_vector(self, metrics: Dict[str, Any]) -> List[float]:
        """
        Construct state vector S_t:
        S_t = [
            worker_count,
            slot_spacing_ms,
            per_account_quota_fractions[0..3],
            cb_trip_thresholds,
            escalation_level,
            repo_count_active
        ] (9 dimensions).
        """
        worker_count = float(metrics.get("worker_count", 4.0))
        slot_spacing_ms = float(metrics.get("slot_spacing_ms", 250.0))

        # 4 accounts quota fractions
        quotas = metrics.get("per_account_quota_fractions", [0.25, 0.25, 0.25, 0.25])
        if len(quotas) < 4:
            quotas = list(quotas) + [0.25] * (4 - len(quotas))
        q0, q1, q2, q3 = [float(x) for x in quotas[:4]]

        cb_trip = float(metrics.get("cb_trip_thresholds", 5.0))
        escalation_level = float(metrics.get("escalation_level", 0.0))
        repo_count_active = float(metrics.get("repo_count_active", 1.0))

        return [
            worker_count,
            slot_spacing_ms,
            q0,
            q1,
            q2,
            q3,
            cb_trip,
            escalation_level,
            repo_count_active,
        ]

    def construct_action_vector(self, candidate_params: Dict[str, float]) -> List[float]:
        """Convert candidate parameter dictionary to ordered action vector."""
        return [candidate_params[d] for d in self.registry.DIMENSIONS]

    def on_cycle_complete(self, cycle_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Event handler triggered upon orchestration cycle completion.
        1. Calculates composite reward.
        2. Appends (S_t, A_t, R_t) to EpisodeBuffer.
        3. Updates Bayesian distributions for active parameters.
        4. If update_every_k cycles reached:
             - Performs policy gradient estimation & applies clipped step.
             - Checks policy staleness via KL divergence and widens variance if needed.
        5. Saves updated registry and generates markdown report.
        """
        self.cycle_counter += 1
        cycle_id = str(cycle_metrics.get("cycle_id", f"cycle_{self.cycle_counter}"))

        # 1. State vector S_t
        state_vec = self.construct_state_vector(cycle_metrics)

        # 2. Action vector A_t
        active_params = cycle_metrics.get("applied_parameters")
        if not active_params:
            active_params = self.registry.sample_candidate()
        action_vec = self.construct_action_vector(active_params)

        # 3. Reward R_t
        reward = self.reward_computer.compute(cycle_metrics)

        # 4. Construct trajectory and buffer
        trajectory = EpisodeTrajectory(
            state_vector=state_vec,
            action_vector=action_vec,
            reward=reward,
            cycle_id=cycle_id,
        )
        self.episode_buffer.append(trajectory)

        # Bayesian update for each dimension using the observed action & reward
        for idx, d_name in enumerate(self.registry.DIMENSIONS):
            observed_val = action_vec[idx]
            # When reward is strong, pull toward observed value; weight observation variance by (1 - reward)
            obs_sigma = max(0.05, 1.0 - reward) * self.registry.dimensions[d_name].sigma_initial
            self.registry.bayesian_update(d_name, observed_target=observed_val, sigma_obs=obs_sigma)

        gradient_applied = False
        staleness_triggered = False

        # Periodic gradient update check
        if self.cycle_counter % self.gradient_estimator.update_every_k == 0:
            batch = self.episode_buffer.get_recent(self.gradient_estimator.update_every_k)
            grads = self.gradient_estimator.estimate_gradient(batch, self.registry)
            self.gradient_estimator.apply_gradient(self.registry, grads)
            gradient_applied = True

            # Staleness verification
            is_stale, kl_div, per_dim_kl = self.staleness_detector.check_staleness(
                self.registry, self.episode_buffer.get_all()
            )
            if is_stale:
                self.staleness_detector.trigger_reestimation(self.registry)
                staleness_triggered = True

        # Persist registry and produce report
        self.registry.save(self.registry_path)
        report_content = self.reporter.generate_report(
            episode_buffer=self.episode_buffer,
            registry=self.registry,
            cycle_id=cycle_id,
            output_path=self.report_path,
        )

        return {
            "cycle_id": cycle_id,
            "reward": reward,
            "gradient_applied": gradient_applied,
            "staleness_triggered": staleness_triggered,
            "exploration_ratio": self.registry.get_exploration_ratio(),
            "report_path": self.report_path,
        }

    def propose_policy_update(self) -> Dict[str, Any]:
        """
        Formulate a DirectiveGovernor-compatible proposal:
          PolicyUpdateDirective(
            source='learning_engine',
            params=candidate_snapshot,
            confidence=1 - mean_sigma/sigma_initial
          )
        """
        candidate = self.registry.sample_candidate()
        exploration_ratio = self.registry.get_exploration_ratio()
        # Higher confidence when exploration ratio is lower (posterior has converged)
        confidence = max(0.0, min(1.0, 1.0 - (exploration_ratio - 0.15) / 0.85))

        directive = PolicyUpdateDirective(
            source="learning_engine",
            params=candidate,
            confidence=confidence,
            confirmation_window_cycles=2,
            cycle_id=f"proposal_{self.cycle_counter}",
        )
        return directive.to_dict()


# ============================================================================
# 9. Factory Function
# ============================================================================

def create_learning_engine(
    registry_path: str = "pipeline_policy_registry.json",
    report_path: str = "reports/policy_evolution_latest.md",
    **kwargs: Any,
) -> PipelineLearningEngine:
    """Module-level factory to instantiate configured learning engine."""
    return PipelineLearningEngine(
        registry_path=registry_path,
        report_path=report_path,
        **kwargs,
    )


# ============================================================================
# 10. Standalone Verification / Simulation
# ============================================================================

if __name__ == "__main__":
    import tempfile
    import shutil

    print("=" * 80)
    print("Hermes Pipeline Learning Engine - Standalone Verification & 50-Cycle Simulation")
    print("=" * 80)

    temp_dir = tempfile.mkdtemp(prefix="hermes_learning_test_")
    test_reg_path = os.path.join(temp_dir, "test_policy_registry.json")
    test_rep_path = os.path.join(temp_dir, "test_policy_evolution.md")

    try:
        engine = create_learning_engine(
            registry_path=test_reg_path,
            report_path=test_rep_path,
            learning_rate=0.02,
            update_every_k=10,
        )

        # Simulate 50 orchestration cycles with realistic dynamic metrics
        for c in range(1, 51):
            # Synthetic progress: throughput tends to improve, token cost drops, occasional spikes
            tp = min(18.0, 4.0 + 0.25 * c + random.uniform(-1.5, 1.5))
            cost = max(0.1, 0.9 - 0.015 * c + random.uniform(-0.1, 0.1))
            af_duration = min(60.0, 20.0 + 0.8 * c + random.uniform(-5.0, 5.0))

            # Candidate parameters from registry
            candidate_params = engine.registry.sample_candidate()

            metrics = {
                "cycle_id": f"sim_cycle_{c:03d}",
                "throughput_milestones_per_hour": tp,
                "normalized_token_cost": cost,
                "anomaly_free_duration_minutes": af_duration,
                "worker_count": candidate_params["worker_pool_size"],
                "slot_spacing_ms": candidate_params["slot_spacing_ms"],
                "per_account_quota_fractions": [
                    candidate_params["quota_weight_0"],
                    candidate_params["quota_weight_1"],
                    candidate_params["quota_weight_2"],
                    candidate_params["quota_weight_3"],
                ],
                "cb_trip_thresholds": candidate_params["cb_trip_threshold"],
                "escalation_level": 0 if c % 12 != 0 else 1,
                "repo_count_active": 3,
                "applied_parameters": candidate_params,
            }

            result = engine.on_cycle_complete(metrics)
            if c % 10 == 0:
                print(
                    f"Cycle {c:02d} | Reward: {result['reward']:.4f} | "
                    f"Grad Applied: {result['gradient_applied']} | "
                    f"Exploration Ratio: {result['exploration_ratio']:.4f}"
                )

        print("\n" + "=" * 80)
        print("Final Canary Proposal Generation:")
        proposal = engine.propose_policy_update()
        print(json.dumps(proposal, indent=2))

        print("\n" + "=" * 80)
        print("Generated Markdown Policy Evolution Report (Excerpt):")
        print("=" * 80)
        if os.path.exists(test_rep_path):
            with open(test_rep_path, "r", encoding="utf-8") as f:
                report_body = f.read()
            print(report_body)
        print("=" * 80)
        print("Verification completed successfully.")

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)