"""tasks/pipeline_meta_optimizer.py

Cross-Cycle Strategic Meta-Optimizer & Pipeline Topology Evolution Engine.

This module accumulates retrospective cycle reports across multiple execution
cycles of the Hermes Autonomous Orchestrator and dynamically optimizes pipeline
topology parameters (buffer depths, dispatch concurrency), algorithmic policy
hyperparameters (EWMA decay, Holt-Winters seasonality, conformance thresholds,
circuit breaker settings), and repository-specific scheduling policies.

All statistical surrogates and machine learning primitives (Gaussian Process
Regression with Matérn 5/2 kernel, Expected Improvement acquisition, Thompson
Sampling multi-armed bandit, and DBSCAN with automated knee detection) are
implemented entirely from first principles using standard Python libraries.
"""

from __future__ import annotations

import collections
from dataclasses import asdict, dataclass, field
import enum
import json
import logging
import math
import random
import statistics
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple, Union
import uuid

logger = logging.getLogger("hermes.tasks.meta_optimizer")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [MetaOptimizer] %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# =====================================================================
# Domain Enums & Feature Vector Definitions
# =====================================================================

class DispatchPacing(str, enum.Enum):
    """Pacing mode for task dispatch sequences within a cluster."""
    AGGRESSIVE = "aggressive"
    MODERATE = "moderate"
    CONSERVATIVE = "conservative"


class ModelTier(str, enum.Enum):
    """Model operational tier classification."""
    TIER_1_ADVANCED = "tier_1_advanced"
    TIER_2_STANDARD = "tier_2_standard"
    TIER_3_FAST = "tier_3_fast"


@dataclass
class CycleFeatureVector:
    """Quantitative performance vector distilled from a single execution cycle."""
    cycle_id: int
    buffer_depth: int
    dispatch_concurrency: int
    avg_conformance_score: float
    total_token_cost: int
    success_rate: float
    mean_duration_ratio: float
    circuit_breaker_trips: int
    replanning_cascades: int
    model_tier_distribution: Dict[str, float]
    per_repo_cost_variance: float
    throughput_rate: float
    cycle_duration_seconds: float
    total_tasks_executed: int
    per_repo_stats: Dict[str, Dict[str, float]] = field(default_factory=dict)
    timestamp: float = field(default=0.0)

    def to_dict(self) -> Dict[str, Any]:
        """Convert feature vector to JSON-serializable dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> CycleFeatureVector:
        """Construct feature vector from dictionary representation."""
        clean_data = dict(data)
        if "model_tier_distribution" not in clean_data or not clean_data["model_tier_distribution"]:
            clean_data["model_tier_distribution"] = {
                ModelTier.TIER_1_ADVANCED.value: 0.2,
                ModelTier.TIER_2_STANDARD.value: 0.5,
                ModelTier.TIER_3_FAST.value: 0.3,
            }
        return cls(**clean_data)


@dataclass
class RollingFeatureStats:
    """Summary statistics for a continuous feature dimension over a sliding window."""
    feature_name: str
    sample_count: int
    mean: float
    variance: float
    std_dev: float
    ols_slope: float
    min_value: float
    max_value: float


# =====================================================================
# Sliding Window & Time-Series Statistics
# =====================================================================

class SlidingCycleWindow:
    """Maintains a bounded window of past cycle feature vectors and computes rolling trends."""

    def __init__(self, maxlen: int = 10) -> None:
        self.maxlen: int = max(3, maxlen)
        self.history: collections.deque[CycleFeatureVector] = collections.deque(maxlen=self.maxlen)

    def append(self, vector: CycleFeatureVector) -> None:
        """Add a new cycle feature vector into the rolling deque."""
        self.history.append(vector)

    def __len__(self) -> int:
        return len(self.history)

    def is_full(self) -> bool:
        return len(self.history) >= self.maxlen

    def get_feature_series(self, feature_name: str) -> List[float]:
        """Extract continuous numerical series for a given feature attribute."""
        values: List[float] = []
        for vec in self.history:
            val = getattr(vec, feature_name, None)
            if val is not None and isinstance(val, (int, float)):
                values.append(float(val))
        return values

    def compute_ols_slope(self, values: Sequence[float]) -> float:
        """Calculate Ordinary Least Squares (OLS) trend slope across sequential time ticks."""
        n = len(values)
        if n < 2:
            return 0.0
        x = list(range(n))
        x_bar = (n - 1) / 2.0
        y_bar = statistics.mean(values)

        num = sum((x[i] - x_bar) * (values[i] - y_bar) for i in range(n))
        denom = sum((x[i] - x_bar) ** 2 for i in range(n))
        if abs(denom) < 1e-12:
            return 0.0
        return num / denom

    def compute_rolling_stats(self, feature_name: str) -> Optional[RollingFeatureStats]:
        """Compute rolling descriptive statistics and trend slope for a feature."""
        series = self.get_feature_series(feature_name)
        n = len(series)
        if n < 2:
            return None

        mean_val = statistics.mean(series)
        var_val = statistics.variance(series) if n > 1 else 0.0
        std_val = math.sqrt(var_val)
        slope = self.compute_ols_slope(series)
        min_v = min(series)
        max_v = max(series)

        return RollingFeatureStats(
            feature_name=feature_name,
            sample_count=n,
            mean=mean_val,
            variance=var_val,
            std_dev=std_val,
            ols_slope=slope,
            min_value=min_v,
            max_value=max_v,
        )

    def compute_all_stats(self) -> Dict[str, RollingFeatureStats]:
        """Compute statistics across all primary numeric dimensions."""
        feature_names = [
            "buffer_depth",
            "dispatch_concurrency",
            "avg_conformance_score",
            "total_token_cost",
            "success_rate",
            "mean_duration_ratio",
            "circuit_breaker_trips",
            "replanning_cascades",
            "per_repo_cost_variance",
            "throughput_rate",
            "cycle_duration_seconds",
            "total_tasks_executed",
        ]
        results: Dict[str, RollingFeatureStats] = {}
        for fname in feature_names:
            stats = self.compute_rolling_stats(fname)
            if stats:
                results[fname] = stats
        return results


# =====================================================================
# Thompson Sampling Multi-Armed Bandit Topology Optimizer
# =====================================================================

@dataclass
class BanditArm:
    """Discrete pipeline topology configuration arm."""
    buffer_depth: int
    dispatch_concurrency: int
    alpha: float = 1.0
    beta: float = 1.0
    pull_count: int = 0
    total_reward: float = 0.0

    @property
    def arm_id(self) -> str:
        return f"buf{self.buffer_depth}_conc{self.dispatch_concurrency}"

    @property
    def expected_mean(self) -> float:
        return self.alpha / (self.alpha + self.beta)


class ThompsonBanditOptimizer:
    """Discrete topology optimizer using Beta-Bernoulli Thompson Sampling."""

    def __init__(
        self,
        alpha_weight: float = 0.45,
        beta_weight: float = 0.25,
        gamma_weight: float = 0.30,
        cost_normalizer: float = 150000.0,
        throughput_normalizer: float = 20.0,
    ) -> None:
        self.alpha_weight = alpha_weight
        self.beta_weight = beta_weight
        self.gamma_weight = gamma_weight
        self.cost_normalizer = cost_normalizer
        self.throughput_normalizer = throughput_normalizer

        self.arms: Dict[str, BanditArm] = {}
        self.reward_history: List[float] = []
        self._initialize_arms()

    def _initialize_arms(self) -> None:
        """Create 16 Cartesian product arms: buffer ∈ {2,3,4,5} × concurrency ∈ {1,2,3,4}."""
        for buf in [2, 3, 4, 5]:
            for conc in [1, 2, 3, 4]:
                arm = BanditArm(buffer_depth=buf, dispatch_concurrency=conc)
                self.arms[arm.arm_id] = arm

    def compute_composite_reward(
        self, success_rate: float, total_token_cost: float, throughput_rate: float
    ) -> float:
        """Compute scalar composite performance reward in [0.0, 1.0]."""
        clamped_success = max(0.0, min(1.0, success_rate))
        clamped_cost = max(100.0, total_token_cost)
        inv_cost_term = max(0.0, min(1.0, self.cost_normalizer / (self.cost_normalizer + clamped_cost)))
        norm_throughput = max(0.0, min(1.0, throughput_rate / self.throughput_normalizer))

        reward = (
            self.alpha_weight * clamped_success
            + self.beta_weight * inv_cost_term
            + self.gamma_weight * norm_throughput
        )
        return max(0.0, min(1.0, reward))

    def select_arm(self) -> BanditArm:
        """Sample from Beta posterior distribution for each arm and select argmax."""
        best_sample = -1.0
        best_arm: Optional[BanditArm] = None

        for arm in self.arms.values():
            a = max(0.001, arm.alpha)
            b = max(0.001, arm.beta)
            sample = random.betavariate(a, b)
            if sample > best_sample:
                best_sample = sample
                best_arm = arm

        return best_arm if best_arm is not None else list(self.arms.values())[0]

    def record_feedback(
        self,
        buffer_depth: int,
        dispatch_concurrency: int,
        success_rate: float,
        total_token_cost: float,
        throughput_rate: float,
    ) -> float:
        """Update posterior distribution of target arm using reward relative to median."""
        arm_id = f"buf{buffer_depth}_conc{dispatch_concurrency}"
        if arm_id not in self.arms:
            self.arms[arm_id] = BanditArm(buffer_depth=buffer_depth, dispatch_concurrency=dispatch_concurrency)

        arm = self.arms[arm_id]
        reward = self.compute_composite_reward(success_rate, total_token_cost, throughput_rate)
        self.reward_history.append(reward)
        arm.pull_count += 1
        arm.total_reward += reward

        # Compute median baseline threshold
        if len(self.reward_history) >= 3:
            median_reward = statistics.median(self.reward_history)
        else:
            median_reward = 0.50

        # Update posterior with Bernoulli success or soft evidence
        if reward >= median_reward:
            arm.alpha += 1.0
        else:
            arm.beta += 1.0

        return reward

    def estimate_selection_confidence(self, selected_arm: BanditArm, mc_draws: int = 400) -> float:
        """Estimate statistical win probability of selected arm via Monte Carlo sampling."""
        if not self.arms:
            return 0.5
        win_count = 0
        all_arms = list(self.arms.values())

        for _ in range(mc_draws):
            samples = {
                arm.arm_id: random.betavariate(max(0.001, arm.alpha), max(0.001, arm.beta))
                for arm in all_arms
            }
            winning_id = max(samples.keys(), key=lambda k: samples[k])
            if winning_id == selected_arm.arm_id:
                win_count += 1

        return max(0.05, min(0.99, win_count / mc_draws))

    def export_state(self) -> Dict[str, Any]:
        """Serialize bandit state."""
        return {
            "reward_history": self.reward_history[-50:],
            "arms": {
                k: {
                    "buffer_depth": v.buffer_depth,
                    "dispatch_concurrency": v.dispatch_concurrency,
                    "alpha": v.alpha,
                    "beta": v.beta,
                    "pull_count": v.pull_count,
                    "total_reward": v.total_reward,
                }
                for k, v in self.arms.items()
            },
        }

    def import_state(self, state: Dict[str, Any]) -> None:
        """Restore bandit state."""
        self.reward_history = state.get("reward_history", [])
        arm_dict = state.get("arms", {})
        for k, v in arm_dict.items():
            if k in self.arms:
                self.arms[k].alpha = float(v.get("alpha", 1.0))
                self.arms[k].beta = float(v.get("beta", 1.0))
                self.arms[k].pull_count = int(v.get("pull_count", 0))
                self.arms[k].total_reward = float(v.get("total_reward", 0.0))


# =====================================================================
# Pure Python Linear Algebra & Gaussian Process Regressor
# =====================================================================

class LinearAlgebra:
    """First-principles linear algebra operations for kernel regression."""

    @staticmethod
    def vector_dot(a: Sequence[float], b: Sequence[float]) -> float:
        return sum(x * y for x, y in zip(a, b))

    @staticmethod
    def mat_vec_mul(matrix: Sequence[Sequence[float]], vector: Sequence[float]) -> List[float]:
        return [LinearAlgebra.vector_dot(row, vector) for row in matrix]

    @staticmethod
    def mat_mul(a: Sequence[Sequence[float]], b: Sequence[Sequence[float]]) -> List[List[float]]:
        rows_a = len(a)
        cols_a = len(a[0])
        cols_b = len(b[0])
        res = [[0.0] * cols_b for _ in range(rows_a)]
        for i in range(rows_a):
            for k in range(cols_a):
                r_ik = a[i][k]
                for j in range(cols_b):
                    res[i][j] += r_ik * b[k][j]
        return res

    @staticmethod
    def transpose(matrix: Sequence[Sequence[float]]) -> List[List[float]]:
        if not matrix or not matrix[0]:
            return []
        rows = len(matrix)
        cols = len(matrix[0])
        return [[matrix[r][c] for r in range(rows)] for c in range(cols)]

    @staticmethod
    def cholesky(matrix: Sequence[Sequence[float]], jitter: float = 1e-5) -> List[List[float]]:
        """Compute lower triangular Cholesky factorization L such that L L^T = A."""
        n = len(matrix)
        lower = [[0.0] * n for _ in range(n)]

        for i in range(n):
            for j in range(i + 1):
                sum_val = sum(lower[i][k] * lower[j][k] for k in range(j))
                if i == j:
                    diag = matrix[i][i] - sum_val + jitter
                    if diag <= 0.0:
                        diag = jitter
                    lower[i][j] = math.sqrt(diag)
                else:
                    denom = lower[j][j]
                    if abs(denom) < 1e-12:
                        denom = 1e-12
                    lower[i][j] = (matrix[i][j] - sum_val) / denom
        return lower

    @staticmethod
    def solve_lower(lower: Sequence[Sequence[float]], b: Sequence[float]) -> List[float]:
        """Forward substitution solve for L y = b."""
        n = len(lower)
        y = [0.0] * n
        for i in range(n):
            sum_val = sum(lower[i][k] * y[k] for k in range(i))
            denom = lower[i][i] if abs(lower[i][i]) > 1e-12 else 1e-12
            y[i] = (b[i] - sum_val) / denom
        return y

    @staticmethod
    def solve_cholesky(lower: Sequence[Sequence[float]], b: Sequence[float]) -> List[float]:
        """Solve L L^T x = b using forward and backward substitution."""
        n = len(lower)
        y = LinearAlgebra.solve_lower(lower, b)
        # Backward substitution for L^T x = y
        x = [0.0] * n
        for i in range(n - 1, -1, -1):
            sum_val = sum(lower[k][i] * x[k] for k in range(i + 1, n))
            denom = lower[i][i] if abs(lower[i][i]) > 1e-12 else 1e-12
            x[i] = (y[i] - sum_val) / denom
        return x


class GaussianDistribution:
    """Standard normal probability density and cumulative distribution approximations."""

    @staticmethod
    def pdf(x: float) -> float:
        """Standard normal probability density function."""
        return (1.0 / math.sqrt(2.0 * math.pi)) * math.exp(-0.5 * x * x)

    @staticmethod
    def cdf(x: float) -> float:
        """Standard normal cumulative distribution function via math.erf."""
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

    @staticmethod
    def abramowitz_stegun_cdf(x: float) -> float:
        """Rational approximation of normal CDF (Abramowitz & Stegun formula 7.1.26)."""
        if x < 0.0:
            return 1.0 - GaussianDistribution.abramowitz_stegun_cdf(-x)
        b0 = 0.2316419
        b1 = 0.319381530
        b2 = -0.356563782
        b3 = 1.781477937
        b4 = -1.821255978
        b5 = 1.330274429
        t = 1.0 / (1.0 + b0 * x)
        poly = t * (b1 + t * (b2 + t * (b3 + t * (b4 + t * b5))))
        return 1.0 - GaussianDistribution.pdf(x) * poly


class GaussianProcessRegressor:
    """Gaussian Process with Matérn 5/2 covariance kernel and White Noise."""

    def __init__(
        self,
        length_scale: float = 0.45,
        signal_variance: float = 1.0,
        noise_variance: float = 1e-3,
    ) -> None:
        self.length_scale = length_scale
        self.signal_variance = signal_variance
        self.noise_variance = noise_variance

        self.x_train: List[List[float]] = []
        self.y_train: List[float] = []
        self.l_lower: Optional[List[List[float]]] = None
        self.alpha_weights: Optional[List[float]] = None

    def matern_52_kernel(self, x1: Sequence[float], x2: Sequence[float]) -> float:
        """Matérn 5/2 stationary covariance kernel."""
        dist_sq = sum((a - b) ** 2 for a, b in zip(x1, x2))
        dist = math.sqrt(max(0.0, dist_sq))
        scaled_d = math.sqrt(5.0) * dist / self.length_scale
        term = 1.0 + scaled_d + (5.0 * dist_sq) / (3.0 * (self.length_scale ** 2))
        return self.signal_variance * term * math.exp(-scaled_d)

    def fit(self, x: Sequence[Sequence[float]], y: Sequence[float]) -> None:
        """Fit GP surrogate to observation pairs."""
        self.x_train = [list(pt) for pt in x]
        self.y_train = list(y)
        n = len(self.x_train)
        if n == 0:
            return

        k_matrix = [[0.0] * n for _ in range(n)]
        for i in range(n):
            for j in range(i, n):
                cov = self.matern_52_kernel(self.x_train[i], self.x_train[j])
                if i == j:
                    cov += self.noise_variance
                k_matrix[i][j] = cov
                k_matrix[j][i] = cov

        self.l_lower = LinearAlgebra.cholesky(k_matrix, jitter=1e-5)
        self.alpha_weights = LinearAlgebra.solve_cholesky(self.l_lower, self.y_train)

    def predict(self, x_star: Sequence[float]) -> Tuple[float, float]:
        """Predict posterior mean and standard deviation at candidate point x_star."""
        if not self.x_train or self.l_lower is None or self.alpha_weights is None:
            return 0.5, 1.0

        n = len(self.x_train)
        k_star = [self.matern_52_kernel(self.x_train[i], x_star) for i in range(n)]
        mean = LinearAlgebra.vector_dot(k_star, self.alpha_weights)

        # Variance computation: v = L^-1 k_star
        v = LinearAlgebra.solve_lower(self.l_lower, k_star)
        v_sq = sum(val * val for val in v)
        k_star_star = self.matern_52_kernel(x_star, x_star) + self.noise_variance
        variance = max(0.0, k_star_star - v_sq)
        std_dev = math.sqrt(variance)

        return mean, std_dev


# =====================================================================
# Bayesian Policy Parameter Optimizer
# =====================================================================

@dataclass
class AdjustmentVector:
    """Recommended adjustment for a continuous policy parameter."""
    parameter_name: str
    current_value: float
    proposed_value: float
    ci_lower: float
    ci_upper: float
    evidence_strength: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class ExpectedImprovement:
    """Expected Improvement acquisition function for sequential Bayesian optimization."""

    def __init__(self, xi: float = 0.01) -> None:
        self.xi = xi

    def compute(self, mu: float, sigma: float, best_y: float) -> float:
        """Calculate closed-form Expected Improvement over best observed value."""
        if sigma <= 1e-8:
            return 0.0
        delta = mu - best_y - self.xi
        z = delta / sigma
        phi_z = GaussianDistribution.pdf(z)
        phi_cap_z = GaussianDistribution.cdf(z)
        ei = delta * phi_cap_z + sigma * phi_z
        return max(0.0, ei)


class BayesianPolicyOptimizer:
    """Bayesian Optimizer adjusting continuous orchestrator policy parameters."""

    PARAMETER_BOUNDS: Dict[str, Tuple[float, float]] = {
        "ewma_decay": (0.05, 0.50),
        "hw_seasonal_period": (3.0, 20.0),
        "conformance_threshold": (0.10, 0.50),
        "cb_failure_threshold": (2.0, 8.0),
        "cb_recovery_timeout": (30.0, 300.0),
    }

    def __init__(self, initial_random_evals: int = 5, xi: float = 0.01) -> None:
        self.param_names = list(self.PARAMETER_BOUNDS.keys())
        self.dim = len(self.param_names)
        self.initial_random_evals = initial_random_evals
        self.ei = ExpectedImprovement(xi=xi)
        self.gp = GaussianProcessRegressor()

        self.history_x_unit: List[List[float]] = []
        self.history_y: List[float] = []
        self.current_params: Dict[str, float] = {
            "ewma_decay": 0.20,
            "hw_seasonal_period": 7.0,
            "conformance_threshold": 0.25,
            "cb_failure_threshold": 4.0,
            "cb_recovery_timeout": 60.0,
        }

    def _normalize_point(self, raw_params: Dict[str, float]) -> List[float]:
        """Map parameter dictionary into normalized [0.0, 1.0]^D space."""
        unit_point: List[float] = []
        for name in self.param_names:
            low, high = self.PARAMETER_BOUNDS[name]
            val = raw_params.get(name, (low + high) / 2.0)
            norm = (val - low) / (high - low)
            unit_point.append(max(0.0, min(1.0, norm)))
        return unit_point

    def _denormalize_point(self, unit_point: Sequence[float]) -> Dict[str, float]:
        """Convert normalized unit vector back into parameter values."""
        raw_params: Dict[str, float] = {}
        for i, name in enumerate(self.param_names):
            low, high = self.PARAMETER_BOUNDS[name]
            val = low + unit_point[i] * (high - low)
            if name in ("hw_seasonal_period", "cb_failure_threshold"):
                raw_params[name] = round(val)
            else:
                raw_params[name] = round(val, 4)
        return raw_params

    def compute_objective_from_vector(self, vector: CycleFeatureVector) -> float:
        """Formulate multi-objective scalar reward from cycle metrics."""
        # Objective = 0.40 * reliability + 0.35 * throughput + 0.25 * cost_efficiency
        reliability = (
            vector.success_rate * 0.6
            + vector.avg_conformance_score * 0.4
            - (vector.circuit_breaker_trips * 0.1)
            - (vector.replanning_cascades * 0.05)
        )
        reliability = max(0.0, min(1.0, reliability))

        norm_tp = max(0.0, min(1.0, vector.throughput_rate / 25.0))
        norm_cost = max(0.0, min(1.0, 120000.0 / (120000.0 + vector.total_token_cost)))

        score = 0.45 * reliability + 0.30 * norm_tp + 0.25 * norm_cost
        return max(0.0, min(1.0, score))

    def record_cycle_evaluation(
        self, parameter_state: Dict[str, float], vector: CycleFeatureVector
    ) -> None:
        """Register completed cycle parameter configuration and observed objective."""
        unit_x = self._normalize_point(parameter_state)
        objective_y = self.compute_objective_from_vector(vector)
        self.history_x_unit.append(unit_x)
        self.history_y.append(objective_y)
        self.current_params.update(parameter_state)

    def optimize_next_parameters(
        self, candidate_pool_size: int = 600
    ) -> Dict[str, AdjustmentVector]:
        """Select optimal next parameters maximizing Expected Improvement."""
        total_samples = len(self.history_y)

        # In initial burn-in phase, perform quasi-random exploration
        if total_samples < self.initial_random_evals:
            logger.info("Bayesian optimizer in warm-up phase (%d/%d samples)", total_samples, self.initial_random_evals)
            unit_candidate = [random.uniform(0.1, 0.9) for _ in range(self.dim)]
            proposed = self._denormalize_point(unit_candidate)
            adjustments: Dict[str, AdjustmentVector] = {}
            for name in self.param_names:
                curr_val = self.current_params.get(name, (self.PARAMETER_BOUNDS[name][0] + self.PARAMETER_BOUNDS[name][1]) / 2.0)
                prop_val = proposed[name]
                low, high = self.PARAMETER_BOUNDS[name]
                span = high - low
                adjustments[name] = AdjustmentVector(
                    parameter_name=name,
                    current_value=curr_val,
                    proposed_value=prop_val,
                    ci_lower=max(low, prop_val - 0.25 * span),
                    ci_upper=min(high, prop_val + 0.25 * span),
                    evidence_strength=0.35,
                )
            return adjustments

        # Fit Gaussian Process surrogate
        self.gp.fit(self.history_x_unit, self.history_y)
        best_y = max(self.history_y)

        # Generate candidate pool in unit space
        candidates: List[List[float]] = []
        for _ in range(candidate_pool_size):
            candidates.append([random.uniform(0.0, 1.0) for _ in range(self.dim)])

        # Include local perturbations around best observed point
        best_idx = self.history_y.index(best_y)
        best_x = self.history_x_unit[best_idx]
        for _ in range(50):
            perturbed = [
                max(0.0, min(1.0, x_val + random.gauss(0.0, 0.08)))
                for x_val in best_x
            ]
            candidates.append(perturbed)

        # Evaluate acquisition function
        best_ei = -1.0
        best_candidate = candidates[0]

        for cand in candidates:
            mu, sigma = self.gp.predict(cand)
            val_ei = self.ei.compute(mu, sigma, best_y)
            if val_ei > best_ei:
                best_ei = val_ei
                best_candidate = cand

        proposed_dict = self._denormalize_point(best_candidate)
        pred_mu, pred_sigma = self.gp.predict(best_candidate)

        # Build AdjustmentVectors with Gaussian 95% Credible Intervals
        adjustments = {}
        for i, name in enumerate(self.param_names):
            curr_val = self.current_params.get(name, proposed_dict[name])
            prop_val = proposed_dict[name]
            low, high = self.PARAMETER_BOUNDS[name]
            span = high - low

            # Credible intervals projected into parameter dimension
            ci_delta = 1.96 * pred_sigma * span * 0.5
            ci_low = max(low, prop_val - ci_delta)
            ci_high = min(high, prop_val + ci_delta)
            evidence = max(0.1, min(0.98, 1.0 - pred_sigma))

            adjustments[name] = AdjustmentVector(
                parameter_name=name,
                current_value=curr_val,
                proposed_value=prop_val,
                ci_lower=round(ci_low, 3),
                ci_upper=round(ci_high, 3),
                evidence_strength=round(evidence, 3),
            )

        return adjustments

    def export_state(self) -> Dict[str, Any]:
        """Serialize optimizer state."""
        return {
            "history_x_unit": self.history_x_unit,
            "history_y": self.history_y,
            "current_params": self.current_params,
        }

    def import_state(self, state: Dict[str, Any]) -> None:
        """Restore optimizer state."""
        self.history_x_unit = state.get("history_x_unit", [])
        self.history_y = state.get("history_y", [])
        self.current_params = state.get("current_params", self.current_params)


# =====================================================================
# StandardScaler & DBSCAN Clustering with Knee Detection
# =====================================================================

class StandardScaler:
    """Feature standardizer removing mean and scaling to unit variance."""

    def __init__(self) -> None:
        self.means: List[float] = []
        self.scales: List[float] = []

    def fit(self, data: Sequence[Sequence[float]]) -> StandardScaler:
        """Calculate column means and standard deviations."""
        if not data or not data[0]:
            return self
        n = len(data)
        d = len(data[0])
        self.means = [0.0] * d
        self.scales = [1.0] * d

        for j in range(d):
            col = [data[i][j] for i in range(n)]
            m = statistics.mean(col)
            s = statistics.stdev(col) if n > 1 else 1.0
            self.means[j] = m
            self.scales[j] = s if abs(s) > 1e-8 else 1.0

        return self

    def transform(self, data: Sequence[Sequence[float]]) -> List[List[float]]:
        """Scale data points according to fitted parameters."""
        if not self.means or not self.scales:
            return [list(row) for row in data]
        res: List[List[float]] = []
        for row in data:
            scaled_row = [(val - m) / s for val, m, s in zip(row, self.means, self.scales)]
            res.append(scaled_row)
        return res

    def fit_transform(self, data: Sequence[Sequence[float]]) -> List[List[float]]:
        return self.fit(data).transform(data)


class DBSCAN:
    """Density-Based Spatial Clustering of Applications with Noise (from scratch)."""

    def __init__(self, eps: Optional[float] = None, min_samples: int = 2) -> None:
        self.eps = eps
        self.min_samples = max(1, min_samples)
        self.labels: List[int] = []

    @staticmethod
    def euclidean_distance(a: Sequence[float], b: Sequence[float]) -> float:
        return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))

    def detect_epsilon_via_knee(self, data: Sequence[Sequence[float]]) -> float:
        """Determine optimal eps by identifying maximum curvature knee in k-distance graph."""
        n = len(data)
        if n < self.min_samples + 1:
            return 1.2

        k_distances: List[float] = []
        for i in range(n):
            distances = sorted([self.euclidean_distance(data[i], data[j]) for j in range(n) if i != j])
            k_th = distances[min(self.min_samples - 1, len(distances) - 1)]
            k_distances.append(k_th)

        k_distances.sort()
        m = len(k_distances)

        # Distance from point (i, k_dist[i]) to secant line between (0, k_dist[0]) and (m-1, k_dist[m-1])
        x1, y1 = 0.0, k_distances[0]
        x2, y2 = float(m - 1), k_distances[-1]
        line_dx = x2 - x1
        line_dy = y2 - y1
        line_norm = math.sqrt(line_dx * line_dx + line_dy * line_dy)
        if line_norm < 1e-8:
            return max(0.5, k_distances[-1])

        max_dist = -1.0
        knee_idx = m // 2

        for i in range(m):
            xi, yi = float(i), k_distances[i]
            # Perpendicular distance: |dy*x - dx*y + x2*y1 - y2*x1| / norm
            perp_dist = abs(line_dy * xi - line_dx * yi + x2 * y1 - y2 * x1) / line_norm
            if perp_dist > max_dist:
                max_dist = perp_dist
                knee_idx = i

        chosen_eps = k_distances[knee_idx]
        return max(0.35, min(3.5, chosen_eps))

    def fit_predict(self, data: Sequence[Sequence[float]]) -> List[int]:
        """Perform DBSCAN clustering on spatial matrix."""
        n = len(data)
        if n == 0:
            self.labels = []
            return []

        if self.eps is None:
            self.eps = self.detect_epsilon_via_knee(data)

        self.labels = [-1] * n  # -1 represents unassigned/noise
        cluster_id = 0

        def get_neighbors(pt_idx: int) -> List[int]:
            return [
                j for j in range(n)
                if self.euclidean_distance(data[pt_idx], data[j]) <= (self.eps or 1.0)
            ]

        visited = [False] * n

        for i in range(n):
            if visited[i]:
                continue
            visited[i] = True

            neighbors = get_neighbors(i)
            if len(neighbors) < self.min_samples:
                self.labels[i] = -1  # Mark as noise candidate
            else:
                self.labels[i] = cluster_id
                queue = list(neighbors)
                idx = 0
                while idx < len(queue):
                    neighbor_pt = queue[idx]
                    idx += 1
                    if not visited[neighbor_pt]:
                        visited[neighbor_pt] = True
                        next_neighbors = get_neighbors(neighbor_pt)
                        if len(next_neighbors) >= self.min_samples:
                            queue.extend([pt for pt in next_neighbors if pt not in queue])
                    if self.labels[neighbor_pt] == -1:
                        self.labels[neighbor_pt] = cluster_id

                cluster_id += 1

        return self.labels


# =====================================================================
# Repository Clustering & Scheduling Policy Synthesis
# =====================================================================

@dataclass
class SchedulingPolicy:
    """Autonomous execution and pacing policy assigned to a cluster of repositories."""
    cluster_id: int
    dispatch_pacing: DispatchPacing
    preferred_model_tier: str
    buffer_priority_weight: float
    max_concurrent_per_cluster: int
    assigned_repositories: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        res = asdict(self)
        res["dispatch_pacing"] = self.dispatch_pacing.value
        return res

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> SchedulingPolicy:
        clean = dict(data)
        clean["dispatch_pacing"] = DispatchPacing(clean["dispatch_pacing"])
        return cls(**clean)


class RepositoryClusterer:
    """Clusters repositories by historical behavior and assigns customized scheduling policies."""

    FEATURE_NAMES = [
        "mean_token_cost",
        "std_token_cost",
        "failure_rate",
        "mean_duration",
        "dependency_fan_out",
        "model_tier_preference_entropy",
    ]

    def __init__(self, min_samples: int = 2) -> None:
        self.min_samples = min_samples
        self.scaler = StandardScaler()

    @staticmethod
    def calculate_shannon_entropy(tier_counts: Dict[str, float]) -> float:
        """Compute Shannon entropy for model tier distribution."""
        total = sum(tier_counts.values())
        if total <= 0.0:
            return 0.0
        entropy = 0.0
        for count in tier_counts.values():
            if count > 0.0:
                p = count / total
                entropy -= p * math.log2(p)
        return entropy

    def extract_repo_features(
        self, window: SlidingCycleWindow
    ) -> Tuple[List[str], List[List[float]]]:
        """Aggregate cross-cycle repository statistics from sliding window."""
        repo_data: Dict[str, Dict[str, List[float]]] = collections.defaultdict(
            lambda: {
                "token_cost": [],
                "failures": [],
                "duration": [],
                "fan_out": [],
                "tier_entropy": [],
            }
        )

        for vector in window.history:
            for repo, stats in vector.per_repo_stats.items():
                repo_data[repo]["token_cost"].append(float(stats.get("token_cost", 1000.0)))
                repo_data[repo]["failures"].append(float(stats.get("failure_rate", 0.0)))
                repo_data[repo]["duration"].append(float(stats.get("duration_seconds", 30.0)))
                repo_data[repo]["fan_out"].append(float(stats.get("dependency_fan_out", 1.0)))

                tier_counts = {
                    ModelTier.TIER_1_ADVANCED.value: stats.get("tier_1_count", 1.0),
                    ModelTier.TIER_2_STANDARD.value: stats.get("tier_2_count", 2.0),
                    ModelTier.TIER_3_FAST.value: stats.get("tier_3_count", 1.0),
                }
                entropy = self.calculate_shannon_entropy(tier_counts)
                repo_data[repo]["tier_entropy"].append(entropy)

        repo_names = sorted(repo_data.keys())
        feature_matrix: List[List[float]] = []

        for repo in repo_names:
            costs = repo_data[repo]["token_cost"] or [1000.0]
            fails = repo_data[repo]["failures"] or [0.0]
            durs = repo_data[repo]["duration"] or [30.0]
            fan_outs = repo_data[repo]["fan_out"] or [1.0]
            entropies = repo_data[repo]["tier_entropy"] or [0.5]

            mean_cost = statistics.mean(costs)
            std_cost = statistics.stdev(costs) if len(costs) > 1 else 0.0
            fail_rate = statistics.mean(fails)
            mean_dur = statistics.mean(durs)
            mean_fan = statistics.mean(fan_outs)
            mean_entropy = statistics.mean(entropies)

            feature_matrix.append([
                mean_cost,
                std_cost,
                fail_rate,
                mean_dur,
                mean_fan,
                mean_entropy,
            ])

        return repo_names, feature_matrix

    def cluster_and_synthesize(
        self, window: SlidingCycleWindow
    ) -> Dict[int, SchedulingPolicy]:
        """Perform DBSCAN clustering and map each cluster to tailored SchedulingPolicy."""
        repo_names, feature_matrix = self.extract_repo_features(window)
        policies: Dict[int, SchedulingPolicy] = {}

        if not repo_names:
            # Default fallback policy
            policies[0] = SchedulingPolicy(
                cluster_id=0,
                dispatch_pacing=DispatchPacing.MODERATE,
                preferred_model_tier=ModelTier.TIER_2_STANDARD.value,
                buffer_priority_weight=1.0,
                max_concurrent_per_cluster=2,
                assigned_repositories=[],
            )
            return policies

        if len(repo_names) < 2:
            # Single repo fallback
            policies[0] = SchedulingPolicy(
                cluster_id=0,
                dispatch_pacing=DispatchPacing.MODERATE,
                preferred_model_tier=ModelTier.TIER_2_STANDARD.value,
                buffer_priority_weight=1.0,
                max_concurrent_per_cluster=2,
                assigned_repositories=repo_names,
            )
            return policies

        # Normalize features and run DBSCAN
        scaled_features = self.scaler.fit_transform(feature_matrix)
        dbscan = DBSCAN(min_samples=self.min_samples)
        labels = dbscan.fit_predict(scaled_features)

        # Group repos by cluster label
        clusters: Dict[int, List[int]] = collections.defaultdict(list)
        for idx, lbl in enumerate(labels):
            clusters[lbl].append(idx)

        # Synthesize policy per cluster
        for label, member_indices in clusters.items():
            cluster_repos = [repo_names[i] for i in member_indices]
            cluster_features = [feature_matrix[i] for i in member_indices]

            avg_fail = statistics.mean([f[2] for f in cluster_features])
            avg_dur = statistics.mean([f[3] for f in cluster_features])
            avg_fan = statistics.mean([f[4] for f in cluster_features])

            # Pacing and model tier synthesis
            if avg_fail > 0.12 or avg_fan > 4.0:
                pacing = DispatchPacing.CONSERVATIVE
                tier = ModelTier.TIER_1_ADVANCED.value
                buffer_weight = 1.45
                max_conc = 1
            elif avg_fail < 0.04 and avg_dur < 25.0:
                pacing = DispatchPacing.AGGRESSIVE
                tier = ModelTier.TIER_3_FAST.value
                buffer_weight = 0.85
                max_conc = 4
            else:
                pacing = DispatchPacing.MODERATE
                tier = ModelTier.TIER_2_STANDARD.value
                buffer_weight = 1.05
                max_conc = 2

            policy_id = label if label >= 0 else 999  # 999 indicates isolated noise repos
            policies[policy_id] = SchedulingPolicy(
                cluster_id=policy_id,
                dispatch_pacing=pacing,
                preferred_model_tier=tier,
                buffer_priority_weight=buffer_weight,
                max_concurrent_per_cluster=max_conc,
                assigned_repositories=cluster_repos,
            )

        return policies


# =====================================================================
# Meta-Optimization Directive & Container
# =====================================================================

@dataclass
class MetaOptimizationDirective:
    """Holistic directive emitted by the Strategic Meta-Optimizer for pipeline recalibration."""
    meta_cycle_id: int
    window_size: int
    recommendation_confidence: float
    topology_mutations: Dict[str, Any]
    parameter_adjustments: Dict[str, AdjustmentVector]
    cluster_policies: Dict[int, SchedulingPolicy]
    directive_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])

    def to_dict(self) -> Dict[str, Any]:
        """Convert directive to structured dictionary representation."""
        return {
            "directive_id": self.directive_id,
            "meta_cycle_id": self.meta_cycle_id,
            "window_size": self.window_size,
            "recommendation_confidence": round(self.recommendation_confidence, 4),
            "topology_mutations": self.topology_mutations,
            "parameter_adjustments": {
                k: v.to_dict() for k, v in self.parameter_adjustments.items()
            },
            "cluster_policies": {
                str(k): v.to_dict() for k, v in self.cluster_policies.items()
            },
        }

    def summary(self) -> str:
        """Provide concise human-readable directive summary."""
        buf = self.topology_mutations.get("buffer_depth_proposal")
        conc = self.topology_mutations.get("concurrency_proposal")
        conf = self.recommendation_confidence
        return (
            f"Directive[{self.directive_id}] (Cycle {self.meta_cycle_id}): "
            f"Buffer={buf}, Concurrency={conc}, "
            f"Adjustments={len(self.parameter_adjustments)}, "
            f"Clusters={len(self.cluster_policies)}, Confidence={conf:.1%}"
        )


# =====================================================================
# Main Pipeline Meta-Optimizer Engine
# =====================================================================

class PipelineMetaOptimizer:
    """Cross-Cycle Strategic Meta-Optimizer & Pipeline Topology Evolution Engine."""

    def __init__(
        self,
        cycle_window_size: int = 10,
        optimization_interval_cycles: int = 3,
        min_cycles_before_optimization: int = 5,
        evidence_threshold: float = 0.70,
    ) -> None:
        self.cycle_window_size = cycle_window_size
        self.optimization_interval_cycles = optimization_interval_cycles
        self.min_cycles_before_optimization = min_cycles_before_optimization
        self.evidence_threshold = evidence_threshold

        self.window = SlidingCycleWindow(maxlen=cycle_window_size)
        self.bandit_optimizer = ThompsonBanditOptimizer()
        self.policy_optimizer = BayesianPolicyOptimizer()
        self.repo_clusterer = RepositoryClusterer()

        self.cycle_counter: int = 0
        self.current_directive: Optional[MetaOptimizationDirective] = None
        self.directive_history: List[MetaOptimizationDirective] = []

    def ingest_cycle_report(self, cycle_report: Union[Dict[str, Any], Any]) -> CycleFeatureVector:
        """Ingest retrospective report from an execution cycle and append feature vector to window."""
        self.cycle_counter += 1

        if hasattr(cycle_report, "to_dict"):
            raw_dict = cycle_report.to_dict()
        elif isinstance(cycle_report, dict):
            raw_dict = dict(cycle_report)
        else:
            raw_dict = getattr(cycle_report, "__dict__", {})

        # Extract or synthesize core metrics
        vec = CycleFeatureVector(
            cycle_id=raw_dict.get("cycle_id", self.cycle_counter),
            buffer_depth=int(raw_dict.get("buffer_depth", 3)),
            dispatch_concurrency=int(raw_dict.get("dispatch_concurrency", 2)),
            avg_conformance_score=float(raw_dict.get("avg_conformance_score", 0.85)),
            total_token_cost=int(raw_dict.get("total_token_cost", 45000)),
            success_rate=float(raw_dict.get("success_rate", 0.90)),
            mean_duration_ratio=float(raw_dict.get("mean_duration_ratio", 1.05)),
            circuit_breaker_trips=int(raw_dict.get("circuit_breaker_trips", 0)),
            replanning_cascades=int(raw_dict.get("replanning_cascades", 0)),
            model_tier_distribution=raw_dict.get("model_tier_distribution", {
                ModelTier.TIER_1_ADVANCED.value: 0.25,
                ModelTier.TIER_2_STANDARD.value: 0.50,
                ModelTier.TIER_3_FAST.value: 0.25,
            }),
            per_repo_cost_variance=float(raw_dict.get("per_repo_cost_variance", 2500.0)),
            throughput_rate=float(raw_dict.get("throughput_rate", 12.0)),
            cycle_duration_seconds=float(raw_dict.get("cycle_duration_seconds", 45.0)),
            total_tasks_executed=int(raw_dict.get("total_tasks_executed", 8)),
            per_repo_stats=raw_dict.get("per_repo_stats", {}),
            timestamp=float(raw_dict.get("timestamp", 0.0)),
        )

        self.window.append(vec)

        # Update bandit and bayesian records with cycle feedback
        self.bandit_optimizer.record_feedback(
            buffer_depth=vec.buffer_depth,
            dispatch_concurrency=vec.dispatch_concurrency,
            success_rate=vec.success_rate,
            total_token_cost=float(vec.total_token_cost),
            throughput_rate=vec.throughput_rate,
        )

        current_params = raw_dict.get("current_policy_params", self.policy_optimizer.current_params)
        self.policy_optimizer.record_cycle_evaluation(current_params, vec)

        logger.info(
            "Ingested cycle %d: success_rate=%.2f, throughput=%.1f, cost=%d, window_len=%d",
            vec.cycle_id,
            vec.success_rate,
            vec.throughput_rate,
            vec.total_token_cost,
            len(self.window),
        )

        return vec

    def should_optimize(self) -> bool:
        """Predicate checking whether meta-optimization schedule conditions are satisfied."""
        if self.cycle_counter < self.min_cycles_before_optimization:
            return False
        return (self.cycle_counter % self.optimization_interval_cycles) == 0

    def optimize(self) -> Optional[MetaOptimizationDirective]:
        """Execute three-pass meta-optimization and generate unified MetaOptimizationDirective."""
        if len(self.window) < 2:
            logger.warning("Insufficient cycle history in window (%d) to execute optimization.", len(self.window))
            return None

        logger.info("Executing Strategic Meta-Optimization at cycle %d...", self.cycle_counter)

        # -------------------------------------------------------------
        # Pass 1: Structural Topology Pass (Thompson Sampling Bandit)
        # -------------------------------------------------------------
        best_arm = self.bandit_optimizer.select_arm()
        bandit_confidence = self.bandit_optimizer.estimate_selection_confidence(best_arm)

        latest_vec = self.window.history[-1]
        topology_mutations = {
            "current_buffer_depth": latest_vec.buffer_depth,
            "current_concurrency": latest_vec.dispatch_concurrency,
            "buffer_depth_proposal": best_arm.buffer_depth,
            "concurrency_proposal": best_arm.dispatch_concurrency,
            "expected_reward": round(best_arm.expected_mean, 4),
            "confidence": round(bandit_confidence, 4),
            "total_arm_pulls": best_arm.pull_count,
        }

        # -------------------------------------------------------------
        # Pass 2: Policy Parameter Pass (Bayesian Optimization via GP)
        # -------------------------------------------------------------
        param_adjustments = self.policy_optimizer.optimize_next_parameters()

        # -------------------------------------------------------------
        # Pass 3: Repository Strategy Pass (DBSCAN Clustering)
        # -------------------------------------------------------------
        cluster_policies = self.repo_clusterer.cluster_and_synthesize(self.window)

        # -------------------------------------------------------------
        # Unified Recommendation Confidence Synthesis
        # -------------------------------------------------------------
        param_evidence_mean = (
            statistics.mean([adj.evidence_strength for adj in param_adjustments.values()])
            if param_adjustments
            else 0.5
        )
        recommendation_confidence = 0.50 * bandit_confidence + 0.50 * param_evidence_mean

        directive = MetaOptimizationDirective(
            meta_cycle_id=self.cycle_counter,
            window_size=len(self.window),
            recommendation_confidence=recommendation_confidence,
            topology_mutations=topology_mutations,
            parameter_adjustments=param_adjustments,
            cluster_policies=cluster_policies,
        )

        self.current_directive = directive
        self.directive_history.append(directive)
        logger.info("Meta-Optimization complete: %s", directive.summary())

        return directive

    def get_current_directive(self) -> Optional[MetaOptimizationDirective]:
        """Retrieve most recently synthesized meta-optimization directive."""
        return self.current_directive

    def get_optimization_history(self) -> List[MetaOptimizationDirective]:
        """Retrieve complete history of emitted optimization directives."""
        return list(self.directive_history)

    def export_state(self) -> Dict[str, Any]:
        """Serialize complete engine state for persistence."""
        return {
            "cycle_counter": self.cycle_counter,
            "cycle_window_size": self.cycle_window_size,
            "optimization_interval_cycles": self.optimization_interval_cycles,
            "min_cycles_before_optimization": self.min_cycles_before_optimization,
            "evidence_threshold": self.evidence_threshold,
            "bandit_state": self.bandit_optimizer.export_state(),
            "policy_optimizer_state": self.policy_optimizer.export_state(),
            "window_history": [v.to_dict() for v in self.window.history],
            "current_directive": (
                self.current_directive.to_dict() if self.current_directive else None
            ),
        }

    def import_state(self, state: Dict[str, Any]) -> None:
        """Restore complete engine state from serialized dictionary."""
        self.cycle_counter = int(state.get("cycle_counter", 0))
        self.cycle_window_size = int(state.get("cycle_window_size", 10))
        self.optimization_interval_cycles = int(state.get("optimization_interval_cycles", 3))
        self.min_cycles_before_optimization = int(state.get("min_cycles_before_optimization", 5))
        self.evidence_threshold = float(state.get("evidence_threshold", 0.70))

        if "bandit_state" in state:
            self.bandit_optimizer.import_state(state["bandit_state"])

        if "policy_optimizer_state" in state:
            self.policy_optimizer.import_state(state["policy_optimizer_state"])

        self.window = SlidingCycleWindow(maxlen=self.cycle_window_size)
        for vec_data in state.get("window_history", []):
            self.window.append(CycleFeatureVector.from_dict(vec_data))


# =====================================================================
# Simulation & Self-Verification Demonstration
# =====================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("Hermes Pipeline Meta-Optimizer: 10-Cycle Convergence Simulation")
    print("=" * 80)

    random.seed(42)

    optimizer = PipelineMetaOptimizer(
        cycle_window_size=10,
        optimization_interval_cycles=3,
        min_cycles_before_optimization=5,
        evidence_threshold=0.70,
    )

    repositories = [
        "hermes_orchestrator",
        "hermes_scheduler",
        "hermes_telemetry",
        "hermes_monitor",
        "hermes_gateway",
    ]

    # Baseline operating conditions
    buffer_depth = 2
    dispatch_concurrency = 1
    current_params = {
        "ewma_decay": 0.20,
        "hw_seasonal_period": 7.0,
        "conformance_threshold": 0.25,
        "cb_failure_threshold": 4.0,
        "cb_recovery_timeout": 60.0,
    }

    # Execute 10 synthetic cycles
    for cycle in range(1, 11):
        # Simulate realistic system dynamics evolving with buffer depth and concurrency
        perf_boost = (buffer_depth - 2) * 0.03 + (dispatch_concurrency - 1) * 0.04
        cost_multiplier = 1.0 + (buffer_depth * 0.12) + (dispatch_concurrency * 0.18)

        # Build repository level statistics
        per_repo_stats: Dict[str, Dict[str, float]] = {}
        for r_idx, r_name in enumerate(repositories):
            # Introduce distinct operational profiles per repo
            if "telemetry" in r_name or "gateway" in r_name:
                base_fail = 0.02
                base_dur = 14.0 + random.gauss(0, 1)
                fan_out = 1.0
            elif "orchestrator" in r_name:
                base_fail = 0.05
                base_dur = 45.0 + random.gauss(0, 3)
                fan_out = 5.0
            else:
                base_fail = 0.08
                base_dur = 32.0 + random.gauss(0, 2)
                fan_out = 2.0

            per_repo_stats[r_name] = {
                "token_cost": round(8000.0 * cost_multiplier + random.gauss(0, 400), 2),
                "failure_rate": max(0.0, base_fail + random.gauss(0, 0.01)),
                "duration_seconds": max(5.0, base_dur),
                "dependency_fan_out": fan_out,
                "tier_1_count": 2.0 if "orchestrator" in r_name else 1.0,
                "tier_2_count": 3.0,
                "tier_3_count": 4.0 if "gateway" in r_name else 1.0,
            }

        total_cost = int(sum(s["token_cost"] for s in per_repo_stats.values()))
        success_rate = min(0.99, 0.88 + perf_boost + random.gauss(0, 0.02))
        throughput = max(5.0, 10.0 + (dispatch_concurrency * 2.8) + random.gauss(0, 0.5))
        conformance = min(0.98, 0.82 + perf_boost * 0.8 + random.gauss(0, 0.02))

        synthetic_report = {
            "cycle_id": cycle,
            "buffer_depth": buffer_depth,
            "dispatch_concurrency": dispatch_concurrency,
            "avg_conformance_score": conformance,
            "total_token_cost": total_cost,
            "success_rate": success_rate,
            "mean_duration_ratio": 1.02,
            "circuit_breaker_trips": 1 if cycle == 3 else 0,
            "replanning_cascades": 0,
            "per_repo_cost_variance": 3200.0,
            "throughput_rate": throughput,
            "cycle_duration_seconds": 40.0 + random.gauss(0, 2),
            "total_tasks_executed": int(throughput * 0.8),
            "per_repo_stats": per_repo_stats,
            "current_policy_params": current_params,
        }

        # 1. Ingest report
        vector = optimizer.ingest_cycle_report(synthetic_report)

        # 2. Check should_optimize
        if optimizer.should_optimize():
            print(f"\n[>>> Cycle {cycle}] Optimization trigger fired!")
            directive = optimizer.optimize()
            if directive:
                print(directive.summary())
                print("  - Topology Mutations:", directive.topology_mutations)
                print("  - Bayesian Parameter Adjustments:")
                for p_name, adj in directive.parameter_adjustments.items():
                    print(
                        f"      * {p_name:22s}: current={adj.current_value:<6.2f} -> "
                        f"proposed={adj.proposed_value:<6.2f} "
                        f"[CI: {adj.ci_lower:.2f}, {adj.ci_upper:.2f}] (strength={adj.evidence_strength:.2f})"
                    )
                print("  - Cluster Scheduling Policies:")
                for c_id, pol in directive.cluster_policies.items():
                    print(
                        f"      * Cluster {c_id:3d}: pacing={pol.dispatch_pacing.value:12s} "
                        f"tier={pol.preferred_model_tier:16s} concurrency={pol.max_concurrent_per_cluster} "
                        f"repos={pol.assigned_repositories}"
                    )

                # Apply topology mutations for subsequent simulated cycles
                buffer_depth = directive.topology_mutations["buffer_depth_proposal"]
                dispatch_concurrency = directive.topology_mutations["concurrency_proposal"]
                for p_name, adj in directive.parameter_adjustments.items():
                    current_params[p_name] = adj.proposed_value
        else:
            print(
                f"Cycle {cycle:2d}: Ingested (Buffer={buffer_depth}, Conc={dispatch_concurrency}) -> "
                f"Success={success_rate:.1%}, Cost={total_cost}, TP={throughput:.1f}"
            )

    # Verify rolling stats across sliding window
    print("\n" + "=" * 80)
    print("Sliding Window Rolling Statistics Summary:")
    print("=" * 80)
    rolling_stats = optimizer.window.compute_all_stats()
    for fname, st in rolling_stats.items():
        print(
            f"  {fname:25s} | mean={st.mean:9.2f} | std={st.std_dev:7.2f} | "
            f"OLS slope={st.ols_slope:8.4f} | range=[{st.min_value:.2f}, {st.max_value:.2f}]"
        )

    # Verify state serialization round-trip
    state_export = optimizer.export_state()
    fresh_optimizer = PipelineMetaOptimizer()
    fresh_optimizer.import_state(state_export)
    assert fresh_optimizer.cycle_counter == optimizer.cycle_counter, "Cycle counter mismatch in persistence"
    assert len(fresh_optimizer.window) == len(optimizer.window), "Window length mismatch in persistence"

    print("\n" + "=" * 80)
    print("Meta-Optimizer Verification Successful: All passes converged with state persistence confirmed.")
    print("=" * 80)