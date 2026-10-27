"""
Pipeline Simulation Sandbox & Counterfactual Replay Engine.

This module provides a discrete event simulation (DES) sandbox that replays
historical orchestration episodes under counterfactual parameter configurations.
It enables risk-free policy and capacity verification prior to live canary or
production deployment in the Hermes Orchestrator pipeline.
"""

from __future__ import annotations

import collections
from dataclasses import asdict, dataclass, field
import json
import logging
import math
import random
import statistics
import threading
import time
from typing import Any, Dict, List, Optional, Tuple, Union

# Module logger configuration
logger = logging.getLogger("SimulationSandbox")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] [SimulationSandbox] %(message)s")
    )
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)

# Optional Observability Fabric / Phoenix Tracer integration
try:
    from phoenix_tracer import SpanKind, tracer  # type: ignore
except ImportError:
    tracer = None

    class _MockSpanKind:
        SIMULATION = "SIMULATION"
        INTERNAL = "INTERNAL"

    SpanKind = _MockSpanKind()  # type: ignore


class _NoOpSpan:
    """Fallback span context manager when Phoenix tracer is unavailable."""

    def __enter__(self) -> "_NoOpSpan":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass

    def set_attribute(self, key: str, value: Any) -> None:
        pass


def _trace_span(name: str, attributes: Optional[Dict[str, Any]] = None):
    """Context manager helper for Phoenix tracing with zero external dependencies."""
    if tracer is not None and hasattr(tracer, "start_as_current_span"):
        span_kind = getattr(SpanKind, "SIMULATION", "SIMULATION")
        span = tracer.start_as_current_span(name, kind=span_kind)
        if attributes:
            for k, v in attributes.items():
                try:
                    span.set_attribute(k, v)
                except Exception:
                    pass
        return span
    return _NoOpSpan()


# ---------------------------------------------------------------------------
# 1. Episode Tuple Definition
# ---------------------------------------------------------------------------
EpisodeTuple = collections.namedtuple(
    "EpisodeTuple",
    ["state", "action", "reward", "timestamp", "metadata"],
)
EpisodeTuple.__doc__ = (
    "Represents one completed orchestration cycle trajectory.\n"
    "Attributes:\n"
    "    state (dict): Capacity envelope dimensions and baseline metrics.\n"
    "    action (dict): Active governance parameters during the cycle.\n"
    "    reward (float): Composite utility score achieved.\n"
    "    timestamp (float): UNIX epoch timestamp of episode completion.\n"
    "    metadata (dict): Granular event execution logs, token counts, tasks."
)


# ---------------------------------------------------------------------------
# 2. Episode Archive (Circular Buffer)
# ---------------------------------------------------------------------------
class EpisodeArchive:
    """Thread-safe circular buffer for recording and querying orchestration episodes."""

    def __init__(self, maxlen: int = 500) -> None:
        self._maxlen = maxlen
        self._archive: collections.deque[EpisodeTuple] = collections.deque(maxlen=maxlen)
        self._lock = threading.Lock()

    def record_episode(self, episode: EpisodeTuple) -> None:
        """Appends an EpisodeTuple to the archive in a thread-safe manner."""
        with self._lock:
            self._archive.append(episode)
            logger.debug(
                "Recorded episode at ts=%.3f, current buffer size=%d",
                episode.timestamp,
                len(self._archive),
            )

    def get_episodes(self, window_seconds: Optional[float] = None) -> List[EpisodeTuple]:
        """Returns a snapshot of episodes, optionally filtered by historical time window."""
        with self._lock:
            if not self._archive:
                return []
            if window_seconds is None:
                return list(self._archive)

            now = time.time()
            latest_ts = max((ep.timestamp for ep in self._archive), default=now)
            effective_now = max(now, latest_ts)
            cutoff = effective_now - window_seconds
            return [ep for ep in self._archive if ep.timestamp >= cutoff]

    def get_arrival_rate_params(self) -> Tuple[float]:
        """Estimates task arrival rate parameter (lambda_hat) via EWMA of inter-arrival times."""
        with self._lock:
            if len(self._archive) < 2:
                return (1.0,)

            sorted_eps = sorted(self._archive, key=lambda x: x.timestamp)
            inter_arrivals = [
                max(sorted_eps[i].timestamp - sorted_eps[i - 1].timestamp, 0.001)
                for i in range(1, len(sorted_eps))
            ]

            alpha = 0.2
            ewma_dt = inter_arrivals[0]
            for dt in inter_arrivals[1:]:
                ewma_dt = alpha * dt + (1.0 - alpha) * ewma_dt

            lambda_hat = 1.0 / max(ewma_dt, 0.001)
            return (float(lambda_hat),)

    def get_token_consumption_params(self) -> Tuple[float, float]:
        """Estimates (mu_hat, sigma_hat) for token consumption via Log-Normal distribution fitting."""
        with self._lock:
            tokens: List[float] = []
            for ep in self._archive:
                tok = (
                    ep.metadata.get("tokens")
                    or ep.metadata.get("tokens_consumed")
                    or ep.state.get("tokens")
                    or ep.state.get("token_burn")
                )
                if tok is not None and float(tok) > 0.0:
                    tokens.append(float(tok))

            if len(tokens) < 2:
                # Default parameters: median ~1500 tokens (exp(7.31) ~ 1500)
                return (math.log(1500.0), 0.35)

            log_tokens = [math.log(t) for t in tokens]
            mu_hat = statistics.mean(log_tokens)
            sigma_hat = statistics.stdev(log_tokens) if len(log_tokens) > 1 else 0.35
            return (float(mu_hat), max(float(sigma_hat), 1e-4))

    def __len__(self) -> int:
        with self._lock:
            return len(self._archive)


# ---------------------------------------------------------------------------
# 3. Discrete Event Simulation (DES) Kernel
# ---------------------------------------------------------------------------
class DESKernel:
    """Discrete Event Simulation kernel stepping through task arrivals and completions.

    Evaluates counterfactual governance parameters (worker_pool_size, quota_weights,
    circuit_breaker_thresholds, rate_governor_spacing) against an empirical trajectory.
    """

    def __init__(
        self,
        weight_throughput: float = 0.4,
        weight_cost: float = 0.3,
        weight_safety: float = 0.3,
        amdahl_parallel_fraction: float = 0.85,
    ) -> None:
        self.w1 = weight_throughput
        self.w2 = weight_cost
        self.w3 = weight_safety
        self.p_amdahl = amdahl_parallel_fraction

    def _amdahl_speedup(self, workers: float) -> float:
        """Calculates theoretical speedup factor using Amdahl's law."""
        w = max(1.0, float(workers))
        return 1.0 / ((1.0 - self.p_amdahl) + (self.p_amdahl / w))

    def simulate_episode(
        self,
        episode: EpisodeTuple,
        params: Dict[str, Any],
        perturbed_events: Optional[List[Dict[str, float]]] = None,
    ) -> float:
        """Replays an episode trajectory under counterfactual parameters.

        Computes simulated reward:
            R = w1 * throughput_normalized + w2 * cost_efficiency + w3 * anomaly_free_ratio
        """
        # Baseline parameter extraction
        base_workers = float(
            episode.action.get("worker_pool_size")
            or episode.state.get("worker_pool_size")
            or 4.0
        )
        cf_workers = float(params.get("worker_pool_size", base_workers))

        base_speedup = self._amdahl_speedup(base_workers)
        cf_speedup = self._amdahl_speedup(cf_workers)
        speedup_factor = cf_speedup / max(base_speedup, 1e-6)

        # Rate governor spacing
        rate_spacing = float(
            params.get(
                "rate_governor_spacing",
                episode.action.get("rate_governor_spacing", 0.05),
            )
        )

        # Quota weights for token consumption
        raw_quota = params.get(
            "quota_weights",
            episode.action.get("quota_weights", 1.0),
        )
        if isinstance(raw_quota, dict):
            quota_multiplier = float(
                sum(raw_quota.values()) / max(len(raw_quota), 1)
            )
        else:
            quota_multiplier = float(raw_quota)

        # Circuit breaker error threshold
        raw_cb = params.get(
            "circuit_breaker_thresholds",
            episode.action.get("circuit_breaker_thresholds", 0.8),
        )
        if isinstance(raw_cb, dict):
            cb_threshold = float(sum(raw_cb.values()) / max(len(raw_cb), 1))
        else:
            cb_threshold = float(raw_cb)

        # Discrete task events reconstruction or perturbation
        if perturbed_events is not None:
            tasks = perturbed_events
        else:
            tasks = episode.metadata.get("events") or []
            if not tasks:
                count = int(episode.state.get("task_count", 10))
                base_tokens = float(
                    episode.metadata.get("tokens")
                    or episode.state.get("tokens", 1500.0)
                ) / max(count, 1)
                base_duration = float(episode.state.get("mean_duration", 1.0))
                tasks = [
                    {
                        "inter_arrival": 0.2,
                        "tokens": base_tokens,
                        "duration": base_duration,
                    }
                    for _ in range(count)
                ]

        # Discrete Event Step-Through
        current_time = 0.0
        dispatch_time = 0.0
        num_workers = max(1, int(round(cf_workers)))
        worker_free_times = [0.0] * num_workers
        total_tokens = 0.0
        anomalies_detected = 0

        for task in tasks:
            dt = task.get("inter_arrival", 0.1)
            current_time += dt

            # Apply rate governor spacing constraint
            dispatch_time = max(current_time, dispatch_time + rate_spacing)

            # Assign to earliest available worker
            worker_idx = min(
                range(num_workers), key=lambda i: worker_free_times[i]
            )
            start_time = max(dispatch_time, worker_free_times[worker_idx])

            # Service time scales via Amdahl's law
            base_dur = task.get("duration", 1.0)
            service_time = base_dur / max(speedup_factor, 1e-4)
            finish_time = start_time + service_time
            worker_free_times[worker_idx] = finish_time

            # Token burn adjusted by quota policy
            task_tokens = task.get("tokens", 150.0) * quota_multiplier
            total_tokens += task_tokens

            # Anomaly & saturation tracking (latency / queue depth)
            queue_delay = start_time - dispatch_time
            if queue_delay > (cb_threshold * 2.5):
                anomalies_detected += 1

        total_elapsed = max(max(worker_free_times), current_time, 0.1)
        completed_tasks = len(tasks)

        # 1. Normalized throughput
        simulated_throughput = completed_tasks / total_elapsed
        target_throughput = 15.0
        throughput_norm = min(1.0, simulated_throughput / target_throughput)

        # 2. Cost efficiency (inversely proportional to simulated token burn)
        ref_tokens = 2500.0
        cost_efficiency = ref_tokens / (ref_tokens + max(total_tokens, 1.0))

        # 3. Anomaly-free duration ratio derived from circuit breaker headroom
        error_rate = anomalies_detected / max(completed_tasks, 1)
        headroom = (cb_threshold - error_rate) / max(cb_threshold, 1e-4)
        if headroom >= 0.0:
            anomaly_free_ratio = min(1.0, max(0.0, 0.75 + 0.25 * math.tanh(headroom * 2.0)))
        else:
            anomaly_free_ratio = max(0.0, 0.75 + 0.75 * headroom)

        # Composite utility function
        composite_reward = (
            self.w1 * throughput_norm
            + self.w2 * cost_efficiency
            + self.w3 * anomaly_free_ratio
        )
        return float(min(1.0, max(0.0, composite_reward)))


DiscreteEventReplayEngine = DESKernel


# ---------------------------------------------------------------------------
# 4. Monte Carlo Counterfactual Replay Engine
# ---------------------------------------------------------------------------
@dataclass
class ReplayResult:
    """Statistical summary of Monte Carlo replay trajectories."""

    rewards: List[float]
    mean: float
    stdev: float
    percentile_5: float
    percentile_95: float


class MonteCarloReplayEngine:
    """Executes N stochastic counterfactual replays without external scientific libraries."""

    def __init__(self, kernel: Optional[DESKernel] = None) -> None:
        self.kernel = kernel or DESKernel()

    def run_replays(
        self,
        episodes: List[EpisodeTuple],
        params: Dict[str, Any],
        arrival_lambda: float,
        token_mu: float,
        token_sigma: float,
        n_replays: int = 200,
        task_count_per_replay: int = 15,
    ) -> ReplayResult:
        """Executes n_replays with Exponential arrivals and Log-Normal token consumption."""
        if not episodes:
            logger.warning("Empty episode trajectory provided to MonteCarloReplayEngine.")
            return ReplayResult([], 0.0, 0.0, 0.0, 0.0)

        rewards: List[float] = []
        rng = random.Random(42)

        for _ in range(n_replays):
            base_episode = rng.choice(episodes)

            # Generate stochastically perturbed task arrivals and token payloads
            perturbed_events: List[Dict[str, float]] = []
            for _ in range(task_count_per_replay):
                inter_arr = rng.expovariate(max(arrival_lambda, 1e-4))
                toks = rng.lognormvariate(token_mu, max(token_sigma, 1e-4))
                dur = max(0.05, rng.gauss(0.8, 0.2))
                perturbed_events.append(
                    {
                        "inter_arrival": inter_arr,
                        "tokens": toks,
                        "duration": dur,
                    }
                )

            r_sim = self.kernel.simulate_episode(
                base_episode,
                params,
                perturbed_events=perturbed_events,
            )
            rewards.append(r_sim)

        rewards_sorted = sorted(rewards)
        mean_r = float(statistics.mean(rewards_sorted))
        stdev_r = float(statistics.stdev(rewards_sorted)) if len(rewards_sorted) > 1 else 0.0

        p5_idx = int(0.05 * (len(rewards_sorted) - 1))
        p95_idx = int(0.95 * (len(rewards_sorted) - 1))
        p5 = float(rewards_sorted[p5_idx])
        p95 = float(rewards_sorted[p95_idx])

        return ReplayResult(
            rewards=rewards_sorted,
            mean=mean_r,
            stdev=stdev_r,
            percentile_5=p5,
            percentile_95=p95,
        )


# ---------------------------------------------------------------------------
# 5. Counterfactual Improvement Gate (Statistical Hypothesis Testing)
# ---------------------------------------------------------------------------
def _betacf(a: float, b: float, x: float, max_iter: int = 150, eps: float = 1e-12) -> float:
    """Evaluates continued fraction for the regularized incomplete beta function."""
    qab = a + b
    qap = a + 1.0
    qam = a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    if abs(d) < 1e-30:
        d = 1e-30
    d = 1.0 / d
    h = d

    for m in range(1, max_iter + 1):
        m2 = 2 * m
        # Even step
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        if abs(d) < 1e-30:
            d = 1e-30
        c = 1.0 + aa / c
        if abs(c) < 1e-30:
            c = 1e-30
        d = 1.0 / d
        h *= d * c

        # Odd step
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        if abs(d) < 1e-30:
            d = 1e-30
        c = 1.0 + aa / c
        if abs(c) < 1e-30:
            c = 1e-30
        d = 1.0 / d
        del_val = d * c
        h *= del_val
        if abs(del_val - 1.0) < eps:
            break
    return h


def _incbeta(a: float, b: float, x: float) -> float:
    """Computes the regularized incomplete beta function I_x(a, b)."""
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0

    lbeta = math.lgamma(a) + math.lgamma(b) - math.lgamma(a + b)
    bt = math.exp(math.log(x) * a + math.log(1.0 - x) * b - lbeta)

    if x < (a + 1.0) / (a + b + 2.0):
        return bt * _betacf(a, b, x) / a
    else:
        return 1.0 - bt * _betacf(b, a, 1.0 - x) / b


def _student_t_sf(t: float, df: float) -> float:
    """Survival function P(T >= t) for Student's t distribution with df degrees of freedom."""
    if df <= 0.0:
        return 0.5
    if math.isinf(df) or df > 1000.0:
        return 0.5 * math.erfc(t / math.sqrt(2.0))

    x = df / (df + t * t)
    prob = 0.5 * _incbeta(0.5 * df, 0.5, x)
    if t >= 0.0:
        return prob
    else:
        return 1.0 - prob


class CounterfactualImprovementGate:
    """Performs Welch's one-sided t-test and Cohen's d effect size validation."""

    @staticmethod
    def evaluate(
        proposed_rewards: List[float],
        baseline_rewards: List[float],
        alpha: float = 0.05,
        min_effect_size: float = 0.2,
    ) -> Dict[str, Any]:
        """Evaluates statistical significance and practical effect size.

        Null Hypothesis H0: mu_proposed <= mu_baseline
        Alternative H1: mu_proposed > mu_baseline
        """
        n1 = len(proposed_rewards)
        n2 = len(baseline_rewards)

        if n1 < 2 or n2 < 2:
            return {
                "t_statistic": 0.0,
                "p_value": 1.0,
                "cohens_d": 0.0,
                "passes_significance": False,
                "passes_effect_size": False,
                "approved": False,
            }

        m1 = statistics.mean(proposed_rewards)
        m2 = statistics.mean(baseline_rewards)
        v1 = statistics.variance(proposed_rewards)
        v2 = statistics.variance(baseline_rewards)

        se1 = v1 / n1
        se2 = v2 / n2
        se_diff = math.sqrt(se1 + se2)

        # Welch's t-statistic
        if se_diff > 1e-12:
            t_stat = (m1 - m2) / se_diff
        else:
            t_stat = 100.0 if m1 > m2 else (-100.0 if m1 < m2 else 0.0)

        # Welch-Satterthwaite degrees of freedom
        denom_df = 0.0
        if n1 > 1 and se1 > 0:
            denom_df += (se1 * se1) / (n1 - 1)
        if n2 > 1 and se2 > 0:
            denom_df += (se2 * se2) / (n2 - 1)

        if denom_df > 1e-12:
            df = ((se1 + se2) ** 2) / denom_df
        else:
            df = float(n1 + n2 - 2)

        # One-sided p-value: P(T >= t_stat)
        p_val = _student_t_sf(t_stat, df)

        # Cohen's d effect size computation
        pooled_var = (((n1 - 1) * v1) + ((n2 - 1) * v2)) / max(n1 + n2 - 2, 1)
        s_pooled = math.sqrt(max(pooled_var, 0.0))
        if s_pooled > 1e-12:
            cohens_d = (m1 - m2) / s_pooled
        else:
            cohens_d = 0.0

        passes_sig = bool(p_val < alpha and t_stat > 0.0)
        passes_eff = bool(cohens_d >= min_effect_size)
        approved = bool(passes_sig and passes_eff)

        return {
            "t_statistic": float(t_stat),
            "p_value": float(p_val),
            "cohens_d": float(cohens_d),
            "passes_significance": passes_sig,
            "passes_effect_size": passes_eff,
            "approved": approved,
        }


WelchHypothesisValidator = CounterfactualImprovementGate


# ---------------------------------------------------------------------------
# 6. Sensitivity Analyzer (Finite Differences)
# ---------------------------------------------------------------------------
class SensitivityAnalyzer:
    """Computes partial reward derivatives and curvature via finite difference perturbation."""

    @staticmethod
    def analyze(
        baseline_params: Dict[str, Any],
        replay_engine: MonteCarloReplayEngine,
        episodes: List[EpisodeTuple],
        arrival_lambda: float,
        token_mu: float,
        token_sigma: float,
        epsilon: float = 0.01,
        n_eval_replays: int = 50,
    ) -> Dict[str, Dict[str, Union[float, str]]]:
        """Calculates gradient, curvature, and stability for each governance parameter."""
        results: Dict[str, Dict[str, Union[float, str]]] = {}

        # Baseline evaluation
        base_res = replay_engine.run_replays(
            episodes=episodes,
            params=baseline_params,
            arrival_lambda=arrival_lambda,
            token_mu=token_mu,
            token_sigma=token_sigma,
            n_replays=n_eval_replays,
        )
        r0 = base_res.mean

        for param_key, param_val in baseline_params.items():
            if not isinstance(param_val, (int, float)):
                continue

            val = float(param_val)
            h = max(abs(val) * epsilon, epsilon)

            # Forward perturbation
            params_plus = dict(baseline_params)
            params_plus[param_key] = val + h
            res_plus = replay_engine.run_replays(
                episodes=episodes,
                params=params_plus,
                arrival_lambda=arrival_lambda,
                token_mu=token_mu,
                token_sigma=token_sigma,
                n_replays=n_eval_replays,
            )
            r_plus = res_plus.mean

            # Backward perturbation
            params_minus = dict(baseline_params)
            params_minus[param_key] = max(0.001, val - h)
            res_minus = replay_engine.run_replays(
                episodes=episodes,
                params=params_minus,
                arrival_lambda=arrival_lambda,
                token_mu=token_mu,
                token_sigma=token_sigma,
                n_replays=n_eval_replays,
            )
            r_minus = res_minus.mean

            # Partial derivative and second derivative
            gradient = (r_plus - r_minus) / (2.0 * h)
            curvature = (r_plus - 2.0 * r0 + r_minus) / (h * h)

            if curvature < -1e-4:
                stability = "stable"
            elif curvature > 1e-4:
                stability = "unstable"
            else:
                stability = "neutral"

            results[param_key] = {
                "gradient": float(round(gradient, 6)),
                "curvature": float(round(curvature, 6)),
                "stability": stability,
            }

        return results


SensitivityGradientAnalyzer = SensitivityAnalyzer


# ---------------------------------------------------------------------------
# 7. Simulation Report Dataclass
# ---------------------------------------------------------------------------
@dataclass
class SimulationReport:
    """Serializable evaluation report detailing counterfactual simulation findings."""

    predicted_reward_mean: float
    predicted_reward_std: float
    confidence_interval_5_95: Tuple[float, float]
    t_statistic: float
    p_value: float
    cohens_d: float
    improvement_approved: bool
    parameter_sensitivity: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_json(self, indent: int = 2) -> str:
        """Serializes the report to structured JSON format."""
        data = asdict(self)
        return json.dumps(data, indent=indent)


# ---------------------------------------------------------------------------
# 8. Simulation Sandbox Facade
# ---------------------------------------------------------------------------
class SimulationSandbox:
    """Main facade for historical counterfactual replay and governance simulation."""

    def __init__(
        self,
        archive: Optional[EpisodeArchive] = None,
        des_kernel: Optional[DESKernel] = None,
    ) -> None:
        self.archive = archive or EpisodeArchive()
        self.kernel = des_kernel or DESKernel()
        self.replay_engine = MonteCarloReplayEngine(self.kernel)
        self.gate = CounterfactualImprovementGate()
        self.analyzer = SensitivityAnalyzer()
        self._lock = threading.Lock()
        logger.info("SimulationSandbox initialized successfully.")

    def record_episode(self, episode: EpisodeTuple) -> None:
        """Thread-safe recording of an executed episode into the sandbox archive."""
        self.archive.record_episode(episode)

    def simulate_policy_proposal(
        self,
        proposed_params: Dict[str, Any],
        baseline_params: Dict[str, Any],
        n_replays: int = 200,
    ) -> SimulationReport:
        """Evaluates a proposed policy configuration against baseline parameters.

        Called by the Learning Engine before emitting DirectiveGovernor proposals.
        """
        span_attrs = {
            "simulation.type": "policy_proposal",
            "simulation.n_replays": n_replays,
        }
        with _trace_span("simulate_policy_proposal", span_attrs):
            logger.info(
                "Simulating policy proposal: proposed=%s vs baseline=%s (replays=%d)",
                proposed_params,
                baseline_params,
                n_replays,
            )

            episodes = self.archive.get_episodes(window_seconds=7200)
            if not episodes:
                episodes = self.archive.get_episodes()

            if not episodes:
                logger.warning("No archived episodes found; constructing fallback baseline episode.")
                episodes = [
                    EpisodeTuple(
                        state={"worker_pool_size": 4.0, "tokens": 1500.0, "task_count": 10},
                        action=baseline_params,
                        reward=0.65,
                        timestamp=time.time(),
                        metadata={"tokens": 1500.0},
                    )
                ]

            arrival_lambda = self.archive.get_arrival_rate_params()[0]
            token_mu, token_sigma = self.archive.get_token_consumption_params()

            # Run Monte Carlo simulations for proposed and baseline
            prop_res = self.replay_engine.run_replays(
                episodes=episodes,
                params=proposed_params,
                arrival_lambda=arrival_lambda,
                token_mu=token_mu,
                token_sigma=token_sigma,
                n_replays=n_replays,
            )

            base_res = self.replay_engine.run_replays(
                episodes=episodes,
                params=baseline_params,
                arrival_lambda=arrival_lambda,
                token_mu=token_mu,
                token_sigma=token_sigma,
                n_replays=n_replays,
            )

            # Hypothesis testing and effect size
            eval_metrics = self.gate.evaluate(
                proposed_rewards=prop_res.rewards,
                baseline_rewards=base_res.rewards,
            )

            # Sensitivity analysis around baseline parameters
            sensitivity = self.analyzer.analyze(
                baseline_params=baseline_params,
                replay_engine=self.replay_engine,
                episodes=episodes,
                arrival_lambda=arrival_lambda,
                token_mu=token_mu,
                token_sigma=token_sigma,
            )

            report = SimulationReport(
                predicted_reward_mean=prop_res.mean,
                predicted_reward_std=prop_res.stdev,
                confidence_interval_5_95=(prop_res.percentile_5, prop_res.percentile_95),
                t_statistic=eval_metrics["t_statistic"],
                p_value=eval_metrics["p_value"],
                cohens_d=eval_metrics["cohens_d"],
                improvement_approved=eval_metrics["approved"],
                parameter_sensitivity=sensitivity,
                metadata={
                    "baseline_mean": base_res.mean,
                    "baseline_std": base_res.stdev,
                    "n_replays": n_replays,
                    "episodes_sampled": len(episodes),
                    "passes_significance": eval_metrics["passes_significance"],
                    "passes_effect_size": eval_metrics["passes_effect_size"],
                },
            )

            logger.info(
                "Policy proposal simulation complete. Approved: %s (p=%.4f, d=%.3f, mean=%.3f)",
                report.improvement_approved,
                report.p_value,
                report.cohens_d,
                report.predicted_reward_mean,
            )
            return report

    def simulate_capacity_adjustment(
        self,
        proposed_snapshot: Dict[str, Any],
        current_snapshot: Dict[str, Any],
        n_replays: int = 200,
    ) -> SimulationReport:
        """Evaluates dynamic capacity adjustments prior to canary submission.

        Called by Capacity Governor to verify worker and quota envelopes.
        """
        span_attrs = {
            "simulation.type": "capacity_adjustment",
            "simulation.n_replays": n_replays,
        }
        with _trace_span("simulate_capacity_adjustment", span_attrs):
            logger.info(
                "Simulating capacity adjustment: proposed_snapshot=%s (replays=%d)",
                proposed_snapshot,
                n_replays,
            )
            return self.simulate_policy_proposal(
                proposed_params=proposed_snapshot,
                baseline_params=current_snapshot,
                n_replays=n_replays,
            )


# ---------------------------------------------------------------------------
# Module Exports
# ---------------------------------------------------------------------------
__all__ = [
    "EpisodeTuple",
    "EpisodeArchive",
    "DESKernel",
    "DiscreteEventReplayEngine",
    "ReplayResult",
    "MonteCarloReplayEngine",
    "CounterfactualImprovementGate",
    "WelchHypothesisValidator",
    "SensitivityAnalyzer",
    "SensitivityGradientAnalyzer",
    "SimulationReport",
    "SimulationSandbox",
]


# ---------------------------------------------------------------------------
# Self-Contained Execution & Validation Block
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 80)
    print("Pipeline Simulation Sandbox & Counterfactual Replay Engine — Demo Execution")
    print("=" * 80)

    # 1. Instantiate the Sandbox
    archive = EpisodeArchive(maxlen=200)
    sandbox = SimulationSandbox(archive=archive)

    # 2. Populate archive with 50 synthetic orchestration episodes
    sim_rng = random.Random(1337)
    base_ts = time.time() - 3600.0

    print("Populating archive with 50 synthetic episodes...")
    for idx in range(50):
        ts = base_ts + (idx * 60.0)
        workers = sim_rng.choice([4.0, 6.0, 8.0])
        token_burn = sim_rng.gauss(1500.0, 250.0)
        reward = sim_rng.uniform(0.60, 0.78)

        synthetic_events = [
            {
                "inter_arrival": sim_rng.expovariate(1.2),
                "tokens": token_burn / 10.0,
                "duration": sim_rng.uniform(0.4, 1.2),
            }
            for _ in range(10)
        ]

        ep = EpisodeTuple(
            state={
                "worker_pool_size": workers,
                "throughput": sim_rng.uniform(8.0, 14.0),
                "tokens": token_burn,
                "error_rate": sim_rng.uniform(0.01, 0.05),
                "task_count": 10,
                "mean_duration": 0.8,
            },
            action={
                "worker_pool_size": workers,
                "quota_weights": 1.0,
                "circuit_breaker_thresholds": 0.8,
                "rate_governor_spacing": 0.05,
            },
            reward=reward,
            timestamp=ts,
            metadata={
                "tokens": token_burn,
                "events": synthetic_events,
            },
        )
        sandbox.record_episode(ep)

    print(f"Archive populated with {len(sandbox.archive)} episodes.")

    # 3. Define counterfactual parameter proposals
    baseline_parameters = {
        "worker_pool_size": 4.0,
        "quota_weights": 1.0,
        "circuit_breaker_thresholds": 0.8,
        "rate_governor_spacing": 0.05,
    }

    # Proposed configuration: expanded workers and optimized governor spacing
    proposed_parameters = {
        "worker_pool_size": 8.0,
        "quota_weights": 0.95,
        "circuit_breaker_thresholds": 0.85,
        "rate_governor_spacing": 0.03,
    }

    # 4. Run counterfactual replay simulation
    print("\nRunning counterfactual replay simulation (N=200)...")
    report = sandbox.simulate_policy_proposal(
        proposed_params=proposed_parameters,
        baseline_params=baseline_parameters,
        n_replays=200,
    )

    # 5. Output generated SimulationReport JSON
    print("\n" + "=" * 40 + " SIMULATION REPORT " + "=" * 40)
    print(report.to_json(indent=2))
    print("=" * 99)
    print("Execution verified: Simulation sandbox completed successfully.")
    print("=" * 99)