"""
Hermes Orchestrator - Pipeline Autonomic Loop Calibration Monitor & Drift Detector
tasks/pipeline_calibration_monitor.py

Meta-observability layer that monitors the statistical calibration and health
of the autonomic control loop (Learning Engine -> Simulation Sandbox -> Experiment Controller).
"""

from __future__ import annotations

import json
import math
import os
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np


class LearningState(str, Enum):
    """Categorical convergence states for policy parameter optimization."""
    CONVERGING = "CONVERGING"
    STAGNANT = "STAGNANT"
    OSCILLATING = "OSCILLATING"
    DIVERGING = "DIVERGING"
    PLATEAU = "PLATEAU"


@dataclass
class CalibrationDriftAlert:
    """Alert emitted when CUSUM detects simulation-reality divergence."""
    timestamp: float
    magnitude: float
    direction: str  # 'optimistic' | 'pessimistic'
    kl_divergence: float
    cusum_value: float
    threshold: float
    message: str


@dataclass
class CalibrationDriftStatus:
    """Current state of simulation-reality prediction tracking."""
    ewma_delta: float
    cusum_upper: float
    cusum_lower: float
    is_drifting: bool
    drift_direction: str  # 'optimistic' | 'pessimistic' | 'none'
    kl_divergence: float
    sample_count: int
    sigma_delta: float
    active_alert: Optional[CalibrationDriftAlert] = None


@dataclass
class LearningConvergenceStatus:
    """Diagnostics for parameter trajectory and reward convergence."""
    state: LearningState
    last_l2_norm: float
    l2_norm_history: List[float]
    last_cosine_similarity: float
    cosine_similarity_history: List[float]
    reward_variance: float
    step_count: int
    message: str


@dataclass
class CalibrationBin:
    """Metrics for an individual calibration bucket in the ECE reliability diagram."""
    bin_index: int
    bin_lower: float
    bin_upper: float
    bin_center: float
    count: int
    accuracy: float
    confidence: float


@dataclass
class PromotionCalibration:
    """Audit ledger calibration statistics for promotion decisions."""
    brier_score: float
    ece: float
    total_decisions: int
    completed_evaluations: int
    calibration_curve: List[Dict[str, Any]]


@dataclass
class HealthIndex:
    """Autonomic Loop Health Index (ALHI) and constituent scores."""
    alhi: float
    component_scores: Dict[str, float]
    timestamp: float
    recommended_actions: List[str]


@dataclass
class CalibrationReport:
    """Comprehensive health and calibration telemetry report for the autonomic loop."""
    timestamp: float
    health_index: HealthIndex
    simulation_drift: CalibrationDriftStatus
    learning_convergence: LearningConvergenceStatus
    promotion_calibration: PromotionCalibration
    recommended_actions: List[str]
    summary: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert report to JSON-serializable dictionary."""
        return asdict(self)


class SimulationRealityDivergenceTracker:
    """
    Tracks discrepancy between Monte Carlo simulation predictions and real-world outcomes.
    
    Implements:
    - Signed prediction error delta = R_observed - R_predicted
    - Exponentially Weighted Moving Average (EWMA) of delta
    - Two-sided Cumulative Sum (CUSUM) control chart
    - Sturges-binned empirical Kullback-Leibler divergence D_KL(P_observed || P_predicted)
    """

    def __init__(
        self,
        ewma_span: int = 20,
        cusum_k_factor: float = 0.5,
        cusum_h_factor: float = 4.0,
        trailing_window_size: int = 100,
    ) -> None:
        self.ewma_span = max(1, ewma_span)
        self.alpha = 2.0 / (self.ewma_span + 1.0)
        self.cusum_k_factor = cusum_k_factor
        self.cusum_h_factor = cusum_h_factor
        self.trailing_window_size = trailing_window_size

        self.deltas: List[float] = []
        self.observed_history: List[float] = []
        self.predicted_history: List[float] = []

        self.ewma_delta: float = 0.0
        self.cusum_upper: float = 0.0
        self.cusum_lower: float = 0.0
        self.last_alert: Optional[CalibrationDriftAlert] = None

    def record_experiment_outcome(
        self,
        predicted_reward: float,
        observed_reward: float,
        timestamp: Optional[float] = None,
    ) -> CalibrationDriftStatus:
        """
        Record a pair of simulated predicted reward and live observed reward.
        Updates EWMA, CUSUM chart, and computes KL divergence.
        """
        now = timestamp if timestamp is not None else time.time()
        delta = float(observed_reward - predicted_reward)

        self.deltas.append(delta)
        self.observed_history.append(float(observed_reward))
        self.predicted_history.append(float(predicted_reward))

        # Maintain trailing window bounds
        if len(self.deltas) > self.trailing_window_size * 2:
            self.deltas = self.deltas[-self.trailing_window_size :]
            self.observed_history = self.observed_history[-self.trailing_window_size :]
            self.predicted_history = self.predicted_history[-self.trailing_window_size :]

        # Update EWMA
        if len(self.deltas) == 1:
            self.ewma_delta = delta
        else:
            self.ewma_delta = self.alpha * delta + (1.0 - self.alpha) * self.ewma_delta

        # Estimate standard deviation sigma_delta
        if len(self.deltas) >= 2:
            sigma_delta = float(np.std(self.deltas, ddof=1))
            if sigma_delta < 1e-6:
                sigma_delta = 1e-6
        else:
            sigma_delta = max(abs(delta), 1.0)

        # Update CUSUM control chart
        k = self.cusum_k_factor * sigma_delta
        h = self.cusum_h_factor * sigma_delta

        self.cusum_upper = max(0.0, self.cusum_upper + delta - k)
        self.cusum_lower = max(0.0, self.cusum_lower - delta - k)

        is_drifting = False
        drift_direction = "none"
        active_alert = None

        if self.cusum_upper > h or self.cusum_lower > h:
            is_drifting = True
            # delta = observed - predicted
            # If cusum_lower > h, delta is strongly negative => observed < predicted (Simulation was Optimistic)
            # If cusum_upper > h, delta is strongly positive => observed > predicted (Simulation was Pessimistic)
            if self.cusum_lower > self.cusum_upper:
                drift_direction = "optimistic"
                violating_cusum = self.cusum_lower
            else:
                drift_direction = "pessimistic"
                violating_cusum = self.cusum_upper

            kl_div = self._compute_kl_divergence()
            active_alert = CalibrationDriftAlert(
                timestamp=now,
                magnitude=abs(self.ewma_delta),
                direction=drift_direction,
                kl_divergence=kl_div,
                cusum_value=violating_cusum,
                threshold=h,
                message=(
                    f"CUSUM drift alert: simulation exhibits {drift_direction} bias "
                    f"(EWMA delta: {self.ewma_delta:.4f}, CUSUM: {violating_cusum:.4f} > {h:.4f}, "
                    f"D_KL: {kl_div:.4f})."
                ),
            )
            self.last_alert = active_alert
        else:
            self.last_alert = None

        kl_div = self._compute_kl_divergence()

        return CalibrationDriftStatus(
            ewma_delta=float(self.ewma_delta),
            cusum_upper=float(self.cusum_upper),
            cusum_lower=float(self.cusum_lower),
            is_drifting=is_drifting,
            drift_direction=drift_direction,
            kl_divergence=float(kl_div),
            sample_count=len(self.deltas),
            sigma_delta=float(sigma_delta),
            active_alert=active_alert,
        )

    def _compute_kl_divergence(self) -> float:
        """
        Compute empirical Kullback-Leibler divergence D_KL(P_observed || P_predicted)
        using histogram density estimation with Sturges' rule binning.
        """
        n = len(self.observed_history)
        if n < 5:
            return 0.0

        window_size = min(n, self.trailing_window_size)
        obs_window = np.array(self.observed_history[-window_size:], dtype=np.float64)
        pred_window = np.array(self.predicted_history[-window_size:], dtype=np.float64)

        # Sturges' rule: k = ceil(1 + log2(n))
        num_bins = max(3, int(np.ceil(1.0 + np.log2(window_size))))

        min_val = min(float(np.min(obs_window)), float(np.min(pred_window)))
        max_val = max(float(np.max(obs_window)), float(np.max(pred_window)))

        if math.isclose(min_val, max_val, abs_tol=1e-8):
            return 0.0

        # Create joint bin boundaries
        bins = np.linspace(min_val - 1e-6, max_val + 1e-6, num_bins + 1)

        hist_obs, _ = np.histogram(obs_window, bins=bins)
        hist_pred, _ = np.histogram(pred_window, bins=bins)

        # Laplace smoothing to ensure non-zero densities
        epsilon = 1e-7
        p = (hist_obs + epsilon) / (np.sum(hist_obs) + epsilon * num_bins)
        q = (hist_pred + epsilon) / (np.sum(hist_pred) + epsilon * num_bins)

        # D_KL(P || Q) = sum(P * log(P / Q))
        kl_div = float(np.sum(p * np.log(p / q)))
        return max(0.0, kl_div)

    def get_status(self) -> CalibrationDriftStatus:
        """Return current calibration drift status."""
        sigma_delta = float(np.std(self.deltas, ddof=1)) if len(self.deltas) >= 2 else 1.0
        sigma_delta = max(sigma_delta, 1e-6)
        h = self.cusum_h_factor * sigma_delta

        is_drifting = self.cusum_upper > h or self.cusum_lower > h
        if is_drifting:
            direction = "optimistic" if self.cusum_lower > self.cusum_upper else "pessimistic"
        else:
            direction = "none"

        return CalibrationDriftStatus(
            ewma_delta=float(self.ewma_delta),
            cusum_upper=float(self.cusum_upper),
            cusum_lower=float(self.cusum_lower),
            is_drifting=is_drifting,
            drift_direction=direction,
            kl_divergence=float(self._compute_kl_divergence()),
            sample_count=len(self.deltas),
            sigma_delta=float(sigma_delta),
            active_alert=self.last_alert,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize state to dictionary."""
        return {
            "ewma_span": self.ewma_span,
            "cusum_k_factor": self.cusum_k_factor,
            "cusum_h_factor": self.cusum_h_factor,
            "trailing_window_size": self.trailing_window_size,
            "deltas": self.deltas,
            "observed_history": self.observed_history,
            "predicted_history": self.predicted_history,
            "ewma_delta": self.ewma_delta,
            "cusum_upper": self.cusum_upper,
            "cusum_lower": self.cusum_lower,
            "last_alert": asdict(self.last_alert) if self.last_alert else None,
        }

    def from_dict(self, data: Dict[str, Any]) -> None:
        """Load state from dictionary."""
        self.ewma_span = data.get("ewma_span", self.ewma_span)
        self.alpha = 2.0 / (self.ewma_span + 1.0)
        self.cusum_k_factor = data.get("cusum_k_factor", self.cusum_k_factor)
        self.cusum_h_factor = data.get("cusum_h_factor", self.cusum_h_factor)
        self.trailing_window_size = data.get("trailing_window_size", self.trailing_window_size)
        self.deltas = data.get("deltas", [])
        self.observed_history = data.get("observed_history", [])
        self.predicted_history = data.get("predicted_history", [])
        self.ewma_delta = data.get("ewma_delta", 0.0)
        self.cusum_upper = data.get("cusum_upper", 0.0)
        self.cusum_lower = data.get("cusum_lower", 0.0)
        raw_alert = data.get("last_alert")
        if raw_alert:
            self.last_alert = CalibrationDriftAlert(**raw_alert)
        else:
            self.last_alert = None


class LearningConvergenceDiagnostics:
    """
    Evaluates learning engine optimization trajectory.
    
    Computes:
    - L2 norm of parameter changes ||Delta theta_t||_2
    - Cosine similarity between consecutive updates cos(Delta theta_t, Delta theta_{t-1})
    - Running variance of composite reward signal Var(R_t)
    - Finite-state automaton classification: CONVERGING, STAGNANT, OSCILLATING, DIVERGING, PLATEAU
    """

    def __init__(
        self,
        stagnation_epsilon: float = 1e-4,
        stagnation_window: int = 5,
        convergence_window: int = 10,
        divergence_growth_ratio: float = 1.05,
    ) -> None:
        self.stagnation_epsilon = stagnation_epsilon
        self.stagnation_window = stagnation_window
        self.convergence_window = convergence_window
        self.divergence_growth_ratio = divergence_growth_ratio

        self.theta_history: List[List[float]] = []
        self.l2_norm_history: List[float] = []
        self.cosine_history: List[float] = []
        self.reward_history: List[float] = []

        self.current_state: LearningState = LearningState.CONVERGING
        self.step_count: int = 0

    def record_parameter_update(self, theta: Union[List[float], np.ndarray]) -> float:
        """
        Record parameter vector theta_t, compute update norm and cosine similarity.
        Returns the computed L2 update norm.
        """
        curr_theta = np.array(theta, dtype=np.float64)
        self.step_count += 1

        if len(self.theta_history) > 0:
            prev_theta = np.array(self.theta_history[-1], dtype=np.float64)
            delta_curr = curr_theta - prev_theta
            norm_curr = float(np.linalg.norm(delta_curr))
            self.l2_norm_history.append(norm_curr)

            if len(self.theta_history) >= 2:
                prev_prev_theta = np.array(self.theta_history[-2], dtype=np.float64)
                delta_prev = prev_theta - prev_prev_theta
                norm_prev = float(np.linalg.norm(delta_prev))

                denom = norm_curr * norm_prev
                if denom > 1e-12:
                    cos_sim = float(np.dot(delta_curr, delta_prev) / denom)
                else:
                    cos_sim = 1.0
                self.cosine_history.append(cos_sim)
        else:
            norm_curr = 0.0

        self.theta_history.append(curr_theta.tolist())

        # Cap history bounds
        max_buf = self.convergence_window * 4
        if len(self.theta_history) > max_buf:
            self.theta_history = self.theta_history[-max_buf:]
            self.l2_norm_history = self.l2_norm_history[-max_buf:]
            self.cosine_history = self.cosine_history[-max_buf:]

        self._classify_state()
        return norm_curr

    def record_reward(self, reward: float) -> float:
        """
        Record composite reward signal and update internal classification.
        Returns running variance over the convergence window.
        """
        self.reward_history.append(float(reward))
        max_buf = self.convergence_window * 4
        if len(self.reward_history) > max_buf:
            self.reward_history = self.reward_history[-max_buf:]

        self._classify_state()
        return self._compute_reward_variance()

    def _compute_reward_variance(self) -> float:
        """Compute variance of reward over trailing convergence window."""
        if len(self.reward_history) < 2:
            return 0.0
        w = self.reward_history[-self.convergence_window :]
        return float(np.var(w, ddof=1)) if len(w) >= 2 else 0.0

    def _classify_state(self) -> None:
        """
        Finite-state automaton to classify learning dynamics with hysteresis.
        Priority: DIVERGING -> STAGNANT -> OSCILLATING -> PLATEAU -> CONVERGING.
        """
        # Need minimal updates to classify
        if len(self.l2_norm_history) < self.stagnation_window:
            self.current_state = LearningState.CONVERGING
            return

        recent_norms = self.l2_norm_history[-self.convergence_window :]
        recent_cosines = self.cosine_history[-min(len(self.cosine_history), 5) :]
        reward_var = self._compute_reward_variance()

        # 1. DIVERGING check: monotonically increasing L2 updates over window
        if len(recent_norms) >= self.convergence_window:
            strictly_increasing = True
            for i in range(1, len(recent_norms)):
                if recent_norms[i] <= recent_norms[i - 1] * 1.001:
                    strictly_increasing = False
                    break
            if strictly_increasing and recent_norms[-1] > 1e-2:
                self.current_state = LearningState.DIVERGING
                return

        # 2. STAGNANT check with hysteresis
        # Exit threshold is higher than entry threshold
        exit_eps = self.stagnation_epsilon * 1.5
        stag_norms = self.l2_norm_history[-self.stagnation_window :]
        all_stagnant = all(n < self.stagnation_epsilon for n in stag_norms)

        if self.current_state == LearningState.STAGNANT:
            if not all(n < exit_eps for n in stag_norms[-2:]):
                # Has broken out of stagnation hysteresis
                pass
            else:
                return
        elif all_stagnant:
            self.current_state = LearningState.STAGNANT
            return

        # 3. OSCILLATING check: >=3 negative cosine similarities in last 5 updates
        if len(recent_cosines) >= 3:
            neg_count = sum(1 for c in recent_cosines if c < -0.05)
            if neg_count >= 3:
                self.current_state = LearningState.OSCILLATING
                return

        # 4. PLATEAU check: reward variance is vanishingly small
        if len(self.reward_history) >= self.convergence_window and reward_var < 1e-6:
            self.current_state = LearningState.PLATEAU
            return

        # 5. Default healthy state
        self.current_state = LearningState.CONVERGING

    def get_status(self) -> LearningConvergenceStatus:
        """Produce the current diagnostics status."""
        last_norm = self.l2_norm_history[-1] if self.l2_norm_history else 0.0
        last_cos = self.cosine_history[-1] if self.cosine_history else 1.0
        var_r = self._compute_reward_variance()

        messages = {
            LearningState.CONVERGING: "Policy parameter trajectory is smoothly converging.",
            LearningState.STAGNANT: f"Update step size has stagnated below {self.stagnation_epsilon:.2e}.",
            LearningState.OSCILLATING: "Consecutive parameter updates show severe directional oscillation.",
            LearningState.DIVERGING: "Gradient updates are expanding exponentially; policy is diverging.",
            LearningState.PLATEAU: f"Reward variance ({var_r:.2e}) indicates performance plateau.",
        }

        return LearningConvergenceStatus(
            state=self.current_state,
            last_l2_norm=float(last_norm),
            l2_norm_history=list(self.l2_norm_history[-self.convergence_window :]),
            last_cosine_similarity=float(last_cos),
            cosine_similarity_history=list(self.cosine_history[-self.convergence_window :]),
            reward_variance=float(var_r),
            step_count=self.step_count,
            message=messages.get(self.current_state, "Unknown state"),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize state to dictionary."""
        return {
            "stagnation_epsilon": self.stagnation_epsilon,
            "stagnation_window": self.stagnation_window,
            "convergence_window": self.convergence_window,
            "theta_history": self.theta_history,
            "l2_norm_history": self.l2_norm_history,
            "cosine_history": self.cosine_history,
            "reward_history": self.reward_history,
            "current_state": self.current_state.value,
            "step_count": self.step_count,
        }

    def from_dict(self, data: Dict[str, Any]) -> None:
        """Load state from dictionary."""
        self.stagnation_epsilon = data.get("stagnation_epsilon", self.stagnation_epsilon)
        self.stagnation_window = data.get("stagnation_window", self.stagnation_window)
        self.convergence_window = data.get("convergence_window", self.convergence_window)
        self.theta_history = data.get("theta_history", [])
        self.l2_norm_history = data.get("l2_norm_history", [])
        self.cosine_history = data.get("cosine_history", [])
        self.reward_history = data.get("reward_history", [])
        state_str = data.get("current_state", LearningState.CONVERGING.value)
        self.current_state = LearningState(state_str)
        self.step_count = data.get("step_count", 0)


class PromotionDecisionAuditor:
    """
    Maintains ledger of Experiment Controller promotion/rollback decisions.
    
    Computes:
    - Brier score of promotion improvement probability predictions
    - Expected Calibration Error (ECE) across 10 reliability bins
    - Full calibration curve diagnostics
    """

    def __init__(self, num_bins: int = 10) -> None:
        self.num_bins = num_bins
        # ledger: experiment_id -> dict
        self.ledger: Dict[str, Dict[str, Any]] = {}

    def record_decision(
        self,
        experiment_id: str,
        p_value: float,
        was_promoted: bool,
        simulation_confidence: float = 1.0,
        timestamp: Optional[float] = None,
    ) -> None:
        """
        Record an initial promotion or rejection decision.
        Improvement probability is mapped from statistical confidence: p_improvement = 1 - p_value.
        """
        p_val = max(0.0, min(1.0, float(p_value)))
        p_improvement = 1.0 - p_val

        self.ledger[experiment_id] = {
            "experiment_id": experiment_id,
            "p_value": p_val,
            "p_improvement": p_improvement,
            "was_promoted": bool(was_promoted),
            "simulation_confidence": float(simulation_confidence),
            "improved": None,  # Pending post-promotion verification
            "timestamp": timestamp if timestamp is not None else time.time(),
        }

    def record_post_promotion_outcome(
        self,
        experiment_id: str,
        improved: bool,
    ) -> bool:
        """
        Record empirical post-promotion outcome (y_improvement in {0, 1}).
        Returns True if the experiment ID was successfully matched in the ledger.
        """
        if experiment_id not in self.ledger:
            return False

        self.ledger[experiment_id]["improved"] = bool(improved)
        return True

    def compute_calibration(self) -> PromotionCalibration:
        """
        Compute Brier score and Expected Calibration Error (ECE) over completed evaluations.
        """
        completed = [
            record for record in self.ledger.values()
            if record["improved"] is not None
        ]

        total_decisions = len(self.ledger)
        num_completed = len(completed)

        if num_completed == 0:
            empty_bins = []
            for b in range(self.num_bins):
                low = b / self.num_bins
                high = (b + 1) / self.num_bins
                empty_bins.append({
                    "bin_index": b,
                    "bin_lower": low,
                    "bin_upper": high,
                    "bin_center": (low + high) / 2.0,
                    "count": 0,
                    "accuracy": 0.0,
                    "confidence": (low + high) / 2.0,
                })
            return PromotionCalibration(
                brier_score=0.0,
                ece=0.0,
                total_decisions=total_decisions,
                completed_evaluations=0,
                calibration_curve=empty_bins,
            )

        # 1. Brier score: (1/N) * sum((p_pred - y_true)^2)
        brier_sum = 0.0
        bin_counts = [0] * self.num_bins
        bin_accuracies = [0.0] * self.num_bins
        bin_confidences = [0.0] * self.num_bins

        for rec in completed:
            p = float(rec["p_improvement"])
            y = 1.0 if rec["improved"] else 0.0
            brier_sum += (p - y) ** 2

            # Determine bin index [0, 0.1), ..., [0.9, 1.0]
            b_idx = min(self.num_bins - 1, int(p * self.num_bins))
            bin_counts[b_idx] += 1
            bin_accuracies[b_idx] += y
            bin_confidences[b_idx] += p

        brier_score = brier_sum / num_completed

        # 2. Expected Calibration Error (ECE)
        ece = 0.0
        calibration_curve: List[Dict[str, Any]] = []

        for b in range(self.num_bins):
            low = b / self.num_bins
            high = (b + 1) / self.num_bins
            cnt = bin_counts[b]

            if cnt > 0:
                acc = bin_accuracies[b] / cnt
                conf = bin_confidences[b] / cnt
                ece += (cnt / num_completed) * abs(acc - conf)
            else:
                acc = 0.0
                conf = (low + high) / 2.0

            calibration_curve.append({
                "bin_index": b,
                "bin_lower": round(low, 2),
                "bin_upper": round(high, 2),
                "bin_center": round((low + high) / 2.0, 2),
                "count": cnt,
                "accuracy": round(acc, 4),
                "confidence": round(conf, 4),
            })

        return PromotionCalibration(
            brier_score=float(round(brier_score, 4)),
            ece=float(round(ece, 4)),
            total_decisions=total_decisions,
            completed_evaluations=num_completed,
            calibration_curve=calibration_curve,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize state to dictionary."""
        return {
            "num_bins": self.num_bins,
            "ledger": self.ledger,
        }

    def from_dict(self, data: Dict[str, Any]) -> None:
        """Load state from dictionary."""
        self.num_bins = data.get("num_bins", self.num_bins)
        self.ledger = data.get("ledger", {})


class CalibrationMonitor:
    """
    Primary orchestrator for autonomic loop health calibration and drift detection.
    
    Synthesizes:
    - SimulationRealityDivergenceTracker
    - LearningConvergenceDiagnostics
    - PromotionDecisionAuditor
    - Autonomic Loop Health Index (ALHI) via weighted harmonic mean
    - Prescriptive corrective actions and persistence
    """

    def __init__(
        self,
        ewma_span: int = 20,
        cusum_k_factor: float = 0.5,
        cusum_h_factor: float = 4.0,
        stagnation_epsilon: float = 1e-4,
        stagnation_window: int = 5,
        convergence_window: int = 10,
        promotion_horizon: int = 10,
        health_weights: Optional[Dict[str, float]] = None,
        freshness_tau: float = 3600.0,
        persistence_path: Optional[str] = None,
    ) -> None:
        self.persistence_path = persistence_path
        self.freshness_tau = freshness_tau
        self.promotion_horizon = promotion_horizon

        # Health index weights (must be positive)
        self.health_weights = health_weights or {
            "simulation": 0.35,
            "learning": 0.30,
            "promotion": 0.20,
            "freshness": 0.15,
        }
        total_w = sum(self.health_weights.values())
        self.health_weights = {k: v / total_w for k, v in self.health_weights.items()}

        # Sub-components
        self.divergence_tracker = SimulationRealityDivergenceTracker(
            ewma_span=ewma_span,
            cusum_k_factor=cusum_k_factor,
            cusum_h_factor=cusum_h_factor,
        )
        self.learning_diagnostics = LearningConvergenceDiagnostics(
            stagnation_epsilon=stagnation_epsilon,
            stagnation_window=stagnation_window,
            convergence_window=convergence_window,
        )
        self.promotion_auditor = PromotionDecisionAuditor(num_bins=10)

        self.last_episode_timestamp: float = time.time()
        self.last_health_index: Optional[HealthIndex] = None

        if self.persistence_path and os.path.exists(self.persistence_path):
            self.load_state(self.persistence_path)

    def record_experiment_outcome(
        self,
        predicted_reward: float,
        observed_reward: float,
        timestamp: Optional[float] = None,
    ) -> CalibrationDriftStatus:
        """Record live experiment result against Monte Carlo simulation."""
        now = timestamp if timestamp is not None else time.time()
        self.last_episode_timestamp = now
        status = self.divergence_tracker.record_experiment_outcome(
            predicted_reward, observed_reward, timestamp=now
        )
        return status

    def record_parameter_update(
        self,
        theta: Union[List[float], np.ndarray],
    ) -> float:
        """Record policy gradient parameter update vector."""
        return self.learning_diagnostics.record_parameter_update(theta)

    def record_reward(self, reward: float) -> float:
        """Record reward from training iteration."""
        return self.learning_diagnostics.record_reward(reward)

    def record_promotion_decision(
        self,
        experiment_id: str,
        p_value: float,
        was_promoted: bool,
        simulation_confidence: float = 1.0,
        timestamp: Optional[float] = None,
    ) -> None:
        """Record canary promotion or rollback decision."""
        self.promotion_auditor.record_decision(
            experiment_id=experiment_id,
            p_value=p_value,
            was_promoted=was_promoted,
            simulation_confidence=simulation_confidence,
            timestamp=timestamp,
        )

    def record_post_promotion_outcome(
        self,
        experiment_id: str,
        improved: bool,
    ) -> bool:
        """Record post-promotion empirical outcome after verification horizon."""
        return self.promotion_auditor.record_post_promotion_outcome(
            experiment_id=experiment_id,
            improved=improved,
        )

    def compute_health_index(self, timestamp: Optional[float] = None) -> HealthIndex:
        """
        Compute Autonomic Loop Health Index (ALHI) in [0, 1] as a weighted harmonic mean:
            ALHI = sum(w_i) / sum(w_i / s_i)
        Constituents:
            1. Simulation calibration score = 1 - min(1, |EWMA(delta)| / sigma_delta)
            2. Learning convergence score: CONVERGING=1.0, PLATEAU=0.6, STAGNANT=0.3, OSCILLATING=0.2, DIVERGING=0.0
            3. Promotion calibration score = 1 - ECE
            4. Data freshness score = exp(-(t_now - t_last) / tau)
        """
        now = timestamp if timestamp is not None else time.time()

        # 1. Simulation Calibration Score
        drift_status = self.divergence_tracker.get_status()
        sigma = max(drift_status.sigma_delta, 1e-6)
        sim_penalty = min(1.0, abs(drift_status.ewma_delta) / sigma)
        simulation_score = max(0.0, 1.0 - sim_penalty)

        # 2. Learning Convergence Score
        learning_status = self.learning_diagnostics.get_status()
        learning_score_map = {
            LearningState.CONVERGING: 1.0,
            LearningState.PLATEAU: 0.6,
            LearningState.STAGNANT: 0.3,
            LearningState.OSCILLATING: 0.2,
            LearningState.DIVERGING: 0.0,
        }
        learning_score = learning_score_map.get(learning_status.state, 0.5)

        # 3. Promotion Calibration Score
        promo_calib = self.promotion_auditor.compute_calibration()
        promotion_score = max(0.0, 1.0 - promo_calib.ece)

        # 4. Data Freshness Score
        elapsed = max(0.0, now - self.last_episode_timestamp)
        freshness_score = math.exp(-elapsed / max(1.0, self.freshness_tau))

        component_scores = {
            "simulation": float(round(simulation_score, 4)),
            "learning": float(round(learning_score, 4)),
            "promotion": float(round(promotion_score, 4)),
            "freshness": float(round(freshness_score, 4)),
        }

        # Weighted Harmonic Mean with numerical floor
        epsilon = 1e-4
        total_w = sum(self.health_weights.values())
        harmonic_denom = 0.0

        for key, weight in self.health_weights.items():
            sc = component_scores.get(key, 1.0)
            harmonic_denom += weight / max(sc, epsilon)

        if harmonic_denom > 0:
            alhi = total_w / harmonic_denom
        else:
            alhi = 0.0

        alhi = float(round(max(0.0, min(1.0, alhi)), 4))

        # Determine prescriptive corrective actions
        actions = []
        if drift_status.is_drifting or drift_status.kl_divergence > 0.4:
            actions.append("RECOMMEND_ARCHIVE_REFRESH")
        if learning_status.state == LearningState.STAGNANT:
            actions.append("RECOMMEND_EXPLORATION_BOOST")
        elif learning_status.state == LearningState.OSCILLATING:
            actions.append("RECOMMEND_LEARNING_RATE_DECAY")
        elif learning_status.state == LearningState.DIVERGING:
            actions.append("RECOMMEND_POLICY_ROLLBACK")
        elif learning_status.state == LearningState.PLATEAU:
            actions.append("RECOMMEND_TASK_CURRICULUM_EXPANSION")
        if promo_calib.ece > 0.15:
            actions.append("RECOMMEND_EXPERIMENT_EXTENSION")
        if freshness_score < 0.2:
            actions.append("RECOMMEND_DISPATCH_HEARTBEAT")

        health_index = HealthIndex(
            alhi=alhi,
            component_scores=component_scores,
            timestamp=now,
            recommended_actions=actions,
        )
        self.last_health_index = health_index
        return health_index

    def get_calibration_report(self, timestamp: Optional[float] = None) -> CalibrationReport:
        """
        Generate complete structured CalibrationReport JSON artifact with all sub-statuses
        and high-level synthesis summary.
        """
        now = timestamp if timestamp is not None else time.time()
        health_index = self.compute_health_index(timestamp=now)
        drift_status = self.divergence_tracker.get_status()
        learning_status = self.learning_diagnostics.get_status()
        promotion_status = self.promotion_auditor.compute_calibration()

        # Build readable diagnostic narrative
        summary_lines = [
            f"Autonomic Loop Health Index (ALHI): {health_index.alhi:.2%}",
            f"  - Simulation Calibration: {health_index.component_scores['simulation']:.2f} "
            f"(EWMA delta: {drift_status.ewma_delta:.4f}, D_KL: {drift_status.kl_divergence:.4f}, Drift: {drift_status.is_drifting})",
            f"  - Learning Convergence: {health_index.component_scores['learning']:.2f} "
            f"(State: {learning_status.state.value}, Step: {learning_status.step_count})",
            f"  - Promotion Reliability: {health_index.component_scores['promotion']:.2f} "
            f"(ECE: {promotion_status.ece:.4f}, Brier: {promotion_status.brier_score:.4f}, Decisions: {promotion_status.total_decisions})",
            f"  - Data Freshness: {health_index.component_scores['freshness']:.2f}",
        ]
        if health_index.recommended_actions:
            summary_lines.append(f"Recommended Interventions: {', '.join(health_index.recommended_actions)}")
        else:
            summary_lines.append("Autonomic loop running nominally. No corrective action required.")

        summary_text = "\n".join(summary_lines)

        return CalibrationReport(
            timestamp=now,
            health_index=health_index,
            simulation_drift=drift_status,
            learning_convergence=learning_status,
            promotion_calibration=promotion_status,
            recommended_actions=health_index.recommended_actions,
            summary=summary_text,
        )

    def save_state(self, filepath: Optional[str] = None) -> str:
        """
        Serialize monitor state to a JSON file.
        """
        target_path = filepath or self.persistence_path
        if not target_path:
            raise ValueError("No persistence path provided for save_state.")

        dir_name = os.path.dirname(target_path)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)

        payload = {
            "timestamp": time.time(),
            "last_episode_timestamp": self.last_episode_timestamp,
            "freshness_tau": self.freshness_tau,
            "promotion_horizon": self.promotion_horizon,
            "health_weights": self.health_weights,
            "divergence_tracker": self.divergence_tracker.to_dict(),
            "learning_diagnostics": self.learning_diagnostics.to_dict(),
            "promotion_auditor": self.promotion_auditor.to_dict(),
        }

        with open(target_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

        return target_path

    def load_state(self, filepath: Optional[str] = None) -> bool:
        """
        Deserialize and restore monitor state from a JSON file.
        """
        target_path = filepath or self.persistence_path
        if not target_path or not os.path.exists(target_path):
            return False

        try:
            with open(target_path, "r", encoding="utf-8") as f:
                payload = json.load(f)

            self.last_episode_timestamp = payload.get("last_episode_timestamp", time.time())
            self.freshness_tau = payload.get("freshness_tau", self.freshness_tau)
            self.promotion_horizon = payload.get("promotion_horizon", self.promotion_horizon)
            self.health_weights = payload.get("health_weights", self.health_weights)

            if "divergence_tracker" in payload:
                self.divergence_tracker.from_dict(payload["divergence_tracker"])
            if "learning_diagnostics" in payload:
                self.learning_diagnostics.from_dict(payload["learning_diagnostics"])
            if "promotion_auditor" in payload:
                self.promotion_auditor.from_dict(payload["promotion_auditor"])

            return True
        except Exception:
            return False


if __name__ == "__main__":
    print("=" * 80)
    print("HERMES PIPELINE CALIBRATION MONITOR & DRIFT DETECTOR - SIMULATION TEST")
    print("=" * 80)

    # Initialize Monitor
    monitor = CalibrationMonitor(
        ewma_span=20,
        cusum_k_factor=0.5,
        cusum_h_factor=4.0,
        stagnation_epsilon=1e-4,
        stagnation_window=5,
        convergence_window=10,
        persistence_path="/tmp/pipeline_calibration_monitor_test.json",
    )

    np.random.seed(42)

    # 1. Simulate 50 experiment outcomes: first 25 balanced, next 25 optimistic divergence
    print("\n[Phase 1] Ingesting 50 experiment outcomes (gradual optimistic bias injection)...")
    base_reward = 1.0
    for i in range(50):
        predicted = base_reward + np.random.normal(0.0, 0.05)
        if i < 25:
            # Unbiased simulation
            observed = predicted + np.random.normal(0.0, 0.05)
        else:
            # Simulation over-predicts reward: observed drops, creating negative delta
            drift_bias = -0.04 * (i - 24)
            observed = predicted + drift_bias + np.random.normal(0.0, 0.03)

        status = monitor.record_experiment_outcome(predicted, observed)
        if status.is_drifting and status.active_alert:
            print(f"  -> Episode {i+1}: Drift Detected! Direction: {status.drift_direction}, "
                  f"EWMA Delta: {status.ewma_delta:.4f}, CUSUM: {status.cusum_lower:.2f}")

    # 2. Simulate 30 parameter updates: first 20 converging smoothly, last 10 stagnant
    print("\n[Phase 2] Ingesting 30 parameter updates (transitioning from converging to stagnant)...")
    theta = np.array([1.0, -1.0, 0.5, 0.2], dtype=np.float64)
    for step in range(30):
        if step < 20:
            # Steady gradient descent updates
            step_size = 0.05 * (0.85 ** step)
            theta += step_size * np.random.uniform(0.8, 1.2, size=theta.shape)
            reward = 0.5 + 0.02 * step + np.random.normal(0.0, 0.01)
        else:
            # Stagnant updates: step size < 1e-5
            theta += np.random.normal(0.0, 1e-5, size=theta.shape)
            reward = 0.95 + np.random.normal(0.0, 1e-5)

        monitor.record_parameter_update(theta)
        monitor.record_reward(reward)

    # 3. Simulate 20 promotion decisions with known outcomes
    print("\n[Phase 3] Ingesting 20 canary promotion decisions with post-promotion tracking...")
    for j in range(20):
        exp_id = f"exp_canary_{j+1:03d}"
        # Synthetic p-values: low p-value => high confidence in improvement
        p_val = float(np.clip(np.random.beta(0.5, 1.5), 0.01, 0.95))
        promoted = p_val < 0.20

        monitor.record_promotion_decision(
            experiment_id=exp_id,
            p_value=p_val,
            was_promoted=promoted,
            simulation_confidence=1.0 - p_val,
        )

        # Ground truth outcome: generally follows improvement probability with some noise
        p_improvement = 1.0 - p_val
        actually_improved = bool(np.random.rand() < p_improvement)
        monitor.record_post_promotion_outcome(exp_id, actually_improved)

    # 4. Generate Calibration Report
    print("\n[Phase 4] Computing Autonomic Loop Health Index (ALHI) & Calibration Report...")
    report = monitor.get_calibration_report()

    print("\n" + "=" * 40 + " CALIBRATION REPORT " + "=" * 40)
    print(report.summary)
    print("-" * 80)
    print("Reliability Diagram (First 5 Bins):")
    for b in report.promotion_calibration.calibration_curve[:5]:
        print(f"  Bin [{b['bin_lower']:.1f}-{b['bin_upper']:.1f}]: "
              f"Count={b['count']:2d}, Conf={b['confidence']:.2f}, Acc={b['accuracy']:.2f}")

    # 5. Test State Persistence
    print("\n[Phase 5] Verifying State Persistence (save_state & load_state)...")
    saved_path = monitor.save_state()
    print(f"  State saved to: {saved_path}")

    new_monitor = CalibrationMonitor(persistence_path=saved_path)
    loaded_report = new_monitor.get_calibration_report()
    assert math.isclose(report.health_index.alhi, loaded_report.health_index.alhi, rel_tol=1e-5), \
        "Loaded ALHI does not match saved ALHI!"
    print("  State persistence validation successful: Deserialized ALHI matches exactly.")
    print("=" * 80)