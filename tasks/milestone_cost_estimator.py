"""
tasks/milestone_cost_estimator.py

Predictive Milestone Cost Estimator & Pre-Dispatch Feasibility Gate
Part of the Hermes Autonomous Multi-Repo Orchestrator Suite.

Provides data-driven predictive cost estimation and admission gating for planned
milestones before LLM dispatch. Integrates with:
  - SteeringMemory (MemoryStore) for historical burn ratios & distilled profiles.
  - QuotaArbitrageEngine for real-time account headroom & token allocations.
  - TelemetryDetector (AnomalyDetector) for circuit breaker states and anomaly flags.
  - LifecycleStateMachine event bus for continuous model retraining on terminal transitions.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import numpy as np

# Configure module-level logger
logger = logging.getLogger("hermes.milestone_cost_estimator")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s")
    )
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


# =====================================================================
# Data Structures & Enums
# =====================================================================

class GateVerdict(str, Enum):
    """Feasibility gate admission decisions."""
    PASS = "PASS"
    DEFER = "DEFER"
    DECOMPOSE = "DECOMPOSE"


@dataclass(frozen=True)
class CostEstimate:
    """Detailed predictive cost estimation for a milestone prior to dispatch.

    Attributes:
        predicted_tokens: Point estimate of token expenditure (EWMA burn * complexity).
        token_confidence_interval: (lower, upper) bound 95% CI (mu +/- 1.96 * sigma).
        predicted_duration_s: Expected wall-clock duration in seconds (Log-Normal MLE).
        duration_confidence_interval: (lower, upper) bound 95% CI for duration.
        success_probability: Model-estimated probability of milestone completion [0.0, 1.0].
        quota_headroom_ratio: available_tokens / upper_bound_predicted_tokens.
        features_used: Raw feature map utilized in statistical inference.
    """
    predicted_tokens: int
    token_confidence_interval: Tuple[int, int]
    predicted_duration_s: float
    duration_confidence_interval: Tuple[float, float]
    success_probability: float
    quota_headroom_ratio: float
    features_used: Dict[str, Any]


@dataclass(frozen=True)
class GateDecision:
    """Pre-dispatch feasibility gate evaluation outcome.

    Attributes:
        decision: GateVerdict indicating PASS, DEFER, or DECOMPOSE.
        reason: Human-readable rationale detailing thresholds and quantitative metrics.
        cost_estimate: The underlying CostEstimate object evaluated.
        retry_after: Suggested UTC timestamp after which deferred milestone may retry.
    """
    decision: GateVerdict
    reason: str
    cost_estimate: CostEstimate
    retry_after: Optional[datetime] = None


@dataclass
class OutcomeRecord:
    """Historical execution record used for continuous parameter estimation and SGD."""
    milestone_id: str
    repo: str
    category: str
    estimated_tokens: int
    actual_tokens: int
    duration_s: float
    status: str  # e.g., "COMPLETED", "SUCCESS", "FAILED", "CIRCUIT_TRIPPED"
    file_count: int = 1
    dependency_depth: int = 0
    circuit_breaker_state: float = 0.0  # 0.0=CLOSED, 0.5=HALF_OPEN, 1.0=OPEN
    timestamp: float = field(default_factory=lambda: datetime.now(timezone.utc).timestamp())

    @property
    def is_success(self) -> bool:
        return self.status.strip().upper() in ("COMPLETED", "SUCCESS", "PASSED")

    @property
    def burn_ratio(self) -> float:
        est = max(1, self.estimated_tokens)
        return float(self.actual_tokens) / float(est)


# =====================================================================
# Milestone Cost Estimator
# =====================================================================

class MilestoneCostEstimator:
    """Statistical predictive engine for milestone token burn, duration, and success rate.

    Features:
      - Exponentially Weighted Moving Average (EWMA, alpha=0.3) for token burn ratios.
      - Log-Normal Maximum Likelihood Estimation (MLE) for execution duration.
      - Online Logistic Regression trained via SGD with L2 regularization (lambda=0.01)
        for dynamic success probability estimation.
      - Headroom calculation relative to active QuotaArbitrageEngine token pools.
    """

    # Feature weights order: [bias, file_count/10, dep_depth/5, repo_fail_rate, cb_state, burn_ratio]
    DEFAULT_WEIGHTS = np.array([1.5, -0.35, -0.25, -1.80, -2.50, -0.60], dtype=np.float64)

    def __init__(
        self,
        steering_memory: Any,
        quota_engine: Any,
        telemetry_detector: Any,
        l2_lambda: float = 0.01,
        learning_rate: float = 0.05,
    ) -> None:
        """Initialize the estimator with runtime dependencies.

        Args:
            steering_memory: Duck-typed MemoryStore providing get_steering_context.
            quota_engine: Duck-typed QuotaArbitrageEngine providing get_account_headroom.
            telemetry_detector: Duck-typed AnomalyDetector providing get_circuit_breaker_state.
            l2_lambda: L2 regularization hyperparameter for logistic SGD.
            learning_rate: Step size for online gradient descent updates.
        """
        self.steering_memory = steering_memory
        self.quota_engine = quota_engine
        self.telemetry_detector = telemetry_detector
        self.l2_lambda = l2_lambda
        self.learning_rate = learning_rate

        # Historical outcomes buffer
        self.outcomes: List[OutcomeRecord] = []

        # Online logistic regression weights (6-dimensional)
        self.logistic_weights: np.ndarray = np.copy(self.DEFAULT_WEIGHTS)

        logger.info(
            "MilestoneCostEstimator initialized with L2 lambda=%.4f, learning_rate=%.4f",
            self.l2_lambda,
            self.learning_rate,
        )

    # -----------------------------------------------------------------
    # Statistical Sub-Routines
    # -----------------------------------------------------------------

    def _compute_ewma_burn_ratio(self, repo: str, category: str, alpha: float = 0.3) -> float:
        """Compute sliding EWMA over historical token burn ratios (actual / estimated).

        Falls back hierarchically: (repo, category) -> repo -> category -> all -> steering memory.
        """
        # Filter matching records in chronological order
        records = [
            r for r in self.outcomes
            if r.repo == repo and r.category.lower() == category.lower()
        ]

        if not records:
            records = [r for r in self.outcomes if r.repo == repo]
        if not records:
            records = [r for r in self.outcomes if r.category.lower() == category.lower()]
        if not records:
            records = self.outcomes

        if not records:
            # Query steering memory for distilled profile if available
            if hasattr(self.steering_memory, "get_steering_context"):
                try:
                    ctx = self.steering_memory.get_steering_context(repo=repo)
                    if isinstance(ctx, dict) and "historical_burn_ratio" in ctx:
                        return float(ctx["historical_burn_ratio"])
                except Exception as ex:
                    logger.debug("Failed querying steering_memory for burn ratio: %s", ex)
            return 1.0  # Safe neutral default

        # Calculate chronological EWMA
        ratios = [r.burn_ratio for r in records]
        ewma = ratios[0]
        for val in ratios[1:]:
            ewma = alpha * val + (1.0 - alpha) * ewma

        return float(ewma)

    def _fit_logistic_weights(
        self,
        outcomes: List[OutcomeRecord],
        epochs: int = 15
    ) -> np.ndarray:
        """Fit logistic regression weights via online SGD with L2 regularization.

        Objective:
            min_w  -1/N sum_i [ y_i log(p_i) + (1 - y_i) log(1 - p_i) ] + (lambda/2) ||w_{1:}||^2
        """
        if not outcomes:
            return self.logistic_weights

        # Assemble feature matrix X and target vector y
        X_list = []
        y_list = []

        for rec in outcomes:
            x_vec = self._extract_feature_vector(
                file_count=rec.file_count,
                dep_depth=rec.dependency_depth,
                repo=rec.repo,
                cb_state=rec.circuit_breaker_state,
                burn_ratio=rec.burn_ratio,
            )
            X_list.append(x_vec)
            y_list.append(1.0 if rec.is_success else 0.0)

        X = np.array(X_list, dtype=np.float64)
        y = np.array(y_list, dtype=np.float64)
        n_samples = len(y)

        w = np.copy(self.logistic_weights)
        lr = self.learning_rate

        for _ in range(epochs):
            # Shuffle indices for mini-epoch stochastic updates
            indices = np.random.permutation(n_samples)
            for idx in indices:
                xi = X[idx]
                yi = y[idx]
                z = float(np.dot(w, xi))
                # Stable sigmoid
                pi = 1.0 / (1.0 + math.exp(-max(min(z, 25.0), -25.0)))
                error = pi - yi

                # Gradient calculation (exclude bias from L2 penalty)
                grad = error * xi
                grad[1:] += self.l2_lambda * w[1:]

                # SGD step
                w -= lr * grad

        self.logistic_weights = np.clip(w, -10.0, 10.0)
        return self.logistic_weights

    def _extract_feature_vector(
        self,
        file_count: int,
        dep_depth: int,
        repo: str,
        cb_state: float,
        burn_ratio: float,
    ) -> np.ndarray:
        """Construct normalized feature vector:

        x = [1.0 (bias), file_count / 10.0, dep_depth / 5.0, repo_fail_rate, cb_state, burn_ratio]
        """
        # Calculate historical repo failure rate
        repo_records = [r for r in self.outcomes if r.repo == repo]
        if repo_records:
            fail_count = sum(1 for r in repo_records if not r.is_success)
            repo_fail_rate = float(fail_count) / float(len(repo_records))
        else:
            repo_fail_rate = 0.10  # Baseline 10% failure rate assumption

        return np.array([
            1.0,
            float(file_count) / 10.0,
            float(dep_depth) / 5.0,
            float(repo_fail_rate),
            float(cb_state),
            float(burn_ratio),
        ], dtype=np.float64)

    def _estimate_duration_lognormal(
        self,
        category: str
    ) -> Tuple[float, Tuple[float, float]]:
        """Compute duration point estimate and 95% confidence interval using Log-Normal MLE.

        Returns:
            (predicted_duration_s, (lower_ci, upper_ci))
        """
        records = [
            r for r in self.outcomes
            if r.category.lower() == category.lower() and r.duration_s > 0.0
        ]
        if not records:
            records = [r for r in self.outcomes if r.duration_s > 0.0]

        if len(records) >= 2:
            durations = np.array([r.duration_s for r in records], dtype=np.float64)
            log_dur = np.log(durations)
            mu_ln = float(np.mean(log_dur))
            sigma_ln = float(np.std(log_dur, ddof=1))
            sigma_ln = max(0.15, min(sigma_ln, 1.8))  # Bounded dispersion
        else:
            # Baseline prior: mean duration ~ 150s, log-sigma ~ 0.40
            mu_ln = math.log(150.0)
            sigma_ln = 0.40

        # Expected value of log-normal distribution: E[T] = exp(mu + sigma^2 / 2)
        predicted_duration = math.exp(mu_ln + (sigma_ln ** 2) / 2.0)

        # 95% Confidence Interval in log space: [mu - 1.96*sigma, mu + 1.96*sigma]
        lower_bound = math.exp(mu_ln - 1.96 * sigma_ln)
        upper_bound = math.exp(mu_ln + 1.96 * sigma_ln)

        return float(predicted_duration), (float(lower_bound), float(upper_bound))

    def _get_active_circuit_breaker_state(self, repo: str) -> float:
        """Query AnomalyDetector for repo circuit breaker status (0.0=CLOSED, 0.5=HALF_OPEN, 1.0=OPEN)."""
        if not self.telemetry_detector:
            return 0.0

        if hasattr(self.telemetry_detector, "get_circuit_breaker_state"):
            try:
                state = self.telemetry_detector.get_circuit_breaker_state(repo)
                if isinstance(state, (int, float)):
                    return float(state)
                state_str = str(state).strip().upper()
                if "OPEN" in state_str and "HALF" not in state_str:
                    return 1.0
                elif "HALF" in state_str:
                    return 0.5
                return 0.0
            except Exception as ex:
                logger.warning("Failed querying telemetry_detector for CB state of %s: %s", repo, ex)
        return 0.0

    def _get_active_token_headroom(self, repo: str) -> float:
        """Query QuotaArbitrageEngine for real-time available token headroom."""
        if not self.quota_engine:
            return 250_000.0  # Permissive fallback default

        if hasattr(self.quota_engine, "get_account_headroom"):
            try:
                try:
                    res = self.quota_engine.get_account_headroom(repo=repo)
                except TypeError:
                    res = self.quota_engine.get_account_headroom()

                if isinstance(res, (int, float)):
                    return float(res)
                elif isinstance(res, dict):
                    for k in ("available_tokens", "headroom_tokens", "available", "headroom"):
                        if k in res:
                            return float(res[k])
            except Exception as ex:
                logger.warning("Failed querying quota_engine headroom: %s", ex)

        return 250_000.0

    # -----------------------------------------------------------------
    # Public Estimation API
    # -----------------------------------------------------------------

    def estimate(self, milestone_meta: Dict[str, Any]) -> CostEstimate:
        """Calculate full predictive cost estimate for a milestone prior to dispatch.

        Args:
            milestone_meta: Dictionary containing metadata:
                - repo (str): Target repository identifier.
                - category (str): Milestone domain ('feature', 'bugfix', 'refactor', etc.).
                - estimated_tokens / complexity_tokens (int): Initial planner estimate.
                - file_count / files (int or list): Number of affected files.
                - dep_depth / dependency_depth (int): Depth in topological graph.

        Returns:
            CostEstimate dataclass containing point predictions, CIs, and probabilities.
        """
        repo = str(milestone_meta.get("repo", "default"))
        category = str(milestone_meta.get("category", "general"))

        # 1. Complexity & Predicted Tokens (EWMA burn ratio)
        planner_complexity = (
            milestone_meta.get("estimated_tokens")
            or milestone_meta.get("complexity_tokens")
            or milestone_meta.get("estimated_complexity")
            or 1500
        )
        planner_complexity = int(planner_complexity)

        burn_ratio = self._compute_ewma_burn_ratio(repo, category, alpha=0.3)
        predicted_tokens = max(100, int(round(burn_ratio * planner_complexity)))

        # Historical token standard deviation
        relevant_records = [
            r for r in self.outcomes
            if r.repo == repo or r.category.lower() == category.lower()
        ]
        if len(relevant_records) >= 3:
            actual_arr = np.array([r.actual_tokens for r in relevant_records], dtype=np.float64)
            burn_stds = np.std(actual_arr, ddof=1)
            token_sigma = max(100.0, float(burn_stds * 0.35))
        else:
            token_sigma = max(150.0, predicted_tokens * 0.25)

        token_ci_lower = max(50, int(round(predicted_tokens - 1.96 * token_sigma)))
        token_ci_upper = int(round(predicted_tokens + 1.96 * token_sigma))
        token_ci = (token_ci_lower, token_ci_upper)

        # 2. Predicted Duration (Log-Normal MLE)
        pred_duration_s, dur_ci = self._estimate_duration_lognormal(category)

        # 3. Success Probability (Logistic Regression)
        raw_files = milestone_meta.get("files") or milestone_meta.get("target_files")
        if isinstance(raw_files, (list, tuple, set)):
            file_count = len(raw_files)
        else:
            file_count = int(milestone_meta.get("file_count", 1))

        dep_depth = int(
            milestone_meta.get("dep_depth")
            or milestone_meta.get("dependency_depth")
            or 0
        )
        cb_state = self._get_active_circuit_breaker_state(repo)

        x_vec = self._extract_feature_vector(
            file_count=file_count,
            dep_depth=dep_depth,
            repo=repo,
            cb_state=cb_state,
            burn_ratio=burn_ratio,
        )

        logit = float(np.dot(self.logistic_weights, x_vec))
        # Stable logistic sigmoid calculation
        success_prob = 1.0 / (1.0 + math.exp(-max(min(logit, 25.0), -25.0)))
        success_prob = max(0.01, min(success_prob, 0.99))

        # 4. Quota Headroom Ratio
        available_tokens = self._get_active_token_headroom(repo)
        upper_bound_tokens = max(1, token_ci_upper)
        headroom_ratio = float(available_tokens) / float(upper_bound_tokens)

        features_used = {
            "planner_complexity": planner_complexity,
            "ewma_burn_ratio": round(burn_ratio, 4),
            "token_sigma": round(token_sigma, 2),
            "file_count": file_count,
            "dependency_depth": dep_depth,
            "circuit_breaker_state": cb_state,
            "available_tokens": available_tokens,
            "repo_failure_rate": round(x_vec[3], 4),
        }

        return CostEstimate(
            predicted_tokens=predicted_tokens,
            token_confidence_interval=token_ci,
            predicted_duration_s=round(pred_duration_s, 2),
            duration_confidence_interval=(round(dur_ci[0], 2), round(dur_ci[1], 2)),
            success_probability=round(success_prob, 4),
            quota_headroom_ratio=round(headroom_ratio, 4),
            features_used=features_used,
        )

    def update_from_outcome(self, outcome: Union[OutcomeRecord, Dict[str, Any]]) -> None:
        """Incrementally update statistical models when a milestone terminates."""
        if isinstance(outcome, dict):
            record = OutcomeRecord(
                milestone_id=str(outcome.get("milestone_id", "anon")),
                repo=str(outcome.get("repo", "default")),
                category=str(outcome.get("category", "general")),
                estimated_tokens=int(outcome.get("estimated_tokens", 1000)),
                actual_tokens=int(outcome.get("actual_tokens", 1000)),
                duration_s=float(outcome.get("duration_s", 60.0)),
                status=str(outcome.get("status", "COMPLETED")),
                file_count=int(outcome.get("file_count", 1)),
                dependency_depth=int(outcome.get("dependency_depth", 0)),
                circuit_breaker_state=float(outcome.get("circuit_breaker_state", 0.0)),
            )
        else:
            record = outcome

        self.outcomes.append(record)
        logger.info(
            "Registered outcome for %s (status=%s, actual_tokens=%d, duration=%.1fs). Retraining online weights...",
            record.milestone_id,
            record.status,
            record.actual_tokens,
            record.duration_s,
        )

        # Trigger online parameter update
        self._fit_logistic_weights(self.outcomes[-50:], epochs=10)


# =====================================================================
# Pre-Dispatch Feasibility Gate
# =====================================================================

class FeasibilityGate:
    """Pre-dispatch admission gate acting as admission middleware in the orchestrator.

    Enforces resource sufficiency, structural feasibility, and tactical risk controls:
      - Quota Headroom < 1.0  -> DEFER (Insufficient tokens to absorb upper-bound burn)
      - Success Prob < 0.65   -> DECOMPOSE (High failure risk; must be split into atomic sub-tasks)
      - Headroom < 1.30 AND
        Success Prob < 0.80   -> DEFER (Marginal headroom with sub-optimal reliability)
      - Else                  -> PASS (Admit for immediate dispatch)
    """

    def __init__(
        self,
        estimator: MilestoneCostEstimator,
        success_threshold: float = 0.65,
        headroom_threshold: float = 1.30,
        defer_cooldown_minutes: int = 15,
    ) -> None:
        """Initialize the feasibility gate.

        Args:
            estimator: Instance of MilestoneCostEstimator.
            success_threshold: Minimum acceptable success probability (default 0.65).
            headroom_threshold: Safe quota headroom margin (default 1.30).
            defer_cooldown_minutes: Standard backoff window for deferred candidates.
        """
        self.estimator = estimator
        self.success_threshold = success_threshold
        self.headroom_threshold = headroom_threshold
        self.defer_cooldown_minutes = defer_cooldown_minutes

    def evaluate(self, milestone_meta: Dict[str, Any]) -> GateDecision:
        """Evaluate a candidate milestone against gating heuristics."""
        cost = self.estimator.estimate(milestone_meta)
        mid = milestone_meta.get("milestone_id") or milestone_meta.get("id") or "unspecified"
        now = datetime.now(timezone.utc)

        # Gate Rule 1: Absolute quota exhaustion
        if cost.quota_headroom_ratio < 1.0:
            retry_after = now + timedelta(minutes=self.defer_cooldown_minutes)
            reason = (
                f"Deficit quota headroom ratio ({cost.quota_headroom_ratio:.2f} < 1.00): "
                f"available tokens are insufficient to cover 95% upper bound "
                f"({cost.token_confidence_interval[1]:,} tokens). Dispatch deferred until quota replenishment."
            )
            logger.warning("[GATE DEFER] Milestone '%s': %s", mid, reason)
            return GateDecision(
                decision=GateVerdict.DEFER,
                reason=reason,
                cost_estimate=cost,
                retry_after=retry_after,
            )

        # Gate Rule 2: Excessive execution complexity / failure risk
        if cost.success_probability < self.success_threshold:
            reason = (
                f"Elevated failure probability (predicted success {cost.success_probability:.1%} < "
                f"threshold {self.success_threshold:.1%}). File count, dependency depth, or historical failure rate "
                f"indicates task should be decomposed into smaller atomic milestones."
            )
            logger.info("[GATE DECOMPOSE] Milestone '%s': %s", mid, reason)
            return GateDecision(
                decision=GateVerdict.DECOMPOSE,
                reason=reason,
                cost_estimate=cost,
                retry_after=None,
            )

        # Gate Rule 3: Marginal headroom combined with moderate risk
        if cost.quota_headroom_ratio < self.headroom_threshold and cost.success_probability < 0.80:
            retry_after = now + timedelta(minutes=max(5, self.defer_cooldown_minutes // 2))
            reason = (
                f"Marginal headroom with sub-optimal reliability: headroom ratio {cost.quota_headroom_ratio:.2f} "
                f"< {self.headroom_threshold:.2f} and success probability {cost.success_probability:.1%} < 80.0%. "
                f"Deferred to preserve quota buffers for higher-confidence milestones."
            )
            logger.warning("[GATE DEFER] Milestone '%s': %s", mid, reason)
            return GateDecision(
                decision=GateVerdict.DEFER,
                reason=reason,
                cost_estimate=cost,
                retry_after=retry_after,
            )

        # Admission granted
        reason = (
            f"Admitted for dispatch: success probability {cost.success_probability:.1%} >= {self.success_threshold:.1%}, "
            f"quota headroom ratio {cost.quota_headroom_ratio:.2f} >= 1.00."
        )
        logger.info("[GATE PASS] Milestone '%s': %s", mid, reason)
        return GateDecision(
            decision=GateVerdict.PASS,
            reason=reason,
            cost_estimate=cost,
            retry_after=None,
        )

    def evaluate_batch(
        self,
        candidates: List[Dict[str, Any]]
    ) -> List[Tuple[Dict[str, Any], GateDecision]]:
        """Evaluate a batch of candidate milestones and prioritize for admission.

        Sorted descending by Priority Score: (success_probability * quota_headroom_ratio).
        Passed items are placed at the front, prioritized by optimal return on token expenditure.
        """
        evaluated: List[Tuple[Dict[str, Any], GateDecision]] = []
        for candidate in candidates:
            decision = self.evaluate(candidate)
            evaluated.append((candidate, decision))

        def _sort_key(item: Tuple[Dict[str, Any], GateDecision]) -> Tuple[int, float]:
            meta, dec = item
            # Verdict rank: PASS = 2, DECOMPOSE = 1, DEFER = 0
            if dec.decision == GateVerdict.PASS:
                status_rank = 2
            elif dec.decision == GateVerdict.DECOMPOSE:
                status_rank = 1
            else:
                status_rank = 0

            # Priority metric: product of success probability and headroom
            score = dec.cost_estimate.success_probability * dec.cost_estimate.quota_headroom_ratio
            return (status_rank, score)

        evaluated.sort(key=_sort_key, reverse=True)
        return evaluated


# =====================================================================
# Factory Functions & Integration Hooks
# =====================================================================

def create_estimator(
    steering_memory: Any = None,
    quota_engine: Any = None,
    telemetry_detector: Any = None,
    **kwargs: Any,
) -> MilestoneCostEstimator:
    """Factory helper to construct a configured MilestoneCostEstimator instance."""
    return MilestoneCostEstimator(
        steering_memory=steering_memory,
        quota_engine=quota_engine,
        telemetry_detector=telemetry_detector,
        **kwargs,
    )


def create_gate(
    estimator: MilestoneCostEstimator,
    success_threshold: float = 0.65,
    headroom_threshold: float = 1.30,
    **kwargs: Any,
) -> FeasibilityGate:
    """Factory helper to construct a configured FeasibilityGate instance."""
    return FeasibilityGate(
        estimator=estimator,
        success_threshold=success_threshold,
        headroom_threshold=headroom_threshold,
        **kwargs,
    )


def on_lifecycle_transition(event: Any, estimator: MilestoneCostEstimator) -> None:
    """LifecycleStateMachine terminal transition event handler.

    Extracts execution telemetry from terminal events (COMPLETED, FAILED, ABORTED)
    and pushes training records to the MilestoneCostEstimator.
    """
    if event is None or estimator is None:
        return

    # Extract event attributes whether event is an object or dictionary
    if isinstance(event, dict):
        to_state = str(event.get("to_state", "") or event.get("state", "")).upper()
        payload = event.get("payload") or event.get("meta") or event
    else:
        to_state = str(getattr(event, "to_state", "") or getattr(event, "state", "")).upper()
        payload = getattr(event, "payload", getattr(event, "meta", event))

    terminal_states = {"COMPLETED", "SUCCESS", "PASSED", "FAILED", "CIRCUIT_TRIPPED", "TERMINATED"}
    if to_state not in terminal_states:
        return

    if isinstance(payload, dict):
        milestone_id = payload.get("milestone_id") or payload.get("id") or "evt-unknown"
        repo = payload.get("repo", "default")
        category = payload.get("category", "general")
        estimated = payload.get("estimated_tokens", 1500)
        actual = payload.get("actual_tokens", estimated)
        duration = payload.get("duration_s", 60.0)
        file_count = payload.get("file_count", 1)
        dep_depth = payload.get("dep_depth", 0)
        cb_state = payload.get("circuit_breaker_state", 0.0)
    else:
        milestone_id = getattr(payload, "milestone_id", "evt-unknown")
        repo = getattr(payload, "repo", "default")
        category = getattr(payload, "category", "general")
        estimated = getattr(payload, "estimated_tokens", 1500)
        actual = getattr(payload, "actual_tokens", estimated)
        duration = getattr(payload, "duration_s", 60.0)
        file_count = getattr(payload, "file_count", 1)
        dep_depth = getattr(payload, "dep_depth", 0)
        cb_state = getattr(payload, "circuit_breaker_state", 0.0)

    record = OutcomeRecord(
        milestone_id=str(milestone_id),
        repo=str(repo),
        category=str(category),
        estimated_tokens=int(estimated),
        actual_tokens=int(actual),
        duration_s=float(duration),
        status=to_state,
        file_count=int(file_count),
        dependency_depth=int(dep_depth),
        circuit_breaker_state=float(cb_state),
    )

    logger.debug("Received lifecycle transition event to %s for milestone %s", to_state, milestone_id)
    estimator.update_from_outcome(record)


# =====================================================================
# Self-Contained Verification Demo Block
# =====================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("  Hermes Milestone Cost Estimator & Feasibility Gate Demonstration")
    print("=" * 80)

    # 1. Construct Mock Dependencies matching real interfaces
    class MockSteeringMemory:
        def get_steering_context(self, repo: str) -> Dict[str, Any]:
            return {"historical_burn_ratio": 1.12, "reliability_score": 0.94}

    class MockQuotaArbitrageEngine:
        def __init__(self, token_pool: float = 120_000.0) -> None:
            self.token_pool = token_pool

        def get_account_headroom(self, repo: Optional[str] = None) -> float:
            return self.token_pool

    class MockTelemetryDetector:
        def __init__(self, tripped_repos: Optional[List[str]] = None) -> None:
            self.tripped_repos = tripped_repos or []

        def get_circuit_breaker_state(self, repo: str) -> float:
            return 1.0 if repo in self.tripped_repos else 0.0

    memory = MockSteeringMemory()
    quota = MockQuotaArbitrageEngine(token_pool=25_000.0)  # Constrained pool for testing
    telemetry = MockTelemetryDetector(tripped_repos=["flaky-subsystem"])

    estimator = create_estimator(memory, quota, telemetry)
    gate = create_gate(estimator, success_threshold=0.65, headroom_threshold=1.30)

    # 2. Seed synthetic historical training outcomes
    synthetic_history = [
        OutcomeRecord("M-01", "repo-alpha", "feature", 1500, 1800, 140.0, "SUCCESS", 2, 1),
        OutcomeRecord("M-02", "repo-alpha", "feature", 2000, 2400, 185.0, "SUCCESS", 3, 2),
        OutcomeRecord("M-03", "repo-alpha", "bugfix", 800, 750, 45.0, "SUCCESS", 1, 0),
        OutcomeRecord("M-04", "repo-alpha", "feature", 3000, 4200, 290.0, "FAILED", 6, 4),
        OutcomeRecord("M-05", "flaky-subsystem", "refactor", 2500, 3100, 220.0, "FAILED", 8, 3, circuit_breaker_state=1.0),
        OutcomeRecord("M-06", "flaky-subsystem", "bugfix", 1000, 1900, 160.0, "FAILED", 4, 2, circuit_breaker_state=1.0),
        OutcomeRecord("M-07", "repo-beta", "feature", 1200, 1300, 110.0, "SUCCESS", 2, 0),
    ]

    for rec in synthetic_history:
        estimator.update_from_outcome(rec)

    print(f"\n[+] Seeded {len(synthetic_history)} historical records. Logistic Weights:")
    print("   ", [round(w, 4) for w in estimator.logistic_weights])

    # 3. Formulate Candidate Test Milestones
    candidates = [
        {
            "milestone_id": "M-FEAT-CLEAN",
            "repo": "repo-alpha",
            "category": "feature",
            "estimated_tokens": 1200,
            "files": ["tasks/estimator.py", "tasks/gate.py"],
            "dep_depth": 1,
        },
        {
            "milestone_id": "M-MONOLITH-HEAVY",
            "repo": "repo-alpha",
            "category": "feature",
            "estimated_tokens": 9500,
            "files": [f"file_{i}.py" for i in range(12)],
            "dep_depth": 5,
        },
        {
            "milestone_id": "M-FLAKY-CIRCUIT",
            "repo": "flaky-subsystem",
            "category": "refactor",
            "estimated_tokens": 2000,
            "files": ["tasks/legacy.py"],
            "dep_depth": 2,
        },
        {
            "milestone_id": "M-QUOTA-OVERFLOW",
            "repo": "repo-beta",
            "category": "feature",
            "estimated_tokens": 18000,
            "files": ["large_dataset.py"],
            "dep_depth": 1,
        },
    ]

    print("\n[+] Evaluating Candidates against Pre-Dispatch Feasibility Gate:")
    evaluations = gate.evaluate_batch(candidates)

    for item, dec in evaluations:
        est = dec.cost_estimate
        print("\n" + "-" * 75)
        print(f"Milestone: {item['milestone_id']} (Repo: {item['repo']}, Cat: {item['category']})")
        print(f"Verdict:   [{dec.decision.value}] -> {dec.reason}")
        print(f"  * Predicted Tokens:   {est.predicted_tokens:,} (95% CI: {est.token_confidence_interval[0]:,} - {est.token_confidence_interval[1]:,})")
        print(f"  * Predicted Duration: {est.predicted_duration_s:.1f}s (95% CI: {est.duration_confidence_interval[0]:.1f}s - {est.duration_confidence_interval[1]:.1f}s)")
        print(f"  * Success Prob:       {est.success_probability:.1%}")
        print(f"  * Quota Headroom:     {est.quota_headroom_ratio:.2f}x (Pool: {quota.token_pool:,.0f} tokens)")
        if dec.retry_after:
            print(f"  * Retry After:        {dec.retry_after.strftime('%Y-%m-%d %H:%M:%S UTC')}")

    # 4. Test Event Bus Hook
    print("\n[+] Testing LifecycleStateMachine Transition Hook:")
    lifecycle_event = {
        "to_state": "COMPLETED",
        "payload": {
            "milestone_id": "M-FEAT-CLEAN",
            "repo": "repo-alpha",
            "category": "feature",
            "estimated_tokens": 1200,
            "actual_tokens": 1350,
            "duration_s": 125.0,
            "file_count": 2,
            "dep_depth": 1,
            "circuit_breaker_state": 0.0,
        }
    }
    on_lifecycle_transition(lifecycle_event, estimator)
    print("  * Event processed successfully. Model updated dynamically.")
    print("=" * 80)
    print("  Demonstration completed successfully.")
    print("=" * 80)
"""
End of tasks/milestone_cost_estimator.py
"""