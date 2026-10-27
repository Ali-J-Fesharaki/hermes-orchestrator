"""Pipeline Live Policy Experimentation & Canary Promotion Controller.

This module closes the autonomic control loop in the Hermes orchestration
framework by deploying simulation-validated policy proposals from the
Simulation Sandbox into controlled live canary experiments. It evaluates
experimental governance parameters against the incumbent baseline policy
using real-time telemetry, sequential testing (SPRT), and distribution
divergence monitoring (Cramér-von Mises test).

Key Architectural Components:
1. Experiment Lifecycle Manager: Finite state machine governing policy transitions
   {PROPOSED, SIMULATION_VALIDATED, CANARY_ACTIVE, GRADUATED, ROLLED_BACK, EXPIRED}.
2. Traffic Splitting & Cohort Assignment: Deterministic, stateless hashing of
   (experiment_id || cycle_id) into experimental vs. control cohorts.
3. Real-Time Comparative Telemetry Collector: Parallel EWMA accumulators and
   incremental Welch t-statistics comparing live rewards and governance dimensions.
4. Sequential Testing Promotion Gate: Wald's Sequential Probability Ratio Test
   (SPRT) with configurable Type I (alpha) and Type II (beta) error bounds for
   optimal early stopping.
5. Simulation-Reality Divergence Detector: Continuous Cramér-von Mises goodness-of-fit
   tracking between sandbox predictions and live reward realizations.
6. Graduated Policy Installer: Atomic policy promotion, parameter diff lineage
   recording, and archive tagging for the Learning Engine.
7. Rollback & Immunization Registry: Automatic reversion on empirical deficit and
   creation of axis-aligned parameter exclusion zones to safeguard future exploration.
8. Experiment Dashboard Metrics Emitter: Structured telemetry payloads for real-time
   cockpit monitoring and strategic evolution alignment.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import statistics
from typing import Any, Dict, List, Optional, Tuple, Union

logger = logging.getLogger("hermes.pipeline.experiment_controller")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [ExperimentController] %(message)s"
    )
    _handler.setFormatter(_formatter)
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)


class ExperimentState(str, Enum):
    """Lifecycle states for governance policy experiments."""
    PROPOSED = "PROPOSED"
    SIMULATION_VALIDATED = "SIMULATION_VALIDATED"
    CANARY_ACTIVE = "CANARY_ACTIVE"
    GRADUATED = "GRADUATED"
    ROLLED_BACK = "ROLLED_BACK"
    EXPIRED = "EXPIRED"


# ============================================================================
# Statistical & Numerical Helpers (Pure Standard Library)
# ============================================================================

def _betacf(a: float, b: float, x: float) -> float:
    """Continued fraction evaluation for the incomplete beta function (Lentz's method)."""
    max_iter = 200
    eps = 1e-14
    fp_min = 1e-30

    qab = a + b
    qap = a + 1.0
    qam = a - 1.0

    c = 1.0
    d = 1.0 - qab * x / qap
    if abs(d) < fp_min:
        d = fp_min
    d = 1.0 / d
    h = d

    for m in range(1, max_iter + 1):
        m2 = 2 * m
        # Even step
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        if abs(d) < fp_min:
            d = fp_min
        c = 1.0 + aa / c
        if abs(c) < fp_min:
            c = fp_min
        d = 1.0 / d
        h *= d * c

        # Odd step
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        if abs(d) < fp_min:
            d = fp_min
        c = 1.0 + aa / c
        if abs(c) < fp_min:
            c = fp_min
        d = 1.0 / d
        del_h = d * c
        h *= del_h
        if abs(del_h - 1.0) < eps:
            break

    return h


def _regularized_incomplete_beta(a: float, b: float, x: float) -> float:
    """Computes the regularized incomplete beta function I_x(a, b)."""
    if x < 0.0 or x > 1.0:
        raise ValueError(f"x must be in [0, 1], got {x}")
    if x == 0.0:
        return 0.0
    if x == 1.0:
        return 1.0

    log_factor = (
        a * math.log(x)
        + b * math.log(1.0 - x)
        + math.lgamma(a + b)
        - math.lgamma(a)
        - math.lgamma(b)
        - math.log(a)
    )
    factor = math.exp(log_factor)

    if x < (a + 1.0) / (a + b + 2.0):
        return factor * _betacf(a, b, x)
    else:
        log_factor_sym = (
            b * math.log(1.0 - x)
            + a * math.log(x)
            + math.lgamma(a + b)
            - math.lgamma(a)
            - math.lgamma(b)
            - math.log(b)
        )
        return 1.0 - math.exp(log_factor_sym) * _betacf(b, a, 1.0 - x)


def compute_welch_t_test(
    sample1: List[float], sample2: List[float]
) -> Tuple[float, float, float]:
    """Computes Welch's t-test for two independent samples with unequal variances.

    Uses the Welch-Satterthwaite equation for degrees of freedom and the
    regularized incomplete beta function for exact two-tailed p-values.

    Returns:
        (t_statistic, p_value, degrees_of_freedom)
    """
    n1 = len(sample1)
    n2 = len(sample2)

    if n1 < 2 or n2 < 2:
        return 0.0, 1.0, 1.0

    m1 = statistics.mean(sample1)
    m2 = statistics.mean(sample2)
    v1 = statistics.variance(sample1)
    v2 = statistics.variance(sample2)

    denom_sq = (v1 / n1) + (v2 / n2)
    if denom_sq <= 1e-15:
        if math.isclose(m1, m2, abs_tol=1e-9):
            return 0.0, 1.0, float(n1 + n2 - 2)
        else:
            t_inf = 1e6 if m1 > m2 else -1e6
            return t_inf, 0.0, float(n1 + n2 - 2)

    t_stat = (m1 - m2) / math.sqrt(denom_sq)

    num_df = denom_sq**2
    denom_df = ((v1 / n1) ** 2) / (n1 - 1) + ((v2 / n2) ** 2) / (n2 - 1)
    df = num_df / denom_df if denom_df > 1e-15 else float(n1 + n2 - 2)
    df = max(1.0, df)

    # Two-tailed p-value calculation via Student's t distribution:
    # p = I_{df / (df + t^2)}(df / 2, 1 / 2)
    x = df / (df + t_stat * t_stat)
    try:
        p_val = _regularized_incomplete_beta(df / 2.0, 0.5, x)
        p_val = max(0.0, min(1.0, p_val))
    except Exception as exc:
        logger.debug("Incomplete beta calculation fallback: %s", exc)
        # Large df asymptotic normal approximation fallback
        z = abs(t_stat)
        p_val = math.erfc(z / math.sqrt(2.0))
        p_val = max(0.0, min(1.0, p_val))

    return t_stat, p_val, df


def compute_cramer_von_mises(
    observed_samples: List[float], theoretical_mean: float, theoretical_variance: float
) -> Tuple[float, float, bool]:
    """Computes the Cramér-von Mises statistic against a Gaussian reference distribution.

    Evaluates the empirical cumulative distribution function against the
    hypothesized Gaussian CDF N(theoretical_mean, theoretical_variance).

    Returns:
        (cvm_statistic, critical_value_p01, is_divergent)
    """
    n = len(observed_samples)
    if n < 3:
        return 0.0, 0.7435, False

    std_dev = math.sqrt(max(theoretical_variance, 1e-6))
    sorted_samples = sorted(observed_samples)

    u_values = []
    for x in sorted_samples:
        z = (x - theoretical_mean) / (std_dev * math.sqrt(2.0))
        u = 0.5 * (1.0 + math.erf(z))
        u = max(1e-12, min(1.0 - 1e-12, u))
        u_values.append(u)

    sum_sq_diff = 0.0
    for i, u in enumerate(u_values, start=1):
        emp_midpoint = (2.0 * i - 1.0) / (2.0 * n)
        sum_sq_diff += (u - emp_midpoint) ** 2

    cvm_stat = sum_sq_diff + (1.0 / (12.0 * n))
    critical_val = 0.7435  # Asymptotic critical value at alpha = 0.01
    is_divergent = cvm_stat > critical_val

    return cvm_stat, critical_val, is_divergent


def compute_ewma(
    current_value: float, previous_ewma: Optional[float], alpha: float = 0.15
) -> float:
    """Updates an Exponentially Weighted Moving Average (EWMA)."""
    if previous_ewma is None:
        return float(current_value)
    return float(alpha * current_value + (1.0 - alpha) * previous_ewma)


# ============================================================================
# Dataclasses
# ============================================================================

@dataclass
class PolicyExperiment:
    """Stateful record tracking a live policy canary experiment."""
    experiment_id: str
    proposed_params: Dict[str, Any]
    incumbent_params: Dict[str, Any]
    traffic_fraction: float = 0.1
    max_duration_cycles: int = 20
    state: ExperimentState = ExperimentState.SIMULATION_VALIDATED
    simulation_p_value: float = 0.0
    simulation_predicted_reward: Dict[str, float] = field(
        default_factory=lambda: {
            "mean": 1.0,
            "variance": 0.25,
            "p5": 0.5,
            "p95": 1.5,
            "predicted_improvement": 0.10,
        }
    )
    created_at: str = field(
        default_factory=lambda: datetime.utcnow().isoformat() + "Z"
    )
    cycles_elapsed: int = 0
    sprt_log_likelihood_ratio: float = 0.0
    control_rewards: List[float] = field(default_factory=list)
    experimental_rewards: List[float] = field(default_factory=list)
    cvm_statistic: float = 0.0
    control_metrics_ewma: Dict[str, float] = field(default_factory=dict)
    experimental_metrics_ewma: Dict[str, float] = field(default_factory=dict)
    parent_policy_version: str = "v1.0.0"
    graduated_policy_version: Optional[str] = None
    decision_reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serializes the experiment state to a plain dictionary."""
        data = dataclasses.asdict(self)
        data["state"] = self.state.value
        return data


# ============================================================================
# Core Experiment Controller
# ============================================================================

class ExperimentController:
    """Live policy experimentation and canary promotion controller.

    Closes the autonomic loop by guiding candidate governance policies
    through canary deployment, sequential testing (SPRT), divergence verification,
    and automated promotion/rollback.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initializes the ExperimentController with runtime parameters and storage paths."""
        cfg = config or {}
        self.traffic_fraction = float(cfg.get("traffic_fraction", 0.10))
        self.traffic_fraction = max(0.01, min(0.50, self.traffic_fraction))
        self.max_duration_cycles = int(cfg.get("max_duration_cycles", 20))
        self.sprt_alpha = float(cfg.get("sprt_alpha", 0.05))
        self.sprt_beta = float(cfg.get("sprt_beta", 0.10))
        self.cvm_significance = float(cfg.get("cvm_significance", 0.01))
        self.immunization_cooldown_cycles = int(
            cfg.get("immunization_cooldown_cycles", 50)
        )
        self.min_samples_for_decision = int(cfg.get("min_samples_for_decision", 3))

        # SPRT Decision Boundaries
        # Promote when LLR >= ln((1 - beta) / alpha)
        # Rollback when LLR <= ln(beta / (1 - alpha))
        self.promote_boundary = math.log((1.0 - self.sprt_beta) / self.sprt_alpha)
        self.rollback_boundary = math.log(self.sprt_beta / (1.0 - self.sprt_alpha))

        # File paths
        self.experiment_journal_path = Path(
            cfg.get("experiment_journal_path", "logs/experiment_journal.jsonl")
        )
        self.policy_lineage_path = Path(
            cfg.get("policy_lineage_path", "logs/policy_lineage.json")
        )
        self.immunization_registry_path = Path(
            cfg.get("immunization_registry_path", "logs/immunization_registry.json")
        )

        self._ensure_parent_dirs()

        # In-memory indices
        self.experiments: Dict[str, PolicyExperiment] = {}
        self.current_cycle: int = 0
        self.immunization_registry: List[Dict[str, Any]] = self._load_json_list(
            self.immunization_registry_path
        )
        self.policy_lineage: List[Dict[str, Any]] = self._load_json_list(
            self.policy_lineage_path
        )

        logger.info(
            "ExperimentController initialized (SPRT promote_bound=%.4f, rollback_bound=%.4f, traffic=%.2f)",
            self.promote_boundary,
            self.rollback_boundary,
            self.traffic_fraction,
        )

    def _ensure_parent_dirs(self) -> None:
        """Ensures that storage directories exist."""
        for p in [
            self.experiment_journal_path,
            self.policy_lineage_path,
            self.immunization_registry_path,
        ]:
            os.makedirs(p.parent, exist_ok=True)

    def _load_json_list(self, path: Path) -> List[Dict[str, Any]]:
        """Loads a JSON array from disk or returns an empty list."""
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                    if content:
                        data = json.loads(content)
                        if isinstance(data, list):
                            return data
            except Exception as exc:
                logger.warning("Failed to load JSON list from %s: %s", path, exc)
        return []

    def _save_immunization_registry(self) -> None:
        """Flushes the immunization registry to disk."""
        self._ensure_parent_dirs()
        try:
            with open(self.immunization_registry_path, "w", encoding="utf-8") as f:
                json.dump(self.immunization_registry, f, indent=2)
        except Exception as exc:
            logger.error("Failed to write immunization registry: %s", exc)

    def _save_policy_lineage(self) -> None:
        """Flushes the policy lineage ledger to disk."""
        self._ensure_parent_dirs()
        try:
            with open(self.policy_lineage_path, "w", encoding="utf-8") as f:
                json.dump(self.policy_lineage, f, indent=2)
        except Exception as exc:
            logger.error("Failed to write policy lineage: %s", exc)

    def _persist_journal_entry(
        self,
        experiment: PolicyExperiment,
        event_type: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Appends a structured event line to the experiment journal."""
        self._ensure_parent_dirs()
        entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "event_type": event_type,
            "experiment_id": experiment.experiment_id,
            "state": experiment.state.value,
            "cycles_elapsed": experiment.cycles_elapsed,
            "sprt_llr": round(experiment.sprt_log_likelihood_ratio, 6),
            "control_samples": len(experiment.control_rewards),
            "experimental_samples": len(experiment.experimental_rewards),
            "cvm_statistic": round(experiment.cvm_statistic, 6),
            "details": details or {},
        }
        try:
            with open(self.experiment_journal_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception as exc:
            logger.error("Failed to persist journal entry: %s", exc)

    # ------------------------------------------------------------------------
    # Proposal Submission & Counterfactual Gate
    # ------------------------------------------------------------------------

    def submit_proposal(
        self,
        proposed_params: Dict[str, Any],
        simulation_report: Dict[str, Any],
        incumbent_params: Optional[Dict[str, Any]] = None,
        parent_policy_version: str = "v1.0.0",
    ) -> str:
        """Validates and registers a proposed policy experiment.

        Checks:
        1. Counterfactual Improvement Gate (Welch p-value < alpha).
        2. Parameter immunization exclusion zones.

        Returns:
            experiment_id (UUID string)
        Raises:
            ValueError if counterfactual verification fails or parameters fall in an exclusion zone.
        """
        sim_p = float(
            simulation_report.get(
                "p_value", simulation_report.get("counterfactual_p_value", 1.0)
            )
        )
        gate_alpha = float(simulation_report.get("alpha", self.sprt_alpha))

        if sim_p >= gate_alpha:
            err = (
                f"Proposal rejected by Counterfactual Improvement Gate: "
                f"simulation p-value {sim_p:.4f} >= {gate_alpha:.4f}"
            )
            logger.warning(err)
            raise ValueError(err)

        # Immunization Registry Verification
        # Purge expired zones
        self.immunization_registry = [
            zone
            for zone in self.immunization_registry
            if zone.get("expiry_cycle", 0) > self.current_cycle
        ]

        for param_name, proposed_val in proposed_params.items():
            if not isinstance(proposed_val, (int, float)):
                continue
            for zone in self.immunization_registry:
                if zone.get("parameter") == param_name:
                    z_min = zone.get("min_value", float("-inf"))
                    z_max = zone.get("max_value", float("inf"))
                    if z_min <= proposed_val <= z_max:
                        err = (
                            f"Proposal rejected: parameter '{param_name}'={proposed_val} "
                            f"falls in immunized exclusion zone [{z_min:.4f}, {z_max:.4f}] "
                            f"from failed experiment {zone.get('experiment_id')} "
                            f"(cooldown active until cycle {zone.get('expiry_cycle')})"
                        )
                        logger.warning(err)
                        raise ValueError(err)

        experiment_id = hashlib.sha256(
            f"{json.dumps(proposed_params, sort_keys=True)}_{datetime.utcnow().isoformat()}".encode()
        ).hexdigest()[:16]

        pred_reward = simulation_report.get(
            "predicted_reward",
            {
                "mean": 1.0,
                "variance": 0.25,
                "p5": 0.5,
                "p95": 1.5,
                "predicted_improvement": 0.10,
            },
        )

        experiment = PolicyExperiment(
            experiment_id=experiment_id,
            proposed_params=proposed_params,
            incumbent_params=incumbent_params or {},
            traffic_fraction=self.traffic_fraction,
            max_duration_cycles=self.max_duration_cycles,
            state=ExperimentState.SIMULATION_VALIDATED,
            simulation_p_value=sim_p,
            simulation_predicted_reward=pred_reward,
            parent_policy_version=parent_policy_version,
        )

        self.experiments[experiment_id] = experiment
        self._persist_journal_entry(
            experiment,
            "PROPOSAL_ACCEPTED",
            {
                "simulation_report_summary": {
                    "p_value": sim_p,
                    "predicted_reward": pred_reward,
                }
            },
        )

        logger.info(
            "Experiment %s accepted into SIMULATION_VALIDATED state (sim_p=%.4f)",
            experiment_id,
            sim_p,
        )
        return experiment_id

    # ------------------------------------------------------------------------
    # Traffic Splitting & Cohort Assignment
    # ------------------------------------------------------------------------

    def assign_cohort(self, experiment_id: str, cycle_id: Union[int, str]) -> str:
        """Deterministically assigns an orchestration cycle to 'experimental' or 'control'.

        Computes: hash(experiment_id || cycle_id) mod 1000.
        Returns 'experimental' if hash_val < traffic_fraction * 1000, else 'control'.
        """
        experiment = self.experiments.get(experiment_id)
        fraction = (
            experiment.traffic_fraction if experiment else self.traffic_fraction
        )

        hash_input = f"{experiment_id}{cycle_id}".encode("utf-8")
        digest = hashlib.sha256(hash_input).hexdigest()
        cohort_val = int(digest, 16) % 1000
        threshold = int(fraction * 1000)

        cohort = "experimental" if cohort_val < threshold else "control"

        if experiment and experiment.state == ExperimentState.SIMULATION_VALIDATED:
            experiment.state = ExperimentState.CANARY_ACTIVE
            self._persist_journal_entry(
                experiment,
                "CANARY_ACTIVATED",
                {"first_cycle_id": cycle_id, "first_cohort": cohort},
            )

        return cohort

    # ------------------------------------------------------------------------
    # Outcome Recording & EWMA Aggregation
    # ------------------------------------------------------------------------

    def record_cycle_outcome(
        self,
        experiment_id: str,
        cohort: str,
        composite_reward: float,
        per_dimension_metrics: Optional[Dict[str, float]] = None,
        cycle_id: Optional[Union[int, str]] = None,
    ) -> Tuple[ExperimentState, Dict[str, Any]]:
        """Ingests cycle feedback, updates telemetry accumulators, and triggers evaluation."""
        if experiment_id not in self.experiments:
            raise KeyError(f"Unknown experiment_id: {experiment_id}")

        experiment = self.experiments[experiment_id]
        if experiment.state not in (
            ExperimentState.SIMULATION_VALIDATED,
            ExperimentState.CANARY_ACTIVE,
        ):
            logger.debug(
                "Experiment %s is in state %s; recording telemetry without state transition.",
                experiment_id,
                experiment.state.value,
            )

        reward = float(composite_reward)
        if cohort == "experimental":
            experiment.experimental_rewards.append(reward)
        else:
            experiment.control_rewards.append(reward)

        # Update per-dimension EWMA
        metrics = per_dimension_metrics or {}
        ewma_target = (
            experiment.experimental_metrics_ewma
            if cohort == "experimental"
            else experiment.control_metrics_ewma
        )
        for dim, val in metrics.items():
            prev = ewma_target.get(dim)
            ewma_target[dim] = compute_ewma(val, prev, alpha=0.15)

        experiment.cycles_elapsed += 1
        self.current_cycle += 1

        # Recompute SPRT Log-Likelihood Ratio
        self._compute_sprt_llr(experiment)

        # Monitor simulation-reality divergence
        self._check_simulation_divergence(experiment)

        # Evaluate stopping condition / boundary crossing
        decision_state, decision_payload = self.evaluate_experiment(experiment_id)

        self._persist_journal_entry(
            experiment,
            "CYCLE_OUTCOME_RECORDED",
            {
                "cycle_id": cycle_id,
                "cohort": cohort,
                "reward": reward,
                "metrics": metrics,
                "decision_state": decision_state.value,
            },
        )

        return decision_state, decision_payload

    # ------------------------------------------------------------------------
    # Sequential Probability Ratio Test (SPRT)
    # ------------------------------------------------------------------------

    def _compute_sprt_llr(self, experiment: PolicyExperiment) -> float:
        """Computes the SPRT log-likelihood ratio for H1 (mean_diff = delta) vs H0 (mean_diff = 0).

        Model:
            Difference of cohort means D = mean(experimental) - mean(control).
            Under H0: D ~ N(0, var_diff)
            Under H1: D ~ N(delta, var_diff)
            LLR = (delta / var_diff) * (D - delta / 2)
        """
        n_e = len(experiment.experimental_rewards)
        n_c = len(experiment.control_rewards)

        if (
            n_e < self.min_samples_for_decision
            or n_c < self.min_samples_for_decision
        ):
            experiment.sprt_log_likelihood_ratio = 0.0
            return 0.0

        m_e = statistics.mean(experiment.experimental_rewards)
        m_c = statistics.mean(experiment.control_rewards)
        diff = m_e - m_c

        v_e = (
            statistics.variance(experiment.experimental_rewards)
            if n_e > 1
            else 0.01
        )
        v_c = statistics.variance(experiment.control_rewards) if n_c > 1 else 0.01

        # Pooled sample variance
        pooled_num = (n_e - 1) * v_e + (n_c - 1) * v_c
        pooled_den = max(1, n_e + n_c - 2)
        pooled_var = max(1e-6, pooled_num / pooled_den)

        var_diff = max(1e-6, pooled_var * ((1.0 / n_e) + (1.0 / n_c)))

        # Expected improvement magnitude delta
        sim_pred = experiment.simulation_predicted_reward
        delta = sim_pred.get("predicted_improvement")
        if delta is None or delta <= 0.0:
            pred_mean = sim_pred.get("mean", 1.0)
            delta = max(0.05, pred_mean - m_c)
        delta = max(0.02, float(delta))

        llr = (delta / var_diff) * (diff - (delta / 2.0))
        # Clamp to avoid numerical overflow in extreme distributions
        llr = max(-100.0, min(100.0, llr))

        experiment.sprt_log_likelihood_ratio = llr
        return llr

    # ------------------------------------------------------------------------
    # Simulation-Reality Divergence (Cramér-von Mises)
    # ------------------------------------------------------------------------

    def _check_simulation_divergence(
        self, experiment: PolicyExperiment
    ) -> Tuple[float, bool]:
        """Calculates CvM divergence between simulation predictions and observed live rewards."""
        if len(experiment.experimental_rewards) < 3:
            return 0.0, False

        sim_pred = experiment.simulation_predicted_reward
        theo_mean = sim_pred.get("mean", 1.0)
        theo_var = max(1e-6, sim_pred.get("variance", 0.25))

        cvm_stat, crit_val, is_div = compute_cramer_von_mises(
            experiment.experimental_rewards, theo_mean, theo_var
        )
        experiment.cvm_statistic = cvm_stat

        if is_div:
            logger.warning(
                "[SIMULATION_DIVERGENCE] Experiment %s observed rewards diverge from "
                "simulation prediction (CvM=%.4f > crit=%.4f). Workload drift indicated.",
                experiment.experiment_id,
                cvm_stat,
                crit_val,
            )
            self._persist_journal_entry(
                experiment,
                "SIMULATION_DIVERGENCE",
                {
                    "cvm_statistic": cvm_stat,
                    "critical_value": crit_val,
                    "samples_observed": len(experiment.experimental_rewards),
                },
            )

        return cvm_stat, is_div

    # ------------------------------------------------------------------------
    # Sequential Decision Gate: Promotion, Rollback & Expiration
    # ------------------------------------------------------------------------

    def evaluate_experiment(
        self, experiment_id: str
    ) -> Tuple[ExperimentState, Dict[str, Any]]:
        """Evaluates SPRT boundaries and time limits to decide promotion or rollback."""
        experiment = self.experiments[experiment_id]

        if experiment.state not in (
            ExperimentState.SIMULATION_VALIDATED,
            ExperimentState.CANARY_ACTIVE,
        ):
            return experiment.state, {
                "action": "NONE",
                "state": experiment.state.value,
                "reason": "Experiment inactive",
            }

        llr = experiment.sprt_log_likelihood_ratio
        n_e = len(experiment.experimental_rewards)
        n_c = len(experiment.control_rewards)

        # Minimum sample threshold check before making irreversible transitions
        if (
            n_e < self.min_samples_for_decision
            or n_c < self.min_samples_for_decision
        ):
            return experiment.state, {
                "action": "CONTINUE",
                "state": experiment.state.value,
                "reason": (
                    f"Insufficient samples (exp={n_e}, ctrl={n_c}, min={self.min_samples_for_decision})"
                ),
                "sprt_llr": llr,
            }

        # SPRT Promotion Boundary Crossed
        if llr >= self.promote_boundary:
            directive = self._graduate_policy(experiment)
            return experiment.state, directive

        # SPRT Rollback Boundary Crossed
        if llr <= self.rollback_boundary:
            directive = self._rollback_policy(
                experiment, reason="SPRT_ROLLBACK_BOUNDARY_CROSSED"
            )
            return experiment.state, directive

        # Expiration without definitive separation
        if experiment.cycles_elapsed >= experiment.max_duration_cycles:
            experiment.state = ExperimentState.EXPIRED
            directive = self._rollback_policy(
                experiment, reason="MAX_DURATION_EXPIRED_NO_DECISION"
            )
            return experiment.state, directive

        return experiment.state, {
            "action": "CONTINUE",
            "state": experiment.state.value,
            "sprt_llr": llr,
            "promote_boundary": self.promote_boundary,
            "rollback_boundary": self.rollback_boundary,
            "cycles_remaining": experiment.max_duration_cycles
            - experiment.cycles_elapsed,
        }

    # ------------------------------------------------------------------------
    # Policy Graduation
    # ------------------------------------------------------------------------

    def _graduate_policy(
        self, experiment: PolicyExperiment
    ) -> Dict[str, Any]:
        """Promotes experimental parameters to the primary production configuration."""
        experiment.state = ExperimentState.GRADUATED
        experiment.decision_reason = "SPRT_PROMOTION_CONFIRMED"

        m_e = statistics.mean(experiment.experimental_rewards)
        m_c = statistics.mean(experiment.control_rewards)
        observed_improvement = m_e - m_c

        t_stat, p_val, df = compute_welch_t_test(
            experiment.experimental_rewards, experiment.control_rewards
        )

        param_diffs: Dict[str, Dict[str, Any]] = {}
        for k, v in experiment.proposed_params.items():
            inc = experiment.incumbent_params.get(k)
            delta = None
            if isinstance(v, (int, float)) and isinstance(inc, (int, float)):
                delta = v - inc
            param_diffs[k] = {"incumbent": inc, "graduated": v, "delta": delta}

        grad_hash = hashlib.sha256(
            json.dumps(experiment.proposed_params, sort_keys=True).encode()
        ).hexdigest()[:8]
        graduated_version = f"policy_grad_{grad_hash}"
        experiment.graduated_policy_version = graduated_version

        lineage_entry = {
            "experiment_id": experiment.experiment_id,
            "parent_policy_version": experiment.parent_policy_version,
            "graduated_policy_version": graduated_version,
            "parameter_diffs": param_diffs,
            "observed_improvement": round(observed_improvement, 6),
            "welch_t_statistic": round(t_stat, 4),
            "welch_p_value": round(p_val, 6),
            "degrees_of_freedom": round(df, 2),
            "cycles_to_decision": experiment.cycles_elapsed,
            "final_sprt_llr": round(experiment.sprt_log_likelihood_ratio, 6),
            "graduated_at": datetime.utcnow().isoformat() + "Z",
            "provenance": "experimentally_validated",
        }

        self.policy_lineage.append(lineage_entry)
        self._save_policy_lineage()

        directive = {
            "directive_type": "PROMOTE_CANARY_POLICY",
            "experiment_id": experiment.experiment_id,
            "parent_policy_version": experiment.parent_policy_version,
            "graduated_policy_version": graduated_version,
            "active_parameters": experiment.proposed_params,
            "parameter_diffs": param_diffs,
            "observed_improvement": observed_improvement,
            "welch_p_value": p_val,
            "cycles_elapsed": experiment.cycles_elapsed,
            "sprt_llr": experiment.sprt_log_likelihood_ratio,
            "provenance": "experimentally_validated",
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }

        self._persist_journal_entry(
            experiment, "POLICY_GRADUATED", {"directive": directive}
        )
        logger.info(
            "Experiment %s GRADUATED to %s (LLR=%.4f >= %.4f, delta=%.4f, p=%.4f)",
            experiment.experiment_id,
            graduated_version,
            experiment.sprt_log_likelihood_ratio,
            self.promote_boundary,
            observed_improvement,
            p_val,
        )

        return directive

    # ------------------------------------------------------------------------
    # Rollback & Parameter Immunization
    # ------------------------------------------------------------------------

    def _rollback_policy(
        self, experiment: PolicyExperiment, reason: str
    ) -> Dict[str, Any]:
        """Rolls back canary parameters and registers parameter exclusion zones."""
        if experiment.state != ExperimentState.EXPIRED:
            experiment.state = ExperimentState.ROLLED_BACK
        experiment.decision_reason = reason

        m_e = (
            statistics.mean(experiment.experimental_rewards)
            if experiment.experimental_rewards
            else 0.0
        )
        m_c = (
            statistics.mean(experiment.control_rewards)
            if experiment.control_rewards
            else 0.0
        )
        deficit = m_c - m_e

        # Construct Axis-Aligned Parameter Exclusion Zones:
        # [proposed - 0.1 * abs(proposed - incumbent), proposed + 0.1 * abs(proposed - incumbent)]
        created_zones = []
        for param_name, proposed_val in experiment.proposed_params.items():
            incumbent_val = experiment.incumbent_params.get(
                param_name, proposed_val
            )
            if isinstance(proposed_val, (int, float)) and isinstance(
                incumbent_val, (int, float)
            ):
                span = abs(proposed_val - incumbent_val)
                margin = (
                    0.10 * span
                    if span > 1e-6
                    else max(0.05 * abs(proposed_val), 0.01)
                )
                zone = {
                    "parameter": param_name,
                    "min_value": proposed_val - margin,
                    "max_value": proposed_val + margin,
                    "proposed_value": proposed_val,
                    "incumbent_value": incumbent_val,
                    "expiry_cycle": self.current_cycle
                    + self.immunization_cooldown_cycles,
                    "experiment_id": experiment.experiment_id,
                    "created_at": datetime.utcnow().isoformat() + "Z",
                }
                self.immunization_registry.append(zone)
                created_zones.append(zone)

        self._save_immunization_registry()

        directive = {
            "directive_type": "ROLLBACK_CANARY_POLICY",
            "experiment_id": experiment.experiment_id,
            "revert_parameters": experiment.incumbent_params,
            "observed_deficit": deficit,
            "cycles_elapsed": experiment.cycles_elapsed,
            "reason": reason,
            "immunization_zones_created": len(created_zones),
            "cooldown_cycles": self.immunization_cooldown_cycles,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }

        self._persist_journal_entry(
            experiment, "POLICY_ROLLED_BACK", {"directive": directive}
        )
        logger.info(
            "Experiment %s ROLLED BACK (%s, LLR=%.4f <= %.4f, deficit=%.4f, zones=%d)",
            experiment.experiment_id,
            reason,
            experiment.sprt_log_likelihood_ratio,
            self.rollback_boundary,
            deficit,
            len(created_zones),
        )

        return directive

    # ------------------------------------------------------------------------
    # Dashboard & Status Emitter
    # ------------------------------------------------------------------------

    def get_experiment_status(self) -> Dict[str, Any]:
        """Builds a structured status report for cockpit monitoring."""
        experiments_data = []

        for exp in self.experiments.values():
            n_e = len(exp.experimental_rewards)
            n_c = len(exp.control_rewards)

            m_e = statistics.mean(exp.experimental_rewards) if n_e > 0 else 0.0
            m_c = statistics.mean(exp.control_rewards) if n_c > 0 else 0.0
            diff = m_e - m_c

            ci_95 = [0.0, 0.0]
            if n_e >= 2 and n_c >= 2:
                v_e = statistics.variance(exp.experimental_rewards)
                v_c = statistics.variance(exp.control_rewards)
                se = math.sqrt((v_e / n_e) + (v_c / n_c))
                ci_95 = [round(diff - 1.96 * se, 4), round(diff + 1.96 * se, 4)]

            experiments_data.append(
                {
                    "experiment_id": exp.experiment_id,
                    "state": exp.state.value,
                    "cycles_elapsed": exp.cycles_elapsed,
                    "max_duration_cycles": exp.max_duration_cycles,
                    "traffic_fraction": exp.traffic_fraction,
                    "sprt_llr": round(exp.sprt_log_likelihood_ratio, 4),
                    "sprt_boundaries": {
                        "promote": round(self.promote_boundary, 4),
                        "rollback": round(self.rollback_boundary, 4),
                    },
                    "samples": {"experimental": n_e, "control": n_c},
                    "reward_means": {
                        "experimental": round(m_e, 4),
                        "control": round(m_c, 4),
                        "difference": round(diff, 4),
                    },
                    "difference_95_ci": ci_95,
                    "cvm_statistic": round(exp.cvm_statistic, 4),
                    "simulation_divergence_detected": (
                        exp.cvm_statistic > 0.7435
                    ),
                    "ewma_metrics": {
                        "control": {
                            k: round(v, 4)
                            for k, v in exp.control_metrics_ewma.items()
                        },
                        "experimental": {
                            k: round(v, 4)
                            for k, v in exp.experimental_metrics_ewma.items()
                        },
                    },
                    "created_at": exp.created_at,
                    "decision_reason": exp.decision_reason,
                    "graduated_version": exp.graduated_policy_version,
                }
            )

        active_count = sum(
            1
            for exp in self.experiments.values()
            if exp.state
            in (
                ExperimentState.SIMULATION_VALIDATED,
                ExperimentState.CANARY_ACTIVE,
            )
        )

        return {
            "current_cycle": self.current_cycle,
            "active_experiments_count": active_count,
            "total_experiments_count": len(self.experiments),
            "active_immunization_zones_count": len(
                [
                    z
                    for z in self.immunization_registry
                    if z.get("expiry_cycle", 0) > self.current_cycle
                ]
            ),
            "sprt_thresholds": {
                "alpha": self.sprt_alpha,
                "beta": self.sprt_beta,
                "promote_boundary": round(self.promote_boundary, 4),
                "rollback_boundary": round(self.rollback_boundary, 4),
            },
            "experiments": experiments_data,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }


# ============================================================================
# Self-Verification Execution Entry Point
# ============================================================================

if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as tmp_dir:
        test_cfg = {
            "experiment_journal_path": os.path.join(tmp_dir, "journal.jsonl"),
            "policy_lineage_path": os.path.join(tmp_dir, "lineage.json"),
            "immunization_registry_path": os.path.join(
                tmp_dir, "immunization.json"
            ),
            "traffic_fraction": 0.20,
            "max_duration_cycles": 15,
            "sprt_alpha": 0.05,
            "sprt_beta": 0.10,
        }

        controller = ExperimentController(test_cfg)

        # 1. Validation of proposal submission
        sim_report = {
            "p_value": 0.012,
            "predicted_reward": {
                "mean": 1.45,
                "variance": 0.15,
                "p5": 0.90,
                "p95": 1.95,
                "predicted_improvement": 0.35,
            },
        }
        proposed = {"worker_concurrency": 8, "rate_limit_token_bucket": 150}
        incumbent = {"worker_concurrency": 4, "rate_limit_token_bucket": 100}

        exp_id = controller.submit_proposal(
            proposed, sim_report, incumbent_params=incumbent
        )
        assert exp_id in controller.experiments
        assert (
            controller.experiments[exp_id].state
            == ExperimentState.SIMULATION_VALIDATED
        )

        # 2. Deterministic cohort assignment test
        c1 = controller.assign_cohort(exp_id, 1)
        c2 = controller.assign_cohort(exp_id, 1)
        assert c1 == c2, "Cohort assignment must be deterministic across identical inputs."

        # 3. Simulate sequential testing cycles leading to promotion
        promoted = False
        for cycle in range(1, 20):
            cohort = controller.assign_cohort(exp_id, cycle)
            reward = 1.40 if cohort == "experimental" else 1.05
            metrics = {
                "worker_utilization_ratio": 0.88,
                "token_budget_adherence": 0.99,
            }
            state, payload = controller.record_cycle_outcome(
                exp_id,
                cohort,
                reward,
                per_dimension_metrics=metrics,
                cycle_id=cycle,
            )
            if state == ExperimentState.GRADUATED:
                promoted = True
                assert (
                    payload.get("directive_type") == "PROMOTE_CANARY_POLICY"
                )
                break

        status = controller.get_experiment_status()
        assert status["total_experiments_count"] == 1
        assert len(status["experiments"]) == 1
        logger.info(
            "Self-verification test passed successfully (promoted=%s).",
            promoted,
        )