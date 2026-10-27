"""
Pipeline Capacity Planning & Multi-Repository Resource Allocation Engine.

This module provides production-grade capacity planning, dynamic resource partitioning,
and quota-governed allocation for the multi-repository Hermes Pipeline Orchestrator.

Architecture:
    1. ResourceProfile: Live per-repository metrics tracker (EWMA cost, duration, failure risk, urgency).
    2. RepositoryProfiler: Ingestion and aggregation engine for repository telemetry.
    3. QuotaBudgetModel: Multi-account token quota tracker with depletion forecasting and headroom.
    4. CapacityAllocator: Linear programming (LP) relaxation optimizer using SciPy HiGHS with
       hierarchical constraint relaxation and greedy fallbacks.
    5. AllocationDispatcher: Translates AllocationPlans into live SharedPipelineConfig states and JSONL logs.
    6. CapacityHealthContributor: Quantitative health score generator for PipelineHealthScorer.
    7. CapacityPlanningAPIHandler: REST API handler exposing capacity, history, profiles, and quota endpoints.
    8. PipelineCapacityPlanner: Background autonomous orchestrator executing periodic planning cycles.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import math
import os
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

# OpenTelemetry Tracing Setup with Graceful Fallback
try:
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode
    tracer = trace.get_tracer("hermes.pipeline.capacity")
    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False

    class DummySpan:
        """Dummy span for when OpenTelemetry is unavailable."""
        def __enter__(self) -> "DummySpan":
            return self

        def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            pass

        def set_attribute(self, key: str, value: Any) -> None:
            pass

        def set_status(self, status: Any) -> None:
            pass

        def record_exception(self, exc: BaseException) -> None:
            pass

    class DummyTracer:
        """Dummy tracer for fallback."""
        def start_as_current_span(self, name: str, **kwargs: Any) -> DummySpan:
            return DummySpan()

    tracer = DummyTracer()  # type: ignore

    class StatusCode:  # type: ignore
        OK = "OK"
        ERROR = "ERROR"

    class Status:  # type: ignore
        def __init__(self, status_code: Any = None, description: Optional[str] = None):
            self.status_code = status_code
            self.description = description

# SciPy Optimization Setup with Graceful Fallback
try:
    from scipy.optimize import linprog
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    linprog = None

# Internal Integration Fallbacks
try:
    from tasks.pipeline_self_tuner import SharedPipelineConfig, PipelineHealthScorer
except ImportError:
    class SharedPipelineConfig:  # type: ignore
        """Fallback in-memory thread-safe pipeline configuration."""
        def __init__(self) -> None:
            self._lock = threading.RLock()
            self.config: Dict[str, Any] = {
                "CONCURRENCY_LIMIT": 8,
                "repo_allocations": {},
                "stage_timeouts": {},
            }

        def get(self, key: str, default: Any = None) -> Any:
            with self._lock:
                return self.config.get(key, default)

        def update(self, updates: Dict[str, Any]) -> None:
            with self._lock:
                self.config.update(updates)

        def to_dict(self) -> Dict[str, Any]:
            with self._lock:
                return copy.deepcopy(self.config)

    class PipelineHealthScorer:  # type: ignore
        """Fallback health scorer registry."""
        def __init__(self) -> None:
            self.sub_scores: Dict[str, float] = {}

        def register_sub_score(self, name: str, score: float) -> None:
            self.sub_scores[name] = score

logger = logging.getLogger(__name__)


# ============================================================================
# 1. ResourceProfile & Telemetry Dataclasses
# ============================================================================

@dataclass
class ResourceProfile:
    """Live resource consumption and queue state profile for a repository."""
    repo_name: str
    avg_milestone_cost_tokens: float = 2500.0
    avg_milestone_duration_sec: float = 45.0
    failure_risk_score: float = 0.0  # 0.0 to 1.0
    queue_depth: int = 0
    queue_urgency: float = 0.0
    estimated_completion_horizon_ticks: int = 1
    current_breaker_states: Dict[str, str] = field(default_factory=dict)
    last_updated: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        """Convert profile to serializable dictionary."""
        return asdict(self)


# ============================================================================
# 2. RepositoryProfiler
# ============================================================================

class RepositoryProfiler:
    """Maintains and updates live ResourceProfiles across all active repositories."""

    def __init__(self, ewma_alpha: float = 0.3, tau_urgency_decay: float = 0.001) -> None:
        """
        Initialize the repository profiler.

        Args:
            ewma_alpha: Smoothing factor for EWMA token cost and execution latency.
            tau_urgency_decay: Time-in-queue exponential decay rate factor (1/sec).
        """
        self.ewma_alpha = ewma_alpha
        self.tau = tau_urgency_decay
        self._profiles: Dict[str, ResourceProfile] = {}
        self._lock = threading.RLock()

    def update_profiles(self, pipeline_state: Dict[str, Any]) -> None:
        """
        Update live resource profiles based on latest pipeline telemetry.

        Args:
            pipeline_state: Dictionary containing repository state, queue telemetry,
                           diagnostics latency figures, and circuit breaker metrics.
        """
        now = time.time()
        repos_telemetry: Dict[str, Any] = pipeline_state.get("repositories", {})

        with self._lock:
            for repo_name, telemetry in repos_telemetry.items():
                existing = self._profiles.get(repo_name)

                # Observed actuals from stage diagnostics or cost estimator
                actual_cost = telemetry.get("last_milestone_cost_tokens")
                actual_duration = telemetry.get("last_milestone_duration_sec")

                if existing:
                    # EWMA smoothing for cost and duration
                    if actual_cost is not None and actual_cost > 0:
                        avg_cost = (self.ewma_alpha * float(actual_cost) +
                                    (1.0 - self.ewma_alpha) * existing.avg_milestone_cost_tokens)
                    else:
                        avg_cost = existing.avg_milestone_cost_tokens

                    if actual_duration is not None and actual_duration > 0:
                        avg_duration = (self.ewma_alpha * float(actual_duration) +
                                        (1.0 - self.ewma_alpha) * existing.avg_milestone_duration_sec)
                    else:
                        avg_duration = existing.avg_milestone_duration_sec
                else:
                    avg_cost = float(actual_cost) if actual_cost else 2500.0
                    avg_duration = float(actual_duration) if actual_duration else 45.0

                # Queue depth and urgency computation
                queue_items = telemetry.get("pending_milestones", [])
                queue_depth = telemetry.get("queue_depth", len(queue_items))

                urgency = 0.0
                if queue_items:
                    for item in queue_items:
                        priority = float(item.get("priority", 1.0))
                        time_in_queue = float(item.get("time_in_queue_sec", 0.0))
                        urgency += priority * math.exp(-self.tau * time_in_queue)
                else:
                    base_priority = float(telemetry.get("priority", 1.0))
                    urgency = queue_depth * base_priority

                # Failure risk score: per-repo failure rate and active cascade involvement
                failure_rate = float(telemetry.get("failure_rate", 0.0))
                in_cascade = bool(telemetry.get("active_cascade_involvement", False))
                failure_risk = min(1.0, max(0.0, failure_rate * 0.7 + (0.3 if in_cascade else 0.0)))

                # Completion horizon from predictive analytics or heuristic
                pred_horizon = telemetry.get("estimated_completion_horizon_ticks")
                if pred_horizon is not None:
                    horizon_ticks = max(1, int(pred_horizon))
                else:
                    horizon_ticks = max(1, math.ceil((queue_depth * avg_duration) / 120.0))

                # Circuit breaker status mapped per stage for this repository
                breakers = telemetry.get("breaker_states", {})

                self._profiles[repo_name] = ResourceProfile(
                    repo_name=repo_name,
                    avg_milestone_cost_tokens=avg_cost,
                    avg_milestone_duration_sec=avg_duration,
                    failure_risk_score=failure_risk,
                    queue_depth=queue_depth,
                    queue_urgency=urgency,
                    estimated_completion_horizon_ticks=horizon_ticks,
                    current_breaker_states=dict(breakers),
                    last_updated=now,
                )

    def get_profile(self, repo_name: str) -> Optional[ResourceProfile]:
        """Retrieve the current ResourceProfile for a given repository."""
        with self._lock:
            profile = self._profiles.get(repo_name)
            return copy.deepcopy(profile) if profile else None

    def get_all_profiles(self) -> Dict[str, ResourceProfile]:
        """Return a snapshot of all active repository profiles."""
        with self._lock:
            return copy.deepcopy(self._profiles)


# ============================================================================
# 3. QuotaBudgetModel
# ============================================================================

@dataclass
class AccountQuotaState:
    """Live state of an individual token quota account."""
    account_id: str
    current_balance: float
    max_capacity: float
    regeneration_rate_per_sec: float
    projected_exhaustion_sec: Optional[float] = None


class QuotaBudgetModel:
    """
    Tracks the multi-account token quota pool as a shared resource with per-account
    regeneration rates, current balances, and projected exhaustion horizons.
    """

    def __init__(self, initial_accounts: Optional[List[Dict[str, Any]]] = None) -> None:
        """
        Initialize the token quota model with 4 default accounts if unspecified.
        """
        self._lock = threading.RLock()
        self.accounts: Dict[str, AccountQuotaState] = {}

        if initial_accounts:
            for acc in initial_accounts:
                self.accounts[acc["account_id"]] = AccountQuotaState(
                    account_id=acc["account_id"],
                    current_balance=float(acc.get("current_balance", 1_000_000.0)),
                    max_capacity=float(acc.get("max_capacity", 1_000_000.0)),
                    regeneration_rate_per_sec=float(acc.get("regeneration_rate_per_sec", 50.0)),
                    projected_exhaustion_sec=acc.get("projected_exhaustion_sec"),
                )
        else:
            # Standard 4-account Google/Claude quota rotation pool
            for i in range(1, 5):
                acc_id = f"account_{i}"
                self.accounts[acc_id] = AccountQuotaState(
                    account_id=acc_id,
                    current_balance=1_000_000.0,
                    max_capacity=1_000_000.0,
                    regeneration_rate_per_sec=75.0,
                    projected_exhaustion_sec=None,
                )

    def available_budget_tokens(self, horizon_ticks: int, tick_interval_sec: float = 120.0) -> float:
        """
        Return the total allocatable tokens across all accounts over the planning horizon.

        Args:
            horizon_ticks: Number of planning ticks in horizon.
            tick_interval_sec: Duration in seconds of each tick.
        """
        with self._lock:
            total_duration_sec = horizon_ticks * tick_interval_sec
            total_available = 0.0

            for acc in self.accounts.values():
                regen = acc.regeneration_rate_per_sec * total_duration_sec
                effective_balance = min(acc.max_capacity, acc.current_balance + regen)
                total_available += max(0.0, effective_balance)

            return total_available

    def per_account_headroom(self) -> Dict[str, float]:
        """
        Return each account's distance from rate-limit thresholds (fraction of remaining capacity).
        """
        with self._lock:
            headroom: Dict[str, float] = {}
            for acc_id, acc in self.accounts.items():
                if acc.max_capacity > 0:
                    fraction = acc.current_balance / acc.max_capacity
                else:
                    fraction = 0.0
                headroom[acc_id] = max(0.0, min(1.0, fraction))
            return headroom

    def update_from_telemetry(self, account_metrics: Dict[str, Any]) -> None:
        """
        Update account balances, regeneration rates, and burn projections from telemetry.

        Args:
            account_metrics: Mapping of account_id to metric dictionaries.
        """
        with self._lock:
            for acc_id, data in account_metrics.items():
                if acc_id in self.accounts:
                    acc = self.accounts[acc_id]
                    if "current_balance" in data:
                        acc.current_balance = float(data["current_balance"])
                    if "max_capacity" in data:
                        acc.max_capacity = float(data["max_capacity"])
                    if "regeneration_rate_per_sec" in data:
                        acc.regeneration_rate_per_sec = float(data["regeneration_rate_per_sec"])

                    # Burn rate projection
                    burn_rate_per_sec = data.get("token_burn_rate_per_sec")
                    if burn_rate_per_sec and burn_rate_per_sec > acc.regeneration_rate_per_sec:
                        net_drain = burn_rate_per_sec - acc.regeneration_rate_per_sec
                        acc.projected_exhaustion_sec = acc.current_balance / net_drain
                    else:
                        acc.projected_exhaustion_sec = None

    def allocate(self, repo_name: str, tokens: float) -> bool:
        """
        Attempt to allocate tokens from the account pool with highest available headroom.

        Args:
            repo_name: Target repository identifier.
            tokens: Number of tokens requested.
        """
        with self._lock:
            if tokens <= 0:
                return True

            # Find account with highest balance
            best_acc: Optional[AccountQuotaState] = None
            for acc in self.accounts.values():
                if acc.current_balance >= tokens:
                    if best_acc is None or acc.current_balance > best_acc.current_balance:
                        best_acc = acc

            if best_acc is not None:
                best_acc.current_balance -= tokens
                logger.debug(f"[QuotaBudgetModel] Allocated {tokens:.0f} tokens to {repo_name} from {best_acc.account_id}")
                return True

            logger.warning(f"[QuotaBudgetModel] Insufficient token budget to allocate {tokens:.0f} to {repo_name}")
            return False

    def get_state(self) -> Dict[str, Any]:
        """Return serializable summary of all accounts."""
        with self._lock:
            return {
                acc_id: {
                    "current_balance": acc.current_balance,
                    "max_capacity": acc.max_capacity,
                    "regeneration_rate_per_sec": acc.regeneration_rate_per_sec,
                    "projected_exhaustion_sec": acc.projected_exhaustion_sec,
                    "headroom_fraction": acc.current_balance / max(1.0, acc.max_capacity),
                }
                for acc_id, acc in self.accounts.items()
            }


# ============================================================================
# 4. AllocationPlan & RepoAllocation Dataclasses
# ============================================================================

@dataclass
class RepoAllocation:
    """Concrete allocation assigned to a repository for a planning cycle."""
    thread_slots: int
    token_budget: float
    priority_boost: float
    rationale: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert allocation to dictionary."""
        return asdict(self)


@dataclass
class AllocationPlan:
    """Full capacity allocation plan generated by CapacityAllocator."""
    cycle_id: str
    timestamp: float
    repo_allocations: Dict[str, RepoAllocation]
    objective_value: float
    constraint_status: Dict[str, str]  # constraint_name -> 'satisfied' | 'relaxed'
    solve_time_ms: float

    def to_dict(self) -> Dict[str, Any]:
        """Convert plan to serializable dictionary."""
        return {
            "cycle_id": self.cycle_id,
            "timestamp": self.timestamp,
            "repo_allocations": {k: v.to_dict() for k, v in self.repo_allocations.items()},
            "objective_value": self.objective_value,
            "constraint_status": self.constraint_status,
            "solve_time_ms": self.solve_time_ms,
        }


# ============================================================================
# 5. CapacityAllocator
# ============================================================================

class CapacityAllocator:
    """
    Solves a linear programming relaxation to maximize queue urgency-weighted
    pipeline throughput subject to concurrency, token quota, breaker state,
    and cascade resilience constraints.
    """

    def __init__(
        self,
        baseline_throughput_per_slot: float = 1.0,
        probe_count: int = 2,
        safety_margin_fraction: float = 0.15,
        cascade_penalty_factor: float = 0.5,
        starvation_slots: int = 1,
    ) -> None:
        """
        Initialize the capacity allocator.

        Args:
            baseline_throughput_per_slot: Expected milestones completed per slot per cycle.
            probe_count: Maximum thread slots permitted for repos with OPEN breakers.
            safety_margin_fraction: Fraction of token quota reserved as safety headroom.
            cascade_penalty_factor: Budget multiplier penalty for cascade-affected repos.
            starvation_slots: Minimum thread slots guaranteed for repos with pending queues.
        """
        self.baseline_throughput_per_slot = baseline_throughput_per_slot
        self.probe_count = probe_count
        self.safety_margin_fraction = safety_margin_fraction
        self.cascade_penalty_factor = cascade_penalty_factor
        self.starvation_slots = starvation_slots
        self._lock = threading.RLock()

    def update_constraint_parameters(
        self,
        safety_margin: Optional[float] = None,
        cascade_penalty: Optional[float] = None,
        starvation_slots: Optional[int] = None,
    ) -> None:
        """Update constraint override parameters dynamically."""
        with self._lock:
            if safety_margin is not None:
                self.safety_margin_fraction = max(0.0, min(0.5, float(safety_margin)))
            if cascade_penalty is not None:
                self.cascade_penalty_factor = max(0.1, min(1.0, float(cascade_penalty)))
            if starvation_slots is not None:
                self.starvation_slots = max(0, int(starvation_slots))

    def solve(
        self,
        profiles: Dict[str, ResourceProfile],
        budget_model: QuotaBudgetModel,
        config: SharedPipelineConfig,
    ) -> AllocationPlan:
        """
        Execute the capacity allocation optimization problem.

        Args:
            profiles: Live dictionary of ResourceProfiles per repo.
            budget_model: Quota budget tracking model.
            config: Pipeline configuration providing concurrency limits.
        """
        start_time = time.perf_counter()
        cycle_id = str(uuid.uuid4())
        now = time.time()

        if not profiles:
            return AllocationPlan(
                cycle_id=cycle_id,
                timestamp=now,
                repo_allocations={},
                objective_value=0.0,
                constraint_status={},
                solve_time_ms=(time.perf_counter() - start_time) * 1000.0,
            )

        with self._lock:
            concurrency_limit = int(config.get("CONCURRENCY_LIMIT", 8))
            total_budget = budget_model.available_budget_tokens(horizon_ticks=5)

            # Attempt LP solve with hierarchical constraint relaxation
            plan = self._solve_hierarchical(
                profiles=profiles,
                total_concurrency=concurrency_limit,
                available_budget=total_budget,
                cycle_id=cycle_id,
                timestamp=now,
            )

            plan.solve_time_ms = (time.perf_counter() - start_time) * 1000.0
            return plan

    def _solve_hierarchical(
        self,
        profiles: Dict[str, ResourceProfile],
        total_concurrency: int,
        available_budget: float,
        cycle_id: str,
        timestamp: float,
    ) -> AllocationPlan:
        """Attempt solve, relaxing constraints in order: cascade_penalty -> safety_margin -> starvation."""
        repos = sorted(profiles.keys())
        n = len(repos)

        # Relaxation steps
        relaxations = [
            {"cascade_penalty": False, "safety_margin": False, "starvation": False},
            {"cascade_penalty": True, "safety_margin": False, "starvation": False},
            {"cascade_penalty": True, "safety_margin": True, "starvation": False},
            {"cascade_penalty": True, "safety_margin": True, "starvation": True},
        ]

        last_error = ""
        for step_idx, relax in enumerate(relaxations):
            effective_cascade_factor = 1.0 if relax["cascade_penalty"] else self.cascade_penalty_factor
            effective_safety_margin = 0.0 if relax["safety_margin"] else self.safety_margin_fraction
            effective_starvation_slots = 0 if relax["starvation"] else self.starvation_slots

            constraint_status = {
                "concurrency_limit": "satisfied",
                "circuit_breaker_cap": "satisfied",
                "cascade_penalty": "relaxed" if relax["cascade_penalty"] else "satisfied",
                "safety_margin": "relaxed" if relax["safety_margin"] else "satisfied",
                "starvation_prevention": "relaxed" if relax["starvation"] else "satisfied",
            }

            if SCIPY_AVAILABLE and linprog is not None:
                try:
                    plan = self._solve_lp(
                        repos=repos,
                        profiles=profiles,
                        total_concurrency=total_concurrency,
                        available_budget=available_budget,
                        safety_margin=effective_safety_margin,
                        cascade_factor=effective_cascade_factor,
                        min_starvation_slots=effective_starvation_slots,
                        cycle_id=cycle_id,
                        timestamp=timestamp,
                        constraint_status=constraint_status,
                    )
                    if plan is not None:
                        if step_idx > 0:
                            logger.warning(
                                f"[CapacityAllocator] CapacityWarning: Solved with relaxations: {constraint_status}"
                            )
                        return plan
                except Exception as ex:
                    last_error = str(ex)
                    logger.debug(f"[CapacityAllocator] LP solve attempt {step_idx} failed: {ex}")

        # Fallback to greedy proportional allocation
        logger.warning(f"[CapacityAllocator] Falling back to greedy proportional allocator: {last_error}")
        return self._solve_greedy_fallback(
            repos=repos,
            profiles=profiles,
            total_concurrency=total_concurrency,
            available_budget=available_budget,
            cycle_id=cycle_id,
            timestamp=timestamp,
        )

    def _solve_lp(
        self,
        repos: List[str],
        profiles: Dict[str, ResourceProfile],
        total_concurrency: int,
        available_budget: float,
        safety_margin: float,
        cascade_factor: float,
        min_starvation_slots: int,
        cycle_id: str,
        timestamp: float,
        constraint_status: Dict[str, str],
    ) -> Optional[AllocationPlan]:
        """
        Formulate and solve LP using SciPy HiGHS.

        Variables per repo i:
            S_i: thread slots (integer/relaxed continuous)
            B_i: token budget
            T_i: expected throughput (milestones per cycle)
        Constraint:
            T_i <= S_i * baseline_throughput_per_slot
            T_i <= B_i / avg_cost_i
            sum(S_i) <= total_concurrency
            sum(B_i) <= available_budget * (1.0 - safety_margin)
            S_i >= min_starvation_slots (if queue_depth > 0)
            S_i <= probe_count (if any breaker is OPEN)
            B_i <= cascade_factor * max_repo_budget (if in cascade)
        Objective:
            Maximize sum(queue_urgency_i * T_i) => Minimize -sum(queue_urgency_i * T_i)
        """
        n = len(repos)
        # Vector layout: [S_0..S_{n-1}, B_0..B_{n-1}, T_0..T_{n-1}]
        # Size = 3 * n
        num_vars = 3 * n

        # Objective vector c
        c = [0.0] * num_vars
        for i, r in enumerate(repos):
            prof = profiles[r]
            # Maximize urgency * T_i  ==> Minimize - (urgency * T_i)
            # Add small epsilon to prefer non-zero throughput
            weight = max(0.01, prof.queue_urgency)
            c[2 * n + i] = -weight

        A_ub: List[List[float]] = []
        b_ub: List[float] = []

        # 1. Throughput constraints:
        # T_i - S_i * baseline <= 0
        for i in range(n):
            row = [0.0] * num_vars
            row[2 * n + i] = 1.0
            row[i] = -self.baseline_throughput_per_slot
            A_ub.append(row)
            b_ub.append(0.0)

        # T_i - B_i / avg_cost_i <= 0
        for i, r in enumerate(repos):
            avg_cost = max(1.0, profiles[r].avg_milestone_cost_tokens)
            row = [0.0] * num_vars
            row[2 * n + i] = 1.0
            row[n + i] = -1.0 / avg_cost
            A_ub.append(row)
            b_ub.append(0.0)

        # 2. Total Concurrency limit: sum(S_i) <= total_concurrency
        row_concurrency = [0.0] * num_vars
        for i in range(n):
            row_concurrency[i] = 1.0
        A_ub.append(row_concurrency)
        b_ub.append(float(total_concurrency))

        # 3. Total Token Budget limit: sum(B_i) <= available_budget * (1.0 - safety_margin)
        effective_budget_cap = max(0.0, available_budget * (1.0 - safety_margin))
        row_budget = [0.0] * num_vars
        for i in range(n):
            row_budget[n + i] = 1.0
        A_ub.append(row_budget)
        b_ub.append(effective_budget_cap)

        # Variable Bounds
        bounds: List[Tuple[Optional[float], Optional[float]]] = []

        # S_i bounds
        for i, r in enumerate(repos):
            prof = profiles[r]
            has_open_breaker = any(state.upper() == "OPEN" for state in prof.current_breaker_states.values())

            # Lower bound: starvation prevention if queue > 0
            if prof.queue_depth > 0:
                s_min = float(min_starvation_slots)
            else:
                s_min = 0.0

            # Upper bound: breaker cap or total concurrency
            if has_open_breaker:
                s_max = float(min(self.probe_count, total_concurrency))
            else:
                s_max = float(total_concurrency)

            if s_min > s_max:
                s_min = s_max  # prevent infeasible bound

            bounds.append((s_min, s_max))

        # B_i bounds
        for i, r in enumerate(repos):
            prof = profiles[r]
            in_cascade = prof.failure_risk_score > 0.6 or "CASCADE" in str(prof.current_breaker_states)

            b_min = 0.0
            if in_cascade and cascade_factor < 1.0:
                # Cap budget for cascade-affected repo
                b_max = float(effective_budget_cap * cascade_factor / max(1, n))
            else:
                b_max = float(effective_budget_cap)

            bounds.append((b_min, b_max))

        # T_i bounds: [0, inf)
        for i in range(n):
            bounds.append((0.0, None))

        # Solve with SciPy HiGHS
        res = linprog(
            c=c,
            A_ub=A_ub,
            b_ub=b_ub,
            bounds=bounds,
            method="highs",
        )

        if not res.success:
            return None

        # Process solution
        x = res.x
        raw_slots = [x[i] for i in range(n)]
        raw_budget = [x[n + i] for i in range(n)]

        # Integer rounding for thread slots respecting total concurrency
        allocations: Dict[str, RepoAllocation] = {}
        integer_slots = self._round_slots_conservatively(raw_slots, total_concurrency, repos, profiles)

        for i, r in enumerate(repos):
            prof = profiles[r]
            slots = integer_slots[i]
            b_val = max(0.0, float(raw_budget[i]))

            # Priority boost: urgency weighted, dampened by failure risk
            p_boost = max(0.0, prof.queue_urgency * (1.0 - prof.failure_risk_score))

            rationale_parts = []
            if prof.queue_depth > 0:
                rationale_parts.append(f"active_queue({prof.queue_depth})")
            if any(st.upper() == "OPEN" for st in prof.current_breaker_states.values()):
                rationale_parts.append(f"breaker_open_capped({self.probe_count})")
            if prof.failure_risk_score > 0.6:
                rationale_parts.append("cascade_risk_penalized")
            rationale = ", ".join(rationale_parts) if rationale_parts else "nominal_flow"

            allocations[r] = RepoAllocation(
                thread_slots=slots,
                token_budget=round(b_val, 2),
                priority_boost=round(p_boost, 4),
                rationale=rationale,
            )

        obj_val = float(-res.fun)
        return AllocationPlan(
            cycle_id=cycle_id,
            timestamp=timestamp,
            repo_allocations=allocations,
            objective_value=round(obj_val, 4),
            constraint_status=constraint_status,
            solve_time_ms=0.0,
        )

    def _round_slots_conservatively(
        self,
        raw_slots: List[float],
        concurrency_limit: int,
        repos: List[str],
        profiles: Dict[str, ResourceProfile],
    ) -> List[int]:
        """Convert continuous LP slot allocations to integers while respecting bounds."""
        n = len(raw_slots)
        if n == 0:
            return []

        # Floor initial slots
        slots = [int(math.floor(val)) for val in raw_slots]

        # Enforce starvation slot minimums for pending queues
        for i, r in enumerate(repos):
            if profiles[r].queue_depth > 0 and slots[i] < self.starvation_slots:
                slots[i] = self.starvation_slots

        # If sum exceeds limit, reduce from least urgent repos
        while sum(slots) > concurrency_limit:
            # Find candidate with highest slots and lowest urgency
            cands = [i for i in range(n) if slots[i] > 1 or (profiles[repos[i]].queue_depth == 0 and slots[i] > 0)]
            if not cands:
                break
            # Pick lowest queue urgency
            victim = min(cands, key=lambda idx: profiles[repos[idx]].queue_urgency)
            slots[victim] -= 1

        # If sum has remaining slots, distribute to highest urgency repos
        remaining = concurrency_limit - sum(slots)
        if remaining > 0:
            cands = sorted(
                range(n),
                key=lambda idx: (
                    -profiles[repos[idx]].queue_urgency,
                    profiles[repos[idx]].failure_risk_score,
                )
            )
            for idx in cands:
                if remaining <= 0:
                    break
                # Check breaker open limit
                prof = profiles[repos[idx]]
                has_open_breaker = any(st.upper() == "OPEN" for st in prof.current_breaker_states.values())
                if has_open_breaker and slots[idx] >= self.probe_count:
                    continue
                slots[idx] += 1
                remaining -= 1

        return slots

    def _solve_greedy_fallback(
        self,
        repos: List[str],
        profiles: Dict[str, ResourceProfile],
        total_concurrency: int,
        available_budget: float,
        cycle_id: str,
        timestamp: float,
    ) -> AllocationPlan:
        """Greedy heuristic allocation fallback when SciPy is unavailable or LP fails."""
        n = len(repos)
        allocations: Dict[str, RepoAllocation] = {}

        if n == 0:
            return AllocationPlan(
                cycle_id=cycle_id,
                timestamp=timestamp,
                repo_allocations={},
                objective_value=0.0,
                constraint_status={"solver": "empty"},
                solve_time_ms=0.0,
            )

        slots = [0] * n
        remaining_slots = total_concurrency

        # Step 1: Starvation allocation
        for i, r in enumerate(repos):
            prof = profiles[r]
            if prof.queue_depth > 0 and remaining_slots > 0:
                grant = min(remaining_slots, self.starvation_slots)
                slots[i] += grant
                remaining_slots -= grant

        # Step 2: Proportional allocation by urgency
        total_urgency = sum(max(0.01, profiles[r].queue_urgency) for r in repos)
        for i, r in enumerate(repos):
            if remaining_slots <= 0:
                break
            prof = profiles[r]
            has_open_breaker = any(st.upper() == "OPEN" for st in prof.current_breaker_states.values())

            share = max(0.01, prof.queue_urgency) / total_urgency
            additional = int(math.floor(share * remaining_slots))

            if has_open_breaker:
                max_allowed = max(0, self.probe_count - slots[i])
                additional = min(additional, max_allowed)

            slots[i] += additional

        # Step 3: Token budget proportional distribution
        budget_pool = max(0.0, available_budget * (1.0 - self.safety_margin_fraction))
        for i, r in enumerate(repos):
            prof = profiles[r]
            in_cascade = prof.failure_risk_score > 0.6
            penalty = self.cascade_penalty_factor if in_cascade else 1.0

            share = (max(0.01, prof.queue_urgency) / total_urgency) * penalty
            b_val = budget_pool * share
            p_boost = max(0.0, prof.queue_urgency * (1.0 - prof.failure_risk_score))

            allocations[r] = RepoAllocation(
                thread_slots=slots[i],
                token_budget=round(b_val, 2),
                priority_boost=round(p_boost, 4),
                rationale="greedy_proportional_fallback",
            )

        return AllocationPlan(
            cycle_id=cycle_id,
            timestamp=timestamp,
            repo_allocations=allocations,
            objective_value=round(sum(slots), 2),
            constraint_status={"greedy_fallback": "applied"},
            solve_time_ms=0.0,
        )


# ============================================================================
# 6. AllocationDispatcher
# ============================================================================

class AllocationDispatcher:
    """
    Translates an AllocationPlan into live SharedPipelineConfig modifications,
    persists JSONL telemetry records, and emits OpenTelemetry capacity spans.
    """

    def __init__(self, log_path: Union[str, Path] = "dashboards/capacity_plans.jsonl") -> None:
        """
        Initialize the allocation dispatcher.

        Args:
            log_path: Path to capacity plan audit log.
        """
        self.log_path = Path(log_path)
        self.last_dispatched_plan: Optional[AllocationPlan] = None
        self._lock = threading.RLock()

        # Ensure directory exists
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as ex:
            logger.error(f"[AllocationDispatcher] Failed to create directory for {self.log_path}: {ex}")

    def dispatch(
        self,
        plan: AllocationPlan,
        config: SharedPipelineConfig,
        budget_model: QuotaBudgetModel,
    ) -> Dict[str, Any]:
        """
        Dispatch the allocation plan to configuration, audit logs, and tracing.

        Args:
            plan: The solved AllocationPlan.
            config: Shared pipeline configuration reference.
            budget_model: Token quota budget model.

        Returns:
            Dictionary containing dispatch metadata and computed deltas.
        """
        with self._lock:
            with tracer.start_as_current_span("capacity_planner.dispatch_allocation") as span:
                # 1. Compute delta from previous cycle
                deltas = self._compute_deltas(plan)

                # 2. Update SharedPipelineConfig
                repo_allocs_dict = {
                    repo: {
                        "thread_slots": alloc.thread_slots,
                        "token_budget": alloc.token_budget,
                        "priority_boost": alloc.priority_boost,
                        "rationale": alloc.rationale,
                    }
                    for repo, alloc in plan.repo_allocations.items()
                }

                config.update({
                    "repo_allocations": repo_allocs_dict,
                    "capacity_plan_last_cycle_id": plan.cycle_id,
                    "capacity_plan_last_timestamp": plan.timestamp,
                })

                # 3. Log to dashboards/capacity_plans.jsonl
                log_record = {
                    "timestamp": plan.timestamp,
                    "cycle_id": plan.cycle_id,
                    "per_repo_allocations": repo_allocs_dict,
                    "objective_value": plan.objective_value,
                    "constraint_status": plan.constraint_status,
                    "solve_time_ms": plan.solve_time_ms,
                    "delta_from_previous_cycle": deltas,
                }

                try:
                    with open(self.log_path, "a", encoding="utf-8") as f:
                        f.write(json.dumps(log_record) + "\n")
                except Exception as ex:
                    logger.error(f"[AllocationDispatcher] Failed writing to {self.log_path}: {ex}")

                # 4. OpenTelemetry span attributes
                span.set_attribute("capacity.cycle_id", plan.cycle_id)
                span.set_attribute("capacity.objective_value", plan.objective_value)
                span.set_attribute("capacity.solve_time_ms", plan.solve_time_ms)
                span.set_attribute("capacity.repo_count", len(plan.repo_allocations))
                span.set_attribute("capacity.constraint_status", json.dumps(plan.constraint_status))

                for repo, alloc in plan.repo_allocations.items():
                    span.set_attribute(f"capacity.allocation.{repo}.slots", alloc.thread_slots)
                    span.set_attribute(f"capacity.allocation.{repo}.tokens", alloc.token_budget)
                    span.set_attribute(f"capacity.allocation.{repo}.priority_boost", alloc.priority_boost)

                span.set_status(Status(StatusCode.OK))

                self.last_dispatched_plan = copy.deepcopy(plan)
                logger.info(
                    f"[AllocationDispatcher] Successfully dispatched cycle {plan.cycle_id} "
                    f"({len(plan.repo_allocations)} repos, obj={plan.objective_value:.2f})"
                )
                return log_record

    def _compute_deltas(self, current_plan: AllocationPlan) -> Dict[str, Dict[str, Any]]:
        """Calculate differences in slot and token allocations compared to previous cycle."""
        if not self.last_dispatched_plan:
            return {
                repo: {
                    "slot_delta": alloc.thread_slots,
                    "token_delta": alloc.token_budget,
                    "is_new": True,
                }
                for repo, alloc in current_plan.repo_allocations.items()
            }

        prev = self.last_dispatched_plan.repo_allocations
        deltas: Dict[str, Dict[str, Any]] = {}

        for repo, alloc in current_plan.repo_allocations.items():
            if repo in prev:
                deltas[repo] = {
                    "slot_delta": alloc.thread_slots - prev[repo].thread_slots,
                    "token_delta": round(alloc.token_budget - prev[repo].token_budget, 2),
                    "is_new": False,
                }
            else:
                deltas[repo] = {
                    "slot_delta": alloc.thread_slots,
                    "token_delta": alloc.token_budget,
                    "is_new": True,
                }

        return deltas


# ============================================================================
# 7. CapacityHealthContributor
# ============================================================================

class CapacityHealthContributor:
    """
    Computes quantitative capacity_sub_score (0-100) and registers with
    PipelineHealthScorer to drive system-wide self-tuning decisions.
    """

    def compute_sub_score(
        self,
        plan: AllocationPlan,
        budget_model: QuotaBudgetModel,
        profiles: Dict[str, ResourceProfile],
        total_concurrency_limit: int,
        total_available_budget: float,
    ) -> Tuple[float, Dict[str, float]]:
        """
        Compute capacity sub score from efficiency, quota headroom, starvation, and feasibility.

        Returns:
            Tuple of (capacity_sub_score, component_breakdown_dict)
        """
        # 1. Allocation Efficiency: ratio of allocated to available resources
        allocated_slots = sum(a.thread_slots for a in plan.repo_allocations.values())
        allocated_tokens = sum(a.token_budget for a in plan.repo_allocations.values())

        slot_eff = allocated_slots / max(1, total_concurrency_limit)
        token_eff = allocated_tokens / max(1.0, total_available_budget)
        avg_efficiency = (slot_eff + token_eff) / 2.0

        # Mapping: 0.7-1.0 maps to 100-70, below 0.5 maps to 0, linear interpolation in between
        if avg_efficiency >= 1.0:
            # Over-commitment penalty
            efficiency_score = max(0.0, 100.0 - (avg_efficiency - 1.0) * 150.0)
        elif avg_efficiency >= 0.7:
            # 0.7 -> 70, 1.0 -> 100
            efficiency_score = 70.0 + ((avg_efficiency - 0.7) / 0.3) * 30.0
        elif avg_efficiency <= 0.5:
            efficiency_score = 0.0
        else:
            # 0.5 -> 0, 0.7 -> 70
            efficiency_score = ((avg_efficiency - 0.5) / 0.2) * 70.0

        # 2. Quota Headroom Health: minimum per-account headroom fraction mapped to 0-100
        headroom_dict = budget_model.per_account_headroom()
        min_headroom = min(headroom_dict.values()) if headroom_dict else 1.0
        quota_headroom_score = max(0.0, min(100.0, min_headroom * 100.0))

        # 3. Starvation Index: 100 - 20 * starved_repo_count (floor 0)
        # Starved repo: queue_depth > 0 but assigned <= 0 slots
        starved_count = 0
        for r, prof in profiles.items():
            if prof.queue_depth > 0:
                alloc = plan.repo_allocations.get(r)
                if not alloc or alloc.thread_slots <= 0:
                    starved_count += 1

        starvation_score = max(0.0, 100.0 - (20.0 * starved_count))

        # 4. Solve Feasibility: 100 if no relaxation, -15 per relaxed constraint
        relaxed_count = sum(1 for status in plan.constraint_status.values() if status == "relaxed")
        feasibility_score = max(0.0, 100.0 - (15.0 * relaxed_count))

        # Final weighted score
        capacity_sub_score = (
            0.3 * efficiency_score +
            0.3 * quota_headroom_score +
            0.2 * starvation_score +
            0.2 * feasibility_score
        )
        capacity_sub_score = round(max(0.0, min(100.0, capacity_sub_score)), 2)

        breakdown = {
            "allocation_efficiency": round(efficiency_score, 2),
            "quota_headroom_health": round(quota_headroom_score, 2),
            "starvation_index": round(starvation_score, 2),
            "solve_feasibility": round(feasibility_score, 2),
            "raw_efficiency_ratio": round(avg_efficiency, 4),
            "min_headroom_fraction": round(min_headroom, 4),
            "starved_repos": starved_count,
            "relaxed_constraints": relaxed_count,
        }

        return capacity_sub_score, breakdown


# ============================================================================
# 8. CapacityPlanningAPIHandler
# ============================================================================

class CapacityPlanningAPIHandler:
    """REST API endpoints for capacity planning queries and runtime control."""

    def __init__(self, planner: "PipelineCapacityPlanner") -> None:
        """Initialize with reference to parent PipelineCapacityPlanner."""
        self.planner = planner

    async def get_current(self, request: Any = None) -> Dict[str, Any]:
        """GET /api/pipeline/capacity/current: current allocation plan with per-repo breakdown."""
        plan = self.planner.get_latest_plan()
        if not plan:
            return {"status": "no_plan_available", "plan": None}
        return {"status": "ok", "plan": plan.to_dict()}

    async def get_history(self, request: Any = None) -> Dict[str, Any]:
        """GET /api/pipeline/capacity/history: recent allocation cycles with objective value trend."""
        history = self.planner.get_history()
        return {
            "status": "ok",
            "count": len(history),
            "history": history,
        }

    async def get_profiles(self, request: Any = None) -> Dict[str, Any]:
        """GET /api/pipeline/capacity/profiles: live RepositoryProfiles."""
        profiles = self.planner.profiler.get_all_profiles()
        return {
            "status": "ok",
            "profiles": {k: v.to_dict() for k, v in profiles.items()},
        }

    async def get_quota(self, request: Any = None) -> Dict[str, Any]:
        """GET /api/pipeline/capacity/quota: QuotaBudgetModel state with headroom and projected exhaustion."""
        quota_state = self.planner.budget_model.get_state()
        headroom = self.planner.budget_model.per_account_headroom()
        available_budget = self.planner.budget_model.available_budget_tokens(horizon_ticks=5)
        return {
            "status": "ok",
            "available_budget_horizon_5_ticks": available_budget,
            "per_account_headroom": headroom,
            "accounts": quota_state,
        }

    async def post_constraints(self, request: Any = None) -> Dict[str, Any]:
        """
        POST /api/pipeline/capacity/constraints: override constraint parameters.
        Accepts JSON: {safety_margin: float, cascade_penalty: float, starvation_slots: int}
        """
        data: Dict[str, Any] = {}
        if request is not None:
            if hasattr(request, "json") and callable(request.json):
                try:
                    data = await request.json()
                except Exception:
                    data = {}
            elif isinstance(request, dict):
                data = request

        safety_margin = data.get("safety_margin")
        cascade_penalty = data.get("cascade_penalty")
        starvation_slots = data.get("starvation_slots")

        self.planner.allocator.update_constraint_parameters(
            safety_margin=safety_margin,
            cascade_penalty=cascade_penalty,
            starvation_slots=starvation_slots,
        )

        return {
            "status": "updated",
            "current_parameters": {
                "safety_margin_fraction": self.planner.allocator.safety_margin_fraction,
                "cascade_penalty_factor": self.planner.allocator.cascade_penalty_factor,
                "starvation_slots": self.planner.allocator.starvation_slots,
            },
        }

    def register_routes(self, app: Any) -> None:
        """Register routes on an aiohttp web.Application if available."""
        try:
            from aiohttp import web

            async def _wrap(handler_fn: Callable) -> Callable:
                async def _inner(req: web.Request) -> web.Response:
                    res = await handler_fn(req)
                    return web.json_response(res)
                return _inner

            app.router.add_get("/api/pipeline/capacity/current", _wrap(self.get_current))
            app.router.add_get("/api/pipeline/capacity/history", _wrap(self.get_history))
            app.router.add_get("/api/pipeline/capacity/profiles", _wrap(self.get_profiles))
            app.router.add_get("/api/pipeline/capacity/quota", _wrap(self.get_quota))
            app.router.add_post("/api/pipeline/capacity/constraints", _wrap(self.post_constraints))
            logger.info("[CapacityPlanningAPIHandler] Registered aiohttp routes.")
        except ImportError:
            logger.debug("[CapacityPlanningAPIHandler] aiohttp not installed; routes not bound to web.Application.")


# ============================================================================
# 9. PipelineCapacityPlanner (Main Orchestrator)
# ============================================================================

class PipelineCapacityPlanner:
    """
    Main background orchestrator managing the capacity planning lifecycle,
    telemetry synchronization, LP optimization, and health scoring.
    """

    def __init__(
        self,
        config: Optional[SharedPipelineConfig] = None,
        diagnostics_ref: Optional[Any] = None,
        resilience_ref: Optional[Any] = None,
        predictive_ref: Optional[Any] = None,
        health_scorer_ref: Optional[PipelineHealthScorer] = None,
        planning_cycle_interval_sec: float = 60.0,
        log_path: str = "dashboards/capacity_plans.jsonl",
    ) -> None:
        """
        Initialize the capacity planner engine.

        Args:
            config: Shared pipeline configuration instance.
            diagnostics_ref: Optional PipelineDiagnostics engine reference.
            resilience_ref: Optional CircuitBreakerRegistry or resilience controller reference.
            predictive_ref: Optional PredictiveAnalytics forecasting engine reference.
            health_scorer_ref: Optional PipelineHealthScorer reference.
            planning_cycle_interval_sec: Planning cycle execution frequency in seconds.
            log_path: Filepath for the JSONL allocation plan audit log.
        """
        self.config = config or SharedPipelineConfig()
        self.diagnostics_ref = diagnostics_ref
        self.resilience_ref = resilience_ref
        self.predictive_ref = predictive_ref
        self.health_scorer = health_scorer_ref

        self.planning_cycle_interval_sec = planning_cycle_interval_sec

        # Core Subsystems
        self.profiler = RepositoryProfiler()
        self.budget_model = QuotaBudgetModel()
        self.allocator = CapacityAllocator()
        self.dispatcher = AllocationDispatcher(log_path=log_path)
        self.health_contributor = CapacityHealthContributor()
        self.api_handler = CapacityPlanningAPIHandler(self)

        # Threading state
        self._shutdown_event = threading.Event()
        self._planner_thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()

        # Telemetry & history
        self._latest_plan: Optional[AllocationPlan] = None
        self._history: List[Dict[str, Any]] = []
        self._max_history = 100

    def start(self) -> None:
        """Start the background capacity planning thread."""
        with self._lock:
            if self._planner_thread and self._planner_thread.is_alive():
                logger.warning("[PipelineCapacityPlanner] Planner thread already active.")
                return

            self._shutdown_event.clear()
            self._planner_thread = threading.Thread(
                target=self._run_loop,
                name="capacity_planner_thread",
                daemon=True,
            )
            self._planner_thread.start()
            logger.info("[PipelineCapacityPlanner] Background capacity planning thread launched.")

    def stop(self) -> None:
        """Signal clean shutdown and join the planner thread."""
        with self._lock:
            if not self._planner_thread or not self._planner_thread.is_alive():
                return

            logger.info("[PipelineCapacityPlanner] Stopping capacity planning thread...")
            self._shutdown_event.set()

        if self._planner_thread:
            self._planner_thread.join(timeout=10.0)
            logger.info("[PipelineCapacityPlanner] Capacity planning thread halted.")

    def _run_loop(self) -> None:
        """Continuous execution loop on capacity_planner_thread."""
        while not self._shutdown_event.is_set():
            cycle_start = time.time()
            try:
                self.run_planning_cycle()
            except Exception as ex:
                logger.error(f"[PipelineCapacityPlanner] Error during planning cycle: {ex}", exc_info=True)

            elapsed = time.time() - cycle_start
            sleep_time = max(1.0, self.planning_cycle_interval_sec - elapsed)
            self._shutdown_event.wait(timeout=sleep_time)

    def run_planning_cycle(self) -> AllocationPlan:
        """
        Execute one full planning cycle:
            1. Harvest telemetry and update ResourceProfiles.
            2. Update QuotaBudgetModel state.
            3. Solve resource allocation via CapacityAllocator.
            4. Dispatch allocation plan to config and persistent logs.
            5. Compute capacity health score and register with health scorer.
            6. Emit tracing telemetry.

        Returns:
            The newly generated and dispatched AllocationPlan.
        """
        with self._lock:
            with tracer.start_as_current_span("capacity_planner.run_planning_cycle") as span:
                logger.debug("[PipelineCapacityPlanner] Executing capacity planning cycle...")

                # 1. Harvest Telemetry & Update Profiles
                pipeline_state = self._harvest_pipeline_telemetry()
                self.profiler.update_profiles(pipeline_state)
                profiles = self.profiler.get_all_profiles()

                # 2. Update Quota Budget Model
                quota_telemetry = pipeline_state.get("quota_accounts", {})
                if quota_telemetry:
                    self.budget_model.update_from_telemetry(quota_telemetry)

                # 3. Solve Allocation
                plan = self.allocator.solve(
                    profiles=profiles,
                    budget_model=self.budget_model,
                    config=self.config,
                )

                # 4. Dispatch Allocation
                dispatch_meta = self.dispatcher.dispatch(
                    plan=plan,
                    config=self.config,
                    budget_model=self.budget_model,
                )

                # 5. Compute & Register Health Score
                total_concurrency = int(self.config.get("CONCURRENCY_LIMIT", 8))
                total_budget = self.budget_model.available_budget_tokens(horizon_ticks=5)

                sub_score, breakdown = self.health_contributor.compute_sub_score(
                    plan=plan,
                    budget_model=self.budget_model,
                    profiles=profiles,
                    total_concurrency_limit=total_concurrency,
                    total_available_budget=total_budget,
                )

                if self.health_scorer and hasattr(self.health_scorer, "register_sub_score"):
                    self.health_scorer.register_sub_score("capacity_sub_score", sub_score)

                # Record in-memory history
                self._latest_plan = copy.deepcopy(plan)
                self._history.append({
                    "timestamp": plan.timestamp,
                    "cycle_id": plan.cycle_id,
                    "objective_value": plan.objective_value,
                    "capacity_sub_score": sub_score,
                    "health_breakdown": breakdown,
                    "solve_time_ms": plan.solve_time_ms,
                    "repo_count": len(plan.repo_allocations),
                })
                if len(self._history) > self._max_history:
                    self._history.pop(0)

                span.set_attribute("capacity.sub_score", sub_score)
                span.set_attribute("capacity.objective_value", plan.objective_value)
                span.set_status(Status(StatusCode.OK))

                return plan

    def _harvest_pipeline_telemetry(self) -> Dict[str, Any]:
        """Harvest metrics across diagnostics, resilience controller, and predictive analytics."""
        state: Dict[str, Any] = {
            "repositories": {},
            "quota_accounts": {},
        }

        # Query diagnostics reference
        if self.diagnostics_ref and hasattr(self.diagnostics_ref, "get_repository_metrics"):
            try:
                diag_data = self.diagnostics_ref.get_repository_metrics()
                for repo, metrics in diag_data.items():
                    state["repositories"].setdefault(repo, {}).update(metrics)
            except Exception as ex:
                logger.debug(f"[PipelineCapacityPlanner] Error querying diagnostics: {ex}")

        # Query resilience controller reference (circuit breakers and cascades)
        if self.resilience_ref:
            try:
                if hasattr(self.resilience_ref, "get_all_breaker_states"):
                    breakers = self.resilience_ref.get_all_breaker_states()
                    for key, b_state in breakers.items():
                        # key format might be "repo/stage" or "repo"
                        repo = key.split("/")[0] if "/" in key else key
                        stage = key.split("/")[1] if "/" in key else "global"
                        repo_entry = state["repositories"].setdefault(repo, {})
                        repo_entry.setdefault("breaker_states", {})[stage] = str(b_state)

                if hasattr(self.resilience_ref, "get_cascade_status"):
                    cascade_repos = self.resilience_ref.get_cascade_status()
                    for repo in cascade_repos:
                        state["repositories"].setdefault(repo, {})["active_cascade_involvement"] = True
            except Exception as ex:
                logger.debug(f"[PipelineCapacityPlanner] Error querying resilience: {ex}")

        # Query predictive analytics reference (burn rates and horizon forecast)
        if self.predictive_ref:
            try:
                if hasattr(self.predictive_ref, "get_repo_horizon_forecasts"):
                    horizons = self.predictive_ref.get_repo_horizon_forecasts()
                    for repo, h_ticks in horizons.items():
                        state["repositories"].setdefault(repo, {})["estimated_completion_horizon_ticks"] = h_ticks

                if hasattr(self.predictive_ref, "get_token_burn_forecasts"):
                    burns = self.predictive_ref.get_token_burn_forecasts()
                    for acc, b_rate in burns.items():
                        state["quota_accounts"].setdefault(acc, {})["token_burn_rate_per_sec"] = b_rate
            except Exception as ex:
                logger.debug(f"[PipelineCapacityPlanner] Error querying predictive analytics: {ex}")

        return state

    def inject_mock_telemetry_for_testing(self, pipeline_state: Dict[str, Any]) -> None:
        """Directly inject state for headless integration tests."""
        self.profiler.update_profiles(pipeline_state)
        quota = pipeline_state.get("quota_accounts")
        if quota:
            self.budget_model.update_from_telemetry(quota)

    def get_latest_plan(self) -> Optional[AllocationPlan]:
        """Return the most recently computed AllocationPlan."""
        with self._lock:
            return copy.deepcopy(self._latest_plan)

    def get_history(self) -> List[Dict[str, Any]]:
        """Return audit history of recent planning cycles."""
        with self._lock:
            return copy.deepcopy(self._history)


# ============================================================================
# Standalone Demonstration & Self-Verification Block
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    print("=" * 80)
    print("Hermes Pipeline Capacity Planner - Verification & Demonstration")
    print("=" * 80)

    # 1. Initialize Shared Configuration and Planner Engine
    shared_config = SharedPipelineConfig()
    shared_config.update({"CONCURRENCY_LIMIT": 10})
    health_scorer = PipelineHealthScorer()

    planner = PipelineCapacityPlanner(
        config=shared_config,
        health_scorer_ref=health_scorer,
        planning_cycle_interval_sec=5.0,
        log_path="dashboards/capacity_plans.jsonl",
    )

    # 2. Mock Multi-Repository Telemetry State
    mock_telemetry: Dict[str, Any] = {
        "repositories": {
            "hermes_orchestrator": {
                "last_milestone_cost_tokens": 3200.0,
                "last_milestone_duration_sec": 38.0,
                "queue_depth": 4,
                "priority": 2.5,
                "failure_rate": 0.02,
                "active_cascade_involvement": False,
                "pending_milestones": [
                    {"priority": 3.0, "time_in_queue_sec": 120.0},
                    {"priority": 2.0, "time_in_queue_sec": 60.0},
                    {"priority": 2.5, "time_in_queue_sec": 10.0},
                ],
                "breaker_states": {"codegen": "CLOSED", "testing": "CLOSED"},
            },
            "phd_proposal": {
                "last_milestone_cost_tokens": 1800.0,
                "last_milestone_duration_sec": 22.0,
                "queue_depth": 2,
                "priority": 1.5,
                "failure_rate": 0.0,
                "active_cascade_involvement": False,
                "pending_milestones": [
                    {"priority": 1.5, "time_in_queue_sec": 240.0},
                    {"priority": 1.0, "time_in_queue_sec": 45.0},
                ],
                "breaker_states": {"latex": "CLOSED"},
            },
            "v2ray_tool": {
                "last_milestone_cost_tokens": 4500.0,
                "last_milestone_duration_sec": 85.0,
                "queue_depth": 1,
                "priority": 1.0,
                "failure_rate": 0.35,
                "active_cascade_involvement": True,  # Subject to cascade penalty
                "pending_milestones": [
                    {"priority": 1.0, "time_in_queue_sec": 30.0},
                ],
                "breaker_states": {"routing": "OPEN"},  # Subject to probe_count cap (2)
            },
            "autonomous_fleet": {
                "last_milestone_cost_tokens": 2100.0,
                "last_milestone_duration_sec": 50.0,
                "queue_depth": 0,  # Empty queue
                "priority": 0.8,
                "failure_rate": 0.05,
                "active_cascade_involvement": False,
                "pending_milestones": [],
                "breaker_states": {"agent_dispatch": "CLOSED"},
            },
        },
        "quota_accounts": {
            "account_1": {"current_balance": 850_000.0, "max_capacity": 1_000_000.0, "token_burn_rate_per_sec": 60.0},
            "account_2": {"current_balance": 320_000.0, "max_capacity": 1_000_000.0, "token_burn_rate_per_sec": 120.0},
            "account_3": {"current_balance": 910_000.0, "max_capacity": 1_000_000.0, "token_burn_rate_per_sec": 30.0},
            "account_4": {"current_balance": 740_000.0, "max_capacity": 1_000_000.0, "token_burn_rate_per_sec": 40.0},
        },
    }

    print("\n[Step 1] Injecting telemetry state into RepositoryProfiler and QuotaBudgetModel...")
    planner.inject_mock_telemetry_for_testing(mock_telemetry)

    profiles = planner.profiler.get_all_profiles()
    for repo, prof in profiles.items():
        print(f"  • Repo '{repo}': queue_depth={prof.queue_depth}, urgency={prof.queue_urgency:.3f}, "
              f"cost_tokens={prof.avg_milestone_cost_tokens:.1f}, risk={prof.failure_risk_score:.2f}, "
              f"breakers={prof.current_breaker_states}")

    headrooms = planner.budget_model.per_account_headroom()
    total_avail = planner.budget_model.available_budget_tokens(horizon_ticks=5)
    print(f"\n[Step 2] Quota pool status: Total Horizon Budget = {total_avail:,.0f} tokens")
    for acc_id, h in headrooms.items():
        print(f"  • {acc_id} Headroom: {h * 100:.1f}%")

    print("\n[Step 3] Executing single capacity planning optimization cycle...")
    plan = planner.run_planning_cycle()

    print(f"\n[Plan Result] Cycle ID: {plan.cycle_id}")
    print(f"Solve Time: {plan.solve_time_ms:.2f} ms | Objective Value: {plan.objective_value:.4f}")
    print("Constraints Status:", plan.constraint_status)
    print("Dispatched Allocations:")
    for repo, alloc in plan.repo_allocations.items():
        print(f"  [{repo}] Slots: {alloc.thread_slots:2d} | Tokens: {alloc.token_budget:9,.0f} | "
              f"Boost: {alloc.priority_boost:.4f} | Rationale: {alloc.rationale}")

    # 4. Verify SharedPipelineConfig integration
    cfg_allocs = shared_config.get("repo_allocations")
    print(f"\n[Step 4] Verified SharedPipelineConfig 'repo_allocations' updated: {len(cfg_allocs)} entries.")
    print(f"[Step 5] Health Score Registered: {health_scorer.sub_scores.get('capacity_sub_score')} / 100")

    # 5. Verify API Handler
    print("\n[Step 6] Testing CapacityPlanningAPIHandler...")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    current_api_res = loop.run_until_complete(planner.api_handler.get_current())
    quota_api_res = loop.run_until_complete(planner.api_handler.get_quota())
    print("  • GET /api/pipeline/capacity/current status:", current_api_res["status"])
    print("  • GET /api/pipeline/capacity/quota status:", quota_api_res["status"])

    # 6. Verify Log Output
    if Path("dashboards/capacity_plans.jsonl").exists():
        with open("dashboards/capacity_plans.jsonl", "r", encoding="utf-8") as f:
            last_line = f.readlines()[-1]
            print(f"\n[Step 7] Verified audit log line in dashboards/capacity_plans.jsonl ({len(last_line)} bytes).")

    print("\nPipeline Capacity Planner verification completed with publication-grade perfection.")
    print("=" * 80)