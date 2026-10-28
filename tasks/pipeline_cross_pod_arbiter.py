#!/usr/bin/env python3
"""
Pipeline Autonomic Cross-Pod Fair-Share Arbiter & Deficit Round-Robin Dispatch Governor
======================================================================================
Module: tasks.pipeline_cross_pod_arbiter
Repository: hermes_orchestrator

Provides thread-safe Deficit Round-Robin (DRR) scheduling, global and per-pod
concurrency governance, anti-starvation emergency priority escalation, burst token
debt amortization, and Jain's Fairness Index telemetry tracking across distributed
repository actor pods.

Zero external dependencies: relies exclusively on standard Python libraries.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from enum import Enum
import json
import math
import os
from pathlib import Path
import sys
import threading
import time
from typing import Dict, List, Optional, Tuple, Any


class PodSchedulingState(str, Enum):
    """Lifecycle and dispatch state of an Actor Pod."""
    IDLE = "IDLE"
    ACTIVE = "ACTIVE"
    STARVED = "STARVED"
    BACKOFF = "BACKOFF"
    QUOTA_THROTTLED = "QUOTA_THROTTLED"


class LeaseDecisionStatus(str, Enum):
    """Outcome states for dispatch lease requests."""
    GRANTED = "GRANTED"
    DEFICIT_EXHAUSTED = "DEFICIT_EXHAUSTED"
    GLOBAL_CONCURRENCY_SATURATED = "GLOBAL_CONCURRENCY_SATURATED"
    POD_CONCURRENCY_SATURATED = "POD_CONCURRENCY_SATURATED"
    RATE_LIMITED = "RATE_LIMITED"


@dataclass
class PodProfile:
    """Internal scheduling and performance state profile for a repository pod."""
    repo_id: str
    priority_weight: float = 1.0
    deficit_credits: float = 0.0
    active_workers: int = 0
    total_dispatched: int = 0
    total_tokens_spent: int = 0
    last_dispatch_ts: float = field(default_factory=time.time)
    starvation_duration_sec: float = 0.0
    scheduling_state: PodSchedulingState = PodSchedulingState.IDLE
    
    # EWMA Token Efficiency Tracking: eta = estimated / actual
    eta_hat: float = 1.0
    alpha_ewma: float = 0.2
    
    # Cooldown / Burst Penalty
    backoff_until_ts: float = 0.0
    active_leases: Dict[str, Dict[str, Any]] = field(default_factory=dict)


@dataclass
class LeaseDecision:
    """Decision token emitted by the arbiter upon a lease request."""
    status: LeaseDecisionStatus
    lease_id: Optional[str]
    repo_id: str
    credits_granted: float
    queue_delay_sec: float
    reason: str


@dataclass
class CrossPodFairnessSnapshot:
    """Immutable snapshot of the global fairness state."""
    timestamp: float
    jains_fairness_index: float
    active_pods: int
    global_active_workers: int
    pod_metrics: Dict[str, Dict[str, Any]]


class CrossPodArbiter:
    """
    Deficit Round-Robin (DRR) Fair-Share Arbiter and Concurrency Governor.
    
    Manages token deficit quanta, ensures strict global and pod concurrency limits,
    mitigates starvation through emergency priority amplification, and penalizes
    token consumption bursts.
    """

    def __init__(
        self,
        q_base: int = 1000,
        global_max_concurrency: int = 5,
        pod_max_concurrency: int = 2,
        t_starve_sec: float = 180.0,
        deficit_max: float = 10000.0,
        alpha_ewma: float = 0.2,
        state_dir: Optional[str] = None
    ) -> None:
        self.q_base = float(q_base)
        self.global_max_concurrency = int(global_max_concurrency)
        self.pod_max_concurrency = int(pod_max_concurrency)
        self.t_starve_sec = float(t_starve_sec)
        self.deficit_max = float(deficit_max)
        self.alpha_ewma = float(alpha_ewma)
        
        # Thread safety primitive
        self._lock = threading.RLock()
        
        # Registered pods
        self.pods: Dict[str, PodProfile] = {}
        
        # Scheduling rounds
        self.round_counter: int = 0
        
        # Lease registry
        self._active_leases: Dict[str, Tuple[str, float, float]] = {}  # lease_id -> (repo_id, estimated_tokens, start_ts)
        self._lease_seq: int = 0
        
        # File paths for persistence
        if state_dir:
            self.base_dir = Path(state_dir)
        else:
            # Resolve relative to repo root if possible, or relative to current file
            self.base_dir = Path(__file__).resolve().parent.parent
            
        self.dashboards_dir = self.base_dir / "dashboards"
        self.json_snapshot_path = self.dashboards_dir / "cross_pod_fairness.json"
        self.md_snapshot_path = self.dashboards_dir / "CROSS_POD_FAIRNESS.md"

    def register_pod(self, repo_id: str, priority_weight: float = 1.0) -> None:
        """Register or update an Actor Pod with a base priority weight."""
        with self._lock:
            weight = max(1.0, float(priority_weight))
            if repo_id not in self.pods:
                now = time.time()
                initial_quantum = self.q_base * weight
                self.pods[repo_id] = PodProfile(
                    repo_id=repo_id,
                    priority_weight=weight,
                    deficit_credits=initial_quantum,
                    last_dispatch_ts=now,
                    alpha_ewma=self.alpha_ewma
                )
            else:
                self.pods[repo_id].priority_weight = weight

    def replenish_quanta(self) -> None:
        """
        Advance the scheduling cycle and allocate deficit credits to all active pods.
        
        Dynamic Quantum:
          Q_i = Q_base * W(P_i) * clamp(eta_hat_i, 0.5, 2.0)
          Deficit_i(r) = min(Deficit_i(r-1) + Q_i, Deficit_max)
        """
        with self._lock:
            self.round_counter += 1
            for pod in self.pods.values():
                clamped_eta = max(0.5, min(2.0, pod.eta_hat))
                quantum = self.q_base * pod.priority_weight * clamped_eta
                pod.deficit_credits = min(self.deficit_max, pod.deficit_credits + quantum)

    def evaluate_starvation(self) -> Dict[str, float]:
        """
        Detect starved pods and apply emergency quantum amplification.
        
        If delta_t_wait,i > T_starve:
          Gamma_boost = 1 + (delta_t_wait,i - T_starve) / T_starve
        """
        with self._lock:
            now = time.time()
            boost_factors: Dict[str, float] = {}
            for repo_id, pod in self.pods.items():
                if pod.active_workers == 0:
                    delta_wait = now - pod.last_dispatch_ts
                    pod.starvation_duration_sec = max(0.0, delta_wait)
                    if delta_wait > self.t_starve_sec:
                        pod.scheduling_state = PodSchedulingState.STARVED
                        gamma_boost = 1.0 + ((delta_wait - self.t_starve_sec) / self.t_starve_sec)
                        emergency_quantum = self.q_base * pod.priority_weight * gamma_boost
                        pod.deficit_credits = min(self.deficit_max, pod.deficit_credits + emergency_quantum)
                        boost_factors[repo_id] = gamma_boost
                    elif pod.backoff_until_ts > now:
                        pod.scheduling_state = PodSchedulingState.BACKOFF
                    else:
                        pod.scheduling_state = PodSchedulingState.IDLE
                else:
                    pod.starvation_duration_sec = 0.0
                    pod.scheduling_state = PodSchedulingState.ACTIVE
            return boost_factors

    def request_dispatch_lease(self, repo_id: str, estimated_tokens: int) -> LeaseDecision:
        """
        Evaluate admission invariants and issue a dispatch lease if satisfied.
        
        Invariants:
          1. Global concurrency < global_max_concurrency
          2. Pod concurrency < pod_max_concurrency
          3. Pod deficit >= estimated_tokens (or Starvation bypass)
          4. Pod not in backoff cooldown
        """
        with self._lock:
            now = time.time()
            if repo_id not in self.pods:
                self.register_pod(repo_id, 1.0)
            
            pod = self.pods[repo_id]
            cost = max(1.0, float(estimated_tokens))
            
            # Check rate limiting / backoff
            if pod.backoff_until_ts > now:
                remaining_delay = pod.backoff_until_ts - now
                return LeaseDecision(
                    status=LeaseDecisionStatus.RATE_LIMITED,
                    lease_id=None,
                    repo_id=repo_id,
                    credits_granted=0.0,
                    queue_delay_sec=remaining_delay,
                    reason=f"Pod in quadratic backoff for {remaining_delay:.2f}s due to prior token burst debt."
                )

            # Check Global Concurrency
            total_active_workers = sum(p.active_workers for p in self.pods.values())
            if total_active_workers >= self.global_max_concurrency:
                return LeaseDecision(
                    status=LeaseDecisionStatus.GLOBAL_CONCURRENCY_SATURATED,
                    lease_id=None,
                    repo_id=repo_id,
                    credits_granted=0.0,
                    queue_delay_sec=1.5,
                    reason=f"Global worker capacity saturated ({total_active_workers}/{self.global_max_concurrency})."
                )

            # Check Pod Concurrency
            if pod.active_workers >= self.pod_max_concurrency:
                return LeaseDecision(
                    status=LeaseDecisionStatus.POD_CONCURRENCY_SATURATED,
                    lease_id=None,
                    repo_id=repo_id,
                    credits_granted=0.0,
                    queue_delay_sec=1.0,
                    reason=f"Pod worker capacity saturated ({pod.active_workers}/{self.pod_max_concurrency})."
                )

            # Evaluate Deficit vs Cost
            delta_wait = now - pod.last_dispatch_ts
            is_starved = (delta_wait > self.t_starve_sec)

            if pod.deficit_credits < cost and not is_starved:
                return LeaseDecision(
                    status=LeaseDecisionStatus.DEFICIT_EXHAUSTED,
                    lease_id=None,
                    repo_id=repo_id,
                    credits_granted=0.0,
                    queue_delay_sec=2.0,
                    reason=f"Insufficient deficit credits ({pod.deficit_credits:.1f} < {cost:.1f})."
                )

            # Grant lease
            self._lease_seq += 1
            lease_id = f"lease_{repo_id}_{self._lease_seq}_{int(now*1000)}"
            
            # Update balances
            pod.deficit_credits -= cost
            pod.active_workers += 1
            pod.total_dispatched += 1
            pod.last_dispatch_ts = now
            pod.scheduling_state = PodSchedulingState.ACTIVE
            
            self._active_leases[lease_id] = (repo_id, cost, now)
            pod.active_leases[lease_id] = {
                "estimated_tokens": cost,
                "granted_at": now
            }

            return LeaseDecision(
                status=LeaseDecisionStatus.GRANTED,
                lease_id=lease_id,
                repo_id=repo_id,
                credits_granted=cost,
                queue_delay_sec=0.0,
                reason="Lease successfully admitted under DRR fair-share invariants."
            )

    def release_dispatch_lease(self, lease_id: str, actual_tokens: int, success: bool = True) -> None:
        """
        Release an active lease, reconcile token debt, update EWMA efficiency,
        and calculate quadratic backoff penalties if token burst occurred.
        """
        with self._lock:
            now = time.time()
            if lease_id not in self._active_leases:
                return

            repo_id, estimated_tokens, start_ts = self._active_leases.pop(lease_id)
            if repo_id not in self.pods:
                return

            pod = self.pods[repo_id]
            pod.active_workers = max(0, pod.active_workers - 1)
            pod.active_leases.pop(lease_id, None)
            
            actual_cost = max(1.0, float(actual_tokens))
            pod.total_tokens_spent += int(actual_cost)

            # Update EWMA Historical Token Efficiency Ratio: eta = estimated / actual
            sample_eta = estimated_tokens / actual_cost
            pod.eta_hat = (self.alpha_ewma * sample_eta) + ((1.0 - self.alpha_ewma) * pod.eta_hat)

            # Token Debt Amortization & Burst Penalty
            if actual_cost > estimated_tokens:
                token_debt = actual_cost - estimated_tokens
                # Deduct token debt directly from deficit credits
                pod.deficit_credits -= token_debt
                
                # Apply quadratic cooldown penalty: kappa = 1 + (Delta / tau_estimated)^2
                ratio = token_debt / estimated_tokens
                kappa = 1.0 + (ratio ** 2)
                cooldown_sec = min(60.0, kappa * 2.0)
                pod.backoff_until_ts = now + cooldown_sec
                pod.scheduling_state = PodSchedulingState.BACKOFF
            else:
                # Credit surplus bonus if pod conserved tokens
                token_savings = estimated_tokens - actual_cost
                pod.deficit_credits = min(self.deficit_max, pod.deficit_credits + (token_savings * 0.5))
                if pod.active_workers == 0:
                    pod.scheduling_state = PodSchedulingState.IDLE

    def compute_jains_fairness_index(self) -> float:
        """
        Compute Jain's Fairness Index across all registered pods:
        
            J(x_1, x_2, ..., x_n) = (sum(x_i))^2 / (n * sum(x_i^2))
            
        where x_i is the normalized service allocation:
            x_i = (total_tokens_spent_i + epsilon) / (priority_weight_i)
        """
        with self._lock:
            if not self.pods:
                return 1.0

            normalized_services: List[float] = []
            for pod in self.pods.values():
                # Avoid division by zero; treat minimum unit as 1.0
                service = max(1.0, float(pod.total_tokens_spent)) / max(0.1, pod.priority_weight)
                normalized_services.append(service)

            n = len(normalized_services)
            if n <= 1:
                return 1.0

            sum_x = sum(normalized_services)
            sum_x_sq = sum(x ** 2 for x in normalized_services)

            if sum_x_sq <= 1e-9:
                return 1.0

            j_index = (sum_x ** 2) / (n * sum_x_sq)
            return float(max(0.0, min(1.0, j_index)))

    def get_fairness_state(self) -> Dict[str, Any]:
        """Generate structured snapshot of active fairness parameters and pod metrics."""
        with self._lock:
            now = time.time()
            pod_metrics: Dict[str, Dict[str, Any]] = {}
            total_active_workers = 0

            for repo_id, pod in self.pods.items():
                total_active_workers += pod.active_workers
                pod_metrics[repo_id] = {
                    "priority_weight": pod.priority_weight,
                    "deficit_credits": round(pod.deficit_credits, 2),
                    "active_workers": pod.active_workers,
                    "total_dispatched": pod.total_dispatched,
                    "total_tokens_spent": pod.total_tokens_spent,
                    "eta_hat_efficiency": round(pod.eta_hat, 4),
                    "starvation_sec": round(now - pod.last_dispatch_ts, 1) if pod.active_workers == 0 else 0.0,
                    "scheduling_state": pod.scheduling_state.value,
                    "backoff_remaining_sec": max(0.0, round(pod.backoff_until_ts - now, 2))
                }

            return {
                "timestamp": now,
                "jains_fairness_index": round(self.compute_jains_fairness_index(), 4),
                "round_counter": self.round_counter,
                "global_active_workers": total_active_workers,
                "global_max_concurrency": self.global_max_concurrency,
                "pod_max_concurrency": self.pod_max_concurrency,
                "active_pods": len(self.pods),
                "pod_metrics": pod_metrics
            }

    def get_cockpit_telemetry(self) -> Dict[str, Any]:
        """Format telemetry payload for control_center / Hermes status server."""
        state = self.get_fairness_state()
        state["arbiter_status"] = "HEALTHY"
        state["governor_algorithm"] = "DEFICIT_ROUND_ROBIN_WITH_EWMA_AND_BURST_AMORTIZATION"
        return state

    def save_state(self) -> Tuple[str, str]:
        """
        Atomically persist state snapshot to JSON and generate publication-grade
        Markdown report with ASCII fairness and deficit allocation charts.
        """
        with self._lock:
            state = self.get_fairness_state()
            self.dashboards_dir.mkdir(parents=True, exist_ok=True)

            # 1. Atomic write JSON snapshot
            temp_json = self.json_snapshot_path.with_suffix(".tmp")
            with open(temp_json, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)
            temp_json.replace(self.json_snapshot_path)

            # 2. Build Publication-Grade Markdown with ASCII charts
            jfi = state["jains_fairness_index"]
            jfi_bar_len = int(jfi * 30)
            jfi_bar = "█" * jfi_bar_len + "░" * (30 - jfi_bar_len)

            md_lines: List[str] = [
                "# Autonomic Cross-Pod Fair-Share Arbiter & Dispatch Governor",
                "",
                f"> **System Timestamp:** `{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(state['timestamp']))}`  ",
                f"> **Scheduling Round:** `{state['round_counter']}` | **Global Concurrency:** `{state['global_active_workers']}/{state['global_max_concurrency']}`  ",
                f"> **Jain's Fairness Index:** `{jfi:.4f}` `[{jfi_bar}]`",
                "",
                "## 1. Executive Summary & Mathematical Invariants",
                "",
                "The Cross-Pod Fair-Share Arbiter governs task dispatch across independent actor pods",
                "using a weighted Deficit Round-Robin (DRR) algorithm with EWMA token efficiency tracking",
                "and quadratic burst penalties.",
                "",
                "$$\\mathcal{Q}_i = Q_{base} \\times W(P_i) \\times \\text{clamp}(\\hat{\\eta}_i, 0.5, 2.0)$$",
                "$$\\Delta_{\\text{wait}, i} > T_{\\text{starve}} \\implies \\Gamma_{\\text{boost}} = 1 + \\frac{\\Delta_{\\text{wait}, i} - T_{\\text{starve}}}{T_{\\text{starve}}}$$",
                "",
                "## 2. Pod Deficit Allocations & Concurrency Metrics",
                "",
                "| Repository / Pod | Priority | State | Deficit Credits | Active Workers | Dispatched | Total Tokens | Efficiency (η̂) | Cooldown |",
                "|:---|:---:|:---:|---:|:---:|---:|---:|:---:|---:|"
            ]

            for repo_id, m in state["pod_metrics"].items():
                cooldown_str = f"{m['backoff_remaining_sec']}s" if m["backoff_remaining_sec"] > 0 else "-"
                row = (
                    f"| **`{repo_id}`** | `{m['priority_weight']}x` | `{m['scheduling_state']}` | "
                    f"`{m['deficit_credits']:.1f}` | `{m['active_workers']}/{self.pod_max_concurrency}` | "
                    f"`{m['total_dispatched']}` | `{m['total_tokens_spent']:,}` | "
                    f"`{m['eta_hat_efficiency']:.2f}` | `{cooldown_str}` |"
                )
                md_lines.append(row)

            md_lines.extend([
                "",
                "## 3. Real-Time Deficit Credit Allocation Chart",
                "",
                "```text"
            ])

            # ASCII Deficit Graph
            max_credits = max([abs(m["deficit_credits"]) for m in state["pod_metrics"].values()] + [1000.0])
            for repo_id, m in state["pod_metrics"].items():
                credits = m["deficit_credits"]
                if credits >= 0:
                    bar_w = int((credits / max_credits) * 40)
                    chart = "■" * bar_w + f" ({credits:.1f})"
                else:
                    bar_w = int((abs(credits) / max_credits) * 40)
                    chart = "DEBT: " + "□" * bar_w + f" ({credits:.1f})"
                md_lines.append(f"{repo_id.rjust(20)} : {chart}")

            md_lines.extend([
                "```",
                "",
                "## 4. Jain's Fairness Index & Service Equity",
                "",
                "```text",
                f"Global Service Fairness: [ {jfi_bar} ] {jfi * 100:.1f}%",
                "Equity Scale: 0.0 (Total Starvation / Monopoly) ===> 1.0 (Strict Proportional Equity)",
                "```",
                "",
                "---",
                "*Generated automatically by `tasks.pipeline_cross_pod_arbiter.CrossPodArbiter`.*"
            ])

            temp_md = self.md_snapshot_path.with_suffix(".tmp")
            with open(temp_md, "w", encoding="utf-8") as f:
                f.write("\n".join(md_lines) + "\n")
            temp_md.replace(self.md_snapshot_path)

            return str(self.json_snapshot_path), str(self.md_snapshot_path)


# =====================================================================
# Verification & Self-Test Suite
# =====================================================================
if __name__ == "__main__":
    print("=" * 80)
    print(">>> Initializing CrossPodArbiter Comprehensive Self-Test Suite")
    print("=" * 80)

    # 1. Initialize Arbiter with test constraints
    test_dir = Path("/tmp/hermes_arbiter_test")
    arbiter = CrossPodArbiter(
        q_base=1000,
        global_max_concurrency=4,
        pod_max_concurrency=2,
        t_starve_sec=2.0,  # short duration for test verification
        deficit_max=8000.0,
        state_dir=str(test_dir)
    )

    # 2. Register 4 Concurrent Repositories with distinct weights
    # P1: PhD Proposal (3.0), P2: hermes_orchestrator (2.0), P2: Ali Resume (2.0), P3: Map2Gz (1.0)
    arbiter.register_pod("phd_proposal", priority_weight=3.0)
    arbiter.register_pod("hermes_orchestrator", priority_weight=2.0)
    arbiter.register_pod("ali_resume", priority_weight=2.0)
    arbiter.register_pod("map2gz", priority_weight=1.0)

    print("\n[STEP 1] Validating Initial Quanta and DRR Registration...")
    assert len(arbiter.pods) == 4, f"Expected 4 pods, found {len(arbiter.pods)}"
    assert arbiter.pods["phd_proposal"].deficit_credits == 3000.0
    assert arbiter.pods["map2gz"].deficit_credits == 1000.0
    print("  ✓ Pod registration and initial priority-weighted quantum verified.")

    # 3. Test Admission & Global/Pod Concurrency Limits
    print("\n[STEP 2] Testing Concurrency Invariants & Admission Leases...")
    l1 = arbiter.request_dispatch_lease("phd_proposal", estimated_tokens=1500)
    assert l1.status == LeaseDecisionStatus.GRANTED, f"Expected GRANTED, got {l1.status}"
    
    l2 = arbiter.request_dispatch_lease("phd_proposal", estimated_tokens=1000)
    assert l2.status == LeaseDecisionStatus.GRANTED, f"Expected GRANTED, got {l2.status}"

    # Pod concurrency ceiling test (pod_max_concurrency=2)
    l3 = arbiter.request_dispatch_lease("phd_proposal", estimated_tokens=500)
    assert l3.status == LeaseDecisionStatus.POD_CONCURRENCY_SATURATED, f"Expected POD_CONCURRENCY_SATURATED, got {l3.status}"
    print("  ✓ Pod maximum concurrency saturation invariant confirmed.")

    # Other pods admitting to fill global concurrency (4 total)
    l4 = arbiter.request_dispatch_lease("hermes_orchestrator", estimated_tokens=1200)
    l5 = arbiter.request_dispatch_lease("ali_resume", estimated_tokens=800)
    assert l4.status == LeaseDecisionStatus.GRANTED
    assert l5.status == LeaseDecisionStatus.GRANTED

    # Global concurrency saturation test
    l6 = arbiter.request_dispatch_lease("map2gz", estimated_tokens=500)
    assert l6.status == LeaseDecisionStatus.GLOBAL_CONCURRENCY_SATURATED, f"Expected GLOBAL_CONCURRENCY_SATURATED, got {l6.status}"
    print("  ✓ Global maximum concurrency saturation invariant confirmed.")

    # 4. Release Leases and Test Token Debt Burst Amortization
    print("\n[STEP 3] Testing Token Debt Amortization & Quadratic Backoff...")
    # Release l1 with normal consumption
    arbiter.release_dispatch_lease(l1.lease_id, actual_tokens=1400, success=True)
    
    # Release l4 with massive burst debt (estimated 1200, actual 3600 -> +2400 debt)
    arbiter.release_dispatch_lease(l4.lease_id, actual_tokens=3600, success=True)
    hermes_pod = arbiter.pods["hermes_orchestrator"]
    assert hermes_pod.deficit_credits < 0, f"Expected negative deficit for debt, got {hermes_pod.deficit_credits}"
    assert hermes_pod.backoff_until_ts > time.time(), "Expected active backoff penalty for burst."
    assert hermes_pod.scheduling_state == PodSchedulingState.BACKOFF
    
    # Requesting lease during backoff should return RATE_LIMITED
    l_blocked = arbiter.request_dispatch_lease("hermes_orchestrator", estimated_tokens=200)
    assert l_blocked.status == LeaseDecisionStatus.RATE_LIMITED, f"Expected RATE_LIMITED, got {l_blocked.status}"
    print("  ✓ Token debt deduction and quadratic backoff rate limiting verified.")

    # Release remaining leases
    arbiter.release_dispatch_lease(l2.lease_id, actual_tokens=1000)
    arbiter.release_dispatch_lease(l5.lease_id, actual_tokens=800)

    # 5. Replenish Quanta Cycle
    print("\n[STEP 4] Testing DRR Round Replenishment...")
    initial_map2gz = arbiter.pods["map2gz"].deficit_credits
    arbiter.replenish_quanta()
    assert arbiter.pods["map2gz"].deficit_credits > initial_map2gz
    print("  ✓ Deficit credits replenished dynamically per round.")

    # 6. Anti-Starvation Verification
    print("\n[STEP 5] Testing Anti-Starvation Emergency Boost Theorem...")
    # Artificially set map2gz last_dispatch_ts backwards to trigger starvation (> 2.0s)
    arbiter.pods["map2gz"].last_dispatch_ts = time.time() - 10.0
    boost_factors = arbiter.evaluate_starvation()
    assert "map2gz" in boost_factors, "Expected map2gz to trigger starvation boost."
    assert boost_factors["map2gz"] > 1.0, f"Expected boost factor > 1.0, got {boost_factors['map2gz']}"
    assert arbiter.pods["map2gz"].scheduling_state == PodSchedulingState.STARVED
    print(f"  ✓ Anti-starvation boost applied (factor: {boost_factors['map2gz']:.2f}x).")

    # 7. Simulate Proportional Dispatches & Jain's Fairness Index Calculation
    print("\n[STEP 6] Calculating Jain's Fairness Index across Weighted Pods...")
    # Balance executions according to weights: P1(3x)=9000, P2(2x)=6000, P2(2x)=6000, P3(1x)=3000
    arbiter.pods["phd_proposal"].total_tokens_spent = 9000
    arbiter.pods["hermes_orchestrator"].total_tokens_spent = 6000
    arbiter.pods["ali_resume"].total_tokens_spent = 6000
    arbiter.pods["map2gz"].total_tokens_spent = 3000

    jfi = arbiter.compute_jains_fairness_index()
    print(f"  Calculated Jain's Fairness Index: {jfi:.4f}")
    assert jfi >= 0.85, f"Expected Jain's Fairness Index >= 0.85, achieved {jfi}"
    print("  ✓ Jain's Fairness Index satisfies equity bound (JFI >= 0.85).")

    # 8. Test State Persistence (JSON & Markdown)
    print("\n[STEP 7] Verifying State Persistence and ASCII Dashboard Generation...")
    json_path, md_path = arbiter.save_state()
    assert os.path.exists(json_path), f"JSON snapshot missing at {json_path}"
    assert os.path.exists(md_path), f"Markdown dashboard missing at {md_path}"
    
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        assert data["jains_fairness_index"] == round(jfi, 4)
        assert len(data["pod_metrics"]) == 4

    with open(md_path, "r", encoding="utf-8") as f:
        md_text = f.read()
        assert "Autonomic Cross-Pod Fair-Share Arbiter" in md_text
        assert "Jain's Fairness Index" in md_text
        assert "phd_proposal" in md_text
        assert "map2gz" in md_text

    print(f"  ✓ Atomically written JSON: {json_path}")
    print(f"  ✓ Publication-grade Markdown generated: {md_path}")

    # 9. Cockpit Telemetry Format Test
    telemetry = arbiter.get_cockpit_telemetry()
    assert telemetry["arbiter_status"] == "HEALTHY"
    assert "governor_algorithm" in telemetry
    print("  ✓ Hermes Cockpit Telemetry payload verified.")

    print("\n" + "=" * 80)
    print(">>> ALL CROSS-POD ARBITER TESTS PASSED SUCCESSFULLY (Exit Code 0)")
    print("=" * 80)
    sys.exit(0)