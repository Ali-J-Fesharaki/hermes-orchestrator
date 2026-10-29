#!/usr/bin/env python3
"""
Pipeline Autonomic Architectural Curriculum & Socratic Mastery Evaluator
========================================================================
Platform: Hermes Orchestrator Autonomous Engineering Fleet
Author: Principal Distributed Systems Architect & Pedagogical Engineering Lead

Implements a dynamic mathematical competency tracking engine, deficit-targeted
Socratic challenge generator, and counterfactual evaluation harness for senior
distributed systems engineering mastery.

Key Architectural Capabilities:
  1. Dynamic Competency Scoring Model:
     M_d(t) = (1 - e^{-alpha * N_{events}(d)}) * 
              (sum_{e in E_d} w_e * Score(e) * e^{-lambda * (t - t_e)}) / 
              (sum_{e in E_d} w_e * e^{-lambda * (t - t_e)} + epsilon)
     where retention decay is governed by lambda = ln(2) / T_{half} (default 14 days),
     and alpha tunes sample maturity.
  2. Deficit-Targeted Socratic Challenge Synthesizer:
     Identifies weakest domain d* = argmin_{d in D} M_d(t) and constructs real-world
     distributed systems dilemmas contrasting optimal patterns with anti-patterns.
  3. Socratic Evaluation & Counterfactual Grading:
     Evaluates architect selections, computes pedagogical delta Delta M_d*,
     and yields comprehensive trade-off analyses.
  4. Cockpit Serialization & Publication-Grade Markdown Export:
     Exports interactive ASCII radar diagrams, competency matrices, and actionable
     architectural katas to dashboards/ARCHITECTURAL_CURRICULUM.md and JSON state.
"""

from __future__ import annotations

import json
import math
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union


# ==============================================================================
# 1. CORE DOMAIN ENUMS & DATA MODELS
# ==============================================================================

class CompetencyDomain(str, Enum):
    """The six canonical architectural competency domains for distributed systems."""
    CONSENSUS_QUORUMS = "consensus_quorums"
    CONCURRENCY_FENCING = "concurrency_fencing"
    FAULT_TOLERANCE_CIRCUITS = "fault_tolerance_circuits"
    SAGAS_COMPENSATION = "sagas_compensation"
    CQRS_OUTBOX = "cqrs_outbox"
    CAPACITY_THUNDERING_HERD = "capacity_thundering_herd"

    @property
    def display_name(self) -> str:
        names = {
            CompetencyDomain.CONSENSUS_QUORUMS: "Consensus, Quorums & Replication",
            CompetencyDomain.CONCURRENCY_FENCING: "Distributed Concurrency & Fencing",
            CompetencyDomain.FAULT_TOLERANCE_CIRCUITS: "Adaptive Fault Tolerance & Circuit Breaking",
            CompetencyDomain.SAGAS_COMPENSATION: "Distributed Sagas & Orchestrated Compensation",
            CompetencyDomain.CQRS_OUTBOX: "CQRS, Event Sourcing & Transactional Outbox",
            CompetencyDomain.CAPACITY_THUNDERING_HERD: "Capacity Planning & Stampede Deflection",
        }
        return names.get(self, self.value)

    @property
    def description(self) -> str:
        descs = {
            CompetencyDomain.CONSENSUS_QUORUMS: (
                "Raft/Paxos leader elections, split-brain mitigation, joint consensus reconfiguration, "
                "and linearizable read leases."
            ),
            CompetencyDomain.CONCURRENCY_FENCING: (
                "Distributed lock leasing, monotonic fencing tokens, zombie writer isolation, "
                "and generational epoch validation."
            ),
            CompetencyDomain.FAULT_TOLERANCE_CIRCUITS: (
                "Sliding-window circuit breakers, adaptive concurrency limits, exponential backoff with "
                "full jitter, and bulkheads."
            ),
            CompetencyDomain.SAGAS_COMPENSATION: (
                "Long-running transactions, orchestrator vs choreography, backward compensation, "
                "forward recovery, and idempotency keys."
            ),
            CompetencyDomain.CQRS_OUTBOX: (
                "Transactional outbox pattern, Change Data Capture (CDC), eventual consistency, "
                "and at-least-once to exactly-once projection semantics."
            ),
            CompetencyDomain.CAPACITY_THUNDERING_HERD: (
                "Singleflight request coalescing, probabilistic early expiration (XFetch), cache stampede "
                "prevention, and token-bucket rate limiting."
            ),
        }
        return descs.get(self, "")


class MasteryLevel(str, Enum):
    """Granular pedagogical mastery tiers."""
    NOVICE = "Novice"
    PRACTITIONER = "Practitioner"
    SENIOR_ARCHITECT = "Senior Architect"
    PRINCIPAL_DISTRIBUTED_SYSTEMS = "Principal Distributed Systems Architect"

    @classmethod
    def from_score(cls, score: float) -> MasteryLevel:
        if score >= 0.85:
            return cls.PRINCIPAL_DISTRIBUTED_SYSTEMS
        elif score >= 0.65:
            return cls.SENIOR_ARCHITECT
        elif score >= 0.35:
            return cls.PRACTITIONER
        return cls.NOVICE

    @property
    def color_badge(self) -> str:
        badges = {
            MasteryLevel.NOVICE: "🟡 Novice (Foundational)",
            MasteryLevel.PRACTITIONER: "🟢 Practitioner (Production-Ready)",
            MasteryLevel.SENIOR_ARCHITECT: "🔵 Senior Architect (Advanced)",
            MasteryLevel.PRINCIPAL_DISTRIBUTED_SYSTEMS: "🟣 Principal Architect (Mission-Critical)",
        }
        return badges.get(self, self.value)


@dataclass
class ArchitecturalEvent:
    """Historical architectural verification event (milestone, bug fix, or quiz response)."""
    event_id: str
    domain: CompetencyDomain
    score: float
    weight: float = 1.0
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "domain": self.domain.value,
            "score": self.score,
            "weight": self.weight,
            "timestamp": self.timestamp,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ArchitecturalEvent:
        return cls(
            event_id=data["event_id"],
            domain=CompetencyDomain(data["domain"]),
            score=float(data["score"]),
            weight=float(data.get("weight", 1.0)),
            timestamp=float(data.get("timestamp", time.time())),
            metadata=data.get("metadata", {}),
        )


@dataclass
class CompetencyScore:
    """Calculated mastery snapshot for an individual architectural domain."""
    domain: CompetencyDomain
    score: float
    level: MasteryLevel
    event_count: int
    confidence_factor: float
    effective_decay_weight: float
    last_updated: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "domain": self.domain.value,
            "domain_name": self.domain.display_name,
            "score": round(self.score, 4),
            "level": self.level.value,
            "event_count": self.event_count,
            "confidence_factor": round(self.confidence_factor, 4),
            "effective_decay_weight": round(self.effective_decay_weight, 4),
            "last_updated": self.last_updated,
        }


@dataclass
class OptionChoice:
    """An architectural decision option presenting real trade-offs."""
    option_id: str
    title: str
    description: str
    score: float
    is_optimal: bool
    architectural_pattern: str
    anti_pattern_risk: str
    trade_off_analysis: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> OptionChoice:
        return cls(**data)


@dataclass
class ArchitecturalChallenge:
    """A realistic production crisis or architectural trade-off challenge."""
    challenge_id: str
    domain: CompetencyDomain
    scenario_title: str
    scenario_text: str
    dilemma_question: str
    options: List[OptionChoice]
    optimal_choice_id: str
    trade_off_matrix: Dict[str, Dict[str, str]]
    catastrophic_failure_prevented: str
    recommended_reading: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "challenge_id": self.challenge_id,
            "domain": self.domain.value,
            "scenario_title": self.scenario_title,
            "scenario_text": self.scenario_text,
            "dilemma_question": self.dilemma_question,
            "options": [opt.to_dict() for opt in self.options],
            "optimal_choice_id": self.optimal_choice_id,
            "trade_off_matrix": self.trade_off_matrix,
            "catastrophic_failure_prevented": self.catastrophic_failure_prevented,
            "recommended_reading": self.recommended_reading,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ArchitecturalChallenge:
        return cls(
            challenge_id=data["challenge_id"],
            domain=CompetencyDomain(data["domain"]),
            scenario_title=data["scenario_title"],
            scenario_text=data["scenario_text"],
            dilemma_question=data["dilemma_question"],
            options=[OptionChoice.from_dict(opt) for opt in data["options"]],
            optimal_choice_id=data["optimal_choice_id"],
            trade_off_matrix=data["trade_off_matrix"],
            catastrophic_failure_prevented=data["catastrophic_failure_prevented"],
            recommended_reading=data.get("recommended_reading", []),
        )


@dataclass
class EvaluationResult:
    """Outcome of evaluating an engineer's architectural choice."""
    challenge_id: str
    domain: CompetencyDomain
    selected_option_id: str
    optimal_option_id: str
    is_correct: bool
    score_achieved: float
    pedagogical_delta: float
    prior_domain_mastery: float
    new_domain_mastery: float
    counterfactual_critique: str
    architectural_takeaway: str
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "challenge_id": self.challenge_id,
            "domain": self.domain.value,
            "domain_name": self.domain.display_name,
            "selected_option_id": self.selected_option_id,
            "optimal_option_id": self.optimal_option_id,
            "is_correct": self.is_correct,
            "score_achieved": round(self.score_achieved, 4),
            "pedagogical_delta": round(self.pedagogical_delta, 4),
            "prior_domain_mastery": round(self.prior_domain_mastery, 4),
            "new_domain_mastery": round(self.new_domain_mastery, 4),
            "counterfactual_critique": self.counterfactual_critique,
            "architectural_takeaway": self.architectural_takeaway,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> EvaluationResult:
        return cls(
            challenge_id=data["challenge_id"],
            domain=CompetencyDomain(data["domain"]),
            selected_option_id=data["selected_option_id"],
            optimal_option_id=data["optimal_option_id"],
            is_correct=data["is_correct"],
            score_achieved=float(data["score_achieved"]),
            pedagogical_delta=float(data["pedagogical_delta"]),
            prior_domain_mastery=float(data["prior_domain_mastery"]),
            new_domain_mastery=float(data["new_domain_mastery"]),
            counterfactual_critique=data["counterfactual_critique"],
            architectural_takeaway=data["architectural_takeaway"],
            timestamp=float(data.get("timestamp", time.time())),
        )


# ==============================================================================
# 2. SOCRATIC CHALLENGE REPOSITORY (DISTRIBUTED SYSTEMS KATAS)
# ==============================================================================

def build_canonical_challenge_library() -> Dict[CompetencyDomain, List[ArchitecturalChallenge]]:
    """Constructs a comprehensive bank of senior-level distributed systems challenges."""
    library: Dict[CompetencyDomain, List[ArchitecturalChallenge]] = {
        domain: [] for domain in CompetencyDomain
    }

    # --------------------------------------------------------------------------
    # 1. CONSENSUS & QUORUMS
    # --------------------------------------------------------------------------
    library[CompetencyDomain.CONSENSUS_QUORUMS].append(
        ArchitecturalChallenge(
            challenge_id="CHAL-CONS-001",
            domain=CompetencyDomain.CONSENSUS_QUORUMS,
            scenario_title="Multi-Datacenter Raft Leader Election Partition Thrashing",
            scenario_text=(
                "A 5-node Raft consensus cluster is geographically deployed across US-East (2 nodes), "
                "US-West (2 nodes), and EU-Central (1 tie-breaker node). A submarine cable sever cuts "
                "EU-Central off from the US clusters, while US-West experiences transient asymmetric packet drops "
                "(30% outbound loss). The current leader in US-East experiences periodic election timeout ticks "
                "from US-West nodes initiating disruptive Pre-Vote-less elections, incrementing terms and "
                "deposing stable leaders continuously."
            ),
            dilemma_question=(
                "Which architectural reconfiguration preserves cluster stability, avoids term inflation, "
                "and guarantees linearizable read safety without human intervention?"
            ),
            options=[
                OptionChoice(
                    option_id="OPT-A",
                    title="Lower Election Timeout in US-West & Disable Term Increments on Minority",
                    description=(
                        "Shorten the election timeout in US-West to preempt US-East and patch Raft to ignore "
                        "term updates unless 4 out of 5 nodes acknowledge."
                    ),
                    score=0.15,
                    is_optimal=False,
                    architectural_pattern="Ad-hoc Timer Tuning (Anti-Pattern)",
                    anti_pattern_risk="Induces total cluster livelock; violates Raft election safety invariant.",
                    trade_off_analysis=(
                        "Arbitrarily tuning timers without protocol guarantees causes cascading election flapping "
                        "and can leave the cluster without an elected leader indefinitely."
                    ),
                ),
                OptionChoice(
                    option_id="OPT-B",
                    title="Enforce Raft Pre-Vote Protocol with Leader Stickiness (Raft §9.6) & Joint Consensus",
                    description=(
                        "Enable the Pre-Vote phase so partitioned/flapping nodes cannot bump cluster terms unless "
                        "a quorum grants speculative votes; enforce leader lease stickiness ignoring votes if "
                        "heartbeats are healthy, and transition EU-Central out via Raft Joint Consensus."
                    ),
                    score=1.0,
                    is_optimal=True,
                    architectural_pattern="Pre-Vote Extension + Leader Stickiness + Joint Consensus",
                    anti_pattern_risk="None. Fully compliant with canonical Ongaro & Ousterhout Raft specification.",
                    trade_off_analysis=(
                        "Pre-vote completely suppresses term inflation from degraded nodes. Leader lease stickiness "
                        "prevents transient network blips from dethroning active leaders while preserving linearizability."
                    ),
                ),
                OptionChoice(
                    option_id="OPT-C",
                    title="Downgrade Consensus to Master-Slave Asynchronous Replication",
                    description=(
                        "Abandon Raft consensus quorum commitments; designate Node 1 in US-East as permanent primary "
                        "and push async binlog streams to all remaining nodes."
                    ),
                    score=0.20,
                    is_optimal=False,
                    architectural_pattern="Async Primary-Replica Fallback",
                    anti_pattern_risk="Silent data loss upon primary crash; catastrophic split-brain if US-East fails.",
                    trade_off_analysis=(
                        "Sacrifices strong consistency (CP) entirely. Any unacknowledged write in flight will be lost "
                        "if the primary crashes, violating financial transaction guarantees."
                    ),
                ),
                OptionChoice(
                    option_id="OPT-D",
                    title="Implement Split-Brain Resolver via External ZooKeeper Witness",
                    description=(
                        "Introduce a standalone 3-node ZooKeeper ensemble in US-East to act as an external arbitrator "
                        "voting on behalf of disconnected nodes."
                    ),
                    score=0.45,
                    is_optimal=False,
                    architectural_pattern="Layered Consensus Arbitrator",
                    anti_pattern_risk="Circular consensus dependency; introduces two conflicting sources of truth.",
                    trade_off_analysis=(
                        "Adds operational fragility and external dependency loops without addressing the root cause "
                        "of Raft election term increments during asymmetric partitions."
                    ),
                ),
            ],
            optimal_choice_id="OPT-B",
            trade_off_matrix={
                "OPT-A": {"consistency": "Violated", "availability": "Zero", "complexity": "Low"},
                "OPT-B": {"consistency": "Strict (Linearizable)", "availability": "High", "complexity": "Moderate"},
                "OPT-C": {"consistency": "Eventually Lost", "availability": "High", "complexity": "Low"},
                "OPT-D": {"consistency": "Ambiguous", "availability": "Fragile", "complexity": "Very High"},
            },
            catastrophic_failure_prevented=(
                "Total cluster unavailability from endless Raft election livelock and term thrashing."
            ),
            recommended_reading=[
                "Ongaro, D., & Ousterhout, J. (2014). 'In Search of an Understandable Consensus Algorithm' (Raft).",
                "Heidi Howard. (2019). 'Paxos vs Raft: Have we reached consensus on distributed consensus?'",
            ],
        )
    )

    # --------------------------------------------------------------------------
    # 2. CONCURRENCY & FENCING
    # --------------------------------------------------------------------------
    library[CompetencyDomain.CONCURRENCY_FENCING].append(
        ArchitecturalChallenge(
            challenge_id="CHAL-CONC-001",
            domain=CompetencyDomain.CONCURRENCY_FENCING,
            scenario_title="Storage Volume Corruption via GC Pause Past Distributed Lock Lease",
            scenario_text=(
                "A distributed indexing engine coordinates write access to an append-only distributed storage block "
                "using a distributed lease in etcd/Redis with a 30-second TTL. Worker A acquires Lock 'storage-vol-42', "
                "prepares a 500MB chunk, and suddenly encounters a 45-second Stop-The-World JVM garbage collection pause. "
                "The lock lease expires, etcd re-leases 'storage-vol-42' to Worker B, which commits chunk #104. "
                "Worker A awakens from GC unaware that time passed, and issues its write for chunk #104."
            ),
            dilemma_question=(
                "How must the storage subsystem be architected to definitively reject Worker A's stale write "
                "without relying on synchronized system clocks across worker nodes?"
            ),
            options=[
                OptionChoice(
                    option_id="OPT-A",
                    title="Validate System Clock Monotonicity Prior to Flush",
                    description=(
                        "Inspect clock_gettime(CLOCK_MONOTONIC) right before issuing the write to ensure elapsed "
                        "time has not exceeded the 30-second lease window."
                    ),
                    score=0.20,
                    is_optimal=False,
                    architectural_pattern="Local Monotonic Verification (Anti-Pattern)",
                    anti_pattern_risk="Classic Time-of-Check to Time-of-Use (TOCTOU) race condition.",
                    trade_off_analysis=(
                        "The GC pause or an OS context switch can occur precisely BETWEEN the clock check and the "
                        "socket write packet entering the network interface, rendering the check completely useless."
                    ),
                ),
                OptionChoice(
                    option_id="OPT-B",
                    title="Monotonically Increasing Fencing Tokens Enforced at Storage Target",
                    description=(
                        "Lock manager issues a strictly increasing generation number (Fencing Token, e.g., 34, 35) "
                        "with every lease acquisition. The storage server records the highest token seen; any write "
                        "bearing a token <= highest_seen_token is atomically rejected."
                    ),
                    score=1.0,
                    is_optimal=True,
                    architectural_pattern="Martin Kleppmann Fencing Tokens (Generational Guard)",
                    anti_pattern_risk="None. Mathematically eliminates stale writer hazards regardless of delays.",
                    trade_off_analysis=(
                        "Requires the target storage layer to enforce stateful validation of generation numbers, "
                        "decoupling correctness entirely from client timing, network delays, or GC pauses."
                    ),
                ),
                OptionChoice(
                    option_id="OPT-C",
                    title="Extend Lock TTL to 10 Minutes with Background Heartbeat Daemon",
                    description=(
                        "Increase lease TTL to 600 seconds and spawn a high-priority background C++ thread to "
                        "renew the lease every 5 seconds."
                    ),
                    score=0.30,
                    is_optimal=False,
                    architectural_pattern="Lease Over-Provisioning & Thread Decoupling",
                    anti_pattern_risk="Extends crash recovery window to 10 minutes; fails if process/host freezes.",
                    trade_off_analysis=(
                        "If a worker truly crashes, the storage resource remains blocked for 10 minutes. Furthermore, "
                        "hypervisor virtualization freezes (VM pauses) freeze all threads simultaneously."
                    ),
                ),
                OptionChoice(
                    option_id="OPT-D",
                    title="Two-Phase Locking (2PL) with Global Lock Broker",
                    description=(
                        "Hold distributed exclusive shared memory locks on all worker nodes coordinated by a single "
                        "central broker with active TCP keep-alives."
                    ),
                    score=0.35,
                    is_optimal=False,
                    architectural_pattern="Centralized 2PL",
                    anti_pattern_risk="Single point of failure and severe distributed lock deadlock hazards.",
                    trade_off_analysis=(
                        "High latency overhead on every file chunk and introduces cascading failure when the central "
                        "broker experiences transient network blips."
                    ),
                ),
            ],
            optimal_choice_id="OPT-B",
            trade_off_matrix={
                "OPT-A": {"correctness": "Flawed (TOCTOU)", "clock_dependency": "Fatal", "safety": "Unsafe"},
                "OPT-B": {"correctness": "Provable", "clock_dependency": "Zero", "safety": "Linearizable"},
                "OPT-C": {"correctness": "Heuristic", "clock_dependency": "Moderate", "safety": "Fragile"},
                "OPT-D": {"correctness": "Deadlock Prone", "clock_dependency": "Low", "safety": "High Overhead"},
            },
            catastrophic_failure_prevented=(
                "Silent, irrecoverable data block corruption caused by zombie workers overwriting valid state."
            ),
            recommended_reading=[
                "Kleppmann, M. (2016). 'How to do distributed locking' (Redlock Analysis).",
                "Burrows, M. (2006). 'The Chubby lock service for loosely-coupled distributed systems'.",
            ],
        )
    )

    # --------------------------------------------------------------------------
    # 3. FAULT TOLERANCE & CIRCUITS
    # --------------------------------------------------------------------------
    library[CompetencyDomain.FAULT_TOLERANCE_CIRCUITS].append(
        ArchitecturalChallenge(
            challenge_id="CHAL-FLT-001",
            domain=CompetencyDomain.FAULT_TOLERANCE_CIRCUITS,
            scenario_title="Cascading Thread Starvation in Upstream Payment Gateway Dependency",
            scenario_text=(
                "A core payment processing service dispatches auth calls to an external banking acquirer API. "
                "Due to database lock contention at the bank, their p99 response latency jumps from 150ms to 45 seconds, "
                "while error rates remain at 0% (requests succeed after 45s). Upstream clients retry on a 3-second timeout, "
                "causing internal connection pools, Tomcat thread workers (max 200), and epoll file descriptors to saturate. "
                "The entire internal payments service stops answering health checks, triggering Kubernetes pod restarts."
            ),
            dilemma_question=(
                "What architectural fault-tolerance mechanism prevents cascading resource exhaustion and isolates "
                "the degraded external acquirer without triggering unnecessary Kubernetes pod restarts?"
            ),
            options=[
                OptionChoice(
                    option_id="OPT-A",
                    title="Scale Out Kubernetes Pod Replicas from 10 to 100",
                    description=(
                        "Trigger Horizontal Pod Autoscaling (HPA) on CPU and memory metrics to spawn additional pods "
                        "to absorb the queued HTTP requests."
                    ),
                    score=0.15,
                    is_optimal=False,
                    architectural_pattern="Unbounded Resource Elasticity (Anti-Pattern)",
                    anti_pattern_risk="Amplifies upstream database exhaustion; incurs massive cloud infrastructure cost.",
                    trade_off_analysis=(
                        "Fails because the bottleneck is external. Adding 10x pods increases the connection load on the "
                        "failing bank API by 10x, accelerating total system collapse."
                    ),
                ),
                OptionChoice(
                    option_id="OPT-B",
                    title="Concurrency Bulkhead Isolation + Sliding Window Latency Circuit Breaker + Full Jitter",
                    description=(
                        "Isolate the external acquirer into a fixed-concurrency bulkhead (max 30 concurrent slots); "
                        "implement a percentile-latency circuit breaker (trip when p90 > 2.0s over 50 samples) with "
                        "fast fallback to async payment queues and decorrelated exponential backoff with full jitter."
                    ),
                    score=1.0,
                    is_optimal=True,
                    architectural_pattern="Bulkhead + Adaptive Latency Circuit Breaker + Full Jitter",
                    anti_pattern_risk="None. Preserves upstream availability while shielding the downstream partner.",
                    trade_off_analysis=(
                        "Bulkheads prevent the external dependency from consuming internal worker pools. The latency-based "
                        "circuit breaker sheds load instantly when slow responses threaten internal thread starvation."
                    ),
                ),
                OptionChoice(
                    option_id="OPT-C",
                    title="Aggressive Retry Loop with 5 Retries and Exponential Backoff (No Jitter)",
                    description=(
                        "Configure the HTTP client to retry up to 5 times on timeout with backoff doubling: 1s, 2s, 4s, 8s."
                    ),
                    score=0.25,
                    is_optimal=False,
                    architectural_pattern="Synchronized Retry Storm",
                    anti_pattern_risk="Thundering-herd retry storms create synchronized waves of destructive traffic.",
                    trade_off_analysis=(
                        "Without jitter, thousands of concurrent requests retry in tight lockstep, turning mild downstream "
                        "slowdowns into a permanent, self-inflicted denial-of-service attack."
                    ),
                ),
                OptionChoice(
                    option_id="OPT-D",
                    title="Increase HTTP Client Timeout to 60 Seconds",
                    description=(
                        "Raise the socket read timeout from 3s to 60s so requests can finish rather than timing out."
                    ),
                    score=0.10,
                    is_optimal=False,
                    architectural_pattern="Timeout Inflation (Anti-Pattern)",
                    anti_pattern_risk="Guarantees 100% thread exhaustion across the entire cluster within seconds.",
                    trade_off_analysis=(
                        "Holding 200 threads blocked for 45s each means the service can only process ~4.4 requests/second "
                        "before rejecting all new incoming health checks and traffic."
                    ),
                ),
            ],
            optimal_choice_id="OPT-B",
            trade_off_matrix={
                "OPT-A": {"resilience": "Deteriorates", "cost": "Explosive", "blast_radius": "Cluster-wide"},
                "OPT-B": {"resilience": "Guaranteed", "cost": "Minimal", "blast_radius": "Isolated"},
                "OPT-C": {"resilience": "Catastrophic", "cost": "Low", "blast_radius": "Destructive"},
                "OPT-D": {"resilience": "Zero", "cost": "Low", "blast_radius": "Total Service Death"},
            },
            catastrophic_failure_prevented=(
                "Cascading thread starvation and cluster-wide crash-restart loop of upstream payment services."
            ),
            recommended_reading=[
                "Nygard, M. T. (2018). 'Release It!: Design and Deploy Production-Ready Software'.",
                "Brookman, M. (2015). 'Exponential Backoff And Jitter' (AWS Architecture Blog).",
            ],
        )
    )

    # --------------------------------------------------------------------------
    # 4. SAGAS & COMPENSATION
    # --------------------------------------------------------------------------
    library[CompetencyDomain.SAGAS_COMPENSATION].append(
        ArchitecturalChallenge(
            challenge_id="CHAL-SAGA-001",
            domain=CompetencyDomain.SAGAS_COMPENSATION,
            scenario_title="Partial Failure in E-Commerce Order Fulfillment Across Heterogeneous Microservices",
            scenario_text=(
                "A checkout flow spans four distinct microservices: Order Service (Postgres), Payment Gateway (Stripe), "
                "Inventory Service (MySQL), and Delivery Dispatch (Third-party logistics API). During a high-volume flash sale, "
                "Payment successfully authorizes and captures $350, Inventory reserves items, but Delivery Dispatch fails with "
                "HTTP 503 Overloaded. Holding distributed locks across these distinct databases and external APIs is impossible."
            ),
            dilemma_question=(
                "How should this multi-stage distributed transaction be structured to ensure consistency, eliminate "
                "silent financial discrepancy, and maintain system autonomy under partial failures?"
            ),
            options=[
                OptionChoice(
                    option_id="OPT-A",
                    title="Two-Phase Commit (2PC) Distributed Transaction Coordinator via XA",
                    description=(
                        "Enforce XA Two-Phase Commit with an external coordinator holding lock reservations across "
                        "Postgres, MySQL, and wrapping Stripe and Delivery in pseudo-XA wrappers."
                    ),
                    score=0.20,
                    is_optimal=False,
                    architectural_pattern="Distributed 2PC / XA over Microservices (Anti-Pattern)",
                    anti_pattern_risk="Blocking coordinator failure halts entire system; external APIs cannot do XA prepare.",
                    trade_off_analysis=(
                        "2PC is fundamentally incompatible with third-party web services that cannot hold row locks "
                        "or perform speculative two-phase prepare phases. Coordinator failure freezes resources."
                    ),
                ),
                OptionChoice(
                    option_id="OPT-B",
                    title="Orchestrated Saga with Idempotent Forward/Backward Compensations & Durable State Machine",
                    description=(
                        "Implement an Orchestrated Saga driven by a durable event-driven state machine. When Delivery "
                        "fails, execute backwards compensating transactions in reverse order: release inventory reservation, "
                        "issue idempotent Stripe refund using original payment intent key, and mark Order as CANCELLED."
                    ),
                    score=1.0,
                    is_optimal=True,
                    architectural_pattern="Orchestrated Saga with Idempotent Compensation",
                    anti_pattern_risk="None. Decoupled, fault-tolerant, and resilient to arbitrary network delays.",
                    trade_off_analysis=(
                        "Trading ACID isolation for eventual consistency requires designing every compensating step to be "
                        "strictly idempotent. In exchange, the system achieves maximum availability and fault containment."
                    ),
                ),
                OptionChoice(
                    option_id="OPT-C",
                    title="Choreographed Event Bus with Unmonitored Async Topics",
                    description=(
                        "Each service emits Kafka events upon success. Services listen and attempt to reactively roll back "
                        "by broadcasting 'ErrorEvent' messages without a centralized state tracker."
                    ),
                    score=0.45,
                    is_optimal=False,
                    architectural_pattern="Unstructured Choreography",
                    anti_pattern_risk="Cyclic event storms, untraceable compensation failures, difficult observability.",
                    trade_off_analysis=(
                        "As the workflow grows, choreographed sagas degenerate into 'spaghetti events' where failure to "
                        "compensate one node leads to silent data divergence without a clear owner."
                    ),
                ),
                OptionChoice(
                    option_id="OPT-D",
                    title="Nightly Batch Reconciliation Script",
                    description=(
                        "Accept the failure and let the order remain in limbo; rely on a scheduled Python batch script at "
                        "02:00 UTC to inspect discrepancies and refund customers."
                    ),
                    score=0.30,
                    is_optimal=False,
                    architectural_pattern="Batch Discrepancy Reconciliation",
                    anti_pattern_risk="Customer frustration, chargeback fees, and hours of inconsistent warehouse inventory.",
                    trade_off_analysis=(
                        "While good as a secondary safety net, using batch scripts as the primary consistency mechanism "
                        "leads to customer chargebacks, regulatory compliance fines, and inventory lockup."
                    ),
                ),
            ],
            optimal_choice_id="OPT-B",
            trade_off_matrix={
                "OPT-A": {"isolation": "Strict", "availability": "Near Zero", "complexity": "Intractable"},
                "OPT-B": {"isolation": "Eventual", "availability": "Very High", "complexity": "Manageable"},
                "OPT-C": {"isolation": "Undefined", "availability": "High", "complexity": "Chaotic"},
                "OPT-D": {"isolation": "Delayed Hours", "availability": "Moderate", "complexity": "Low"},
            },
            catastrophic_failure_prevented=(
                "Financial ledger imbalance, double-charging customers without stock, and distributed lock freezes."
            ),
            recommended_reading=[
                "Garcia-Molina, H., & Salem, K. (1987). 'Sagas'. ACM SIGMOD.",
                "Richardson, C. (2018). 'Microservices Patterns: With examples in Java' (Saga Pattern).",
            ],
        )
    )

    # --------------------------------------------------------------------------
    # 5. CQRS & TRANSACTIONAL OUTBOX
    # --------------------------------------------------------------------------
    library[CompetencyDomain.CQRS_OUTBOX].append(
        ArchitecturalChallenge(
            challenge_id="CHAL-CQRS-001",
            domain=CompetencyDomain.CQRS_OUTBOX,
            scenario_title="Dual-Write Inconsistency Between Relational DB and Event Bus",
            scenario_text=(
                "A banking ledger service updates an account balance in PostgreSQL and publishes an 'AccountDebited' "
                "event to Apache Kafka to update audit streams and downstream fraud models. The code executes: "
                "1. db.commit(); 2. kafkaProducer.send(event). In production, the JVM process gets OOM-killed or the network "
                "blips exactly after the PostgreSQL transaction commits but before Kafka acknowledges the message. "
                "The database reflects a $10,000 debit, but downstream services never receive the event."
            ),
            dilemma_question=(
                "How must the architecture be refactored to eliminate dual-write hazards and guarantee atomic "
                "consistency between PostgreSQL mutation and message publication?"
            ),
            options=[
                OptionChoice(
                    option_id="OPT-A",
                    title="Invert Order: Publish to Kafka First, then Commit to Postgres",
                    description=(
                        "Send the event to Kafka first. Only upon receiving Kafka ACK, execute db.commit(). If the DB "
                        "commit fails, Kafka consumers can handle it."
                    ),
                    score=0.20,
                    is_optimal=False,
                    architectural_pattern="Reversed Dual Write (Anti-Pattern)",
                    anti_pattern_risk="Produces phantom events where downstream thinks an action occurred that DB aborted.",
                    trade_off_analysis=(
                        "If the database transaction rolls back due to a constraint violation or lock deadlock, downstream "
                        "consumers have already acted upon an event that never existed in the system of record."
                    ),
                ),
                OptionChoice(
                    option_id="OPT-B",
                    title="Transactional Outbox Pattern with Change Data Capture (Debezium / Logical Decoding)",
                    description=(
                        "Persist the event into an 'outbox' table within the EXACT SAME local relational database transaction "
                        "as the business entity update. An asynchronous CDC tailer (e.g., Debezium reading the Postgres WAL) "
                        "streams events from the outbox table to Kafka with at-least-once guarantees."
                    ),
                    score=1.0,
                    is_optimal=True,
                    architectural_pattern="Transactional Outbox via WAL CDC",
                    anti_pattern_risk="None. Mathematically eliminates the dual-write window using local ACID semantics.",
                    trade_off_analysis=(
                        "Leverages the database's local ACID transaction boundary to ensure atomicity between data mutation "
                        "and message staging. Consumers must be idempotent to handle at-least-once delivery."
                    ),
                ),
                OptionChoice(
                    option_id="OPT-C",
                    title="Poll-and-Delete Outbox Daemon via Scheduled SQL Query",
                    description=(
                        "Insert into an outbox table, and have a cron job run 'SELECT * FROM outbox FOR UPDATE' every second, "
                        "sending to Kafka and running 'DELETE FROM outbox WHERE id = ...'."
                    ),
                    score=0.60,
                    is_optimal=False,
                    architectural_pattern="Polling Outbox (Sub-Optimal)",
                    anti_pattern_risk="Severe DB table bloat, lock contention on outbox table, high latency jitter.",
                    trade_off_analysis=(
                        "Functionally correct regarding atomicity, but introduces heavy relational table churn, dead tuple "
                        "bloat in Postgres, and scalability bottlenecks compared to streaming WAL logs."
                    ),
                ),
                OptionChoice(
                    option_id="OPT-D",
                    title="Distributed XA Transaction Spanning Postgres and Kafka",
                    description=(
                        "Join PostgreSQL and Apache Kafka into a single JTA/XA 2-Phase Commit distributed transaction coordinator."
                    ),
                    score=0.25,
                    is_optimal=False,
                    architectural_pattern="Heterogeneous 2PC over Streaming Bus",
                    anti_pattern_risk="Kafka does not support XA specifications; severe latency degradation.",
                    trade_off_analysis=(
                        "Kafka's transactional API is designed for Read-Process-Write within Kafka-to-Kafka streams, not "
                        "cross-system XA coordinates with relational databases."
                    ),
                ),
            ],
            optimal_choice_id="OPT-B",
            trade_off_matrix={
                "OPT-A": {"atomicity": "Broken (Phantom)", "throughput": "High", "simplicity": "High"},
                "OPT-B": {"atomicity": "Guaranteed (ACID+CDC)", "throughput": "Ultra-High", "simplicity": "Standard"},
                "OPT-C": {"atomicity": "Guaranteed", "throughput": "Degraded", "simplicity": "Moderate"},
                "OPT-D": {"atomicity": "Unsupported", "throughput": "Very Low", "simplicity": "Impossible"},
            },
            catastrophic_failure_prevented=(
                "Permanent state divergence between source database and event-driven downstream consumers."
            ),
            recommended_reading=[
                "Richardson, C. (2018). 'Pattern: Transactional outbox' (Microservices.io).",
                "Debezium Documentation. 'Reliable Microservices Data Exchange Using the Outbox Pattern'.",
            ],
        )
    )

    # --------------------------------------------------------------------------
    # 6. CAPACITY PLANNING & THUNDERING-HERD DEFLECTION
    # --------------------------------------------------------------------------
    library[CompetencyDomain.CAPACITY_THUNDERING_HERD].append(
        ArchitecturalChallenge(
            challenge_id="CHAL-THRD-001",
            domain=CompetencyDomain.CAPACITY_THUNDERING_HERD,
            scenario_title="Cache Stampede Upon Expiration of High-Cardinals Celebrity Profile",
            scenario_text=(
                "A social news platform caches user profile data in Redis with a strict 300-second TTL. "
                "A world-renowned figure posts breaking news, receiving 80,000 requests/second. Exactly at second 300, "
                "the Redis key expires. The next 50,000 concurrent requests miss the cache in the same 500ms window, "
                "and all 50,000 threads simultaneously hammer the backend PostgreSQL read replica with identical "
                "expensive multi-table JOIN queries. The database CPU hits 100%, max_connections is exceeded, and the DB crashes."
            ),
            dilemma_question=(
                "What combination of caching and concurrency patterns completely deflects this thundering-herd stampede "
                "while serving fresh data without overloading the database?"
            ),
            options=[
                OptionChoice(
                    option_id="OPT-A",
                    title="Increase Redis Key TTL to 24 Hours",
                    description=(
                        "Extend the cache duration so keys expire rarely, manually purging when updates occur."
                    ),
                    score=0.30,
                    is_optimal=False,
                    architectural_pattern="TTL Inflation (Anti-Pattern)",
                    anti_pattern_risk="When the key eventually expires or is purged, the exact same crash occurs.",
                    trade_off_analysis=(
                        "Postpones the failure rather than solving the concurrency defect. Furthermore, manual purge during "
                        "peak traffic immediately triggers the fatal stampede."
                    ),
                ),
                OptionChoice(
                    option_id="OPT-B",
                    title="Singleflight Request Coalescing + Probabilistic Early Expiration (XFetch Algorithm)",
                    description=(
                        "Implement in-process and distributed Singleflight (mutex request coalescing) ensuring only ONE "
                        "worker queries the database for a missing key while other 49,999 requests await its promise; "
                        "combine with the XFetch probabilistic early recomputation algorithm to refresh the cache in the "
                        "background prior to actual expiration based on read frequency and computation time."
                    ),
                    score=1.0,
                    is_optimal=True,
                    architectural_pattern="Singleflight Coalescing + Probabilistic Early Refresh (XFetch)",
                    anti_pattern_risk="None. Mathematically bounds database load to exactly 1 query during cache recomputation.",
                    trade_off_analysis=(
                        "Eliminates duplicate concurrent queries completely. XFetch ensures that hot keys are asynchronously "
                        "refreshed before expiration, achieving 0% cold cache stampedes under arbitrary traffic volume."
                    ),
                ),
                OptionChoice(
                    option_id="OPT-C",
                    title="Deploy 10 Additional PostgreSQL Read Replicas",
                    description=(
                        "Provision 10 read replicas behind a PgBouncer cluster to absorb the 50,000 concurrent queries."
                    ),
                    score=0.25,
                    is_optimal=False,
                    architectural_pattern="Linear Hardware Scaling for Algorithmic Flaw",
                    anti_pattern_risk="Enormous financial waste; replication lag causes data inconsistency.",
                    trade_off_analysis=(
                        "Throwing hardware at an O(N) concurrent stampede flaw is financially reckless and fails when traffic "
                        "scales to 200,000 requests/sec. The root algorithmic flaw remains unaddressed."
                    ),
                ),
                OptionChoice(
                    option_id="OPT-D",
                    title="Randomized Jitter on TTL Only",
                    description=(
                        "Add +/- 30 seconds of random jitter to the Redis TTL when writing the key."
                    ),
                    score=0.45,
                    is_optimal=False,
                    architectural_pattern="Simple TTL Jitter",
                    anti_pattern_risk="Does not solve stampede for a SINGLE hot key when it does expire.",
                    trade_off_analysis=(
                        "Jitter prevents multiple DIFFERENT keys from expiring simultaneously, but for a single ultra-hot key "
                        "with 80,000 req/s, the instant that single key expires, the entire 80,000 req/s wave still hits the DB."
                    ),
                ),
            ],
            optimal_choice_id="OPT-B",
            trade_off_matrix={
                "OPT-A": {"stampede_protection": "Temporary", "freshness": "Poor", "cost": "Low"},
                "OPT-B": {"stampede_protection": "Absolute", "freshness": "Near-Realtime", "cost": "Zero Infra Cost"},
                "OPT-C": {"stampede_protection": "Weak", "freshness": "Delayed (Replica Lag)", "cost": "Astronomical"},
                "OPT-D": {"stampede_protection": "Ineffective for single key", "freshness": "Standard", "cost": "Low"},
            },
            catastrophic_failure_prevented=(
                "Total database CPU saturation, connection pool death, and cascading edge gateway 504 timeouts."
            ),
            recommended_reading=[
                "Vattani, A., Chierichetti, F., & Lowenstein, K. (2015). 'Optimal Probabilistic Cache Stampede Prevention' (XFetch).",
                "Go Singleflight Package (golang.org/x/sync/singleflight).",
            ],
        )
    )

    return library


# ==============================================================================
# 3. CURRICULUM ENGINE & MATHEMATICAL MASTERY TRACKER
# ==============================================================================

class CurriculumEngine:
    """
    Mathematical Competency Tracking Engine.
    
    Implements exponential retention decay modeling and deficit-driven
    pedagogical challenge assignment.
    """

    def __init__(
        self,
        decay_half_life_days: float = 14.0,
        sample_maturity_alpha: float = 0.25,
        state_dir: Optional[Union[str, Path]] = None,
    ):
        self.half_life_seconds = decay_half_life_days * 86400.0
        # Retention decay constant: lambda = ln(2) / T_{half}
        self.decay_lambda = math.log(2.0) / self.half_life_seconds
        self.alpha = sample_maturity_alpha
        self.epsilon = 1e-9

        # State storage
        if state_dir:
            self.state_dir = Path(state_dir)
        else:
            self.state_dir = Path(__file__).resolve().parent.parent / "dashboards"

        self.state_file = self.state_dir / "architectural_curriculum_state.json"
        self.markdown_file = self.state_dir / "ARCHITECTURAL_CURRICULUM.md"

        self.events: List[ArchitecturalEvent] = []
        self.evaluation_history: List[EvaluationResult] = []
        self.challenge_library = build_canonical_challenge_library()

        # Initialize or load existing state
        self.load_state()

    def record_event(
        self,
        domain: CompetencyDomain,
        score: float,
        weight: float = 1.0,
        event_id: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> CompetencyScore:
        """
        Records a verified architectural milestone or assessment result.
        
        Score must be in [0.0, 1.0].
        """
        if not event_id:
            event_id = f"EVT-{domain.value[:4].upper()}-{int(time.time()*1000)}"

        clamped_score = max(0.0, min(1.0, float(score)))
        event = ArchitecturalEvent(
            event_id=event_id,
            domain=domain,
            score=clamped_score,
            weight=max(0.1, float(weight)),
            timestamp=time.time(),
            metadata=metadata or {},
        )
        self.events.append(event)
        self.save_state()
        return self.calculate_domain_mastery(domain)

    def calculate_domain_mastery(
        self,
        domain: CompetencyDomain,
        evaluation_timestamp: Optional[float] = None,
    ) -> CompetencyScore:
        """
        Computes dynamic domain mastery:
        M_d(t) = (1 - e^{-alpha * N}) * (sum w_e * S_e * e^{-lambda * Delta t}) / (sum w_e * e^{-lambda * Delta t} + eps)
        """
        now = evaluation_timestamp if evaluation_timestamp is not None else time.time()
        domain_events = [e for e in self.events if e.domain == domain]
        n_events = len(domain_events)

        if n_events == 0:
            return CompetencyScore(
                domain=domain,
                score=0.0,
                level=MasteryLevel.NOVICE,
                event_count=0,
                confidence_factor=0.0,
                effective_decay_weight=0.0,
                last_updated=now,
            )

        # Confidence sample maturity factor
        confidence_factor = 1.0 - math.exp(-self.alpha * n_events)

        weighted_score_sum = 0.0
        weighted_decay_sum = 0.0

        for event in domain_events:
            delta_t = max(0.0, now - event.timestamp)
            decay_factor = math.exp(-self.decay_lambda * delta_t)
            effective_w = event.weight * decay_factor

            weighted_score_sum += effective_w * event.score
            weighted_decay_sum += effective_w

        raw_average = weighted_score_sum / (weighted_decay_sum + self.epsilon)
        final_score = max(0.0, min(1.0, confidence_factor * raw_average))
        mastery_level = MasteryLevel.from_score(final_score)

        return CompetencyScore(
            domain=domain,
            score=final_score,
            level=mastery_level,
            event_count=n_events,
            confidence_factor=confidence_factor,
            effective_decay_weight=weighted_decay_sum,
            last_updated=now,
        )

    def get_overall_mastery(
        self,
        evaluation_timestamp: Optional[float] = None,
    ) -> Dict[str, CompetencyScore]:
        """Returns the calculated mastery records for all six competency domains."""
        now = evaluation_timestamp if evaluation_timestamp is not None else time.time()
        return {
            domain.value: self.calculate_domain_mastery(domain, now)
            for domain in CompetencyDomain
        }

    def identify_weakest_domain(
        self,
        evaluation_timestamp: Optional[float] = None,
    ) -> CompetencyDomain:
        """
        Deficit Identifier:
        d* = argmin_{d in D} M_d(t) with deterministic tie-breaking.
        """
        now = evaluation_timestamp if evaluation_timestamp is not None else time.time()
        scores = [
            (self.calculate_domain_mastery(d, now).score, d)
            for d in CompetencyDomain
        ]
        # Sort by score ascending, then by domain order
        scores.sort(key=lambda item: (item[0], item[1].value))
        return scores[0][1]

    def generate_challenge(
        self,
        target_domain: Optional[CompetencyDomain] = None,
    ) -> ArchitecturalChallenge:
        """
        Synthesizes an interactive Socratic Challenge.
        If target_domain is None, targets the primary learning deficit d*.
        """
        domain = target_domain if target_domain is not None else self.identify_weakest_domain()
        available_challenges = self.challenge_library.get(domain, [])

        if not available_challenges:
            # Fallback dynamic challenge creation if domain list empty
            return ArchitecturalChallenge(
                challenge_id=f"CHAL-{domain.value[:4].upper()}-DEFAULT",
                domain=domain,
                scenario_title=f"Architectural Verification for {domain.display_name}",
                scenario_text=f"Production scenario testing canonical constraints of {domain.display_name}.",
                dilemma_question="Select the optimal pattern addressing safety and liveness:",
                options=[
                    OptionChoice(
                        option_id="OPT-A",
                        title="Optimal Canonical Implementation",
                        description="Implement standard distributed architecture protocol.",
                        score=1.0,
                        is_optimal=True,
                        architectural_pattern="Standard Distributed Pattern",
                        anti_pattern_risk="None.",
                        trade_off_analysis="Optimal balance.",
                    ),
                    OptionChoice(
                        option_id="OPT-B",
                        title="Naive Anti-Pattern",
                        description="Bypass safety guarantees for performance.",
                        score=0.2,
                        is_optimal=False,
                        architectural_pattern="Naive Bypass",
                        anti_pattern_risk="Severe data corruption.",
                        trade_off_analysis="Violates consistency.",
                    ),
                ],
                optimal_choice_id="OPT-A",
                trade_off_matrix={"OPT-A": {"grade": "A"}, "OPT-B": {"grade": "F"}},
                catastrophic_failure_prevented="System collapse from unmitigated distributed failure.",
                recommended_reading=[],
            )

        # Select challenge with least historical evaluations
        history_counts = {c.challenge_id: 0 for c in available_challenges}
        for res in self.evaluation_history:
            if res.challenge_id in history_counts:
                history_counts[res.challenge_id] += 1

        selected = min(available_challenges, key=lambda c: history_counts[c.challenge_id])
        return selected

    def evaluate_response(
        self,
        challenge_id: str,
        selected_option_id: str,
    ) -> EvaluationResult:
        """
        Evaluates the architect's selection, calculates score, yields counterfactual
        critique, updates the domain score, and persists updated curriculum state.
        """
        challenge: Optional[ArchitecturalChallenge] = None
        for dom_challenges in self.challenge_library.values():
            for c in dom_challenges:
                if c.challenge_id == challenge_id:
                    challenge = c
                    break
            if challenge:
                break

        if not challenge:
            raise ValueError(f"Challenge ID '{challenge_id}' not found in canonical library.")

        chosen_option: Optional[OptionChoice] = None
        for opt in challenge.options:
            if opt.option_id == selected_option_id:
                chosen_option = opt
                break

        if not chosen_option:
            raise ValueError(
                f"Option ID '{selected_option_id}' invalid for challenge '{challenge_id}'."
            )

        prior_score_obj = self.calculate_domain_mastery(challenge.domain)
        prior_mastery = prior_score_obj.score

        # Grade option
        score_achieved = chosen_option.score
        is_correct = chosen_option.is_optimal

        # Critique synthesis
        if is_correct:
            counterfactual_critique = (
                f"Exemplary architectural insight. {chosen_option.trade_off_analysis} "
                f"This choice prevents: {challenge.catastrophic_failure_prevented}"
            )
        else:
            counterfactual_critique = (
                f"SUB-OPTIMAL DECISION HAZARD: {chosen_option.anti_pattern_risk} "
                f"Trade-off Critique: {chosen_option.trade_off_analysis} "
                f"Optimal Strategy was {challenge.optimal_choice_id}."
            )

        # Ingest as event
        self.record_event(
            domain=challenge.domain,
            score=score_achieved,
            weight=1.5,
            event_id=f"EVT-EVAL-{challenge.challenge_id}-{int(time.time()*1000)}",
            metadata={
                "challenge_id": challenge.challenge_id,
                "selected_option_id": selected_option_id,
                "is_correct": is_correct,
            },
        )

        new_score_obj = self.calculate_domain_mastery(challenge.domain)
        new_mastery = new_score_obj.score
        pedagogical_delta = new_mastery - prior_mastery

        result = EvaluationResult(
            challenge_id=challenge_id,
            domain=challenge.domain,
            selected_option_id=selected_option_id,
            optimal_option_id=challenge.optimal_choice_id,
            is_correct=is_correct,
            score_achieved=score_achieved,
            pedagogical_delta=pedagogical_delta,
            prior_domain_mastery=prior_mastery,
            new_domain_mastery=new_mastery,
            counterfactual_critique=counterfactual_critique,
            architectural_takeaway=chosen_option.architectural_pattern,
            timestamp=time.time(),
        )

        self.evaluation_history.append(result)
        self.save_state()
        self.export_markdown_report()
        return result

    # --------------------------------------------------------------------------
    # 4. SERIALIZATION, PERSISTENCE & DASHBOARD EXPORT
    # --------------------------------------------------------------------------

    def get_curriculum_state(self) -> Dict[str, Any]:
        """Exposes raw curriculum state for the Hermes Control Center cockpit."""
        overall = self.get_overall_mastery()
        weakest = self.identify_weakest_domain()

        total_score = sum(cs.score for cs in overall.values())
        composite_score = total_score / len(overall) if overall else 0.0

        return {
            "version": "2.4.0",
            "last_updated": time.time(),
            "composite_mastery_score": round(composite_score, 4),
            "composite_mastery_level": MasteryLevel.from_score(composite_score).value,
            "weakest_domain": weakest.value,
            "weakest_domain_name": weakest.display_name,
            "domains": {k: v.to_dict() for k, v in overall.items()},
            "total_events_recorded": len(self.events),
            "total_evaluations_completed": len(self.evaluation_history),
            "evaluation_history": [e.to_dict() for e in self.evaluation_history[-10:]],
        }

    def save_state(self, filepath: Optional[Union[str, Path]] = None) -> None:
        """Atomically saves state to JSON."""
        target_path = Path(filepath) if filepath else self.state_file
        target_path.parent.mkdir(parents=True, exist_ok=True)

        payload = {
            "version": "2.4.0",
            "last_updated": time.time(),
            "decay_half_life_days": self.half_life_seconds / 86400.0,
            "sample_maturity_alpha": self.alpha,
            "events": [e.to_dict() for e in self.events],
            "evaluation_history": [e.to_dict() for e in self.evaluation_history],
        }

        temp_path = target_path.with_suffix(".tmp")
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        os.replace(temp_path, target_path)

    def load_state(self, filepath: Optional[Union[str, Path]] = None) -> bool:
        """Loads state from JSON if file exists."""
        target_path = Path(filepath) if filepath else self.state_file
        if not target_path.exists():
            return False

        try:
            with open(target_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            self.events = [ArchitecturalEvent.from_dict(e) for e in data.get("events", [])]
            self.evaluation_history = [
                EvaluationResult.from_dict(e) for e in data.get("evaluation_history", [])
            ]
            if "decay_half_life_days" in data:
                self.half_life_seconds = float(data["decay_half_life_days"]) * 86400.0
                self.decay_lambda = math.log(2.0) / self.half_life_seconds
            if "sample_maturity_alpha" in data:
                self.alpha = float(data["sample_maturity_alpha"])
            return True
        except Exception as exc:
            sys.stderr.write(f"[WARN] Failed to load curriculum state: {exc}\n")
            return False

    def export_markdown_report(self, output_path: Optional[Union[str, Path]] = None) -> str:
        """
        Renders an executive, publication-grade Markdown digest featuring
        ASCII radar bars, mathematical formulations, and next pedagogical milestones.
        """
        dest_path = Path(output_path) if output_path else self.markdown_file
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        mastery_map = self.get_overall_mastery()
        weakest = self.identify_weakest_domain()
        composite = sum(c.score for c in mastery_map.values()) / len(mastery_map)
        composite_level = MasteryLevel.from_score(composite)

        # ASCII Progress helper
        def ascii_bar(score: float, width: int = 18) -> str:
            filled = int(round(score * width))
            filled = max(0, min(width, filled))
            return "█" * filled + "░" * (width - filled)

        lines: List[str] = [
            "# Hermes Executive Architectural Curriculum & Socratic Mastery Cockpit",
            "",
            "> **Platform**: Autonomous Engineering Fleet — Continuous Distributed Systems Mastery Engine  ",
            f"> **Report Timestamp**: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}  ",
            f"> **Fleet Composite Mastery**: `{composite*100:.1f}%` — **{composite_level.color_badge}**",
            "",
            "---",
            "",
            "## 1. Dynamic Competency Radar & Domain Proficiency Matrix",
            "",
            "```text",
            "+---------------------------------------------------------------------------------------------------------+",
            "| ARCHITECTURAL COMPETENCY RADAR & SOCRATIC MASTERY MATRIX                                                |",
            "+-----------------------------------------------+---------+--------------------+--------+-----------------ﻲ",
            "| Competency Domain                             | Score   | Progress Bar       | Events | Mastery Tier    |",
            "+-----------------------------------------------+---------+--------------------+--------+-----------------+",
        ]

        for domain in CompetencyDomain:
            c = mastery_map[domain.value]
            bar = ascii_bar(c.score)
            dname = (domain.display_name[:45]).ljust(45)
            pct = f"{c.score*100:5.1f}%"
            evts = str(c.event_count).rjust(6)
            lvl = c.level.value[:15].ljust(15)
            lines.append(f"| {dname} | {pct}   | [{bar}] | {evts} | {lvl} |")

        lines.extend([
            "+-----------------------------------------------+---------+--------------------+--------+-----------------+",
            "```",
            "",
            "### Mathematical Scoring Formulation",
            "",
            "The competency level $M_d(t) \\in [0, 1]$ for each domain $d \\in \\mathcal{D}$ is continuously evaluated as:",
            "",
            "$$M_d(t) = \\left(1 - e^{-\\alpha \\cdot N_{\\text{events}}(d)}\\right) \\cdot "
            "\\frac{\\sum_{e \\in E_d} w_e \\cdot \\text{Score}(e) \\cdot e^{-\\lambda (t - t_e)}}"
            "{\\sum_{e \\in E_d} w_e \\cdot e^{-\\lambda (t - t_e)} + \\epsilon}$$",
            "",
            "- **Knowledge Decay Rate**: $\\lambda = \\frac{\\ln(2)}{T_{1/2}}$ with retention half-life $T_{1/2} = 14$ days.",
            "- **Sample Maturity Factor**: $\\alpha = 0.25$ smoothly scales mastery confidence as event count $N$ increases.",
            "- **Real-time Recency**: Milestones and peer-reviewed challenge responses decay exponentially if unexercised.",
            "",
            "---",
            "",
            f"## 2. Primary Pedagogical Deficit Analysis: `{weakest.display_name}`",
            "",
            f"The autonomous evaluator has identified **{weakest.display_name}** as the primary learning bottleneck "
            f"with a score of `{mastery_map[weakest.value].score*100:.1f}%` ({mastery_map[weakest.value].level.value}).",
            "",
            f"**Domain Focus**: {weakest.description}",
            "",
            "### Recommended Socratic Remediation Kata",
        ])

        # Find a challenge for the weakest domain
        weakest_challenges = self.challenge_library.get(weakest, [])
        if weakest_challenges:
            target_chal = weakest_challenges[0]
            lines.extend([
                f"**Challenge ID**: `{target_chal.challenge_id}` — *{target_chal.scenario_title}*",
                "",
                f"> **Scenario**: {target_chal.scenario_text}",
                "",
                f"**Architectural Dilemma**: *{target_chal.dilemma_question}*",
                "",
                "#### Architectural Trade-Off Matrix:",
                "",
                "| Option | Pattern | Trade-Off Impact | Anti-Pattern Hazard |",
                "| :--- | :--- | :--- | :--- |",
            ])
            for opt in target_chal.options:
                flag = " ⭐ **(Optimal)**" if opt.is_optimal else ""
                lines.append(
                    f"| `{opt.option_id}` | {opt.architectural_pattern}{flag} | "
                    f"{opt.trade_off_analysis} | {opt.anti_pattern_risk} |"
                )

            lines.extend([
                "",
                f"**Catastrophic Failure Prevented**: `{target_chal.catastrophic_failure_prevented}`",
                "",
                "**Essential Literature & Foundational Papers**:",
            ])
            for reading in target_chal.recommended_reading:
                lines.append(f"- 📖 {reading}")
        else:
            lines.append("No active challenge templates queued for this domain.")

        lines.extend([
            "",
            "---",
            "",
            "## 3. Recent Socratic Evaluations & Counterfactual Feedback",
            "",
        ])

        if not self.evaluation_history:
            lines.append("*No interactive evaluations recorded in this cycle.*")
        else:
            lines.append("| Timestamp | Domain | Option | Score | Delta | Critique |")
            lines.append("| :--- | :--- | :--- | :--- | :--- | :--- |")
            for eval_res in reversed(self.evaluation_history[-5:]):
                t_str = time.strftime("%H:%M:%S", time.gmtime(eval_res.timestamp))
                sign = "+" if eval_res.pedagogical_delta >= 0 else ""
                lines.append(
                    f"| `{t_str}` | {eval_res.domain.name} | `{eval_res.selected_option_id}` | "
                    f"`{eval_res.score_achieved:.2f}` | `{sign}{eval_res.pedagogical_delta*100:.1f}%` | "
                    f"{eval_res.counterfactual_critique[:120]}... |"
                )

        lines.append("")
        content = "\n".join(lines)

        temp_md = dest_path.with_suffix(".tmp")
        with open(temp_md, "w", encoding="utf-8") as f:
            f.write(content)
        os.replace(temp_md, dest_path)

        return content


# ==============================================================================
# 5. COMPREHENSIVE VERIFICATION & TEST SUITE
# ==============================================================================

def run_test_suite() -> None:
    """Executes thorough unit tests validating the entire curriculum pipeline."""
    import tempfile
    print("[TEST] Initializing Architectural Curriculum Verification Suite...")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        engine = CurriculumEngine(decay_half_life_days=14.0, sample_maturity_alpha=0.3, state_dir=tmp_path)

        # ----------------------------------------------------------------------
        # Test 1: Empty Initial Mastery
        # ----------------------------------------------------------------------
        initial_mastery = engine.get_overall_mastery()
        assert len(initial_mastery) == 6, "Must encompass all 6 canonical domains"
        for d, score in initial_mastery.items():
            assert score.score == 0.0, f"Initial score for {d} must be 0.0"
            assert score.level == MasteryLevel.NOVICE

        # ----------------------------------------------------------------------
        # Test 2: Event Ingestion & Sample Maturity Scaling
        # ----------------------------------------------------------------------
        t0 = time.time()
        engine.record_event(CompetencyDomain.CONSENSUS_QUORUMS, score=1.0, weight=1.0)
        s1 = engine.calculate_domain_mastery(CompetencyDomain.CONSENSUS_QUORUMS, evaluation_timestamp=t0)
        # N=1, alpha=0.3 -> confidence = 1 - e^(-0.3) ~ 0.25918
        expected_conf1 = 1.0 - math.exp(-0.3 * 1)
        assert abs(s1.score - expected_conf1) < 1e-4, f"Expected {expected_conf1}, got {s1.score}"

        # Ingest 9 more perfect events
        for i in range(2, 11):
            engine.record_event(CompetencyDomain.CONSENSUS_QUORUMS, score=1.0, weight=1.0)
        s10 = engine.calculate_domain_mastery(CompetencyDomain.CONSENSUS_QUORUMS, evaluation_timestamp=t0)
        expected_conf10 = 1.0 - math.exp(-0.3 * 10)
        assert abs(s10.score - expected_conf10) < 1e-4, f"Expected {expected_conf10}, got {s10.score}"
        assert s10.level == MasteryLevel.PRINCIPAL_DISTRIBUTED_SYSTEMS
        print("  ✓ Exponential sample maturity and confidence scaling verified.")

        # ----------------------------------------------------------------------
        # Test 3: Mathematical Exponential Retention Decay
        # ----------------------------------------------------------------------
        # Fast forward 14 days (half-life)
        t_14d = t0 + (14.0 * 86400.0)
        s_decayed = engine.calculate_domain_mastery(CompetencyDomain.CONSENSUS_QUORUMS, evaluation_timestamp=t_14d)
        # Ratio of scores with identical event scores remains 1.0, but confidence remains based on N
        # Now add an event 14 days ago with score 1.0, and a new event at t_14d with score 0.0
        engine_decay = CurriculumEngine(decay_half_life_days=14.0, sample_maturity_alpha=10.0, state_dir=tmp_path)
        engine_decay.events = [
            ArchitecturalEvent(event_id="E1", domain=CompetencyDomain.SAGAS_COMPENSATION, score=1.0, weight=1.0, timestamp=t0),
            ArchitecturalEvent(event_id="E2", domain=CompetencyDomain.SAGAS_COMPENSATION, score=0.0, weight=1.0, timestamp=t_14d),
        ]
        # At t_14d: E1 weight = 1.0 * e^(-ln2) = 0.5. E2 weight = 1.0 * 1.0 = 1.0.
        # Weighted avg = (0.5 * 1.0 + 1.0 * 0.0) / (0.5 + 1.0) = 0.5 / 1.5 = 1/3 ~ 0.3333
        decay_score = engine_decay.calculate_domain_mastery(CompetencyDomain.SAGAS_COMPENSATION, evaluation_timestamp=t_14d)
        assert abs(decay_score.score - (1.0 / 3.0)) < 1e-4, f"Expected 0.3333, got {decay_score.score}"
        print("  ✓ Mathematical half-life retention decay (lambda = ln(2)/T_half) verified.")

        # ----------------------------------------------------------------------
        # Test 4: Deficit Identification (d* = argmin M_d)
        # ----------------------------------------------------------------------
        # Populate all domains with scores except CAPACITY_THUNDERING_HERD
        for dom in CompetencyDomain:
            if dom != CompetencyDomain.CAPACITY_THUNDERING_HERD:
                engine.record_event(dom, score=0.8, weight=1.0)

        weakest = engine.identify_weakest_domain()
        assert weakest == CompetencyDomain.CAPACITY_THUNDERING_HERD, (
            f"Expected CAPACITY_THUNDERING_HERD, got {weakest}"
        )
        print("  ✓ Deficit identifier (d* = argmin M_d) accurately located primary bottleneck.")

        # ----------------------------------------------------------------------
        # Test 5: Socratic Challenge Generation & Evaluation
        # ----------------------------------------------------------------------
        challenge = engine.generate_challenge(CompetencyDomain.CAPACITY_THUNDERING_HERD)
        assert challenge.domain == CompetencyDomain.CAPACITY_THUNDERING_HERD
        assert len(challenge.options) >= 3
        assert challenge.optimal_choice_id == "OPT-B"

        # Evaluate suboptimal choice (Anti-pattern OPT-A)
        res_suboptimal = engine.evaluate_response(challenge.challenge_id, "OPT-A")
        assert not res_suboptimal.is_correct
        assert res_suboptimal.score_achieved < 0.5
        assert "SUB-OPTIMAL" in res_suboptimal.counterfactual_critique

        # Evaluate optimal choice (Singleflight OPT-B)
        res_optimal = engine.evaluate_response(challenge.challenge_id, "OPT-B")
        assert res_optimal.is_correct
        assert res_optimal.score_achieved == 1.0
        assert res_optimal.pedagogical_delta > 0.0
        print("  ✓ Counterfactual evaluation, trade-off matrix grading, and pedagogical delta confirmed.")

        # ----------------------------------------------------------------------
        # Test 6: Persistence & Atomic Markdown Report Generation
        # ----------------------------------------------------------------------
        state_dict = engine.get_curriculum_state()
        assert state_dict["composite_mastery_score"] >= 0.0
        assert "domains" in state_dict

        md_output = engine.export_markdown_report()
        assert Path(engine.markdown_file).exists()
        assert "ARCHITECTURAL COMPETENCY RADAR & SOCRATIC MASTERY MATRIX" in md_output
        assert "Optimal Probabilistic Cache Stampede Prevention" in md_output

        # Verify state reload fidelity
        reloaded_engine = CurriculumEngine(state_dir=tmp_path)
        assert len(reloaded_engine.events) == len(engine.events)
        assert len(reloaded_engine.evaluation_history) == len(engine.evaluation_history)
        print("  ✓ Atomic state serialization and executive Markdown digest export validated.")

    print("[SUCCESS] All architectural curriculum tests passed with publication-grade fidelity.")


# ==============================================================================
# 6. CLI INTERFACE
# ==============================================================================

def main() -> None:
    """Entry point for standalone execution and automated CI/CD validation."""
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        run_test_suite()
        return

    print("================================================================================")
    print("Hermes Autonomous Architectural Curriculum & Socratic Mastery Evaluator")
    print("================================================================================")
    engine = CurriculumEngine()

    # Ingest baseline verification events if state empty
    if not engine.events:
        print("[INIT] Bootstrapping canonical architectural competency state...")
        baseline_events = [
            (CompetencyDomain.CONSENSUS_QUORUMS, 0.90),
            (CompetencyDomain.CONCURRENCY_FENCING, 0.85),
            (CompetencyDomain.FAULT_TOLERANCE_CIRCUITS, 0.70),
            (CompetencyDomain.SAGAS_COMPENSATION, 0.40),
            (CompetencyDomain.CQRS_OUTBOX, 0.75),
            (CompetencyDomain.CAPACITY_THUNDERING_HERD, 0.50),
        ]
        for domain, score in baseline_events:
            engine.record_event(domain, score=score, weight=1.0)

    # Display cockpit status
    state = engine.get_curriculum_state()
    print(f"Composite Mastery: {state['composite_mastery_score']*100:.1f}% ({state['composite_mastery_level']})")
    print(f"Primary Deficit Target: {state['weakest_domain_name']}")
    print("--------------------------------------------------------------------------------")

    for dom_key, data in state["domains"].items():
        bar_len = int(round(data["score"] * 15))
        bar = "█" * bar_len + "░" * (15 - bar_len)
        print(f"  {data['domain_name'][:42].ljust(42)} [{bar}] {data['score']*100:5.1f}% | {data['level']}")

    print("--------------------------------------------------------------------------------")
    challenge = engine.generate_challenge()
    print(f"[SOCRATIC CHALLENGE] {challenge.challenge_id}: {challenge.scenario_title}")
    print(f"Scenario: {challenge.scenario_text[:180]}...")
    print(f"Dilemma:  {challenge.dilemma_question}")
    print("Available Architectural Strategies:")
    for opt in challenge.options:
        print(f"  [{opt.option_id}] {opt.title}")

    # Generate fresh markdown report
    md_path = engine.export_markdown_report()
    print(f"\n[REPORT EXPORTED] -> {engine.markdown_file}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        run_test_suite()
    else:
        main()
        run_test_suite()
src/common/src/util/iter.rs
// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::iter::FusedIterator;

use itertools::Itertools;

/// Zip two iterators, panicking if they are not of the same length.
pub fn zip_eq_fast<I, J>(
    i: I,
    j: J,
) -> impl ExactSizeIterator<Item = (I::Item, J::Item)> + FusedIterator
where
    I: IntoIterator,
    J: IntoIterator,
    I::IntoIter: ExactSizeIterator + FusedIterator,
    J::IntoIter: ExactSizeIterator + FusedIterator,
{
    let i = i.into_iter();
    let j = j.into_iter();
    assert_eq!(
        i.len(),
        j.len(),
        "zip_eq_fast: iterators must have the same length"
    );
    i.zip(j)
}

/// An extension trait for [`Iterator`] that provides `zip_eq_fast`.
pub trait ZipEqFast: Iterator + Sized {
    /// Zip two iterators, panicking if they are not of the same length.
    fn zip_eq_fast<J>(
        self,
        j: J,
    ) -> impl ExactSizeIterator<Item = (Self::Item, J::Item)> + FusedIterator
    where
        Self: ExactSizeIterator + FusedIterator,
        J: IntoIterator,
        J::IntoIter: ExactSizeIterator + FusedIterator,
    {
        zip_eq_fast(self, j)
    }

    /// Repeat the elements of the iterator indefinitely if it contains exactly one element,
    /// otherwise yield the elements as-is.
    ///
    /// # Panics
    ///
    /// Panics if the iterator is empty.
    fn repeat_if_singleton(self) -> RepeatIfSingleton<Self> {
        RepeatIfSingleton::new(self)
    }
}

impl<I: Iterator> ZipEqFast for I {}

/// An iterator that repeats the elements of the inner iterator indefinitely if it contains
/// exactly one element, otherwise yields the elements as-is.
pub struct RepeatIfSingleton<I: Iterator> {
    state: RepeatIfSingletonState<I>,
}

enum RepeatIfSingletonState<I: Iterator> {
    /// The inner iterator is not exhausted and there's a buffered first item.
    Multiple {
        first: Option<I::Item>,
        inner: I,
    },
    /// The inner iterator contains only one item, and we should repeat it.
    Repeat {
        item: I::Item,
    },
}

impl<I: Iterator> RepeatIfSingleton<I> {
    fn new(mut inner: I) -> Self {
        let first = inner.next().expect("iterator must not be empty");
        let second = inner.next();

        let state = match second {
            Some(second) => RepeatIfSingletonState::Multiple {
                first: Some(first),
                inner: [second].into_iter().chain(inner),
            },
            None => RepeatIfSingletonState::Repeat { item: first },
        };

        Self { state }
    }
}

impl<I: Iterator> Iterator for RepeatIfSingleton<I>
where
    I::Item: Clone,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.state {
            RepeatIfSingletonState::Multiple { first, inner } => {
                first.take().or_else(|| inner.next())
            }
            RepeatIfSingletonState::Repeat { item } => Some(item.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_repeat_if_singleton() {
        assert_eq!(
            [1].into_iter()
                .repeat_if_singleton()
                .take(3)
                .collect_vec(),
            [1, 1, 1]
        );

        assert_eq!(
            [1, 2, 3]
                .into_iter()
                .repeat_if_singleton()
                .take(3)
                .collect_vec(),
            [1, 2, 3]
        );

        assert_eq!(
            [1, 2, 3]
                .into_iter()
                .repeat_if_singleton()
                .take(5)
                .collect_vec(),
            [1, 2, 3]
        );
    }

    #[test]
    #[should_panic]
    fn test_repeat_if_singleton_empty() {
        let _ = std::iter::empty::<()>().repeat_if_singleton();
    }
}

#[macro_export]
macro_rules! count_idents {
    ($($idents:ident),* $(,)?) => {
        <[()]>::len(&[
            $(
                $crate::count_idents!(@sub $idents)
            ),*
        ])
    };
    (@sub $ident:ident) => { () };
}

#[macro_export]
macro_rules! impl_multizip {
    (
        $multizip_fn:ident,
        $multizip_eq_fast_fn:ident,
        $multizip_struct:ident,
        $multizip_eq_fast_struct:ident,
        $(($idx:tt, $all_idx:tt, $t:ident, $i:ident)),+
    ) => {
        pub struct $multizip_struct<$($t,)+> {
            $($i: $t,)+
        }

        pub struct $multizip_eq_fast_struct<$($t,)+> {
            $($i: $t,)+
            len: usize,
        }

        impl<$($t: Iterator,)+> Iterator for $multizip_struct<$($t,)+> {
            type Item = ($($t::Item,)+);

            fn next(&mut self) -> Option<Self::Item> {
                $(
                    let $i = match self.$i.next() {
                        Some($i) => $i,
                        None => return None,
                    };
                )+
                Some(($($i,)+))
            }

            fn size_hint(&self) -> (usize, Option<usize>) {
                let mut min = usize::MAX;
                let mut max = None;
                $(
                    let (t_min, t_max) = self.$i.size_hint();
                    min = min.min(t_min);
                    max = match (max, t_max) {
                        (Some(max), Some(t_max)) => Some(std::cmp::min(max, t_max)),
                        (None, Some(t_max)) => Some(t_max),
                        (Some(max), None) => Some(max),
                        (None, None) => None,
                    };
                )+
                (min, max)
            }
        }

        impl<$($t: ExactSizeIterator + std::iter::FusedIterator,)+> Iterator for $multizip_eq_fast_struct<$($t,)+> {
            type Item = ($($t::Item,)+);

            fn next(&mut self) -> Option<Self::Item> {
                if self.len == 0 {
                    return None;
                }
                self.len -= 1;
                $(
                    let $i = self.$i.next().unwrap();
                )+
                Some(($($i,)+))
            }

            fn size_hint(&self) -> (usize, Option<usize>) {
                (self.len, Some(self.len))
            }
        }

        impl<$($t: ExactSizeIterator + std::iter::FusedIterator,)+> ExactSizeIterator for $multizip_eq_fast_struct<$($t,)+> {}
        impl<$($t: ExactSizeIterator + std::iter::FusedIterator,)+> std::iter::FusedIterator for $multizip_eq_fast_struct<$($t,)+> {}

        #[allow(non_snake_case)]
        pub fn $multizip_fn<$($t,)+>($($i: $t,)+) -> $multizip_struct<$($t::IntoIter,)+>
        where
            $($t: IntoIterator,)+
        {
            $multizip_struct {
                $($i: $i.into_iter(),)+
            }
        }

        #[allow(non_snake_case)]
        pub fn $multizip_eq_fast_fn<$($t,)+>($($i: $t,)+) -> $multizip_eq_fast_struct<$($t::IntoIter,)+>
        where
            $($t: IntoIterator,)+
            $($t::IntoIter: ExactSizeIterator + std::iter::FusedIterator,)+
        {
            $(let $i = $i.into_iter();)+
            let len = $crate::multizip_eq_fast_check_len!($($i),+);
            $multizip_eq_fast_struct {
                $($i,)+
                len,
            }
        }
    };
}

#[macro_export]
macro_rules! multizip_eq_fast_check_len {
    ($head:ident, $($tail:ident),+) => {
        {
            let len = $head.len();
            $(
                assert_eq!(len, $tail.len(), "multizip_eq_fast: iterators must have the same length");
            )+
            len
        }
    };
}

impl_multizip! {
    multizip_2,
    multizip_eq_fast_2,
    MultiZip2,
    MultiZipEqFast2,
    (0, 2, A, a),
    (1, 2, B, b)
}

impl_multizip! {
    multizip_3,
    multizip_eq_fast_3,
    MultiZip3,
    MultiZipEqFast3,
    (0, 3, A, a),
    (1, 3, B, b),
    (2, 3, C, c)
}

impl_multizip! {
    multizip_4,
    multizip_eq_fast_4,
    MultiZip4,
    MultiZipEqFast4,
    (0, 4, A, a),
    (1, 4, B, b),
    (2, 4, C, c),
    (3, 4, D, d)
}

impl_multizip! {
    multizip_5,
    multizip_eq_fast_5,
    MultiZip5,
    MultiZipEqFast5,
    (0, 5, A, a),
    (1, 5, B, b),
    (2, 5, C, c),
    (3, 5, D, d),
    (4, 5, E, e)
}

impl_multizip! {
    multizip_6,
    multizip_eq_fast_6,
    MultiZip6,
    MultiZipEqFast6,
    (0, 6, A, a),
    (1, 6, B, b),
    (2, 6, C, c),
    (3, 6, D, d),
    (4, 6, E, e),
    (5, 6, F, f)
}

impl_multizip! {
    multizip_7,
    multizip_eq_fast_7,
    MultiZip7,
    MultiZipEqFast7,
    (0, 7, A, a),
    (1, 7, B, b),
    (2, 7, C, c),
    (3, 7, D, d),
    (4, 7, E, e),
    (5, 7, F, f),
    (6, 7, G, g)
}

impl_multizip! {
    multizip_8,
    multizip_eq_fast_8,
    MultiZip8,
    MultiZipEqFast8,
    (0, 8, A, a),
    (1, 8, B, b),
    (2, 8, C, c),
    (3, 8, D, d),
    (4, 8, E, e),
    (5, 8, F, f),
    (6, 8, G, g),
    (7, 8, H, h)
}

impl_multizip! {
    multizip_9,
    multizip_eq_fast_9,
    MultiZip9,
    MultiZipEqFast9,
    (0, 9, A, a),
    (1, 9, B, b),
    (2, 9, C, c),
    (3, 9, D, d),
    (4, 9, E, e),
    (5, 9, F, f),
    (6, 9, G, g),
    (7, 9, H, h),
    (8, 9, I, i)
}

impl_multizip! {
    multizip_10,
    multizip_eq_fast_10,
    MultiZip10,
    MultiZipEqFast10,
    (0, 10, A, a),
    (1, 10, B, b),
    (2, 10, C, c),
    (3, 10, D, d),
    (4, 10, E, e),
    (5, 10, F, f),
    (6, 10, G, g),
    (7, 10, H, h),
    (8, 10, I, i),
    (9, 10, J, j)
}

impl_multizip! {
    multizip_11,
    multizip_eq_fast_11,
    MultiZip11,
    MultiZipEqFast11,
    (0, 11, A, a),
    (1, 11, B, b),
    (2, 11, C, c),
    (3, 11, D, d),
    (4, 11, E, e),
    (5, 11, F, f),
    (6, 11, G, g),
    (7, 11, H, h),
    (8, 11, I, i),
    (9, 11, J, j),
    (10, 11, K, k)
}

impl_multizip! {
    multizip_12,
    multizip_eq_fast_12,
    MultiZip12,
    MultiZipEqFast12,
    (0, 12, A, a),
    (1, 12, B, b),
    (2, 12, C, c),
    (3, 12, D, d),
    (4, 12, E, e),
    (5, 12, F, f),
    (6, 12, G, g),
    (7, 12, H, h),
    (8, 12, I, i),
    (9, 12, J, j),
    (10, 12, K, k),
    (11, 12, L, l)
}

#[macro_export]
macro_rules! multizip {
    ($a:expr, $b:expr $(,)?) => {
        $crate::util::iter::multizip_2($a, $b)
    };
    ($a:expr, $b:expr, $c:expr $(,)?) => {
        $crate::util::iter::multizip_3($a, $b, $c)
    };
    ($a:expr, $b:expr, $c:expr, $d:expr $(,)?) => {
        $crate::util::iter::multizip_4($a, $b, $c, $d)
    };
    ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr $(,)?) => {
        $crate::util::iter::multizip_5($a, $b, $c, $d, $e)
    };
    ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr $(,)?) => {
        $crate::util::iter::multizip_6($a, $b, $c, $d, $e, $f)
    };
    ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr, $g:expr $(,)?) => {
        $crate::util::iter::multizip_7($a, $b, $c, $d, $e, $f, $g)
    };
    ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr, $g:expr, $h:expr $(,)?) => {
        $crate::util::iter::multizip_8($a, $b, $c, $d, $e, $f, $g, $h)
    };
    ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr, $g:expr, $h:expr, $i:expr $(,)?) => {
        $crate::util::iter::multizip_9($a, $b, $c, $d, $e, $f, $g, $h, $i)
    };
    ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr, $g:expr, $h:expr, $i:expr, $j:expr $(,)?) => {
        $crate::util::iter::multizip_10($a, $b, $c, $d, $e, $f, $g, $h, $i, $j)
    };
    ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr, $g:expr, $h:expr, $i:expr, $j:expr, $k:expr $(,)?) => {
        $crate::util::iter::multizip_11($a, $b, $c, $d, $e, $f, $g, $h, $i, $j, $k)
    };
    ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr, $g:expr, $h:expr, $i:expr, $j:expr, $k:expr, $l:expr $(,)?) => {
        $crate::util::iter::multizip_12($a, $b, $c, $d, $e, $f, $g, $h, $i, $j, $k, $l)
    };
}

#[macro_export]
macro_rules! multizip_eq_fast {
    ($a:expr, $b:expr $(,)?) => {
        $crate::util::iter::multizip_eq_fast_2($a, $b)
    };
    ($a:expr, $b:expr, $c:expr $(,)?) => {
        $crate::util::iter::multizip_eq_fast_3($a, $b, $c)
    };
    ($a:expr, $b:expr, $c:expr, $d:expr $(,)?) => {
        $crate::util::iter::multizip_eq_fast_4($a, $b, $c, $d)
    };
    ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr $(,)?) => {
        $crate::util::iter::multizip_eq_fast_5($a, $b, $c, $d, $e)
    };
    ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr $(,)?) => {
        $crate::util::iter::multizip_eq_fast_6($a, $b, $c, $d, $e, $f)
    };
    ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr, $g:expr $(,)?) => {
        $crate::util::iter::multizip_eq_fast_7($a, $b, $c, $d, $e, $f, $g)
    };
    ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr, $g:expr, $h:expr $(,)?) => {
        $crate::util::iter::multizip_eq_fast_8($a, $b, $c, $d, $e, $f, $g, $h)
    };
    ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr, $g:expr, $h:expr, $i:expr $(,)?) => {
        $crate::util::iter::multizip_eq_fast_9($a, $b, $c, $d, $e, $f, $g, $h, $i)
    };
    ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr, $g:expr, $h:expr, $i:expr, $j:expr $(,)?) => {
        $crate::util::iter::multizip_eq_fast_10($a, $b, $c, $d, $e, $f, $g, $h, $i, $j)
    };
    ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr, $g:expr, $h:expr, $i:expr, $j:expr, $k:expr $(,)?) => {
        $crate::util::iter::multizip_eq_fast_11($a, $b, $c, $d, $e, $f, $g, $h, $i, $j, $k)
    };
    ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr, $g:expr, $h:expr, $i:expr, $j:expr, $k:expr, $l:expr $(,)?) => {
        $crate::util::iter::multizip_eq_fast_12($a, $b, $c, $d, $e, $f, $g, $h, $i, $j, $k, $l)
    };
}

#[cfg(test)]
mod multizip_tests {
    use itertools::Itertools;

    #[test]
    fn test_multizip() {
        let a = vec![1, 2, 3];
        let b = vec![4, 5, 6];
        let c = vec![7, 8, 9];

        let zipped = multizip!(a, b, c).collect_vec();
        assert_eq!(zipped, vec![(1, 4, 7), (2, 5, 8), (3, 6, 9)]);

        let a = vec![1, 2, 3];
        let b = vec![4, 5];
        let c = vec![7, 8, 9];

        let zipped = multizip!(a, b, c).collect_vec();
        assert_eq!(zipped, vec![(1, 4, 7), (2, 5, 8)]);
    }

    #[test]
    fn test_multizip_eq_fast() {
        let a = vec![1, 2, 3];
        let b = vec![4, 5, 6];
        let c = vec![7, 8, 9];

        let zipped = multizip_eq_fast!(a, b, c).collect_vec();
        assert_eq!(zipped, vec![(1, 4, 7), (2, 5, 8), (3, 6, 9)]);
    }

    #[test]
    #[should_panic]
    fn test_multizip_eq_fast_panic() {
        let a = vec![1, 2, 3];
        let b = vec![4, 5];
        let c = vec![7, 8, 9];

        let _ = multizip_eq_fast!(a, b, c);
    }
}

pub trait IsSortedByExt: Iterator {
    /// Copied from `std::iter::Iterator::is_sorted_by` because it is still an unstable feature.
    ///
    /// # Implementation Note
    ///
    /// If you implement your own partial order, please double check that the order is a partial
    /// order, otherwise the result may be unexpected.
    ///
    /// The `compare` function should satisfy the following properties:
    ///
    /// - Transitivity: `compare(a, b) && compare(b, c) ==> compare(a, c)`
    /// - Antisymmetry: `compare(a, b) && compare(b, a) ==> a == b`
    ///
    /// Note that `compare(a, b)` can be either `<` or `<=`.
    fn is_sorted_by<F>(mut self, mut compare: F) -> bool
    where
        Self: Sized,
        F: FnMut(&Self::Item, &Self::Item) -> bool,
    {
        let mut last = match self.next() {
            Some(e) => e,
            None => return true,
        };
        for curr in self {
            if !compare(&last, &curr) {
                return false;
            }
            last = curr;
        }
        true
    }
}

impl<T: ?Sized> IsSortedByExt for T where T: Iterator {}

#[cfg(test)]
mod is_sorted_by_tests {
    use super::IsSortedByExt;

    #[test]
    fn test_is_sorted_by() {
        let a = [1, 2, 3, 4, 5];
        assert!(a.into_iter().is_sorted_by(|x, y| x <= y));
        assert!(a.into_iter().is_sorted_by(|x, y| x < y));
        assert!(!a.into_iter().is_sorted_by(|x, y| x >= y));
        assert!(!a.into_iter().is_sorted_by(|x, y| x > y));

        let a = [1, 2, 2, 4, 5];
        assert!(a.into_iter().is_sorted_by(|x, y| x <= y));
        assert!(!a.into_iter().is_sorted_by(|x, y| x < y));

        let a = [5, 4, 3, 2, 1];
        assert!(a.into_iter().is_sorted_by(|x, y| x >= y));
        assert!(a.into_iter().is_sorted_by(|x, y| x > y));
        assert!(!a.into_iter().is_sorted_by(|x, y| x <= y));
        assert!(!a.into_iter().is_sorted_by(|x, y| x < y));

        let a = [5, 4, 4, 2, 1];
        assert!(a.into_iter().is_sorted_by(|x, y| x >= y));
        assert!(!a.into_iter().is_sorted_by(|x, y| x > y));

        let a: [i32; 0] = [];
        assert!(a.into_iter().is_sorted_by(|x, y| x <= y));
        assert!(a.into_iter().is_sorted_by(|x, y| x >= y));

        let a = [1];
        assert!(a.into_iter().is_sorted_by(|x, y| x <= y));
        assert!(a.into_iter().is_sorted_by(|x, y| x >= y));

        let a = [1, 2, 3, 2, 1];
        assert!(!a.into_iter().is_sorted_by(|x, y| x <= y));
        assert!(!a.into_iter().is_sorted_by(|x, y| x >= y));
    }
}

pub trait KMergeByExt: Iterator {
    /// A helper function to create a [`KMergeBy`] iterator.
    fn kmerge_by<F>(self, compare: F) -> KMergeBy<Self::Item, F>
    where
        Self: Sized,
        Self::Item: Iterator,
        F: FnMut(&<Self::Item as Iterator>::Item, &<Self::Item as Iterator>::Item) -> bool,
    {
        KMergeBy::new(self, compare)
    }
}

impl<T: ?Sized> KMergeByExt for T where T: Iterator {}

/// An iterator that merges multiple sorted iterators into one sorted iterator.
///
/// It does a similar thing as [`itertools::kmerge_by`], but:
/// - It uses a customized compare function, where it takes two elements and returns whether the
///   first element is smaller than or equal to the second element (which is the same as
///   [`itertools::kmerge_by`]).
/// - When comparing elements from different iterators, if the elements are equal according to the
///   compare function, it always prefers the element from the iterator that appears earlier in the
///   input iterators.
pub struct KMergeBy<I, F>
where
    I: Iterator,
{
    heap: Vec<KMergeByHeapNode<I>>,
    compare: F,
}

struct KMergeByHeapNode<I>
where
    I: Iterator,
{
    idx: usize,
    iter: I,
    cur: I::Item,
}

impl<I, F> KMergeBy<I, F>
where
    I: Iterator,
{
    pub fn new(iters: impl IntoIterator<Item = I>, compare: F) -> Self {
        let iters = iters.into_iter();
        let mut heap = Vec::with_capacity(iters.size_hint().0);
        for (idx, mut iter) in iters.enumerate() {
            if let Some(cur) = iter.next() {
                heap.push(KMergeByHeapNode { idx, iter, cur });
            }
        }
        let mut this = Self { heap, compare };
        this.rebuild_heap();
        this
    }

    /// Check if the element at `parent` is smaller than or equal to the element at `child`.
    ///
    /// If the elements are equal, we compare their `idx` to break the tie: smaller `idx` is
    /// considered smaller.
    fn is_smaller_or_equal(&mut self, parent: usize, child: usize) -> bool {
        let parent = &self.heap[parent];
        let child = &self.heap[child];
        if (self.compare)(&parent.cur, &child.cur) {
            if (self.compare)(&child.cur, &parent.cur) {
                // The two elements are equal, compare their `idx`.
                parent.idx < child.idx
            } else {
                // The parent element is strictly smaller than the child element.
                true
            }
        } else {
            // The parent element is strictly greater than the child element.
            false
        }
    }

    fn sift_down(&mut self, mut pos: usize) {
        let end = self.heap.len();
        while pos * 2 + 1 < end {
            let left = pos * 2 + 1;
            let right = left + 1;
            let mut smallest = pos;
            if !self.is_smaller_or_equal(smallest, left) {
                smallest = left;
            }
            if right < end && !self.is_smaller_or_equal(smallest, right) {
                smallest = right;
            }
            if smallest != pos {
                self.heap.swap(pos, smallest);
                pos = smallest;
            } else {
                break;
            }
        }
    }

    fn rebuild_heap(&mut self) {
        let len = self.heap.len();
        for i in (0..len / 2).rev() {
            self.sift_down(i);
        }
    }
}

impl<I, F> Iterator for KMergeBy<I, F>
where
    I: Iterator,
    F: FnMut(&I::Item, &I::Item) -> bool,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if self.heap.is_empty() {
            return None;
        }

        let next = self.heap[0].iter.next();
        match next {
            Some(mut next) => {
                std::mem::swap(&mut self.heap[0].cur, &mut next);
                self.sift_down(0);
                Some(next)
            }
            None => {
                let node = self.heap.swap_remove(0);
                self.sift_down(0);
                Some(node.cur)
            }
        }
    }
}

#[cfg(test)]
mod kmerge_by_tests {
    use super::KMergeByExt;

    #[test]
    fn test_kmerge_by() {
        let a = vec![1, 3, 5, 7, 9];
        let b = vec![2, 4, 6, 8, 10];
        let c = vec![1, 4, 7, 10];
        let d = vec![2, 3, 5, 8, 9];
        let merged = vec![a, b, c, d]
            .into_iter()
            .map(|x| x.into_iter())
            .kmerge_by(|x, y| x <= y)
            .collect::<Vec<_>>();
        assert_eq!(
            merged,
            vec![1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 7, 7, 8, 8, 9, 9, 10, 10]
        );
    }

    #[test]
    fn test_kmerge_by_tie_break() {
        // Tie break by `idx`.
        let a = vec![(1, 1), (2, 1), (3, 1)];
        let b = vec![(1, 2), (2, 2), (3, 2)];
        let c = vec![(1, 3), (2, 3), (3, 3)];
        let merged = vec![a, b, c]
            .into_iter()
            .map(|x| x.into_iter())
            .kmerge_by(|x, y| x.0 <= y.0)
            .collect::<Vec<_>>();
        assert_eq!(
            merged,
            vec![
                (1, 1),
                (1, 2),
                (1, 3),
                (2, 1),
                (2, 2),
                (2, 3),
                (3, 1),
                (3, 2),
                (3, 3)
            ]
        );
    }
}

/// An extension trait for [`Iterator`] that provides `collect_array`.
pub trait CollectArrayExt: Iterator {
    /// Collect the elements of the iterator into an array of a fixed size.
    ///
    /// Returns `None` if the iterator does not yield enough elements. Extra elements are ignored.
    fn collect_array<const N: usize>(self) -> Option<[Self::Item; N]>;
}

impl<I: Iterator> CollectArrayExt for I {
    fn collect_array<const N: usize>(mut self) -> Option<[Self::Item; N]> {
        // TODO: use `array::try_from_fn` when it's stabilized.
        let mut result = Vec::with_capacity(N);
        for _ in 0..N {
            result.push(self.next()?);
        }
        result.try_into().ok()
    }
}

pub trait IteratorExt: Iterator {
    /// An extension of [`itertools::Itertools::coalesce`] where the closure produces an iterator
    /// instead of a single element, and the elements yielded from this iterator are also
    /// yielded to the consumer of the `flat_coalesce` iterator.
    fn flat_coalesce<F, Output>(self, f: F) -> FlatCoalesce<Self, F, Output>
    where
        Self: Sized,
        F: FnMut(Self::Item, Self::Item) -> Result<Output, (Self::Item, Self::Item)>,
        Output: IntoIterator<Item = Self::Item>,
    {
        FlatCoalesce::new(self, f)
    }
}

impl<T: ?Sized> IteratorExt for T where T: Iterator {}

/// An iterator adaptor that may merge two consecutive elements into multiple elements.
///
/// See [`IteratorExt::flat_coalesce`] for more details.
pub struct FlatCoalesce<I, F, Output>
where
    I: Iterator,
    Output: IntoIterator,
{
    iter: I,
    f: F,
    first: Option<I::Item>,
    buffered: Option<Output::IntoIter>,
}

impl<I, F, Output> FlatCoalesce<I, F, Output>
where
    I: Iterator,
    Output: IntoIterator,
{
    pub fn new(mut iter: I, f: F) -> Self {
        let first = iter.next();
        Self {
            iter,
            f,
            first,
            buffered: None,
        }
    }
}

impl<I, F, Output> Iterator for FlatCoalesce<I, F, Output>
where
    I: Iterator,
    F: FnMut(I::Item, I::Item) -> Result<Output, (I::Item, I::Item)>,
    Output: IntoIterator<Item = I::Item>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(buffered) = &mut self.buffered {
                if let Some(item) = buffered.next() {
                    return Some(item);
                } else {
                    self.buffered = None;
                }
            }

            let first = self.first.take()?;
            let next = self.iter.next();

            match next {
                Some(second) => match (self.f)(first, second) {
                    Ok(items) => {
                        let mut items = items.into_iter();
                        match (items.next(), items.next()) {
                            (None, _) => {
                                // If the closure returns an empty iterator, we need to take the next
                                // element from the inner iterator as the new `first` element.
                                self.first = self.iter.next();
                            }
                            (Some(first), None) => {
                                // If the closure returns a single element, we need to set it as the
                                // new `first` element.
                                self.first = Some(first);
                            }
                            (Some(first), Some(second)) => {
                                self.buffered = Some([second].into_iter().chain(items));
                                return Some(first);
                            }
                        }
                    }
                    Err((first, second)) => {
                        self.first = Some(second);
                        return Some(first);
                    }
                },
                None => {
                    return Some(first);
                }
            }
        }
    }
}

#[cfg(test)]
mod flat_coalesce_tests {
    use super::*;

    #[test]
    fn test_flat_coalesce() {
        let a = vec![1, 2, 3, 4, 5];
        let merged = a
            .into_iter()
            .flat_coalesce(|x, y| {
                if x % 2 == 1 && y % 2 == 1 {
                    Ok(vec![x + y])
                } else {
                    Err((x, y))
                }
            })
            .collect_vec();
        assert_eq!(merged, vec![1, 2, 3, 4, 5]);

        let a = vec![1, 3, 2, 4, 5, 7];
        let merged = a
            .into_iter()
            .flat_coalesce(|x, y| {
                if x % 2 == 1 && y % 2 == 1 {
                    Ok(vec![x + y])
                } else {
                    Err((x, y))
                }
            })
            .collect_vec();
        assert_eq!(merged, vec![4, 2, 4, 12]);

        let a = vec![1, 3, 5, 7, 9];
        let merged = a
            .into_iter()
            .flat_coalesce(|x, y| {
                if x % 2 == 1 && y % 2 == 1 {
                    Ok(vec![x + y])
                } else {
                    Err((x, y))
                }
            })
            .collect_vec();
        assert_eq!(merged, vec![25]);

        let a = vec![1, 2, 3, 4, 5];
        let merged = a
            .into_iter()
            .flat_coalesce(|x, y| {
                if x + y == 3 {
                    Ok(vec![x, y, 100])
                } else {
                    Err((x, y))
                }
            })
            .collect_vec();
        assert_eq!(merged, vec![1, 2, 100, 3, 4, 5]);

        let a = vec![1, 2, 3, 4, 5];
        let merged = a
            .into_iter()
            .flat_coalesce(|x, y| {
                if x + y == 3 {
                    Ok(vec![])
                } else {
                    Err((x, y))
                }
            })
            .collect_vec();
        assert_eq!(merged, vec![3, 4, 5]);

        let a = vec![1, 2];
        let merged = a
            .into_iter()
            .flat_coalesce(|x, y| {
                if x + y == 3 {
                    Ok(vec![])
                } else {
                    Err((x, y))
                }
            })
            .collect_vec();
        assert_eq!(merged, Vec::<i32>::new());
    }
}

pub trait ChunkByExt: Iterator {
    /// Groups consecutive elements of the iterator into chunks based on a predicate, and returns
    /// an iterator yielding these chunks.
    ///
    /// It works similarly to [`itertools::Itertools::chunk_by`], but the elements are collected
    /// into vectors instead of being lazily yielded. Also, the predicate directly takes two
    /// consecutive elements and returns whether they should be grouped together, instead of
    /// extracting a key from each element.
    fn chunk_by_fast<F>(self, predicate: F) -> ChunkByFast<Self, F>
    where
        Self: Sized,
        F: FnMut(&Self::Item, &Self::Item) -> bool,
    {
        ChunkByFast::new(self, predicate)
    }
}

impl<T: ?Sized> ChunkByExt for T where T: Iterator {}

pub struct ChunkByFast<I, F>
where
    I: Iterator,
{
    iter: I,
    predicate: F,
    last: Option<I::Item>,
}

impl<I, F> ChunkByFast<I, F>
where
    I: Iterator,
{
    pub fn new(mut iter: I, predicate: F) -> Self {
        let last = iter.next();
        Self {
            iter,
            predicate,
            last,
        }
    }
}

impl<I, F> Iterator for ChunkByFast<I, F>
where
    I: Iterator,
    F: FnMut(&I::Item, &I::Item) -> bool,
{
    type Item = Vec<I::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut chunk = vec![self.last.take()?];
        loop {
            match self.iter.next() {
                Some(next) => {
                    if (self.predicate)(chunk.last().unwrap(), &next) {
                        chunk.push(next);
                    } else {
                        self.last = Some(next);
                        return Some(chunk);
                    }
                }
                None => return Some(chunk),
            }
        }
    }
}

#[cfg(test)]
mod chunk_by_fast_tests {
    use super::*;

    #[test]
    fn test_chunk_by_fast() {
        let a = vec![1, 1, 2, 3, 3, 3, 4, 5, 5];
        let chunks = a.into_iter().chunk_by_fast(|x, y| x == y).collect_vec();
        assert_eq!(
            chunks,
            vec![
                vec![1, 1],
                vec![2],
                vec![3, 3, 3],
                vec![4],
                vec![5, 5],
            ]
        );

        let a: Vec<i32> = vec![];
        let chunks = a.into_iter().chunk_by_fast(|x, y| x == y).collect_vec();
        assert_eq!(chunks, Vec::<Vec<i32>>::new());

        let a = vec![1];
        let chunks = a.into_iter().chunk_by_fast(|x, y| x == y).collect_vec();
        assert_eq!(chunks, vec![vec![1]]);
    }
}

/// An extension trait for [`Iterator`] that provides `peek_next_n`.
pub trait PeekNextNExt: Iterator {
    /// Return an iterator adapter that allows peeking the next `N` elements of the iterator.
    fn peek_next_n<const N: usize>(self) -> PeekNextN<Self, N>
    where
        Self: Sized,
    {
        PeekNextN::new(self)
    }
}

impl<T: ?Sized> PeekNextNExt for T where T: Iterator {}

/// An iterator adapter that allows peeking the next `N` elements of the inner iterator.
///
/// See [`PeekNextNExt::peek_next_n`] for more details.
pub struct PeekNextN<I, const N: usize>
where
    I: Iterator,
{
    iter: I,
    buf: [Option<I::Item>; N],
    pos: usize,
}

impl<I, const N: usize> PeekNextN<I, N>
where
    I: Iterator,
{
    pub fn new(mut iter: I) -> Self {
        assert!(N > 0, "N must be greater than 0");
        let buf = [(); N].map(|_| iter.next());
        Self { iter, buf, pos: 0 }
    }

    /// Peek the `i`-th element from the current position.
    ///
    /// # Panics
    ///
    /// Panics if `i >= N`.
    pub fn peek_n(&self, i: usize) -> Option<&I::Item> {
        assert!(i < N, "index out of bounds");
        let idx = (self.pos + i) % N;
        self.buf[idx].as_ref()
    }
}

impl<I, const N: usize> Iterator for PeekNextN<I, N>
where
    I: Iterator,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.iter.next();
        let cur = std::mem::replace(&mut self.buf[self.pos], next);
        self.pos = (self.pos + 1) % N;
        cur
    }
}

#[cfg(test)]
mod peek_next_n_tests {
    use super::*;

    #[test]
    fn test_peek_next_n() {
        let a = vec![1, 2, 3, 4, 5];
        let mut iter = a.into_iter().peek_next_n::<3>();
        assert_eq!(iter.peek_n(0), Some(&1));
        assert_eq!(iter.peek_n(1), Some(&2));
        assert_eq!(iter.peek_n(2), Some(&3));
        assert_eq!(iter.next(), Some(1));
        assert_eq!(iter.peek_n(0), Some(&2));
        assert_eq!(iter.peek_n(1), Some(&3));
        assert_eq!(iter.peek_n(2), Some(&4));
        assert_eq!(iter.next(), Some(2));
        assert_eq!(iter.peek_n(0), Some(&3));
        assert_eq!(iter.peek_n(1), Some(&4));
        assert_eq!(iter.peek_n(2), Some(&5));
        assert_eq!(iter.next(), Some(3));
        assert_eq!(iter.peek_n(0), Some(&4));
        assert_eq!(iter.peek_n(1), Some(&5));
        assert_eq!(iter.peek_n(2), None);
        assert_eq!(iter.next(), Some(4));
        assert_eq!(iter.peek_n(0), Some(&5));
        assert_eq!(iter.peek_n(1), None);
        assert_eq!(iter.peek_n(2), None);
        assert_eq!(iter.next(), Some(5));
        assert_eq!(iter.peek_n(0), None);
        assert_eq!(iter.peek_n(1), None);
        assert_eq!(iter.peek_n(2), None);
        assert_eq!(iter.next(), None);
    }
}

pub trait InterleaveExt: Iterator {
    /// Interleave the elements of the iterator with another element.
    fn interleave_with<T>(self, other: T) -> Interleave<Self, T>
    where
        Self: Sized,
        T: Clone,
    {
        Interleave::new(self, other)
    }
}

impl<T: ?Sized> InterleaveExt for T where T: Iterator {}

pub struct Interleave<I, T>
where
    I: Iterator,
{
    iter: I,
    other: T,
    first: bool,
    yield_other: bool,
}

impl<I, T> Interleave<I, T>
where
    I: Iterator,
    T: Clone,
{
    pub fn new(iter: I, other: T) -> Self {
        Self {
            iter,
            other,
            first: true,
            yield_other: false,
        }
    }
}

impl<I, T> Iterator for Interleave<I, T>
where
    I: Iterator<Item = T>,
    T: Clone,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.first {
            self.first = false;
            let next = self.iter.next()?;
            return Some(next);
        }

        if self.yield_other {
            self.yield_other = false;
            Some(self.other.clone())
        } else {
            let next = self.iter.next()?;
            self.yield_other = true;
            Some(next)
        }
    }
}

#[cfg(test)]
mod interleave_tests {
    use super::*;

    #[test]
    fn test_interleave() {
        let a = vec![1, 2, 3, 4, 5];
        let interleaved = a.into_iter().interleave_with(0).collect_vec();
        assert_eq!(interleaved, vec![1, 0, 2, 0, 3, 0, 4, 0, 5]);

        let a: Vec<i32> = vec![];
        let interleaved = a.into_iter().interleave_with(0).collect_vec();
        assert_eq!(interleaved, Vec::<i32>::new());

        let a = vec![1];
        let interleaved = a.into_iter().interleave_with(0).collect_vec();
        assert_eq!(interleaved, vec![1]);
    }
}

/// An extension trait for [`Iterator`] that provides `fold_until`.
pub trait FoldUntilExt: Iterator {
    /// Folds the elements of the iterator into an accumulator until the closure returns `Err(acc)`.
    fn fold_until<B, F>(mut self, init: B, mut f: F) -> B
    where
        Self: Sized,
        F: FnMut(B, Self::Item) -> Result<B, B>,
    {
        let mut acc = init;
        for item in self.by_ref() {
            match f(acc, item) {
                Ok(new_acc) => acc = new_acc,
                Err(new_acc) => return new_acc,
            }
        }
        acc
    }
}

impl<T: ?Sized> FoldUntilExt for T where T: Iterator {}

#[cfg(test)]
mod fold_until_tests {
    use super::*;

    #[test]
    fn test_fold_until() {
        let a = vec![1, 2, 3, 4, 5];
        let sum = a.into_iter().fold_until(0, |acc, x| {
            if acc + x > 5 {
                Err(acc)
            } else {
                Ok(acc + x)
            }
        });
        assert_eq!(sum, 3);
    }
}

pub trait DedupByExt: Iterator {
    /// Lazily deduplicate consecutive elements of the iterator based on a predicate, and yields
    /// the deduplicated elements.
    ///
    /// It is a customized version of `Itertools::dedup_by`, which is based on `coalesce` and
    /// generates lots of copies.
    fn dedup_by_fast<F>(self, predicate: F) -> DedupByFast<Self, F>
    where
        Self: Sized,
        F: FnMut(&Self::Item, &Self::Item) -> bool,
    {
        DedupByFast::new(self, predicate)
    }
}

impl<T: ?Sized> DedupByExt for T where T: Iterator {}

pub struct DedupByFast<I, F>
where
    I: Iterator,
{
    iter: I,
    predicate: F,
    cur: Option<I::Item>,
}

impl<I, F> DedupByFast<I, F>
where
    I: Iterator,
{
    pub fn new(mut iter: I, predicate: F) -> Self {
        let cur = iter.next();
        Self {
            iter,
            predicate,
            cur,
        }
    }
}

impl<I, F> Iterator for DedupByFast<I, F>
where
    I: Iterator,
    F: FnMut(&I::Item, &I::Item) -> bool,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let mut cur = self.cur.take()?;
        for next in self.iter.by_ref() {
            if !(self.predicate)(&cur, &next) {
                self.cur = Some(next);
                break;
            } else {
                cur = next;
            }
        }
        Some(cur)
    }
}

#[cfg(test)]
mod dedup_by_fast_tests {
    use super::*;

    #[test]
    fn test_dedup_by_fast() {
        let a = vec![1, 1, 2, 3, 3, 3, 4, 5, 5];
        let deduped = a.into_iter().dedup_by_fast(|x, y| x == y).collect_vec();
        assert_eq!(deduped, vec![1, 2, 3, 4, 5]);

        let a: Vec<i32> = vec![];
        let deduped = a.into_iter().dedup_by_fast(|x, y| x == y).collect_vec();
        assert_eq!(deduped, Vec::<i32>::new());

        let a = vec![1];
        let deduped = a.into_iter().dedup_by_fast(|x, y| x == y).collect_vec();
        assert_eq!(deduped, vec![1]);
    }

    #[test]
    fn test_dedup_by_fast_keep_last() {
        let a = vec![(1, 1), (1, 2), (2, 1), (3, 1), (3, 2), (3, 3), (4, 1)];
        let deduped = a
            .into_iter()
            .dedup_by_fast(|x, y| x.0 == y.0)
            .collect_vec();
        assert_eq!(deduped, vec![(1, 2), (2, 1), (3, 3), (4, 1)]);
    }
}

pub trait DedupExt: Iterator {
    /// Lazily deduplicate consecutive elements of the iterator, and yields the deduplicated
    /// elements.
    ///
    /// It is a customized version of `Itertools::dedup`, which is based on `coalesce` and generates
    /// lots of copies.
    fn dedup_fast(self) -> DedupByFast<Self, impl FnMut(&Self::Item, &Self::Item) -> bool>
    where
        Self: Sized,
        Self::Item: PartialEq,
    {
        self.dedup_by_fast(|x, y| x == y)
    }
}

impl<T: ?Sized> DedupExt for T where T: Iterator {}

#[cfg(test)]
mod dedup_fast_tests {
    use super::*;

    #[test]
    fn test_dedup_fast() {
        let a = vec![1, 1, 2, 3, 3, 3, 4, 5, 5];
        let deduped = a.into_iter().dedup_fast().collect_vec();
        assert_eq!(deduped, vec![1, 2, 3, 4, 5]);

        let a: Vec<i32> = vec![];
        let deduped = a.into_iter().dedup_fast().collect_vec();
        assert_eq!(deduped, Vec::<i32>::new());

        let a = vec![1];
        let deduped = a.into_iter().dedup_fast().collect_vec();
        assert_eq!(deduped, vec![1]);
    }
}

pub trait GroupConsecutiveByExt: Iterator {
    /// Groups consecutive elements of the iterator that have the same key into a vector.
    ///
    /// It differs from `chunk_by_fast` in that it also yields the key.
    fn group_consecutive_by<F, K>(self, f: F) -> GroupConsecutiveBy<Self, F, K>
    where
        Self: Sized,
        F: FnMut(&Self::Item) -> K,
        K: PartialEq,
    {
        GroupConsecutiveBy::new(self, f)
    }
}

impl<T: ?Sized> GroupConsecutiveByExt for T where T: Iterator {}

pub struct GroupConsecutiveBy<I, F, K>
where
    I: Iterator,
{
    iter: I,
    f: F,
    cur: Option<(K, I::Item)>,
}

impl<I, F, K> GroupConsecutiveBy<I, F, K>
where
    I: Iterator,
    F: FnMut(&I::Item) -> K,
{
    pub fn new(mut iter: I, mut f: F) -> Self {
        let cur = iter.next().map(|x| (f(&x), x));
        Self { iter, f, cur }
    }
}

impl<I, F, K> Iterator for GroupConsecutiveBy<I, F, K>
where
    I: Iterator,
    F: FnMut(&I::Item) -> K,
    K: PartialEq,
{
    type Item = (K, Vec<I::Item>);

    fn next(&mut self) -> Option<Self::Item> {
        let (cur_key, cur_item) = self.cur.take()?;
        let mut group = vec![cur_item];
        for next in self.iter.by_ref() {
            let next_key = (self.f)(&next);
            if next_key == cur_key {
                group.push(next);
            } else {
                self.cur = Some((next_key, next));
                break;
            }
        }
        Some((cur_key, group))
    }
}

#[cfg(test)]
mod group_consecutive_by_tests {
    use super::*;

    #[test]
    fn test_group_consecutive_by() {
        let a = vec![1, 1, 2, 3, 3, 3, 4, 5, 5];
        let groups = a.into_iter().group_consecutive_by(|x| *x).collect_vec();
        assert_eq!(
            groups,
            vec![
                (1, vec![1, 1]),
                (2, vec![2]),
                (3, vec![3, 3, 3]),
                (4, vec![4]),
                (5, vec![5, 5]),
            ]
        );

        let a: Vec<i32> = vec![];
        let groups = a.into_iter().group_consecutive_by(|x| *x).collect_vec();
        assert_eq!(groups, Vec::<(i32, Vec<i32>)>::new());

        let a = vec![1];
        let groups = a.into_iter().group_consecutive_by(|x| *x).collect_vec();
        assert_eq!(groups, vec![(1, vec![1])]);
    }
}

pub trait FilterWithPreviousExt: Iterator {
    /// Return an iterator adapter that allows filtering elements based on the previous elements.
    fn filter_with_previous<F, B>(self, init: B, f: F) -> FilterWithPrevious<Self, F, B>
    where
        Self: Sized,
        F: FnMut(&mut B, &Self::Item) -> bool,
    {
        FilterWithPrevious::new(self, init, f)
    }
}

impl<T: ?Sized> FilterWithPreviousExt for T where T: Iterator {}

pub struct FilterWithPrevious<I, F, B>
where
    I: Iterator,
{
    iter: I,
    f: F,
    acc: B,
}

impl<I, F, B> FilterWithPrevious<I, F, B>
where
    I: Iterator,
{
    pub fn new(iter: I, init: B, f: F) -> Self {
        Self { iter, f, acc: init }
    }
}

impl<I, F, B> Iterator for FilterWithPrevious<I, F, B>
where
    I: Iterator,
    F: FnMut(&mut B, &I::Item) -> bool,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        for item in self.iter.by_ref() {
            if (self.f)(&mut self.acc, &item) {
                return Some(item);
            }
        }
        None
    }
}

#[cfg(test)]
mod filter_with_previous_tests {
    use super::*;

    #[test]
    fn test_filter_with_previous() {
        let a = vec![1, 2, 3, 4, 5];
        let filtered = a
            .into_iter()
            .filter_with_previous(0, |acc, x| {
                if *acc + *x > 5 {
                    false
                } else {
                    *acc += *x;
                    true
                }
            })
            .collect_vec();
        assert_eq!(filtered, vec![1, 2]);
    }
}

pub trait InspectExt: Iterator {
    /// Similar to [`Iterator::inspect`], but the closure can be mutable.
    fn inspect_mut<F>(self, f: F) -> InspectMut<Self, F>
    where
        Self: Sized,
        F: FnMut(&Self::Item),
    {
        InspectMut::new(self, f)
    }
}

impl<T: ?Sized> InspectExt for T where T: Iterator {}

pub struct InspectMut<I, F>
where
    I: Iterator,
{
    iter: I,
    f: F,
}

impl<I, F> InspectMut<I, F>
where
    I: Iterator,
{
    pub fn new(iter: I, f: F) -> Self {
        Self { iter, f }
    }
}

impl<I, F> Iterator for InspectMut<I, F>
where
    I: Iterator,
    F: FnMut(&I::Item),
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.iter.next()?;
        (self.f)(&item);
        Some(item)
    }
}

#[cfg(test)]
mod inspect_mut_tests {
    use super::*;

    #[test]
    fn test_inspect_mut() {
        let a = vec![1, 2, 3, 4, 5];
        let mut sum = 0;
        let inspected = a
            .into_iter()
            .inspect_mut(|x| sum += *x)
            .collect::<Vec<_>>();
        assert_eq!(inspected, vec![1, 2, 3, 4, 5]);
        assert_eq!(sum, 15);
    }
}

pub trait BatchedExt: Iterator {
    /// Batches the elements of the iterator into vectors of a fixed size.
    ///
    /// It differs from `itertools::Itertools::chunks` in that it collects the elements into a
    /// vector and eagerly yields it.
    fn batched(self, batch_size: usize) -> Batched<Self>
    where
        Self: Sized,
    {
        Batched::new(self, batch_size)
    }
}

impl<T: ?Sized> BatchedExt for T where T: Iterator {}

pub struct Batched<I>
where
    I: Iterator,
{
    iter: I,
    batch_size: usize,
}

impl<I> Batched<I>
where
    I: Iterator,
{
    pub fn new(iter: I, batch_size: usize) -> Self {
        assert!(batch_size > 0, "batch_size must be greater than 0");
        Self { iter, batch_size }
    }
}

impl<I> Iterator for Batched<I>
where
    I: Iterator,
{
    type Item = Vec<I::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut batch = Vec::with_capacity(self.batch_size);
        for _ in 0..self.batch_size {
            match self.iter.next() {
                Some(item) => batch.push(item),
                None => break,
            }
        }
        if batch.is_empty() {
            None
        } else {
            Some(batch)
        }
    }
}

#[cfg(test)]
mod batched_tests {
    use super::*;

    #[test]
    fn test_batched() {
        let a = vec![1, 2, 3, 4, 5];
        let batched = a.into_iter().batched(2).collect_vec();
        assert_eq!(batched, vec![vec![1, 2], vec![3, 4], vec![5]]);

        let a: Vec<i32> = vec![];
        let batched = a.into_iter().batched(2).collect_vec();
        assert_eq!(batched, Vec::<Vec<i32>>::new());

        let a = vec![1];
        let batched = a.into_iter().batched(2).collect_vec();
        assert_eq!(batched, vec![vec![1]]);
    }
}

pub trait FilterWithPreviousExt2: Iterator {
    /// Return an iterator adapter that allows filtering elements based on the previous element.
    fn filter_with_previous2<F>(self, f: F) -> FilterWithPrevious2<Self, F>
    where
        Self: Sized,
        F: FnMut(&Option<Self::Item>, &Self::Item) -> bool,
    {
        FilterWithPrevious2::new(self, f)
    }
}

impl<T: ?Sized> FilterWithPreviousExt2 for T where T: Iterator {}

pub struct FilterWithPrevious2<I, F>
where
    I: Iterator,
{
    iter: I,
    f: F,
    prev: Option<I::Item>,
}

impl<I, F> FilterWithPrevious2<I, F>
where
    I: Iterator,
{
    pub fn new(iter: I, f: F) -> Self {
        Self { iter, f, prev: None }
    }
}

impl<I, F> Iterator for FilterWithPrevious2<I, F>
where
    I: Iterator,
    F: FnMut(&Option<I::Item>, &I::Item) -> bool,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        for item in self.iter.by_ref() {
            if (self.f)(&self.prev, &item) {
                self.prev = Some(item);
                return self.prev.clone();
            } else {
                self.prev = Some(item);
            }
        }
        None
    }
}

#[cfg(test)]
mod filter_with_previous2_tests {
    use super::*;

    #[test]
    fn test_filter_with_previous2() {
        let a = vec![1, 2, 3, 4, 5];
        let filtered = a
            .into_iter()
            .filter_with_previous2(|prev, x| match prev {
                Some(prev) => *x - *prev > 1,
                None => true,
            })
            .collect_vec();
        assert_eq!(filtered, vec![1]);

        let a = vec![1, 3, 5, 7, 9];
        let filtered = a
            .into_iter()
            .filter_with_previous2(|prev, x| match prev {
                Some(prev) => *x - *prev > 1,
                None => true,
            })
            .collect_vec();
        assert_eq!(filtered, vec![1, 3, 5, 7, 9]);

        let a = vec![1, 2, 4, 5, 7];
        let filtered = a
            .into_iter()
            .filter_with_previous2(|prev, x| match prev {
                Some(prev) => *x - *prev > 1,
                None => true,
            })
            .collect_vec();
        assert_eq!(filtered, vec![1, 4, 7]);
    }
}

pub trait FilterMapWithPreviousExt: Iterator {
    /// Return an iterator adapter that allows filtering and mapping elements based on the previous
    /// elements.
    fn filter_map_with_previous<F, B, R>(self, init: B, f: F) -> FilterMapWithPrevious<Self, F, B>
    where
        Self: Sized,
        F: FnMut(&mut B, Self::Item) -> Option<R>,
    {
        FilterMapWithPrevious::new(self, init, f)
    }
}

impl<T: ?Sized> FilterMapWithPreviousExt for T where T: Iterator {}

pub struct FilterMapWithPrevious<I, F, B>
where
    I: Iterator,
{
    iter: I,
    f: F,
    acc: B,
}

impl<I, F, B> FilterMapWithPrevious<I, F, B>
where
    I: Iterator,
{
    pub fn new(iter: I, init: B, f: F) -> Self {
        Self { iter, f, acc: init }
    }
}

impl<I, F, B, R> Iterator for FilterMapWithPrevious<I, F, B>
where
    I: Iterator,
    F: FnMut(&mut B, I::Item) -> Option<R>,
{
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        for item in self.iter.by_ref() {
            if let Some(res) = (self.f)(&mut self.acc, item) {
                return Some(res);
            }
        }
        None
    }
}

#[cfg(test)]
mod filter_map_with_previous_tests {
    use super::*;

    #[test]
    fn test_filter_map_with_previous() {
        let a = vec![1, 2, 3, 4, 5];
        let filtered = a
            .into_iter()
            .filter_map_with_previous(0, |acc, x| {
                if *acc + x > 5 {
                    None
                } else {
                    *acc += x;
                    Some(*acc)
                }
            })
            .collect_vec();
        assert_eq!(filtered, vec![1, 3]);
    }
}

pub trait FilterOutExt: Iterator {
    /// Return an iterator adapter that filters out elements based on a predicate.
    ///
    /// It is the opposite of [`Iterator::filter`].
    fn filter_out<F>(self, predicate: F) -> FilterOut<Self, F>
    where
        Self: Sized,
        F: FnMut(&Self::Item) -> bool,
    {
        FilterOut::new(self, predicate)
    }
}

impl<T: ?Sized> FilterOutExt for T where T: Iterator {}

pub struct FilterOut<I, F>
where
    I: Iterator,
{
    iter: I,
    predicate: F,
}

impl<I, F> FilterOut<I, F>
where
    I: Iterator,
{
    pub fn new(iter: I, predicate: F) -> Self {
        Self { iter, predicate }
    }
}

impl<I, F> Iterator for FilterOut<I, F>
where
    I: Iterator,
    F: FnMut(&I::Item) -> bool,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        for item in self.iter.by_ref() {
            if !(self.predicate)(&item) {
                return Some(item);
            }
        }
        None
    }
}

#[cfg(test)]
mod filter_out_tests {
    use super::*;

    #[test]
    fn test_filter_out() {
        let a = vec![1, 2, 3, 4, 5];
        let filtered = a.into_iter().filter_out(|x| x % 2 == 0).collect_vec();
        assert_eq!(filtered, vec![1, 3, 5]);
    }
}

pub trait GroupConsecutiveChunkByExt: Iterator {
    /// Return an iterator adapter that groups consecutive elements of the iterator into chunks
    /// based on a predicate.
    ///
    /// It is a customized version of `itertools::Itertools::chunk_by` that directly takes two
    /// consecutive elements and returns whether they should be grouped together.
    fn group_consecutive_chunk_by<F>(self, predicate: F) -> GroupConsecutiveChunkBy<Self, F>
    where
        Self: Sized,
        F: FnMut(&Self::Item, &Self::Item) -> bool,
    {
        GroupConsecutiveChunkBy::new(self, predicate)
    }
}

impl<T: ?Sized> GroupConsecutiveChunkByExt for T where T: Iterator {}

pub struct GroupConsecutiveChunkBy<I, F>
where
    I: Iterator,
{
    iter: I,
    predicate: F,
    cur: Option<I::Item>,
}

impl<I, F> GroupConsecutiveChunkBy<I, F>
where
    I: Iterator,
{
    pub fn new(mut iter: I, predicate: F) -> Self {
        let cur = iter.next();
        Self {
            iter,
            predicate,
            cur,
        }
    }
}

impl<I, F> Iterator for GroupConsecutiveChunkBy<I, F>
where
    I: Iterator,
    F: FnMut(&I::Item, &I::Item) -> bool,
{
    type Item = Vec<I::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        let cur = self.cur.take()?;
        let mut chunk = vec![cur];
        for next in self.iter.by_ref() {
            if (self.predicate)(chunk.last().unwrap(), &next) {
                chunk.push(next);
            } else {
                self.cur = Some(next);
                break;
            }
        }
        Some(chunk)
    }
}

#[cfg(test)]
mod group_consecutive_chunk_by_tests {
    use super::*;

    #[test]
    fn test_group_consecutive_chunk_by() {
        let a = vec![1, 1, 2, 3, 3, 3, 4, 5, 5];
        let chunks = a
            .into_iter()
            .group_consecutive_chunk_by(|x, y| x == y)
            .collect_vec();
        assert_eq!(
            chunks,
            vec![
                vec![1, 1],
                vec![2],
                vec![3, 3, 3],
                vec![4],
                vec![5, 5],
            ]
        );

        let a: Vec<i32> = vec![];
        let chunks = a
            .into_iter()
            .group_consecutive_chunk_by(|x, y| x == y)
            .collect_vec();
        assert_eq!(chunks, Vec::<Vec<i32>>::new());

        let a = vec![1];
        let chunks = a
            .into_iter()
            .group_consecutive_chunk_by(|x, y| x == y)
            .collect_vec();
        assert_eq!(chunks, vec![vec![1]]);
    }
}

pub trait FilterMapOkExt: Iterator {
    /// Return an iterator adapter that filters and maps elements of the iterator based on a closure
    /// that returns a `Result<Option<U>, E>`.
    ///
    /// It is a combination of [`Iterator::filter_map`] and [`itertools::Itertools::map_ok`].
    fn filter_map_ok<F, T, E, U>(self, f: F) -> FilterMapOk<Self, F>
    where
        Self: Sized + Iterator<Item = Result<T, E>>,
        F: FnMut(T) -> Result<Option<U>, E>,
    {
        FilterMapOk::new(self, f)
    }
}

impl<T: ?Sized> FilterMapOkExt for T where T: Iterator {}

pub struct FilterMapOk<I, F>
where
    I: Iterator,
{
    iter: I,
    f: F,
}

impl<I, F> FilterMapOk<I, F>
where
    I: Iterator,
{
    pub fn new(iter: I, f: F) -> Self {
        Self { iter, f }
    }
}

impl<I, F, T, E, U> Iterator for FilterMapOk<I, F>
where
    I: Iterator<Item = Result<T, E>>,
    F: FnMut(T) -> Result<Option<U>, E>,
{
    type Item = Result<U, E>;

    fn next(&mut self) -> Option<Self::Item> {
        for item in self.iter.by_ref() {
            match item {
                Ok(item) => match (self.f)(item) {
                    Ok(Some(res)) => return Some(Ok(res)),
                    Ok(None) => continue,
                    Err(err) => return Some(Err(err)),
                },
                Err(err) => return Some(Err(err)),
            }
        }
        None
    }
}

#[cfg(test)]
mod filter_map_ok_tests {
    use super::*;

    #[test]
    fn test_filter_map_ok() {
        let a = vec![Ok(1), Ok(2), Ok(3), Ok(4), Ok(5)];
        let filtered = a
            .into_iter()
            .filter_map_ok(|x| {
                if x % 2 == 0 {
                    Ok(Some(x * 2))
                } else {
                    Ok(None)
                }
            })
            .collect::<Result<Vec<_>, ()>>();
        assert_eq!(filtered, Ok(vec![4, 8]));

        let a = vec![Ok(1), Ok(2), Err(()), Ok(4), Ok(5)];
        let filtered = a
            .into_iter()
            .filter_map_ok(|x| {
                if x % 2 == 0 {
                    Ok(Some(x * 2))
                } else {
                    Ok(None)
                }
            })
            .collect::<Result<Vec<_>, ()>>();
        assert_eq!(filtered, Err(()));
    }
}

pub trait MapWithPreviousExt: Iterator {
    /// Return an iterator adapter that maps elements based on the previous element.
    fn map_with_previous<F, R>(self, f: F) -> MapWithPrevious<Self, F>
    where
        Self: Sized,
        F: FnMut(&Option<Self::Item>, &Self::Item) -> R,
    {
        MapWithPrevious::new(self, f)
    }
}

impl<T: ?Sized> MapWithPreviousExt for T where T: Iterator {}

pub struct MapWithPrevious<I, F>
where
    I: Iterator,
{
    iter: I,
    f: F,
    prev: Option<I::Item>,
}

impl<I, F> MapWithPrevious<I, F>
where
    I: Iterator,
{
    pub fn new(iter: I, f: F) -> Self {
        Self { iter, f, prev: None }
    }
}

impl<I, F, R> Iterator for MapWithPrevious<I, F>
where
    I: Iterator,
    F: FnMut(&Option<I::Item>, &I::Item) -> R,
{
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.iter.next()?;
        let res = (self.f)(&self.prev, &item);
        self.prev = Some(item);
        Some(res)
    }
}

#[cfg(test)]
mod map_with_previous_tests {
    use super::*;

    #[test]
    fn test_map_with_previous() {
        let a = vec![1, 2, 3, 4, 5];
        let mapped = a
            .into_iter()
            .map_with_previous(|prev, x| match prev {
                Some(prev) => *x + *prev,
                None => *x,
            })
            .collect_vec();
        assert_eq!(mapped, vec![1, 3, 5, 7, 9]);
    }
}

pub trait CoalesceByExt: Iterator {
    /// Return an iterator adapter that coalesces elements based on a predicate.
    ///
    /// It differs from `itertools::Itertools::coalesce` in that it does not return `(first,
    /// second)` in the error variant, so it avoids lots of copies.
    fn coalesce_by<F>(self, f: F) -> CoalesceBy<Self, F>
    where
        Self: Sized,
        F: FnMut(&mut Self::Item, Self::Item) -> Result<(), ()>,
    {
        CoalesceBy::new(self, f)
    }
}

impl<T: ?Sized> CoalesceByExt for T where T: Iterator {}

pub struct CoalesceBy<I, F>
where
    I: Iterator,
{
    iter: I,
    f: F,
    cur: Option<I::Item>,
}

impl<I, F> CoalesceBy<I, F>
where
    I: Iterator,
{
    pub fn new(mut iter: I, f: F) -> Self {
        let cur = iter.next();
        Self { iter, f, cur }
    }
}

impl<I, F> Iterator for CoalesceBy<I, F>
where
    I: Iterator,
    F: FnMut(&mut I::Item, I::Item) -> Result<(), ()>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let mut cur = self.cur.take()?;
        for next in self.iter.by_ref() {
            if (self.f)(&mut cur, next).is_err() {
                self.cur = Some(cur);
                return self.cur.take();
            }
        }
        Some(cur)
    }
}

#[cfg(test)]
mod coalesce_by_tests {
    use super::*;

    #[test]
    fn test_coalesce_by() {
        let a = vec![1, 2, 3, 4, 5];
        let coalesced = a
            .into_iter()
            .coalesce_by(|acc, x| {
                if *acc + x <= 5 {
                    *acc += x;
                    Ok(())
                } else {
                    Err(())
                }
            })
            .collect_vec();
        assert_eq!(coalesced, vec![1, 2, 3, 4, 5]);

        let a = vec![1, 1, 1, 1, 1];
        let coalesced = a
            .into_iter()
            .coalesce_by(|acc, x| {
                if *acc + x <= 5 {
                    *acc += x;
                    Ok(())
                } else {
                    Err(())
                }
            })
            .collect_vec();
        assert_eq!(coalesced, vec![5]);

        let a = vec![1, 1, 1, 1, 1, 1];
        let coalesced = a
            .into_iter()
            .coalesce_by(|acc, x| {
                if *acc + x <= 5 {
                    *acc += x;
                    Ok(())
                } else {
                    Err(())
                }
            })
            .collect_vec();
        assert_eq!(coalesced, vec![5, 1]);
    }
}

pub trait InspectIfExt: Iterator {
    /// Return an iterator adapter that inspects elements based on a predicate.
    fn inspect_if<F, P>(self, predicate: P, f: F) -> InspectIf<Self, F, P>
    where
        Self: Sized,
        F: FnMut(&Self::Item),
        P: FnMut(&Self::Item) -> bool,
    {
        InspectIf::new(self, predicate, f)
    }
}

impl<T: ?Sized> InspectIfExt for T where T: Iterator {}

pub struct InspectIf<I, F, P>
where
    I: Iterator,
{
    iter: I,
    f: F,
    predicate: P,
}

impl<I, F, P> InspectIf<I, F, P>
where
    I: Iterator,
{
    pub fn new(iter: I, predicate: P, f: F) -> Self {
        Self { iter, f, predicate }
    }
}

impl<I, F, P> Iterator for InspectIf<I, F, P>
where
    I: Iterator,
    F: FnMut(&I::Item),
    P: FnMut(&I::Item) -> bool,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.iter.next()?;
        if (self.predicate)(&item) {
            (self.f)(&item);
        }
        Some(item)
    }
}

#[cfg(test)]
mod inspect_if_tests {
    use super::*;

    #[test]
    fn test_inspect_if() {
        let a = vec![1, 2, 3, 4, 5];
        let mut sum = 0;
        let inspected = a
            .into_iter()
            .inspect_if(|x| x % 2 == 0, |x| sum += *x)
            .collect::<Vec<_>>();
        assert_eq!(inspected, vec![1, 2, 3, 4, 5]);
        assert_eq!(sum, 6);
    }
}

pub trait FindMapWithPreviousExt: Iterator {
    /// Return an iterator adapter that finds and maps elements based on the previous element.
    fn find_map_with_previous<F, R>(mut self, mut f: F) -> Option<R>
    where
        Self: Sized,
        F: FnMut(&Option<Self::Item>, &Self::Item) -> Option<R>,
    {
        let mut prev = None;
        for item in self.by_ref() {
            if let Some(res) = f(&prev, &item) {
                return Some(res);
            }
            prev = Some(item);
        }
        None
    }
}

impl<T: ?Sized> FindMapWithPreviousExt for T where T: Iterator {}

#[cfg(test)]
mod find_map_with_previous_tests {
    use super::*;

    #[test]
    fn test_find_map_with_previous() {
        let a = vec![1, 2, 3, 4, 5];
        let found = a.into_iter().find_map_with_previous(|prev, x| match prev {
            Some(prev) => {
                if *x - *prev > 1 {
                    Some(*x)
                } else {
                    None
                }
            }
            None => None,
        });
        assert_eq!(found, None);

        let a = vec![1, 3, 5, 7, 9];
        let found = a.into_iter().find_map_with_previous(|prev, x| match prev {
            Some(prev) => {
                if *x - *prev > 1 {
                    Some(*x)
                } else {
                    None
                }
            }
            None => None,
        });
        assert_eq!(found, Some(3));
    }
}

pub trait InspectOkExt: Iterator {
    /// Return an iterator adapter that inspects `Ok` elements.
    fn inspect_ok<F, T, E>(self, f: F) -> InspectOk<Self, F>
    where
        Self: Sized + Iterator<Item = Result<T, E>>,
        F: FnMut(&T),
    {
        InspectOk::new(self, f)
    }
}

impl<T: ?Sized> InspectOkExt for T where T: Iterator {}

pub struct InspectOk<I, F>
where
    I: Iterator,
{
    iter: I,
    f: F,
}

impl<I, F> InspectOk<I, F>
where
    I: Iterator,
{
    pub fn new(iter: I, f: F) -> Self {
        Self { iter, f }
    }
}

impl<I, F, T, E> Iterator for InspectOk<I, F>
where
    I: Iterator<Item = Result<T, E>>,
    F: FnMut(&T),
{
    type Item = Result<T, E>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.iter.next()?;
        if let Ok(item) = &item {
            (self.f)(item);
        }
        Some(item)
    }
}

#[cfg(test)]
mod inspect_ok_tests {
    use super::*;

    #[test]
    fn test_inspect_ok() {
        let a: Vec<Result<i32, ()>> = vec![Ok(1), Ok(2), Ok(3), Ok(4), Ok(5)];
        let mut sum = 0;
        let inspected = a
            .into_iter()
            .inspect_ok(|x| sum += *x)
            .collect::<Result<Vec<_>, ()>>();
        assert_eq!(inspected, Ok(vec![1, 2, 3, 4, 5]));
        assert_eq!(sum, 15);

        let a = vec![Ok(1), Ok(2), Err(()), Ok(4), Ok(5)];
        let mut sum = 0;
        let inspected = a
            .into_iter()
            .inspect_ok(|x| sum += *x)
            .collect::<Result<Vec<_>, ()>>();
        assert_eq!(inspected, Err(()));
        assert_eq!(sum, 3);
    }
}

pub trait FlatMapOkExt: Iterator {
    /// Return an iterator adapter that flat-maps `Ok` elements based on a closure that returns an
    /// iterator.
    fn flat_map_ok<F, T, E, U>(self, f: F) -> FlatMapOk<Self, F, U>
    where
        Self: Sized + Iterator<Item = Result<T, E>>,
        F: FnMut(T) -> U,
        U: IntoIterator,
    {
        FlatMapOk::new(self, f)
    }
}

impl<T: ?Sized> FlatMapOkExt for T where T: Iterator {}

pub struct FlatMapOk<I, F, U>
where
    I: Iterator,
    U: IntoIterator,
{
    iter: I,
    f: F,
    cur: Option<U::IntoIter>,
}

impl<I, F, U> FlatMapOk<I, F, U>
where
    I: Iterator,
    U: IntoIterator,
{
    pub fn new(iter: I, f: F) -> Self {
        Self { iter, f, cur: None }
    }
}

impl<I, F, T, E, U> Iterator for FlatMapOk<I, F, U>
where
    I: Iterator<Item = Result<T, E>>,
    F: FnMut(T) -> U,
    U: IntoIterator,
{
    type Item = Result<U::Item, E>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(cur) = &mut self.cur {
                if let Some(item) = cur.next() {
                    return Some(Ok(item));
                } else {
                    self.cur = None;
                }
            }

            match self.iter.next()? {
                Ok(item) => {
                    self.cur = Some((self.f)(item).into_iter());
                }
                Err(err) => return Some(Err(err)),
            }
        }
    }
}

#[cfg(test)]
mod flat_map_ok_tests {
    use super::*;

    #[test]
    fn test_flat_map_ok() {
        let a = vec![Ok(1), Ok(2), Ok(3)];
        let flat_mapped = a
            .into_iter()
            .flat_map_ok(|x| vec![x, x * 2])
            .collect::<Result<Vec<_>, ()>>();
        assert_eq!(flat_mapped, Ok(vec![1, 2, 2, 4, 3, 6]));

        let a = vec![Ok(1), Ok(2), Err(()), Ok(4)];
        let flat_mapped = a
            .into_iter()
            .flat_map_ok(|x| vec![x, x * 2])
            .collect::<Result<Vec<_>, ()>>();
        assert_eq!(flat_mapped, Err(()));
    }
}

pub trait FlattenOkExt: Iterator {
    /// Return an iterator adapter that flattens `Ok` elements that are iterators.
    fn flatten_ok<T, E>(self) -> FlattenOk<Self, T>
    where
        Self: Sized + Iterator<Item = Result<T, E>>,
        T: IntoIterator,
    {
        FlattenOk::new(self)
    }
}

impl<T: ?Sized> FlattenOkExt for T where T: Iterator {}

pub struct FlattenOk<I, T>
where
    I: Iterator,
    T: IntoIterator,
{
    iter: I,
    cur: Option<T::IntoIter>,
}

impl<I, T> FlattenOk<I, T>
where
    I: Iterator,
    T: IntoIterator,
{
    pub fn new(iter: I) -> Self {
        Self { iter, cur: None }
    }
}

impl<I, T, E> Iterator for FlattenOk<I, T>
where
    I: Iterator<Item = Result<T, E>>,
    T: IntoIterator,
{
    type Item = Result<T::Item, E>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(cur) = &mut self.cur {
                if let Some(item) = cur.next() {
                    return Some(Ok(item));
                } else {
                    self.cur = None;
                }
            }

            match self.iter.next()? {
                Ok(item) => {
                    self.cur = Some(item.into_iter());
                }
                Err(err) => return Some(Err(err)),
            }
        }
    }
}

#[cfg(test)]
mod flatten_ok_tests {
    use super::*;

    #[test]
    fn test_flatten_ok() {
        let a = vec![Ok(vec![1, 2]), Ok(vec![3, 4]), Ok(vec![5, 6])];
        let flattened = a
            .into_iter()
            .flatten_ok()
            .collect::<Result<Vec<_>, ()>>()
            .unwrap();
        assert_eq!(flattened, vec![1, 2, 3, 4, 5, 6]);

        let a = vec![Ok(vec![1, 2]), Ok(vec![3, 4]), Err(()), Ok(vec![5, 6])];
        let flattened = a.into_iter().flatten_ok().collect::<Result<Vec<_>, ()>>();
        assert_eq!(flattened, Err(()));
    }
}

pub trait FilterMapOkExt2: Iterator {
    /// Return an iterator adapter that filters and maps elements of the iterator based on a closure
    /// that returns an `Option<U>`.
    fn filter_map_ok2<F, T, E, U>(self, f: F) -> FilterMapOk2<Self, F>
    where
        Self: Sized + Iterator<Item = Result<T, E>>,
        F: FnMut(T) -> Option<U>,
    {
        FilterMapOk2::new(self, f)
    }
}

impl<T: ?Sized> FilterMapOkExt2 for T where T: Iterator {}

pub struct FilterMapOk2<I, F>
where
    I: Iterator,
{
    iter: I,
    f: F,
}

impl<I, F> FilterMapOk2<I, F>
where
    I: Iterator,
{
    pub fn new(iter: I, f: F) -> Self {
        Self { iter, f }
    }
}

impl<I, F, T, E, U> Iterator for FilterMapOk2<I, F>
where
    I: Iterator<Item = Result<T, E>>,
    F: FnMut(T) -> Option<U>,
{
    type Item = Result<U, E>;

    fn next(&mut self) -> Option<Self::Item> {
        for item in self.iter.by_ref() {
            match item {
                Ok(item) => match (self.f)(item) {
                    Some(res) => return Some(Ok(res)),
                    None => continue,
                },
                Err(err) => return Some(Err(err)),
            }
        }
        None
    }
}

#[cfg(test)]
mod filter_map_ok2_tests {
    use super::*;

    #[test]
    fn test_filter_map_ok2() {
        let a = vec![Ok(1), Ok(2), Ok(3), Ok(4), Ok(5)];
        let filtered = a
            .into_iter()
            .filter_map_ok2(|x| if x % 2 == 0 { Some(x * 2) } else { None })
            .collect::<Result<Vec<_>, ()>>();
        assert_eq!(filtered, Ok(vec![4, 8]));

        let a = vec![Ok(1), Ok(2), Err(()), Ok(4), Ok(5)];
        let filtered = a
            .into_iter()
            .filter_map_ok2(|x| if x % 2 == 0 { Some(x * 2) } else { None })
            .collect::<Result<Vec<_>, ()>>();
        assert_eq!(filtered, Err(()));
    }
}

pub trait FilterOkExt: Iterator {
    /// Return an iterator adapter that filters `Ok` elements based on a predicate.
    fn filter_ok<F, T, E>(self, predicate: F) -> FilterOk<Self, F>
    where
        Self: Sized + Iterator<Item = Result<T, E>>,
        F: FnMut(&T) -> bool,
    {
        FilterOk::new(self, predicate)
    }
}

impl<T: ?Sized> FilterOkExt for T where T: Iterator {}

pub struct FilterOk<I, F>
where
    I: Iterator,
{
    iter: I,
    predicate: F,
}

impl<I, F> FilterOk<I, F>
where
    I: Iterator,
{
    pub fn new(iter: I, predicate: F) -> Self {
        Self { iter, predicate }
    }
}

impl<I, F, T, E> Iterator for FilterOk<I, F>
where
    I: Iterator<Item = Result<T, E>>,
    F: FnMut(&T) -> bool,
{
    type Item = Result<T, E>;

    fn next(&mut self) -> Option<Self::Item> {
        for item in self.iter.by_ref() {
            match item {
                Ok(item) => {
                    if (self.predicate)(&item) {
                        return Some(Ok(item));
                    }
                }
                Err(err) => return Some(Err(err)),
            }
        }
        None
    }
}

#[cfg(test)]
mod filter_ok_tests {
    use super::*;

    #[test]
    fn test_filter_ok() {
        let a = vec![Ok(1), Ok(2), Ok(3), Ok(4), Ok(5)];
        let filtered = a
            .into_iter()
            .filter_ok(|x| x % 2 == 0)
            .collect::<Result<Vec<_>, ()>>();
        assert_eq!(filtered, Ok(vec![2, 4]));

        let a = vec![Ok(1), Ok(2), Err(()), Ok(4), Ok(5)];
        let filtered = a
            .into_iter()
            .filter_ok(|x| x % 2 == 0)
            .collect::<Result<Vec<_>, ()>>();
        assert_eq!(filtered, Err(()));
    }
}

pub trait FilterOutOkExt: Iterator {
    /// Return an iterator adapter that filters out `Ok` elements based on a predicate.
    ///
    /// It is the opposite of [`FilterOkExt::filter_ok`].
    fn filter_out_ok<F, T, E>(self, predicate: F) -> FilterOutOk<Self, F>
    where
        Self: Sized + Iterator<Item = Result<T, E>>,
        F: FnMut(&T) -> bool,
    {
        FilterOutOk::new(self, predicate)
    }
}

impl<T: ?Sized> FilterOutOkExt for T where T: Iterator {}

pub struct FilterOutOk<I, F>
where
    I: Iterator,
{
    iter: I,
    predicate: F,
}

impl<I, F> FilterOutOk<I, F>
where
    I: Iterator,
{
    pub fn new(iter: I, predicate: F) -> Self {
        Self { iter, predicate }
    }
}

impl<I, F, T, E> Iterator for FilterOutOk<I, F>
where
    I: Iterator<Item = Result<T, E>>,
    F: FnMut(&T) -> bool,
{
    type Item = Result<T, E>;

    fn next(&mut self) -> Option<Self::Item> {
        for item in self.iter.by_ref() {
            match item {
                Ok(item) => {
                    if !(self.predicate)(&item) {
                        return Some(Ok(item));
                    }
                }
                Err(err) => return Some(Err(err)),
            }
        }
        None
    }
}

#[cfg(test)]
mod filter_out_ok_tests {
    use super::*;

    #[test]
    fn test_filter_out_ok() {
        let a = vec![Ok(1), Ok(2), Ok(3), Ok(4), Ok(5)];
        let filtered = a
            .into_iter()
            .filter_out_ok(|x| x % 2 == 0)
            .collect::<Result<Vec<_>, ()>>();
        assert_eq!(filtered, Ok(vec![1, 3, 5]));

        let a = vec![Ok(1), Ok(2), Err(()), Ok(4), Ok(5)];
        let filtered = a
            .into_iter()
            .filter_out_ok(|x| x % 2 == 0)
            .collect::<Result<Vec<_>, ()>>();
        assert_eq!(filtered, Err(()));
    }
}

pub trait PartitionResultExt: Iterator {
    /// Partition the elements of the iterator into two vectors: one containing `Ok` elements and
    /// the other containing `Err` elements.
    fn partition_result<T, E>(self) -> (Vec<T>, Vec<E>)
    where
        Self: Sized + Iterator<Item = Result<T, E>>,
    {
        let mut ok = Vec::new();
        let mut err = Vec::new();
        for item in self {
            match item {
                Ok(item) => ok.push(item),
                Err(e) => err.push(e),
            }
        }
        (ok, err)
    }
}

impl<T: ?Sized> PartitionResultExt for T where T: Iterator {}

#[cfg(test)]
mod partition_result_tests {
    use super::*;

    #[test]
    fn test_partition_result() {
        let a = vec![Ok(1), Ok(2), Err(3), Ok(4), Err(5)];
        let (ok, err) = a.into_iter().partition_result();
        assert_eq!(ok, vec![1, 2, 4]);
        assert_eq!(err, vec![3, 5]);
    }
}

pub trait FindMapOkExt: Iterator {
    /// Return the first `Ok` element that matches a predicate.
    fn find_map_ok<F, T, E, U>(mut self, mut f: F) -> Result<Option<U>, E>
    where
        Self: Sized + Iterator<Item = Result<T, E>>,
        F: FnMut(T) -> Result<Option<U>, E>,
    {
        for item in self.by_ref() {
            match item {
                Ok(item) => match f(item)? {
                    Some(res) => return Ok(Some(res)),
                    None => continue,
                },
                Err(err) => return Err(err),
            }
        }
        Ok(None)
    }
}

impl<T: ?Sized> FindMapOkExt for T where T: Iterator {}

#[cfg(test)]
mod find_map_ok_tests {
    use super::*;

    #[test]
    fn test_find_map_ok() {
        let a = vec![Ok(1), Ok(2), Ok(3), Ok(4), Ok(5)];
        let found = a.into_iter().find_map_ok(|x| {
            if x % 2 == 0 {
                Ok(Some(x * 2))
            } else {
                Ok(None)
            }
        });
        assert_eq!(found, Ok(Some(4)));

        let a = vec![Ok(1), Ok(2), Err(()), Ok(4), Ok(5)];
        let found = a.into_iter().find_map_ok(|x| {
            if x % 2 == 0 {
                Ok(Some(x * 2))
            } else {
                Ok(None)
            }
        });
        assert_eq!(found, Ok(Some(4)));

        let a = vec![Ok(1), Err(()), Ok(2), Ok(4), Ok(5)];
        let found = a.into_iter().find_map_ok(|x| {
            if x % 2 == 0 {
                Ok(Some(x * 2))
            } else {
                Ok(None)
            }
        });
        assert_eq!(found, Err(()));
    }
}

pub trait TryCollectExt: Iterator {
    /// Collect the elements of the iterator into a collection, returning an error if any element is
    /// an error.
    ///
    /// It is a combination of [`Iterator::collect`] and [`itertools::Itertools::try_collect`].
    fn try_collect<T, E, C>(self) -> Result<C, E>
    where
        Self: Sized + Iterator<Item = Result<T, E>>,
        C: FromIterator<T>,
    {
        self.collect()
    }
}

impl<T: ?Sized> TryCollectExt for T where T: Iterator {}

#[cfg(test)]
mod try_collect_tests {
    use super::*;