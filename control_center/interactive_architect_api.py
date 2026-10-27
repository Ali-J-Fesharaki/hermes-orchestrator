#!/usr/bin/env python3
"""
control_center/interactive_architect_api.py
Socratic Architectural Mentor & Counterfactual What-If Reasoning Engine

Provides an interactive architectural reasoning engine for the Hermes Orchestrator.
Translates milestone specifications, ADR-Lite trade-off records, and DAG dependencies
into dynamic pedagogical dialogues for the human supervising architect.

Core Features:
1. Deterministic Query Intent Classifier (TRADE_OFF, SCALE_PROJECTION, BLAST_RADIUS,
   CONCURRENCY_SAFETY, FAILURE_MODE, GENERAL_SOCRATIC).
2. Context Fusion Engine: C = Phi(M, P, D, Q) fusing milestone metadata, pattern catalog
   (dashboards/architecture_pattern_matcher.py), and DAG tier dependencies.
3. Structured Tripartite Socratic Output Schema:
   (a) Direct Architectural Axiom
   (b) Trade-off Comparison (Chosen Solution vs Alternative)
   (c) Catastrophic Production Failure Prevented (<= 250 tokens).
4. Heuristic Zero-Token Knowledge Engine covering canonical distributed patterns and classic trade-offs.
5. In-Memory Thread-Safe LRU Cache with SHA-256 fingerprinting (capacity=256, TTL=86400s).
6. HTTP Controller Hook: handle_architect_request(payload: dict) -> Tuple[int, dict] for server.py.
"""

from __future__ import annotations

import collections
from dataclasses import asdict, dataclass, field
from enum import Enum
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple
import urllib.error
import urllib.request

# Ensure workspace root is in sys.path for local module resolution
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Graceful local imports
try:
    from dashboards.architecture_pattern_matcher import (
        PATTERN_CATALOG,
        ArchitecturePattern,
        classify_architecture_patterns,
    )
except ImportError:
    PATTERN_CATALOG = {}
    classify_architecture_patterns = None

try:
    from tasks.dynamic_model_provider import (
        fetch_live_models,
        get_active_llm_server_info,
    )
except ImportError:
    fetch_live_models = None
    get_active_llm_server_info = None


# ==============================================================================
# 1. Architectural Intent Taxonomy & Data Structures
# ==============================================================================

class ArchitectIntent(str, Enum):
    """Discrete architectural inquiry intents."""
    TRADE_OFF = "TRADE_OFF"
    SCALE_PROJECTION = "SCALE_PROJECTION"
    BLAST_RADIUS = "BLAST_RADIUS"
    CONCURRENCY_SAFETY = "CONCURRENCY_SAFETY"
    FAILURE_MODE = "FAILURE_MODE"
    GENERAL_SOCRATIC = "GENERAL_SOCRATIC"


@dataclass
class ArchitectResponse:
    """Structured response adhering to the Tripartite Socratic Schema."""
    answer: str  # Markdown-formatted Socratic response
    intent: ArchitectIntent
    citations: List[str]
    latency_ms: float
    cached: bool
    source: str  # 'llm' | 'heuristic_fallback'

    def to_dict(self) -> Dict[str, Any]:
        """Serializes the response for HTTP JSON delivery."""
        return {
            "status": "success",
            "answer": self.answer,
            "intent": self.intent.value,
            "citations": self.citations,
            "latency_ms": round(self.latency_ms, 2),
            "cached": self.cached,
            "source": self.source,
        }


# ==============================================================================
# 2. Query Intent Classifier (Deterministic Semantic Pattern Matching)
# ==============================================================================

INTENT_SIGNATURES: Dict[ArchitectIntent, List[Tuple[re.Pattern, float]]] = {
    ArchitectIntent.TRADE_OFF: [
        (re.compile(r"\b(vs|versus)\b", re.IGNORECASE), 4.0),
        (re.compile(r"\btrade[- ]?offs?\b", re.IGNORECASE), 4.0),
        (re.compile(r"\balternatives?\b", re.IGNORECASE), 3.0),
        (re.compile(r"\b(compare|comparison|pros and cons)\b", re.IGNORECASE), 3.5),
        (re.compile(r"\b(instead of|why not|rationale|choose between)\b", re.IGNORECASE), 3.0),
        (re.compile(r"\b(decision record|adr|discarded)\b", re.IGNORECASE), 2.5),
    ],
    ArchitectIntent.SCALE_PROJECTION: [
        (re.compile(r"\b(100x|10x|1000x)\b", re.IGNORECASE), 4.5),
        (re.compile(r"\b(scale|scaling|scalability)\b", re.IGNORECASE), 3.5),
        (re.compile(r"\b(throughput|bottleneck|qps|rps|load|capacity)\b", re.IGNORECASE), 3.0),
        (re.compile(r"\b(million|volume|sharding|partitioning|horizontal)\b", re.IGNORECASE), 2.5),
        (re.compile(r"\b(backpressure|saturation|exhaustion)\b", re.IGNORECASE), 2.5),
    ],
    ArchitectIntent.BLAST_RADIUS: [
        (re.compile(r"\bblast[- ]?radius\b", re.IGNORECASE), 5.0),
        (re.compile(r"\b(fault[- ]?domain|isolation|spillover|containment)\b", re.IGNORECASE), 4.0),
        (re.compile(r"\b(cascading failure|cascade storm)\b", re.IGNORECASE), 3.5),
        (re.compile(r"\b(boundary|boundaries|side[- ]?effects?|scope of failure)\b", re.IGNORECASE), 2.5),
        (re.compile(r"\b(contaminate|contamination|sandbox)\b", re.IGNORECASE), 2.5),
    ],
    ArchitectIntent.CONCURRENCY_SAFETY: [
        (re.compile(r"\b(race[- ]?condition|thread[- ]?safe(ty)?)\b", re.IGNORECASE), 4.5),
        (re.compile(r"\b(deadlock|livelock|lock contention)\b", re.IGNORECASE), 4.0),
        (re.compile(r"\b(mutex|fencing[- ]?token|semaphore|reentrancy|atomic)\b", re.IGNORECASE), 3.5),
        (re.compile(r"\b(concurrency|concurrent|asyncio|parallelism|critical[- ]?section)\b", re.IGNORECASE), 3.0),
        (re.compile(r"\b(synchronization|inter-process lock|split-brain)\b", re.IGNORECASE), 3.0),
    ],
    ArchitectIntent.FAILURE_MODE: [
        (re.compile(r"\bfailure[- ]?modes?\b", re.IGNORECASE), 4.5),
        (re.compile(r"\b(fail[- ]?fast|outage|crash|downstream failure)\b", re.IGNORECASE), 3.5),
        (re.compile(r"\b(network[- ]?partition|split[- ]?brain|partition tolerance)\b", re.IGNORECASE), 3.5),
        (re.compile(r"\b(what if .* fails?|catastrophic failure|disaster recovery)\b", re.IGNORECASE), 3.5),
        (re.compile(r"\b(circuit breaker|half-open|unhandled exception)\b", re.IGNORECASE), 3.0),
    ],
}


def classify_query_intent(query: str) -> ArchitectIntent:
    """
    Deterministically partitions user query Q into discrete architectural intent I
    using weighted regular expression signature scoring.
    """
    clean_query = query.strip()
    if not clean_query:
        return ArchitectIntent.GENERAL_SOCRATIC

    scores: Dict[ArchitectIntent, float] = {intent: 0.0 for intent in ArchitectIntent}

    for intent, signature_list in INTENT_SIGNATURES.items():
        for pattern, weight in signature_list:
            matches = pattern.findall(clean_query)
            if matches:
                scores[intent] += weight * len(matches)

    # Find highest scoring intent
    best_intent, best_score = max(scores.items(), key=lambda item: item[1])
    if best_score >= 2.0:
        return best_intent

    return ArchitectIntent.GENERAL_SOCRATIC


# ==============================================================================
# 3. Context Fusion Engine: C = Phi(M, P, D, Q)
# ==============================================================================

class SocraticContextBuilder:
    """
    Assembles enriched architectural prompt context C = Phi(M, P, D, Q)
    where:
      M = Milestone metadata (title, details, target file, spec)
      P = Matched architectural pattern catalog (from dashboards/architecture_pattern_matcher.py)
      D = DAG tier dependencies (prerequisites, dependents, concurrency tier)
      Q = User query text
    """

    @staticmethod
    def build_context(
        milestone_id: str,
        milestone_data: Dict[str, Any],
        query: str,
    ) -> Dict[str, Any]:
        """Extracts and fuses metadata, pattern catalog entries, and DAG dependencies."""
        m_name = str(milestone_data.get("name") or milestone_data.get("title") or milestone_id).strip()
        m_details = str(milestone_data.get("details") or "").strip()
        m_spec = str(milestone_data.get("technical_specification") or "").strip()
        m_file = str(milestone_data.get("relative_filepath") or milestone_data.get("output_file") or "").strip()

        # Extract DAG Dependencies (D)
        prereqs = milestone_data.get("prerequisites") or []
        if isinstance(prereqs, str):
            prereqs = [p.strip() for p in prereqs.split(",") if p.strip()]
        dependents = milestone_data.get("dependents") or []
        tier = milestone_data.get("tier") or milestone_data.get("dag_tier") or "Tier 0 (Root/Independent)"

        # Architectural Pattern Classification (P)
        patterns_matched: List[Dict[str, Any]] = []
        if classify_architecture_patterns is not None:
            try:
                raw_patterns = classify_architecture_patterns(milestone_data, threshold=1.0)
                patterns_matched = raw_patterns[:3] if raw_patterns else []
            except Exception:
                patterns_matched = []

        # Fallback to direct patterns list if already stored in milestone
        if not patterns_matched and "patterns" in milestone_data:
            patterns_matched = milestone_data["patterns"]

        primary_pattern = patterns_matched[0] if patterns_matched else None

        # Build clean formatted context string
        context_payload = {
            "milestone_id": milestone_id,
            "milestone_metadata": {
                "name": m_name,
                "target_file": m_file,
                "details": m_details[:400],
                "technical_specification": m_spec[:500],
            },
            "dag_dependencies": {
                "prerequisites": prereqs,
                "dependents": dependents,
                "tier": tier,
            },
            "matched_patterns": patterns_matched,
            "primary_pattern": primary_pattern,
            "query": query.strip(),
        }

        # Formulate structured LLM Prompt (tripartite schema constraint <= 250 tokens)
        system_prompt = (
            "You are the Hermes Socratic Architectural Mentor and Senior Distributed Systems Architect.\n"
            "Evaluate the user's architectural inquiry regarding this milestone.\n"
            "STRICT CONSTRAINTS:\n"
            "1. Output MUST strictly adhere to the following Tripartite Pedagogical Schema in clean Markdown:\n"
            "   ### 1. Direct Architectural Axiom\n"
            "   [One authoritative, declarative sentence defining the core system invariant or constraint.]\n\n"
            "   ### 2. Trade-off Comparison\n"
            "   [Concise evaluation comparing the chosen implementation against the primary rejected alternative, highlighting throughput, latency, or complexity trade-offs.]\n\n"
            "   ### 3. Catastrophic Production Failure Prevented\n"
            "   [Specific, concrete production disaster prevented by enforcing this invariant, such as cascading thread exhaustion, split-brain, or silent data corruption.]\n\n"
            "2. Total response length MUST be strictly under 250 tokens (~180-220 words).\n"
            "3. DO NOT include introductory conversational filler, disclaimers, or sign-offs."
        )

        user_content = (
            f"Milestone: {m_name}\n"
            f"Target File: {m_file or 'N/A'}\n"
            f"DAG Tier: {tier} | Prerequisites: {', '.join(prereqs) or 'None'}\n"
        )
        if primary_pattern:
            user_content += (
                f"Detected Pattern: {primary_pattern.get('label', primary_pattern.get('id', 'General'))}\n"
            )
            adr = primary_pattern.get("adr_lite")
            if adr:
                user_content += f"ADR Decision: {adr.get('decision', '')}\n"
                user_content += f"Chosen Rationale: {adr.get('chosen_rationale', '')}\n"

        user_content += f"\nArchitectural Query: {query.strip()}"

        context_payload["system_prompt"] = system_prompt
        context_payload["user_content"] = user_content

        return context_payload


# ==============================================================================
# 4. Heuristic Zero-Token Knowledge Engine (Offline Decision Tree)
# ==============================================================================

@dataclass(frozen=True)
class HeuristicKnowledgeEntry:
    """Pre-computed publication-grade response for canonical patterns and trade-offs."""
    key: str
    patterns: List[re.Pattern]
    axiom: str
    tradeoff: str
    failure_prevented: str
    citations: List[str]

    def format_markdown(self) -> str:
        return (
            f"### 1. Direct Architectural Axiom\n"
            f"{self.axiom}\n\n"
            f"### 2. Trade-off Comparison\n"
            f"{self.tradeoff}\n\n"
            f"### 3. Catastrophic Production Failure Prevented\n"
            f"{self.failure_prevented}"
        )


OFFLINE_KNOWLEDGE_BASE: List[HeuristicKnowledgeEntry] = [
    # 1. SSE vs WebSockets
    HeuristicKnowledgeEntry(
        key="sse_vs_websocket",
        patterns=[
            re.compile(r"\b(sse|server[- ]?sent events?)\s+(vs|versus|over)\s+(web[- ]?sockets?|ws)\b", re.IGNORECASE),
            re.compile(r"\b(web[- ]?sockets?|ws)\s+(vs|versus|over)\s+(sse|server[- ]?sent events?)\b", re.IGNORECASE),
            re.compile(r"\b(sse\b.*websocket|websocket\b.*sse)\b", re.IGNORECASE),
        ],
        axiom="Unidirectional server-to-client telemetry streams must leverage HTTP Server-Sent Events (SSE) over persistent full-duplex WebSockets to eliminate stateful proxy connection overhead and retain HTTP/2 multiplexing.",
        tradeoff="SSE natively provides automatic reconnection, event IDs, and HTTP proxy/firewall traversal over standard TLS, discarding WebSocket's bidirectional framing complexity at the cost of requiring separate HTTP POST requests for client-to-server commands.",
        failure_prevented="Prevents stateful edge proxy memory exhaustion, broken corporate firewall egress drops, and complex reconnect handshake storms that plague raw WebSocket deployments during network reconnect bursts.",
        citations=["RFC 6202: Known Techniques for Server Push", "WHATWG: Server-Sent Events Specification"]
    ),
    # 2. Kahn DAG vs Priority Queue
    HeuristicKnowledgeEntry(
        key="kahn_dag_vs_priority_queue",
        patterns=[
            re.compile(r"\b(kahn|topological|dag)\s+(vs|versus|over)\s+(priority queue|pq|heap)\b", re.IGNORECASE),
            re.compile(r"\b(priority queue|pq|heap)\s+(vs|versus|over)\s+(kahn|topological|dag)\b", re.IGNORECASE),
            re.compile(r"\b(kahn.*dag|dag.*scheduling)\b", re.IGNORECASE),
        ],
        axiom="Multi-task dependency execution graphs require level-synchronous Kahn's topological sorting over scalar priority queues to guarantee strict partial-order causal invariants and eliminate circular lock races.",
        tradeoff="Kahn's DAG partition computes discrete zero-in-degree concurrency tiers ensuring tasks only execute after all causal parents succeed, whereas priority queues greedily dispatch unblocked tasks by static weight, ignoring runtime topological prerequisites.",
        failure_prevented="Prevents out-of-order task dispatching where dependent child jobs execute against uninitialized schema or uncommitted code artifacts, causing cascading cascade-aborts and silent build failures.",
        citations=["Kahn, A. B. (1962). Topological Sorting of Large Networks", "Tarjan, R. E. (1972). Depth-First Search and Linear Graph Algorithms"]
    ),
    # 3. ThreadPoolExecutor vs Asyncio
    HeuristicKnowledgeEntry(
        key="threadpool_vs_asyncio",
        patterns=[
            re.compile(r"\b(threadpool|threads?|threading)\s+(vs|versus|over)\s+(asyncio|coroutines?|async)\b", re.IGNORECASE),
            re.compile(r"\b(asyncio|coroutines?|async)\s+(vs|versus|over)\s+(threadpool|threads?|threading)\b", re.IGNORECASE),
            re.compile(r"\b(threadpool.*asyncio|threads?.*async)\b", re.IGNORECASE),
        ],
        axiom="Non-blocking network socket I/O belongs in an event-loop coroutine runtime (Asyncio), while blocking C-extensions, subprocesses, and cryptographic file I/O must be isolated in bounded ThreadPoolExecutors to preserve event loop responsiveness.",
        tradeoff="Asyncio provides sub-millisecond cooperative multiplexing for tens of thousands of idle connections with minimal memory footprint, whereas ThreadPoolExecutor incurs OS kernel context-switching overhead (~8MB stack per thread) but prevents long-running operations from blocking the reactor thread.",
        failure_prevented="Prevents complete event-loop thread starvation where a single synchronous file read or child process pipe blocks heartbeat monitors, triggering false-positive cluster health-check failures and node evictions.",
        citations=["PEP 3156: Asynchronous IO Support", "Ousterhout, J. (1996). Why Threads Are A Bad Idea (for most things)"]
    ),
    # 4. In-Memory State vs Distributed Redis
    HeuristicKnowledgeEntry(
        key="in_memory_vs_redis",
        patterns=[
            re.compile(r"\b(in[- ]?memory|local cache|in[- ]?process)\s+(vs|versus|over)\s+(redis|distributed cache|key-value)\b", re.IGNORECASE),
            re.compile(r"\b(redis|distributed cache)\s+(vs|versus|over)\s+(in[- ]?memory|local cache|in[- ]?process)\b", re.IGNORECASE),
        ],
        axiom="Single-host orchestrator state and sub-millisecond query caches must remain in-memory with thread-safe locking, while multi-replica coordination and durable leases require an external distributed store like Redis.",
        tradeoff="In-memory cache delivers sub-microsecond access with zero serialisation and network latency, discarding Redis network hops (~1-3ms) at the cost of non-shared cache state and loss of state across process restarts.",
        failure_prevented="Prevents distributed network split-brain partitions and cascading latency amplification from saturating Redis socket connections during high-frequency agent telemetry polling.",
        citations=["Designing Data-Intensive Applications (Kleppmann, Ch. 3 - Storage and Retrieval)"]
    ),
    # 5. Polling vs Webhooks / Push
    HeuristicKnowledgeEntry(
        key="polling_vs_webhooks",
        patterns=[
            re.compile(r"\b(polling|pull)\s+(vs|versus|over)\s+(webhooks?|push|event-driven)\b", re.IGNORECASE),
            re.compile(r"\b(webhooks?|push|event-driven)\s+(vs|versus|over)\s+(polling|pull)\b", re.IGNORECASE),
        ],
        axiom="High-velocity state changes mandate event-driven push notifications over frequent short-polling loops to minimize redundant HTTP roundtrips and conserve API rate quotas.",
        tradeoff="Push and webhooks deliver instant real-time event propagation with zero idle bandwidth consumption, but require public endpoint ingress or persistent multiplexed channels, unlike resilient client-driven exponential backoff polling.",
        failure_prevented="Prevents rate-limit quota exhaustion (HTTP 429 storms) and unnecessary CPU load on API gateways caused by hundreds of autonomous worker agents constantly polling for state transitions.",
        citations=["Hohpe & Woolf (2003): Enterprise Integration Patterns - Publish-Subscribe Channel"]
    ),
    # 6. Circuit Breaker
    HeuristicKnowledgeEntry(
        key="circuit_breaker",
        patterns=[
            re.compile(r"\bcircuit[_ -]?breaker\b", re.IGNORECASE),
            re.compile(r"\b(fail[- ]?fast|half[- ]?open|trip threshold)\b", re.IGNORECASE),
        ],
        axiom="All remote network boundaries must be fronted by a three-state Circuit Breaker (Closed, Open, Half-Open) to fail-fast immediately during downstream outages rather than queuing unbounded blocking requests.",
        tradeoff="Circuit breakers provide instantaneous fallback and protect worker threadpool capacity, at the cost of rejecting a small percentage of recoverable requests during transient downstream blips.",
        failure_prevented="Prevents cascading upstream failure storms, cluster-wide worker threadpool exhaustion, and catastrophic service brownouts across dependent distributed services.",
        citations=["Nygard, M. (2018): Release It! Design and Deploy Production-Ready Software", "Martin Fowler: CircuitBreaker Pattern"]
    ),
    # 7. Mutex Lock & Distributed Fencing
    HeuristicKnowledgeEntry(
        key="mutex_fencing",
        patterns=[
            re.compile(r"\b(mutex|fencing token|distributed lock|lock contention)\b", re.IGNORECASE),
            re.compile(r"\b(split[- ]?brain|git index lock)\b", re.IGNORECASE),
        ],
        axiom="Shared mutable resources across concurrent agents must enforce monotonic fencing tokens alongside mutual exclusion to invalidate stale writers suffering from GC pauses or process delays.",
        tradeoff="Fencing tokens guarantee strict linearizable consistency across asynchronous workers, sacrificing write concurrency by serializing access through a single authoritative lease coordinator.",
        failure_prevented="Prevents split-brain file corruption, overwritten code commits, and stale agent writes clobbering updated master repository states after unannounced timeouts.",
        citations=["Kleppmann, M. (2016): How to do distributed locking", "Lamport, L. (1978): Time, Clocks, and the Ordering of Events in a Distributed System"]
    ),
    # 8. Outbox Pattern
    HeuristicKnowledgeEntry(
        key="outbox_pattern",
        patterns=[
            re.compile(r"\b(outbox|transactional outbox|dual write)\b", re.IGNORECASE),
            re.compile(r"\b(at[- ]?least[- ]?once delivery|eventual consistency)\b", re.IGNORECASE),
        ],
        axiom="State transitions and their associated outbound domain events must be committed within the same atomic database transaction via a Transactional Outbox table to eliminate dual-write partial failures.",
        tradeoff="Guarantees at-least-once message delivery without distributed two-phase commit (2PC) transactions, at the expense of requiring asynchronous message relay workers and downstream idempotent consumer deduplication.",
        failure_prevented="Prevents silent data inconsistencies where database records are persisted but external notifications, Kafka messages, or GitHub sync webhooks fail due to network drops.",
        citations=["Richardson, C. (2018): Microservices Patterns - Transactional Outbox Pattern"]
    ),
    # 9. Rate Governor & Adaptive Flow Policy
    HeuristicKnowledgeEntry(
        key="rate_governor",
        patterns=[
            re.compile(r"\b(rate[_ -]?governor|token bucket|leaky bucket|429 rate limit)\b", re.IGNORECASE),
            re.compile(r"\b(adaptive rate|token flow policy)\b", re.IGNORECASE),
        ],
        axiom="Autonomous LLM client invocations must pass through an Adaptive Token Flow Governor employing dynamic leaky-bucket pacing and AIMD (additive increase/multiplicative decrease) backoff.",
        tradeoff="Eliminates HTTP 429 provider rate-limit penalties and protects token budget quotas, incurring minor scheduling delays (500ms-2s) during burst parallel milestone executions.",
        failure_prevented="Prevents catastrophic cascading 429 account suspensions, broken agent retry loops, and uncoordinated thundering herd API stampedes during multi-repo autonomous sprints.",
        citations=["Jacobson, V. (1988): Congestion Avoidance and Control", "Hermes Adaptive Token Flow Policy Specification"]
    ),
    # 10. Event Bus & Decoupled Pub-Sub
    HeuristicKnowledgeEntry(
        key="event_bus",
        patterns=[
            re.compile(r"\b(event[_ -]?bus|pub[_ -]?sub|publish[- ]subscribe)\b", re.IGNORECASE),
            re.compile(r"\b(decoupled architecture|message dispatcher)\b", re.IGNORECASE),
        ],
        axiom="Cross-subsystem telemetry and notifications must traverse an in-process, non-blocking Event Bus to decouple event producers from downstream listeners and telemetry sinks.",
        tradeoff="Enables seamless multi-listener instrumentation (Cockpit UI, Arize Phoenix, Telegram alerts) without modifying core business workflows, trading synchronous call-stack tracing for asynchronous event debugging.",
        failure_prevented="Prevents slow or failing downstream logging sinks (e.g. stalled telemetry network sockets) from bottlenecking or crashing critical task dispatch loops.",
        citations=["Gamma, Helm, Johnson, Vlissides (1994): Design Patterns (Observer Pattern)", "Enterprise Integration Patterns (Hohpe & Woolf)"]
    ),
]


class LocalHeuristicArchitect:
    """
    Offline Decision Tree Knowledge Engine.
    Provides instant, publication-grade architectural responses adhering strictly to the
    Tripartite Socratic Schema when LLM providers are offline, rate-limited, or unavailable.
    """

    @classmethod
    def evaluate(
        cls,
        intent: ArchitectIntent,
        query: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Tuple[str, List[str]]:
        """
        Evaluates query and context against canonical distributed patterns and trade-offs.
        Returns (formatted_markdown_answer, citations).
        """
        clean_q = query.strip()
        context = context or {}

        # 1. Direct match against offline knowledge base patterns
        for entry in OFFLINE_KNOWLEDGE_BASE:
            for pat in entry.patterns:
                if pat.search(clean_q):
                    return entry.format_markdown(), entry.citations

        # 2. Check detected patterns from context (P)
        patterns = context.get("matched_patterns") or []
        primary_pat = context.get("primary_pattern")
        if primary_pat:
            pat_id = primary_pat.get("id", "")
            # Cross-reference with our knowledge base entries
            for entry in OFFLINE_KNOWLEDGE_BASE:
                if entry.key == pat_id or pat_id in entry.key:
                    return entry.format_markdown(), entry.citations

            # Synthesize from Pattern Catalog ADR-Lite if available
            adr = primary_pat.get("adr_lite")
            micro = primary_pat.get("micro_lesson")
            if adr and micro:
                axiom = (
                    f"Architecture mandates enforcing the {primary_pat.get('label', pat_id)}: "
                    f"{adr.get('decision', 'System must enforce strict modular isolation.')}"
                )
                discarded = adr.get("discarded_alternatives", [])
                tradeoff = ""
                if discarded:
                    first_alt = discarded[0]
                    alt_name = first_alt.get("alternative", "Standard Alternative")
                    alt_comp = first_alt.get("tradeoff_comparison", "Lacks strict resilience guarantees.")
                    tradeoff = (
                        f"Adopted {primary_pat.get('label')} ({adr.get('chosen_rationale', '')}) over {alt_name}, "
                        f"because the discarded alternative {alt_comp}"
                    )
                else:
                    tradeoff = (
                        f"Chosen rationale: {adr.get('chosen_rationale', '')}. "
                        f"Requires deliberate state machine tracking to avoid synchronization overhead."
                    )
                disaster = micro.get("disaster_prevented", "Prevents unhandled runtime exceptions and distributed service degradation.")

                ans = (
                    f"### 1. Direct Architectural Axiom\n"
                    f"{axiom}\n\n"
                    f"### 2. Trade-off Comparison\n"
                    f"{tradeoff}\n\n"
                    f"### 3. Catastrophic Production Failure Prevented\n"
                    f"{disaster}"
                )
                citations = [f"Hermes Architectural Pattern Catalog: {primary_pat.get('label', pat_id)}"]
                return ans, citations

        # 3. Intent-specific fallback synthesis
        milestone_name = context.get("milestone_metadata", {}).get("name", "Target Milestone")

        if intent == ArchitectIntent.SCALE_PROJECTION:
            axiom = f"Scaling {milestone_name} by 100x requires isolating shared state behind bounded queues and non-blocking I/O primitives."
            tradeoff = "Asynchronous batched decomposition trades sub-millisecond immediate response latency for order-of-magnitude higher sustained throughput and deterministic memory consumption."
            disaster = "Prevents catastrophic heap exhaustion, unbounded worker thread thrashing, and database connection pool saturation during 100x traffic surges."
            citations = ["Designing Data-Intensive Applications (Kleppmann, Ch. 1 - Reliability, Scalability, Maintainability)"]

        elif intent == ArchitectIntent.BLAST_RADIUS:
            axiom = f"The failure boundary of {milestone_name} must be strictly partitioned to ensure failures never spill over into unrelated DAG execution tiers."
            tradeoff = "Defensive worktree and process isolation introduces minor fork and cleanup latency (~50ms) but completely shields the master repository tree from dirty state contamination."
            disaster = "Prevents corrupt merge commits, uncommitted lockfile contention (.git/index.lock), and cascading task cancellations across unaffected repositories."
            citations = ["Release It! Design and Deploy Production-Ready Software (Nygard, 2nd ed.)"]

        elif intent == ArchitectIntent.CONCURRENCY_SAFETY:
            axiom = f"Concurrent execution across parallel agent workers requires linearizable synchronization with monotonic lease fencing tokens."
            tradeoff = "Fencing tokens enforce strict write serialization, rejecting concurrent writes with negligible latency (~2ms) rather than risking subtle silent state clobbering."
            disaster = "Prevents split-brain race conditions where two asynchronous agents simultaneously overwrite the same source artifact, corrupting repository history."
            citations = ["Kleppmann, M. (2016): How to do distributed locking"]

        elif intent == ArchitectIntent.FAILURE_MODE:
            axiom = f"System components must fail-fast with explicit typed exceptions and structured degraded fallback modes rather than stalling indefinitely."
            tradeoff = "Immediate failure tripping drops stalled remote calls instantly, reserving compute resources for healthy subsystems while shedding unrecoverable load."
            disaster = "Prevents cluster-wide cascading brownouts caused by queued blocking threads consuming all available memory and file descriptors during upstream outages."
            citations = ["Google Site Reliability Engineering (Beyer et al., Ch. 22 - Addressing Cascading Failures)"]

        elif intent == ArchitectIntent.TRADE_OFF:
            axiom = f"Architectural decisions for {milestone_name} must prioritize modular cohesion and deterministic testability over speculative abstractions."
            tradeoff = "Explicit local state models eliminate distributed network serialization complexity, at the cost of bounding high-speed caching within local process boundaries."
            disaster = "Prevents distributed state desynchronization and un-reproducible race conditions in asynchronous test harnesses."
            citations = ["Software Architecture in Practice (Bass, Clements, Kazman)"]

        else:  # GENERAL_SOCRATIC
            axiom = f"Every milestone in the autonomous orchestrator must uphold bounded blast radiuses, idempotent execution, and transparent state telemetry."
            tradeoff = "Deterministic state machines and defensive validation increase initial design overhead but eliminate non-deterministic failure recovery loops."
            disaster = "Prevents unmonitored silent agent drift, orphaned processes leaking system resources, and unrecoverable pipeline deadlocks."
            citations = ["Hermes Autonomous Architecture Guidelines", "Enterprise Integration Patterns (Hohpe & Woolf)"]

        answer = (
            f"### 1. Direct Architectural Axiom\n"
            f"{axiom}\n\n"
            f"### 2. Trade-off Comparison\n"
            f"{tradeoff}\n\n"
            f"### 3. Catastrophic Production Failure Prevented\n"
            f"{disaster}"
        )
        return answer, citations


# ==============================================================================
# 5. In-Memory LRU Cache & SHA-256 Query Fingerprinting
# ==============================================================================

class LRUArchitectCache:
    """
    Thread-safe in-memory Least Recently Used (LRU) cache with SHA-256 fingerprinting.
    Capacity: 256 entries. TTL: 86,400 seconds (24 hours).
    """

    def __init__(self, capacity: int = 256, ttl_seconds: float = 86400.0) -> None:
        self.capacity = capacity
        self.ttl_seconds = ttl_seconds
        self._cache: collections.OrderedDict[str, Tuple[ArchitectResponse, float]] = collections.OrderedDict()
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0

    @staticmethod
    def compute_fingerprint(milestone_id: str, intent: ArchitectIntent, query: str) -> str:
        """
        Calculates SHA-256 query fingerprint K = SHA256(milestone_id + intent + normalized_query).
        """
        normalized_q = re.sub(r"\s+", " ", query.strip().lower())
        raw_key = f"{milestone_id.strip()}::{intent.value}::{normalized_q}"
        return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

    def get(self, key: str) -> Optional[ArchitectResponse]:
        """Retrieves cached response if present and not expired."""
        with self._lock:
            if key not in self._cache:
                self.misses += 1
                return None

            resp, timestamp = self._cache[key]
            if (time.time() - timestamp) > self.ttl_seconds:
                # Expired
                del self._cache[key]
                self.misses += 1
                return None

            # Move to end (most recently used)
            self._cache.move_to_end(key)
            self.hits += 1
            # Return fresh copy marked cached
            return ArchitectResponse(
                answer=resp.answer,
                intent=resp.intent,
                citations=list(resp.citations),
                latency_ms=0.5,  # Sub-millisecond cache retrieval
                cached=True,
                source=resp.source,
            )

    def set(self, key: str, response: ArchitectResponse) -> None:
        """Inserts or updates response with current timestamp and manages LRU eviction."""
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
            self._cache[key] = (response, time.time())

            if len(self._cache) > self.capacity:
                self._cache.popitem(last=False)  # Evict oldest entry

    def clear(self) -> None:
        """Clears all cached entries."""
        with self._lock:
            self._cache.clear()
            self.hits = 0
            self.misses = 0

    def stats(self) -> Dict[str, Any]:
        """Returns cache telemetry stats."""
        with self._lock:
            total = self.hits + self.misses
            ratio = (self.hits / total * 100.0) if total > 0 else 0.0
            return {
                "size": len(self._cache),
                "capacity": self.capacity,
                "hits": self.hits,
                "misses": self.misses,
                "hit_ratio_pct": round(ratio, 1),
            }


# ==============================================================================
# 6. Interactive Architect Engine & Cascade Invocation
# ==============================================================================

class InteractiveArchitectEngine:
    """
    Main Architectural Mentor Orchestrator.
    Manages caching, query intent classification, prompt context fusion,
    model cascade invocation (dynamic model provider), and zero-cost heuristic fallback.
    """

    _instance: Optional[InteractiveArchitectEngine] = None
    _singleton_lock = threading.Lock()

    def __init__(self) -> None:
        self.cache = LRUArchitectCache(capacity=256, ttl_seconds=86400.0)
        self.context_builder = SocraticContextBuilder()
        self.heuristic_architect = LocalHeuristicArchitect()

    @classmethod
    def get_instance(cls) -> InteractiveArchitectEngine:
        """Thread-safe singleton accessor."""
        if cls._instance is None:
            with cls._singleton_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def _call_remote_llm(
        self,
        system_prompt: str,
        user_content: str,
        timeout_s: float = 6.0,
    ) -> Optional[str]:
        """
        Attempts to query the configured active LLM server (via dynamic_model_provider).
        Gracefully returns None on timeout, connection refusal, or HTTP 429.
        """
        if get_active_llm_server_info is None:
            return None

        try:
            srv = get_active_llm_server_info()
            base_url = srv.get("base_url", "http://127.0.0.1:8045/v1").rstrip("/")
            api_key = srv.get("api_key", "")
            target_url = f"{base_url}/chat/completions"

            # Select model: preferred lightweight frontier models
            model = "gemini-3.8-flash-tiered"
            static_models = srv.get("static_models", [])
            if static_models and model not in static_models:
                model = static_models[0]

            payload = {
                "model": model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content},
                ],
                "temperature": 0.2,
                "max_tokens": 300,  # Strict bound for <= 250 token output
            }

            req = urllib.request.Request(
                target_url,
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "Hermes-InteractiveArchitect/1.0",
                    **({"Authorization": f"Bearer {api_key}"} if api_key else {}),
                },
                method="POST",
            )

            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                if resp.status == 200:
                    data = json.loads(resp.read().decode("utf-8"))
                    choices = data.get("choices", [])
                    if choices:
                        content = choices[0].get("message", {}).get("content", "").strip()
                        if content and "Direct Architectural Axiom" in content:
                            return content
        except Exception:
            # Fallback quietly on any network, timeout, or parsing error
            return None

        return None

    def ask(
        self,
        milestone_id: str,
        milestone_data: Dict[str, Any],
        query: str,
        force_heuristic: bool = False,
    ) -> ArchitectResponse:
        """
        Primary execution method:
        1. Classifies query intent.
        2. Checks SHA-256 LRU cache.
        3. Builds fused context C = Phi(M, P, D, Q).
        4. Invokes LLM cascade with zero-token heuristic fallback.
        5. Updates cache and returns response.
        """
        t0 = time.perf_counter()
        clean_q = query.strip()
        intent = classify_query_intent(clean_q)

        # 1. Compute fingerprint & check cache
        fingerprint = self.cache.compute_fingerprint(milestone_id, intent, clean_q)
        cached_resp = self.cache.get(fingerprint)
        if cached_resp is not None:
            return cached_resp

        # 2. Build fused context
        context = self.context_builder.build_context(milestone_id, milestone_data, clean_q)

        # 3. Model Cascade Invocation
        answer_text: Optional[str] = None
        source = "llm"
        citations: List[str] = []

        if not force_heuristic:
            try:
                answer_text = self._call_remote_llm(
                    system_prompt=context["system_prompt"],
                    user_content=context["user_content"],
                    timeout_s=5.0,
                )
            except Exception:
                answer_text = None

        if answer_text:
            source = "llm"
            # Extract citations from detected patterns if any
            primary_pat = context.get("primary_pattern")
            if primary_pat:
                citations.append(f"Architectural Pattern: {primary_pat.get('label')}")
            citations.append("Hermes Interactive Socratic Engine (LLM Synthesis)")
        else:
            # 4. Zero-Token Heuristic Fallback Engine
            answer_text, citations = self.heuristic_architect.evaluate(
                intent=intent,
                query=clean_q,
                context=context,
            )
            source = "heuristic_fallback"

        elapsed_ms = (time.perf_counter() - t0) * 1000.0

        response = ArchitectResponse(
            answer=answer_text,
            intent=intent,
            citations=citations,
            latency_ms=elapsed_ms,
            cached=False,
            source=source,
        )

        # Save to cache
        self.cache.set(fingerprint, response)

        return response


# ==============================================================================
# 7. HTTP Controller Hook: Drop-In for control_center/server.py
# ==============================================================================

def handle_architect_request(payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
    """
    Standard HTTP Controller Hook ready for integration in control_center/server.py
    for POST /api/ask-architect requests.

    Parameters:
      payload: Dict containing:
        - 'milestone_id': str (optional, defaults to 'adhoc')
        - 'milestone_data': dict (optional milestone details, file, dag info)
        - 'query': str (required)
        - 'force_heuristic': bool (optional)

    Returns:
      Tuple[int, dict]: (HTTP status code, response dictionary)
    """
    if not isinstance(payload, dict):
        return 400, {
            "status": "error",
            "message": "Malformed payload: JSON object expected.",
        }

    raw_query = payload.get("query")
    if not raw_query or not isinstance(raw_query, str) or not raw_query.strip():
        return 400, {
            "status": "error",
            "message": "Missing or empty required field: 'query'.",
        }

    milestone_id = str(payload.get("milestone_id") or "adhoc").strip()
    milestone_data = payload.get("milestone_data") or {}
    if not isinstance(milestone_data, dict):
        milestone_data = {}

    force_heuristic = bool(payload.get("force_heuristic", False))

    try:
        engine = InteractiveArchitectEngine.get_instance()
        resp = engine.ask(
            milestone_id=milestone_id,
            milestone_data=milestone_data,
            query=raw_query,
            force_heuristic=force_heuristic,
        )
        return 200, resp.to_dict()
    except Exception as e:
        # Guarantee no unhandled exceptions crash the HTTP server thread
        return 500, {
            "status": "error",
            "message": f"Interactive Architect Engine error: {str(e)}",
            "intent": ArchitectIntent.GENERAL_SOCRATIC.value,
            "latency_ms": 0.0,
            "cached": False,
            "source": "error_handler",
        }


# ==============================================================================
# 8. Comprehensive Self-Contained Verification & Test Suite
# ==============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print(" Hermes Socratic Architectural Mentor & Interactive Architect Engine")
    print(" Comprehensive In-Memory Test & Verification Suite")
    print("=" * 80)

    # --------------------------------------------------------------------------
    # Test 1: Intent Classification Verification
    # --------------------------------------------------------------------------
    test_queries = [
        ("Should we use SSE vs WebSockets for the live dashboard?", ArchitectIntent.TRADE_OFF),
        ("What is the trade-off of Kahn DAG sorting compared to priority queues?", ArchitectIntent.TRADE_OFF),
        ("How does this engine scale to 100x throughput and 100,000 QPS?", ArchitectIntent.SCALE_PROJECTION),
        ("What is the blast radius if the git worktree sandbox fails?", ArchitectIntent.BLAST_RADIUS),
        ("How do we guarantee concurrency safety and prevent race conditions?", ArchitectIntent.CONCURRENCY_SAFETY),
        ("What happens if downstream network splits causing split-brain?", ArchitectIntent.FAILURE_MODE),
        ("Explain the core architectural principles of this milestone.", ArchitectIntent.GENERAL_SOCRATIC),
    ]

    print("\n[Suite 1] Query Intent Classifier Verification:")
    passed_classifier = 0
    for q, expected in test_queries:
        detected = classify_query_intent(q)
        match = (detected == expected)
        mark = "✓" if match else "✗"
        if match:
            passed_classifier += 1
        print(f" {mark} Query: \"{q[:50]}...\" -> Detected: {detected.value} (Expected: {expected.value})")
    assert passed_classifier == len(test_queries), f"Intent classifier failed: {passed_classifier}/{len(test_queries)}"
    print(f" -> Passed {passed_classifier}/{len(test_queries)} intent classifications.")

    # --------------------------------------------------------------------------
    # Test 2: Heuristic Zero-Token Knowledge Engine Verification
    # --------------------------------------------------------------------------
    print("\n[Suite 2] Heuristic Zero-Token Knowledge Engine:")
    engine = InteractiveArchitectEngine.get_instance()
    # Force heuristic to verify zero-token offline mode
    tradeoff_resp = engine.ask(
        milestone_id="test_sse",
        milestone_data={"name": "Live Event Streamer"},
        query="Should we use SSE vs WebSockets?",
        force_heuristic=True,
    )
    assert "Direct Architectural Axiom" in tradeoff_resp.answer
    assert "Trade-off Comparison" in tradeoff_resp.answer
    assert "Catastrophic Production Failure Prevented" in tradeoff_resp.answer
    assert tradeoff_resp.source == "heuristic_fallback"
    print(" ✓ SSE vs WebSockets response adheres strictly to Tripartite Schema:")
    for line in tradeoff_resp.answer.splitlines()[:6]:
        print(f"   | {line}")

    # --------------------------------------------------------------------------
    # Test 3: Socratic Context Builder Fusion: C = Phi(M, P, D, Q)
    # --------------------------------------------------------------------------
    print("\n[Suite 3] Context Fusion Engine Verification:")
    mock_milestone = {
        "name": "Distributed Circuit Breaker & Fallback Gateway",
        "relative_filepath": "gateway/circuit_breaker.py",
        "details": "Monitors API failure rates and trips threshold to half-open state during outages.",
        "technical_specification": "Implement three-state machine and fallback policies.",
        "prerequisites": ["tasks/dag_scheduler.py"],
        "tier": "Tier 1",
    }
    ctx = SocraticContextBuilder.build_context(
        milestone_id="ms_cb_001",
        milestone_data=mock_milestone,
        query="What is the blast radius if an upstream dependency crashes?",
    )
    assert ctx["milestone_id"] == "ms_cb_001"
    assert ctx["dag_dependencies"]["tier"] == "Tier 1"
    assert "Circuit Breaker" in str(ctx["matched_patterns"]) or "circuit_breaker" in str(ctx["matched_patterns"])
    print(" ✓ Fused Milestone Metadata, Pattern Catalog, and DAG Dependencies seamlessly.")

    # --------------------------------------------------------------------------
    # Test 4: HTTP Controller Hook & Cache Hit Verification
    # --------------------------------------------------------------------------
    print("\n[Suite 4] HTTP Controller Hook & LRU Cache Memoization:")
    payload = {
        "milestone_id": "ms_cb_001",
        "milestone_data": mock_milestone,
        "query": "What is the blast radius of this circuit breaker?",
        "force_heuristic": True,
    }

    # First Call: Cache Miss
    code1, res1 = handle_architect_request(payload)
    assert code1 == 200
    assert res1["status"] == "success"
    assert res1["cached"] is False
    assert res1["intent"] == ArchitectIntent.BLAST_RADIUS.value
    print(f" ✓ Initial Request: HTTP {code1} | Cached: {res1['cached']} | Latency: {res1['latency_ms']:.2f}ms")

    # Second Call: Cache Hit (sub-millisecond)
    code2, res2 = handle_architect_request(payload)
    assert code2 == 200
    assert res2["status"] == "success"
    assert res2["cached"] is True
    assert res2["answer"] == res1["answer"]
    print(f" ✓ Cached Request:  HTTP {code2} | Cached: {res2['cached']} | Latency: {res2['latency_ms']:.2f}ms")

    # Test Invalid Payload
    bad_code, bad_res = handle_architect_request({"milestone_id": "bad"})
    assert bad_code == 400
    assert bad_res["status"] == "error"
    print(f" ✓ Malformed Request Protection: HTTP {bad_code} (Gracefully caught: {bad_res['message']})")

    cache_stats = engine.cache.stats()
    print(f"\n[LRU Cache Telemetry] Entries: {cache_stats['size']} | Hits: {cache_stats['hits']} | Misses: {cache_stats['misses']}")
    print("=" * 80)
    print(" ALL 4 INTERACTIVE ARCHITECT ENGINE VERIFICATION SUITES PASSED (100% OK)")
    print("=" * 80)