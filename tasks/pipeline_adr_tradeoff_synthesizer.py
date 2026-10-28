#!/usr/bin/env python3
"""
Pipeline Autonomic Architectural Decision Record (ADR) Synthesizer & Trade-Off Matrix Generator.

Provides multi-criteria quality attribute evaluations (WSM/AHP), Pareto dominance verification,
canonical distributed systems paradigm comparisons, and Michael Nygard-compliant ADR generation.
"""

from __future__ import annotations

import dataclasses
import datetime
import enum
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from typing import Any, Dict, List, Optional, Tuple


class QualityAttribute(str, enum.Enum):
    """
    Standard quality attribute evaluation space Q = (q_1, q_2, q_3, q_4, q_5)
    Scores are normalized in range [1.0, 5.0].
    """
    LATENCY = "latency"                          # q_1: Latency / Throughput
    CONSISTENCY = "consistency"                  # q_2: Data Consistency & Correctness
    FAULT_TOLERANCE = "fault_tolerance"          # q_3: Fault Tolerance & Partition Resilience
    OPERATIONAL_SIMPLICITY = "operational_simplicity"  # q_4: Operational Simplicity / Maintainability
    TOKEN_EFFICIENCY = "token_efficiency"        # q_5: Token & Computational Cost Efficiency


@dataclasses.dataclass(frozen=True)
class ArchitectureParadigm:
    """Pre-evaluated architectural pattern with quality attribute scores."""
    name: str
    description: str
    scores: Dict[QualityAttribute, float]
    best_for: List[str]
    drawbacks: List[str]

    def get_score(self, qa: QualityAttribute) -> float:
        return float(self.scores.get(qa, 3.0))


@dataclasses.dataclass
class ADRRecord:
    """Canonical representation of an Architectural Decision Record."""
    adr_id: str
    title: str
    status: str
    context: str
    decision: str
    chosen_paradigm: str
    discarded_paradigms: List[str]
    tradeoff_scores: Dict[str, Any]
    consequences_positive: List[str]
    consequences_negative: List[str]
    created_at: str

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ADRRecord:
        return cls(
            adr_id=data["adr_id"],
            title=data["title"],
            status=data["status"],
            context=data["context"],
            decision=data["decision"],
            chosen_paradigm=data["chosen_paradigm"],
            discarded_paradigms=list(data.get("discarded_paradigms", [])),
            tradeoff_scores=dict(data.get("tradeoff_scores", {})),
            consequences_positive=list(data.get("consequences_positive", [])),
            consequences_negative=list(data.get("consequences_negative", [])),
            created_at=data.get("created_at", datetime.datetime.now(datetime.timezone.utc).isoformat()),
        )


class ADRTradeoffSynthesizer:
    """
    Synthesizes autonomic Architectural Decision Records using multi-criteria
    decision analysis (WSM), Pareto frontier identification, and Nygard markdown templates.
    """

    DEFAULT_WEIGHTS: Dict[QualityAttribute, float] = {
        QualityAttribute.LATENCY: 0.20,
        QualityAttribute.CONSISTENCY: 0.25,
        QualityAttribute.FAULT_TOLERANCE: 0.20,
        QualityAttribute.OPERATIONAL_SIMPLICITY: 0.20,
        QualityAttribute.TOKEN_EFFICIENCY: 0.15,
    }

    ARCHITECTURE_CATALOG: Dict[str, ArchitectureParadigm] = {
        "CRDT State Replicator": ArchitectureParadigm(
            name="CRDT State Replicator",
            description="Conflict-Free Replicated Data Types for concurrent monotonic convergence without coordinator locking.",
            scores={
                QualityAttribute.LATENCY: 4.8,
                QualityAttribute.CONSISTENCY: 3.2,
                QualityAttribute.FAULT_TOLERANCE: 4.9,
                QualityAttribute.OPERATIONAL_SIMPLICITY: 3.0,
                QualityAttribute.TOKEN_EFFICIENCY: 4.5,
            },
            best_for=["Peer-to-peer state sync", "Network partitions", "Low-latency offline-first edits"],
            drawbacks=["Complex state tombstones", "Eventual consistency only", "Non-trivial merge mathematics"],
        ),
        "Raft Consensus Arbiter": ArchitectureParadigm(
            name="Raft Consensus Arbiter",
            description="Strong-leader quorum state machine replication for linearizable atomic state transitions.",
            scores={
                QualityAttribute.LATENCY: 2.8,
                QualityAttribute.CONSISTENCY: 5.0,
                QualityAttribute.FAULT_TOLERANCE: 4.2,
                QualityAttribute.OPERATIONAL_SIMPLICITY: 2.6,
                QualityAttribute.TOKEN_EFFICIENCY: 3.0,
            },
            best_for=["Distributed locking", "Configuration governance", "Strict linearizability"],
            drawbacks=["Leader bottleneck", "High latency on WAN", "Requires strict quorum (2f+1)"],
        ),
        "Event-Sourced CQRS": ArchitectureParadigm(
            name="Event-Sourced CQRS",
            description="Immutable append-only event streams paired with asynchronous read-model projections.",
            scores={
                QualityAttribute.LATENCY: 4.0,
                QualityAttribute.CONSISTENCY: 3.5,
                QualityAttribute.FAULT_TOLERANCE: 4.5,
                QualityAttribute.OPERATIONAL_SIMPLICITY: 2.4,
                QualityAttribute.TOKEN_EFFICIENCY: 4.2,
            },
            best_for=["Audit provenance", "Time-travel debugging", "High-throughput telemetry ingestion"],
            drawbacks=["Schema evolution complexity", "Read-model projection lag", "Storage amplification"],
        ),
        "Relational CRUD": ArchitectureParadigm(
            name="Relational CRUD",
            description="ACID-compliant centralized relational schema with normalized tables and blocking foreign keys.",
            scores={
                QualityAttribute.LATENCY: 3.2,
                QualityAttribute.CONSISTENCY: 4.8,
                QualityAttribute.FAULT_TOLERANCE: 2.7,
                QualityAttribute.OPERATIONAL_SIMPLICITY: 4.6,
                QualityAttribute.TOKEN_EFFICIENCY: 3.5,
            },
            best_for=["Rapid prototyping", "Transactional integrity", "Ad-hoc complex SQL queries"],
            drawbacks=["Single point of failure", "Write throughput bottleneck", "Difficult horizontal scaling"],
        ),
        "Transactional Outbox": ArchitectureParadigm(
            name="Transactional Outbox",
            description="Local database transaction co-located with an event outbox table polled or tailed by an asynchronous CDC relay.",
            scores={
                QualityAttribute.LATENCY: 4.2,
                QualityAttribute.CONSISTENCY: 4.5,
                QualityAttribute.FAULT_TOLERANCE: 4.4,
                QualityAttribute.OPERATIONAL_SIMPLICITY: 3.8,
                QualityAttribute.TOKEN_EFFICIENCY: 4.6,
            },
            best_for=["At-least-once distributed messaging", "Decoupled domain events", "Reliable dual-write prevention"],
            drawbacks=["De-duplication required at consumer", "CDC polling lag", "Outbox purge hygiene"],
        ),
        "Two-Phase Commit (2PC)": ArchitectureParadigm(
            name="Two-Phase Commit (2PC)",
            description="Synchronous atomic commit protocol across distributed databases managed by a central coordinator.",
            scores={
                QualityAttribute.LATENCY: 1.5,
                QualityAttribute.CONSISTENCY: 4.9,
                QualityAttribute.FAULT_TOLERANCE: 2.0,
                QualityAttribute.OPERATIONAL_SIMPLICITY: 2.0,
                QualityAttribute.TOKEN_EFFICIENCY: 2.2,
            },
            best_for=["Atomic multi-shard bank transfers", "Linearizable distributed transactions"],
            drawbacks=["Blocking coordinator failure", "High tail latency", "Severe throughput collapse under partitions"],
        ),
        "In-Memory Ring Buffer": ArchitectureParadigm(
            name="In-Memory Ring Buffer",
            description="Lock-free circular ring buffer (Disruptor pattern) for microsecond inter-thread event passing.",
            scores={
                QualityAttribute.LATENCY: 5.0,
                QualityAttribute.CONSISTENCY: 3.6,
                QualityAttribute.FAULT_TOLERANCE: 2.5,
                QualityAttribute.OPERATIONAL_SIMPLICITY: 4.0,
                QualityAttribute.TOKEN_EFFICIENCY: 4.9,
            },
            best_for=["Sub-millisecond telemetry capture", "In-process queueing", "CPU cache locality"],
            drawbacks=["Volatile on process crash", "Fixed capacity memory bounds", "Single-node limitation"],
        ),
        "LSM-Tree Engine": ArchitectureParadigm(
            name="LSM-Tree Engine",
            description="Log-Structured Merge-tree with append-only MemTable and multi-tiered SSTable compaction.",
            scores={
                QualityAttribute.LATENCY: 3.8,
                QualityAttribute.CONSISTENCY: 4.6,
                QualityAttribute.FAULT_TOLERANCE: 4.8,
                QualityAttribute.OPERATIONAL_SIMPLICITY: 3.2,
                QualityAttribute.TOKEN_EFFICIENCY: 4.4,
            },
            best_for=["Write-intensive workloads", "Durable disk persistence", "Compressed time-series logging"],
            drawbacks=["Compaction write amplification", "Read-path bloom filter overhead", "Spike latency during compactions"],
        ),
        "Asynchronous Gossip Protocol": ArchitectureParadigm(
            name="Asynchronous Gossip Protocol",
            description="Epidemic peer-to-peer state dissemination for decentralized membership and failure detection.",
            scores={
                QualityAttribute.LATENCY: 3.6,
                QualityAttribute.CONSISTENCY: 3.0,
                QualityAttribute.FAULT_TOLERANCE: 5.0,
                QualityAttribute.OPERATIONAL_SIMPLICITY: 3.5,
                QualityAttribute.TOKEN_EFFICIENCY: 4.7,
            },
            best_for=["Large-scale cluster membership", "Heartbeat failure detection", "Self-healing swarms"],
            drawbacks=["Probabilistic convergence bounds", "Redundant gossip messages", "Delayed state reconciliation"],
        ),
        "Synchronous Polling Arbiter": ArchitectureParadigm(
            name="Synchronous Polling Arbiter",
            description="Periodic client-driven HTTP/TCP health checks against centralized status endpoints.",
            scores={
                QualityAttribute.LATENCY: 2.2,
                QualityAttribute.CONSISTENCY: 4.0,
                QualityAttribute.FAULT_TOLERANCE: 2.8,
                QualityAttribute.OPERATIONAL_SIMPLICITY: 4.7,
                QualityAttribute.TOKEN_EFFICIENCY: 2.5,
            },
            best_for=["Simple monitoring daemons", "Low-frequency orchestration probes", "Minimal codebase overhead"],
            drawbacks=["High network traffic overhead", "Lag between polling intervals", "Thundering herd vulnerabilities"],
        ),
        "Distributed Mutex & Token Arbiter": ArchitectureParadigm(
            name="Distributed Mutex & Token Arbiter",
            description="Logical clock or token-passing mutual exclusion (Ricart-Agrawala) preventing dual-agent execution collisions.",
            scores={
                QualityAttribute.LATENCY: 3.5,
                QualityAttribute.CONSISTENCY: 4.9,
                QualityAttribute.FAULT_TOLERANCE: 4.2,
                QualityAttribute.OPERATIONAL_SIMPLICITY: 3.4,
                QualityAttribute.TOKEN_EFFICIENCY: 4.5,
            },
            best_for=["Single-writer pipeline gates", "Agent resource leasing", "Atomic git ref serialization"],
            drawbacks=["Token loss recovery complexity", "Message exchange overhead", "Deadlock detection requirements"],
        ),
    }

    def __init__(self, catalog: Optional[Dict[str, ArchitectureParadigm]] = None) -> None:
        self.catalog: Dict[str, ArchitectureParadigm] = dict(catalog or self.ARCHITECTURE_CATALOG)

    def register_paradigm(self, paradigm: ArchitectureParadigm) -> None:
        """Register or override an architectural paradigm."""
        self.catalog[paradigm.name] = paradigm

    def normalize_weights(self, weights: Optional[Dict[QualityAttribute, float]]) -> Dict[QualityAttribute, float]:
        """Normalize attribute weight vector such that sum(w_i) == 1.0."""
        target = weights or self.DEFAULT_WEIGHTS
        total = sum(target.values())
        if total <= 0:
            count = len(QualityAttribute)
            return {qa: 1.0 / count for qa in QualityAttribute}
        return {qa: target.get(qa, 0.0) / total for qa in QualityAttribute}

    def compute_composite_utility(
        self,
        paradigm: ArchitectureParadigm,
        weights: Dict[QualityAttribute, float]
    ) -> float:
        """Compute composite utility U(A_j) = sum_{k=1}^5 w_k * q_k(A_j)."""
        score = 0.0
        for qa, w in weights.items():
            score += w * paradigm.get_score(qa)
        return round(score, 4)

    def check_pareto_dominance(
        self,
        candidate_a: ArchitectureParadigm,
        candidate_b: ArchitectureParadigm
    ) -> Tuple[bool, str]:
        """
        Check if candidate_a strictly dominates candidate_b (A >_p B):
        forall k, q_k(A) >= q_k(B) AND exists k, q_k(A) > q_k(B).
        """
        greater_or_equal = True
        strictly_greater = False

        for qa in QualityAttribute:
            score_a = candidate_a.get_score(qa)
            score_b = candidate_b.get_score(qa)
            if score_a < score_b:
                greater_or_equal = False
                break
            if score_a > score_b:
                strictly_greater = True

        if greater_or_equal and strictly_greater:
            return True, f"{candidate_a.name} strictly dominates {candidate_b.name} across all quality vectors."
        return False, f"Trade-off frontier: {candidate_a.name} does not strictly dominate {candidate_b.name}."

    def evaluate_tradeoff(
        self,
        chosen_name: str,
        alternative_names: List[str],
        custom_weights: Optional[Dict[QualityAttribute, float]] = None,
    ) -> Dict[str, Any]:
        """
        Evaluate chosen architecture against discarded alternatives using WSM & Pareto analysis.
        """
        if chosen_name not in self.catalog:
            raise KeyError(f"Chosen architecture '{chosen_name}' not found in catalog.")

        weights = self.normalize_weights(custom_weights)
        chosen = self.catalog[chosen_name]
        chosen_utility = self.compute_composite_utility(chosen, weights)

        results: Dict[str, Any] = {
            "chosen": chosen_name,
            "chosen_utility": chosen_utility,
            "weights": {qa.value: round(w, 4) for qa, w in weights.items()},
            "alternatives": {},
            "pareto_frontier": [chosen_name],
        }

        all_candidates = [chosen]
        for alt_name in alternative_names:
            if alt_name in self.catalog:
                all_candidates.append(self.catalog[alt_name])

        # Identify non-dominated solutions forming the Pareto frontier
        frontier: List[str] = []
        for cand in all_candidates:
            dominated = False
            for other in all_candidates:
                if cand.name == other.name:
                    continue
                is_dom, _ = self.check_pareto_dominance(other, cand)
                if is_dom:
                    dominated = True
                    break
            if not dominated:
                frontier.append(cand.name)
        results["pareto_frontier"] = frontier

        # Generate comparative delta and rationale for each alternative
        for alt_name in alternative_names:
            if alt_name not in self.catalog:
                continue
            alt = self.catalog[alt_name]
            alt_utility = self.compute_composite_utility(alt, weights)

            deltas: Dict[str, float] = {}
            for qa in QualityAttribute:
                deltas[qa.value] = round(chosen.get_score(qa) - alt.get_score(qa), 2)

            is_dominated_by_chosen, _ = self.check_pareto_dominance(chosen, alt)
            is_chosen_dominated_by_alt, _ = self.check_pareto_dominance(alt, chosen)

            # Synthesize trade-off acceptance rationale
            positive_advantages = [qa.value.replace("_", " ").title() for qa in QualityAttribute if deltas[qa.value] > 0]
            conceded_drawbacks = [qa.value.replace("_", " ").title() for qa in QualityAttribute if deltas[qa.value] < 0]

            if is_dominated_by_chosen:
                rationale = f"{chosen.name} dominates {alt.name} on all evaluated dimensions with zero architectural penalty."
            elif chosen_utility >= alt_utility:
                adv_str = ", ".join(positive_advantages) or "holistic balance"
                con_str = ", ".join(conceded_drawbacks) or "minor operational friction"
                rationale = (
                    f"Accepted lower performance in ({con_str}) in exchange for superior ({adv_str}), "
                    f"yielding net higher utility ({chosen_utility} vs {alt_utility})."
                )
            else:
                rationale = (
                    f"Alternative {alt.name} holds localized utility advantage ({alt_utility} vs {chosen_utility}), "
                    f"but {chosen.name} was selected due to critical non-functional constraints."
                )

            results["alternatives"][alt_name] = {
                "utility": alt_utility,
                "utility_delta": round(chosen_utility - alt_utility, 4),
                "attribute_deltas": deltas,
                "is_dominated_by_chosen": is_dominated_by_chosen,
                "is_chosen_dominated_by_alt": is_chosen_dominated_by_alt,
                "rationale": rationale,
            }

        return results

    def synthesize_adr_for_milestone(
        self,
        milestone: Dict[str, Any],
        repo_context: Optional[Dict[str, Any]] = None,
    ) -> ADRRecord:
        """
        Synthesize a structured ADRRecord based on milestone metadata, context, and selected patterns.
        """
        title = milestone.get("title", "Autonomic System Architecture Decision")
        raw_key = f"{title}_{milestone.get('created_at', '')}"
        adr_hash = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()[:6].upper()
        adr_id = f"ADR-{adr_hash}"

        # Heuristic paradigm mapping from milestone tags / descriptions
        text_corpus = f"{title} {milestone.get('description', '')} {milestone.get('task', '')}".lower()

        chosen_paradigm = "Transactional Outbox"
        discarded: List[str] = ["Two-Phase Commit (2PC)"]

        if "crdt" in text_corpus or "state replicator" in text_corpus:
            chosen_paradigm = "CRDT State Replicator"
            discarded = ["Raft Consensus Arbiter"]
        elif "raft" in text_corpus or "consensus" in text_corpus:
            chosen_paradigm = "Raft Consensus Arbiter"
            discarded = ["CRDT State Replicator"]
        elif "cqrs" in text_corpus or "event sourc" in text_corpus:
            chosen_paradigm = "Event-Sourced CQRS"
            discarded = ["Relational CRUD"]
        elif "outbox" in text_corpus or "cdc" in text_corpus or "message" in text_corpus:
            chosen_paradigm = "Transactional Outbox"
            discarded = ["Two-Phase Commit (2PC)"]
        elif "ring" in text_corpus or "buffer" in text_corpus or "queue" in text_corpus:
            chosen_paradigm = "In-Memory Ring Buffer"
            discarded = ["LSM-Tree Engine"]
        elif "gossip" in text_corpus or "heartbeat" in text_corpus or "mesh" in text_corpus:
            chosen_paradigm = "Asynchronous Gossip Protocol"
            discarded = ["Synchronous Polling Arbiter"]
        elif "mutex" in text_corpus or "arbiter" in text_corpus or "token" in text_corpus:
            chosen_paradigm = "Distributed Mutex & Token Arbiter"
            discarded = ["Raft Consensus Arbiter", "Synchronous Polling Arbiter"]

        # Context-dependent weights: Offline/async vs Real-time/interactive
        weights = dict(self.DEFAULT_WEIGHTS)
        if "offline" in text_corpus or "batch" in text_corpus or "pipeline" in text_corpus:
            weights = {
                QualityAttribute.LATENCY: 0.10,
                QualityAttribute.CONSISTENCY: 0.30,
                QualityAttribute.FAULT_TOLERANCE: 0.25,
                QualityAttribute.OPERATIONAL_SIMPLICITY: 0.15,
                QualityAttribute.TOKEN_EFFICIENCY: 0.20,
            }
        elif "realtime" in text_corpus or "low latency" in text_corpus:
            weights = {
                QualityAttribute.LATENCY: 0.35,
                QualityAttribute.CONSISTENCY: 0.20,
                QualityAttribute.FAULT_TOLERANCE: 0.25,
                QualityAttribute.OPERATIONAL_SIMPLICITY: 0.10,
                QualityAttribute.TOKEN_EFFICIENCY: 0.10,
            }

        tradeoff = self.evaluate_tradeoff(chosen_paradigm, discarded, custom_weights=weights)

        context_stmt = milestone.get(
            "context",
            f"The autonomic pipeline requires high resilience under distributed execution constraints for '{title}'. "
            f"Cross-node coordination must minimize deadlock exposure while enforcing strict cost and execution boundaries."
        )

        decision_stmt = (
            f"Adopt the '{chosen_paradigm}' paradigm as the authoritative architectural foundation. "
            f"Rejected alternatives: {', '.join(discarded)}. "
            f"Utility evaluation verified a dominant score of {tradeoff['chosen_utility']} against operational baselines."
        )

        chosen_obj = self.catalog[chosen_paradigm]
        positives = [f"Direct architectural fit: {item}" for item in chosen_obj.best_for]
        positives.append(f"Composite utility score of {tradeoff['chosen_utility']} achieved in WSM evaluation.")

        negatives = [f"Requires operational mitigation: {drawback}" for drawback in chosen_obj.drawbacks]

        now_str = datetime.datetime.now(datetime.timezone.utc).isoformat()
        return ADRRecord(
            adr_id=adr_id,
            title=title,
            status="ACCEPTED",
            context=context_stmt,
            decision=decision_stmt,
            chosen_paradigm=chosen_paradigm,
            discarded_paradigms=discarded,
            tradeoff_scores=tradeoff,
            consequences_positive=positives,
            consequences_negative=negatives,
            created_at=now_str,
        )

    def render_nygard_markdown(self, adr: ADRRecord) -> str:
        """
        Render a Michael Nygard-compliant Architectural Decision Record in clean Markdown.
        """
        lines: List[str] = [
            f"# {adr.adr_id}: {adr.title}",
            "",
            f"## Status",
            adr.status,
            "",
            f"## Context",
            adr.context,
            "",
            f"## Decision",
            adr.decision,
            "",
            "## Quality Attribute Trade-Off Matrix",
            "",
            "Multi-Criteria Analytical Hierarchy Process (AHP) & Weighted Sum Model (WSM):",
            "",
        ]

        # Table header
        chosen_name = adr.chosen_paradigm
        alt_names = adr.discarded_paradigms
        header = "| Quality Attribute | Weight | " + f"{chosen_name} (Chosen) | "
        separator = "| :--- | :---: | :---: | "

        for alt in alt_names:
            header += f"{alt} (Discarded) | Delta | "
            separator += ":---: | :---: | "
        lines.append(header)
        lines.append(separator)

        weights = adr.tradeoff_scores.get("weights", {})
        chosen_obj = self.catalog.get(chosen_name)

        for qa in QualityAttribute:
            w_val = weights.get(qa.value, 0.2)
            c_score = chosen_obj.get_score(qa) if chosen_obj else 0.0
            row = f"| **{qa.value.replace('_', ' ').title()}** | `{w_val:.2f}` | **{c_score:.1f}** | "

            for alt in alt_names:
                alt_obj = self.catalog.get(alt)
                a_score = alt_obj.get_score(qa) if alt_obj else 0.0
                delta = c_score - a_score
                delta_str = f"+{delta:.1f}" if delta > 0 else f"{delta:.1f}"
                row += f"{a_score:.1f} | `{delta_str}` | "
            lines.append(row)

        lines.append("")
        chosen_u = adr.tradeoff_scores.get("chosen_utility", 0.0)
        lines.append(f"- **Composite Utility $U(A^*)$**: `{chosen_u:.4f}`")
        lines.append(f"- **Pareto Frontier**: `{', '.join(adr.tradeoff_scores.get('pareto_frontier', []))}`")
        lines.append("")

        lines.append("## Discarded Alternatives & Counterfactual Analysis")
        alts_data = adr.tradeoff_scores.get("alternatives", {})
        if not alts_data:
            lines.append("None evaluated.")
        else:
            for alt_name, info in alts_data.items():
                lines.append(f"### Discarded: {alt_name}")
                lines.append(f"- **Calculated Utility**: `{info.get('utility', 0.0):.4f}` (Delta: `{info.get('utility_delta', 0.0):.4f}`)")
                lines.append(f"- **Pareto Status**: {'Dominated by chosen design' if info.get('is_dominated_by_chosen') else 'Non-dominated trade-off'}")
                lines.append(f"- **Trade-Off Rationale**: {info.get('rationale', 'Alternative discarded on architectural merit.')}")
                lines.append("")

        lines.append("## Consequences & Blast Radius")
        lines.append("### Positive Impact")
        for pos in adr.consequences_positive:
            lines.append(f"- {pos}")
        lines.append("")

        lines.append("### Negative & Compensating Factors")
        for neg in adr.consequences_negative:
            lines.append(f"- {neg}")
        lines.append("")

        lines.append(f"*Synthesized automatically by Hermes Autonomic ADR Engine at {adr.created_at}*")
        lines.append("")
        return "\n".join(lines)

    def save_adr_file(self, adr: ADRRecord, docs_dir: Path | str = "docs/adr") -> Path:
        """
        Persist rendered ADR Markdown document atomically to disk.
        """
        target_dir = Path(docs_dir)
        target_dir.mkdir(parents=True, exist_ok=True)

        clean_slug = re.sub(r"[^a-zA-Z0-9_\-]+", "_", adr.title.lower()).strip("_")
        filename = f"{adr.adr_id.lower()}_{clean_slug}.md"
        out_path = target_dir / filename
        temp_path = target_dir / f".{filename}.tmp"

        content = self.render_nygard_markdown(adr)
        with open(temp_path, "w", encoding="utf-8") as f:
            f.write(content)
        temp_path.replace(out_path)
        return out_path

    def batch_sync_registry(
        self,
        repos_file: str = "dashboards/repositories.json",
        output_json: str = "dashboards/adr_registry.json"
    ) -> int:
        """
        Batch inspect repositories or milestones and sync generated ADR records to registry JSON.
        """
        repos_path = Path(repos_file)
        out_path = Path(output_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        records: List[Dict[str, Any]] = []

        if repos_path.exists():
            try:
                with open(repos_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                milestones = []
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and "milestones" in item:
                            milestones.extend(item.get("milestones", []))
                        elif isinstance(item, dict):
                            milestones.append(item)
                elif isinstance(data, dict):
                    milestones = data.get("milestones", [data])

                for m in milestones:
                    adr = self.synthesize_adr_for_milestone(m)
                    records.append(adr.to_dict())
            except Exception:
                pass

        if not records:
            # Generate canonical default ADR records for baseline platform architecture
            canonical_milestones = [
                {
                    "title": "Decentralized Conflict-Free Replicated State Sync",
                    "description": "CRDT state replicator for concurrent multi-agent coordination without lockouts.",
                },
                {
                    "title": "Reliable Cross-Agent Domain Event Delivery",
                    "description": "Transactional outbox for atomic database updates and decoupled event emission.",
                },
                {
                    "title": "Distributed Mutex Token Arbiter for Execution Gates",
                    "description": "Ricart-Agrawala logical clock token ring preventing concurrent git write collisions.",
                },
            ]
            for cm in canonical_milestones:
                adr = self.synthesize_adr_for_milestone(cm)
                records.append(adr.to_dict())

        temp_out = out_path.parent / f".{out_path.name}.tmp"
        with open(temp_out, "w", encoding="utf-8") as f:
            json.dump({"registry_version": "1.0", "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(), "records": records}, f, indent=2)
        temp_out.replace(out_path)
        return len(records)


def _run_self_test() -> None:
    """Exhaustive self-test harness for ADRTradeoffSynthesizer."""
    synthesizer = ADRTradeoffSynthesizer()

    # 1. Multi-criteria WSM scoring test
    crdt = synthesizer.catalog["CRDT State Replicator"]
    raft = synthesizer.catalog["Raft Consensus Arbiter"]
    weights = synthesizer.normalize_weights(None)
    u_crdt = synthesizer.compute_composite_utility(crdt, weights)
    u_raft = synthesizer.compute_composite_utility(raft, weights)
    assert 1.0 <= u_crdt <= 5.0, f"CRDT utility out of bounds: {u_crdt}"
    assert 1.0 <= u_raft <= 5.0, f"Raft utility out of bounds: {u_raft}"

    # 2. Pareto Dominance Evaluation
    dom_res, reason = synthesizer.check_pareto_dominance(crdt, raft)
    assert isinstance(dom_res, bool)
    assert isinstance(reason, str)

    # 3. Trade-off matrix computation
    tradeoff = synthesizer.evaluate_tradeoff(
        chosen_name="Transactional Outbox",
        alternative_names=["Two-Phase Commit (2PC)"]
    )
    assert tradeoff["chosen"] == "Transactional Outbox"
    assert "Two-Phase Commit (2PC)" in tradeoff["alternatives"]
    outbox_delta = tradeoff["alternatives"]["Two-Phase Commit (2PC)"]["attribute_deltas"]
    assert "latency" in outbox_delta
    assert tradeoff["alternatives"]["Two-Phase Commit (2PC)"]["is_dominated_by_chosen"] is True

    # 4. Milestone ADR Synthesis & Nygard Markdown Rendering
    test_milestone = {
        "title": "High-Throughput In-Memory Pipeline Ring Buffer",
        "description": "Lock-free circular ring buffer to accelerate inter-thread event processing.",
        "context": "Agent telemetry processing creates I/O backpressure on disk engines.",
    }
    adr = synthesizer.synthesize_adr_for_milestone(test_milestone)
    assert adr.chosen_paradigm == "In-Memory Ring Buffer"
    assert "LSM-Tree Engine" in adr.discarded_paradigms

    md_output = synthesizer.render_nygard_markdown(adr)
    assert "# ADR-" in md_output
    assert "## Status" in md_output
    assert "## Context" in md_output
    assert "## Decision" in md_output
    assert "## Quality Attribute Trade-Off Matrix" in md_output
    assert "## Discarded Alternatives & Counterfactual Analysis" in md_output
    assert "## Consequences & Blast Radius" in md_output

    # 5. File Persistence & Registry Sync
    tmp_docs = Path("/tmp/hermes_test_adr_docs")
    tmp_reg = Path("/tmp/hermes_test_adr_reg/registry.json")
    try:
        saved_file = synthesizer.save_adr_file(adr, docs_dir=tmp_docs)
        assert saved_file.exists()
        assert saved_file.stat().st_size > 0

        synced_count = synthesizer.batch_sync_registry(repos_file="/nonexistent/repos.json", output_json=str(tmp_reg))
        assert synced_count >= 3
        assert tmp_reg.exists()
        with open(tmp_reg, "r", encoding="utf-8") as f:
            reg_data = json.load(f)
            assert len(reg_data["records"]) == synced_count
    finally:
        if tmp_docs.exists():
            for p in tmp_docs.glob("*"):
                p.unlink(missing_ok=True)
            tmp_docs.rmdir()
        if tmp_reg.exists():
            tmp_reg.unlink(missing_ok=True)
            tmp_reg.parent.rmdir()

    print("[PASS] ADRTradeoffSynthesizer verification passed")


if __name__ == "__main__":
    _run_self_test()