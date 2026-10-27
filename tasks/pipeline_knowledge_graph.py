#!/usr/bin/env python3
"""tasks/pipeline_knowledge_graph.py

Pipeline Autonomic Skill-to-Milestone Provenance Mapper & Architectural Knowledge Graph Engine.
================================================================================================
Unifies architectural metadata from across the Hermes Orchestrator pipeline:
  - Architecture Pattern Matcher (pattern instantiations)
  - Post-Mortem Pedagogy Engine (anti-pattern discoveries & broken assumptions)
  - Provenance Ledger (cryptographic execution proofs & milestone outcomes)
  - Configuration Schema Registry (versioned parameter modifications)
  - Distributed Quorum Consensus Engine (governance conflict verdicts)

Provides an in-memory, thread-safe, directed multi-relational knowledge graph
with typed nodes and edges, topological query capabilities (co-pattern Jaccard similarity,
subsystem module coupling matrices, Pearson causal parameter impact correlations,
bounded BFS causal chain reconstructions), and atomic JSON persistence.

Zero external dependencies - implemented entirely with Python standard library.
"""

from __future__ import annotations

import collections
from dataclasses import dataclass, field
from enum import Enum
import json
import math
import os
import re
import tempfile
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Union


# ==============================================================================
# Enumerations
# ==============================================================================

class NodeType(str, Enum):
    """Categorical classification of entities within the architectural graph."""
    MILESTONE = "MILESTONE"
    ARCHITECTURE_PATTERN = "ARCHITECTURE_PATTERN"
    ANTI_PATTERN = "ANTI_PATTERN"
    CONFIG_PARAMETER = "CONFIG_PARAMETER"
    SUBSYSTEM_MODULE = "SUBSYSTEM_MODULE"
    CONSENSUS_CONFLICT = "CONSENSUS_CONFLICT"
    PROVENANCE_RECORD = "PROVENANCE_RECORD"


class EdgeType(str, Enum):
    """Categorical classification of directed relationships between graph entities."""
    INSTANTIATES = "INSTANTIATES"           # milestone -> architecture_pattern (or milestone -> provenance)
    REVEALED = "REVEALED"                   # milestone -> anti_pattern
    MODIFIED = "MODIFIED"                   # milestone -> config_parameter
    TOUCHED = "TOUCHED"                     # milestone -> subsystem_module
    TRIGGERED = "TRIGGERED"                 # milestone -> consensus_conflict
    CAUSED_BY = "CAUSED_BY"                 # anti_pattern -> config_parameter change
    CORRELATED_WITH = "CORRELATED_WITH"     # consensus_conflict -> config_parameter


# ==============================================================================
# Graph Primitives (Nodes & Edges)
# ==============================================================================

@dataclass
class KnowledgeNode:
    """Represents a discrete entity in the architectural knowledge graph.

    Attributes:
        node_id: Unique identifier string for the node.
        node_type: Categorical classification conforming to NodeType enum.
        attributes: Contextual metadata dictionary.
        created_at: Epoch timestamp of node creation.
    """
    node_id: str
    node_type: NodeType
    attributes: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        """Serializes node instance to a dictionary."""
        return {
            "node_id": self.node_id,
            "node_type": self.node_type.value if isinstance(self.node_type, NodeType) else str(self.node_type),
            "attributes": dict(self.attributes),
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> KnowledgeNode:
        """Constructs a KnowledgeNode instance from serialized dictionary."""
        raw_type = data.get("node_type", "SUBSYSTEM_MODULE")
        node_type = NodeType(raw_type) if raw_type in NodeType._value2member_map_ else NodeType[raw_type]
        return cls(
            node_id=str(data["node_id"]),
            node_type=node_type,
            attributes=dict(data.get("attributes", {})),
            created_at=float(data.get("created_at", time.time())),
        )

    def __repr__(self) -> str:
        return f"KnowledgeNode(id={self.node_id!r}, type={self.node_type.value})"


@dataclass
class KnowledgeEdge:
    """Represents a directed relationship between two entities in the knowledge graph.

    Attributes:
        source_id: Originating node identifier.
        target_id: Destination node identifier.
        edge_type: Categorical classification conforming to EdgeType enum.
        weight: Quantitative strength or confidence of the relationship.
        metadata: Contextual metadata dictionary.
        created_at: Epoch timestamp of edge creation.
    """
    source_id: str
    target_id: str
    edge_type: EdgeType
    weight: float = 1.0
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        """Serializes edge instance to a dictionary."""
        return {
            "source_id": self.source_id,
            "target_id": self.target_id,
            "edge_type": self.edge_type.value if isinstance(self.edge_type, EdgeType) else str(self.edge_type),
            "weight": float(self.weight),
            "metadata": dict(self.metadata),
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> KnowledgeEdge:
        """Constructs a KnowledgeEdge instance from serialized dictionary."""
        raw_type = data.get("edge_type", "TOUCHED")
        edge_type = EdgeType(raw_type) if raw_type in EdgeType._value2member_map_ else EdgeType[raw_type]
        return cls(
            source_id=str(data["source_id"]),
            target_id=str(data["target_id"]),
            edge_type=edge_type,
            weight=float(data.get("weight", 1.0)),
            metadata=dict(data.get("metadata", {})),
            created_at=float(data.get("created_at", time.time())),
        )

    def __repr__(self) -> str:
        return f"KnowledgeEdge({self.source_id} -[{self.edge_type.value}:{self.weight:.2f}]-> {self.target_id})"


# ==============================================================================
# Statistical & Topological Math Helpers
# ==============================================================================

def compute_pearson_correlation(x: list[float], y: list[float]) -> float:
    """Computes sample Pearson correlation coefficient r between two numeric series.

    Formula:
        r = Σ((xi - x̄) * (yi - ȳ)) / (sqrt(Σ(xi - x̄)²) * sqrt(Σ(yi - ȳ)²))

    Returns 0.0 if sample size < 2, or if either series has zero variance.
    Clamps result within [-1.0, 1.0].
    """
    if len(x) != len(y) or len(x) < 2:
        return 0.0

    n = len(x)
    mean_x = sum(x) / n
    mean_y = sum(y) / n

    cov = 0.0
    var_x = 0.0
    var_y = 0.0

    for xi, yi in zip(x, y):
        dx = xi - mean_x
        dy = yi - mean_y
        cov += dx * dy
        var_x += dx * dx
        var_y += dy * dy

    denom = math.sqrt(var_x * var_y)
    if denom == 0.0:
        return 0.0

    r = cov / denom
    return max(-1.0, min(1.0, r))


def compute_jaccard_similarity(set_a: set[Any], set_b: set[Any]) -> float:
    """Computes the Jaccard similarity index between two sets.

    Formula:
        J(A, B) = |A ∩ B| / |A ∪ B|

    Returns 0.0 if both sets are empty.
    """
    if not set_a and not set_b:
        return 0.0
    union = set_a | set_b
    if not union:
        return 0.0
    intersection = set_a & set_b
    return len(intersection) / len(union)


# ==============================================================================
# Architectural Knowledge Graph Engine
# ==============================================================================

class ArchitecturalKnowledgeGraph:
    """In-memory, thread-safe architectural knowledge graph engine.

    Maintains dual adjacency lists (outgoing and incoming) for rapid graph traversal,
    enables multi-relational queries across pipeline entities, and supports atomic persistence.
    """

    def __init__(self, persistence_path: Optional[str] = "logs/knowledge_graph.json") -> None:
        self.persistence_path = persistence_path
        self._nodes: dict[str, KnowledgeNode] = {}
        self._outgoing: dict[str, list[KnowledgeEdge]] = collections.defaultdict(list)
        self._incoming: dict[str, list[KnowledgeEdge]] = collections.defaultdict(list)
        self._milestone_timeline: list[str] = []
        self._lock = threading.Lock()

    # --------------------------------------------------------------------------
    # Core Graph Mutation Primitives
    # --------------------------------------------------------------------------

    def add_node(self, node: KnowledgeNode) -> None:
        """Adds a node to the graph with deduplication by node_id.

        If a node with the same node_id already exists, merges incoming attributes
        into existing node attributes without overwriting unaffected fields.
        """
        with self._lock:
            if node.node_id in self._nodes:
                self._nodes[node.node_id].attributes.update(node.attributes)
            else:
                self._nodes[node.node_id] = node
                if node.node_id not in self._outgoing:
                    self._outgoing[node.node_id] = []
                if node.node_id not in self._incoming:
                    self._incoming[node.node_id] = []

    def add_edge(self, edge: KnowledgeEdge) -> None:
        """Adds a directed edge with deduplication by (source_id, target_id, edge_type).

        If the edge already exists, updates its weight and merges its metadata.
        Ensures both source and target nodes exist in the graph.
        """
        with self._lock:
            # Ensure placeholder nodes exist if not explicitly added
            if edge.source_id not in self._nodes:
                self._nodes[edge.source_id] = KnowledgeNode(
                    node_id=edge.source_id,
                    node_type=NodeType.MILESTONE if edge.source_id.startswith("ms_") else NodeType.SUBSYSTEM_MODULE,
                    attributes={"auto_created": True},
                    created_at=edge.created_at,
                )
                self._outgoing[edge.source_id] = []
                self._incoming[edge.source_id] = []

            if edge.target_id not in self._nodes:
                self._nodes[edge.target_id] = KnowledgeNode(
                    node_id=edge.target_id,
                    node_type=NodeType.SUBSYSTEM_MODULE,
                    attributes={"auto_created": True},
                    created_at=edge.created_at,
                )
                self._outgoing[edge.target_id] = []
                self._incoming[edge.target_id] = []

            # Check for existing edge in outgoing list
            existing_edge: Optional[KnowledgeEdge] = None
            for e in self._outgoing[edge.source_id]:
                if e.target_id == edge.target_id and e.edge_type == edge.edge_type:
                    existing_edge = e
                    break

            if existing_edge is not None:
                existing_edge.weight = edge.weight
                existing_edge.metadata.update(edge.metadata)
                # Also update corresponding entry in incoming list
                for in_e in self._incoming[edge.target_id]:
                    if in_e.source_id == edge.source_id and in_e.edge_type == edge.edge_type:
                        in_e.weight = edge.weight
                        in_e.metadata.update(edge.metadata)
                        break
            else:
                self._outgoing[edge.source_id].append(edge)
                self._incoming[edge.target_id].append(edge)

    # --------------------------------------------------------------------------
    # Subsystem Ingestion Endpoints
    # --------------------------------------------------------------------------

    def ingest_milestone_completion(
        self,
        milestone_id: str,
        milestone_metadata: Optional[dict[str, Any]] = None,
        patterns: Optional[list[str]] = None,
        touched_files: Optional[list[str]] = None,
        config_changes: Optional[dict[str, Any]] = None,
        consensus_verdicts: Optional[list[dict[str, Any]]] = None,
        provenance_entry: Optional[dict[str, Any]] = None,
    ) -> None:
        """Ingests completed milestone metadata into the architectural graph.

        Creates:
          - MILESTONE node
          - ARCHITECTURE_PATTERN nodes + INSTANTIATES edges
          - SUBSYSTEM_MODULE nodes + TOUCHED edges
          - CONFIG_PARAMETER nodes + MODIFIED edges
          - CONSENSUS_CONFLICT nodes + TRIGGERED edges from milestone + CORRELATED_WITH edges to config
          - PROVENANCE_RECORD node + INSTANTIATES edge
        """
        ms_meta = dict(milestone_metadata or {})
        prov_data = dict(provenance_entry or {})

        # Determine success status from metadata or provenance
        is_success = True
        if "success" in ms_meta:
            is_success = bool(ms_meta["success"])
        elif "success" in prov_data:
            is_success = bool(prov_data["success"])
        elif "outcome" in prov_data:
            is_success = (str(prov_data["outcome"]).upper() == "SUCCESS")

        ms_meta["success"] = is_success

        # 1. Milestone Node
        ms_node = KnowledgeNode(
            node_id=milestone_id,
            node_type=NodeType.MILESTONE,
            attributes=ms_meta,
        )
        self.add_node(ms_node)

        with self._lock:
            if milestone_id not in self._milestone_timeline:
                self._milestone_timeline.append(milestone_id)

        # 2. Architecture Patterns (INSTANTIATES)
        for pat in (patterns or []):
            pat_name = str(pat).strip()
            if not pat_name:
                continue
            pat_id = f"pattern:{pat_name}"
            pat_node = KnowledgeNode(
                node_id=pat_id,
                node_type=NodeType.ARCHITECTURE_PATTERN,
                attributes={"name": pat_name},
            )
            self.add_node(pat_node)
            self.add_edge(KnowledgeEdge(
                source_id=milestone_id,
                target_id=pat_id,
                edge_type=EdgeType.INSTANTIATES,
                weight=1.0,
            ))

        # 3. Touched Files / Subsystem Modules (TOUCHED)
        for filepath in (touched_files or []):
            clean_path = str(filepath).strip()
            if not clean_path:
                continue
            mod_id = f"module:{clean_path}"
            mod_node = KnowledgeNode(
                node_id=mod_id,
                node_type=NodeType.SUBSYSTEM_MODULE,
                attributes={"path": clean_path, "filename": os.path.basename(clean_path)},
            )
            self.add_node(mod_node)
            self.add_edge(KnowledgeEdge(
                source_id=milestone_id,
                target_id=mod_id,
                edge_type=EdgeType.TOUCHED,
                weight=1.0,
            ))

        # 4. Config Parameter Changes (MODIFIED)
        for param_key, change_info in (config_changes or {}).items():
            param_str = str(param_key).strip()
            if not param_str:
                continue
            cfg_id = f"config:{param_str}"
            cfg_node = KnowledgeNode(
                node_id=cfg_id,
                node_type=NodeType.CONFIG_PARAMETER,
                attributes={"param_key": param_str},
            )
            self.add_node(cfg_node)

            # Format edge metadata to preserve delta or value
            meta: dict[str, Any] = {}
            if isinstance(change_info, dict):
                meta = dict(change_info)
                if "delta" not in meta and "old" in meta and "new" in meta:
                    try:
                        meta["delta"] = float(meta["new"]) - float(meta["old"])
                    except (ValueError, TypeError):
                        pass
            elif isinstance(change_info, (int, float)):
                meta = {"value": float(change_info), "delta": float(change_info)}
            else:
                meta = {"value": str(change_info)}

            self.add_edge(KnowledgeEdge(
                source_id=milestone_id,
                target_id=cfg_id,
                edge_type=EdgeType.MODIFIED,
                weight=1.0,
                metadata=meta,
            ))

        # 5. Consensus Verdicts (TRIGGERED & CORRELATED_WITH)
        for idx, verdict in enumerate(consensus_verdicts or []):
            v_dict = dict(verdict)
            conflict_id = v_dict.get("conflict_id") or f"conflict:{milestone_id}:{v_dict.get('parameter_key', idx)}"
            conflict_node = KnowledgeNode(
                node_id=conflict_id,
                node_type=NodeType.CONSENSUS_CONFLICT,
                attributes=v_dict,
            )
            self.add_node(conflict_node)

            severity = v_dict.get("conflict_severity", v_dict.get("severity", "BENIGN"))
            self.add_edge(KnowledgeEdge(
                source_id=milestone_id,
                target_id=conflict_id,
                edge_type=EdgeType.TRIGGERED,
                weight=1.0,
                metadata={"severity": severity},
            ))

            param_key = v_dict.get("parameter_key")
            if param_key:
                cfg_id = f"config:{str(param_key).strip()}"
                self.add_node(KnowledgeNode(
                    node_id=cfg_id,
                    node_type=NodeType.CONFIG_PARAMETER,
                    attributes={"param_key": str(param_key).strip()},
                ))
                self.add_edge(KnowledgeEdge(
                    source_id=conflict_id,
                    target_id=cfg_id,
                    edge_type=EdgeType.CORRELATED_WITH,
                    weight=1.0,
                    metadata=v_dict,
                ))

        # 6. Provenance Record (INSTANTIATES)
        if prov_data:
            prov_id = prov_data.get("record_id") or f"provenance:{milestone_id}"
            prov_node = KnowledgeNode(
                node_id=prov_id,
                node_type=NodeType.PROVENANCE_RECORD,
                attributes=prov_data,
            )
            self.add_node(prov_node)
            self.add_edge(KnowledgeEdge(
                source_id=milestone_id,
                target_id=prov_id,
                edge_type=EdgeType.INSTANTIATES,
                weight=1.0,
                metadata={"provenance_attestation": True},
            ))

    def ingest_failure_postmortem(
        self,
        milestone_id: str,
        anti_patterns: Optional[list[Union[dict[str, Any], str]]] = None,
        broken_assumptions: Optional[list[str]] = None,
    ) -> None:
        """Ingests failure postmortem analysis into the architectural graph.

        Creates:
          - Marks milestone as failed (success=False)
          - ANTI_PATTERN nodes linked to milestone via REVEALED edges
          - Identifies config parameters in broken assumptions and links anti-patterns via CAUSED_BY edges
        """
        # Ensure milestone exists and update its status
        with self._lock:
            if milestone_id in self._nodes:
                self._nodes[milestone_id].attributes["success"] = False
                self._nodes[milestone_id].attributes["postmortem_analyzed"] = True
            else:
                self._nodes[milestone_id] = KnowledgeNode(
                    node_id=milestone_id,
                    node_type=NodeType.MILESTONE,
                    attributes={"success": False, "postmortem_analyzed": True},
                )
                self._outgoing[milestone_id] = []
                self._incoming[milestone_id] = []
            if milestone_id not in self._milestone_timeline:
                self._milestone_timeline.append(milestone_id)

        # Collect candidate config parameter keys currently in the graph
        known_config_params: dict[str, str] = {}
        with self._lock:
            for nid, node in self._nodes.items():
                if node.node_type == NodeType.CONFIG_PARAMETER:
                    key = node.attributes.get("param_key") or nid.replace("config:", "")
                    known_config_params[key.lower()] = nid

        # Ingest anti-patterns
        created_ap_ids: list[str] = []
        for item in (anti_patterns or []):
            if isinstance(item, dict):
                ap_name = str(item.get("name") or item.get("anti_pattern_id") or "unspecified_antipattern").strip()
                attrs = dict(item)
            else:
                ap_name = str(item).strip()
                attrs = {"name": ap_name}

            ap_id = f"antipattern:{ap_name}"
            ap_node = KnowledgeNode(
                node_id=ap_id,
                node_type=NodeType.ANTI_PATTERN,
                attributes=attrs,
            )
            self.add_node(ap_node)
            self.add_edge(KnowledgeEdge(
                source_id=milestone_id,
                target_id=ap_id,
                edge_type=EdgeType.REVEALED,
                weight=1.0,
            ))
            created_ap_ids.append(ap_id)

        # Link anti-patterns to config parameters referenced in broken assumptions (CAUSED_BY)
        for assumption in (broken_assumptions or []):
            assumption_str = str(assumption).strip()
            if not assumption_str:
                continue

            # Detect referenced config parameters
            matched_config_nodes: set[str] = set()

            # Check known parameters
            assumption_lower = assumption_str.lower()
            for key_lower, cfg_node_id in known_config_params.items():
                if key_lower in assumption_lower:
                    matched_config_nodes.add(cfg_node_id)

            # Also scan for regex tokens like `config:param_name` or isolated parameter names
            token_matches = re.findall(r"(?:config[:/]|parameter[:/])?([a-zA-Z0-9_]+)", assumption_str)
            for token in token_matches:
                t_lower = token.lower()
                if t_lower in known_config_params:
                    matched_config_nodes.add(known_config_params[t_lower])

            # For each matched config node, link every anti-pattern revealed in this postmortem
            for cfg_id in matched_config_nodes:
                for ap_id in created_ap_ids:
                    self.add_edge(KnowledgeEdge(
                        source_id=ap_id,
                        target_id=cfg_id,
                        edge_type=EdgeType.CAUSED_BY,
                        weight=1.0,
                        metadata={"broken_assumption": assumption_str},
                    ))

    # --------------------------------------------------------------------------
    # Graph Query Capabilities
    # --------------------------------------------------------------------------

    def query_pattern_statistics(self) -> dict[str, dict[str, Any]]:
        """Computes comprehensive topological statistics for all architecture patterns.

        For each pattern:
          - instance_count: Total milestone instantiations
          - avg_milestone_success_rate: Fraction of instantiating milestones that succeeded
          - most_common_co_patterns: Jaccard similarity with co-occurring patterns
          - associated_anti_patterns: Anti-patterns revealed in milestones sharing this pattern

        Returns:
            Dictionary mapping pattern names to their statistical metrics.
        """
        with self._lock:
            pattern_nodes = [
                n for n in self._nodes.values()
                if n.node_type == NodeType.ARCHITECTURE_PATTERN
            ]

            # Map pattern_id -> set of milestone_ids instantiating it
            pattern_milestones: dict[str, set[str]] = {}
            for pnode in pattern_nodes:
                ms_ids = {
                    e.source_id for e in self._incoming.get(pnode.node_id, [])
                    if e.edge_type == EdgeType.INSTANTIATES and
                    self._nodes.get(e.source_id) and
                    self._nodes[e.source_id].node_type == NodeType.MILESTONE
                }
                pattern_milestones[pnode.node_id] = ms_ids

            stats: dict[str, dict[str, Any]] = {}
            for pnode in pattern_nodes:
                pid = pnode.node_id
                p_name = pnode.attributes.get("name") or pid.replace("pattern:", "")
                ms_set = pattern_milestones[pid]
                instance_count = len(ms_set)

                # 1. Milestone Success Rate
                if instance_count > 0:
                    successes = 0
                    for ms_id in ms_set:
                        ms_node = self._nodes.get(ms_id)
                        if ms_node:
                            # Direct milestone success attribute
                            is_succ = ms_node.attributes.get("success")
                            if is_succ is None:
                                # Inspect linked provenance node
                                prov_edges = [
                                    e for e in self._outgoing.get(ms_id, [])
                                    if self._nodes.get(e.target_id) and
                                    self._nodes[e.target_id].node_type == NodeType.PROVENANCE_RECORD
                                ]
                                if prov_edges:
                                    prov_node = self._nodes[prov_edges[0].target_id]
                                    is_succ = prov_node.attributes.get("success")
                                    if is_succ is None:
                                        is_succ = (str(prov_node.attributes.get("outcome", "")).upper() == "SUCCESS")
                            if is_succ is True or is_succ is None:
                                successes += 1
                    avg_success_rate = round(successes / instance_count, 4)
                else:
                    avg_success_rate = 0.0

                # 2. Co-Pattern Jaccard Similarity
                co_patterns: dict[str, float] = {}
                for other_node in pattern_nodes:
                    other_id = other_node.node_id
                    if other_id == pid:
                        continue
                    other_name = other_node.attributes.get("name") or other_id.replace("pattern:", "")
                    other_set = pattern_milestones[other_id]
                    jaccard = compute_jaccard_similarity(ms_set, other_set)
                    if jaccard > 0.0:
                        co_patterns[other_name] = round(jaccard, 4)

                sorted_co_patterns = dict(sorted(co_patterns.items(), key=lambda kv: kv[1], reverse=True))

                # 3. Associated Anti-Patterns
                associated_aps: set[str] = set()
                for ms_id in ms_set:
                    for edge in self._outgoing.get(ms_id, []):
                        if edge.edge_type == EdgeType.REVEALED:
                            target = self._nodes.get(edge.target_id)
                            if target and target.node_type == NodeType.ANTI_PATTERN:
                                ap_name = target.attributes.get("name") or target.node_id.replace("antipattern:", "")
                                associated_aps.add(ap_name)

                stats[p_name] = {
                    "pattern_id": pid,
                    "pattern_name": p_name,
                    "instance_count": instance_count,
                    "avg_milestone_success_rate": avg_success_rate,
                    "most_common_co_patterns": sorted_co_patterns,
                    "associated_anti_patterns": sorted(list(associated_aps)),
                }

            return stats

    def query_coupling_matrix(self) -> dict[tuple[str, str], float]:
        """Computes pairwise coupling coefficients between subsystem modules.

        Coupling coefficient is defined as the Jaccard similarity of milestone touches:
            Coupling(M_A, M_B) = |Touches(M_A) ∩ Touches(M_B)| / |Touches(M_A) ∪ Touches(M_B)|

        Returns:
            Dictionary mapping sorted (module_a, module_b) tuples to their coupling coefficient.
        """
        with self._lock:
            module_nodes = [
                n for n in self._nodes.values()
                if n.node_type == NodeType.SUBSYSTEM_MODULE
            ]

            # Map module_path -> set of milestone_ids that touched it
            module_milestones: dict[str, set[str]] = {}
            for mod in module_nodes:
                ms_ids = {
                    e.source_id for e in self._incoming.get(mod.node_id, [])
                    if e.edge_type == EdgeType.TOUCHED and
                    self._nodes.get(e.source_id) and
                    self._nodes[e.source_id].node_type == NodeType.MILESTONE
                }
                mod_name = mod.attributes.get("path") or mod.attributes.get("filename") or mod.node_id.replace("module:", "")
                module_milestones[mod_name] = ms_ids

            sorted_modules = sorted(module_milestones.keys())
            coupling_matrix: dict[tuple[str, str], float] = {}

            for i in range(len(sorted_modules)):
                for j in range(i + 1, len(sorted_modules)):
                    mod_a = sorted_modules[i]
                    mod_b = sorted_modules[j]
                    coeff = compute_jaccard_similarity(module_milestones[mod_a], module_milestones[mod_b])
                    coupling_matrix[(mod_a, mod_b)] = round(coeff, 4)

            return coupling_matrix

    def query_config_impact_correlation(self, param_key: str) -> dict[str, Any]:
        """Correlates parameter modifications with downstream operational metrics.

        Computes Pearson correlation r across the chronological version timeline between:
          1. Parameter value change / delta (X)
          2. Downstream milestone health score delta (Y_health)
          3. Downstream consensus conflict frequency (Y_conflicts)
          4. Downstream anti-pattern occurrence rate (Y_antipatterns)

        Args:
            param_key: Target configuration parameter name.

        Returns:
            Dictionary containing Pearson coefficients and chronological timeline data points.
        """
        with self._lock:
            clean_key = param_key.replace("config:", "").strip()
            cfg_id = f"config:{clean_key}"

            if cfg_id not in self._nodes and clean_key not in self._nodes:
                return {
                    "param_key": clean_key,
                    "sample_size": 0,
                    "health_score_delta_correlation": 0.0,
                    "consensus_conflict_correlation": 0.0,
                    "anti_pattern_correlation": 0.0,
                    "timeline_data_points": [],
                }

            active_cfg_id = cfg_id if cfg_id in self._nodes else clean_key

            # Find all milestones that modified this parameter
            modifying_edges = [
                e for e in self._incoming.get(active_cfg_id, [])
                if e.edge_type == EdgeType.MODIFIED
            ]

            # Map milestone_id -> edge metadata
            milestone_deltas: dict[str, float] = {}
            for edge in modifying_edges:
                meta = edge.metadata
                delta_val = meta.get("delta")
                if delta_val is not None:
                    try:
                        milestone_deltas[edge.source_id] = float(delta_val)
                        continue
                    except (ValueError, TypeError):
                        pass
                if "new" in meta and "old" in meta:
                    try:
                        milestone_deltas[edge.source_id] = float(meta["new"]) - float(meta["old"])
                        continue
                    except (ValueError, TypeError):
                        pass
                if "value" in meta:
                    try:
                        milestone_deltas[edge.source_id] = float(meta["value"])
                        continue
                    except (ValueError, TypeError):
                        pass
                milestone_deltas[edge.source_id] = 1.0

            # Traverse milestones in chronological sequence
            ordered_milestones = [
                ms_id for ms_id in self._milestone_timeline
                if ms_id in milestone_deltas
            ]
            # Fallback if timeline didn't capture all
            for ms_id in milestone_deltas:
                if ms_id not in ordered_milestones:
                    ordered_milestones.append(ms_id)

            x_deltas: list[float] = []
            y_health: list[float] = []
            y_conflicts: list[float] = []
            y_antipatterns: list[float] = []
            data_points: list[dict[str, Any]] = []

            for ms_id in ordered_milestones:
                x_val = milestone_deltas[ms_id]
                ms_node = self._nodes.get(ms_id)
                attrs = ms_node.attributes if ms_node else {}

                # 1. Health score delta
                h_delta = attrs.get("health_score_delta")
                if h_delta is None:
                    h_delta = attrs.get("health_delta")
                if h_delta is None:
                    # Heuristic fallback based on outcome
                    is_succ = attrs.get("success", True)
                    h_delta = 0.05 if is_succ else -0.25
                h_delta_val = float(h_delta)

                # 2. Consensus conflict frequency
                conflict_edges = [
                    e for e in self._outgoing.get(ms_id, [])
                    if e.edge_type == EdgeType.TRIGGERED
                ]
                conflict_count = float(len(conflict_edges))

                # 3. Anti-pattern occurrence count
                ap_edges = [
                    e for e in self._outgoing.get(ms_id, [])
                    if e.edge_type == EdgeType.REVEALED
                ]
                ap_count = float(len(ap_edges))

                x_deltas.append(x_val)
                y_health.append(h_delta_val)
                y_conflicts.append(conflict_count)
                y_antipatterns.append(ap_count)

                data_points.append({
                    "milestone_id": ms_id,
                    "param_delta": x_val,
                    "health_score_delta": h_delta_val,
                    "conflict_count": conflict_count,
                    "anti_pattern_count": ap_count,
                })

            sample_size = len(x_deltas)
            corr_health = compute_pearson_correlation(x_deltas, y_health)
            corr_conflicts = compute_pearson_correlation(x_deltas, y_conflicts)
            corr_antipatterns = compute_pearson_correlation(x_deltas, y_antipatterns)

            return {
                "param_key": clean_key,
                "sample_size": sample_size,
                "health_score_delta_correlation": round(corr_health, 4),
                "consensus_conflict_correlation": round(corr_conflicts, 4),
                "anti_pattern_correlation": round(corr_antipatterns, 4),
                "timeline_data_points": data_points,
            }

    def query_causal_chain(self, source_id: str, max_depth: int = 5) -> list[list[KnowledgeEdge]]:
        """Performs BFS graph traversal returning all directed paths originating at source_id.

        Enables provenance chain reconstruction, tracing downstream impacts from milestones
        through conflicts, anti-patterns, and configuration parameters up to max_depth hops.

        Args:
            source_id: Node ID where traversal originates.
            max_depth: Maximum path length in edge hops (default 5).

        Returns:
            List of edge paths (each path is a list of KnowledgeEdge instances).
        """
        with self._lock:
            if source_id not in self._nodes:
                return []

            paths: list[list[KnowledgeEdge]] = []
            # Queue stores tuples of (current_node_id, current_path_of_edges)
            queue: collections.deque[tuple[str, list[KnowledgeEdge]]] = collections.deque([(source_id, [])])

            while queue:
                curr_node_id, current_path = queue.popleft()

                if current_path:
                    paths.append(current_path)

                if len(current_path) >= max_depth:
                    continue

                # Cycle prevention: set of nodes already traversed in this branch
                visited_in_branch = {source_id} | {e.target_id for e in current_path}

                for edge in self._outgoing.get(curr_node_id, []):
                    if edge.target_id not in visited_in_branch:
                        queue.append((edge.target_id, current_path + [edge]))

            return paths

    def get_graph_summary(self) -> dict[str, Any]:
        """Computes topological summary metrics and density for the knowledge graph."""
        with self._lock:
            total_nodes = len(self._nodes)
            total_edges = sum(len(edges) for edges in self._outgoing.values())

            # Counts by NodeType
            node_counts = {nt.value: 0 for nt in NodeType}
            for n in self._nodes.values():
                node_counts[n.node_type.value] += 1

            # Counts by EdgeType
            edge_counts = {et.value: 0 for et in EdgeType}
            for edge_list in self._outgoing.values():
                for e in edge_list:
                    edge_counts[e.edge_type.value] += 1

            # Degree distribution
            node_degrees: dict[str, dict[str, int]] = {}
            for nid in self._nodes:
                in_deg = len(self._incoming.get(nid, []))
                out_deg = len(self._outgoing.get(nid, []))
                node_degrees[nid] = {
                    "in_degree": in_deg,
                    "out_degree": out_deg,
                    "total_degree": in_deg + out_deg,
                }

            # Top-5 highest-degree nodes per NodeType
            top_nodes_per_type: dict[str, list[dict[str, Any]]] = {}
            for nt in NodeType:
                matching_nodes = [n for n in self._nodes.values() if n.node_type == nt]
                matching_nodes.sort(
                    key=lambda n: node_degrees[n.node_id]["total_degree"],
                    reverse=True,
                )
                top_5 = []
                for n in matching_nodes[:5]:
                    deg = node_degrees[n.node_id]
                    top_5.append({
                        "node_id": n.node_id,
                        "total_degree": deg["total_degree"],
                        "in_degree": deg["in_degree"],
                        "out_degree": deg["out_degree"],
                        "attributes": n.attributes,
                    })
                top_nodes_per_type[nt.value] = top_5

            # Graph density for directed graph: |E| / (|V| * (|V| - 1))
            if total_nodes > 1:
                density = total_edges / (total_nodes * (total_nodes - 1))
            else:
                density = 0.0

            return {
                "total_nodes": total_nodes,
                "total_edges": total_edges,
                "density": round(density, 6),
                "node_counts_by_type": node_counts,
                "edge_counts_by_type": edge_counts,
                "top_degree_nodes_per_type": top_nodes_per_type,
            }

    # --------------------------------------------------------------------------
    # Persistence & Serialization
    # --------------------------------------------------------------------------

    def persist(self, path: Optional[str] = None) -> str:
        """Serializes knowledge graph to a JSON adjacency list atomically.

        Uses tempfile and os.replace to guarantee atomic, non-corrupting disk writes.
        """
        target_path = path or self.persistence_path or "logs/knowledge_graph.json"
        abs_path = os.path.abspath(target_path)
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)

        with self._lock:
            all_edges: list[dict[str, Any]] = []
            for edge_list in self._outgoing.values():
                for edge in edge_list:
                    all_edges.append(edge.to_dict())

            payload = {
                "version": 1,
                "schema": "ArchitecturalKnowledgeGraph_v1",
                "saved_at": time.time(),
                "milestone_timeline": list(self._milestone_timeline),
                "nodes": [n.to_dict() for n in self._nodes.values()],
                "edges": all_edges,
            }

            temp_dir = os.path.dirname(abs_path)
            with tempfile.NamedTemporaryFile("w", dir=temp_dir, delete=False, encoding="utf-8") as tf:
                json.dump(payload, tf, indent=2)
                tf.flush()
                os.fsync(tf.fileno())
                temp_name = tf.name

            os.replace(temp_name, abs_path)
            return abs_path

    def load(self, path: Optional[str] = None) -> None:
        """Deserializes knowledge graph from JSON file and reconstructs index structures."""
        target_path = path or self.persistence_path or "logs/knowledge_graph.json"
        abs_path = os.path.abspath(target_path)
        if not os.path.exists(abs_path):
            raise FileNotFoundError(f"Knowledge graph file not found at {abs_path}")

        with self._lock:
            with open(abs_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            self._nodes.clear()
            self._outgoing.clear()
            self._incoming.clear()
            self._milestone_timeline = list(data.get("milestone_timeline", []))

            # Reconstruct nodes
            for node_dict in data.get("nodes", []):
                node = KnowledgeNode.from_dict(node_dict)
                self._nodes[node.node_id] = node
                self._outgoing[node.node_id] = []
                self._incoming[node.node_id] = []

            # Reconstruct edges
            for edge_dict in data.get("edges", []):
                edge = KnowledgeEdge.from_dict(edge_dict)
                if edge.source_id not in self._outgoing:
                    self._outgoing[edge.source_id] = []
                if edge.target_id not in self._incoming:
                    self._incoming[edge.target_id] = []
                self._outgoing[edge.source_id].append(edge)
                self._incoming[edge.target_id].append(edge)


# ==============================================================================
# Comprehensive Self-Test & Verification Suite
# ==============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("  ARCHITECTURAL KNOWLEDGE GRAPH & PROVENANCE MAPPER VERIFICATION SUITE")
    print("=" * 80)

    graph = ArchitecturalKnowledgeGraph(persistence_path="logs/test_knowledge_graph.json")

    # 1. Ingest 5+ Synthetic Milestone Completions
    print("\n[+] Ingesting 5 Synthetic Milestone Completions...")

    ms1_data = {
        "milestone_id": "ms_001_actor_pods",
        "milestone_metadata": {"title": "Decentralized Actor Pods", "health_score_delta": 0.08, "success": True},
        "patterns": ["ActorPod", "EventBus"],
        "touched_files": ["tasks/actor_pod.py", "tasks/event_bus.py"],
        "config_changes": {"governor_dispatch_rate": {"old": 1.0, "new": 1.5, "delta": 0.5}},
        "consensus_verdicts": [{
            "conflict_id": "conflict_001",
            "parameter_key": "governor_dispatch_rate",
            "conflict_severity": "BENIGN",
            "confidence": 0.95,
        }],
        "provenance_entry": {
            "record_id": "prov_001",
            "milestone_id": "ms_001_actor_pods",
            "success": True,
            "outcome": "SUCCESS",
        },
    }
    graph.ingest_milestone_completion(**ms1_data)

    ms2_data = {
        "milestone_id": "ms_002_consensus_quorum",
        "milestone_metadata": {"title": "Quorum Consensus Engine", "health_score_delta": 0.04, "success": True},
        "patterns": ["QuorumConsensus", "EventBus"],
        "touched_files": ["tasks/pipeline_consensus_engine.py", "tasks/event_bus.py"],
        "config_changes": {"governor_dispatch_rate": {"old": 1.5, "new": 2.0, "delta": 0.5}},
        "consensus_verdicts": [{
            "conflict_id": "conflict_002",
            "parameter_key": "governor_dispatch_rate",
            "conflict_severity": "CONTESTED",
            "confidence": 0.80,
        }],
        "provenance_entry": {
            "record_id": "prov_002",
            "milestone_id": "ms_002_consensus_quorum",
            "success": True,
            "outcome": "SUCCESS",
        },
    }
    graph.ingest_milestone_completion(**ms2_data)

    ms3_data = {
        "milestone_id": "ms_003_config_registry",
        "milestone_metadata": {"title": "Config Schema Registry", "health_score_delta": -0.02, "success": True},
        "patterns": ["SchemaRegistry", "ActorPod"],
        "touched_files": ["tasks/pipeline_config_registry.py", "tasks/actor_pod.py"],
        "config_changes": {"governor_dispatch_rate": {"old": 2.0, "new": 3.0, "delta": 1.0}},
        "consensus_verdicts": [{
            "conflict_id": "conflict_003",
            "parameter_key": "governor_dispatch_rate",
            "conflict_severity": "ADVERSARIAL",
            "confidence": 0.65,
        }],
        "provenance_entry": {
            "record_id": "prov_003",
            "milestone_id": "ms_003_config_registry",
            "success": True,
            "outcome": "SUCCESS",
        },
    }
    graph.ingest_milestone_completion(**ms3_data)

    ms4_data = {
        "milestone_id": "ms_004_telemetry_stream",
        "milestone_metadata": {"title": "Telemetry Anomaly Stream", "health_score_delta": -0.15, "success": True},
        "patterns": ["ReactiveStream", "ActorPod"],
        "touched_files": ["tasks/telemetry_anomaly_engine.py", "tasks/actor_pod.py"],
        "config_changes": {"governor_dispatch_rate": {"old": 3.0, "new": 4.5, "delta": 1.5}},
        "consensus_verdicts": [{
            "conflict_id": "conflict_004",
            "parameter_key": "governor_dispatch_rate",
            "conflict_severity": "ADVERSARIAL",
            "confidence": 0.40,
        }],
        "provenance_entry": {
            "record_id": "prov_004",
            "milestone_id": "ms_004_telemetry_stream",
            "success": True,
            "outcome": "SUCCESS",
        },
    }
    graph.ingest_milestone_completion(**ms4_data)

    ms5_data = {
        "milestone_id": "ms_005_provenance_ledger",
        "milestone_metadata": {"title": "Provenance Merkle Ledger", "health_score_delta": -0.32, "success": False},
        "patterns": ["MerkleAuditLedger", "SchemaRegistry"],
        "touched_files": ["tasks/provenance_ledger.py", "tasks/pipeline_config_registry.py"],
        "config_changes": {"governor_dispatch_rate": {"old": 4.5, "new": 6.5, "delta": 2.0}},
        "consensus_verdicts": [{
            "conflict_id": "conflict_005",
            "parameter_key": "governor_dispatch_rate",
            "conflict_severity": "ADVERSARIAL",
            "confidence": 0.30,
        }],
        "provenance_entry": {
            "record_id": "prov_005",
            "milestone_id": "ms_005_provenance_ledger",
            "success": False,
            "outcome": "FAILURE",
        },
    }
    graph.ingest_milestone_completion(**ms5_data)
    print("  • 5 milestone completions successfully ingested.")

    # 2. Ingest 2+ Failure Postmortems
    print("\n[+] Ingesting 2 Failure Postmortems...")
    graph.ingest_failure_postmortem(
        milestone_id="ms_004_telemetry_stream",
        anti_patterns=[{"name": "UnboundedQueueCongestion", "severity": "HIGH", "category": "concurrency"}],
        broken_assumptions=["governor_dispatch_rate was assumed bounded within buffer limits"],
    )

    graph.ingest_failure_postmortem(
        milestone_id="ms_005_provenance_ledger",
        anti_patterns=[{"name": "CascadingDeadlock", "severity": "CRITICAL", "category": "distributed_lock"}],
        broken_assumptions=["governor_dispatch_rate exceeded capacity threshold causing thread exhaustion"],
    )
    print("  • 2 postmortems successfully ingested with anti-patterns and caused-by edges.")

    # 3. Query Pattern Statistics
    print("\n[+] Querying Pattern Statistics (Instance Count, Success Rate, Jaccard Co-Patterns)...")
    pat_stats = graph.query_pattern_statistics()
    assert "ActorPod" in pat_stats, "ActorPod pattern must be present in stats"
    assert "EventBus" in pat_stats, "EventBus pattern must be present in stats"
    for p_name, p_info in pat_stats.items():
        print(f"  • {p_name:18} | Count: {p_info['instance_count']} | Success: {p_info['avg_milestone_success_rate']:.1%} | Co-Patterns: {p_info['most_common_co_patterns']} | Anti-Patterns: {p_info['associated_anti_patterns']}")

    # 4. Query Coupling Matrix
    print("\n[+] Querying Subsystem Coupling Matrix (Jaccard Overlap)...")
    coupling = graph.query_coupling_matrix()
    assert len(coupling) > 0, "Coupling matrix should contain pairwise coefficients"
    for (mod_a, mod_b), coeff in sorted(coupling.items(), key=lambda kv: kv[1], reverse=True)[:5]:
        print(f"  • {os.path.basename(mod_a)} <-> {os.path.basename(mod_b)}: Jaccard = {coeff:.4f}")

    # 5. Query Config Impact Correlation
    print("\n[+] Querying Config Impact Correlation for 'governor_dispatch_rate'...")
    corr_info = graph.query_config_impact_correlation("governor_dispatch_rate")
    print(f"  • Sample Size:                 {corr_info['sample_size']}")
    print(f"  • Health Score Delta Corr:     {corr_info['health_score_delta_correlation']:+.4f}")
    print(f"  • Consensus Conflict Corr:     {corr_info['consensus_conflict_correlation']:+.4f}")
    print(f"  • Anti-Pattern Occurrence Corr:{corr_info['anti_pattern_correlation']:+.4f}")
    assert corr_info["health_score_delta_correlation"] < 0, "Increasing delta should correlate negatively with health score"

    # 6. Trace Causal Chains
    print("\n[+] Tracing Causal Chains from ms_004_telemetry_stream (max_depth=3)...")
    chains = graph.query_causal_chain("ms_004_telemetry_stream", max_depth=3)
    assert len(chains) > 0, "Causal chains must not be empty"
    print(f"  • Found {len(chains)} directed paths originating from ms_004_telemetry_stream:")
    for path in chains[:6]:
        edge_repr = " -> ".join([f"{e.source_id} -[{e.edge_type.value}]-> {e.target_id}" for e in path])
        print(f"    - {edge_repr}")

    # 7. Get Graph Summary
    print("\n[+] Computing Topological Graph Summary...")
    summary = graph.get_graph_summary()
    print(f"  • Total Nodes:   {summary['total_nodes']}")
    print(f"  • Total Edges:   {summary['total_edges']}")
    print(f"  • Graph Density: {summary['density']:.6f}")
    print(f"  • Nodes by Type: {summary['node_counts_by_type']}")
    print(f"  • Edges by Type: {summary['edge_counts_by_type']}")
    assert summary["total_nodes"] > 10, "Summary must reflect all created nodes"
    assert summary["total_edges"] > 10, "Summary must reflect all created edges"

    # 8. Persist and Reload Graph
    test_json = "logs/test_knowledge_graph.json"
    print(f"\n[+] Testing Atomic Persistence & Reload ({test_json})...")
    saved_path = graph.persist(test_json)
    assert os.path.exists(saved_path), "Persisted JSON file must exist"

    reloaded_graph = ArchitecturalKnowledgeGraph()
    reloaded_graph.load(saved_path)
    reloaded_summary = reloaded_graph.get_graph_summary()
    assert reloaded_summary["total_nodes"] == summary["total_nodes"], "Reloaded node count must match"
    assert reloaded_summary["total_edges"] == summary["total_edges"], "Reloaded edge count must match"
    print(f"  • Persistence round-trip confirmed: {reloaded_summary['total_nodes']} nodes, {reloaded_summary['total_edges']} edges match perfectly.")

    # Clean up test artifact
    if os.path.exists(test_json):
        os.remove(test_json)

    print("\n" + "=" * 80)
    print("  ALL VERIFICATION TESTS COMPLETED SUCCESSFULLY WITH ZERO ERRORS.")
    print("=" * 80)
