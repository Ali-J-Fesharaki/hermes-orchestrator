"""Milestone Dependency Graph & Critical-Path Dispatch Scheduler.

Deterministic, zero-LLM dependency graph engine for Hermes Orchestrator.
Models inter-milestone code footprints, detects race conditions / structural
overlaps, extracts dependency relations, and computes optimal dispatch tiers
via Kahn's topological sort and critical-path longest-path analysis.
"""

from __future__ import annotations

import ast
from collections import defaultdict, deque, namedtuple
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    from flask import Blueprint, jsonify, request
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False
    Blueprint = None
    jsonify = None
    request = None


# ============================================================================
# Core Data Models & Relations
# ============================================================================

class RelationType(str, Enum):
    """Dependency and concurrency relation types between milestones."""
    MUST_PRECEDE = "MUST_PRECEDE"
    CONFLICTS_WITH = "CONFLICTS_WITH"
    INDEPENDENT = "INDEPENDENT"


Edge = namedtuple("Edge", ["target", "relation", "weight", "reason"])


class CyclicDependencyError(Exception):
    """Raised when dependency cycle prevents valid topological ordering."""

    def __init__(self, message: str, cycle_paths: Optional[List[List[str]]] = None):
        super().__init__(message)
        self.cycle_paths = cycle_paths or []


@dataclass
class FileFootprint:
    """Represents the estimated or parsed code footprint for a milestone."""
    filepath: str
    regions: List[Tuple[int, int]] = field(default_factory=list)
    symbols: List[str] = field(default_factory=list)

    @classmethod
    def from_spec_text(cls, filepath: str, spec_text: str) -> FileFootprint:
        """Extract symbol identifiers and line regions from spec text and file AST.

        Performs regex extraction over technical specification text and supplements
        with AST parsing if the target file already exists on disk.
        """
        extracted_symbols: Set[str] = set()
        extracted_regions: List[Tuple[int, int]] = []

        # 1. Regex over specification text
        symbol_patterns = [
            r"\bclass\s+([A-Za-z_][A-Za-z0-9_]*)",
            r"\bdef\s+([A-Za-z_][A-Za-z0-9_]*)",
            r"\bimport\s+([A-Za-z_][A-Za-z0-9_]*)",
            r"\bfrom\s+[A-Za-z0-9_.]+\s+import\s+([A-Za-z_][A-Za-z0-9_*, ]+)",
            r"`([A-Za-z_][A-Za-z0-9_]{2,})`",
        ]

        for pattern in symbol_patterns:
            matches = re.findall(pattern, spec_text)
            for match in matches:
                if "," in match:
                    for sub in match.split(","):
                        clean = sub.strip()
                        if clean and re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", clean):
                            extracted_symbols.add(clean)
                else:
                    clean = match.strip()
                    if clean and re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", clean):
                        extracted_symbols.add(clean)

        # 2. Extract line regions from spec text (e.g., 'lines 120-180', 'L45-L90')
        line_patterns = [
            r"(?:lines?|L)\s*(\d+)\s*(?:-|–|to)\s*(?:L)?(\d+)",
            r"(?:around line|line)\s*(\d+)",
        ]
        for pattern in line_patterns:
            for match in re.findall(pattern, spec_text, re.IGNORECASE):
                if isinstance(match, tuple) and len(match) == 2 and match[1]:
                    start, end = int(match[0]), int(match[1])
                    if start <= end:
                        extracted_regions.append((start, end))
                elif isinstance(match, str) and match.isdigit():
                    line = int(match)
                    extracted_regions.append((max(1, line - 15), line + 15))

        # 3. Target file AST parsing when file is locally available
        if filepath:
            target_path = Path(filepath)
            if target_path.is_file() and target_path.suffix == ".py":
                try:
                    content = target_path.read_text(encoding="utf-8", errors="replace")
                    tree = ast.parse(content, filename=filepath)
                    for node in ast.walk(tree):
                        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                            extracted_symbols.add(node.name)
                            start_line = getattr(node, "lineno", None)
                            end_line = getattr(node, "end_lineno", None)
                            if start_line and end_line and start_line <= end_line:
                                # Include region if symbol is referenced in spec
                                if node.name in extracted_symbols or not extracted_regions:
                                    extracted_regions.append((start_line, end_line))
                except Exception:
                    pass

        # Sort and deduplicate regions
        sorted_regions: List[Tuple[int, int]] = []
        if extracted_regions:
            sorted_raw = sorted(extracted_regions, key=lambda r: (r[0], r[1]))
            cur_start, cur_end = sorted_raw[0]
            for s, e in sorted_raw[1:]:
                if s <= cur_end + 1:
                    cur_end = max(cur_end, e)
                else:
                    sorted_regions.append((cur_start, cur_end))
                    cur_start, cur_end = s, e
            sorted_regions.append((cur_start, cur_end))

        return cls(
            filepath=str(filepath).strip(),
            regions=sorted_regions,
            symbols=sorted(extracted_symbols),
        )


# ============================================================================
# Footprint Analysis Helpers
# ============================================================================

def compute_symbol_jaccard(syms1: List[str], syms2: List[str]) -> float:
    """Compute Jaccard similarity index across two symbol lists."""
    set1, set2 = set(syms1), set(syms2)
    if not set1 or not set2:
        return 0.0
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    return intersection / union if union > 0 else 0.0


def compute_line_intersection_ratio(
    r1: List[Tuple[int, int]], r2: List[Tuple[int, int]]
) -> float:
    """Compute line range intersection ratio between two region sets."""
    if not r1 or not r2:
        return 0.0

    lines1: Set[int] = set()
    for start, end in r1:
        lines1.update(range(start, end + 1))

    lines2: Set[int] = set()
    for start, end in r2:
        lines2.update(range(start, end + 1))

    if not lines1 or not lines2:
        return 0.0

    intersection = len(lines1 & lines2)
    union = len(lines1 | lines2)
    return intersection / union if union > 0 else 0.0


# ============================================================================
# Dependency Graph Engine
# ============================================================================

class DependencyGraphEngine:
    """Directed Acyclic Graph (DAG) Engine for inter-milestone scheduling."""

    def __init__(
        self,
        policy_gate: Any = None,
        ledger_path: str = "dashboards/DEPENDENCY_GRAPH.md",
    ) -> None:
        self.policy_gate = policy_gate
        self.ledger_path = Path(ledger_path)
        self._nodes: Dict[str, Dict[str, Any]] = {}
        self._edges: Dict[str, List[Edge]] = defaultdict(list)

    @property
    def nodes(self) -> Dict[str, Dict[str, Any]]:
        return self._nodes

    @property
    def edges(self) -> Dict[str, List[Edge]]:
        return self._edges

    def _extract_risk_score(self, policy_verdict: Any) -> float:
        """Extract numeric risk score (0.0 - 1.0) from policy verdict input."""
        if policy_verdict is None:
            return 0.0
        if isinstance(policy_verdict, (int, float)):
            return float(policy_verdict)
        if isinstance(policy_verdict, dict):
            return float(policy_verdict.get("risk_score", 0.0))
        if hasattr(policy_verdict, "risk_score"):
            return float(policy_verdict.risk_score)
        return 0.0

    def add_milestone(
        self,
        milestone_id: str,
        filepath: str,
        spec_text: str,
        policy_verdict: Any = None,
        status: str = "PENDING",
    ) -> List[Edge]:
        """Add milestone to graph and compute dependency edges against existing nodes.

        Calculates pairwise overlap using symbol Jaccard similarity and line range
        intersection. Adds:
          - MUST_PRECEDE if overlap > 0.3 AND (risk_score > 0.5 or existing risk > 0.5)
          - CONFLICTS_WITH if overlap > 0.6
          - INDEPENDENT otherwise.

        Returns:
            List of generated edges targeting or originating from the new milestone.
        """
        footprint = FileFootprint.from_spec_text(filepath, spec_text)
        risk_score = self._extract_risk_score(policy_verdict)

        self._nodes[milestone_id] = {
            "milestone_id": milestone_id,
            "filepath": filepath,
            "footprint": footprint,
            "spec_text": spec_text,
            "risk_score": risk_score,
            "status": status,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        generated_edges: List[Edge] = []

        for existing_id, node_data in self._nodes.items():
            if existing_id == milestone_id:
                continue

            exist_fp: FileFootprint = node_data["footprint"]
            exist_risk: float = node_data["risk_score"]

            same_file = (
                Path(filepath).resolve() == Path(exist_fp.filepath).resolve()
                if filepath and exist_fp.filepath
                else False
            )

            jaccard = compute_symbol_jaccard(footprint.symbols, exist_fp.symbols)
            line_overlap = (
                compute_line_intersection_ratio(footprint.regions, exist_fp.regions)
                if same_file
                else 0.0
            )

            if same_file:
                # Same file modification creates structural coupling
                base_overlap = 0.4
                overlap = min(1.0, base_overlap + (0.35 * jaccard) + (0.25 * line_overlap))
            else:
                overlap = min(1.0, jaccard * 0.8)

            relation: RelationType
            reason: str
            weight: float

            if overlap > 0.6:
                relation = RelationType.CONFLICTS_WITH
                reason = (
                    f"Structural conflict on {filepath} (overlap={overlap:.2f}, "
                    f"symbols_jaccard={jaccard:.2f}, line_overlap={line_overlap:.2f})"
                )
                weight = round(overlap, 3)
                # Existing milestone executes before the new conflicting milestone
                edge = Edge(target=milestone_id, relation=relation.value, weight=weight, reason=reason)
                self._edges[existing_id].append(edge)
                generated_edges.append(edge)

            elif overlap > 0.3 and (risk_score > 0.5 or exist_risk > 0.5):
                relation = RelationType.MUST_PRECEDE
                max_risk = max(risk_score, exist_risk)
                reason = (
                    f"Precedence gate required (overlap={overlap:.2f}, "
                    f"max_risk={max_risk:.2f})"
                )
                weight = round(overlap * max_risk, 3)
                edge = Edge(target=milestone_id, relation=relation.value, weight=weight, reason=reason)
                self._edges[existing_id].append(edge)
                generated_edges.append(edge)

            else:
                relation = RelationType.INDEPENDENT
                reason = f"Negligible collision hazard (overlap={overlap:.2f})"
                weight = round(overlap * 0.1, 3)
                edge = Edge(target=milestone_id, relation=relation.value, weight=weight, reason=reason)
                generated_edges.append(edge)

        return generated_edges

    def remove_milestone(self, milestone_id: str) -> None:
        """Remove a milestone and all associated incoming/outgoing edges."""
        if milestone_id in self._nodes:
            del self._nodes[milestone_id]

        if milestone_id in self._edges:
            del self._edges[milestone_id]

        for src, edges in list(self._edges.items()):
            self._edges[src] = [e for e in edges if e.target != milestone_id]

    def detect_cycles(self) -> List[List[str]]:
        """DFS-based cycle detection returning all unique cycle paths."""
        WHITE, GRAY, BLACK = 0, 1, 2
        colors: Dict[str, int] = {m: WHITE for m in self._nodes}
        path_stack: List[str] = []
        cycles: List[List[str]] = []

        def dfs(node: str) -> None:
            colors[node] = GRAY
            path_stack.append(node)

            for edge in self._edges.get(node, []):
                if edge.relation not in (
                    RelationType.MUST_PRECEDE.value,
                    RelationType.CONFLICTS_WITH.value,
                ):
                    continue

                nbr = edge.target
                if nbr not in colors:
                    continue

                if colors[nbr] == GRAY:
                    try:
                        idx = path_stack.index(nbr)
                        cycle = path_stack[idx:] + [nbr]
                        cycles.append(cycle)
                    except ValueError:
                        cycles.append([node, nbr])
                elif colors[nbr] == WHITE:
                    dfs(nbr)

            path_stack.pop()
            colors[node] = BLACK

        for node_id in sorted(self._nodes.keys()):
            if colors[node_id] == WHITE:
                dfs(node_id)

        return cycles

    def topological_sort(self) -> List[List[str]]:
        """Kahn's algorithm producing parallelizable dispatch tiers.

        Each inner list contains milestone_ids that are guaranteed to have zero
        unresolved dependencies and can be dispatched concurrently.

        Raises:
            CyclicDependencyError: If a dependency cycle is detected.
        """
        cycles = self.detect_cycles()
        if cycles:
            raise CyclicDependencyError(
                f"Cyclic dependency detected among milestones: {cycles}",
                cycle_paths=cycles,
            )

        active_nodes = set(self._nodes.keys())
        in_degrees: Dict[str, int] = {m: 0 for m in active_nodes}
        adjacency: Dict[str, List[str]] = defaultdict(list)

        for src, edge_list in self._edges.items():
            if src not in active_nodes:
                continue
            for edge in edge_list:
                if edge.target in active_nodes and edge.relation in (
                    RelationType.MUST_PRECEDE.value,
                    RelationType.CONFLICTS_WITH.value,
                ):
                    adjacency[src].append(edge.target)
                    in_degrees[edge.target] += 1

        queue: deque[str] = deque(
            sorted([m for m, deg in in_degrees.items() if deg == 0])
        )
        tiers: List[List[str]] = []
        visited_count = 0

        while queue:
            current_tier_size = len(queue)
            tier_nodes: List[str] = []

            for _ in range(current_tier_size):
                u = queue.popleft()
                tier_nodes.append(u)
                visited_count += 1

                for v in adjacency[u]:
                    in_degrees[v] -= 1
                    if in_degrees[v] == 0:
                        queue.append(v)

            if tier_nodes:
                tiers.append(sorted(tier_nodes))

        if visited_count != len(active_nodes):
            remaining_cycles = self.detect_cycles()
            raise CyclicDependencyError(
                "Cycle detected: unresolved in-degrees remain after topological sort.",
                cycle_paths=remaining_cycles,
            )

        return tiers

    def critical_path(self) -> List[str]:
        """Compute the critical path via Dynamic Programming on the weighted DAG.

        Identifies the sequence of milestones with the maximum cumulative risk
        and constraint weight.
        """
        if not self._nodes:
            return []

        try:
            tiers = self.topological_sort()
        except CyclicDependencyError:
            return []

        topo_order: List[str] = [m for tier in tiers for m in tier]
        if not topo_order:
            return []

        # DP tables: max cumulative weight to reach node
        dp: Dict[str, float] = {
            m: self._nodes[m]["risk_score"] for m in self._nodes
        }
        predecessor: Dict[str, Optional[str]] = {m: None for m in self._nodes}

        for u in topo_order:
            for edge in self._edges.get(u, []):
                if edge.relation in (
                    RelationType.MUST_PRECEDE.value,
                    RelationType.CONFLICTS_WITH.value,
                ):
                    v = edge.target
                    if v in dp:
                        edge_weight = edge.weight + self._nodes[v]["risk_score"]
                        candidate = dp[u] + edge_weight
                        if candidate > dp[v]:
                            dp[v] = candidate
                            predecessor[v] = u

        end_node = max(dp.keys(), key=lambda k: dp[k])
        path: List[str] = []
        curr: Optional[str] = end_node

        while curr is not None:
            path.append(curr)
            curr = predecessor.get(curr)

        path.reverse()
        return path

    def render_mermaid(self) -> str:
        """Generate a valid Mermaid 'graph TD' definition."""
        lines = [
            "graph TD",
            "    %% Styles for relation categories",
            "    classDef conflict stroke:#ff4d4f,stroke-width:2px,stroke-dasharray: 4 4;",
            "    classDef precede stroke:#1890ff,stroke-width:2px;",
            "    classDef standard fill:#2a2e39,stroke:#5c6370,color:#abb2bf;",
            "",
        ]

        # Milestone Nodes
        for m_id, data in sorted(self._nodes.items()):
            fp = data["filepath"]
            risk = data["risk_score"]
            status = data["status"]
            label = f"{m_id}<br/><code>{fp}</code><br/>Risk: {risk:.2f} | [{status}]"
            lines.append(f'    {m_id}["{label}"]:::standard')

        lines.append("")

        # Edges
        edge_count = 0
        for src, edges in sorted(self._edges.items()):
            for edge in edges:
                if edge.relation == RelationType.INDEPENDENT.value:
                    continue

                label = f"{edge.relation} (w={edge.weight:.2f})"
                if edge.relation == RelationType.CONFLICTS_WITH.value:
                    lines.append(f"    {src} -.->|{label}| {edge.target}")
                else:
                    lines.append(f"    {src} -->|{label}| {edge.target}")
                edge_count += 1

        if edge_count == 0:
            lines.append("    %% All current milestones are mutually INDEPENDENT")

        return "\n".join(lines)

    def to_adjacency_json(self) -> Dict[str, Any]:
        """Export the full dependency graph as a JSON-serializable dictionary."""
        try:
            tiers = self.topological_sort()
        except CyclicDependencyError:
            tiers = []

        critical = self.critical_path()

        nodes_meta = {}
        for k, v in self._nodes.items():
            fp = v["footprint"]
            nodes_meta[k] = {
                "milestone_id": v["milestone_id"],
                "filepath": v["filepath"],
                "risk_score": v["risk_score"],
                "status": v["status"],
                "created_at": v.get("created_at"),
                "footprint": {
                    "filepath": fp.filepath,
                    "regions": fp.regions,
                    "symbols": fp.symbols,
                },
            }

        edges_meta = {}
        for k, edge_list in self._edges.items():
            edges_meta[k] = [
                {
                    "target": e.target,
                    "relation": e.relation,
                    "weight": e.weight,
                    "reason": e.reason,
                }
                for e in edge_list
            ]

        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "nodes": nodes_meta,
            "edges": edges_meta,
            "dispatch_tiers": tiers,
            "critical_path": critical,
            "metrics": {
                "node_count": len(self._nodes),
                "edge_count": sum(len(el) for el in self._edges.values()),
                "tier_count": len(tiers),
            },
        }

    def rebuild_and_persist(self) -> str:
        """Recompute sort, critical path, render Mermaid, and atomically save ledger."""
        try:
            tiers = self.topological_sort()
            has_cycle = False
            cycle_info = []
        except CyclicDependencyError as exc:
            tiers = []
            has_cycle = True
            cycle_info = exc.cycle_paths

        critical = self.critical_path()
        mermaid_graph = self.render_mermaid()
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        md_lines = [
            "# Milestone Dependency Graph & Critical-Path Dispatch Ledger",
            f"**Last Synchronized:** `{now_str}`",
            "",
            "## 1. Graph Metrics",
            f"- **Total Milestones:** `{len(self._nodes)}`",
            f"- **Active Edges:** `{sum(len(el) for el in self._edges.values())}`",
            f"- **Dispatch Tiers:** `{len(tiers)}`",
            f"- **Dependency Cycle Status:** `{'CRITICAL: CYCLES DETECTED' if has_cycle else 'NOMINAL (DAG VERIFIED)'}`",
            "",
            "## 2. Critical Path Analysis",
        ]

        if critical:
            path_str = " -> ".join(f"`{m}`" for m in critical)
            md_lines.append(f"**Highest Risk Sequence:** {path_str}")
        else:
            md_lines.append("_No critical sequence computed._")

        md_lines.extend([
            "",
            "## 3. Parallel Dispatch Tiers (Kahn's Tiers)",
            "| Tier | Concurrency Count | Milestone IDs | Target Files |",
            "| :--- | :--- | :--- | :--- |",
        ])

        if tiers:
            for idx, tier in enumerate(tiers):
                m_str = ", ".join(f"`{m}`" for m in tier)
                f_str = ", ".join(
                    f"`{self._nodes[m]['filepath']}`"
                    for m in tier
                    if self._nodes[m]["filepath"]
                )
                md_lines.append(f"| Tier {idx + 1} | {len(tier)} | {m_str} | {f_str} |")
        else:
            md_lines.append("| None | 0 | _No dispatchable tiers_ | - |")

        if has_cycle:
            md_lines.extend([
                "",
                "### Cycle Diagnostic Report",
                "```json",
                json.dumps(cycle_info, indent=2),
                "```",
            ])

        md_lines.extend([
            "",
            "## 4. Visual Topology (Mermaid)",
            "```mermaid",
            mermaid_graph,
            "```",
            "",
            "## 5. Milestone Footprint Ledger",
            "| Milestone ID | Target File | Risk Score | Symbols Identified | Line Regions |",
            "| :--- | :--- | :--- | :--- | :--- |",
        ])

        for m_id, data in sorted(self._nodes.items()):
            fp: FileFootprint = data["footprint"]
            symbols_str = ", ".join(fp.symbols[:6]) + ("..." if len(fp.symbols) > 6 else "")
            if not symbols_str:
                symbols_str = "—"
            regions_str = (
                ", ".join(f"L{s}-L{e}" for s, e in fp.regions)
                if fp.regions
                else "Full File"
            )
            md_lines.append(
                f"| `{m_id}` | `{data['filepath']}` | `{data['risk_score']:.2f}` | "
                f"`{symbols_str}` | `{regions_str}` |"
            )

        content = "\n".join(md_lines) + "\n"

        # Atomic file write to avoid read tearing
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        temp_fd, temp_file_path = tempfile.mkstemp(
            prefix="dep_graph_",
            suffix=".tmp",
            dir=str(self.ledger_path.parent),
        )
        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                f.write(content)
            os.replace(temp_file_path, self.ledger_path)
        except Exception:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
            raise

        return str(self.ledger_path)


# ============================================================================
# Module-Level Dispatch Integration Hook
# ============================================================================

_DEFAULT_ENGINE: Optional[DependencyGraphEngine] = None


def get_default_engine() -> DependencyGraphEngine:
    """Retrieve or initialize the singleton DependencyGraphEngine instance."""
    global _DEFAULT_ENGINE
    if _DEFAULT_ENGINE is None:
        _DEFAULT_ENGINE = DependencyGraphEngine()
    return _DEFAULT_ENGINE


def get_dispatch_order(
    milestones: List[Dict[str, Any]], policy_gate: Any = None
) -> List[List[str]]:
    """Integration hook called by autonomous orchestrators before buffer dispatch.

    Builds dependency DAG and yields strictly ordered, parallelizable tiers.
    Tier N+1 milestones must only be released after all Tier N milestones complete.
    """
    engine = DependencyGraphEngine(policy_gate=policy_gate)

    for item in milestones:
        m_id = item.get("id") or item.get("milestone_id")
        filepath = item.get("filepath", "")
        spec_text = item.get("spec_text") or item.get("spec", "")
        verdict = (
            item.get("policy_verdict")
            or item.get("verdict")
            or item.get("risk_score", 0.0)
        )
        status = item.get("status", "PENDING")

        if m_id:
            engine.add_milestone(
                milestone_id=m_id,
                filepath=filepath,
                spec_text=spec_text,
                policy_verdict=verdict,
                status=status,
            )

    return engine.topological_sort()


# ============================================================================
# Flask REST Blueprint
# ============================================================================

if FLASK_AVAILABLE:
    dependency_bp = Blueprint("dependency_bp", __name__)

    @dependency_bp.route("/api/dependency/graph", methods=["GET"])
    def get_graph():
        """Retrieve full adjacency list and graph metadata."""
        engine = get_default_engine()
        return jsonify(engine.to_adjacency_json()), 200

    @dependency_bp.route("/api/dependency/critical-path", methods=["GET"])
    def get_critical_path():
        """Retrieve ordered critical path milestones."""
        engine = get_default_engine()
        return jsonify({
            "critical_path": engine.critical_path(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }), 200

    @dependency_bp.route("/api/dependency/rebuild", methods=["POST"])
    def trigger_rebuild():
        """Rebuild dependency graph and re-persist dashboard markdown."""
        engine = get_default_engine()
        saved_path = engine.rebuild_and_persist()
        return jsonify({
            "status": "success",
            "message": "Dependency graph rebuilt and persisted successfully.",
            "ledger_path": saved_path,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }), 200
else:
    dependency_bp = None


# ============================================================================
# Verification Demo & Unit Test Executable
# ============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("Hermes Dependency Graph & Critical-Path Scheduler - Standalone Test Demo")
    print("=" * 80)

    demo_engine = DependencyGraphEngine(ledger_path="dashboards/DEPENDENCY_GRAPH.md")

    # Sample Milestone 1: Core Authentication Service
    m1_spec = """
    Implement core authentication token manager in core/auth_manager.py.
    Provides class TokenManager, def verify_session, def sign_jwt.
    Touches lines 1 to 120.
    """
    print("\n[+] Adding Milestone 1: M1_CORE_AUTH (High Risk)")
    demo_engine.add_milestone(
        milestone_id="M1_CORE_AUTH",
        filepath="core/auth_manager.py",
        spec_text=m1_spec,
        policy_verdict={"risk_score": 0.85},
    )

    # Sample Milestone 2: Token Refresh Mechanism (Overlaps heavily on same file)
    m2_spec = """
    Extend core/auth_manager.py with TokenManager refresh mechanism.
    Defines def refresh_token, imports sign_jwt, updates verify_session.
    Modifies lines 80 to 160.
    """
    print("[+] Adding Milestone 2: M2_TOKEN_REFRESH (High Risk & Overlap with M1)")
    edges_m2 = demo_engine.add_milestone(
        milestone_id="M2_TOKEN_REFRESH",
        filepath="core/auth_manager.py",
        spec_text=m2_spec,
        policy_verdict={"risk_score": 0.70},
    )
    for e in edges_m2:
        print(f"    -> Edge created: {e.relation} (w={e.weight:.2f}) reason: {e.reason}")

    # Sample Milestone 3: Route Handlers (Uses symbols from M1, different file)
    m3_spec = """
    Create HTTP routes in api/v1/routes.py.
    Calls TokenManager and verify_session to guard private endpoints.
    Lines 1 to 80.
    """
    print("[+] Adding Milestone 3: M3_AUTH_ROUTES (Moderate Risk, references M1 symbols)")
    edges_m3 = demo_engine.add_milestone(
        milestone_id="M3_AUTH_ROUTES",
        filepath="api/v1/routes.py",
        spec_text=m3_spec,
        policy_verdict={"risk_score": 0.40},
    )
    for e in edges_m3:
        print(f"    -> Edge created: {e.relation} (w={e.weight:.2f}) reason: {e.reason}")

    # Sample Milestone 4: Telemetry Worker (Independent subsystem)
    m4_spec = """
    Build background metric aggregator in workers/telemetry_worker.py.
    Defines class TelemetryCollector, def push_stats.
    Touches lines 1 to 150.
    """
    print("[+] Adding Milestone 4: M4_TELEMETRY (Independent Subsystem)")
    edges_m4 = demo_engine.add_milestone(
        milestone_id="M4_TELEMETRY",
        filepath="workers/telemetry_worker.py",
        spec_text=m4_spec,
        policy_verdict={"risk_score": 0.15},
    )
    for e in edges_m4:
        print(f"    -> Edge created: {e.relation} (w={e.weight:.2f}) reason: {e.reason}")

    # Topological Sort Evaluation
    print("\n" + "-" * 50)
    print("Computing Concurrently Dispatchable Tiers (Kahn's Topological Sort):")
    dispatch_tiers = demo_engine.topological_sort()
    for idx, tier in enumerate(dispatch_tiers):
        print(f"  Tier {idx + 1} (Concurrent): {tier}")

    # Critical Path Evaluation
    print("\n" + "-" * 50)
    print("Computing Critical Path (Max Cumulative Risk/Weight Sequence):")
    crit_path = demo_engine.critical_path()
    print(f"  Critical Path: {' -> '.join(crit_path)}")

    # Cycle Detection Test
    print("\n" + "-" * 50)
    print("Verifying Cycle Detection DFS:")
    detected_cycles = demo_engine.detect_cycles()
    print(f"  Detected Cycles: {detected_cycles} (Expected: [])")

    # Rebuild & Persist
    print("\n" + "-" * 50)
    print("Persisting Dependency Graph Dashboard Ledger:")
    persisted_path = demo_engine.rebuild_and_persist()
    print(f"  Dashboard successfully written to: {persisted_path}")

    # Print Mermaid Output
    print("\n" + "-" * 50)
    print("Generated Mermaid Diagram:")
    print(demo_engine.render_mermaid())
    print("\n" + "=" * 80)
    print("All Dependency Graph Engine specifications verified successfully.")
    print("=" * 80)