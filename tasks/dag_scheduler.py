"""
Multi-Repo Topological Dependency DAG & Concurrency Decomposition Engine
Module: tasks/dag_scheduler.py

Formal Directed Acyclic Graph (DAG) scheduling, Tarjan/DFS cycle detection,
level-synchronous topological tiering, mutex-based parallel batch decomposition,
and Cockpit ASCII/Mermaid/JSON graph visualizers.
"""

from __future__ import annotations

import collections
from dataclasses import dataclass, field
import json
import os
import re
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


class DAGSchedulerException(Exception):
    """Base exception for DAG scheduling operations."""
    pass


class CyclicDependencyException(DAGSchedulerException):
    """Raised when a directed dependency cycle is detected in the milestone graph."""

    def __init__(self, cycle_path: List[str], message: Optional[str] = None):
        self.cycle_path = cycle_path
        path_str = " -> ".join(cycle_path)
        default_msg = f"Cyclic dependency detected in milestone graph: {path_str}"
        super().__init__(message or default_msg)


class UnresolvedDependencyException(DAGSchedulerException):
    """Raised when a prerequisite reference cannot be resolved to any milestone node."""
    pass


@dataclass
class MilestoneNode:
    """
    Represents an atomic milestone node within the multi-repo execution DAG.
    """
    id: str
    name: str
    repo: str
    target_file: Optional[str] = None
    target_dir: Optional[str] = None
    prerequisites: List[str] = field(default_factory=list)
    status: str = "PENDING"
    metadata: Dict[str, Any] = field(default_factory=dict)
    priority: int = 0
    estimated_tokens: int = 0

    def __post_init__(self):
        self.id = str(self.id).strip()
        self.name = str(self.name).strip()
        self.repo = str(self.repo).strip()
        self.prerequisites = [str(p).strip() for p in self.prerequisites if str(p).strip()]

        if self.target_file:
            self.target_file = os.path.normpath(str(self.target_file).strip())
            if not self.target_dir:
                dir_name = os.path.dirname(self.target_file)
                self.target_dir = dir_name if dir_name else "."
        elif self.target_dir:
            self.target_dir = os.path.normpath(str(self.target_dir).strip())
        else:
            self.target_dir = "."

    def to_dict(self) -> Dict[str, Any]:
        """Serializes the milestone node to a dictionary representation."""
        return {
            "id": self.id,
            "name": self.name,
            "repo": self.repo,
            "target_file": self.target_file,
            "target_dir": self.target_dir,
            "prerequisites": list(self.prerequisites),
            "status": self.status,
            "metadata": dict(self.metadata),
            "priority": self.priority,
            "estimated_tokens": self.estimated_tokens,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> MilestoneNode:
        """Constructs a MilestoneNode instance from raw dictionary data."""
        return cls(
            id=data["id"],
            name=data["name"],
            repo=data["repo"],
            target_file=data.get("target_file"),
            target_dir=data.get("target_dir"),
            prerequisites=data.get("prerequisites", []),
            status=data.get("status", "PENDING"),
            metadata=data.get("metadata", {}),
            priority=data.get("priority", 0),
            estimated_tokens=data.get("estimated_tokens", 0),
        )


class DAGScheduler:
    """
    Multi-Repo Topological Dependency DAG Scheduler.
    
    Coordinates milestone dependency graphs G = (V, E), detects circular references,
    computes level-synchronous topological tiers, enforces concurrency safety mutexes
    across file/directory write locks, and exports Cockpit graph artifacts.
    """

    def __init__(self, allow_missing_prereqs: bool = False):
        self.nodes: Dict[str, MilestoneNode] = {}
        # Directed edges: adj[u] = list of nodes v that depend on u (u is prerequisite of v)
        self.adj: Dict[str, Set[str]] = collections.defaultdict(set)
        # Inverted edges: in_edges[v] = set of nodes u that v depends on (preds of v)
        self.in_edges: Dict[str, Set[str]] = collections.defaultdict(set)
        self.allow_missing_prereqs = allow_missing_prereqs
        self._resolved_prereqs: Dict[str, Set[str]] = collections.defaultdict(set)
        self._is_validated: bool = False

    def add_node(self, node: MilestoneNode) -> None:
        """Registers a milestone node in the graph."""
        self.nodes[node.id] = node
        self._is_validated = False

    def add_nodes(self, nodes: Iterable[MilestoneNode]) -> None:
        """Registers multiple milestone nodes in the graph."""
        for node in nodes:
            self.add_node(node)

    @classmethod
    def from_dict_list(cls, data: List[Dict[str, Any]], allow_missing_prereqs: bool = False) -> DAGScheduler:
        """Instantiates and populates a DAGScheduler from a list of node dictionaries."""
        scheduler = cls(allow_missing_prereqs=allow_missing_prereqs)
        for item in data:
            scheduler.add_node(MilestoneNode.from_dict(item))
        return scheduler

    # --------------------------------------------------------------------------
    # Prerequisite Fuzzy Resolution
    # --------------------------------------------------------------------------

    def _normalize_string(self, text: str) -> str:
        """Normalizes text for fuzzy token matching."""
        return re.sub(r"[^a-zA-Z0-9]", "", text).lower()

    def _resolve_prerequisite(self, source_node: MilestoneNode, prereq: str) -> Optional[str]:
        """
        Resolves a prerequisite string to a target node ID using multi-level matching:
        1. Exact ID match
        2. Exact Name match
        3. Normalized case-insensitive alphanumeric match
        4. Target file exact match or base filename match
        5. Unambiguous substring match
        """
        prereq_clean = prereq.strip()

        # 1. Exact ID
        if prereq_clean in self.nodes:
            return prereq_clean

        # 2. Case-insensitive ID
        prereq_lower = prereq_clean.lower()
        for node_id in self.nodes:
            if node_id.lower() == prereq_lower:
                return node_id

        # 3. Exact Name match
        for node_id, node in self.nodes.items():
            if node.name == prereq_clean:
                return node_id

        # 4. Case-insensitive Name match
        name_matches = [
            node_id for node_id, node in self.nodes.items()
            if node.name.lower() == prereq_lower
        ]
        if len(name_matches) == 1:
            return name_matches[0]

        # 5. Target file or basename match
        prereq_norm_path = os.path.normpath(prereq_clean)
        prereq_base = os.path.basename(prereq_norm_path)
        file_matches = []
        for node_id, node in self.nodes.items():
            if node.target_file:
                if node.target_file == prereq_norm_path or os.path.basename(node.target_file) == prereq_base:
                    file_matches.append(node_id)
        if len(file_matches) == 1:
            return file_matches[0]

        # 6. Normalized alphanumeric token match
        norm_query = self._normalize_string(prereq_clean)
        if norm_query:
            token_matches = [
                node_id for node_id, node in self.nodes.items()
                if self._normalize_string(node.name) == norm_query or
                   self._normalize_string(node.id) == norm_query
            ]
            if len(token_matches) == 1:
                return token_matches[0]

        # 7. Unambiguous substring match
        substr_matches = [
            node_id for node_id, node in self.nodes.items()
            if prereq_lower in node.name.lower()
        ]
        if len(substr_matches) == 1:
            return substr_matches[0]

        if self.allow_missing_prereqs:
            return None

        candidates_hint = [f"{n.id} ({n.name})" for n in list(self.nodes.values())[:5]]
        raise UnresolvedDependencyException(
            f"Milestone '{source_node.id}' references prerequisite '{prereq}', "
            f"which cannot be resolved to any milestone in G. Known candidates: {candidates_hint}"
        )

    def _build_edges(self) -> None:
        """Constructs graph adjacency and inverted edge lists from prerequisite declarations."""
        self.adj.clear()
        self.in_edges.clear()
        self._resolved_prereqs.clear()

        # Initialize all nodes in adjacency maps
        for node_id in self.nodes:
            self.adj[node_id] = set()
            self.in_edges[node_id] = set()

        for node_id, node in self.nodes.items():
            for prereq in node.prerequisites:
                target_id = self._resolve_prerequisite(node, prereq)
                if target_id is not None:
                    # target_id is prerequisite of node_id: target_id -> node_id
                    self.adj[target_id].add(node_id)
                    self.in_edges[node_id].add(target_id)
                    self._resolved_prereqs[node_id].add(target_id)

    # --------------------------------------------------------------------------
    # Cycle Detection & Tarjan SCC Validation
    # --------------------------------------------------------------------------

    def find_cycle(self) -> Optional[List[str]]:
        """
        Executes Depth-First Search with node-state coloring (White/Gray/Black)
        to identify directed cycles.
        
        Returns:
            The complete cycle path list (e.g., ['A', 'B', 'C', 'A']), or None.
        """
        # 0 = WHITE (unvisited), 1 = GRAY (visiting), 2 = BLACK (visited)
        color: Dict[str, int] = {node_id: 0 for node_id in self.nodes}
        cycle: List[str] = []

        def dfs(u: str, path: List[str]) -> bool:
            color[u] = 1
            path.append(u)

            # Sort adjacencies for deterministic evaluation
            for v in sorted(self.adj[u]):
                if color[v] == 1:
                    # Back-edge detected; cycle closed!
                    idx = path.index(v)
                    cycle.extend(path[idx:])
                    cycle.append(v)
                    return True
                elif color[v] == 0:
                    if dfs(v, path):
                        return True

            path.pop()
            color[u] = 2
            return False

        for node_id in sorted(self.nodes.keys()):
            if color[node_id] == 0:
                if dfs(node_id, []):
                    return cycle

        return None

    def find_strongly_connected_components(self) -> List[List[str]]:
        """
        Computes strongly connected components (SCC) via Tarjan's algorithm.
        Returns all cyclic components (components with >1 node or self-loops).
        """
        index = 0
        indices: Dict[str, int] = {}
        lowlinks: Dict[str, int] = {}
        on_stack: Dict[str, bool] = {u: False for u in self.nodes}
        stack: List[str] = []
        cyclic_sccs: List[List[str]] = []

        def strongconnect(u: str):
            nonlocal index
            indices[u] = index
            lowlinks[u] = index
            index += 1
            stack.append(u)
            on_stack[u] = True

            for v in self.adj[u]:
                if v not in indices:
                    strongconnect(v)
                    lowlinks[u] = min(lowlinks[u], lowlinks[v])
                elif on_stack[v]:
                    lowlinks[u] = min(lowlinks[u], indices[v])

            if lowlinks[u] == indices[u]:
                scc = []
                while True:
                    w = stack.pop()
                    on_stack[w] = False
                    scc.append(w)
                    if w == u:
                        break
                if len(scc) > 1 or u in self.adj[u]:
                    cyclic_sccs.append(scc)

        for u in sorted(self.nodes.keys()):
            if u not in indices:
                strongconnect(u)

        return cyclic_sccs

    def validate(self) -> None:
        """
        Validates graph integrity: builds edges, resolves prerequisites,
        and enforces strict acyclicity.
        """
        self._build_edges()
        cycle_path = self.find_cycle()
        if cycle_path:
            raise CyclicDependencyException(cycle_path)
        self._is_validated = True

    # --------------------------------------------------------------------------
    # Level-Synchronous Topological Tiering
    # --------------------------------------------------------------------------

    def get_topological_tiers(self) -> List[List[MilestoneNode]]:
        r"""
        Partitions milestones into level-synchronous concurrent execution tiers.
        
        Tier_0 = {v in V | in_degree(v) == 0}
        Tier_{k+1} = {v in V \ U_{j=0}^k Tier_j | forall u in preds(v), u in U_{j=0}^k Tier_j}
        
        Milestones in Tier_k are mathematically disjoint in prerequisites.
        """
        if not self._is_validated:
            self.validate()

        in_degree = {u: len(self.in_edges[u]) for u in self.nodes}
        current_tier = [u for u, deg in in_degree.items() if deg == 0]
        # Deterministic sorting: higher priority first, then lexicographical ID
        current_tier.sort(key=lambda u: (-self.nodes[u].priority, self.nodes[u].id))

        tiers: List[List[str]] = []
        assigned_count = 0

        while current_tier:
            tiers.append(current_tier)
            assigned_count += len(current_tier)
            next_tier = []

            for u in current_tier:
                for v in self.adj[u]:
                    in_degree[v] -= 1
                    if in_degree[v] == 0:
                        next_tier.append(v)

            next_tier.sort(key=lambda v: (-self.nodes[v].priority, self.nodes[v].id))
            current_tier = next_tier

        if assigned_count != len(self.nodes):
            remaining = [u for u, deg in in_degree.items() if deg > 0]
            raise CyclicDependencyException(remaining, f"Topological sort incomplete due to cycle: {remaining}")

        return [[self.nodes[node_id] for node_id in tier] for tier in tiers]

    # --------------------------------------------------------------------------
    # Concurrency Safety Mutex Matrix
    # --------------------------------------------------------------------------

    def evaluate_conflict(self, v_a: MilestoneNode, v_b: MilestoneNode) -> bool:
        """
        Evaluates the Concurrency Safety Conflict Predicate:
        C(v_a, v_b) = [repo(v_a) == repo(v_b) and (target_dir(v_a) == target_dir(v_b) or target_file(v_a) == target_file(v_b))]
        """
        if v_a.repo != v_b.repo:
            return False

        # Direct file collision
        if v_a.target_file and v_b.target_file:
            if v_a.target_file == v_b.target_file:
                return True

        # Directory mutex collision (disallowing concurrent writes to same non-root folder)
        if v_a.target_dir and v_b.target_dir:
            if v_a.target_dir == v_b.target_dir and v_a.target_dir not in (".", ""):
                return True

        # Target file containment in another node's targeted directory
        if v_a.target_file and v_b.target_dir and v_b.target_dir not in (".", ""):
            if os.path.dirname(v_a.target_file) == v_b.target_dir:
                return True
        if v_b.target_file and v_a.target_dir and v_a.target_dir not in (".", ""):
            if os.path.dirname(v_b.target_file) == v_a.target_dir:
                return True

        # If both target the root workspace without target files, assume conflict
        if not v_a.target_file and not v_b.target_file and v_a.target_dir == v_b.target_dir:
            return True

        return False

    def partition_tier_into_mutex_batches(self, tier: List[MilestoneNode]) -> List[List[MilestoneNode]]:
        """
        Partitions a topological tier into disjoint, internally conflict-free
        parallel execution sub-batches using greedy graph vertex coloring.
        """
        batches: List[List[MilestoneNode]] = []

        for node in tier:
            assigned = False
            for batch in batches:
                # Check conflict against every node currently in this candidate batch
                has_conflict = any(self.evaluate_conflict(node, existing) for existing in batch)
                if not has_conflict:
                    batch.append(node)
                    assigned = True
                    break

            if not assigned:
                batches.append([node])

        return batches

    def get_safe_parallel_batch(self, tier: Optional[List[MilestoneNode]] = None) -> List[MilestoneNode]:
        """
        Returns the first maximal conflict-free execution batch for the given tier
        (or for Tier_0 if no tier is explicitly passed).
        """
        if tier is None:
            tiers = self.get_topological_tiers()
            if not tiers:
                return []
            tier = tiers[0]

        mutex_batches = self.partition_tier_into_mutex_batches(tier)
        return mutex_batches[0] if mutex_batches else []

    def get_safe_parallel_batches(self) -> List[List[MilestoneNode]]:
        """
        Decomposes the entire DAG into a sequence of safe, lock-isolated parallel batches.
        Each outer element represents a batch of milestones that can execute concurrently
        with zero Git merge collisions or write contention.
        """
        tiers = self.get_topological_tiers()
        all_safe_batches: List[List[MilestoneNode]] = []
        for tier in tiers:
            batches = self.partition_tier_into_mutex_batches(tier)
            all_safe_batches.extend(batches)
        return all_safe_batches

    # --------------------------------------------------------------------------
    # Visualization & Serialization
    # --------------------------------------------------------------------------

    def export_dag_ascii(self, tiers: Optional[List[List[MilestoneNode]]] = None) -> str:
        """
        Generates an ASCII visualization of the topological execution pipeline.
        """
        if tiers is None:
            tiers = self.get_topological_tiers()

        lines: List[str] = [
            "================================================================================",
            "                   MULTI-REPO TOPOLOGICAL DEPENDENCY DAG                        ",
            "================================================================================",
        ]

        if not tiers:
            lines.append(" (Empty DAG: No milestones registered)")
            lines.append("================================================================================")
            return "\n".join(lines)

        for tier_idx, tier in enumerate(tiers):
            safe_batches = self.partition_tier_into_mutex_batches(tier)
            batch_count = len(safe_batches)

            tier_header = f"TIER {tier_idx} [Nodes: {len(tier)} | Mutex Sub-Batches: {batch_count}]"
            lines.append(f"+- {tier_header} " + "-" * max(2, 74 - len(tier_header)))

            for b_idx, batch in enumerate(safe_batches):
                prefix = f"  |-- Batch {tier_idx}.{b_idx}: "
                lines.append(prefix)
                for node in batch:
                    target_repr = node.target_file or (node.target_dir + "/")
                    prereq_repr = ", ".join(self._resolved_prereqs.get(node.id, [])) or "None"
                    lines.append(f"  |     * [{node.id}] @ <{node.repo}> :: {node.name}")
                    lines.append(f"  |       Target: {target_repr} | Prereqs: [{prereq_repr}]")

            lines.append("  |")
            if tier_idx < len(tiers) - 1:
                lines.append("  V  (Topological Dependency Flow)")
                lines.append("  |")

        lines.append("+-------------------------------------------------------------------------------")
        return "\n".join(lines)

    def export_dag_mermaid(self, tiers: Optional[List[List[MilestoneNode]]] = None) -> str:
        """
        Exports the DAG as a Mermaid.js diagram for web dashboard integration.
        """
        if tiers is None:
            tiers = self.get_topological_tiers()

        def clean_id(raw_id: str) -> str:
            return re.sub(r"[^a-zA-Z0-9_]", "_", raw_id)

        lines: List[str] = ["graph TD"]

        for tier_idx, tier in enumerate(tiers):
            lines.append(f"    subgraph Tier_{tier_idx}[\"Tier {tier_idx}\"]")
            for node in tier:
                node_cid = clean_id(node.id)
                safe_name = node.name.replace('"', "'")
                target = node.target_file or node.target_dir
                lines.append(
                    f"        {node_cid}[\"<b>[{node.id}]</b> {safe_name}<br/>"
                    f"<i>{node.repo}</i> &bull; <code>{target}</code>\"]"
                )
            lines.append("    end")

        # Edges
        for u, dependents in sorted(self.adj.items()):
            u_cid = clean_id(u)
            for v in sorted(dependents):
                v_cid = clean_id(v)
                lines.append(f"    {u_cid} --> {v_cid}")

        return "\n".join(lines)

    def export_dag_dict(self) -> Dict[str, Any]:
        """Exports structured DAG metadata for Cockpit mini-map rendering."""
        tiers = self.get_topological_tiers()
        all_edges = []
        for u, dependents in self.adj.items():
            for v in dependents:
                all_edges.append({"from": u, "to": v})

        tier_data = []
        for tier_idx, tier in enumerate(tiers):
            batches = self.partition_tier_into_mutex_batches(tier)
            tier_data.append({
                "tier_index": tier_idx,
                "milestone_ids": [node.id for node in tier],
                "safe_batches": [[node.id for node in b] for b in batches],
            })

        return {
            "summary": {
                "total_nodes": len(self.nodes),
                "total_edges": len(all_edges),
                "total_tiers": len(tiers),
                "has_cycles": False,
            },
            "tiers": tier_data,
            "nodes": {node_id: node.to_dict() for node_id, node in self.nodes.items()},
            "edges": sorted(all_edges, key=lambda e: (e["from"], e["to"])),
        }

    def export_dag_json(self, indent: int = 2) -> str:
        """Serializes the complete DAG metadata to formatted JSON."""
        return json.dumps(self.export_dag_dict(), indent=indent)


# ==============================================================================
# Comprehensive Verification Test Suite
# ==============================================================================

def _run_unit_tests():
    """Validates diamond dependencies, multi-repo tiers, cycles, and mutex locks."""
    print("[*] Running DAGScheduler Unit Test Suite...")

    # 1. Diamond Dependency Test: A -> B, A -> C, (B, C) -> D
    scheduler = DAGScheduler()
    m_a = MilestoneNode(id="MS-A", name="Init Core Config", repo="repo-core", target_file="config.py")
    m_b = MilestoneNode(id="MS-B", name="Worker Engine", repo="repo-worker", target_file="engine.py", prerequisites=["MS-A"])
    m_c = MilestoneNode(id="MS-C", name="Database Store", repo="repo-db", target_file="store.py", prerequisites=["MS-A"])
    m_d = MilestoneNode(id="MS-D", name="API Gateway", repo="repo-api", target_file="gateway.py", prerequisites=["MS-B", "MS-C"])

    scheduler.add_nodes([m_a, m_b, m_c, m_d])
    tiers = scheduler.get_topological_tiers()

    assert len(tiers) == 3, f"Expected 3 tiers, got {len(tiers)}"
    assert [n.id for n in tiers[0]] == ["MS-A"]
    assert sorted([n.id for n in tiers[1]]) == ["MS-B", "MS-C"]
    assert [n.id for n in tiers[2]] == ["MS-D"]
    print("  [+] Diamond dependency level-synchronous tiering verified.")

    # 2. Mutex Conflict Isolation in Concurrent Tiers
    # In Tier 1, suppose two nodes target the same repository and directory
    m_b2 = MilestoneNode(
        id="MS-B2", name="Worker Monitor", repo="repo-worker",
        target_file="engine.py", prerequisites=["MS-A"]
    )
    scheduler_conflict = DAGScheduler()
    scheduler_conflict.add_nodes([m_a, m_b, m_b2])
    tiers_conflict = scheduler_conflict.get_topological_tiers()
    assert len(tiers_conflict[1]) == 2

    # Verify partition separates MS-B and MS-B2 due to identical repo and file
    sub_batches = scheduler_conflict.partition_tier_into_mutex_batches(tiers_conflict[1])
    assert len(sub_batches) == 2, f"Expected 2 mutex sub-batches for conflicting tasks, got {len(sub_batches)}"
    assert sub_batches[0][0].id != sub_batches[1][0].id
    print("  [+] Concurrency safety mutex matrix & sub-batch partitioning verified.")

    # 3. Cycle Detection Verification: A -> B -> C -> A
    cycle_scheduler = DAGScheduler()
    c1 = MilestoneNode(id="C1", name="Step 1", repo="r1", prerequisites=["C3"])
    c2 = MilestoneNode(id="C2", name="Step 2", repo="r1", prerequisites=["C1"])
    c3 = MilestoneNode(id="C3", name="Step 3", repo="r1", prerequisites=["C2"])
    cycle_scheduler.add_nodes([c1, c2, c3])

    try:
        cycle_scheduler.validate()
        assert False, "Cyclic dependency failed to raise CyclicDependencyException!"
    except CyclicDependencyException as exc:
        assert len(exc.cycle_path) >= 3
        print(f"  [+] Cyclic dependency caught successfully: {' -> '.join(exc.cycle_path)}")

    # 4. Fuzzy Resolution Verification
    fuzzy_scheduler = DAGScheduler()
    base_m = MilestoneNode(id="BASE-101", name="Architecture Pattern Classifier", repo="hermes_core", target_file="dashboards/matcher.py")
    dep_m = MilestoneNode(
        id="DEP-202",
        name="Telemetry Consumer",
        repo="hermes_core",
        target_file="dashboards/consumer.py",
        prerequisites=["matcher.py"]  # Resolved fuzzily via target_file basename
    )
    fuzzy_scheduler.add_nodes([base_m, dep_m])
    fuzzy_tiers = fuzzy_scheduler.get_topological_tiers()
    assert len(fuzzy_tiers) == 2
    assert fuzzy_tiers[0][0].id == "BASE-101"
    assert fuzzy_tiers[1][0].id == "DEP-202"
    print("  [+] Fuzzy prerequisite resolution verified (matched via filename).")

    # 5. ASCII, Mermaid, and JSON Visualizations
    ascii_output = scheduler.export_dag_ascii()
    mermaid_output = scheduler.export_dag_mermaid()
    json_output = scheduler.export_dag_json()

    assert "TIER 0" in ascii_output and "MS-A" in ascii_output
    assert "graph TD" in mermaid_output and "-->" in mermaid_output
    parsed_json = json.loads(json_output)
    assert parsed_json["summary"]["total_nodes"] == 4
    assert len(parsed_json["edges"]) == 4
    print("  [+] ASCII, Mermaid, and JSON export formats verified.")

    print("[SUCCESS] All DAGScheduler unit tests passed with 100% precision.")


if __name__ == "__main__":
    _run_unit_tests()