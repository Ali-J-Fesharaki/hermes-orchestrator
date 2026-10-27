#!/usr/bin/env python3
"""
tasks/dependency_graph_scheduler.py

Cross-Repository Dependency Graph & Topological Milestone Scheduler
Part of the Hermes Orchestrator.

This module provides a directed acyclic graph (DAG) engine to model, validate,
and schedule multi-repository milestone executions. It features:
  - Kahn's algorithm for cycle detection and BFS-based topological sorting.
  - Tarjan's strongly connected components (SCC) algorithm for cycle isolation.
  - Longest path Dynamic Programming for Critical Path Method (CPM) calculation.
  - Dynamic readiness queries for live agent milestone dispatching.
"""

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
import json
import logging
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


@dataclass
class MilestoneNode:
    """
    Represents an atomic or composite milestone within a cross-repository dependency graph.
    """
    milestone_id: str
    repo: str
    name: str
    status: str
    depends_on: List[str] = field(default_factory=list)
    estimated_duration_minutes: float = 30.0
    scheduled_start: Optional[datetime] = None
    critical: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert milestone node to a JSON-serializable dictionary."""
        return {
            "milestone_id": self.milestone_id,
            "repo": self.repo,
            "name": self.name,
            "status": self.status,
            "depends_on": list(self.depends_on),
            "estimated_duration_minutes": float(self.estimated_duration_minutes),
            "scheduled_start": self.scheduled_start.isoformat() if self.scheduled_start else None,
            "critical": self.critical,
        }


class DependencyGraph:
    """
    Directed Acyclic Graph (DAG) for managing execution dependencies between milestones.

    Edges are directed: from_id -> to_id means from_id must complete before to_id can start
    (i.e., to_id depends on from_id).
    """

    def __init__(self) -> None:
        self.adj: Dict[str, Set[str]] = {}          # node_id -> set of dependent node_ids
        self.reverse_adj: Dict[str, Set[str]] = {}  # node_id -> set of prerequisite node_ids
        self.nodes: Dict[str, MilestoneNode] = {}   # node_id -> MilestoneNode
        self.in_degree: Dict[str, int] = {}         # node_id -> current in-degree count

    def add_node(self, node: MilestoneNode) -> None:
        """Register a milestone node and wire its declared dependencies."""
        self.nodes[node.milestone_id] = node
        if node.milestone_id not in self.adj:
            self.adj[node.milestone_id] = set()
        if node.milestone_id not in self.reverse_adj:
            self.reverse_adj[node.milestone_id] = set()
        if node.milestone_id not in self.in_degree:
            self.in_degree[node.milestone_id] = 0

        for dep_id in node.depends_on:
            self.add_edge(dep_id, node.milestone_id)

    def add_edge(self, from_id: str, to_id: str) -> None:
        """
        Add directed dependency edge: from_id -> to_id.
        Meaning: from_id must finish before to_id may begin.
        """
        for nid in (from_id, to_id):
            if nid not in self.adj:
                self.adj[nid] = set()
            if nid not in self.reverse_adj:
                self.reverse_adj[nid] = set()
            if nid not in self.in_degree:
                self.in_degree[nid] = 0

        if to_id not in self.adj[from_id]:
            self.adj[from_id].add(to_id)
            self.reverse_adj[to_id].add(from_id)
            self.in_degree[to_id] += 1

    def has_cycle(self) -> bool:
        """
        Detect whether the graph contains any directed cycles using Kahn's algorithm.

        Kahn's algorithm simulates vertex removal:
          1. Calculate in-degrees for all vertices.
          2. Enqueue all vertices with in-degree == 0.
          3. Pop vertex, decrement in-degree of its neighbors.
          4. When a neighbor reaches in-degree 0, enqueue it.
          5. If the count of visited vertices < total vertices, a cycle exists.

        Returns:
            True if cycle exists, False if the graph is a valid DAG.
        """
        all_nodes = set(self.nodes.keys()) | set(self.adj.keys()) | set(self.reverse_adj.keys())
        if not all_nodes:
            return False

        in_deg = {node: len(self.reverse_adj.get(node, set())) for node in all_nodes}
        queue = deque([node for node, deg in in_deg.items() if deg == 0])
        processed_count = 0

        while queue:
            curr = queue.popleft()
            processed_count += 1
            for nbr in self.adj.get(curr, set()):
                in_deg[nbr] -= 1
                if in_deg[nbr] == 0:
                    queue.append(nbr)

        return processed_count < len(all_nodes)

    def topological_sort(self) -> List[str]:
        """
        Compute a valid linear topological execution order using Kahn's BFS algorithm.

        Returns:
            List[str]: Node IDs in a valid execution order.

        Raises:
            ValueError: If a cycle is detected, preventing topological sorting.
        """
        all_nodes = sorted(list(set(self.nodes.keys()) | set(self.adj.keys()) | set(self.reverse_adj.keys())))
        if not all_nodes:
            return []

        in_deg = {node: len(self.reverse_adj.get(node, set())) for node in all_nodes}
        queue = deque([node for node in all_nodes if in_deg[node] == 0])
        linear_order: List[str] = []

        while queue:
            curr = queue.popleft()
            linear_order.append(curr)
            for nbr in sorted(self.adj.get(curr, set())):
                in_deg[nbr] -= 1
                if in_deg[nbr] == 0:
                    queue.append(nbr)

        if len(linear_order) < len(all_nodes):
            raise ValueError(
                f"Cycle detected in dependency graph. Processed {len(linear_order)} "
                f"of {len(all_nodes)} nodes."
            )

        return linear_order

    def get_ready_nodes(self, completed: Set[str]) -> List[str]:
        """
        Identify nodes ready for immediate dispatch.

        A node is ready if:
          1. It is registered in self.nodes.
          2. It is not already in the completed set.
          3. Its status is not 'done' or 'completed'.
          4. All incoming dependency requirements (reverse_adj) are satisfied in completed.

        Args:
            completed: Set of completed milestone IDs.

        Returns:
            List[str]: Milestone IDs ready to run.
        """
        ready: List[str] = []
        for node_id, node in self.nodes.items():
            if node_id in completed:
                continue
            if node.status.lower() in ("done", "completed", "success"):
                continue

            prereqs = self.reverse_adj.get(node_id, set())
            if prereqs.issubset(completed):
                ready.append(node_id)

        return sorted(ready)


def detect_cycles(graph: DependencyGraph) -> List[List[str]]:
    """
    Identify and extract all cycle groups using Tarjan's Strongly Connected Components (SCC) algorithm.

    Tarjan's algorithm runs in O(V + E) time by doing a depth-first search while maintaining
    discovery indices and lowest-reachable link values on an active exploration stack.

    Returns:
        List[List[str]]: Groups of milestone IDs involved in circular dependency loops.
                         Only components with |SCC| > 1 or self-loops are returned.
    """
    index = 0
    indices: Dict[str, int] = {}
    lowlink: Dict[str, int] = {}
    on_stack: Dict[str, bool] = {}
    stack: List[str] = []
    cycle_groups: List[List[str]] = []

    all_nodes = sorted(list(set(graph.nodes.keys()) | set(graph.adj.keys()) | set(graph.reverse_adj.keys())))

    def strongconnect(v: str) -> None:
        nonlocal index
        indices[v] = index
        lowlink[v] = index
        index += 1
        stack.append(v)
        on_stack[v] = True

        for w in sorted(graph.adj.get(v, set())):
            if w not in indices:
                strongconnect(w)
                lowlink[v] = min(lowlink[v], lowlink[w])
            elif on_stack.get(w, False):
                lowlink[v] = min(lowlink[v], indices[w])

        if lowlink[v] == indices[v]:
            scc: List[str] = []
            while True:
                w = stack.pop()
                on_stack[w] = False
                scc.append(w)
                if w == v:
                    break
            # An SCC represents a cycle if it has > 1 node or a self-loop
            if len(scc) > 1:
                cycle_groups.append(list(reversed(scc)))
            elif len(scc) == 1 and scc[0] in graph.adj.get(scc[0], set()):
                cycle_groups.append(scc)

    for node in all_nodes:
        if node not in indices:
            strongconnect(node)

    return cycle_groups


def critical_path(
    graph: DependencyGraph, durations: Optional[Dict[str, float]] = None
) -> Tuple[List[str], float]:
    """
    Compute the Critical Path (longest duration path) through the DAG.

    Uses Dynamic Programming over topological sorting order:
      - CP_start[v] = max_{u in preds(v)} (CP_start[u] + duration[u])
      - CP_finish[v] = CP_start[v] + duration[v]

    The terminal node with the maximal CP_finish value defines the critical path endpoint.
    The path is reconstructed via predecessor backtracking.

    Args:
        graph: DependencyGraph instance.
        durations: Optional dict mapping milestone_id to duration in minutes.
                   Defaults to node.estimated_duration_minutes or 30.0.

    Returns:
        Tuple[List[str], float]: (critical_path_milestone_ids, total_duration_minutes)
    """
    if not graph.nodes and not graph.adj:
        return ([], 0.0)

    if graph.has_cycle():
        raise ValueError("Cannot calculate critical path on a cyclic graph.")

    topo_order = graph.topological_sort()
    if not topo_order:
        return ([], 0.0)

    dur_map: Dict[str, float] = {}
    for nid in topo_order:
        if durations and nid in durations:
            dur_map[nid] = float(durations[nid])
        elif nid in graph.nodes:
            dur_map[nid] = float(graph.nodes[nid].estimated_duration_minutes)
        else:
            dur_map[nid] = 30.0

    cp_start: Dict[str, float] = {}
    cp_finish: Dict[str, float] = {}
    parent: Dict[str, Optional[str]] = {}

    for v in topo_order:
        preds = [u for u in graph.reverse_adj.get(v, set()) if u in topo_order]
        if not preds:
            cp_start[v] = 0.0
            parent[v] = None
        else:
            best_u = max(preds, key=lambda u: cp_finish[u])
            cp_start[v] = cp_finish[best_u]
            parent[v] = best_u
        cp_finish[v] = cp_start[v] + dur_map[v]

    end_node = max(topo_order, key=lambda v: cp_finish[v])
    total_duration = cp_finish[end_node]

    # Backtrack to reconstruct critical path
    path: List[str] = []
    curr: Optional[str] = end_node
    while curr is not None:
        path.append(curr)
        curr = parent.get(curr)
    path.reverse()

    return (path, total_duration)


class TopologicalMilestoneScheduler:
    """
    High-level orchestrator scheduler that ingests raw milestone specifications,
    builds the cross-repository dependency graph, tracks dispatch readiness,
    and isolates scheduling bottlenecks.
    """

    def __init__(self, milestones: List[Dict[str, Any]]) -> None:
        self.raw_milestones = milestones
        self.graph = DependencyGraph()
        self._build_graph()

    def _build_graph(self) -> None:
        for m in self.raw_milestones:
            sched_start = m.get("scheduled_start")
            if isinstance(sched_start, str):
                try:
                    sched_start = datetime.fromisoformat(sched_start)
                except ValueError:
                    sched_start = None

            node = MilestoneNode(
                milestone_id=str(m["milestone_id"]),
                repo=str(m.get("repo", "default")),
                name=str(m.get("name", m["milestone_id"])),
                status=str(m.get("status", "pending")),
                depends_on=list(m.get("depends_on", [])),
                estimated_duration_minutes=float(m.get("estimated_duration_minutes", 30.0)),
                scheduled_start=sched_start,
                critical=bool(m.get("critical", False)),
            )
            self.graph.add_node(node)

    def get_dispatch_order(self, completed_ids: Set[str]) -> List[MilestoneNode]:
        """
        Get milestones currently unblocked and eligible for immediate autonomous dispatch.
        """
        ready_ids = self.graph.get_ready_nodes(completed_ids)
        return [self.graph.nodes[mid] for mid in ready_ids if mid in self.graph.nodes]

    def get_blocked_milestones(
        self, completed_ids: Optional[Set[str]] = None
    ) -> List[Tuple[MilestoneNode, List[str]]]:
        """
        Identify milestones currently blocked along with their missing prerequisite IDs.
        """
        if completed_ids is None:
            completed_ids = set()

        blocked: List[Tuple[MilestoneNode, List[str]]] = []
        for mid, node in self.graph.nodes.items():
            if mid in completed_ids:
                continue
            if node.status.lower() in ("done", "completed", "success"):
                continue

            unsatisfied = [
                dep for dep in node.depends_on
                if dep not in completed_ids and (
                    dep not in self.graph.nodes or
                    self.graph.nodes[dep].status.lower() not in ("done", "completed", "success")
                )
            ]
            if unsatisfied:
                blocked.append((node, sorted(unsatisfied)))

        blocked.sort(key=lambda item: item[0].milestone_id)
        return blocked

    def get_critical_path(self) -> Dict[str, Any]:
        """
        Calculate the critical path and annotate critical milestone nodes.
        """
        if self.graph.has_cycle():
            return {
                "path": [],
                "total_duration_minutes": 0.0,
                "error": "Cycle detected; critical path cannot be computed."
            }

        durations = {
            mid: node.estimated_duration_minutes
            for mid, node in self.graph.nodes.items()
        }
        path, total_duration = critical_path(self.graph, durations)

        for mid in path:
            if mid in self.graph.nodes:
                self.graph.nodes[mid].critical = True

        return {
            "path": path,
            "total_duration_minutes": total_duration
        }


def schedule_pipeline(all_milestones: List[Dict[str, Any]], completed: Set[str]) -> Dict[str, Any]:
    """
    Main orchestration entry point: builds schedule analysis for multi-repo pipelines.

    Args:
        all_milestones: List of milestone dictionaries.
        completed: Set of completed milestone IDs.

    Returns:
        Dict[str, Any] with:
            - 'ready': List of milestone dictionaries ready for dispatch.
            - 'blocked': List of blocked milestones with unsatisfied dependency IDs.
            - 'critical_path': Critical path node list and duration.
            - 'has_cycles': Boolean cycle indicator.
            - 'cycle_groups': List of detected cycle groups (if any).
    """
    scheduler = TopologicalMilestoneScheduler(all_milestones)
    has_cycles = scheduler.graph.has_cycle()
    cycle_groups = detect_cycles(scheduler.graph) if has_cycles else []

    ready_nodes = scheduler.get_dispatch_order(completed)
    blocked_items = scheduler.get_blocked_milestones(completed)
    cp_info = scheduler.get_critical_path()

    return {
        "ready": [node.to_dict() for node in ready_nodes],
        "blocked": [
            {
                "milestone_id": node.milestone_id,
                "repo": node.repo,
                "name": node.name,
                "unsatisfied_dependencies": missing
            }
            for node, missing in blocked_items
        ],
        "critical_path": cp_info,
        "has_cycles": has_cycles,
        "cycle_groups": cycle_groups,
    }


if __name__ == "__main__":
    # Example cross-repository milestone specification demonstrating dependencies
    # across 'hermes_orchestrator' and 'v2ray_tool'
    example_milestones: List[Dict[str, Any]] = [
        {
            "milestone_id": "M-CORE-01",
            "repo": "hermes_orchestrator",
            "name": "Milestone Lifecycle State Machine & Event Bus",
            "status": "done",
            "depends_on": [],
            "estimated_duration_minutes": 45.0,
            "scheduled_start": "2026-10-26T08:00:00",
            "critical": False,
        },
        {
            "milestone_id": "M-TELE-02",
            "repo": "hermes_orchestrator",
            "name": "Execution Telemetry Anomaly Detector",
            "status": "pending",
            "depends_on": ["M-CORE-01"],
            "estimated_duration_minutes": 35.0,
            "scheduled_start": "2026-10-26T08:45:00",
            "critical": False,
        },
        {
            "milestone_id": "M-V2R-01",
            "repo": "v2ray_tool",
            "name": "Dynamic Proxy Health Probe & Failover Daemon",
            "status": "pending",
            "depends_on": ["M-CORE-01"],
            "estimated_duration_minutes": 60.0,
            "scheduled_start": "2026-10-26T08:45:00",
            "critical": False,
        },
        {
            "milestone_id": "M-V2R-02",
            "repo": "v2ray_tool",
            "name": "Egress Routing Matrix & Quota Traffic Shaper",
            "status": "pending",
            "depends_on": ["M-V2R-01"],
            "estimated_duration_minutes": 50.0,
            "scheduled_start": "2026-10-26T09:45:00",
            "critical": False,
        },
        {
            "milestone_id": "M-ORCH-03",
            "repo": "hermes_orchestrator",
            "name": "Cross-Repo Coordinated Deployment & Gatekeeper",
            "status": "pending",
            "depends_on": ["M-TELE-02", "M-V2R-02"],
            "estimated_duration_minutes": 30.0,
            "scheduled_start": "2026-10-26T10:35:00",
            "critical": False,
        },
    ]

    completed_milestones: Set[str] = {"M-CORE-01"}

    schedule_output = schedule_pipeline(example_milestones, completed=completed_milestones)
    print(json.dumps(schedule_output, indent=2))