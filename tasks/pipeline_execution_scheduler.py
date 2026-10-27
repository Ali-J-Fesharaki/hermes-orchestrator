"""tasks/pipeline_execution_scheduler.py
Pipeline Execution Scheduler & Dependency-Aware Dispatch Sequencer.

Subsystems:
1. DependencyGraph:
   - DAG constructed per planning cycle from repository metadata and milestone annotations.
   - SchedulableUnit nodes with PRODUCES_ARTIFACT, SHARED_RESOURCE, and SOFT_ORDERING edges.
   - Kahn's algorithm cycle detection with lowest-priority edge pruning and logging.
2. ConstrainedScheduler:
   - Priority-aware list-scheduling algorithm over the DependencyGraph.
   - Constrained by CapacityPlanner token budgets, ThreadPoolExecutor slots, CircuitBreakers,
     active AnomalyVerdicts, and max_concurrent_per_repo limits.
   - Generates immutable ExecutionSchedule snapshots with DispatchSlot assignments.
3. DeadlineMonitor:
   - Tracks milestone hard deadlines and computes exponential deadline pressure urgency.
   - Emits WARNING and CRITICAL DeadlineRiskAlerts.
   - Promotes critical units to priority_override mode, relaxing concurrency and anomaly constraints.
   - Bridges alerts to the Predictive Analytics PreemptiveActionDispatcher.
4. ScheduleExecutor:
   - Bridges ExecutionSchedule to the autonomous orchestrator dispatch loop.
   - Precondition validation (circuit breakers, token quotas, predecessor commits).
   - ThreadPoolExecutor dispatch with token budget context injection.
   - Event auditing to dashboards/execution_schedule.jsonl.
   - Rolling schedule adherence metrics over 50 cycles contributing to PipelineHealthScorer.
   - Mid-cycle failure and circuit-breaker trip dynamic re-scheduling.
5. HTTP API Endpoints:
   - GET  /api/pipeline/scheduler/schedule
   - GET  /api/pipeline/scheduler/dependencies
   - GET  /api/pipeline/scheduler/adherence
   - POST /api/pipeline/scheduler/priority-override
"""

from __future__ import annotations

import collections
import concurrent.futures
from dataclasses import asdict, dataclass, field
from enum import Enum
import json
import logging
import math
import os
from pathlib import Path
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# OpenTelemetry Instrumentation Setup (Graceful Fallback)
# ---------------------------------------------------------------------------
try:
    from opentelemetry import trace

    tracer = trace.get_tracer("hermes.pipeline.scheduler")
except ImportError:

    class _NoOpSpan:

        def __enter__(self) -> "_NoOpSpan":
            return self

        def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            pass

        def set_attribute(self, key: str, value: Any) -> None:
            pass

        def add_event(
            self, name: str, attributes: Optional[Dict[str, Any]] = None
        ) -> None:
            pass

    class _NoOpTracer:

        def start_as_current_span(
            self, name: str, *args: Any, **kwargs: Any
        ) -> _NoOpSpan:
            return _NoOpSpan()

        def start_span(self, name: str, *args: Any, **kwargs: Any) -> _NoOpSpan:
            return _NoOpSpan()

    tracer = _NoOpTracer()  # type: ignore[assignment]

logger = logging.getLogger("hermes.pipeline.scheduler")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
    )
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# Enums and Data Models
# ---------------------------------------------------------------------------
class DependencyEdgeType(str, Enum):
    """Classification of dependency relations between SchedulableUnits."""

    PRODUCES_ARTIFACT = "PRODUCES_ARTIFACT"
    SHARED_RESOURCE = "SHARED_RESOURCE"
    SOFT_ORDERING = "SOFT_ORDERING"


class AlertSeverity(str, Enum):
    """Severity classification for deadline risks."""

    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class CircuitBreakerState(str, Enum):
    """Circuit breaker operational states."""

    CLOSED = "CLOSED"
    HALF_OPEN = "HALF_OPEN"
    OPEN = "OPEN"


@dataclass(frozen=True)
class SchedulableUnit:
    """Atomic unit of milestone work schedulable by the engine."""

    repo_id: str
    milestone_id: str
    estimated_cost_tokens: int = 1500
    estimated_duration_ticks: int = 2
    priority_score: float = 1.0
    hard_deadline_tick: Optional[int] = None
    shared_resource_pools: Tuple[str, ...] = ()
    required_artifacts: Tuple[str, ...] = ()
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def unit_id(self) -> str:
        return f"{self.repo_id}::{self.milestone_id}"


@dataclass(frozen=True)
class DependencyEdge:
    """Directed edge in the DependencyGraph."""

    source_unit_id: str
    target_unit_id: str
    edge_type: DependencyEdgeType
    weight: float = 1.0
    description: str = ""


@dataclass(frozen=True)
class DispatchSlot:
    """Assigned execution slot within an ExecutionSchedule."""

    schedulable_unit: SchedulableUnit
    assigned_tick: int
    assigned_thread_slot: int
    token_budget_allocation: int
    estimated_completion_tick: int
    composite_priority: float = 0.0
    priority_override: bool = False
    assigned_shared_resources: Tuple[str, ...] = ()

    @property
    def unit_id(self) -> str:
        return self.schedulable_unit.unit_id


@dataclass(frozen=True)
class ExecutionSchedule:
    """Immutable snapshot of scheduled dispatches across a planning cycle."""

    schedule_id: str
    created_at_tick: int
    slots: Tuple[DispatchSlot, ...]
    unassigned_units: Tuple[SchedulableUnit, ...] = ()
    rejection_reasons: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def get_slots_for_tick(self, tick: int) -> List[DispatchSlot]:
        return [s for s in self.slots if s.assigned_tick == tick]


@dataclass
class DeadlineRiskAlert:
    """Alert emitted when a milestone approaches its hard deadline."""

    alert_id: str
    unit_id: str
    repo_id: str
    milestone_id: str
    current_tick: int
    hard_deadline_tick: int
    ticks_remaining: int
    estimated_duration_ticks: int
    severity: AlertSeverity
    urgency_multiplier: float
    message: str
    timestamp: float = field(default_factory=time.time)


@dataclass
class ScheduleAdherenceMetric:
    """Historical record of an individual dispatch adherence event."""

    tick: int
    repo_id: str
    milestone_id: str
    scheduled_tick: int
    actual_dispatch_tick: int
    delay_ticks: int
    delay_reason: Optional[str]
    token_budget: int
    thread_slot: int
    on_time: bool


# ---------------------------------------------------------------------------
# Configuration Engine
# ---------------------------------------------------------------------------
class SchedulerConfig:
    """Configuration parameters for execution scheduling and sequencing."""

    def __init__(
        self,
        max_concurrent_per_repo: int = 2,
        global_thread_capacity: int = 8,
        anomaly_depriority_factor: float = 0.6,
        deadline_pressure_base: float = 2.0,
        rolling_adherence_window: int = 50,
        planning_horizon_ticks: int = 24,
        jsonl_log_path: str = "dashboards/execution_schedule.jsonl",
        default_throughput_confidence: float = 0.95,
    ) -> None:
        self.max_concurrent_per_repo = max_concurrent_per_repo
        self.global_thread_capacity = global_thread_capacity
        self.anomaly_depriority_factor = anomaly_depriority_factor
        self.deadline_pressure_base = deadline_pressure_base
        self.rolling_adherence_window = rolling_adherence_window
        self.planning_horizon_ticks = planning_horizon_ticks
        self.jsonl_log_path = jsonl_log_path
        self.default_throughput_confidence = default_throughput_confidence

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SchedulerConfig":
        return cls(
            max_concurrent_per_repo=int(data.get("max_concurrent_per_repo", 2)),
            global_thread_capacity=int(data.get("global_thread_capacity", 8)),
            anomaly_depriority_factor=float(
                data.get("anomaly_depriority_factor", 0.6)
            ),
            deadline_pressure_base=float(data.get("deadline_pressure_base", 2.0)),
            rolling_adherence_window=int(data.get("rolling_adherence_window", 50)),
            planning_horizon_ticks=int(data.get("planning_horizon_ticks", 24)),
            jsonl_log_path=str(
                data.get("jsonl_log_path", "dashboards/execution_schedule.jsonl")
            ),
            default_throughput_confidence=float(
                data.get("default_throughput_confidence", 0.95)
            ),
        )


# ---------------------------------------------------------------------------
# Subsystem 1: DependencyGraph
# ---------------------------------------------------------------------------
class DependencyGraph:
    """Directed Acyclic Graph (DAG) for repository milestones.

    Encodes artifact dependencies, shared resource pool constraints, and soft
    orderings. Employs Kahn's algorithm with priority-aware cycle resolution.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._nodes: Dict[str, SchedulableUnit] = {}
        self._edges: Dict[Tuple[str, str], DependencyEdge] = {}
        self._predecessors: Dict[str, Set[str]] = collections.defaultdict(set)
        self._successors: Dict[str, Set[str]] = collections.defaultdict(set)

    def add_unit(self, unit: SchedulableUnit) -> None:
        with self._lock:
            self._nodes[unit.unit_id] = unit
            if unit.unit_id not in self._predecessors:
                self._predecessors[unit.unit_id] = set()
            if unit.unit_id not in self._successors:
                self._successors[unit.unit_id] = set()

    def add_edge(
        self,
        source_unit_id: str,
        target_unit_id: str,
        edge_type: DependencyEdgeType,
        weight: float = 1.0,
        description: str = "",
    ) -> bool:
        with self._lock:
            if (
                source_unit_id not in self._nodes
                or target_unit_id not in self._nodes
            ):
                logger.debug(
                    "Cannot add edge (%s -> %s): missing node(s)",
                    source_unit_id,
                    target_unit_id,
                )
                return False

            edge = DependencyEdge(
                source_unit_id=source_unit_id,
                target_unit_id=target_unit_id,
                edge_type=edge_type,
                weight=weight,
                description=description,
            )
            self._edges[(source_unit_id, target_unit_id)] = edge
            self._successors[source_unit_id].add(target_unit_id)
            self._predecessors[target_unit_id].add(source_unit_id)
            return True

    def remove_edge(self, source_unit_id: str, target_unit_id: str) -> None:
        with self._lock:
            self._edges.pop((source_unit_id, target_unit_id), None)
            if source_unit_id in self._successors:
                self._successors[source_unit_id].discard(target_unit_id)
            if target_unit_id in self._predecessors:
                self._predecessors[target_unit_id].discard(source_unit_id)

    def get_unit(self, unit_id: str) -> Optional[SchedulableUnit]:
        with self._lock:
            return self._nodes.get(unit_id)

    def get_all_units(self) -> List[SchedulableUnit]:
        with self._lock:
            return list(self._nodes.values())

    def get_predecessors(self, unit_id: str) -> Set[str]:
        with self._lock:
            return set(self._predecessors.get(unit_id, set()))

    def get_successors(self, unit_id: str) -> Set[str]:
        with self._lock:
            return set(self._successors.get(unit_id, set()))

    def break_cycles_kahns(self) -> List[DependencyEdge]:
        """Runs Kahn's algorithm; breaks cycles at the lowest-priority edge.

        Edge priority rank:
        1. SOFT_ORDERING (least critical, pruned first)
        2. SHARED_RESOURCE (serialization constraint)
        3. PRODUCES_ARTIFACT (hard artifact prerequisite, pruned last)
        Ties are broken by lower edge weight and lower source node priority.
        """
        with self._lock:
            broken_edges: List[DependencyEdge] = []
            while True:
                # 1. Compute in-degrees considering only active edges
                in_degree: Dict[str, int] = {uid: 0 for uid in self._nodes}
                for src, dst in self._edges:
                    in_degree[dst] = in_degree.get(dst, 0) + 1

                # 2. Queue zero in-degree nodes
                queue = collections.deque(
                    [uid for uid, deg in in_degree.items() if deg == 0]
                )
                visited_count = 0

                # Temporary tracking of current in-degree during traversal
                cur_in_degree = in_degree.copy()

                while queue:
                    curr = queue.popleft()
                    visited_count += 1
                    for succ in self._successors[curr]:
                        cur_in_degree[succ] -= 1
                        if cur_in_degree[succ] == 0:
                            queue.append(succ)

                if visited_count == len(self._nodes):
                    # Fully acyclic DAG confirmed
                    break

                # 3. Cycle detected: identify unvisited nodes involved in cycles
                cycle_candidate_nodes = {
                    uid for uid, deg in cur_in_degree.items() if deg > 0
                }

                # Find edges connecting nodes within the candidate set
                cycle_edges: List[DependencyEdge] = [
                    edge
                    for (src, dst), edge in self._edges.items()
                    if src in cycle_candidate_nodes
                    and dst in cycle_candidate_nodes
                ]

                if not cycle_edges:
                    # Fallback safety in case disconnected cycles exist
                    cycle_edges = list(self._edges.values())

                if not cycle_edges:
                    break

                # Edge type penalty ranking (lower score = pruned first)
                edge_rank = {
                    DependencyEdgeType.SOFT_ORDERING: 1,
                    DependencyEdgeType.SHARED_RESOURCE: 2,
                    DependencyEdgeType.PRODUCES_ARTIFACT: 3,
                }

                def edge_sort_key(e: DependencyEdge) -> Tuple[int, float, float]:
                    src_unit = self._nodes.get(e.source_unit_id)
                    src_prio = src_unit.priority_score if src_unit else 1.0
                    return (edge_rank.get(e.edge_type, 1), e.weight, src_prio)

                cycle_edges.sort(key=edge_sort_key)
                edge_to_break = cycle_edges[0]

                logger.warning(
                    "[CYCLE DETECTED] Breaking lowest-priority edge: %s -> %s "
                    "(type=%s, weight=%.2f, desc=%s)",
                    edge_to_break.source_unit_id,
                    edge_to_break.target_unit_id,
                    edge_to_break.edge_type.value,
                    edge_to_break.weight,
                    edge_to_break.description or "None",
                )

                self.remove_edge(
                    edge_to_break.source_unit_id, edge_to_break.target_unit_id
                )
                broken_edges.append(edge_to_break)

            return broken_edges

    def build_from_manifests(
        self, manifests: List[Dict[str, Any]], units: List[SchedulableUnit]
    ) -> None:
        """Constructs graph from repository manifests and explicit units."""
        with self._lock:
            with tracer.start_as_current_span("dependency_graph.build"):
                for unit in units:
                    self.add_unit(unit)

                for manifest in manifests:
                    repo_id = manifest.get("repo_id") or manifest.get(
                        "repository", ""
                    )
                    deps = manifest.get("dependencies", [])
                    for d in deps:
                        src = d.get("source_unit_id")
                        dst = d.get("target_unit_id")
                        if not src or not dst:
                            # Handle shorthand dependencies specified in repo manifest
                            src_milestone = d.get("depends_on_milestone")
                            src_repo = d.get("depends_on_repo", repo_id)
                            dst_milestone = d.get("milestone_id")
                            if src_milestone and dst_milestone:
                                src = f"{src_repo}::{src_milestone}"
                                dst = f"{repo_id}::{dst_milestone}"

                        if src and dst and src in self._nodes and dst in self._nodes:
                            raw_edge_type = d.get(
                                "edge_type",
                                DependencyEdgeType.PRODUCES_ARTIFACT.value,
                            )
                            try:
                                edge_type = DependencyEdgeType(raw_edge_type)
                            except ValueError:
                                edge_type = DependencyEdgeType.PRODUCES_ARTIFACT

                            weight = float(d.get("weight", 1.0))
                            desc = str(d.get("description", ""))
                            self.add_edge(
                                src, dst, edge_type, weight=weight, description=desc
                            )

                # Cross-reference explicit milestone metadata annotations
                for unit in units:
                    meta = unit.metadata
                    depends_on = meta.get("depends_on", [])
                    for dep in depends_on:
                        if isinstance(dep, str):
                            target_src = (
                                dep
                                if "::" in dep
                                else f"{unit.repo_id}::{dep}"
                            )
                            if target_src in self._nodes:
                                self.add_edge(
                                    target_src,
                                    unit.unit_id,
                                    DependencyEdgeType.PRODUCES_ARTIFACT,
                                    1.0,
                                    "metadata_depends_on",
                                )

                    soft_ordering = meta.get("after_milestones", [])
                    for dep in soft_ordering:
                        if isinstance(dep, str):
                            target_src = (
                                dep
                                if "::" in dep
                                else f"{unit.repo_id}::{dep}"
                            )
                            if target_src in self._nodes:
                                self.add_edge(
                                    target_src,
                                    unit.unit_id,
                                    DependencyEdgeType.SOFT_ORDERING,
                                    0.5,
                                    "metadata_soft_ordering",
                                )

                # Enforce acyclicity
                self.break_cycles_kahns()

    def to_adjacency_list(self) -> Dict[str, Any]:
        """Serializes graph to adjacency format for API queries."""
        with self._lock:
            nodes_data = [
                {
                    "unit_id": u.unit_id,
                    "repo_id": u.repo_id,
                    "milestone_id": u.milestone_id,
                    "estimated_cost_tokens": u.estimated_cost_tokens,
                    "estimated_duration_ticks": u.estimated_duration_ticks,
                    "priority_score": u.priority_score,
                    "hard_deadline_tick": u.hard_deadline_tick,
                    "shared_resources": list(u.shared_resource_pools),
                }
                for u in self._nodes.values()
            ]
            edges_data = [
                {
                    "source": e.source_unit_id,
                    "target": e.target_unit_id,
                    "edge_type": e.edge_type.value,
                    "weight": e.weight,
                    "description": e.description,
                }
                for e in self._edges.values()
            ]
            adj: Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)
            for e in self._edges.values():
                adj[e.source_unit_id].append(
                    {
                        "target": e.target_unit_id,
                        "edge_type": e.edge_type.value,
                        "weight": e.weight,
                    }
                )

            return {
                "node_count": len(nodes_data),
                "edge_count": len(edges_data),
                "nodes": nodes_data,
                "edges": edges_data,
                "adjacency": dict(adj),
            }


# ---------------------------------------------------------------------------
# Subsystem 3: DeadlineMonitor
# ---------------------------------------------------------------------------
class DeadlineMonitor:
    """Monitors milestone completion deadlines and generates urgency multipliers."""

    def __init__(self, config: Optional[SchedulerConfig] = None) -> None:
        self.config = config or SchedulerConfig()
        self._lock = threading.RLock()
        self._alerts_history: List[DeadlineRiskAlert] = []
        self._priority_overrides: Set[str] = set()
        self._external_action_dispatcher: Optional[Callable[[Dict[str, Any]], None]] = None

    def register_preemptive_action_dispatcher(
        self, dispatcher_callback: Callable[[Dict[str, Any]], None]
    ) -> None:
        """Connects to Predictive Analytics PreemptiveActionDispatcher."""
        with self._lock:
            self._external_action_dispatcher = dispatcher_callback

    def compute_urgency_multiplier(
        self, unit: SchedulableUnit, current_tick: int
    ) -> float:
        """Calculates exponential urgency multiplier based on remaining deadline headroom."""
        if unit.hard_deadline_tick is None:
            return 1.0

        ticks_remaining = unit.hard_deadline_tick - current_tick
        headroom = max(1, ticks_remaining - unit.estimated_duration_ticks)

        # Base exponential formula: max(1.0, base ^ (1 / headroom))
        pressure = math.pow(self.config.deadline_pressure_base, 1.0 / headroom)
        return max(1.0, pressure)

    def evaluate_deadlines(
        self, units: List[SchedulableUnit], current_tick: int
    ) -> List[DeadlineRiskAlert]:
        """Assesses deadline risks across all active units."""
        with self._lock:
            with tracer.start_as_current_span("deadline_monitor.evaluate") as span:
                alerts: List[DeadlineRiskAlert] = []
                for unit in units:
                    if unit.hard_deadline_tick is None:
                        continue

                    ticks_remaining = unit.hard_deadline_tick - current_tick
                    urgency_mult = self.compute_urgency_multiplier(unit, current_tick)

                    severity: Optional[AlertSeverity] = None
                    msg = ""

                    if ticks_remaining < unit.estimated_duration_ticks:
                        severity = AlertSeverity.CRITICAL
                        self._priority_overrides.add(unit.unit_id)
                        msg = (
                            f"Milestone {unit.unit_id} exceeded deadline margin! "
                            f"Remaining ticks ({ticks_remaining}) < estimated duration "
                            f"({unit.estimated_duration_ticks}). Promoted to PRIORITY_OVERRIDE."
                        )
                    elif ticks_remaining < (unit.estimated_duration_ticks * 1.5):
                        severity = AlertSeverity.WARNING
                        msg = (
                            f"Milestone {unit.unit_id} approaching hard deadline at tick "
                            f"{unit.hard_deadline_tick}. Remaining ticks: {ticks_remaining}."
                        )

                    if severity is not None:
                        alert = DeadlineRiskAlert(
                            alert_id=f"alert_{unit.unit_id}_{current_tick}",
                            unit_id=unit.unit_id,
                            repo_id=unit.repo_id,
                            milestone_id=unit.milestone_id,
                            current_tick=current_tick,
                            hard_deadline_tick=unit.hard_deadline_tick,
                            ticks_remaining=ticks_remaining,
                            estimated_duration_ticks=unit.estimated_duration_ticks,
                            severity=severity,
                            urgency_multiplier=urgency_mult,
                            message=msg,
                        )
                        alerts.append(alert)
                        self._alerts_history.append(alert)

                        logger.info(
                            "[DEADLINE ALERT] [%s] %s (Urgency Multiplier: %.2f)",
                            severity.value,
                            msg,
                            urgency_mult,
                        )

                        # Cross-system visibility bridge
                        if self._external_action_dispatcher:
                            try:
                                self._external_action_dispatcher(
                                    {
                                        "source": "DeadlineMonitor",
                                        "alert_type": "DEADLINE_PRESSURE",
                                        "severity": severity.value,
                                        "unit_id": unit.unit_id,
                                        "repo_id": unit.repo_id,
                                        "current_tick": current_tick,
                                        "ticks_remaining": ticks_remaining,
                                        "urgency_multiplier": urgency_mult,
                                        "message": msg,
                                    }
                                )
                            except Exception as ex:
                                logger.error(
                                    "Failed dispatching to external PreemptiveActionDispatcher: %s",
                                    ex,
                                )

                span.set_attribute("scheduler.deadline.alerts_emitted", len(alerts))
                return alerts

    def is_priority_overridden(self, unit_id: str) -> bool:
        with self._lock:
            return unit_id in self._priority_overrides

    def add_manual_priority_override(self, unit_id: str) -> None:
        with self._lock:
            self._priority_overrides.add(unit_id)

    def remove_priority_override(self, unit_id: str) -> None:
        with self._lock:
            self._priority_overrides.discard(unit_id)

    def get_recent_alerts(self, limit: int = 50) -> List[DeadlineRiskAlert]:
        with self._lock:
            return list(self._alerts_history[-limit:])


# ---------------------------------------------------------------------------
# Subsystem 2: ConstrainedScheduler
# ---------------------------------------------------------------------------
class ConstrainedScheduler:
    """Priority-aware list-scheduler resolving DAGs with multi-resource constraints."""

    def __init__(
        self,
        config: Optional[SchedulerConfig] = None,
        deadline_monitor: Optional[DeadlineMonitor] = None,
    ) -> None:
        self.config = config or SchedulerConfig()
        self.deadline_monitor = deadline_monitor or DeadlineMonitor(self.config)
        self._manual_overrides: Dict[str, float] = {}
        self._lock = threading.RLock()

    def set_manual_override(self, unit_id: str, boost_score: float) -> None:
        with self._lock:
            self._manual_overrides[unit_id] = boost_score
            self.deadline_monitor.add_manual_priority_override(unit_id)

    def compute_composite_priority(
        self,
        unit: SchedulableUnit,
        current_tick: int,
        anomaly_verdicts: Dict[str, Any],
        forecast_confidence: float,
    ) -> float:
        """Computes priority balancing deadline urgency, anomalies, and confidence.

        Formula:
        composite = base_priority * deadline_urgency * (1 - anomaly_depriority) * confidence
        """
        urgency_multiplier = self.deadline_monitor.compute_urgency_multiplier(
            unit, current_tick
        )
        is_override = self.deadline_monitor.is_priority_overridden(unit.unit_id)

        # Check for repo-level anomaly deprioritization
        anomaly_info = anomaly_verdicts.get(unit.repo_id, {})
        has_critical_anomaly = (
            anomaly_info.get("severity") == "CRITICAL"
            or anomaly_info.get("is_anomalous") is True
        )

        anomaly_penalty = 0.0
        if has_critical_anomaly and not is_override:
            anomaly_penalty = self.config.anomaly_depriority_factor

        base_prio = unit.priority_score
        with self._lock:
            if unit.unit_id in self._manual_overrides:
                base_prio += self._manual_overrides[unit.unit_id]

        composite = (
            base_prio
            * urgency_multiplier
            * (1.0 - anomaly_penalty)
            * forecast_confidence
        )

        # Overrides receive priority floor boost
        if is_override:
            composite = max(composite, base_prio * 3.0)

        return max(0.01, composite)

    def generate_schedule(
        self,
        graph: DependencyGraph,
        current_tick: int,
        resource_allocation_plan: Dict[str, Any],
        circuit_breaker_states: Dict[str, CircuitBreakerState],
        anomaly_verdicts: Optional[Dict[str, Any]] = None,
        committed_units: Optional[Set[str]] = None,
        running_units: Optional[Dict[str, SchedulableUnit]] = None,
        throughput_confidence: Optional[float] = None,
    ) -> ExecutionSchedule:
        """Executes constrained list scheduling algorithm over the planning horizon."""
        anomaly_verdicts = anomaly_verdicts or {}
        committed_units = set(committed_units or set())
        running_units = dict(running_units or {})
        confidence = (
            throughput_confidence
            if throughput_confidence is not None
            else self.config.default_throughput_confidence
        )

        with tracer.start_as_current_span("constrained_scheduler.schedule") as span:
            span.set_attribute("scheduler.current_tick", current_tick)
            all_units = graph.get_all_units()
            self.deadline_monitor.evaluate_deadlines(all_units, current_tick)

            # Per-repo token budgets extracted from ResourceAllocationPlan
            repo_token_budgets: Dict[str, int] = {}
            repo_max_slots: Dict[str, int] = {}
            for repo_id, alloc in resource_allocation_plan.get(
                "allocations", {}
            ).items():
                repo_token_budgets[repo_id] = int(alloc.get("token_budget", 100000))
                repo_max_slots[repo_id] = int(
                    alloc.get("thread_slots", self.config.max_concurrent_per_repo)
                )

            # Track completed units including current committed units
            simulated_completed = set(committed_units)
            # Units currently running occupy capacity
            for r_unit in running_units.values():
                simulated_completed.add(r_unit.unit_id)

            unassigned: List[SchedulableUnit] = []
            rejections: Dict[str, str] = {}
            assigned_slots: List[DispatchSlot] = []

            # Simulation state across future ticks [current_tick ... current_tick + horizon]
            horizon = self.config.planning_horizon_ticks
            ticks = list(range(current_tick, current_tick + horizon))

            # Concurrency tracking per tick
            # tick -> thread_slot_index -> occupied (bool)
            slot_occupancy: Dict[int, List[bool]] = {
                t: [False] * self.config.global_thread_capacity for t in ticks
            }
            # tick -> repo_id -> count of running tasks
            repo_concurrency: Dict[int, Dict[str, int]] = {
                t: collections.defaultdict(int) for t in ticks
            }
            # tick -> set of shared resources locked
            shared_resource_occupancy: Dict[int, Set[str]] = {
                t: set() for t in ticks
            }

            # Seed currently running units into simulation occupancy
            for r_id, r_unit in running_units.items():
                dur = r_unit.estimated_duration_ticks
                for offset in range(dur):
                    t = current_tick + offset
                    if t in slot_occupancy:
                        # Mark first available slot occupied
                        for s_idx in range(self.config.global_thread_capacity):
                            if not slot_occupancy[t][s_idx]:
                                slot_occupancy[t][s_idx] = True
                                break
                        repo_concurrency[t][r_unit.repo_id] += 1
                        for res in r_unit.shared_resource_pools:
                            shared_resource_occupancy[t].add(res)

            candidate_pool = [
                u
                for u in all_units
                if u.unit_id not in committed_units
                and u.unit_id not in running_units
            ]

            # Priority-aware list scheduling
            scheduled_unit_ids: Set[str] = set()

            for tick in ticks:
                # 1. Identify ready units: all graph predecessors satisfied
                ready_units: List[Tuple[float, SchedulableUnit]] = []
                for unit in candidate_pool:
                    if unit.unit_id in scheduled_unit_ids:
                        continue

                    # Predecessor requirement check
                    preds = graph.get_predecessors(unit.unit_id)
                    hard_satisfied = True
                    for pred in preds:
                        edge = graph._edges.get((pred, unit.unit_id))
                        if edge and edge.edge_type in (
                            DependencyEdgeType.PRODUCES_ARTIFACT,
                            DependencyEdgeType.SOFT_ORDERING,
                        ):
                            if pred not in simulated_completed:
                                hard_satisfied = False
                                break

                    if not hard_satisfied:
                        continue

                    prio = self.compute_composite_priority(
                        unit, tick, anomaly_verdicts, confidence
                    )
                    ready_units.append((prio, unit))

                # Sort ready units by composite priority descending
                ready_units.sort(key=lambda x: x[0], reverse=True)

                # 2. Greedy assignment to available resources
                for prio, unit in ready_units:
                    if unit.unit_id in scheduled_unit_ids:
                        continue

                    repo_id = unit.repo_id
                    breaker_state = circuit_breaker_states.get(
                        repo_id, CircuitBreakerState.CLOSED
                    )
                    is_override = self.deadline_monitor.is_priority_overridden(
                        unit.unit_id
                    )

                    # Constraint (a): CircuitBreaker exclusion
                    if breaker_state == CircuitBreakerState.OPEN:
                        rejections[unit.unit_id] = (
                            f"Circuit breaker for repo {repo_id} is OPEN"
                        )
                        continue

                    # Constraint (b): Repo concurrency limit
                    effective_max_concurrent = repo_max_slots.get(
                        repo_id, self.config.max_concurrent_per_repo
                    )
                    if breaker_state == CircuitBreakerState.HALF_OPEN:
                        effective_max_concurrent = 1
                    if is_override:
                        effective_max_concurrent += 2  # Relax concurrency limit

                    # Check span of duration for slot, concurrency, and shared resources
                    duration = max(1, unit.estimated_duration_ticks)
                    fits_time_window = True
                    assigned_thread_idx: Optional[int] = None

                    # Check global thread slots across all duration ticks
                    # We pick a single slot index consistent throughout duration if possible
                    candidate_slots = [
                        s_idx
                        for s_idx in range(self.config.global_thread_capacity)
                        if all(
                            (
                                tick + offset in slot_occupancy
                                and not slot_occupancy[tick + offset][s_idx]
                            )
                            for offset in range(duration)
                            if (tick + offset) in slot_occupancy
                        )
                    ]

                    if not candidate_slots:
                        continue  # No available thread slot at this tick

                    assigned_thread_idx = candidate_slots[0]

                    # Check per-repo concurrency across duration
                    for offset in range(duration):
                        target_t = tick + offset
                        if target_t in repo_concurrency:
                            if (
                                repo_concurrency[target_t][repo_id]
                                >= effective_max_concurrent
                            ):
                                fits_time_window = False
                                break

                    if not fits_time_window:
                        continue

                    # Check SHARED_RESOURCE mutual exclusivity
                    if unit.shared_resource_pools:
                        conflict = False
                        for offset in range(duration):
                            target_t = tick + offset
                            if target_t in shared_resource_occupancy:
                                for res in unit.shared_resource_pools:
                                    if res in shared_resource_occupancy[target_t]:
                                        conflict = True
                                        break
                            if conflict:
                                break
                        if conflict:
                            continue  # Wait for shared resource release

                    # Constraint (c): Token Budget
                    rem_budget = repo_token_budgets.get(repo_id, 100000)
                    if (
                        rem_budget < unit.estimated_cost_tokens
                        and not is_override
                    ):
                        rejections[unit.unit_id] = (
                            f"Insufficient token budget for repo {repo_id}: "
                            f"needs {unit.estimated_cost_tokens}, has {rem_budget}"
                        )
                        continue

                    # Grant dispatch slot!
                    repo_token_budgets[repo_id] = max(
                        0, rem_budget - unit.estimated_cost_tokens
                    )
                    completion_tick = tick + duration

                    for offset in range(duration):
                        target_t = tick + offset
                        if target_t in slot_occupancy:
                            slot_occupancy[target_t][assigned_thread_idx] = True
                            repo_concurrency[target_t][repo_id] += 1
                            for res in unit.shared_resource_pools:
                                shared_resource_occupancy[target_t].add(res)

                    slot = DispatchSlot(
                        schedulable_unit=unit,
                        assigned_tick=tick,
                        assigned_thread_slot=assigned_thread_idx,
                        token_budget_allocation=unit.estimated_cost_tokens,
                        estimated_completion_tick=completion_tick,
                        composite_priority=round(prio, 4),
                        priority_override=is_override,
                        assigned_shared_resources=unit.shared_resource_pools,
                    )
                    assigned_slots.append(slot)
                    scheduled_unit_ids.add(unit.unit_id)
                    simulated_completed.add(unit.unit_id)

            unassigned = [
                u for u in candidate_pool if u.unit_id not in scheduled_unit_ids
            ]

            schedule = ExecutionSchedule(
                schedule_id=f"sched_{current_tick}_{int(time.time())}",
                created_at_tick=current_tick,
                slots=tuple(assigned_slots),
                unassigned_units=tuple(unassigned),
                rejection_reasons=rejections,
                metadata={
                    "total_assigned": len(assigned_slots),
                    "total_unassigned": len(unassigned),
                    "planning_horizon": horizon,
                },
            )

            span.set_attribute(
                "scheduler.schedule.assigned_slots", len(assigned_slots)
            )
            span.set_attribute(
                "scheduler.schedule.unassigned_units", len(unassigned)
            )
            return schedule


# ---------------------------------------------------------------------------
# Subsystem 4: ScheduleExecutor
# ---------------------------------------------------------------------------
class ScheduleExecutor:
    """Bridges ExecutionSchedule to the orchestrator dispatch loop."""

    def __init__(
        self,
        config: Optional[SchedulerConfig] = None,
        scheduler: Optional[ConstrainedScheduler] = None,
    ) -> None:
        self.config = config or SchedulerConfig()
        self.scheduler = scheduler or ConstrainedScheduler(self.config)
        self._current_schedule: Optional[ExecutionSchedule] = None
        self._adherence_history: collections.deque[ScheduleAdherenceMetric] = (
            collections.deque(maxlen=self.config.rolling_adherence_window)
        )
        self._lock = threading.RLock()
        self._reschedule_requested = False

        # Ensure directory for dashboard metrics
        os.makedirs(os.path.dirname(self.config.jsonl_log_path) or ".", exist_ok=True)

    def set_schedule(self, schedule: ExecutionSchedule) -> None:
        with self._lock:
            self._current_schedule = schedule
            self._reschedule_requested = False

    def request_reschedule(self) -> None:
        with self._lock:
            self._reschedule_requested = True

    def is_reschedule_needed(self) -> bool:
        with self._lock:
            return self._reschedule_requested

    def record_adherence_event(self, metric: ScheduleAdherenceMetric) -> None:
        with self._lock:
            self._adherence_history.append(metric)

        # Audit to JSONL
        event_entry = {
            "timestamp": time.time(),
            "tick": metric.tick,
            "repo_id": metric.repo_id,
            "milestone_id": metric.milestone_id,
            "scheduled_tick": metric.scheduled_tick,
            "actual_dispatch_tick": metric.actual_dispatch_tick,
            "delay_ticks": metric.delay_ticks,
            "delay_reason": metric.delay_reason or "NONE",
            "token_budget": metric.token_budget,
            "thread_slot": metric.thread_slot,
            "on_time": metric.on_time,
        }
        try:
            with open(self.config.jsonl_log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(event_entry) + "\n")
        except Exception as ex:
            logger.error("Failed writing to execution schedule JSONL: %s", ex)

    def calculate_schedule_adherence(self) -> float:
        """Returns on-time dispatch ratio over the rolling window [0.0 - 1.0]."""
        with self._lock:
            if not self._adherence_history:
                return 1.0
            on_time = sum(1 for m in self._adherence_history if m.on_time)
            return round(on_time / len(self._adherence_history), 4)

    def get_health_score_contribution(self) -> Dict[str, Any]:
        """Provides scheduling_sub_score to PipelineHealthScorer."""
        with self._lock:
            adherence = self.calculate_schedule_adherence()
            delays_by_reason: Dict[str, int] = collections.defaultdict(int)
            total_delay_ticks = 0
            for m in self._adherence_history:
                if not m.on_time:
                    reason = m.delay_reason or "UNKNOWN"
                    delays_by_reason[reason] += 1
                    total_delay_ticks += m.delay_ticks

            evaluated = len(self._adherence_history)
            avg_delay = (
                round(total_delay_ticks / evaluated, 2) if evaluated > 0 else 0.0
            )

            return {
                "sub_score_name": "scheduling_sub_score",
                "score": round(adherence * 100.0, 2),
                "adherence_ratio": adherence,
                "rolling_cycles_evaluated": evaluated,
                "average_delay_ticks": avg_delay,
                "delays_by_reason": dict(delays_by_reason),
            }

    def execute_tick(
        self,
        current_tick: int,
        thread_pool: concurrent.futures.ThreadPoolExecutor,
        dispatch_fn: Callable[[SchedulableUnit, Dict[str, Any]], None],
        circuit_breaker_states: Dict[str, CircuitBreakerState],
        available_token_pool: Dict[str, int],
        committed_milestones: Set[str],
        dependency_graph: DependencyGraph,
    ) -> List[DispatchSlot]:
        """Validates preconditions and dispatches slots matching current_tick."""
        with tracer.start_as_current_span("schedule_executor.tick") as span:
            span.set_attribute("scheduler.tick", current_tick)
            with self._lock:
                if not self._current_schedule:
                    return []

                due_slots = self._current_schedule.get_slots_for_tick(current_tick)

            dispatched_slots: List[DispatchSlot] = []

            for slot in due_slots:
                unit = slot.schedulable_unit
                repo_id = unit.repo_id
                delay_reason: Optional[str] = None

                # Precondition 1: Circuit Breaker
                breaker = circuit_breaker_states.get(
                    repo_id, CircuitBreakerState.CLOSED
                )
                if breaker == CircuitBreakerState.OPEN:
                    delay_reason = f"CIRCUIT_BREAKER_OPEN ({repo_id})"
                    self.request_reschedule()

                # Precondition 2: Quota exhaustion
                curr_quota = available_token_pool.get(repo_id, 100000)
                if (
                    curr_quota < slot.token_budget_allocation
                    and not slot.priority_override
                ):
                    delay_reason = (
                        f"TOKEN_QUOTA_EXHAUSTED ({curr_quota} < "
                        f"{slot.token_budget_allocation})"
                    )
                    self.request_reschedule()

                # Precondition 3: Predecessor commits verified
                preds = dependency_graph.get_predecessors(unit.unit_id)
                for pred in preds:
                    edge = dependency_graph._edges.get((pred, unit.unit_id))
                    if (
                        edge
                        and edge.edge_type == DependencyEdgeType.PRODUCES_ARTIFACT
                    ):
                        if pred not in committed_milestones:
                            delay_reason = f"PREDECESSOR_UNCOMMITTED ({pred})"
                            self.request_reschedule()
                            break

                is_on_time = delay_reason is None
                delay_ticks = 0 if is_on_time else 1

                metric = ScheduleAdherenceMetric(
                    tick=current_tick,
                    repo_id=unit.repo_id,
                    milestone_id=unit.milestone_id,
                    scheduled_tick=slot.assigned_tick,
                    actual_dispatch_tick=(
                        current_tick if is_on_time else current_tick + 1
                    ),
                    delay_ticks=delay_ticks,
                    delay_reason=delay_reason,
                    token_budget=slot.token_budget_allocation,
                    thread_slot=slot.assigned_thread_slot,
                    on_time=is_on_time,
                )
                self.record_adherence_event(metric)

                if is_on_time:
                    # Context dictionary passed to executor thread
                    dispatch_ctx = {
                        "allocated_tokens": slot.token_budget_allocation,
                        "assigned_thread_slot": slot.assigned_thread_slot,
                        "scheduled_tick": slot.assigned_tick,
                        "composite_priority": slot.composite_priority,
                        "priority_override": slot.priority_override,
                        "shared_resources": list(slot.assigned_shared_resources),
                    }
                    try:
                        thread_pool.submit(dispatch_fn, unit, dispatch_ctx)
                        dispatched_slots.append(slot)
                        logger.info(
                            "[DISPATCH] Unit %s -> Thread Slot %d (Tokens: %d, Priority: %.2f)",
                            unit.unit_id,
                            slot.assigned_thread_slot,
                            slot.token_budget_allocation,
                            slot.composite_priority,
                        )
                    except Exception as dispatch_err:
                        logger.error(
                            "Failed dispatching unit %s to thread pool: %s",
                            unit.unit_id,
                            dispatch_err,
                        )
                        self.request_reschedule()
                else:
                    logger.warning(
                        "[DISPATCH DELAYED] Unit %s deferred from tick %d: %s",
                        unit.unit_id,
                        current_tick,
                        delay_reason,
                    )

            span.set_attribute(
                "scheduler.dispatched_count", len(dispatched_slots)
            )
            return dispatched_slots


# ---------------------------------------------------------------------------
# HTTP API Handlers & Formatting
# ---------------------------------------------------------------------------
class PipelineSchedulerApi:
    """HTTP endpoint router providing Gantt schedules, dependencies, and adherence metrics."""

    def __init__(
        self,
        dependency_graph: DependencyGraph,
        constrained_scheduler: ConstrainedScheduler,
        schedule_executor: ScheduleExecutor,
    ) -> None:
        self.graph = dependency_graph
        self.scheduler = constrained_scheduler
        self.executor = schedule_executor

    def get_schedule_gantt(self) -> Dict[str, Any]:
        """GET /api/pipeline/scheduler/schedule: Gantt-compatible view."""
        with self.executor._lock:
            sched = self.executor._current_schedule
            if not sched:
                return {
                    "schedule_id": None,
                    "created_at_tick": 0,
                    "tasks": [],
                    "unassigned": [],
                }

            gantt_tasks = []
            for slot in sched.slots:
                u = slot.schedulable_unit
                preds = list(self.graph.get_predecessors(u.unit_id))
                gantt_tasks.append(
                    {
                        "id": u.unit_id,
                        "name": u.milestone_id,
                        "repo_id": u.repo_id,
                        "start_tick": slot.assigned_tick,
                        "end_tick": slot.estimated_completion_tick,
                        "duration_ticks": u.estimated_duration_ticks,
                        "thread_slot": slot.assigned_thread_slot,
                        "token_budget": slot.token_budget_allocation,
                        "composite_priority": slot.composite_priority,
                        "priority_override": slot.priority_override,
                        "dependencies": preds,
                        "shared_resources": list(slot.assigned_shared_resources),
                        "status": "SCHEDULED",
                    }
                )

            unassigned_list = [
                {
                    "id": u.unit_id,
                    "repo_id": u.repo_id,
                    "milestone_id": u.milestone_id,
                    "priority_score": u.priority_score,
                    "rejection_reason": sched.rejection_reasons.get(
                        u.unit_id, "Capacity Exhaustion"
                    ),
                }
                for u in sched.unassigned_units
            ]

            return {
                "schedule_id": sched.schedule_id,
                "created_at_tick": sched.created_at_tick,
                "tasks": gantt_tasks,
                "unassigned": unassigned_list,
                "metadata": sched.metadata,
            }

    def get_dependencies(self) -> Dict[str, Any]:
        """GET /api/pipeline/scheduler/dependencies: Adjacency list and edge types."""
        return self.graph.to_adjacency_list()

    def get_adherence(self) -> Dict[str, Any]:
        """GET /api/pipeline/scheduler/adherence: Adherence metrics and delay breakdown."""
        return self.executor.get_health_score_contribution()

    def post_priority_override(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """POST /api/pipeline/scheduler/priority-override: Manual milestone boost."""
        repo_id = payload.get("repo_id", "")
        milestone_id = payload.get("milestone_id", "")
        unit_id = payload.get("unit_id") or f"{repo_id}::{milestone_id}"
        boost_score = float(payload.get("boost_score", 5.0))
        reason = payload.get("reason", "Manual Operator Intervention")

        if not unit_id or unit_id == "::":
            return {"status": "error", "message": "Missing unit_id or repo/milestone"}

        self.scheduler.set_manual_override(unit_id, boost_score)
        self.executor.request_reschedule()

        logger.info(
            "[MANUAL OVERRIDE] Boosted unit %s by +%.2f (Reason: %s)",
            unit_id,
            boost_score,
            reason,
        )
        return {
            "status": "success",
            "unit_id": unit_id,
            "boost_score": boost_score,
            "reason": reason,
            "reschedule_triggered": True,
        }

    def handle_request(
        self, method: str, path: str, body: Optional[Dict[str, Any]] = None
    ) -> Tuple[int, Dict[str, Any]]:
        """Unified router compatible with Hermes dashboard control center."""
        clean_path = path.split("?")[0].rstrip("/")
        if method == "GET" and clean_path == "/api/pipeline/scheduler/schedule":
            return 200, self.get_schedule_gantt()
        elif method == "GET" and clean_path == "/api/pipeline/scheduler/dependencies":
            return 200, self.get_dependencies()
        elif method == "GET" and clean_path == "/api/pipeline/scheduler/adherence":
            return 200, self.get_adherence()
        elif method == "POST" and clean_path == "/api/pipeline/scheduler/priority-override":
            return 200, self.post_priority_override(body or {})
        return 404, {"error": "Not Found", "path": path}


# ---------------------------------------------------------------------------
# Integrated Facade: PipelineExecutionScheduler
# ---------------------------------------------------------------------------
class PipelineExecutionScheduler:
    """Master orchestrator combining graph, list scheduler, deadline monitor, and executor."""

    def __init__(self, config: Optional[SchedulerConfig] = None) -> None:
        self.config = config or SchedulerConfig()
        self.graph = DependencyGraph()
        self.deadline_monitor = DeadlineMonitor(self.config)
        self.constrained_scheduler = ConstrainedScheduler(
            self.config, self.deadline_monitor
        )
        self.executor = ScheduleExecutor(self.config, self.constrained_scheduler)
        self.api = PipelineSchedulerApi(
            self.graph, self.constrained_scheduler, self.executor
        )

    def plan_cycle(
        self,
        current_tick: int,
        units: List[SchedulableUnit],
        manifests: List[Dict[str, Any]],
        resource_allocation_plan: Dict[str, Any],
        circuit_breaker_states: Dict[str, CircuitBreakerState],
        anomaly_verdicts: Optional[Dict[str, Any]] = None,
        committed_milestones: Optional[Set[str]] = None,
        running_units: Optional[Dict[str, SchedulableUnit]] = None,
    ) -> ExecutionSchedule:
        """Full planning pipeline: builds graph, prunes cycles, schedules, updates executor."""
        with tracer.start_as_current_span("scheduler.plan_cycle"):
            self.graph.build_from_manifests(manifests, units)
            schedule = self.constrained_scheduler.generate_schedule(
                graph=self.graph,
                current_tick=current_tick,
                resource_allocation_plan=resource_allocation_plan,
                circuit_breaker_states=circuit_breaker_states,
                anomaly_verdicts=anomaly_verdicts,
                committed_units=committed_milestones,
                running_units=running_units,
            )
            self.executor.set_schedule(schedule)
            return schedule

    def tick(
        self,
        current_tick: int,
        thread_pool: concurrent.futures.ThreadPoolExecutor,
        dispatch_fn: Callable[[SchedulableUnit, Dict[str, Any]], None],
        circuit_breaker_states: Dict[str, CircuitBreakerState],
        available_token_pool: Dict[str, int],
        committed_milestones: Set[str],
    ) -> List[DispatchSlot]:
        """Progresses execution on an orchestrator tick."""
        return self.executor.execute_tick(
            current_tick=current_tick,
            thread_pool=thread_pool,
            dispatch_fn=dispatch_fn,
            circuit_breaker_states=circuit_breaker_states,
            available_token_pool=available_token_pool,
            committed_milestones=committed_milestones,
            dependency_graph=self.graph,
        )


# ---------------------------------------------------------------------------
# Self-Test Verification Harness
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 70)
    print("Hermes Pipeline Execution Scheduler & Dispatch Sequencer (Self-Test)")
    print("=" * 70)

    # 1. Initialize configuration and master engine
    test_log_path = "/tmp/test_execution_schedule.jsonl"
    config = SchedulerConfig(
        max_concurrent_per_repo=2,
        global_thread_capacity=4,
        jsonl_log_path=test_log_path,
        deadline_pressure_base=2.0,
    )
    scheduler_engine = PipelineExecutionScheduler(config)

    # 2. Define test units across repositories with deadlines and shared resources
    units = [
        SchedulableUnit(
            repo_id="repo_core",
            milestone_id="m1_arch",
            estimated_cost_tokens=5000,
            estimated_duration_ticks=2,
            priority_score=2.0,
            hard_deadline_tick=10,
        ),
        SchedulableUnit(
            repo_id="repo_analytics",
            milestone_id="m1_ingest",
            estimated_cost_tokens=4000,
            estimated_duration_ticks=3,
            priority_score=1.5,
            hard_deadline_tick=12,
            shared_resource_pools=("gemini_quota_pool",),
        ),
        SchedulableUnit(
            repo_id="repo_web",
            milestone_id="m1_ui",
            estimated_cost_tokens=3000,
            estimated_duration_ticks=2,
            priority_score=1.0,
            hard_deadline_tick=5,  # Approaching deadline!
            shared_resource_pools=("gemini_quota_pool",),
        ),
        SchedulableUnit(
            repo_id="repo_core",
            milestone_id="m2_db",
            estimated_cost_tokens=7000,
            estimated_duration_ticks=2,
            priority_score=2.5,
            hard_deadline_tick=15,
        ),
    ]

    # 3. Create test manifest with an intentional cycle to verify Kahn's cycle breaking
    manifests = [
        {
            "repo_id": "repo_core",
            "dependencies": [
                {
                    "source_unit_id": "repo_core::m1_arch",
                    "target_unit_id": "repo_analytics::m1_ingest",
                    "edge_type": DependencyEdgeType.PRODUCES_ARTIFACT.value,
                    "weight": 1.0,
                },
                {
                    "source_unit_id": "repo_analytics::m1_ingest",
                    "target_unit_id": "repo_web::m1_ui",
                    "edge_type": DependencyEdgeType.SOFT_ORDERING.value,
                    "weight": 0.5,
                },
                # Intentional cycle: web::m1_ui -> core::m1_arch (SOFT_ORDERING, should be pruned)
                {
                    "source_unit_id": "repo_web::m1_ui",
                    "target_unit_id": "repo_core::m1_arch",
                    "edge_type": DependencyEdgeType.SOFT_ORDERING.value,
                    "weight": 0.2,
                },
            ],
        }
    ]

    allocation_plan = {
        "allocations": {
            "repo_core": {"token_budget": 50000, "thread_slots": 2},
            "repo_analytics": {"token_budget": 40000, "thread_slots": 2},
            "repo_web": {"token_budget": 30000, "thread_slots": 2},
        }
    }

    breakers = {
        "repo_core": CircuitBreakerState.CLOSED,
        "repo_analytics": CircuitBreakerState.CLOSED,
        "repo_web": CircuitBreakerState.CLOSED,
    }

    anomalies = {
        "repo_core": {"is_anomalous": False},
        "repo_web": {"is_anomalous": True, "severity": "CRITICAL"},
    }

    print("\n[Phase 1] Planning cycle and resolving graph dependencies...")
    current_tick = 1
    schedule = scheduler_engine.plan_cycle(
        current_tick=current_tick,
        units=units,
        manifests=manifests,
        resource_allocation_plan=allocation_plan,
        circuit_breaker_states=breakers,
        anomaly_verdicts=anomalies,
        committed_milestones=set(),
    )

    print(f"Generated Schedule ID: {schedule.schedule_id}")
    print(f"Scheduled Slots Count: {len(schedule.slots)}")
    for s in schedule.slots:
        print(
            f"  - Slot: {s.unit_id:25} | Tick: {s.assigned_tick:2} "
            f"| Slot: {s.assigned_thread_slot} | Dur: {s.schedulable_unit.estimated_duration_ticks} "
            f"| Prio: {s.composite_priority:5.2f} | Override: {s.priority_override}"
        )

    print("\n[Phase 2] Verifying API endpoints...")
    status, gantt = scheduler_engine.api.handle_request(
        "GET", "/api/pipeline/scheduler/schedule"
    )
    print(f"GET /schedule -> Status {status}, Tasks count: {len(gantt['tasks'])}")

    status, deps = scheduler_engine.api.handle_request(
        "GET", "/api/pipeline/scheduler/dependencies"
    )
    print(
        f"GET /dependencies -> Status {status}, Nodes: {deps['node_count']}, Edges: {deps['edge_count']}"
    )

    # 4. Dispatch Simulation using ScheduleExecutor and ThreadPool
    print("\n[Phase 3] Simulating tick-by-tick dispatch execution...")
    dispatched_records = []

    def dummy_worker(unit: SchedulableUnit, ctx: Dict[str, Any]) -> None:
        dispatched_records.append((unit.unit_id, ctx["scheduled_tick"]))

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        committed: Set[str] = set()
        token_pool = {
            "repo_core": 100000,
            "repo_analytics": 100000,
            "repo_web": 100000,
        }

        # Simulate ticks 1 through 5
        for sim_tick in range(1, 6):
            slots = scheduler_engine.tick(
                current_tick=sim_tick,
                thread_pool=pool,
                dispatch_fn=dummy_worker,
                circuit_breaker_states=breakers,
                available_token_pool=token_pool,
                committed_milestones=committed,
            )
            for s in slots:
                # Mark as committed when simulated duration ticks expire
                committed.add(s.unit_id)

    print(f"Total dispatched tasks in simulation: {len(dispatched_records)}")

    # 5. Adherence and Health Contribution Check
    status, adherence = scheduler_engine.api.handle_request(
        "GET", "/api/pipeline/scheduler/adherence"
    )
    print(
        f"GET /adherence -> Score: {adherence['score']}%, "
        f"Adherence Ratio: {adherence['adherence_ratio']}"
    )

    # 6. Priority Override Test
    status, override_resp = scheduler_engine.api.handle_request(
        "POST",
        "/api/pipeline/scheduler/priority-override",
        {
            "repo_id": "repo_web",
            "milestone_id": "m1_ui",
            "boost_score": 10.0,
            "reason": "CEO Demo Milestone",
        },
    )
    print(
        f"POST /priority-override -> Status {status}, "
        f"Reschedule Triggered: {override_resp.get('reschedule_triggered')}"
    )
    print(f"Is reschedule needed: {scheduler_engine.executor.is_reschedule_needed()}")

    # Clean up test artifact
    if os.path.exists(test_log_path):
        os.remove(test_log_path)

    print("\n" + "=" * 70)
    print("Self-Test PASSED: PipelineExecutionScheduler is fully operational.")
    print("=" * 70)