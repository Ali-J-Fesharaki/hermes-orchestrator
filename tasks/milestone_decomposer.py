"""tasks/milestone_decomposer.py
Autonomous Milestone Decomposition & Hierarchical Re-Planning Engine.

This module provides autonomous milestone decomposition when the Pre-Dispatch
Feasibility Gate signals DECOMPOSE or the Runtime Execution Governor preempts
a milestone mid-flight. It partitions monolithic specifications into atomic,
topologically sorted SubMilestones with intra-milestone dependency graphs,
historical-weighted cost estimation, and transactional replanning coordination.
"""

from __future__ import annotations

import copy
import json
import logging
import re
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# OpenTelemetry Tracing with In-Memory / No-Op Fallback
# ---------------------------------------------------------------------------
try:
    from opentelemetry import trace

    tracer = trace.get_tracer("hermes.decomposer")
except ImportError:

    class _NoOpSpan:
        """No-op OpenTelemetry span context manager."""

        def __enter__(self) -> _NoOpSpan:
            return self

        def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            pass

        def set_attribute(self, key: str, value: Any) -> None:
            pass

        def set_status(self, *args: Any, **kwargs: Any) -> None:
            pass

        def record_exception(self, exc: BaseException) -> None:
            pass

    class _NoOpTracer:
        """No-op OpenTelemetry tracer."""

        def start_as_current_span(self, name: str, *args: Any, **kwargs: Any) -> _NoOpSpan:
            return _NoOpSpan()

    class _TraceModuleFallback:
        @staticmethod
        def get_tracer(name: str, *args: Any, **kwargs: Any) -> _NoOpTracer:
            return _NoOpTracer()

    trace = _TraceModuleFallback()
    tracer = trace.get_tracer("hermes.decomposer")

# ---------------------------------------------------------------------------
# Sibling Tasks Imports & Resilient Standalone Fallbacks
# ---------------------------------------------------------------------------
try:
    from tasks.milestone_cost_estimator import (
        CostEstimate,
        CostEstimator,
        FeasibilityGate,
        GateDecision,
        GateResult,
    )
except ImportError:
    try:
        from milestone_cost_estimator import (
            CostEstimate,
            CostEstimator,
            FeasibilityGate,
            GateDecision,
            GateResult,
        )
    except ImportError:

        class GateDecision(str, Enum):
            PASS = "PASS"
            DEFER = "DEFER"
            DECOMPOSE = "DECOMPOSE"
            REJECT = "REJECT"

        @dataclass
        class CostEstimate:
            predicted_tokens: int = 1500
            estimated_cost_usd: float = 0.015
            confidence_score: float = 0.85
            reasoning: str = "Standalone fallback cost estimate"

            def to_dict(self) -> Dict[str, Any]:
                return asdict(self)

        @dataclass
        class GateResult:
            decision: GateDecision = GateDecision.PASS
            reason: str = "Fallback gate evaluation passed"
            confidence: float = 0.9

        class CostEstimator:
            def estimate(self, milestone: Dict[str, Any]) -> CostEstimate:
                tokens = milestone.get("predicted_tokens", 1500)
                return CostEstimate(
                    predicted_tokens=tokens,
                    estimated_cost_usd=round(tokens * 0.000003, 6),
                    confidence_score=0.9,
                    reasoning="Estimated via heuristic fallback token model",
                )

        class FeasibilityGate:
            def evaluate(
                self,
                milestone: Dict[str, Any],
                cost_estimate: Optional[CostEstimate] = None,
            ) -> GateResult:
                tokens = (
                    cost_estimate.predicted_tokens
                    if cost_estimate
                    else milestone.get("predicted_tokens", 1500)
                )
                if tokens > 15000:
                    return GateResult(
                        decision=GateDecision.DECOMPOSE,
                        reason=f"Predicted tokens ({tokens}) exceeds execution limit (15000).",
                    )
                return GateResult(decision=GateDecision.PASS, reason="Passed feasibility gate.")

try:
    from tasks.runtime_execution_governor import GovernorCheckpoint
except ImportError:
    try:
        from runtime_execution_governor import GovernorCheckpoint
    except ImportError:

        @dataclass
        class GovernorCheckpoint:
            last_completed_phase: int = 0
            checkpoint_id: str = "chk_default"
            timestamp: float = 0.0
            completed_units: List[str] = field(default_factory=list)

try:
    from tasks.steering_memory_engine import SteeringMemoryEngine
except ImportError:
    try:
        from steering_memory_engine import SteeringMemoryEngine
    except ImportError:

        class SteeringMemoryEngine:
            def __init__(self) -> None:
                self.records: List[Dict[str, Any]] = []

            def get_capability_profile(self, repo_name: str) -> Dict[str, Any]:
                return {
                    "repo_name": repo_name,
                    "repo_historical_LOC_per_spec_char": 0.08,
                    "avg_completion_ratio": 0.94,
                }

            def record_outcome(self, outcome: Dict[str, Any]) -> None:
                self.records.append(outcome)

            def record_decomposition_event(self, event: Dict[str, Any]) -> None:
                self.records.append(event)

try:
    from tasks.dependency_graph_scheduler import TopologicalScheduler
except ImportError:
    try:
        from dependency_graph_scheduler import TopologicalScheduler
    except ImportError:

        class TopologicalScheduler:
            """In-memory DAG scheduler fallback."""

            def __init__(self) -> None:
                self.nodes: Dict[str, Dict[str, Any]] = {}
                self.edges: Dict[str, Set[str]] = {}

            def add_node(self, node_id: str, data: Optional[Dict[str, Any]] = None) -> None:
                self.nodes[node_id] = data or {}
                if node_id not in self.edges:
                    self.edges[node_id] = set()

            def remove_node(self, node_id: str) -> None:
                if node_id in self.nodes:
                    del self.nodes[node_id]
                if node_id in self.edges:
                    del self.edges[node_id]
                for src in self.edges:
                    self.edges[src].discard(node_id)

            def add_edge(self, from_node: str, to_node: str) -> None:
                if from_node not in self.nodes:
                    self.add_node(from_node)
                if to_node not in self.nodes:
                    self.add_node(to_node)
                self.edges[from_node].add(to_node)

            def replace_node_with_subgraph(
                self, parent_node_id: str, sub_nodes: List[Any]
            ) -> None:
                predecessors = [
                    src for src, dsts in self.edges.items() if parent_node_id in dsts
                ]
                successors = list(self.edges.get(parent_node_id, set()))

                self.remove_node(parent_node_id)

                sub_names = [getattr(s, "name", str(s)) for s in sub_nodes]
                sub_dict = {getattr(s, "name", str(s)): s for s in sub_nodes}

                for s in sub_nodes:
                    name = getattr(s, "name", str(s))
                    data = s.to_dict() if hasattr(s, "to_dict") else {"name": name}
                    self.add_node(name, data)

                root_sub_nodes = set(sub_names)
                leaf_sub_nodes = set(sub_names)

                for s in sub_nodes:
                    name = getattr(s, "name", str(s))
                    deps = getattr(s, "dependency_edges", [])
                    for dep in deps:
                        if dep in sub_dict:
                            self.add_edge(dep, name)
                            root_sub_nodes.discard(name)
                            leaf_sub_nodes.discard(dep)

                for pred in predecessors:
                    for root in root_sub_nodes:
                        self.add_edge(pred, root)

                for leaf in leaf_sub_nodes:
                    for succ in successors:
                        self.add_edge(leaf, succ)

logger = logging.getLogger("hermes.decomposer")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter("[%(asctime)s] [%(levelname)s] [hermes.decomposer] %(message)s")
    )
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# Core Enums and Dataclasses
# ---------------------------------------------------------------------------
class DecomposeTrigger(str, Enum):
    """Triggers that mandate autonomous milestone decomposition."""

    FEASIBILITY_GATE = "FEASIBILITY_GATE"
    GOVERNOR_PREEMPT = "GOVERNOR_PREEMPT"


@dataclass
class SubMilestone:
    """Atomic functional sub-unit derived from milestone decomposition."""

    name: str
    details: str
    relative_filepath: str
    technical_specification: str
    executor_prompt: str
    parent_milestone_id: str
    status: str = "pending"
    dependency_edges: List[str] = field(default_factory=list)
    estimated_cost: Optional[CostEstimate] = None
    checkpoint_satisfied: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Serializes the sub-milestone to a dictionary."""
        return {
            "name": self.name,
            "status": self.status,
            "details": self.details,
            "relative_filepath": self.relative_filepath,
            "technical_specification": self.technical_specification,
            "executor_prompt": self.executor_prompt,
            "parent_milestone_id": self.parent_milestone_id,
            "dependency_edges": list(self.dependency_edges),
            "estimated_cost": (
                self.estimated_cost.to_dict()
                if hasattr(self.estimated_cost, "to_dict")
                else self.estimated_cost
            ),
            "checkpoint_satisfied": self.checkpoint_satisfied,
        }


@dataclass
class DecompositionResult:
    """Outcome payload generated by the ReplanningCoordinator."""

    parent_milestone_id: str
    sub_milestones: List[SubMilestone]
    trigger: DecomposeTrigger
    recursive_depth: int
    escalate_to_planner: bool
    details: str = ""
    failure_reason: Optional[str] = None
    checkpoint_phases_skipped: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Serializes the decomposition result to a dictionary."""
        return {
            "parent_milestone_id": self.parent_milestone_id,
            "sub_milestones": [m.to_dict() for m in self.sub_milestones],
            "trigger": (
                self.trigger.value
                if isinstance(self.trigger, DecomposeTrigger)
                else str(self.trigger)
            ),
            "recursive_depth": self.recursive_depth,
            "escalate_to_planner": self.escalate_to_planner,
            "details": self.details,
            "failure_reason": self.failure_reason,
            "checkpoint_phases_skipped": self.checkpoint_phases_skipped,
        }


# ---------------------------------------------------------------------------
# MilestoneDecomposer Engine
# ---------------------------------------------------------------------------
class MilestoneDecomposer:
    """Decomposes milestones into atomic SubMilestones with intra-DAG dependencies."""

    def __init__(
        self,
        steering_memory: Optional[SteeringMemoryEngine] = None,
        cost_estimator: Optional[CostEstimator] = None,
    ) -> None:
        self.steering_memory = steering_memory
        self.cost_estimator = cost_estimator or CostEstimator()

    def decompose(
        self,
        milestone_metadata: Dict[str, Any],
        trigger: DecomposeTrigger,
        checkpoint: Optional[GovernorCheckpoint] = None,
    ) -> List[SubMilestone]:
        """Decomposes a milestone into a topologically ordered list of SubMilestones."""
        parent_name = milestone_metadata.get("name", "unnamed_milestone")
        parent_id = str(milestone_metadata.get("id", parent_name))
        relative_filepath = milestone_metadata.get("relative_filepath", "tasks/unspecified.py")
        technical_specification = milestone_metadata.get("technical_specification", "").strip()
        details = milestone_metadata.get("details", "")

        trigger_val = trigger.value if hasattr(trigger, "value") else str(trigger)

        with tracer.start_as_current_span("milestone_decomposer.decompose") as span:
            span.set_attribute("trigger_type", trigger_val)
            span.set_attribute("parent_milestone_name", parent_name)

            logger.info(
                "Initiating decomposition for '%s' triggered by %s", parent_name, trigger_val
            )

            # (a) Extract functional units via regex-based clause extraction
            raw_units = self._extract_functional_units(technical_specification)
            if not raw_units:
                # Fallback: create two default sub-units if specification could not be parsed
                raw_units = [
                    {
                        "title": "Core Foundations & Interface Implementation",
                        "spec": technical_specification[: len(technical_specification) // 2],
                        "entities": self._extract_named_entities(
                            technical_specification[: len(technical_specification) // 2]
                        ),
                    },
                    {
                        "title": "Business Logic & Integration Coordination",
                        "spec": technical_specification[len(technical_specification) // 2 :],
                        "entities": self._extract_named_entities(
                            technical_specification[len(technical_specification) // 2 :]
                        ),
                    },
                ]

            # (b) Build intra-milestone dependency graph
            ordered_units = self._build_intra_milestone_dag(raw_units, parent_name)

            # (c) Cost allocation based on complexity weight and historical LOC/spec char
            total_parent_tokens = self._extract_parent_predicted_tokens(milestone_metadata)
            historical_loc_factor = self._get_repo_loc_factor(milestone_metadata)

            sub_milestones: List[SubMilestone] = []
            total_complexity_weight = sum(
                max(1.0, len(u["spec"]) * historical_loc_factor) for u in ordered_units
            )

            for idx, unit in enumerate(ordered_units):
                unit_spec = unit["spec"]
                unit_weight = max(1.0, len(unit_spec) * historical_loc_factor)
                allocated_tokens = max(
                    200, int(total_parent_tokens * (unit_weight / total_complexity_weight))
                )

                sub_name = f"{parent_name} [Sub-Task {idx + 1}: {unit['title']}]"
                sub_prompt = self._synthesize_executor_prompt(
                    sub_name=sub_name,
                    parent_name=parent_name,
                    parent_details=details,
                    relative_filepath=relative_filepath,
                    unit_spec=unit_spec,
                    dependency_edges=unit["dependency_edges"],
                )

                sub_m = SubMilestone(
                    name=sub_name,
                    details=f"Sub-task {idx + 1} of {parent_name}: {unit['title']}",
                    relative_filepath=relative_filepath,
                    technical_specification=unit_spec,
                    executor_prompt=sub_prompt,
                    parent_milestone_id=parent_id,
                    status="pending",
                    dependency_edges=list(unit["dependency_edges"]),
                    checkpoint_satisfied=False,
                )

                # Assign estimated cost via CostEstimator or token allocation
                sub_meta_dict = sub_m.to_dict()
                sub_meta_dict["predicted_tokens"] = allocated_tokens
                sub_m.estimated_cost = self.cost_estimator.estimate(sub_meta_dict)

                sub_milestones.append(sub_m)

            # (d) Align checkpoint for GOVERNOR_PREEMPT triggers
            checkpoint_phases_skipped = 0
            if trigger == DecomposeTrigger.GOVERNOR_PREEMPT and checkpoint is not None:
                checkpoint_phases_skipped = self._apply_checkpoint_satisfaction(
                    sub_milestones, checkpoint
                )

            span.set_attribute("sub_milestone_count", len(sub_milestones))
            span.set_attribute("checkpoint_phases_skipped", checkpoint_phases_skipped)

            logger.info(
                "Decomposed '%s' into %d sub-milestones (%d satisfied by checkpoint)",
                parent_name,
                len(sub_milestones),
                checkpoint_phases_skipped,
            )

            return sub_milestones

    def _extract_functional_units(self, spec_text: str) -> List[Dict[str, Any]]:
        """Extracts functional units from the technical specification using regex patterns."""
        if not spec_text:
            return []

        # Strategy 1: Split on explicit numbered list markers e.g. "(1)", "(2)" or "1.", "2."
        numbered_marker_re = re.compile(
            r"(?:\A|\n|\s+)(?:\((?P<num>\d+)\)|(?P<num_dot>\b\d+)[\.\)])\s+"
        )
        matches = list(numbered_marker_re.finditer(spec_text))

        if len(matches) >= 2:
            units: List[Dict[str, Any]] = []
            for i in range(len(matches)):
                start_idx = matches[i].end()
                end_idx = matches[i + 1].start() if i + 1 < len(matches) else len(spec_text)
                clause_text = spec_text[start_idx:end_idx].strip()
                if clause_text:
                    title, clean_spec = self._extract_clause_title_and_spec(clause_text)
                    units.append(
                        {
                            "title": title,
                            "spec": clean_spec,
                            "entities": self._extract_named_entities(clean_spec),
                        }
                    )
            if len(units) >= 2:
                return units

        # Strategy 2: Split on Class / Function / Pipeline Phase markers
        boundary_re = re.compile(
            r"(?:\A|\n|\s+)(?:Phase\s+\d+|Stage\s+\d+|Step\s+\d+|class\s+[A-Za-z0-9_]+|def\s+[A-Za-z0-9_]+)[\:\s\-]",
            re.IGNORECASE,
        )
        b_matches = list(boundary_re.finditer(spec_text))
        if len(b_matches) >= 2:
            units = []
            for i in range(len(b_matches)):
                start_idx = b_matches[i].start()
                end_idx = b_matches[i + 1].start() if i + 1 < len(b_matches) else len(spec_text)
                clause_text = spec_text[start_idx:end_idx].strip()
                if clause_text:
                    title, clean_spec = self._extract_clause_title_and_spec(clause_text)
                    units.append(
                        {
                            "title": title,
                            "spec": clean_spec,
                            "entities": self._extract_named_entities(clean_spec),
                        }
                    )
            if len(units) >= 2:
                return units

        # Strategy 3: Split on transitional conjunction markers (Then, Next, Finally, Additionally)
        transition_re = re.compile(
            r"(?<=[.!?\n])\s*(?:Then|Next|Finally|Additionally|Subsequently)[,\s]+",
            re.IGNORECASE,
        )
        parts = [p.strip() for p in transition_re.split(spec_text) if p.strip()]
        if len(parts) >= 2:
            units = []
            for idx, part in enumerate(parts):
                title, clean_spec = self._extract_clause_title_and_spec(part)
                units.append(
                    {
                        "title": title or f"Phase {idx + 1}",
                        "spec": clean_spec,
                        "entities": self._extract_named_entities(clean_spec),
                    }
                )
            return units

        # Default fallback: split on double newlines if present
        paragraphs = [p.strip() for p in spec_text.split("\n\n") if len(p.strip()) > 30]
        if len(paragraphs) >= 2:
            units = []
            for idx, p in enumerate(paragraphs):
                title, clean_spec = self._extract_clause_title_and_spec(p)
                units.append(
                    {
                        "title": title or f"Component {idx + 1}",
                        "spec": clean_spec,
                        "entities": self._extract_named_entities(clean_spec),
                    }
                )
            return units

        return []

    def _extract_clause_title_and_spec(self, clause_text: str) -> Tuple[str, str]:
        """Separates an initial title / entity identifier from the clause body."""
        # Check for colon separator e.g. "DecomposeTrigger(Enum): FEASIBILITY_GATE..."
        match_colon = re.match(r"\A(?P<title>[A-Za-z0-9_\s\(\)\,\-\.\/]+?)\:\s*(?P<body>.+)\Z", clause_text, re.DOTALL)
        if match_colon:
            title = match_colon.group("title").strip()
            body = match_colon.group("body").strip()
            # Clean title
            title = re.sub(r"[\(\)\:\.\,]", "", title).strip()
            if len(title) > 60:
                title = title[:57] + "..."
            return title, clause_text

        # Otherwise take first 5 words as the title
        words = clause_text.split()
        if words:
            candidate_title = " ".join(words[:min(5, len(words))])
            candidate_title = re.sub(r"[\(\)\:\.\,]", "", candidate_title).strip()
            return candidate_title, clause_text
        return "Functional Unit", clause_text

    def _extract_named_entities(self, text: str) -> Set[str]:
        """Extracts class names, types, and capitalized technical terms from a clause."""
        entities = set()
        # Find explicit class / def declarations
        for m in re.finditer(r"\b(?:class|def|enum|struct|dataclass)\s+([A-Za-z0-9_]+)", text):
            entities.add(m.group(1))

        # Find CamelCase / PascalCase identifiers
        for m in re.finditer(r"\b[A-Z][a-zA-Z0-9_]{2,}\b", text):
            ent = m.group(0)
            if ent not in {"List", "Dict", "Optional", "Tuple", "Set", "Any", "Union", "True", "False", "None"}:
                entities.add(ent)
        return entities

    def _build_intra_milestone_dag(
        self, raw_units: List[Dict[str, Any]], parent_name: str
    ) -> List[Dict[str, Any]]:
        """Constructs intra-milestone dependency edges and orders units topologically."""
        n = len(raw_units)
        # Assign unique names to raw units for referencing
        for idx, u in enumerate(raw_units):
            u["sub_name"] = f"{parent_name} [Sub-Task {idx + 1}: {u['title']}]"
            u["dependency_edges"] = set()

        # Step 1: Detect Output-of-A-is-Input-to-B via named entity overlap
        for i in range(n):
            defined_entities = raw_units[i]["entities"]
            for j in range(n):
                if i == j:
                    continue
                # If unit j references an entity defined in unit i
                spec_j = raw_units[j]["spec"]
                for entity in defined_entities:
                    if re.search(r"\b" + re.escape(entity) + r"\b", spec_j):
                        # Ensure we do not create circular dependencies; respect natural progression
                        if i < j:
                            raw_units[j]["dependency_edges"].add(raw_units[i]["sub_name"])
                        elif i > j and not raw_units[i]["dependency_edges"]:
                            raw_units[i]["dependency_edges"].add(raw_units[j]["sub_name"])

        # Step 2: Ensure pipeline continuity - if a unit has no dependencies and is not the first,
        # connect it to its immediate predecessor to maintain stable execution sequence
        for i in range(1, n):
            if not raw_units[i]["dependency_edges"]:
                raw_units[i]["dependency_edges"].add(raw_units[i - 1]["sub_name"])

        # Step 3: Topological sorting of units
        return self._topological_sort_units(raw_units)

    def _topological_sort_units(self, units: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Sorts units topologically using Kahn's algorithm; falls back to index order on cycles."""
        name_to_unit = {u["sub_name"]: u for u in units}
        in_degree: Dict[str, int] = {u["sub_name"]: 0 for u in units}
        adj_list: Dict[str, List[str]] = {u["sub_name"]: [] for u in units}

        for u in units:
            for dep in u["dependency_edges"]:
                if dep in adj_list:
                    adj_list[dep].append(u["sub_name"])
                    in_degree[u["sub_name"]] += 1

        queue = [u["sub_name"] for u in units if in_degree[u["sub_name"]] == 0]
        sorted_names: List[str] = []

        while queue:
            curr = queue.pop(0)
            sorted_names.append(curr)
            for neighbor in adj_list.get(curr, []):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        # If a cycle was present, fallback to original order while pruning back-edges
        if len(sorted_names) < len(units):
            logger.warning("Cycle detected in intra-milestone dependencies. Falling back to sequential order.")
            seen_names = set()
            for u in units:
                valid_edges = {edge for edge in u["dependency_edges"] if edge in seen_names}
                u["dependency_edges"] = list(valid_edges)
                seen_names.add(u["sub_name"])
            return units

        sorted_units = [name_to_unit[name] for name in sorted_names]
        # Normalize dependency edges to lists
        for u in sorted_units:
            u["dependency_edges"] = sorted(list(u["dependency_edges"]))
        return sorted_units

    def _apply_checkpoint_satisfaction(
        self, sub_milestones: List[SubMilestone], checkpoint: GovernorCheckpoint
    ) -> int:
        """Marks completed phases as satisfied based on GovernorCheckpoint state."""
        last_phase = getattr(checkpoint, "last_completed_phase", -1)
        completed_units = getattr(checkpoint, "completed_units", [])
        satisfied_count = 0

        for idx, sub_m in enumerate(sub_milestones):
            is_satisfied = False
            # Check by index if last_completed_phase is an integer
            if isinstance(last_phase, int) and last_phase >= 0:
                if idx <= last_phase:
                    is_satisfied = True

            # Check by completed_units collection if available
            if completed_units and any(
                unit_name in sub_m.name for unit_name in completed_units
            ):
                is_satisfied = True

            if is_satisfied:
                sub_m.checkpoint_satisfied = True
                sub_m.status = "completed"
                satisfied_count += 1

        return satisfied_count

    def _extract_parent_predicted_tokens(self, milestone: Dict[str, Any]) -> int:
        """Extracts predicted token cost from parent milestone metadata."""
        if "predicted_tokens" in milestone:
            try:
                return int(milestone["predicted_tokens"])
            except (ValueError, TypeError):
                pass
        cost_obj = milestone.get("estimated_cost")
        if cost_obj:
            if hasattr(cost_obj, "predicted_tokens"):
                return getattr(cost_obj, "predicted_tokens")
            if isinstance(cost_obj, dict) and "predicted_tokens" in cost_obj:
                return int(cost_obj["predicted_tokens"])
        return 4000

    def _get_repo_loc_factor(self, milestone: Dict[str, Any]) -> float:
        """Queries SteeringMemoryEngine for repo historical LOC per spec char."""
        if self.steering_memory:
            repo_name = milestone.get("repo_name", "hermes_orchestrator")
            try:
                profile = self.steering_memory.get_capability_profile(repo_name)
                if profile and isinstance(profile, dict):
                    return float(profile.get("repo_historical_LOC_per_spec_char", 0.08))
                if hasattr(profile, "repo_historical_LOC_per_spec_char"):
                    return float(getattr(profile, "repo_historical_LOC_per_spec_char", 0.08))
            except Exception as exc:
                logger.debug("Failed querying SteeringMemory capability profile: %s", exc)
        return 0.08

    def _synthesize_executor_prompt(
        self,
        sub_name: str,
        parent_name: str,
        parent_details: str,
        relative_filepath: str,
        unit_spec: str,
        dependency_edges: Any,
    ) -> str:
        """Constructs an atomic, unambiguous prompt for the sub-milestone execution dispatch."""
        deps_str = (
            ", ".join(dependency_edges)
            if dependency_edges
            else "None (Initial Foundation Task)"
        )
        return (
            f"AUTONOMOUS SUB-TASK MISSION DIRECTIVE:\n"
            f"Target Sub-Milestone: {sub_name}\n"
            f"Parent Milestone: {parent_name}\n"
            f"Context: {parent_details}\n"
            f"Target File: {relative_filepath}\n"
            f"Prerequisite Dependencies: {deps_str}\n\n"
            f"SPECIFICATION FOR THIS COMPONENT:\n"
            f"{unit_spec}\n\n"
            f"EXECUTION GUIDELINES:\n"
            f"1. Implement publication-grade, self-contained Python code strictly satisfying the component specification.\n"
            f"2. Integrate cleanly with existing interfaces and dependencies in {relative_filepath}.\n"
            f"3. Ensure all types, docstrings, and error handling meet production standards."
        )


# ---------------------------------------------------------------------------
# ReplanningCoordinator Class
# ---------------------------------------------------------------------------
class ReplanningCoordinator:
    """Coordinates atomic decomposition, feasibility gating, recursive decomposition,
    DAG replacement, and feedback to the Steering Memory Engine."""

    def __init__(
        self,
        decomposer: MilestoneDecomposer,
        feasibility_gate: Optional[FeasibilityGate] = None,
        scheduler: Optional[TopologicalScheduler] = None,
        steering_memory: Optional[SteeringMemoryEngine] = None,
        cost_estimator: Optional[CostEstimator] = None,
    ) -> None:
        self.decomposer = decomposer
        self.feasibility_gate = feasibility_gate or FeasibilityGate()
        self.scheduler = scheduler
        self.steering_memory = steering_memory
        self.cost_estimator = cost_estimator or CostEstimator()

    def coordinate_decomposition(
        self,
        milestone: Dict[str, Any],
        trigger: DecomposeTrigger,
        checkpoint: Optional[GovernorCheckpoint] = None,
        recursive_depth: int = 0,
    ) -> DecompositionResult:
        """Executes transactional milestone decomposition with recursive depth capping."""
        parent_name = milestone.get("name", "unnamed_milestone")
        parent_id = str(milestone.get("id", parent_name))
        trigger_val = trigger.value if hasattr(trigger, "value") else str(trigger)

        with tracer.start_as_current_span("replanning_coordinator.coordinate_decomposition") as span:
            span.set_attribute("trigger_type", trigger_val)
            span.set_attribute("parent_milestone_name", parent_name)
            span.set_attribute("recursive_depth", recursive_depth)

            logger.info(
                "Starting decomposition coordination for '%s' (depth=%d, trigger=%s)",
                parent_name,
                recursive_depth,
                trigger_val,
            )

            # Step 1: Decompose milestone into initial candidate sub-milestones
            sub_milestones = self.decomposer.decompose(milestone, trigger, checkpoint)
            span.set_attribute("sub_milestone_count", len(sub_milestones))

            skipped_count = sum(1 for m in sub_milestones if m.checkpoint_satisfied)
            span.set_attribute("checkpoint_phases_skipped", skipped_count)

            # Step 2: Estimate & Gate each sub-milestone
            gated_decisions: List[Tuple[SubMilestone, GateResult]] = []
            for sub_m in sub_milestones:
                # If sub-milestone is already satisfied via checkpoint, automatically pass it
                if sub_m.checkpoint_satisfied:
                    gated_decisions.append(
                        (
                            sub_m,
                            GateResult(
                                decision=GateDecision.PASS,
                                reason="Sub-milestone satisfied by governor checkpoint",
                            ),
                        )
                    )
                    continue

                # Ensure estimated cost is present
                if not sub_m.estimated_cost:
                    sub_m.estimated_cost = self.cost_estimator.estimate(sub_m.to_dict())

                gate_res = self.feasibility_gate.evaluate(sub_m.to_dict(), sub_m.estimated_cost)
                gated_decisions.append((sub_m, gate_res))

            # Step 3: Handle recursive decomposition for sub-milestones gating as DECOMPOSE
            final_sub_milestones: List[SubMilestone] = []
            depth_exceeded = False
            failed_gate_count = 0

            for sub_m, gate_res in gated_decisions:
                decision = gate_res.decision
                if decision == GateDecision.DECOMPOSE:
                    if recursive_depth < 2:
                        logger.info(
                            "Sub-milestone '%s' flagged for recursive decomposition (current_depth=%d)",
                            sub_m.name,
                            recursive_depth,
                        )
                        recursive_result = self.coordinate_decomposition(
                            milestone=sub_m.to_dict(),
                            trigger=trigger,
                            checkpoint=None,
                            recursive_depth=recursive_depth + 1,
                        )

                        if recursive_result.escalate_to_planner:
                            logger.warning(
                                "Recursive decomposition of '%s' failed. Aborting decomposition branch.",
                                sub_m.name,
                            )
                            depth_exceeded = True
                            failed_gate_count += 1
                        else:
                            # Splice the recursively generated sub-milestones into DAG
                            self._splice_recursive_sub_milestones(
                                sub_m, recursive_result.sub_milestones
                            )
                            final_sub_milestones.extend(recursive_result.sub_milestones)
                    else:
                        logger.warning(
                            "Recursive decomposition depth limit (2) exceeded for sub-milestone '%s'",
                            sub_m.name,
                        )
                        depth_exceeded = True
                        failed_gate_count += 1
                        final_sub_milestones.append(sub_m)
                elif decision in {GateDecision.PASS, GateDecision.DEFER}:
                    final_sub_milestones.append(sub_m)
                else:
                    # REJECT or FAIL gating decision
                    failed_gate_count += 1
                    final_sub_milestones.append(sub_m)

            # Step 4: Transaction Rollback Evaluation
            total_sub_count = max(1, len(final_sub_milestones))
            failure_ratio = failed_gate_count / total_sub_count

            if depth_exceeded or failure_ratio > 0.5:
                failure_reason = (
                    f"Recursive depth exceeded limit (2)"
                    if depth_exceeded
                    else f">50% of sub-milestones failed feasibility gating ({failed_gate_count}/{total_sub_count})"
                )
                logger.error(
                    "Decomposition aborted and escalated to planner: %s", failure_reason
                )

                # Emit enriched failed decomposition analysis to Steering Memory
                self._record_steering_event(
                    event_type="milestone_decomposition_escalated",
                    milestone_id=parent_id,
                    success=False,
                    details=failure_reason,
                    sub_milestone_count=len(final_sub_milestones),
                    trigger=trigger_val,
                )

                return DecompositionResult(
                    parent_milestone_id=parent_id,
                    sub_milestones=final_sub_milestones,
                    trigger=trigger,
                    recursive_depth=recursive_depth,
                    escalate_to_planner=True,
                    details=f"Decomposition aborted: {failure_reason}",
                    failure_reason=failure_reason,
                    checkpoint_phases_skipped=skipped_count,
                )

            # Step 5: Replace Parent Node in DependencyGraphScheduler DAG
            if self.scheduler:
                try:
                    self.scheduler.replace_node_with_subgraph(parent_id, final_sub_milestones)
                    logger.info(
                        "Replaced node '%s' in DependencyGraphScheduler with %d sub-nodes",
                        parent_id,
                        len(final_sub_milestones),
                    )
                except Exception as exc:
                    logger.exception("Failed to insert subgraph into scheduler: %s", exc)

            # Step 6: Emit Steering Memory decomposition event
            self._record_steering_event(
                event_type="milestone_decomposition_succeeded",
                milestone_id=parent_id,
                success=True,
                details=f"Decomposed into {len(final_sub_milestones)} atomic sub-milestones",
                sub_milestone_count=len(final_sub_milestones),
                trigger=trigger_val,
            )

            return DecompositionResult(
                parent_milestone_id=parent_id,
                sub_milestones=final_sub_milestones,
                trigger=trigger,
                recursive_depth=recursive_depth,
                escalate_to_planner=False,
                details=f"Successfully decomposed into {len(final_sub_milestones)} sub-milestones",
                checkpoint_phases_skipped=skipped_count,
            )

    def _splice_recursive_sub_milestones(
        self, original_sub_m: SubMilestone, children: List[SubMilestone]
    ) -> None:
        """Rewires incoming and internal dependencies when splicing recursively decomposed units."""
        if not children:
            return

        # The root children inherit the parent sub-milestone's inbound dependencies
        root_children = [c for c in children if not c.dependency_edges]
        if not root_children:
            root_children = [children[0]]

        for root in root_children:
            root.dependency_edges = list(
                set(root.dependency_edges).union(original_sub_m.dependency_edges)
            )

    def _record_steering_event(
        self,
        event_type: str,
        milestone_id: str,
        success: bool,
        details: str,
        sub_milestone_count: int,
        trigger: str,
    ) -> None:
        """Emits structured decomposition outcome events to the SteeringMemoryEngine."""
        if not self.steering_memory:
            return

        payload = {
            "event_type": event_type,
            "milestone_id": milestone_id,
            "success": success,
            "details": details,
            "sub_milestone_count": sub_milestone_count,
            "trigger": trigger,
        }

        try:
            if hasattr(self.steering_memory, "record_decomposition_event"):
                self.steering_memory.record_decomposition_event(payload)
            elif hasattr(self.steering_memory, "record_outcome"):
                self.steering_memory.record_outcome(payload)
        except Exception as exc:
            logger.debug("Failed recording outcome to SteeringMemory: %s", exc)


# ---------------------------------------------------------------------------
# Self-Test and Standalone Execution
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 80)
    print("HERMES ORCHESTRATOR - MILESTONE DECOMPOSER STANDALONE VERIFICATION")
    print("=" * 80)

    # Sample milestone metadata matching the current module architecture
    sample_milestone = {
        "id": "milestone_decomposer_engine",
        "name": "Autonomous Milestone Decomposition & Hierarchical Re-Planning Engine",
        "relative_filepath": "tasks/milestone_decomposer.py",
        "details": "Provides autonomous milestone decomposition upon Feasibility Gate or Governor preempt.",
        "predicted_tokens": 6000,
        "technical_specification": (
            "Core Classes: "
            "(1) DecomposeTrigger(Enum): FEASIBILITY_GATE, GOVERNOR_PREEMPT. "
            "(2) SubMilestone(dataclass): name, status='pending', details, relative_filepath, "
            "technical_specification, executor_prompt, parent_milestone_id, dependency_edges: List[str], "
            "estimated_cost: CostEstimate, checkpoint_satisfied: bool. "
            "(3) MilestoneDecomposer: decompose(milestone_metadata: dict, trigger: DecomposeTrigger, "
            "checkpoint: Optional[GovernorCheckpoint]=None) -> List[SubMilestone]. Decomposition algorithm: "
            "(a) Parse technical_specification into functional units via regex-based clause extraction, "
            "(b) Build intra-milestone dependency graph using topological ordering of extracted units, "
            "(c) For GOVERNOR_PREEMPT triggers, align checkpoint.last_completed_phase against extracted units, "
            "(d) Estimate per-sub-milestone cost as parent.predicted_tokens * (unit_complexity_weight / total_complexity_weight). "
            "(4) ReplanningCoordinator: coordinate_decomposition(milestone, trigger, checkpoint?) -> DecompositionResult. "
            "Transaction semantics: decompose -> for each sub-milestone call CostEstimator.estimate() -> "
            "call FeasibilityGate.evaluate() -> if all PASS or DEFER, insert into DependencyGraphScheduler DAG. "
            "(5) Integration points: imports from milestone_cost_estimator, runtime_execution_governor, "
            "steering_memory_engine, dependency_graph_scheduler."
        ),
        "executor_prompt": "Implement tasks/milestone_decomposer.py for the Hermes Orchestrator.",
    }

    steering_engine = SteeringMemoryEngine()
    cost_estimator = CostEstimator()
    scheduler = TopologicalScheduler()
    feasibility_gate = FeasibilityGate()

    # Add initial parent node into the scheduler DAG
    scheduler.add_node(sample_milestone["id"], sample_milestone)

    decomposer = MilestoneDecomposer(
        steering_memory=steering_engine, cost_estimator=cost_estimator
    )
    coordinator = ReplanningCoordinator(
        decomposer=decomposer,
        feasibility_gate=feasibility_gate,
        scheduler=scheduler,
        steering_memory=steering_engine,
        cost_estimator=cost_estimator,
    )

    # 1. Test FEASIBILITY_GATE Trigger
    print("\n[TEST 1] Testing Feasibility Gate Trigger Decomposition...")
    result_gate = coordinator.coordinate_decomposition(
        milestone=sample_milestone,
        trigger=DecomposeTrigger.FEASIBILITY_GATE,
        checkpoint=None,
    )

    print(f"Result Status: Escalate to Planner = {result_gate.escalate_to_planner}")
    print(f"Total Sub-Milestones Created: {len(result_gate.sub_milestones)}")
    for idx, sub in enumerate(result_gate.sub_milestones):
        tokens = sub.estimated_cost.predicted_tokens if sub.estimated_cost else 0
        print(f"  [{idx + 1}] {sub.name}")
        print(f"      Status: {sub.status} | Satisfied: {sub.checkpoint_satisfied}")
        print(f"      Tokens: {tokens} | Dependencies: {sub.dependency_edges}")

    assert not result_gate.escalate_to_planner, "Feasibility Gate decomposition should succeed"
    assert len(result_gate.sub_milestones) >= 4, "Should extract at least 4 functional units"

    # 2. Test GOVERNOR_PREEMPT Trigger with Checkpoint Alignment
    print("\n[TEST 2] Testing Governor Preempt Trigger Decomposition (Phase 1 Checkpointed)...")
    checkpoint = GovernorCheckpoint(last_completed_phase=1, checkpoint_id="chk_flight_01")
    result_preempt = coordinator.coordinate_decomposition(
        milestone=sample_milestone,
        trigger=DecomposeTrigger.GOVERNOR_PREEMPT,
        checkpoint=checkpoint,
    )

    print(f"Phases Skipped: {result_preempt.checkpoint_phases_skipped}")
    satisfied_count = sum(1 for m in result_preempt.sub_milestones if m.checkpoint_satisfied)
    print(f"Sub-Milestones Marked Completed: {satisfied_count}")

    for idx, sub in enumerate(result_preempt.sub_milestones):
        print(f"  [{idx + 1}] {sub.name} -> status='{sub.status}', checkpoint_satisfied={sub.checkpoint_satisfied}")

    assert result_preempt.checkpoint_phases_skipped >= 2, "Governor checkpoint should mark first 2 phases satisfied"

    # 3. Verify Scheduler DAG State
    print("\n[TEST 3] Verifying Scheduler DAG State...")
    print(f"Total nodes in Scheduler DAG: {len(scheduler.nodes)}")
    print(f"Parent node present: {sample_milestone['id'] in scheduler.nodes}")
    assert sample_milestone["id"] not in scheduler.nodes, "Parent node should be replaced in DAG"

    print("\nAll standalone verification checks passed successfully.")
    print("=" * 80)