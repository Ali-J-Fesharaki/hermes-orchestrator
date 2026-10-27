#!/usr/bin/env python3
"""
Pipeline Autonomic Architectural Drift Sentinel & Subsystem Entanglement Governor.

This module provides static AST analysis, formal architectural metrics (Martin's
Instability, Abstractness, and Distance from Main Sequence; Newman-Girvan Modularity;
Entanglement Density), enforces strict multi-tier layering invariants, and acts as a
pre-dispatch architectural gate preventing structural code erosion in the Hermes
autonomous multi-agent orchestrator.

Zero external dependencies beyond the Python standard library.
"""

from __future__ import annotations

import ast
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
import enum
import json
import logging
import math
import os
from pathlib import Path
import re
import sys
import time
from typing import Any, Dict, List, Literal, Optional, Set, Tuple, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [ArchitecturalSentinel] %(message)s"
)
logger = logging.getLogger("ArchitecturalSentinel")


# =====================================================================
# 1. ARCHITECTURAL TIERS & INVARIANT SPECIFICATIONS
# =====================================================================

class ArchitecturalTier(enum.IntEnum):
    """
    Strict layered architectural hierarchy for the autonomous multi-repo platform.
    Lower numerical values represent lower, foundational tiers that must NEVER depend
    on higher-level tiers (strict downward dependency invariant).
    """
    FOUNDATION = 0            # States, schemas, provenance, parameter registries
    CORE_ENGINE = 1           # Orchestrator core, threadpools, git sandboxes, executors
    AUTONOMIC_LOOP = 2        # Learning, simulation, experiment runners, calibration
    GOVERNANCE_CONSENSUS = 3   # Directives, consensus quorum, promotion gateway, sentinel
    COCKPIT_API = 4           # Server, interactive architect API, visual dashboards

    @classmethod
    def canonical_tier(cls, module_or_filename: str) -> ArchitecturalTier:
        """
        Maps a module name, file path, or component slug to its canonical ArchitecturalTier.
        """
        clean_path = module_or_filename.replace("\\", "/").lower()
        base_name = Path(clean_path).stem

        # Cockpit API / Visual Dashboards (Tier 4)
        if any(k in clean_path for k in ["dashboard", "cockpit", "server", "interactive_api", "ui", "control_center"]):
            return cls.COCKPIT_API

        # Governance & Consensus (Tier 3)
        if any(k in base_name for k in [
            "consensus", "governor", "governance", "directive", "promotion_gateway",
            "rollback", "sentinel", "quorum", "architectural_sentinel"
        ]):
            return cls.GOVERNANCE_CONSENSUS

        # Autonomic Loop & Adaptive Systems (Tier 2)
        if any(k in base_name for k in [
            "learning", "simulation", "experiment", "calibration", "knowledge_graph",
            "evolution", "drift", "loop", "cadence", "telemetry", "optimizer"
        ]):
            return cls.AUTONOMIC_LOOP

        # Core Engine (Tier 1)
        if any(k in base_name for k in [
            "orchestrator", "threadpool", "git_sandbox", "executor", "engine",
            "actor_pod", "worker", "sandboxes", "scheduler", "pipeline_core"
        ]):
            return cls.CORE_ENGINE

        # Foundation / Data Contracts (Tier 0)
        if any(k in base_name for k in [
            "config_registry", "state", "provenance", "schema", "parameter_catalog",
            "types", "models", "primitives", "constants", "vault"
        ]):
            return cls.FOUNDATION

        # Default fallback: infer by parent directory
        if "dashboards" in clean_path or "control_center" in clean_path:
            return cls.COCKPIT_API
        if "tasks" in clean_path:
            return cls.AUTONOMIC_LOOP

        return cls.CORE_ENGINE


class InvariantSeverity(enum.Enum):
    """Severity classification for architectural invariant breaches."""
    ADVISORY = "ADVISORY"
    WARNING = "WARNING"
    CRITICAL_BLOCK = "CRITICAL_BLOCK"


@dataclass
class InvariantViolation:
    """Represents a discrete violation of system architectural layering rules."""
    rule_name: str
    source_module: str
    target_module: str
    source_tier: ArchitecturalTier
    target_tier: ArchitecturalTier
    severity: InvariantSeverity
    explanation: str

    def to_dict(self) -> dict:
        return {
            "rule_name": self.rule_name,
            "source_module": self.source_module,
            "target_module": self.target_module,
            "source_tier": self.source_tier.name,
            "target_tier": self.target_tier.name,
            "severity": self.severity.value,
            "explanation": self.explanation
        }


@dataclass
class SubsystemCouplingMetrics:
    """Formal Robert C. Martin package coupling metrics for an individual module."""
    subsystem: str
    tier: ArchitecturalTier
    afferent_coupling_ca: int              # Number of incoming external dependents
    efferent_coupling_ce: int              # Number of outgoing internal dependencies
    instability_i: float                  # I = Ce / (Ca + Ce)
    abstractness_a: float                 # A = abstract_classes / total_classes
    distance_from_main_sequence_d: float  # D = |A + I - 1|
    direct_dependencies: List[str]
    direct_dependents: List[str]
    loc_count: int
    class_count: int
    abstract_class_count: int

    def to_dict(self) -> dict:
        return {
            "subsystem": self.subsystem,
            "tier": self.tier.name,
            "afferent_coupling_ca": self.afferent_coupling_ca,
            "efferent_coupling_ce": self.efferent_coupling_ce,
            "instability_i": round(self.instability_i, 4),
            "abstractness_a": round(self.abstractness_a, 4),
            "distance_from_main_sequence_d": round(self.distance_from_main_sequence_d, 4),
            "direct_dependencies": self.direct_dependencies,
            "direct_dependents": self.direct_dependents,
            "loc_count": self.loc_count,
            "class_count": self.class_count,
            "abstract_class_count": self.abstract_class_count
        }


@dataclass
class ArchitecturalHealthReport:
    """Holistic architectural health audit of the codebase."""
    timestamp: float
    modularity_q: float
    mean_main_sequence_distance: float
    entanglement_density: float
    tier_violations: List[InvariantViolation]
    circular_dependency_chains: List[List[str]]
    metrics_by_subsystem: Dict[str, SubsystemCouplingMetrics]
    high_coupling_pairs: List[Dict[str, Any]]
    refactoring_proposals: List[Dict[str, Any]]

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "modularity_q": round(self.modularity_q, 4),
            "mean_main_sequence_distance": round(self.mean_main_sequence_distance, 4),
            "entanglement_density": round(self.entanglement_density, 4),
            "tier_violations": [v.to_dict() for v in self.tier_violations],
            "circular_dependency_chains": self.circular_dependency_chains,
            "metrics_by_subsystem": {k: v.to_dict() for k, v in self.metrics_by_subsystem.items()},
            "high_coupling_pairs": self.high_coupling_pairs,
            "refactoring_proposals": self.refactoring_proposals
        }


@dataclass
class ArchitecturalGateDecision:
    """Pre-dispatch gate decision evaluating structural impact of a proposed milestone."""
    milestone_id: str
    target_filepath: str
    verdict: Literal["ALLOW", "WARN", "BLOCK", "DECOMPOSE"]
    predicted_delta_d: float
    predicted_delta_coupling: float
    violates_tier_boundary: bool
    introduces_cycle: bool
    blocking_violations: List[str]
    suggested_refactorings: List[str]
    evaluation_timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "milestone_id": self.milestone_id,
            "target_filepath": self.target_filepath,
            "verdict": self.verdict,
            "predicted_delta_d": round(self.predicted_delta_d, 4),
            "predicted_delta_coupling": round(self.predicted_delta_coupling, 4),
            "violates_tier_boundary": self.violates_tier_boundary,
            "introduces_cycle": self.introduces_cycle,
            "blocking_violations": self.blocking_violations,
            "suggested_refactorings": self.suggested_refactorings,
            "evaluation_timestamp": self.evaluation_timestamp
        }


# =====================================================================
# 2. CODEBASE AST ANALYZER
# =====================================================================

class CodebaseASTAnalyzer:
    """
    Parses Python source code ASTs to extract imports, inheritance hierarchies,
    abstract class declarations, function definitions, and volume metrics.
    """

    @staticmethod
    def parse_file(filepath: Union[str, Path]) -> Dict[str, Any]:
        """
        Parses a single Python file into an architectural metadata dictionary.
        Handles relative imports, ABCMeta, @abstractmethod, and function counts.
        """
        path = Path(filepath)
        if not path.exists() or not path.is_file():
            return {
                "filepath": str(path),
                "module_name": path.stem,
                "imports": set(),
                "classes": 0,
                "abstract_classes": 0,
                "functions": 0,
                "loc_count": 0,
                "error": "File does not exist"
            }

        try:
            content = path.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            return {
                "filepath": str(path),
                "module_name": path.stem,
                "imports": set(),
                "classes": 0,
                "abstract_classes": 0,
                "functions": 0,
                "loc_count": 0,
                "error": str(e)
            }

        loc_count = len([line for line in content.splitlines() if line.strip() and not line.strip().startswith("#")])

        try:
            tree = ast.parse(content, filename=str(path))
        except SyntaxError:
            # Fallback regex parsing if AST syntax fails due to syntax edge cases
            raw_imports = set(re.findall(r"^(?:from|import)\s+([\w\.]+)", content, re.MULTILINE))
            return {
                "filepath": str(path),
                "module_name": path.stem,
                "imports": raw_imports,
                "classes": len(re.findall(r"^class\s+\w+", content, re.MULTILINE)),
                "abstract_classes": len(re.findall(r"class\s+\w+\(.*(?:ABC|abstract).*\):", content)),
                "functions": len(re.findall(r"^\s*def\s+\w+", content, re.MULTILINE)),
                "loc_count": loc_count,
                "error": "Parsed via regex fallback (SyntaxError in ast.parse)"
            }

        imports: Set[str] = set()
        total_classes = 0
        abstract_classes = 0
        total_functions = 0

        # Extract imported symbols and modules
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.add(alias.name)
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if node.level and node.level > 0:
                    # Resolve relative import prefix
                    parent_parts = path.parts[:-node.level]
                    if parent_parts:
                        prefix = ".".join(parent_parts[-2:])
                        resolved = f"{prefix}.{module}" if module else prefix
                        imports.add(resolved)
                    else:
                        imports.add(module)
                else:
                    imports.add(module)

            elif isinstance(node, ast.ClassDef):
                total_classes += 1
                is_abstract = False

                # Check base classes for ABC or ABCMeta
                for base in node.bases:
                    if isinstance(base, ast.Name) and base.id in ["ABC", "ABCMeta"]:
                        is_abstract = True
                        break
                    elif isinstance(base, ast.Attribute) and base.attr in ["ABC", "ABCMeta"]:
                        is_abstract = True
                        break

                # Check metaclass keyword
                for keyword in node.keywords:
                    if keyword.arg == "metaclass":
                        if isinstance(keyword.value, ast.Name) and "ABC" in keyword.value.id:
                            is_abstract = True
                        elif isinstance(keyword.value, ast.Attribute) and "ABC" in keyword.value.attr:
                            is_abstract = True

                # Check class body for @abstractmethod decorators
                if not is_abstract:
                    for body_item in node.body:
                        if isinstance(body_item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                            for dec in body_item.decorator_list:
                                if isinstance(dec, ast.Name) and "abstract" in dec.id.lower():
                                    is_abstract = True
                                    break
                                elif isinstance(dec, ast.Attribute) and "abstract" in dec.attr.lower():
                                    is_abstract = True
                                    break
                            if is_abstract:
                                break

                if is_abstract:
                    abstract_classes += 1

            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                total_functions += 1

        return {
            "filepath": str(path),
            "module_name": path.stem,
            "imports": imports,
            "classes": total_classes,
            "abstract_classes": abstract_classes,
            "functions": total_functions,
            "loc_count": loc_count,
            "error": None
        }

    @classmethod
    def scan_directories(cls, search_paths: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Recursively scans directory search paths for Python files and extracts metadata.
        """
        results: Dict[str, Dict[str, Any]] = {}
        for search_path in search_paths:
            p = Path(search_path)
            if not p.exists():
                continue
            if p.is_file() and p.suffix == ".py":
                data = cls.parse_file(p)
                results[p.stem] = data
            elif p.is_dir():
                for py_file in p.rglob("*.py"):
                    if "__pycache__" in py_file.parts or ".pytest_cache" in py_file.parts:
                        continue
                    data = cls.parse_file(py_file)
                    results[py_file.stem] = data
        return results


# =====================================================================
# 3. TARJAN STRONGLY CONNECTED COMPONENTS (CYCLE DETECTOR)
# =====================================================================

class TarjanCycleDetector:
    """
    Implements Tarjan's linear-time algorithm O(V + E) to detect strongly
    connected components (SCCs) and recursive circular dependency chains.
    """

    @staticmethod
    def detect_cycles(adjacency: Dict[str, Set[str]]) -> List[List[str]]:
        """
        Finds all SCCs with size >= 2, or size == 1 with a self-loop.
        Returns canonical lists of module cycles sorted deterministically.
        """
        index = 0
        indices: Dict[str, int] = {}
        lowlink: Dict[str, int] = {}
        stack: List[str] = []
        on_stack: Set[str] = set()
        sccs: List[List[str]] = []

        all_nodes: Set[str] = set(adjacency.keys())
        for targets in adjacency.values():
            all_nodes.update(targets)

        def strongconnect(node: str):
            nonlocal index
            indices[node] = index
            lowlink[node] = index
            index += 1
            stack.append(node)
            on_stack.add(node)

            for neighbor in sorted(adjacency.get(node, set())):
                if neighbor not in indices:
                    strongconnect(neighbor)
                    lowlink[node] = min(lowlink[node], lowlink[neighbor])
                elif neighbor in on_stack:
                    lowlink[node] = min(lowlink[node], indices[neighbor])

            if lowlink[node] == indices[node]:
                current_scc: List[str] = []
                while True:
                    w = stack.pop()
                    on_stack.remove(w)
                    current_scc.append(w)
                    if w == node:
                        break

                # An SCC represents a circular dependency if size >= 2 or self-cycle
                if len(current_scc) >= 2 or (len(current_scc) == 1 and node in adjacency.get(node, set())):
                    sccs.append(sorted(current_scc))

        for item in sorted(all_nodes):
            if item not in indices:
                strongconnect(item)

        return sorted(sccs, key=lambda c: (len(c), c))


# =====================================================================
# 4. ARCHITECTURAL SENTINEL ENGINE
# =====================================================================

class ArchitecturalSentinelEngine:
    """
    Autonomic sentinel enforcing structural invariants, calculating Martin metrics,
    computing Newman-Girvan modularity, and gating pre-dispatch autonomous agent tasks.
    """

    def __init__(
        self,
        search_paths: Optional[List[str]] = None,
        tier_overrides: Optional[Dict[str, ArchitecturalTier]] = None
    ):
        self.search_paths = search_paths or ["tasks", "dashboards", "control_center"]
        self.tier_overrides = tier_overrides or {}
        self.analyzer = CodebaseASTAnalyzer()

    def analyze_module_ast(self, filepath: str) -> Dict[str, Any]:
        """Parses AST for a given module file."""
        return self.analyzer.parse_file(filepath)

    def build_static_dependency_graph(
        self,
        search_paths: Optional[List[str]] = None
    ) -> Tuple[Dict[str, Set[str]], Dict[str, Set[str]]]:
        """
        Builds forward and reverse internal dependency adjacency maps.
        Filters out external third-party and standard library packages.
        """
        paths = search_paths or self.search_paths
        ast_data = self.analyzer.scan_directories(paths)
        known_modules = set(ast_data.keys())

        forward_graph: Dict[str, Set[str]] = {m: set() for m in known_modules}
        reverse_graph: Dict[str, Set[str]] = {m: set() for m in known_modules}

        for module_name, meta in ast_data.items():
            for imported in meta["imports"]:
                if not imported:
                    continue
                # Resolve imported tokens (e.g. 'tasks.pipeline_consensus' -> 'pipeline_consensus')
                target = imported.split(".")[-1]
                if target in known_modules and target != module_name:
                    forward_graph[module_name].add(target)
                    reverse_graph[target].add(module_name)
                elif imported in known_modules and imported != module_name:
                    forward_graph[module_name].add(imported)
                    reverse_graph[imported].add(module_name)
                else:
                    # Substring search for prefix match
                    for km in known_modules:
                        if km != module_name and (imported.endswith(f".{km}") or imported == km):
                            forward_graph[module_name].add(km)
                            reverse_graph[km].add(module_name)

        return forward_graph, reverse_graph

    def detect_cyclic_dependencies(self, dep_graph: Dict[str, Set[str]]) -> List[List[str]]:
        """Finds all dependency cycles in the provided graph."""
        return TarjanCycleDetector.detect_cycles(dep_graph)

    def compute_martin_metrics(
        self,
        dep_graph: Dict[str, Set[str]],
        ast_data: Dict[str, Dict[str, Any]]
    ) -> Dict[str, SubsystemCouplingMetrics]:
        """
        Computes formal Martin metrics for all subsystems:
        - Ca: Afferent Coupling (incoming dependencies)
        - Ce: Efferent Coupling (outgoing dependencies)
        - Instability I = Ce / (Ca + Ce)
        - Abstractness A = abstract_classes / total_classes
        - Distance from Main Sequence D = |A + I - 1|
        """
        # Calculate reverse dependencies (Ca)
        reverse_graph: Dict[str, Set[str]] = defaultdict(set)
        all_nodes = set(dep_graph.keys()).union(ast_data.keys())

        for src, targets in dep_graph.items():
            for tgt in targets:
                reverse_graph[tgt].add(src)

        metrics: Dict[str, SubsystemCouplingMetrics] = {}

        for mod in sorted(all_nodes):
            meta = ast_data.get(mod, {
                "filepath": mod,
                "classes": 0,
                "abstract_classes": 0,
                "loc_count": 0
            })

            deps = sorted(list(dep_graph.get(mod, set())))
            dependents = sorted(list(reverse_graph.get(mod, set())))

            ce = len(deps)
            ca = len(dependents)

            # Instability I
            if (ca + ce) > 0:
                instability_i = ce / float(ca + ce)
            else:
                instability_i = 0.0

            # Abstractness A
            total_classes = meta.get("classes", 0)
            abstract_classes = meta.get("abstract_classes", 0)
            if total_classes > 0:
                abstractness_a = abstract_classes / float(total_classes)
            else:
                abstractness_a = 0.0

            # Distance from Main Sequence D = |A + I - 1|
            distance_d = abs(abstractness_a + instability_i - 1.0)

            tier = self.tier_overrides.get(mod, ArchitecturalTier.canonical_tier(meta.get("filepath", mod)))

            metrics[mod] = SubsystemCouplingMetrics(
                subsystem=mod,
                tier=tier,
                afferent_coupling_ca=ca,
                efferent_coupling_ce=ce,
                instability_i=instability_i,
                abstractness_a=abstractness_a,
                distance_from_main_sequence_d=distance_d,
                direct_dependencies=deps,
                direct_dependents=dependents,
                loc_count=meta.get("loc_count", 0),
                class_count=total_classes,
                abstract_class_count=abstract_classes
            )

        return metrics

    def compute_newman_girvan_modularity(
        self,
        dep_graph: Dict[str, Set[str]],
        tier_map: Dict[str, ArchitecturalTier]
    ) -> float:
        """
        Computes Newman-Girvan Modularity Q across architectural tiers:
        Q = sum_c [ e_c / m - (d_c / (2m))^2 ]
        where e_c is the number of edges within tier c,
        d_c is the total degree (in + out) of nodes in tier c,
        and m is the total number of edges in the network.
        """
        total_edges = sum(len(targets) for targets in dep_graph.values())
        if total_edges == 0:
            return 0.0

        m = float(total_edges)
        two_m = 2.0 * m

        # Group nodes by tier
        tier_nodes: Dict[ArchitecturalTier, Set[str]] = defaultdict(set)
        for node in dep_graph.keys():
            t = tier_map.get(node, ArchitecturalTier.canonical_tier(node))
            tier_nodes[t].add(node)

        # Precompute out and in degrees
        out_degrees: Dict[str, int] = {u: len(targets) for u, targets in dep_graph.items()}
        in_degrees: Dict[str, int] = defaultdict(int)
        for targets in dep_graph.values():
            for v in targets:
                in_degrees[v] += 1

        modularity_q = 0.0

        for tier, nodes in tier_nodes.items():
            # Internal edges within this tier
            e_c = 0
            for u in nodes:
                targets = dep_graph.get(u, set())
                e_c += len(targets.intersection(nodes))

            # Total degree d_c = sum(k_out + k_in) for nodes in tier
            d_c = sum(out_degrees.get(u, 0) + in_degrees.get(u, 0) for u in nodes)

            term1 = e_c / m
            term2 = (d_c / two_m) ** 2
            modularity_q += (term1 - term2)

        return max(-1.0, min(1.0, modularity_q))

    def check_tier_invariants(
        self,
        dep_graph: Dict[str, Set[str]],
        tier_map: Optional[Dict[str, ArchitecturalTier]] = None
    ) -> List[InvariantViolation]:
        """
        Enforces layering invariants across tiers:
        - Downward Layering: Tier L_i must NOT depend on Tier L_j where j > i.
        - Strict Skip Layering: Tier 4 (Cockpit) bypassing intermediate tiers directly to Tier 0.
        """
        violations: List[InvariantViolation] = []
        actual_tier_map = tier_map or {}

        for src, targets in dep_graph.items():
            src_tier = actual_tier_map.get(src, ArchitecturalTier.canonical_tier(src))

            for tgt in targets:
                tgt_tier = actual_tier_map.get(tgt, ArchitecturalTier.canonical_tier(tgt))

                # Violation 1: Upward dependency (Lower tier imports Higher tier)
                if tgt_tier > src_tier:
                    skip = tgt_tier - src_tier
                    severity = InvariantSeverity.CRITICAL_BLOCK if skip >= 1 else InvariantSeverity.WARNING
                    violations.append(InvariantViolation(
                        rule_name="STRICT_DOWNWARD_LAYERING_VIOLATION",
                        source_module=src,
                        target_module=tgt,
                        source_tier=src_tier,
                        target_tier=tgt_tier,
                        severity=severity,
                        explanation=(
                            f"Illegal upward dependency: '{src}' ({src_tier.name}, Tier {src_tier.value}) "
                            f"imports '{tgt}' ({tgt_tier.name}, Tier {tgt_tier.value}). Lower tiers must never depend on higher tiers."
                        )
                    ))

                # Violation 2: Advisory skip layer (Cockpit directly coupling to foundation)
                elif src_tier == ArchitecturalTier.COCKPIT_API and tgt_tier == ArchitecturalTier.FOUNDATION:
                    violations.append(InvariantViolation(
                        rule_name="COCKPIT_FOUNDATION_BYPASS_WARNING",
                        source_module=src,
                        target_module=tgt,
                        source_tier=src_tier,
                        target_tier=tgt_tier,
                        severity=InvariantSeverity.ADVISORY,
                        explanation=(
                            f"Direct foundation coupling: Cockpit module '{src}' directly imports foundation '{tgt}'. "
                            f"Prefer mediating through Core Engine or Governance interfaces."
                        )
                    ))

        return violations

    def audit_architecture(
        self,
        search_paths: Optional[List[str]] = None
    ) -> ArchitecturalHealthReport:
        """
        Performs a full architectural audit across all configured directories,
        calculating metrics, modularity, cycles, tier violations, and refactoring proposals.
        """
        paths = search_paths or self.search_paths
        ast_data = self.analyzer.scan_directories(paths)
        forward_graph, _ = self.build_static_dependency_graph(paths)

        # Build tier map
        tier_map = {
            m: self.tier_overrides.get(m, ArchitecturalTier.canonical_tier(ast_data.get(m, {}).get("filepath", m)))
            for m in ast_data.keys()
        }

        # Cycles
        cycles = self.detect_cyclic_dependencies(forward_graph)

        # Martin Metrics
        metrics = self.compute_martin_metrics(forward_graph, ast_data)

        # Mean Main Sequence Distance
        distances = [m.distance_from_main_sequence_d for m in metrics.values()]
        mean_d = sum(distances) / float(len(distances)) if distances else 0.0

        # Modularity Q
        q = self.compute_newman_girvan_modularity(forward_graph, tier_map)

        # Entanglement Density = total_edges / max_possible_edges
        n = len(forward_graph)
        total_edges = sum(len(t) for t in forward_graph.values())
        max_edges = n * (n - 1) if n > 1 else 1
        density = total_edges / float(max_edges) if n > 1 else 0.0

        # Tier Violations
        tier_violations = self.check_tier_invariants(forward_graph, tier_map)

        # High Coupling Pairs (Bilateral or high Jaccard similarity)
        high_coupling_pairs: List[Dict[str, Any]] = []
        module_list = sorted(list(forward_graph.keys()))
        for i in range(len(module_list)):
            for j in range(i + 1, len(module_list)):
                m1, m2 = module_list[i], module_list[j]
                deps1 = forward_graph.get(m1, set())
                deps2 = forward_graph.get(m2, set())

                union_deps = deps1.union(deps2)
                if not union_deps:
                    continue

                jaccard = len(deps1.intersection(deps2)) / float(len(union_deps))
                mutual = (m2 in deps1) and (m1 in deps2)

                if jaccard > 0.60 or mutual:
                    high_coupling_pairs.append({
                        "module_a": m1,
                        "module_b": m2,
                        "jaccard_coupling": round(jaccard, 3),
                        "mutual_dependency": mutual,
                        "shared_dependencies_count": len(deps1.intersection(deps2))
                    })

        # Synthesize audit report
        report = ArchitecturalHealthReport(
            timestamp=time.time(),
            modularity_q=q,
            mean_main_sequence_distance=mean_d,
            entanglement_density=density,
            tier_violations=tier_violations,
            circular_dependency_chains=cycles,
            metrics_by_subsystem=metrics,
            high_coupling_pairs=high_coupling_pairs,
            refactoring_proposals=[]
        )

        # Generate refactoring proposals
        report.refactoring_proposals = self.generate_refactoring_recommendations(report)
        return report

    def evaluate_milestone(self, milestone_metadata: Dict[str, Any]) -> ArchitecturalGateDecision:
        """
        Pre-dispatch gate evaluating the structural impact of a proposed task milestone.
        Predicts whether adding the target file and proposed imports introduces cyclic
        dependencies, tier breaches, or moves the subsystem into the Zone of Pain.
        """
        milestone_id = milestone_metadata.get("milestone_id", "M-UNKNOWN")
        target_path = milestone_metadata.get("target_filepath") or milestone_metadata.get("relative_filepath", "")
        spec_text = milestone_metadata.get("technical_specification", "")
        proposed_imports = set(milestone_metadata.get("proposed_imports", []))

        # Infer target module and tier
        target_stem = Path(target_path).stem if target_path else "candidate_module"
        target_tier = ArchitecturalTier.canonical_tier(target_path)

        # Extract potential imports mentioned in specification text
        inferred_tokens = re.findall(r"(?:tasks|dashboards|control_center)\.([\w_]+)|(?:from|import)\s+([\w_]+)", spec_text)
        for token_tuple in inferred_tokens:
            for token in token_tuple:
                if token and len(token) > 3 and token != target_stem:
                    proposed_imports.add(token)

        # Load existing graph
        existing_graph, _ = self.build_static_dependency_graph()
        simulated_graph = {k: set(v) for k, v in existing_graph.items()}
        simulated_graph[target_stem] = set()

        blocking_violations: List[str] = []
        violates_tier = False

        # Check tier boundaries for proposed dependencies
        for imp in proposed_imports:
            # Map imported module to tier
            imp_tier = ArchitecturalTier.canonical_tier(imp)
            if imp in existing_graph or any(k in imp for k in ["pipeline_", "orchestrator", "config_"]):
                simulated_graph[target_stem].add(imp)

                if imp_tier > target_tier:
                    violates_tier = True
                    msg = (
                        f"Tier Inversion: Candidate '{target_stem}' ({target_tier.name}) "
                        f"proposes importing higher tier '{imp}' ({imp_tier.name})."
                    )
                    blocking_violations.append(msg)

        # Check for circular dependency introduction
        cycles = TarjanCycleDetector.detect_cycles(simulated_graph)
        introduces_cycle = False
        for cycle in cycles:
            if target_stem in cycle:
                introduces_cycle = True
                blocking_violations.append(f"Cycle Introduction: Subsystem participates in recursive cycle: {' -> '.join(cycle)}.")

        # Estimate Martin metrics impact
        ce_predicted = len(simulated_graph.get(target_stem, set()))
        ca_predicted = 0
        for src, targets in simulated_graph.items():
            if target_stem in targets:
                ca_predicted += 1

        i_pred = ce_predicted / float(ca_predicted + ce_predicted) if (ca_predicted + ce_predicted) > 0 else 0.0
        # Assume standard concrete class unless spec mentions abstract base class or interfaces
        is_abstract = any(k in spec_text.lower() for k in ["abstract", "interface", "protocol", "abc."])
        a_pred = 0.5 if is_abstract else 0.0
        d_pred = abs(a_pred + i_pred - 1.0)

        # Check thresholds
        suggested_refactorings: List[str] = []
        predicted_loc = milestone_metadata.get("predicted_loc", 350)

        if violates_tier or introduces_cycle:
            verdict: Literal["ALLOW", "WARN", "BLOCK", "DECOMPOSE"] = "BLOCK"
            suggested_refactorings.append("Invert dependencies using events, callbacks, or move shared models to Tier 0 FOUNDATION.")
        elif predicted_loc > 800 or ce_predicted > 12:
            verdict = "DECOMPOSE"
            suggested_refactorings.append(
                f"Predicted size ({predicted_loc} LOC, {ce_predicted} dependencies) exceeds atomic threshold. "
                "Decompose milestone into focused modular sub-tasks."
            )
        elif d_pred >= 0.60 or ce_predicted >= 8:
            verdict = "WARN"
            suggested_refactorings.append(
                f"Elevated distance from main sequence (D={round(d_pred, 2)}) or high coupling. Consider introducing Facade or Adapter."
            )
        else:
            verdict = "ALLOW"

        return ArchitecturalGateDecision(
            milestone_id=milestone_id,
            target_filepath=target_path,
            verdict=verdict,
            predicted_delta_d=d_pred,
            predicted_delta_coupling=float(ce_predicted),
            violates_tier_boundary=violates_tier,
            introduces_cycle=introduces_cycle,
            blocking_violations=blocking_violations,
            suggested_refactorings=suggested_refactorings
        )

    def generate_refactoring_recommendations(
        self,
        report: ArchitecturalHealthReport
    ) -> List[Dict[str, Any]]:
        """
        Analyzes the audit report to identify structural anti-patterns:
        - Zone of Pain (D > 0.6, I < 0.2): Concrete, rigid, hard to change.
        - Zone of Uselessness (D > 0.6, I > 0.8): Abstract, no dependents.
        - Entangled Clusters: Recommends Mediator, Facade, or DIP patterns.
        """
        proposals: List[Dict[str, Any]] = []

        for name, metrics in report.metrics_by_subsystem.items():
            # Zone of Pain: High Ca, low Ce, low A (rigid and brittle)
            if metrics.distance_from_main_sequence_d > 0.60 and metrics.instability_i < 0.20:
                proposals.append({
                    "subsystem": name,
                    "tier": metrics.tier.name,
                    "zone": "ZONE_OF_PAIN",
                    "severity": "HIGH",
                    "pattern": "Dependency Inversion / Interface Segregation",
                    "rationale": (
                        f"Subsystem '{name}' is highly stable (I={round(metrics.instability_i, 2)}) and concrete (A={round(metrics.abstractness_a, 2)}), "
                        f"placing it deep in the Zone of Pain (D={round(metrics.distance_from_main_sequence_d, 2)}). Many modules depend on its concrete implementation."
                    ),
                    "action": "Extract Abstract Base Classes or Protocols; decouple concrete implementations via dependency injection."
                })

            # Zone of Uselessness: High I, high A (abstract with zero dependents)
            elif metrics.distance_from_main_sequence_d > 0.60 and metrics.instability_i > 0.80:
                proposals.append({
                    "subsystem": name,
                    "tier": metrics.tier.name,
                    "zone": "ZONE_OF_USELESSNESS",
                    "severity": "MEDIUM",
                    "pattern": "Collapse Speculative Hierarchy",
                    "rationale": (
                        f"Subsystem '{name}' is highly abstract (A={round(metrics.abstractness_a, 2)}) with no dependents, "
                        f"in the Zone of Uselessness (D={round(metrics.distance_from_main_sequence_d, 2)})."
                    ),
                    "action": "Consolidate ABCs with concrete classes or inline unused speculative interfaces."
                })

        # Circular dependencies refactoring
        for cycle in report.circular_dependency_chains:
            proposals.append({
                "subsystem": ", ".join(cycle),
                "tier": "CROSS_TIER",
                "zone": "RECURSIVE_CYCLE",
                "severity": "CRITICAL",
                "pattern": "Mediator / Event Observer",
                "rationale": f"Cyclic dependency detected: {' <-> '.join(cycle)}.",
                "action": "Decouple cycle by extracting shared data models into FOUNDATION tier or using an async event bus."
            })

        # High coupling pairs
        for pair in report.high_coupling_pairs:
            if pair.get("mutual_dependency"):
                proposals.append({
                    "subsystem": f"{pair['module_a']} <-> {pair['module_b']}",
                    "tier": "INTER_MODULE",
                    "zone": "BILATERAL_ENTANGLEMENT",
                    "severity": "HIGH",
                    "pattern": "Extract Common Intermediary",
                    "rationale": f"Modules '{pair['module_a']}' and '{pair['module_b']}' are mutually coupled.",
                    "action": "Merge co-dependent logic or introduce an intermediary mediator to eliminate direct coupling."
                })

        return proposals

    def export_markdown_digest(
        self,
        report: ArchitecturalHealthReport,
        output_path: str = "dashboards/ARCHITECTURAL_HEALTH.md"
    ) -> str:
        """
        Renders the architectural audit report into a publication-grade Markdown digest.
        Saves to output_path if writable and returns the markdown string.
        """
        lines = [
            "# Autonomic Architectural Drift & Subsystem Entanglement Report",
            f"**Audit Timestamp:** `{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(report.timestamp))}`  ",
            f"**Newman-Girvan Modularity (Q):** `{round(report.modularity_q, 4)}`  ",
            f"**Mean Distance from Main Sequence (D):** `{round(report.mean_main_sequence_distance, 4)}`  ",
            f"**Graph Entanglement Density:** `{round(report.entanglement_density * 100, 2)}%`  ",
            f"**Total Tier Invariant Violations:** `{len(report.tier_violations)}`  ",
            f"**Detected Cyclic Dependency Chains:** `{len(report.circular_dependency_chains)}`",
            "",
            "---",
            "",
            "## 1. Architectural Tiers & Layering Invariants",
            "",
            "| Tier Level | Tier Name | Canonical Responsibilities | Subsystem Count |",
            "|:---|:---|:---|:---|"
        ]

        # Tier breakdown
        tier_counts: Dict[ArchitecturalTier, int] = defaultdict(int)
        for m in report.metrics_by_subsystem.values():
            tier_counts[m.tier] += 1

        tier_descriptions = {
            ArchitecturalTier.FOUNDATION: "State machines, config registries, parameter schemas, provenance",
            ArchitecturalTier.CORE_ENGINE: "Orchestrator loop, threadpools, git sandboxes, task executors",
            ArchitecturalTier.AUTONOMIC_LOOP: "Learning engines, experiment simulation, knowledge graphs",
            ArchitecturalTier.GOVERNANCE_CONSENSUS: "Quorum consensus, directive governors, architectural gates",
            ArchitecturalTier.COCKPIT_API: "FastAPI server, interactive architect console, visual dashboards"
        }

        for tier in ArchitecturalTier:
            lines.append(f"| `Tier {tier.value}` | **{tier.name}** | {tier_descriptions.get(tier, '')} | `{tier_counts[tier]}` |")

        lines.extend([
            "",
            "---",
            "",
            "## 2. Invariant Layering Violations",
            ""
        ])

        if not report.tier_violations:
            lines.append("✅ **Clean Layering:** Zero tier boundary violations detected. Downward dependency invariant holds.")
        else:
            lines.append("| Severity | Rule Name | Source Module | Target Module | Source Tier | Target Tier | Explanation |")
            lines.append("|:---|:---|:---|:---|:---|:---|:---|")
            for v in report.tier_violations:
                sev_badge = f"🚨 `{v.severity.value}`" if v.severity == InvariantSeverity.CRITICAL_BLOCK else f"⚠️ `{v.severity.value}`"
                lines.append(
                    f"| {sev_badge} | `{v.rule_name}` | `{v.source_module}` | `{v.target_module}` | "
                    f"`{v.source_tier.name}` | `{v.target_tier.name}` | {v.explanation} |"
                )

        lines.extend([
            "",
            "---",
            "",
            "## 3. Subsystem Martin Metrics (Instability & Abstractness)",
            "",
            "| Subsystem | Tier | Ca (In) | Ce (Out) | Instability (I) | Abstractness (A) | Distance (D) | Status |",
            "|:---|:---|:---:|:---:|:---:|:---:|:---:|:---:|"
        ])

        for name, m in sorted(report.metrics_by_subsystem.items(), key=lambda x: x[1].distance_from_main_sequence_d, reverse=True):
            status = "🟢 Balanced"
            if m.distance_from_main_sequence_d > 0.60:
                if m.instability_i < 0.20:
                    status = "🔴 Zone of Pain"
                elif m.instability_i > 0.80:
                    status = "🟡 Zone of Uselessness"
                else:
                    status = "🟠 High Drift"
            elif m.distance_from_main_sequence_d > 0.35:
                status = "🔵 Moderate Drift"

            lines.append(
                f"| `{name}` | `{m.tier.name}` | `{m.afferent_coupling_ca}` | `{m.efferent_coupling_ce}` | "
                f"`{round(m.instability_i, 2)}` | `{round(m.abstractness_a, 2)}` | `{round(m.distance_from_main_sequence_d, 2)}` | {status} |"
            )

        lines.extend([
            "",
            "---",
            "",
            "## 4. Circular Dependencies & Entanglement",
            ""
        ])

        if not report.circular_dependency_chains:
            lines.append("✅ **Acyclic Directed Graph:** Zero circular dependency chains detected.")
        else:
            lines.append("🚨 **Circular Dependency Chains Detected:**")
            for idx, cycle in enumerate(report.circular_dependency_chains, start=1):
                lines.append(f"{idx}. `{' -> '.join(cycle)} -> {cycle[0]}`")

        lines.extend([
            "",
            "---",
            "",
            "## 5. Automated Refactoring & Decoupling Proposals",
            ""
        ])

        if not report.refactoring_proposals:
            lines.append("✅ Architecture is within nominal operational boundaries. No decoupling refactorings required.")
        else:
            lines.append("| Priority | Target Subsystem | Pattern | Rationale | Prescribed Decoupling Action |")
            lines.append("|:---|:---|:---|:---|:---|")
            for prop in report.refactoring_proposals:
                lines.append(
                    f"| `{prop['severity']}` | `{prop['subsystem']}` | **{prop['pattern']}** | {prop['rationale']} | {prop['action']} |"
                )

        content = "\n".join(lines) + "\n"

        # Attempt to save to disk if directory exists or can be created
        try:
            target_p = Path(output_path)
            target_p.parent.mkdir(parents=True, exist_ok=True)
            target_p.write_text(content, encoding="utf-8")
            logger.info("Exported Architectural Health Digest to: %s", output_path)
        except Exception as e:
            logger.warning("Could not write markdown digest to disk (%s): %s", output_path, e)

        return content


# =====================================================================
# 5. COCKPIT API REST & QUERY HELPERS
# =====================================================================

_GLOBAL_SENTINEL: Optional[ArchitecturalSentinelEngine] = None


def get_sentinel_instance() -> ArchitecturalSentinelEngine:
    """Singleton getter for the ArchitecturalSentinelEngine."""
    global _GLOBAL_SENTINEL
    if _GLOBAL_SENTINEL is None:
        _GLOBAL_SENTINEL = ArchitecturalSentinelEngine()
    return _GLOBAL_SENTINEL


def query_cockpit_architecture_health() -> Dict[str, Any]:
    """Cockpit REST endpoint query helper: returns full architectural audit JSON."""
    sentinel = get_sentinel_instance()
    report = sentinel.audit_architecture()
    return report.to_dict()


def query_cockpit_subsystem_metrics(subsystem_name: str) -> Dict[str, Any]:
    """Cockpit REST endpoint query helper: returns metrics for a single subsystem."""
    sentinel = get_sentinel_instance()
    report = sentinel.audit_architecture()
    metric = report.metrics_by_subsystem.get(subsystem_name)
    if not metric:
        return {"error": f"Subsystem '{subsystem_name}' not found", "status": 404}
    return metric.to_dict()


def query_cockpit_evaluate_gate(milestone_metadata: Dict[str, Any]) -> Dict[str, Any]:
    """Cockpit REST endpoint query helper: evaluates milestone against architectural gates."""
    sentinel = get_sentinel_instance()
    decision = sentinel.evaluate_milestone(milestone_metadata)
    return decision.to_dict()


# =====================================================================
# 6. COMPREHENSIVE SELF-TEST SUITE
# =====================================================================

def run_self_tests() -> None:
    """Executes a rigorous self-test suite validating all architectural algorithms."""
    logger.info("Executing Architectural Drift Sentinel self-test suite...")

    # Test 1: CodebaseASTAnalyzer on synthetic code string
    logger.info("[Test 1] Validating AST analysis on synthetic classes & imports...")
    sample_code = """
import os
import sys
import abc
from abc import ABC, abstractmethod
from tasks.pipeline_states import PipelineState

class AbstractRegistry(ABC):
    @abstractmethod
    def register(self, key: str) -> bool:
        pass

class ConcreteRegistry(AbstractRegistry):
    def register(self, key: str) -> bool:
        return True

def standalone_helper():
    return os.name
"""
    tmp_path = Path("/tmp/sentinel_test_sample.py")
    try:
        tmp_path.write_text(sample_code, encoding="utf-8")
        parsed = CodebaseASTAnalyzer.parse_file(tmp_path)
        assert parsed["classes"] == 2, f"Expected 2 classes, got {parsed['classes']}"
        assert parsed["abstract_classes"] == 1, f"Expected 1 abstract class, got {parsed['abstract_classes']}"
        assert parsed["functions"] == 3, f"Expected 3 functions (2 methods + 1 helper), got {parsed['functions']}"
        assert "tasks.pipeline_states" in parsed["imports"] or "pipeline_states" in parsed["imports"]
        logger.info("✓ Test 1 passed: AST extraction accurate.")
    finally:
        if tmp_path.exists():
            tmp_path.unlink()

    # Test 2: Tarjan Cycle Detection Algorithm
    logger.info("[Test 2] Validating Tarjan's SCC Cycle Detection...")
    graph_with_cycles: Dict[str, Set[str]] = {
        "A": {"B"},
        "B": {"C"},
        "C": {"A", "D"},  # Cycle A-B-C
        "D": {"E"},
        "E": set(),
        "F": {"F"}        # Self-cycle
    }
    detected_cycles = TarjanCycleDetector.detect_cycles(graph_with_cycles)
    assert len(detected_cycles) == 2, f"Expected 2 cycles, found {len(detected_cycles)}: {detected_cycles}"
    cycle_nodes = [set(c) for c in detected_cycles]
    assert {"A", "B", "C"} in cycle_nodes, "Failed to detect A-B-C cycle"
    assert {"F"} in cycle_nodes, "Failed to detect self-cycle F"
    logger.info("✓ Test 2 passed: Tarjan cycle detection accurately isolated SCCs.")

    # Test 3: Martin Metrics (Ca, Ce, I, A, D)
    logger.info("[Test 3] Validating Robert C. Martin coupling metrics...")
    engine = ArchitecturalSentinelEngine()
    test_dep_graph: Dict[str, Set[str]] = {
        "client": {"service_interface"},
        "service_interface": set(),
        "service_impl": {"service_interface"}
    }
    test_ast_data: Dict[str, Dict[str, Any]] = {
        "client": {"classes": 1, "abstract_classes": 0, "loc_count": 50},
        "service_interface": {"classes": 1, "abstract_classes": 1, "loc_count": 20},
        "service_impl": {"classes": 1, "abstract_classes": 0, "loc_count": 100}
    }
    metrics = engine.compute_martin_metrics(test_dep_graph, test_ast_data)

    # service_interface: Ca=2 (client, service_impl), Ce=0 -> I = 0.0. A = 1/1 = 1.0. D = |1.0 + 0.0 - 1| = 0.0
    si = metrics["service_interface"]
    assert si.afferent_coupling_ca == 2, f"Expected Ca=2, got {si.afferent_coupling_ca}"
    assert si.efferent_coupling_ce == 0, f"Expected Ce=0, got {si.efferent_coupling_ce}"
    assert si.instability_i == 0.0, f"Expected I=0.0, got {si.instability_i}"
    assert si.abstractness_a == 1.0, f"Expected A=1.0, got {si.abstractness_a}"
    assert si.distance_from_main_sequence_d == 0.0, f"Expected D=0.0, got {si.distance_from_main_sequence_d}"

    # client: Ca=0, Ce=1 -> I = 1.0. A = 0.0. D = |0.0 + 1.0 - 1| = 0.0
    cl = metrics["client"]
    assert cl.instability_i == 1.0
    assert cl.distance_from_main_sequence_d == 0.0

    logger.info("✓ Test 3 passed: Martin metrics verified against formal specifications.")

    # Test 4: Newman-Girvan Modularity (Q)
    logger.info("[Test 4] Validating Newman-Girvan Modularity calculation...")
    tier_map = {
        "mod_t0_a": ArchitecturalTier.FOUNDATION,
        "mod_t0_b": ArchitecturalTier.FOUNDATION,
        "mod_t1_a": ArchitecturalTier.CORE_ENGINE,
        "mod_t1_b": ArchitecturalTier.CORE_ENGINE
    }
    clustered_graph: Dict[str, Set[str]] = {
        "mod_t0_a": {"mod_t0_b"},
        "mod_t0_b": {"mod_t0_a"},
        "mod_t1_a": {"mod_t1_b", "mod_t0_a"},
        "mod_t1_b": {"mod_t1_a"}
    }
    modularity_q = engine.compute_newman_girvan_modularity(clustered_graph, tier_map)
    assert -1.0 <= modularity_q <= 1.0, f"Modularity Q out of theoretical bounds: {modularity_q}"
    assert modularity_q > 0.0, f"Expected positive modularity for clustered graph, got {modularity_q}"
    logger.info("✓ Test 4 passed: Modularity Q calculation verified (Q=%s).", round(modularity_q, 4))

    # Test 5: Layering Invariant Violations
    logger.info("[Test 5] Validating Layering Invariants...")
    violating_graph: Dict[str, Set[str]] = {
        # Illegal: Foundation (Tier 0) importing Consensus Engine (Tier 3)
        "pipeline_states": {"pipeline_consensus_engine"},
        # Legal: Cockpit (Tier 4) importing Consensus Engine (Tier 3)
        "dashboard_cockpit": {"pipeline_consensus_engine"}
    }
    violations = engine.check_tier_invariants(violating_graph)
    assert len(violations) >= 1, "Expected at least 1 tier violation"
    v = [x for x in violations if x.source_module == "pipeline_states"][0]
    assert v.severity == InvariantSeverity.CRITICAL_BLOCK
    assert "STRICT_DOWNWARD_LAYERING" in v.rule_name
    logger.info("✓ Test 5 passed: Upward layer breach correctly classified as CRITICAL_BLOCK.")

    # Test 6: Pre-Dispatch Milestone Gate Evaluation
    logger.info("[Test 6] Validating Pre-Dispatch Gate Decisions...")

    # Case A: ALLOW clean milestone
    decision_allow = engine.evaluate_milestone({
        "milestone_id": "M-CLEAN",
        "target_filepath": "tasks/pipeline_new_feature.py",
        "technical_specification": "Implement new autonomic task with clean dependencies.",
        "proposed_imports": ["pipeline_config_registry", "pipeline_states"],
        "predicted_loc": 250
    })
    assert decision_allow.verdict in ["ALLOW", "WARN"], f"Expected ALLOW or WARN, got {decision_allow.verdict}"
    assert not decision_allow.violates_tier_boundary

    # Case B: BLOCK illegal layer inversion
    decision_block = engine.evaluate_milestone({
        "milestone_id": "M-ILLEGAL",
        "target_filepath": "tasks/pipeline_config_registry.py",  # Foundation tier 0
        "technical_specification": "Illegal upward coupling from foundation to cockpit.",
        "proposed_imports": ["server", "interactive_api"],     # Cockpit tier 4
        "predicted_loc": 150
    })
    assert decision_block.verdict == "BLOCK", f"Expected BLOCK, got {decision_block.verdict}"
    assert decision_block.violates_tier_boundary
    assert len(decision_block.blocking_violations) > 0

    # Case C: DECOMPOSE oversized milestone
    decision_decompose = engine.evaluate_milestone({
        "milestone_id": "M-OVERSIZED",
        "target_filepath": "tasks/pipeline_giant_system.py",
        "technical_specification": "Massive monolithic subsystem exceeding budget.",
        "proposed_imports": [],
        "predicted_loc": 1200
    })
    assert decision_decompose.verdict == "DECOMPOSE", f"Expected DECOMPOSE, got {decision_decompose.verdict}"

    logger.info("✓ Test 6 passed: Gate correctly issued ALLOW, BLOCK, and DECOMPOSE verdicts.")

    # Test 7: Refactoring Proposals for Zone of Pain & Uselessness
    logger.info("[Test 7] Validating Automated Refactoring Proposals...")
    pain_report = ArchitecturalHealthReport(
        timestamp=time.time(),
        modularity_q=0.5,
        mean_main_sequence_distance=0.7,
        entanglement_density=0.1,
        tier_violations=[],
        circular_dependency_chains=[["alpha", "beta"]],
        metrics_by_subsystem={
            "rigid_core": SubsystemCouplingMetrics(
                subsystem="rigid_core",
                tier=ArchitecturalTier.CORE_ENGINE,
                afferent_coupling_ca=10,
                efferent_coupling_ce=1,
                instability_i=0.09,
                abstractness_a=0.0,
                distance_from_main_sequence_d=0.91,  # Deep in Zone of Pain
                direct_dependencies=["base"],
                direct_dependents=["c1", "c2"],
                loc_count=500,
                class_count=5,
                abstract_class_count=0
            )
        },
        high_coupling_pairs=[],
        refactoring_proposals=[]
    )
    proposals = engine.generate_refactoring_recommendations(pain_report)
    assert any(p["zone"] == "ZONE_OF_PAIN" for p in proposals), "Failed to detect Zone of Pain"
    assert any(p["zone"] == "RECURSIVE_CYCLE" for p in proposals), "Failed to suggest cycle decoupling"
    logger.info("✓ Test 7 passed: Prescribed accurate design patterns (DIP & Mediator).")

    # Test 8: End-to-end Audit & Markdown Export
    logger.info("[Test 8] Validating End-to-End Audit & Markdown Digest Generation...")
    audit_report = engine.audit_architecture(["tasks"])
    markdown_output = engine.export_markdown_digest(audit_report, output_path="/tmp/ARCHITECTURAL_HEALTH_TEST.md")
    assert "# Autonomic Architectural Drift" in markdown_output
    assert "Newman-Girvan Modularity" in markdown_output
    if Path("/tmp/ARCHITECTURAL_HEALTH_TEST.md").exists():
        Path("/tmp/ARCHITECTURAL_HEALTH_TEST.md").unlink()
    logger.info("✓ Test 8 passed: Markdown digest rendered and verified successfully.")

    logger.info("=" * 80)
    logger.info("ALL ARCHITECTURAL SENTINEL SELF-TESTS PASSED WITH 100% SUCCESS.")
    logger.info("=" * 80)


if __name__ == "__main__":
    run_self_tests()
    sys.exit(0)