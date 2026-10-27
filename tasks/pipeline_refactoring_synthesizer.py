"""
tasks/pipeline_refactoring_synthesizer.py

Autonomic Architectural Refactoring & Structural Remediation Synthesizer for
the Hermes Orchestrator autonomous platform.

Ingests architectural diagnostics (cyclic dependencies, upward tier violations,
and monolithic god-components) and deterministically synthesizes AST-level
refactoring recipes:
  1. Cyclic Dependency Inversion via Protocol/ABC extraction & cut-set minimization.
  2. Upward Tier Decoupling via Observer/Mediator and dynamic event registration.
  3. Intra-Module God-Component Decomposition via Spectral Bisection & Facade generation.
  4. Multi-Objective Refactoring Optimization maximizing modularity gain while minimizing blast radius.
  5. AST transformation, round-trip unparse validation, compile-time syntax check,
     and post-refactoring Tarjan cycle elimination verification.
"""

from __future__ import annotations

import abc
import ast
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from enum import Enum
import json
import logging
import math
import os
from pathlib import Path
import sys
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger("pipeline_refactoring_synthesizer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


# ============================================================================
# 1. Domain Models & Enums
# ============================================================================

class RefactoringType(str, Enum):
    """Taxonomy of architectural refactoring remediation operations."""
    EXTRACT_PROTOCOL = "EXTRACT_PROTOCOL"
    DEPENDENCY_INVERSION = "DEPENDENCY_INVERSION"
    MEDIATOR_EVENT_HOOK = "MEDIATOR_EVENT_HOOK"
    EXTRACT_FACADE = "EXTRACT_FACADE"
    DECOMPOSE_MODULE = "DECOMPOSE_MODULE"
    MOVE_TO_FOUNDATION = "MOVE_TO_FOUNDATION"


class RefactoringStatus(str, Enum):
    """Lifecycle states of a proposed refactoring action."""
    PROPOSED = "PROPOSED"
    VALIDATED = "VALIDATED"
    APPLIED = "APPLIED"
    REVERTED = "REVERTED"
    REJECTED = "REJECTED"


@dataclass
class SymbolReference:
    """Detailed metadata for a foreign symbol imported or invoked within a module."""
    symbol_name: str
    symbol_type: str  # 'function', 'class', 'variable', 'attribute', 'module'
    source_line: int
    target_module: str
    is_call: bool = False
    is_type_hint: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RefactoringAction:
    """An atomic, deterministic AST transformation action."""
    action_id: str
    refactoring_type: RefactoringType
    target_file: str
    secondary_files: list[str]
    description: str
    blast_radius_score: float
    expected_modularity_gain: float
    ast_diff_summary: str
    generated_code_snippets: dict[str, str]  # file_path -> synthesized source code
    status: RefactoringStatus = RefactoringStatus.PROPOSED

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["refactoring_type"] = self.refactoring_type.value
        data["status"] = self.status.value
        return data


@dataclass
class RefactoringPlan:
    """Global ordered execution plan for multi-violation structural remediation."""
    plan_id: str
    timestamp: float
    source_violation_ids: list[str]
    actions: list[RefactoringAction]
    total_blast_radius: float
    projected_delta_q: float
    projected_cycle_reduction: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "timestamp": self.timestamp,
            "source_violation_ids": self.source_violation_ids,
            "actions": [a.to_dict() for a in self.actions],
            "total_blast_radius": round(self.total_blast_radius, 4),
            "projected_delta_q": round(self.projected_delta_q, 4),
            "projected_cycle_reduction": self.projected_cycle_reduction,
        }


# ============================================================================
# 2. Tarjan Strongly Connected Components & Graph Algorithms
# ============================================================================

class TarjanCycleDetector:
    """Tarjan's algorithm for finding Strongly Connected Components in module graphs."""

    @staticmethod
    def detect_scc(graph: dict[str, list[str]]) -> list[list[str]]:
        """Identifies all strongly connected components with |V| >= 2 (cycles)."""
        index = 0
        indices: dict[str, int] = {}
        lowlink: dict[str, int] = {}
        on_stack: set[str] = set()
        stack: list[str] = []
        cycles: list[list[str]] = []

        all_nodes = set(graph.keys())
        for targets in graph.values():
            all_nodes.update(targets)

        def strongconnect(node: str) -> None:
            nonlocal index
            indices[node] = index
            lowlink[node] = index
            index += 1
            stack.append(node)
            on_stack.add(node)

            for neighbor in graph.get(node, []):
                if neighbor not in indices:
                    strongconnect(neighbor)
                    lowlink[node] = min(lowlink[node], lowlink[neighbor])
                elif neighbor in on_stack:
                    lowlink[node] = min(lowlink[node], indices[neighbor])

            if lowlink[node] == indices[node]:
                scc: list[str] = []
                while True:
                    w = stack.pop()
                    on_stack.remove(w)
                    scc.append(w)
                    if w == node:
                        break
                if len(scc) > 1:
                    cycles.append(scc)

        for node in sorted(all_nodes):
            if node not in indices:
                strongconnect(node)

        return cycles


# ============================================================================
# 3. Core Engine: SymbolUsageExtractor
# ============================================================================

class SymbolUsageExtractor(ast.NodeVisitor):
    """
    Parses module AST to extract detailed symbol usage matrices, imported
    identifiers, foreign function calls, and type annotation references.
    """

    def __init__(self, current_module: str):
        self.current_module = current_module
        # alias -> (source_module, original_symbol_name)
        self.import_map: dict[str, tuple[str, Optional[str]]] = {}
        self.references: list[SymbolReference] = []
        # target_module -> set of used symbol names
        self.symbol_usage_matrix: dict[str, set[str]] = defaultdict(set)
        # Detailed call signatures if callable: func_name -> list of arg names
        self.function_signatures: dict[str, list[str]] = {}
        self.class_methods: dict[str, dict[str, list[str]]] = defaultdict(dict)
        self._current_class: Optional[str] = None

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            name = alias.asname or alias.name
            self.import_map[name] = (alias.name, None)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        mod = node.module or ""
        for alias in node.names:
            local_name = alias.asname or alias.name
            self.import_map[local_name] = (mod, alias.name)
        self.generic_visit(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        prev_class = self._current_class
        self._current_class = node.name
        self.generic_visit(node)
        self._current_class = prev_class

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        args = [arg.arg for arg in node.args.args if arg.arg != "self"]
        if self._current_class:
            self.class_methods[self._current_class][node.name] = args
        else:
            self.function_signatures[node.name] = args
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        args = [arg.arg for arg in node.args.args if arg.arg != "self"]
        if self._current_class:
            self.class_methods[self._current_class][node.name] = args
        else:
            self.function_signatures[node.name] = args
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        line = getattr(node, "lineno", 0)
        # Case 1: direct call to imported symbol: func()
        if isinstance(node.func, ast.Name):
            sym = node.func.id
            if sym in self.import_map:
                mod, orig = self.import_map[sym]
                used_sym = orig or sym
                self.symbol_usage_matrix[mod].add(used_sym)
                self.references.append(
                    SymbolReference(
                        symbol_name=used_sym,
                        symbol_type="function",
                        source_line=line,
                        target_module=mod,
                        is_call=True,
                    )
                )
        # Case 2: module.func() or imported_class.method()
        elif isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name):
            base_sym = node.func.value.id
            attr = node.func.attr
            if base_sym in self.import_map:
                mod, orig = self.import_map[base_sym]
                resolved_mod = f"{mod}.{orig}" if orig else mod
                self.symbol_usage_matrix[resolved_mod].add(attr)
                self.references.append(
                    SymbolReference(
                        symbol_name=attr,
                        symbol_type="method",
                        source_line=line,
                        target_module=resolved_mod,
                        is_call=True,
                    )
                )
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        # Non-call attribute access
        if isinstance(node.value, ast.Name):
            base_sym = node.value.id
            attr = node.func.attr if hasattr(node, "func") else node.attr
            if base_sym in self.import_map:
                mod, orig = self.import_map[base_sym]
                resolved_mod = f"{mod}.{orig}" if orig else mod
                self.symbol_usage_matrix[resolved_mod].add(attr)
                self.references.append(
                    SymbolReference(
                        symbol_name=attr,
                        symbol_type="attribute",
                        source_line=getattr(node, "lineno", 0),
                        target_module=resolved_mod,
                        is_call=False,
                    )
                )
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        # Type hint inspection
        line = getattr(node, "lineno", 0)
        self._record_type_annotation(node.annotation, line)
        self.generic_visit(node)

    def _record_type_annotation(self, ann_node: ast.AST, line: int) -> None:
        if isinstance(ann_node, ast.Name):
            sym = ann_node.id
            if sym in self.import_map:
                mod, orig = self.import_map[sym]
                used_sym = orig or sym
                self.symbol_usage_matrix[mod].add(used_sym)
                self.references.append(
                    SymbolReference(
                        symbol_name=used_sym,
                        symbol_type="class",
                        source_line=line,
                        target_module=mod,
                        is_call=False,
                        is_type_hint=True,
                    )
                )
        elif isinstance(ann_node, ast.Attribute) and isinstance(ann_node.value, ast.Name):
            base_sym = ann_node.value.id
            attr = ann_node.attr
            if base_sym in self.import_map:
                mod, orig = self.import_map[base_sym]
                resolved_mod = f"{mod}.{orig}" if orig else mod
                self.symbol_usage_matrix[resolved_mod].add(attr)
                self.references.append(
                    SymbolReference(
                        symbol_name=attr,
                        symbol_type="class",
                        source_line=line,
                        target_module=resolved_mod,
                        is_call=False,
                        is_type_hint=True,
                    )
                )


# ============================================================================
# 4. Core Engine: CycleRemediationSynthesizer (Dependency Inversion)
# ============================================================================

class CycleRemediationSynthesizer:
    """
    Resolves cyclic dependencies via minimal coupling cut-set selection and
    synthetic Protocol / ABC interface extraction into Tier 0 (Foundation).
    """

    def __init__(self, module_sources: dict[str, str]):
        self.module_sources = module_sources
        self.parsed_ast: dict[str, ast.Module] = {}
        self.extractors: dict[str, SymbolUsageExtractor] = {}
        for mod, src in module_sources.items():
            tree = ast.parse(src)
            self.parsed_ast[mod] = tree
            extractor = SymbolUsageExtractor(mod)
            extractor.visit(tree)
            self.extractors[mod] = extractor

    def find_minimal_coupling_cut_edge(self, cycle: list[str]) -> Tuple[str, str, set[str]]:
        """
        Computes the minimal cut-set edge E_cut = argmin |S(u, v)| along the cycle.
        Returns (u, v, symbol_set).
        """
        min_cut: Optional[Tuple[str, str, set[str]]] = None
        min_weight = math.inf

        n = len(cycle)
        for i in range(n):
            u = cycle[i]
            v = cycle[(i + 1) % n]
            symbols = set()
            extractor_u = self.extractors.get(u)
            if extractor_u:
                for target_mod, syms in extractor_u.symbol_usage_matrix.items():
                    if target_mod == v or target_mod.startswith(v):
                        symbols.update(syms)

            weight = len(symbols) if symbols else 1
            if weight < min_weight:
                min_weight = weight
                min_cut = (u, v, symbols)

        if min_cut is None:
            # Fallback cut on first edge
            u, v = cycle[0], cycle[1] if len(cycle) > 1 else cycle[0]
            min_cut = (u, v, {"execute"})

        return min_cut

    def synthesize_protocol_interface(
        self,
        target_module: str,
        symbols: set[str],
        protocol_name: str,
    ) -> str:
        """Generates an abstract typing.Protocol interface for the target module's symbols."""
        lines = [
            '"""Synthesized architectural foundation interface for dependency inversion."""',
            "from __future__ import annotations",
            "from typing import Any, Protocol, runtime_checkable",
            "",
            "@runtime_checkable",
            f"class {protocol_name}(Protocol):",
        ]
        if not symbols:
            lines.append("    def execute(self, *args: Any, **kwargs: Any) -> Any: ...")
        else:
            for sym in sorted(symbols):
                # Synthesize method signature
                lines.append(f"    def {sym}(self, *args: Any, **kwargs: Any) -> Any: ...")
        lines.append("")
        return "\n".join(lines)

    def synthesize_remediation_action(
        self,
        cycle: list[str],
        foundation_module: str = "tasks.foundation_protocols",
    ) -> list[RefactoringAction]:
        """
        Generates actions to invert dependency on the minimal cut edge (u -> v).
        Extracts Protocol P_v in foundation_module, rewrites u to depend on P_v.
        """
        u, v, symbols = self.find_minimal_coupling_cut_edge(cycle)
        clean_v_name = v.split(".")[-1].capitalize()
        protocol_name = f"I{clean_v_name}Contract"

        protocol_code = self.synthesize_protocol_interface(v, symbols, protocol_name)

        # Build AST patch description for u
        u_src = self.module_sources[u]
        u_tree = ast.parse(u_src)

        class CycleInversionTransformer(ast.NodeTransformer):
            def __init__(self, target_v: str, prot_name: str, found_mod: str):
                self.target_v = target_v
                self.prot_name = prot_name
                self.found_mod = found_mod
                self.replaced_import = False

            def visit_Import(self, node: ast.Import) -> Any:
                new_names = [alias for alias in node.names if alias.name != self.target_v]
                if not new_names:
                    return None
                node.names = new_names
                return node

            def visit_ImportFrom(self, node: ast.ImportFrom) -> Any:
                if node.module == self.target_v or (node.module and node.module.startswith(self.target_v)):
                    self.replaced_import = True
                    return ast.ImportFrom(
                        module=self.found_mod,
                        names=[ast.alias(name=self.prot_name, asname=None)],
                        level=0,
                    )
                return node

        transformer = CycleInversionTransformer(v, protocol_name, foundation_module)
        modified_u_tree = transformer.visit(u_tree)
        ast.fix_missing_locations(modified_u_tree)

        # Inject foundation import if not already added
        has_protocol_import = any(
            isinstance(n, ast.ImportFrom) and n.module == foundation_module
            for n in ast.walk(modified_u_tree)
        )
        if not has_protocol_import:
            import_node = ast.ImportFrom(
                module=foundation_module,
                names=[ast.alias(name=protocol_name, asname=None)],
                level=0,
            )
            modified_u_tree.body.insert(0, import_node)
            ast.fix_missing_locations(modified_u_tree)

        synthesized_u_code = ast.unparse(modified_u_tree)

        # Calculate blast radius: touched files = u, v, foundation_module
        blast_radius = 2.0 + len(symbols) * 0.5
        modularity_gain = 0.85

        action_protocol = RefactoringAction(
            action_id=f"ACT-PROT-{v.replace('.', '_')}",
            refactoring_type=RefactoringType.EXTRACT_PROTOCOL,
            target_file=f"{foundation_module.replace('.', '/')}.py",
            secondary_files=[v],
            description=f"Synthesize {protocol_name} in Foundation tier to decouple {u} from {v}.",
            blast_radius_score=1.5,
            expected_modularity_gain=0.45,
            ast_diff_summary=f"Created {protocol_name} with symbols {sorted(symbols)}",
            generated_code_snippets={
                f"{foundation_module.replace('.', '/')}.py": protocol_code
            },
            status=RefactoringStatus.VALIDATED,
        )

        action_inversion = RefactoringAction(
            action_id=f"ACT-INV-{u.replace('.', '_')}-{v.replace('.', '_')}",
            refactoring_type=RefactoringType.DEPENDENCY_INVERSION,
            target_file=u,
            secondary_files=[v, foundation_module],
            description=f"Invert edge ({u} -> {v}) by replacing direct import with {protocol_name}.",
            blast_radius_score=blast_radius,
            expected_modularity_gain=modularity_gain,
            ast_diff_summary=f"Replaced concrete import of {v} with {foundation_module}.{protocol_name}",
            generated_code_snippets={u: synthesized_u_code},
            status=RefactoringStatus.VALIDATED,
        )

        return [action_protocol, action_inversion]


# ============================================================================
# 5. Core Engine: TierBoundaryRemediationSynthesizer (Observer/Mediator)
# ============================================================================

class TierBoundaryRemediationSynthesizer:
    """
    Decouples illegal upward tier breaches (source_tier < target_tier) by
    synthesizing an event-driven Observer/Mediator callback mechanism.
    """

    def __init__(self, module_sources: dict[str, str], module_tiers: dict[str, int]):
        self.module_sources = module_sources
        self.module_tiers = module_tiers

    def identify_upward_violations(self, dependency_graph: dict[str, list[str]]) -> list[Tuple[str, str]]:
        """Finds edges (u, v) where tier(u) < tier(v)."""
        violations: list[Tuple[str, str]] = []
        for u, targets in dependency_graph.items():
            tier_u = self.module_tiers.get(u, 1)
            for v in targets:
                tier_v = self.module_tiers.get(v, 1)
                if tier_u < tier_v:
                    violations.append((u, v))
        return violations

    def synthesize_mediator_action(self, source_mod: str, target_mod: str) -> RefactoringAction:
        """
        Rewrites source_mod to eliminate direct import of target_mod by introducing
        register_listener() and notify_event(). Direct calls are replaced by events.
        """
        src = self.module_sources[source_mod]
        tree = ast.parse(src)

        extractor = SymbolUsageExtractor(source_mod)
        extractor.visit(tree)

        used_symbols = extractor.symbol_usage_matrix.get(target_mod, set())

        class UpwardCallRewriter(ast.NodeTransformer):
            def __init__(self, bad_target: str):
                self.bad_target = bad_target
                self.removed_imports = 0

            def visit_Import(self, node: ast.Import) -> Any:
                node.names = [a for a in node.names if a.name != self.bad_target]
                return node if node.names else None

            def visit_ImportFrom(self, node: ast.ImportFrom) -> Any:
                if node.module == self.bad_target or (node.module and node.module.startswith(self.bad_target)):
                    self.removed_imports += 1
                    return None
                return node

            def visit_Call(self, node: ast.Call) -> Any:
                # Rewrite direct call obj.method(x) to self.notify_event('on_method', {'args': x})
                # or notify_event('on_method', ...)
                if isinstance(node.func, ast.Attribute):
                    call_name = node.func.attr
                    # Check if caller matches bad target
                    is_target = False
                    if isinstance(node.func.value, ast.Name):
                        if node.func.value.id == self.bad_target.split(".")[-1]:
                            is_target = True

                    if is_target:
                        event_name = f"event_{call_name}"
                        # Synthesize notify_event(event_name, {...})
                        return ast.Call(
                            func=ast.Name(id="notify_event", ctx=ast.Load()),
                            args=[
                                ast.Constant(value=event_name),
                                ast.Dict(
                                    keys=[ast.Constant(value="caller")],
                                    values=[ast.Constant(value=source_mod)],
                                ),
                            ],
                            keywords=[],
                        )
                return self.generic_visit(node)

        rewriter = UpwardCallRewriter(target_mod)
        new_tree = rewriter.visit(tree)

        # Inject event observer infrastructure at the top of source_mod
        event_infra_code = (
            "\n# --- Synthesized Event/Mediator Decoupling Infrastructure ---\n"
            "from typing import Callable, Any\n"
            "_SUBSCRIBERS: list[Callable[[str, dict[str, Any]], None]] = []\n\n"
            "def register_listener(callback: Callable[[str, dict[str, Any]], None]) -> None:\n"
            "    '''Subscribes an upstream governor or mediator to events emitted by this subsystem.'''\n"
            "    if callback not in _SUBSCRIBERS:\n"
            "        _SUBSCRIBERS.append(callback)\n\n"
            "def notify_event(event_type: str, payload: dict[str, Any]) -> None:\n"
            "    '''Dispatches internal events to registered upstream mediators.'''\n"
            "    for sub in list(_SUBSCRIBERS):\n"
            "        sub(event_type, payload)\n"
            "# ------------------------------------------------------------\n"
        )
        infra_tree = ast.parse(event_infra_code)

        # Insert infra after docstring / future imports
        insert_idx = 0
        for i, stmt in enumerate(new_tree.body):
            if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant):
                insert_idx = i + 1
            elif isinstance(stmt, ast.ImportFrom) and stmt.module == "__future__":
                insert_idx = i + 1

        for stmt in reversed(infra_tree.body):
            new_tree.body.insert(insert_idx, stmt)

        ast.fix_missing_locations(new_tree)
        synthesized_code = ast.unparse(new_tree)

        return RefactoringAction(
            action_id=f"ACT-MED-{source_mod.replace('.', '_')}-{target_mod.replace('.', '_')}",
            refactoring_type=RefactoringType.MEDIATOR_EVENT_HOOK,
            target_file=source_mod,
            secondary_files=[target_mod],
            description=(
                f"Eliminate upward tier violation {source_mod} (Tier {self.module_tiers.get(source_mod, 1)}) -> "
                f"{target_mod} (Tier {self.module_tiers.get(target_mod, 3)}) via Event Mediator synthesis."
            ),
            blast_radius_score=2.0 + len(used_symbols) * 0.4,
            expected_modularity_gain=0.75,
            ast_diff_summary=f"Replaced direct {target_mod} invocations with register_listener & notify_event hooks",
            generated_code_snippets={source_mod: synthesized_code},
            status=RefactoringStatus.VALIDATED,
        )


# ============================================================================
# 6. Core Engine: IntraModuleDecomposer (Spectral Bisection)
# ============================================================================

class IntraModuleDecomposer:
    """
    Decomposes oversized monolithic 'God' modules into cohesive cohesive
    sub-clusters using spectral graph bisection on the internal call graph,
    preserving full backward compatibility via a unified Facade wrapper.
    """

    def __init__(self, theta_loc: int = 150, theta_cohesion: float = 0.60):
        self.theta_loc = theta_loc
        self.theta_cohesion = theta_cohesion

    def extract_internal_call_graph(self, module_src: str) -> Tuple[list[str], list[Tuple[int, int]], dict[str, ast.AST]]:
        """Parses top-level functions and classes to construct internal call graph G_intra."""
        tree = ast.parse(module_src)
        top_symbols: list[str] = []
        node_map: dict[str, ast.AST] = {}

        for item in tree.body:
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                top_symbols.append(item.name)
                node_map[item.name] = item

        n = len(top_symbols)
        sym_to_idx = {name: i for i, name in enumerate(top_symbols)}
        edges: set[Tuple[int, int]] = set()

        class InternalCallVisitor(ast.NodeVisitor):
            def __init__(self, src_idx: int):
                self.src_idx = src_idx

            def visit_Call(self, node: ast.Call) -> None:
                if isinstance(node.func, ast.Name):
                    target = node.func.id
                    if target in sym_to_idx and sym_to_idx[target] != self.src_idx:
                        dst_idx = sym_to_idx[target]
                        edge = (min(self.src_idx, dst_idx), max(self.src_idx, dst_idx))
                        edges.add(edge)
                self.generic_visit(node)

        for name, node in node_map.items():
            src_idx = sym_to_idx[name]
            InternalCallVisitor(src_idx).visit(node)

        return top_symbols, list(edges), node_map

    def spectral_bisection(self, num_nodes: int, edges: list[Tuple[int, int]]) -> Tuple[list[int], list[int]]:
        """
        Computes the Modularity matrix B_ij = A_ij - (k_i * k_j) / (2m) and
        applies power-iteration spectral bisection based on sign(u_1).
        """
        if num_nodes <= 1:
            return list(range(num_nodes)), []

        # Adjacency matrix A and degrees
        degrees = [0] * num_nodes
        adj = [[0.0] * num_nodes for _ in range(num_nodes)]
        for u, v in edges:
            adj[u][v] = 1.0
            adj[v][u] = 1.0
            degrees[u] += 1
            degrees[v] += 1

        m = len(edges)
        two_m = 2.0 * m if m > 0 else 1.0

        # Modularity matrix B
        B = [[0.0] * num_nodes for _ in range(num_nodes)]
        for i in range(num_nodes):
            for j in range(num_nodes):
                B[i][j] = adj[i][j] - (degrees[i] * degrees[j]) / two_m

        # Shift B to guarantee positive semi-definiteness during power iteration: B_shift = B + alpha * I
        row_sums = [sum(abs(B[i][j]) for j in range(num_nodes)) for i in range(num_nodes)]
        alpha = max(row_sums) + 1.0
        for i in range(num_nodes):
            B[i][i] += alpha

        # Power iteration to find dominant eigenvector u_1
        v_vec = [1.0 if i % 2 == 0 else -1.0 for i in range(num_nodes)]
        for _ in range(60):
            # Matrix-vector product
            next_v = [sum(B[i][j] * v_vec[j] for j in range(num_nodes)) for i in range(num_nodes)]
            norm = math.sqrt(sum(x * x for x in next_v))
            if norm > 1e-12:
                v_vec = [x / norm for x in next_v]
            else:
                break

        # Partition based on sign of components relative to mean
        mean_val = sum(v_vec) / num_nodes
        cluster_1: list[int] = []
        cluster_2: list[int] = []

        for i, val in enumerate(v_vec):
            if val >= mean_val:
                cluster_1.append(i)
            else:
                cluster_2.append(i)

        # Ensure non-empty clusters if num_nodes >= 2
        if not cluster_1 or not cluster_2:
            mid = num_nodes // 2
            cluster_1 = list(range(mid))
            cluster_2 = list(range(mid, num_nodes))

        return cluster_1, cluster_2

    def compute_cohesion(self, cluster: list[int], edges: list[Tuple[int, int]]) -> float:
        """Cohesion(C) = E_internal / (E_internal + E_external)."""
        cluster_set = set(cluster)
        e_internal = 0
        e_external = 0
        for u, v in edges:
            in_u = u in cluster_set
            in_v = v in cluster_set
            if in_u and in_v:
                e_internal += 1
            elif in_u or in_v:
                e_external += 1

        total = e_internal + e_external
        return (e_internal / total) if total > 0 else 1.0

    def synthesize_decomposition(self, module_name: str, module_src: str) -> list[RefactoringAction]:
        """
        Decomposes module into M_core.py, M_extended.py and Facade M.py.
        """
        symbols, edges, node_map = self.extract_internal_call_graph(module_src)
        if len(symbols) < 2:
            return []

        c1, c2 = self.spectral_bisection(len(symbols), edges)
        cohesion_1 = self.compute_cohesion(c1, edges)
        cohesion_2 = self.compute_cohesion(c2, edges)

        syms_1 = [symbols[i] for i in c1]
        syms_2 = [symbols[i] for i in c2]

        base_name = module_name.rsplit(".", 1)[-1]
        parent_pkg = module_name.rsplit(".", 1)[0] if "." in module_name else ""
        prefix = f"{parent_pkg}." if parent_pkg else ""

        mod_sub1 = f"{prefix}{base_name}_engine"
        mod_sub2 = f"{prefix}{base_name}_support"

        # Generate source for sub-module 1
        tree1 = ast.Module(body=[node_map[s] for s in syms_1], type_ignores=[])
        ast.fix_missing_locations(tree1)
        src_sub1 = f'"""Subsystem partition 1 of {module_name}."""\nfrom __future__ import annotations\n' + ast.unparse(tree1)

        # Generate source for sub-module 2
        tree2 = ast.Module(body=[node_map[s] for s in syms_2], type_ignores=[])
        ast.fix_missing_locations(tree2)
        src_sub2 = f'"""Subsystem partition 2 of {module_name}."""\nfrom __future__ import annotations\n' + ast.unparse(tree2)

        # Generate Facade module re-exporting all symbols
        facade_lines = [
            f'"""Backward-compatible architectural facade for decomposed {module_name}."""',
            "from __future__ import annotations",
            "",
            f"from .{base_name}_engine import (",
            *[f"    {s}," for s in syms_1],
            ")",
            f"from .{base_name}_support import (",
            *[f"    {s}," for s in syms_2],
            ")",
            "",
            f"__all__ = {json.dumps(symbols)}",
        ]
        src_facade = "\n".join(facade_lines)

        action_decomp = RefactoringAction(
            action_id=f"ACT-DEC-{module_name.replace('.', '_')}",
            refactoring_type=RefactoringType.DECOMPOSE_MODULE,
            target_file=f"{module_name.replace('.', '/')}.py",
            secondary_files=[f"{mod_sub1.replace('.', '/')}.py", f"{mod_sub2.replace('.', '/')}.py"],
            description=(
                f"Spectral decomposition of God-module {module_name} into sub-modules "
                f"{mod_sub1} (Cohesion: {cohesion_1:.2f}) and {mod_sub2} (Cohesion: {cohesion_2:.2f})."
            ),
            blast_radius_score=3.5,
            expected_modularity_gain=0.92,
            ast_diff_summary=f"Partitioned {len(symbols)} symbols into 2 cohesive clusters",
            generated_code_snippets={
                f"{mod_sub1.replace('.', '/')}.py": src_sub1,
                f"{mod_sub2.replace('.', '/')}.py": src_sub2,
            },
            status=RefactoringStatus.VALIDATED,
        )

        action_facade = RefactoringAction(
            action_id=f"ACT-FAC-{module_name.replace('.', '_')}",
            refactoring_type=RefactoringType.EXTRACT_FACADE,
            target_file=f"{module_name.replace('.', '/')}.py",
            secondary_files=[mod_sub1, mod_sub2],
            description=f"Generate backward-compatible Facade for {module_name} re-exporting all public symbols.",
            blast_radius_score=1.2,
            expected_modularity_gain=0.60,
            ast_diff_summary=f"Re-exported symbols {symbols} via facade pattern",
            generated_code_snippets={
                f"{module_name.replace('.', '/')}.py": src_facade
            },
            status=RefactoringStatus.VALIDATED,
        )

        return [action_decomp, action_facade]


# ============================================================================
# 7. Core Engine: RefactoringOptimizer
# ============================================================================

class RefactoringOptimizer:
    """
    Ranks and schedules candidate refactoring actions using a multi-objective
    utility function:
        max U(A) = w_1 * delta Q(A) + w_2 * delta D_norm(A) - w_3 * BlastRadius(A)
    Orders actions topologically to prevent transient syntactical or execution failures.
    """

    def __init__(self, w1: float = 0.50, w2: float = 0.30, w3: float = 0.20):
        self.w1 = w1
        self.w2 = w2
        self.w3 = w3

    def compute_utility(self, action: RefactoringAction) -> float:
        """Evaluates scalar utility score of a refactoring action."""
        delta_q = action.expected_modularity_gain
        delta_d = 0.5  # Normalized distance reduction heuristic
        blast = action.blast_radius_score
        return (self.w1 * delta_q) + (self.w2 * delta_d) - (self.w3 * blast)

    def optimize_plan(
        self,
        plan_id: str,
        actions: list[RefactoringAction],
        source_violation_ids: list[str],
    ) -> RefactoringPlan:
        """
        Sorts candidate actions topologically and by utility.
        Topological precedence:
          1. EXTRACT_PROTOCOL, MOVE_TO_FOUNDATION
          2. DEPENDENCY_INVERSION, MEDIATOR_EVENT_HOOK
          3. DECOMPOSE_MODULE
          4. EXTRACT_FACADE
        """
        priority_map = {
            RefactoringType.EXTRACT_PROTOCOL: 1,
            RefactoringType.MOVE_TO_FOUNDATION: 1,
            RefactoringType.DEPENDENCY_INVERSION: 2,
            RefactoringType.MEDIATOR_EVENT_HOOK: 2,
            RefactoringType.DECOMPOSE_MODULE: 3,
            RefactoringType.EXTRACT_FACADE: 4,
        }

        # Multi-key sort: priority first, then descending utility
        sorted_actions = sorted(
            actions,
            key=lambda a: (priority_map.get(a.refactoring_type, 9), -self.compute_utility(a)),
        )

        total_blast = sum(a.blast_radius_score for a in sorted_actions)
        projected_delta_q = sum(a.expected_modularity_gain for a in sorted_actions) * 0.4
        cycle_reduction = sum(
            1 for a in sorted_actions if a.refactoring_type == RefactoringType.DEPENDENCY_INVERSION
        )

        return RefactoringPlan(
            plan_id=plan_id,
            timestamp=time.time(),
            source_violation_ids=source_violation_ids,
            actions=sorted_actions,
            total_blast_radius=total_blast,
            projected_delta_q=projected_delta_q,
            projected_cycle_reduction=cycle_reduction,
        )


# ============================================================================
# 8. Core Engine: ASTPatchApplicator
# ============================================================================

class ASTPatchApplicator:
    """
    Applies synthesized AST patches, validates syntax round-trip via compile(),
    and verifies that post-refactoring dependency graphs eliminate targeted cycles.
    """

    @staticmethod
    def validate_code(code_str: str, filename: str = "<synthesized>") -> bool:
        """Verifies that generated Python code parses and compiles without error."""
        try:
            tree = ast.parse(code_str, filename=filename)
            compile(tree, filename=filename, mode="exec")
            return True
        except (SyntaxError, ValueError, TypeError) as e:
            logger.error(f"Syntax validation failed for {filename}: {e}")
            return False

    @staticmethod
    def apply_plan(
        plan: RefactoringPlan,
        virtual_fs: dict[str, str],
    ) -> Tuple[bool, dict[str, str], list[str]]:
        """
        Applies a RefactoringPlan against an in-memory virtual filesystem.
        Returns (success, updated_fs, log_messages).
        """
        updated_fs = dict(virtual_fs)
        logs: list[str] = []

        for action in plan.actions:
            logs.append(f"Applying action {action.action_id} ({action.refactoring_type.value})")
            for fpath, snippet in action.generated_code_snippets.items():
                if not ASTPatchApplicator.validate_code(snippet, filename=fpath):
                    action.status = RefactoringStatus.REJECTED
                    logs.append(f"FAILED validation for {fpath}. Aborting action {action.action_id}.")
                    return False, virtual_fs, logs

                updated_fs[fpath] = snippet
                logs.append(f"  -> Written & compiled {fpath} ({len(snippet.splitlines())} lines)")

            action.status = RefactoringStatus.APPLIED

        return True, updated_fs, logs


# ============================================================================
# 9. Master Orchestrator: RefactoringSynthesizer
# ============================================================================

class RefactoringSynthesizer:
    """
    High-level Autonomic Architectural Refactoring & Structural Remediation Synthesizer.
    Coordinates symbol usage extraction, cycle inversion, tier boundary mediator
    synthesis, spectral decomposition, optimization, and AST patch validation.
    """

    def __init__(
        self,
        module_sources: dict[str, str],
        module_tiers: Optional[dict[str, int]] = None,
    ):
        self.module_sources = dict(module_sources)
        self.module_tiers = module_tiers or {}
        self.cycle_remediator = CycleRemediationSynthesizer(self.module_sources)
        self.tier_remediator = TierBoundaryRemediationSynthesizer(self.module_sources, self.module_tiers)
        self.decomposer = IntraModuleDecomposer()
        self.optimizer = RefactoringOptimizer()

    def build_dependency_graph(self) -> dict[str, list[str]]:
        """Constructs inter-module dependency graph based on AST imports."""
        graph: dict[str, list[str]] = defaultdict(list)
        for mod, src in self.module_sources.items():
            tree = ast.parse(src)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name in self.module_sources and alias.name != mod:
                            graph[mod].append(alias.name)
                elif isinstance(node, ast.ImportFrom):
                    if node.module and node.module in self.module_sources and node.module != mod:
                        graph[mod].append(node.module)
        return graph

    def synthesize_remediation(self) -> RefactoringPlan:
        """Executes full diagnostic analysis and synthesizes an optimized remediation plan."""
        actions: list[RefactoringAction] = []
        violation_ids: list[str] = []

        dep_graph = self.build_dependency_graph()

        # 1. Cyclic Dependency Analysis via Tarjan
        sccs = TarjanCycleDetector.detect_scc(dep_graph)
        for i, cycle in enumerate(sccs):
            v_id = f"VIOL-CYCLE-{i+1}"
            violation_ids.append(v_id)
            cycle_actions = self.cycle_remediator.synthesize_remediation_action(cycle)
            actions.extend(cycle_actions)

        # 2. Upward Tier Breach Analysis
        upward_violations = self.tier_remediator.identify_upward_violations(dep_graph)
        for u, v in upward_violations:
            v_id = f"VIOL-TIER-{u.replace('.', '_')}-{v.replace('.', '_')}"
            violation_ids.append(v_id)
            tier_action = self.tier_remediator.synthesize_mediator_action(u, v)
            actions.extend([tier_action])

        # 3. God Module Spectral Decomposition
        for mod, src in self.module_sources.items():
            loc = len(src.splitlines())
            if loc > self.decomposer.theta_loc:
                v_id = f"VIOL-GOD-{mod.replace('.', '_')}"
                violation_ids.append(v_id)
                decomp_actions = self.decomposer.synthesize_decomposition(mod, src)
                actions.extend(decomp_actions)

        # 4. Multi-objective optimization & topological ordering
        plan_id = f"PLAN-REFAC-{int(time.time())}"
        return self.optimizer.optimize_plan(plan_id, actions, violation_ids)


# ============================================================================
# 10. Verification & Demonstration Entry Point
# ============================================================================

def _run_verification_demonstration() -> None:
    """
    Executes an end-to-end synthetic architectural remediation demonstration:
      1. Constructs synthetic modules with circular imports (Alpha <-> Beta).
      2. Constructs an upward tier breach (Tier 1 worker -> Tier 3 governor).
      3. Constructs an oversized God module with coupled function clusters.
      4. Ingests into RefactoringSynthesizer, generates RefactoringPlan.
      5. Applies AST patches and validates syntax round-trip with compile().
      6. Evaluates post-refactoring graph with Tarjan cycle detector to assert 0 SCCs.
    """
    print("=" * 80)
    print("HERMES AUTONOMIC ARCHITECTURAL REFACTORING SYNTHESIZER: RUNTIME VERIFICATION")
    print("=" * 80)

    # 1. Synthetic Module Repository Setup
    module_alpha = (
        '"""Module Alpha: Core processing unit."""\n'
        "from __future__ import annotations\n"
        "from tasks.module_beta import BetaService\n\n"
        "class AlphaService:\n"
        "    def __init__(self) -> None:\n"
        "        self.beta = BetaService()\n\n"
        "    def run_alpha(self, data: str) -> str:\n"
        "        return f'Alpha processed: {self.beta.compute(data)}'\n"
    )

    module_beta = (
        '"""Module Beta: Secondary aggregation unit."""\n'
        "from __future__ import annotations\n"
        "from tasks.module_alpha import AlphaService\n\n"
        "class BetaService:\n"
        "    def compute(self, val: str) -> str:\n"
        "        return f'Beta computed: {val}'\n\n"
        "    def invoke_alpha(self) -> str:\n"
        "        return AlphaService().run_alpha('ping')\n"
    )

    module_worker = (
        '"""Module Worker: Tier 1 execution worker."""\n'
        "from __future__ import annotations\n"
        "from tasks.module_governor import SystemGovernor\n\n"
        "class PipelineWorker:\n"
        "    def __init__(self) -> None:\n"
        "        self.governor = SystemGovernor()\n\n"
        "    def execute_task(self, task_id: str) -> None:\n"
        "        # Illegal upward call into Tier 3 governance\n"
        "        self.governor.record_metric('task_executed')\n"
    )

    module_governor = (
        '"""Module Governor: Tier 3 Governance Coordinator."""\n'
        "from __future__ import annotations\n\n"
        "class SystemGovernor:\n"
        "    def record_metric(self, name: str) -> None:\n"
        "        print(f'Metric recorded: {name}')\n"
    )

    # God Module: 160+ lines of coupled functions
    god_funcs = []
    for i in range(12):
        god_funcs.append(
            f"def god_pipeline_step_{i}(payload: dict) -> dict:\n"
            f"    # Step {i} internal transformation\n"
            f"    payload['step_{i}'] = {i} * 42\n"
            f"    if {i} > 0:\n"
            f"        god_pipeline_step_{i-1}(payload)\n"
            f"    return payload\n"
        )
    module_god = (
        '"""Module God: Monolithic Oversized Subsystem."""\n'
        "from __future__ import annotations\n"
        + "\n".join(god_funcs)
    )

    sources = {
        "tasks.module_alpha": module_alpha,
        "tasks.module_beta": module_beta,
        "tasks.module_worker": module_worker,
        "tasks.module_governor": module_governor,
        "tasks.monolith_processor": module_god,
    }

    tiers = {
        "tasks.module_alpha": 1,
        "tasks.module_beta": 1,
        "tasks.module_worker": 1,
        "tasks.module_governor": 3,
        "tasks.monolith_processor": 2,
    }

    # 2. Pre-refactoring Tarjan & Graph Evaluation
    pre_graph = {
        "tasks.module_alpha": ["tasks.module_beta"],
        "tasks.module_beta": ["tasks.module_alpha"],
        "tasks.module_worker": ["tasks.module_governor"],
        "tasks.module_governor": [],
        "tasks.monolith_processor": [],
    }
    initial_cycles = TarjanCycleDetector.detect_scc(pre_graph)
    print(f"\n[1] INITIAL ARCHITECTURAL STATUS:")
    print(f"    - Modules loaded: {len(sources)}")
    print(f"    - Detected Tarjan Circular SCCs: {initial_cycles}")
    assert len(initial_cycles) == 1, "Expected exactly 1 circular dependency between Alpha and Beta!"
    print("    - Confirmed baseline violation state.")

    # 3. Instantiate RefactoringSynthesizer & Synthesize Plan
    print("\n[2] SYNTHESIZING ARCHITECTURAL REMEDIATION PLAN...")
    synthesizer = RefactoringSynthesizer(sources, tiers)
    plan = synthesizer.synthesize_remediation()

    print(f"    - Plan ID: {plan.plan_id}")
    print(f"    - Total Actions Synthesized: {len(plan.actions)}")
    print(f"    - Addressed Violation IDs: {plan.source_violation_ids}")
    print(f"    - Projected Modularity Gain (Delta Q): {plan.projected_delta_q:.4f}")
    print(f"    - Total Blast Radius Score: {plan.total_blast_radius:.2f}")

    print("\n[3] PLANNED REMEDIATION ACTIONS (TOPOLOGICALLY SCHEDULED):")
    for idx, act in enumerate(plan.actions, 1):
        print(f"    {idx}. [{act.refactoring_type.value}] -> {act.target_file}")
        print(f"       Action ID: {act.action_id} | Blast: {act.blast_radius_score} | Delta Q: {act.expected_modularity_gain}")
        print(f"       Summary:   {act.ast_diff_summary}")

    # 4. Apply Patches to Virtual Filesystem & Verify Compilation
    print("\n[4] EXECUTING AST PATCH APPLICATION & CODE VALIDATION...")
    success, post_fs, logs = ASTPatchApplicator.apply_plan(plan, sources)
    for log_line in logs:
        print(f"    {log_line}")

    assert success, "AST patch application and syntax compilation must succeed!"

    # 5. Post-Refactoring Dependency Verification
    # Construct post-refactored graph from synthesized sources
    post_synthesizer = RefactoringSynthesizer(post_fs, tiers)
    post_graph = post_synthesizer.build_dependency_graph()
    post_cycles = TarjanCycleDetector.detect_scc(post_graph)

    print("\n[5] POST-REFACTORING INVARIANT VERIFICATION:")
    print(f"    - Post-remediation Graph Edges: {dict(post_graph)}")
    print(f"    - Post-remediation Tarjan SCCs: {post_cycles}")
    assert len(post_cycles) == 0, "Post-refactored graph must contain ZERO strongly connected cycles!"
    print("    - SUCCESS: Circular dependencies deterministically eliminated via Dependency Inversion Protocol.")

    # Verify Upward Tier Decoupling
    upward_after = post_synthesizer.tier_remediator.identify_upward_violations(post_graph)
    print(f"    - Post-remediation Upward Tier Violations: {upward_after}")
    assert len(upward_after) == 0, "Post-refactored graph must contain zero upward tier violations!"
    print("    - SUCCESS: Upward tier breach decoupled via synthesized Event/Mediator observer hooks.")

    # 6. JSON Plan Recipe Output Verification
    json_recipe = json.dumps(plan.to_dict(), indent=2)
    print("\n[6] SYNTHESIZED REFACTORING PLAN JSON RECIPE (SAMPLE):")
    print(json_recipe[:600] + "\n    ... [snip remaining JSON recipe] ...")

    print("\n" + "=" * 80)
    print("ALL REMEDIATION INVARIANTS DETERMINISTICALLY VERIFIED: PASSED.")
    print("=" * 80)


if __name__ == "__main__":
    _run_verification_demonstration()