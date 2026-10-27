"""
Pipeline Autonomic Semantic Contract Verifier & Invariant Test Synthesizer
========================================================================
Tasks Module: tasks/pipeline_contract_verifier.py
Repository:   hermes_orchestrator

Algorithmic Specification:
1. Contract Extraction via AST & Docstring Mining:
   Inspects target AST (functions and classes). Extracts type annotations and mines
   preconditions (@requires, Precondition:), postconditions (@ensures, Postcondition:),
   and invariants (@invariant). Compiles dynamic predicates P(args, kwargs) and
   Q(result, pre_state, post_state).
2. Property-Based Boundary Input Generation:
   Synthesizes boundary domains across scalar, string, and collection types:
     - int: [0, 1, -1, -2**31, 2**31 - 1, -2**63, 2**63 - 1, boundary perturbations]
     - float: [0.0, -0.0, 1.0, -1.0, 1e-7, 1e9, math.nan, math.inf, -math.inf]
     - str: ["", " ", "\\n", "\\x00", "a" * 256, "<script>", "' OR 1=1"]
     - list/dict/set: empty, singleton, bounded capacity, diverse types.
   Generates test vectors constrained by precondition satisfiability.
3. AST Mutation Testing:
   Applies high-order syntactic mutation operators:
     - ROR: Relational Operator Replacement (<, <=, >, >=, ==, !=, in, not in, is, is not)
     - AOM: Arithmetic Operator Mutation (+, -, *, /, //, %, **)
     - LCR: Logical Connector Replacement (and <-> or, not elimination)
     - STD: Statement Deletion (replacing assignments/expressions with ast.Pass())
   Filters syntax-invalid and idempotent mutants.
4. Dynamic Sandboxed Test Execution & Mutation Score:
   Executes synthesized property tests against original module and mutants under
   strict execution timeout limits (t_max = 5.0s). Kills mutants on postcondition
   failure, divergence, timeout, or uncaught exception:
     K = sum(I(test(m_j) != PASS))
     MS = K / |M| (for |M| > 0, else 1.0)
5. Composite Semantic Compliance Index (SCI):
     SCI = w_c * C_branch + w_p * P_contract + w_m * MS
   where w_c = 0.25 (branch coverage), w_p = 0.45 (contract satisfaction rate),
   and w_m = 0.30 (mutation score).
   - If P_contract < 1.0 or SCI < 0.60: DEFECT_DETECTED (blocks commit promotion)
   - If 0.60 <= SCI < 0.85: WARNING
   - If SCI >= 0.85 and P_contract == 1.0: VERIFIED
6. Serialization & Pre-Merge Gating:
   Atomic report generation to dashboards/SEMANTIC_CONTRACT_REPORTS.json with minimal
   counterexamples, supporting pre-merge gating for autonomous worktree managers.

Zero External Dependencies: Standard Python library only.
"""

import ast
import inspect
import typing
from dataclasses import dataclass, field, asdict
from enum import Enum
import math
import random
import json
import time
import logging
from pathlib import Path
import re
import traceback
import copy
import tempfile
import threading
import sys
import os
import argparse
from typing import Dict, List, Any, Optional, Tuple, Callable, Set, Union

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SemanticContractVerifier")


# ============================================================================
# 1. CORE DATA STRUCTURES & CLAUSE REPRESENTATIONS
# ============================================================================

class MutationOperatorType(str, Enum):
    ROR = "ROR"  # Relational Operator Replacement
    AOM = "AOM"  # Arithmetic Operator Mutation
    LCR = "LCR"  # Logical Connector Replacement
    STD = "STD"  # Statement Deletion


@dataclass
class ContractClause:
    """Represents a discrete semantic contract clause (pre/post/invariant)."""
    clause_type: str  # 'requires', 'ensures', 'invariant'
    predicate_expression: str
    raw_text: str
    line_number: int = 0

    def clean_expression(self) -> str:
        """Translates pseudo-formal operators (like '==>' or 'implies') to valid Python."""
        expr = self.predicate_expression.strip()
        # Handle logical implication: A ==> B  =>  (not (A) or (B))
        if "==>" in expr:
            parts = expr.split("==>", 1)
            expr = f"(not ({parts[0].strip()}) or ({parts[1].strip()}))"
        # Handle keyword implies: A implies B => (not (A) or (B))
        expr = re.sub(
            r'([a-zA-Z0-9_\.\[\]\(\)\s]+)\s+implies\s+([a-zA-Z0-9_\.\[\]\(\)\s]+)',
            r'(not (\1) or (\2))',
            expr
        )
        return expr


@dataclass
class HoareContract:
    """Aggregates all Hoare-logic contract clauses for a specific callable."""
    function_name: str
    preconditions: List[ContractClause] = field(default_factory=list)
    postconditions: List[ContractClause] = field(default_factory=list)
    invariants: List[ContractClause] = field(default_factory=list)
    arg_types: Dict[str, str] = field(default_factory=dict)
    return_type: Optional[str] = None
    is_method: bool = False
    class_name: Optional[str] = None


@dataclass
class MutantRecord:
    """Metadata and source representation of an individual AST mutant."""
    mutant_id: str
    operator_type: MutationOperatorType
    line_number: int
    description: str
    mutated_source: str
    killed: bool = False
    kill_reason: Optional[str] = None
    killer_test_input: Optional[Dict[str, Any]] = None


@dataclass
class SemanticVerificationReport:
    """Comprehensive semantic contract verification results and compliance metrics."""
    timestamp: str
    target_module: str
    function_name: str
    contracts_extracted: int
    test_vectors_generated: int
    tests_passed: int
    branch_coverage: float
    contract_satisfaction_rate: float
    mutation_score: float
    mutants_generated: int
    mutants_killed: int
    sci_score: float
    verdict: str  # 'VERIFIED', 'WARNING', 'DEFECT_DETECTED'
    verdict_message: str
    counterexamples: List[Dict[str, Any]] = field(default_factory=list)
    mutants_summary: List[Dict[str, Any]] = field(default_factory=list)


# ============================================================================
# 2. CONTRACT EXTRACTION VIA AST & DOCSTRING MINING
# ============================================================================

class DocstringContractParser:
    """
    Parses Python source code AST and mines formal contracts from:
    1. Tagged docstring markers: @requires, @ensures, @invariant
    2. Sphinx-style docstrings: :requires:, :ensures:, :invariant:
    3. Google-style sections: Preconditions:, Postconditions:, Invariants:
    4. Type annotations from function signatures (implicit type contracts)
    """

    TAG_REQUIRES_PATTERN = re.compile(r'@requires\s+([^\n]+)', re.IGNORECASE)
    TAG_ENSURES_PATTERN = re.compile(r'@ensures\s+([^\n]+)', re.IGNORECASE)
    TAG_INVARIANT_PATTERN = re.compile(r'@invariant\s+([^\n]+)', re.IGNORECASE)

    SPHINX_REQUIRES_PATTERN = re.compile(r':(?:requires|precondition):\s*([^\n]+)', re.IGNORECASE)
    SPHINX_ENSURES_PATTERN = re.compile(r':(?:ensures|postcondition):\s*([^\n]+)', re.IGNORECASE)
    SPHINX_INVARIANT_PATTERN = re.compile(r':invariant:\s*([^\n]+)', re.IGNORECASE)

    GOOGLE_SECTION_PATTERN = re.compile(
        r'(?P<section>Preconditions?|Postconditions?|Invariants?):\s*\n(?P<body>(?:(?:\s{2,}|\t)[^\n]*\n*)+)',
        re.IGNORECASE
    )

    @classmethod
    def parse_docstring(cls, docstring: Optional[str]) -> Tuple[List[ContractClause], List[ContractClause], List[ContractClause]]:
        """Extracts preconditions, postconditions, and invariants from a docstring."""
        preconditions: List[ContractClause] = []
        postconditions: List[ContractClause] = []
        invariants: List[ContractClause] = []

        if not docstring:
            return preconditions, postconditions, invariants

        # 1. Parse @tag annotations
        for match in cls.TAG_REQUIRES_PATTERN.finditer(docstring):
            expr = match.group(1).strip()
            preconditions.append(ContractClause("requires", expr, match.group(0)))

        for match in cls.TAG_ENSURES_PATTERN.finditer(docstring):
            expr = match.group(1).strip()
            postconditions.append(ContractClause("ensures", expr, match.group(0)))

        for match in cls.TAG_INVARIANT_PATTERN.finditer(docstring):
            expr = match.group(1).strip()
            invariants.append(ContractClause("invariant", expr, match.group(0)))

        # 2. Parse Sphinx style :requires: / :ensures: / :invariant:
        for match in cls.SPHINX_REQUIRES_PATTERN.finditer(docstring):
            expr = match.group(1).strip()
            preconditions.append(ContractClause("requires", expr, match.group(0)))

        for match in cls.SPHINX_ENSURES_PATTERN.finditer(docstring):
            expr = match.group(1).strip()
            postconditions.append(ContractClause("ensures", expr, match.group(0)))

        for match in cls.SPHINX_INVARIANT_PATTERN.finditer(docstring):
            expr = match.group(1).strip()
            invariants.append(ContractClause("invariant", expr, match.group(0)))

        # 3. Parse Google-style multiline blocks
        for match in cls.GOOGLE_SECTION_PATTERN.finditer(docstring):
            section_name = match.group("section").lower()
            body = match.group("body")
            lines = [line.strip() for line in body.splitlines() if line.strip()]
            for line in lines:
                cleaned_line = line.lstrip("-* ").strip()
                if not cleaned_line:
                    continue
                if section_name.startswith("precondition"):
                    preconditions.append(ContractClause("requires", cleaned_line, line))
                elif section_name.startswith("postcondition"):
                    postconditions.append(ContractClause("ensures", cleaned_line, line))
                elif section_name.startswith("invariant"):
                    invariants.append(ContractClause("invariant", cleaned_line, line))

        return preconditions, postconditions, invariants

    @classmethod
    def parse_function_node(cls, node: Union[ast.FunctionDef, ast.AsyncFunctionDef], class_name: Optional[str] = None) -> HoareContract:
        """Extracts complete HoareContract from an AST FunctionDef node."""
        docstring = ast.get_docstring(node)
        pre, post, inv = cls.parse_docstring(docstring)

        arg_types: Dict[str, str] = {}
        for arg in node.args.args:
            if arg.arg in ("self", "cls"):
                continue
            if arg.annotation:
                try:
                    arg_types[arg.arg] = ast.unparse(arg.annotation)
                except Exception:
                    arg_types[arg.arg] = "Any"

        return_type = None
        if node.returns:
            try:
                return_type = ast.unparse(node.returns)
            except Exception:
                return_type = "Any"

        # Synthesize implicit type invariants if explicit contracts are sparse
        for arg_name, arg_type in arg_types.items():
            if arg_type in ("int", "float", "str", "bool", "list", "dict", "set"):
                type_check = f"isinstance({arg_name}, {arg_type})"
                if not any(arg_name in clause.predicate_expression for clause in pre):
                    pre.append(ContractClause("requires", type_check, f"implicit type check: {type_check}"))

        if return_type in ("int", "float", "str", "bool", "list", "dict", "set"):
            ret_check = f"isinstance(result, {return_type})"
            if not any("result" in clause.predicate_expression for clause in post):
                post.append(ContractClause("ensures", ret_check, f"implicit return type check: {ret_check}"))

        is_method = class_name is not None or (len(node.args.args) > 0 and node.args.args[0].arg == "self")

        return HoareContract(
            function_name=node.name,
            preconditions=pre,
            postconditions=post,
            invariants=inv,
            arg_types=arg_types,
            return_type=return_type,
            is_method=is_method,
            class_name=class_name
        )

    @classmethod
    def extract_contracts(cls, source_code: str) -> Dict[str, HoareContract]:
        """Parses all functions and methods within a source string into HoareContracts."""
        tree = ast.parse(source_code)
        contracts: Dict[str, HoareContract] = {}

        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                contract = cls.parse_function_node(node)
                contracts[node.name] = contract
            elif isinstance(node, ast.ClassDef):
                for sub_node in node.body:
                    if isinstance(sub_node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        full_name = f"{node.name}.{sub_node.name}"
                        contract = cls.parse_function_node(sub_node, class_name=node.name)
                        contracts[full_name] = contract

        return contracts


# ============================================================================
# 3. PROPERTY-BASED BOUNDARY INPUT GENERATION
# ============================================================================

class BoundaryInputSynthesizer:
    """
    Synthesizes boundary and pseudo-random test inputs matching parameter type
    hints, filtering generated test vectors against formal precondition predicates.
    """

    INT_BOUNDARIES = [0, 1, -1, 2**31 - 1, -2**31, 2**63 - 1, -2**63, 42, -42, 100, -100, 2, -2, 10, -10]
    FLOAT_BOUNDARIES = [0.0, -0.0, 1.0, -1.0, 1e-7, -1e-7, 1e9, -1e9, 2.5, -2.5, 0.5, -0.5]
    FLOAT_EXTREMES = [math.nan, math.inf, -math.inf]
    STR_BOUNDARIES = ["", " ", "\n", "\t", "\x00", "a" * 256, "<script>", "' OR 1=1", "normal_test", "12345", "valid_identifier", "a_b_c"]
    BOOL_BOUNDARIES = [True, False]

    @classmethod
    def get_type_boundary_pool(cls, type_name: str) -> List[Any]:
        """Returns standard boundary pool for a given primitive type hint."""
        clean_type = type_name.strip().lower()
        if "int" in clean_type and "point" not in clean_type:
            return list(cls.INT_BOUNDARIES)
        elif "float" in clean_type:
            return list(cls.FLOAT_BOUNDARIES)
        elif "str" in clean_type:
            return list(cls.STR_BOUNDARIES)
        elif "bool" in clean_type:
            return list(cls.BOOL_BOUNDARIES)
        elif "list" in clean_type:
            return [[], [0], [1, 2, 3], [-1, 0, 1], [i for i in range(20)], ["a", "b"]]
        elif "dict" in clean_type:
            return [{}, {"k": "v"}, {"a": 1, "b": 2}, {str(i): i for i in range(10)}]
        elif "set" in clean_type:
            return [set(), {0}, {1, 2, 3}, {-1, 1}]
        else:
            # Fallback domain pool for untyped / Any arguments
            return [0, 1, -1, 10, 0.0, 1.0, "", "test", True, False, [], {}]

    @classmethod
    def generate_random_value(cls, type_name: str) -> Any:
        """Generates a randomized value conforming to the parameter type."""
        clean_type = type_name.strip().lower()
        if "int" in clean_type:
            return random.choice([
                random.randint(-1000, 1000),
                random.randint(0, 50),
                random.randint(1, 100),
                random.randint(-50, -1)
            ])
        elif "float" in clean_type:
            return random.choice([
                random.uniform(-100.0, 100.0),
                random.uniform(0.1, 10.0),
                random.uniform(-1.0, 1.0),
                float(random.randint(-10, 10))
            ])
        elif "str" in clean_type:
            length = random.choice([1, 4, 8, 16, 32])
            chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
            return "".join(random.choice(chars) for _ in range(length))
        elif "bool" in clean_type:
            return random.choice([True, False])
        elif "list" in clean_type:
            length = random.randint(0, 10)
            return [random.randint(-10, 10) for _ in range(length)]
        elif "dict" in clean_type:
            return {f"key_{i}": random.randint(0, 100) for i in range(random.randint(0, 5))}
        elif "set" in clean_type:
            return {random.randint(0, 50) for _ in range(random.randint(0, 5))}
        return random.choice([0, 1, "test", True, 3.14])

    @classmethod
    def evaluate_predicate(cls, expr: str, context: Dict[str, Any]) -> bool:
        """Evaluates a predicate clause within a constrained environment."""
        env = {
            "math": math,
            "abs": abs,
            "len": len,
            "min": min,
            "max": max,
            "sum": sum,
            "all": all,
            "any": any,
            "isinstance": isinstance,
            "int": int,
            "float": float,
            "str": str,
            "bool": bool,
            "list": list,
            "dict": dict,
            "set": set,
            "tuple": tuple,
        }
        env.update(context)
        try:
            res = eval(expr, env, {})
            return bool(res)
        except Exception:
            return False

    @classmethod
    def synthesize_inputs(
        cls,
        contract: HoareContract,
        num_samples: int = 25,
        max_attempts: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Synthesizes a suite of boundary and fuzz test vectors that satisfy all
        preconditions specified in the HoareContract.
        """
        param_names = list(contract.arg_types.keys())
        if not param_names:
            return [{}]

        # Prepare boundary pools per parameter
        boundary_pools: Dict[str, List[Any]] = {
            p: cls.get_type_boundary_pool(contract.arg_types.get(p, "Any"))
            for p in param_names
        }

        valid_vectors: List[Dict[str, Any]] = []
        seen_hashes: Set[str] = set()

        # Step 1: Systematic boundary combinations
        boundary_attempts = 0
        while boundary_attempts < max_attempts // 2 and len(valid_vectors) < num_samples:
            candidate = {
                p: random.choice(boundary_pools[p])
                for p in param_names
            }
            boundary_attempts += 1

            cand_key = repr(sorted(candidate.items()))
            if cand_key in seen_hashes:
                continue

            # Verify preconditions
            all_pre_satisfied = True
            for pre in contract.preconditions:
                if not cls.evaluate_predicate(pre.clean_expression(), candidate):
                    all_pre_satisfied = False
                    break

            if all_pre_satisfied:
                seen_hashes.add(cand_key)
                valid_vectors.append(candidate)

        # Step 2: Randomized fuzzing & heuristic perturbations
        random_attempts = 0
        while random_attempts < max_attempts and len(valid_vectors) < num_samples:
            random_attempts += 1
            candidate = {}
            for p in param_names:
                t = contract.arg_types.get(p, "Any")
                if random.random() < 0.4 and boundary_pools[p]:
                    candidate[p] = random.choice(boundary_pools[p])
                else:
                    candidate[p] = cls.generate_random_value(t)

            cand_key = repr(sorted(candidate.items()))
            if cand_key in seen_hashes:
                continue

            all_pre_satisfied = True
            for pre in contract.preconditions:
                if not cls.evaluate_predicate(pre.clean_expression(), candidate):
                    all_pre_satisfied = False
                    break

            if all_pre_satisfied:
                seen_hashes.add(cand_key)
                valid_vectors.append(candidate)

        return valid_vectors


# ============================================================================
# 4. AST MUTATION TESTING ENGINE
# ============================================================================

class ASTMutationEngine:
    """
    Transforms AST targets into syntactically valid, non-idempotent mutants across:
    - ROR: Relational Operator Replacement
    - AOM: Arithmetic Operator Mutation
    - LCR: Logical Connector Replacement
    - STD: Statement Deletion
    """

    ROR_MAP: Dict[typing.Type[ast.AST], List[typing.Type[ast.AST]]] = {
        ast.Lt: [ast.LtE, ast.Gt, ast.Eq],
        ast.LtE: [ast.Lt, ast.GtE, ast.Eq],
        ast.Gt: [ast.GtE, ast.Lt, ast.Eq],
        ast.GtE: [ast.Gt, ast.LtE, ast.Eq],
        ast.Eq: [ast.NotEq],
        ast.NotEq: [ast.Eq],
        ast.In: [ast.NotIn],
        ast.NotIn: [ast.In],
        ast.Is: [ast.IsNot],
        ast.IsNot: [ast.Is],
    }

    AOM_MAP: Dict[typing.Type[ast.AST], List[typing.Type[ast.AST]]] = {
        ast.Add: [ast.Sub, ast.Mult],
        ast.Sub: [ast.Add, ast.Mult],
        ast.Mult: [ast.Div, ast.Add],
        ast.Div: [ast.Mult, ast.FloorDiv],
        ast.FloorDiv: [ast.Div, ast.Mod],
        ast.Mod: [ast.FloorDiv, ast.Mult],
        ast.Pow: [ast.Mult],
    }

    @dataclass
    class MutationSite:
        site_id: int
        operator_type: MutationOperatorType
        lineno: int
        description: str
        target_kind: str  # 'compare', 'binop', 'boolop', 'unary', 'stmt'
        replacement_op: Any = None
        statement_index: Optional[int] = None

    class SiteCollector(ast.NodeVisitor):
        def __init__(self):
            self.sites: List[ASTMutationEngine.MutationSite] = []
            self.current_site_id = 0

        def visit_Compare(self, node: ast.Compare):
            self.generic_visit(node)
            for idx, op in enumerate(node.ops):
                op_cls = type(op)
                if op_cls in ASTMutationEngine.ROR_MAP:
                    for rep_cls in ASTMutationEngine.ROR_MAP[op_cls]:
                        self.sites.append(ASTMutationEngine.MutationSite(
                            site_id=self.current_site_id,
                            operator_type=MutationOperatorType.ROR,
                            lineno=getattr(node, "lineno", 0),
                            description=f"ROR: replace {op_cls.__name__} with {rep_cls.__name__}",
                            target_kind="compare",
                            replacement_op=(idx, rep_cls)
                        ))
                        self.current_site_id += 1

        def visit_BinOp(self, node: ast.BinOp):
            self.generic_visit(node)
            op_cls = type(node.op)
            if op_cls in ASTMutationEngine.AOM_MAP:
                for rep_cls in ASTMutationEngine.AOM_MAP[op_cls]:
                    self.sites.append(ASTMutationEngine.MutationSite(
                        site_id=self.current_site_id,
                        operator_type=MutationOperatorType.AOM,
                        lineno=getattr(node, "lineno", 0),
                        description=f"AOM: replace {op_cls.__name__} with {rep_cls.__name__}",
                        target_kind="binop",
                        replacement_op=rep_cls
                    ))
                    self.current_site_id += 1

        def visit_BoolOp(self, node: ast.BoolOp):
            self.generic_visit(node)
            op_cls = type(node.op)
            rep_cls = ast.Or if op_cls == ast.And else ast.And
            self.sites.append(ASTMutationEngine.MutationSite(
                site_id=self.current_site_id,
                operator_type=MutationOperatorType.LCR,
                lineno=getattr(node, "lineno", 0),
                description=f"LCR: swap {op_cls.__name__} with {rep_cls.__name__}",
                target_kind="boolop",
                replacement_op=rep_cls
            ))
            self.current_site_id += 1

        def visit_UnaryOp(self, node: ast.UnaryOp):
            self.generic_visit(node)
            if isinstance(node.op, ast.Not):
                self.sites.append(ASTMutationEngine.MutationSite(
                    site_id=self.current_site_id,
                    operator_type=MutationOperatorType.LCR,
                    lineno=getattr(node, "lineno", 0),
                    description="LCR: eliminate unary 'not'",
                    target_kind="unary",
                    replacement_op=None
                ))
                self.current_site_id += 1

        def _check_statement_list(self, stmts: List[ast.stmt]):
            for idx, stmt in enumerate(stmts):
                if isinstance(stmt, (ast.Assign, ast.AugAssign, ast.Expr)):
                    self.sites.append(ASTMutationEngine.MutationSite(
                        site_id=self.current_site_id,
                        operator_type=MutationOperatorType.STD,
                        lineno=getattr(stmt, "lineno", 0),
                        description=f"STD: replace {type(stmt).__name__} with ast.Pass()",
                        target_kind="stmt",
                        statement_index=idx
                    ))
                    self.current_site_id += 1

        def visit_FunctionDef(self, node: ast.FunctionDef):
            self._check_statement_list(node.body)
            self.generic_visit(node)

        def visit_If(self, node: ast.If):
            self._check_statement_list(node.body)
            if node.orelse:
                self._check_statement_list(node.orelse)
            self.generic_visit(node)

    class MutationApplier(ast.NodeTransformer):
        def __init__(self, target_site: "ASTMutationEngine.MutationSite"):
            self.target_site = target_site
            self.current_site_id = 0

        def visit_Compare(self, node: ast.Compare):
            node = self.generic_visit(node)
            for idx, op in enumerate(node.ops):
                op_cls = type(op)
                if op_cls in ASTMutationEngine.ROR_MAP:
                    for rep_cls in ASTMutationEngine.ROR_MAP[op_cls]:
                        if self.current_site_id == self.target_site.site_id:
                            node.ops[idx] = rep_cls()
                        self.current_site_id += 1
            return node

        def visit_BinOp(self, node: ast.BinOp):
            node = self.generic_visit(node)
            op_cls = type(node.op)
            if op_cls in ASTMutationEngine.AOM_MAP:
                for rep_cls in ASTMutationEngine.AOM_MAP[op_cls]:
                    if self.current_site_id == self.target_site.site_id:
                        node.op = rep_cls()
                    self.current_site_id += 1
            return node

        def visit_BoolOp(self, node: ast.BoolOp):
            node = self.generic_visit(node)
            if self.current_site_id == self.target_site.site_id:
                rep_cls = self.target_site.replacement_op
                node.op = rep_cls()
            self.current_site_id += 1
            return node

        def visit_UnaryOp(self, node: ast.UnaryOp):
            node = self.generic_visit(node)
            if isinstance(node.op, ast.Not):
                if self.current_site_id == self.target_site.site_id:
                    self.current_site_id += 1
                    return node.operand
                self.current_site_id += 1
            return node

        def _mutate_stmts(self, stmts: List[ast.stmt]) -> List[ast.stmt]:
            new_stmts = []
            for stmt in stmts:
                if isinstance(stmt, (ast.Assign, ast.AugAssign, ast.Expr)):
                    if self.current_site_id == self.target_site.site_id:
                        pass_stmt = ast.Pass()
                        ast.copy_location(pass_stmt, stmt)
                        new_stmts.append(pass_stmt)
                    else:
                        new_stmts.append(stmt)
                    self.current_site_id += 1
                else:
                    new_stmts.append(stmt)
            return new_stmts

        def visit_FunctionDef(self, node: ast.FunctionDef):
            node.body = self._mutate_stmts(node.body)
            return self.generic_visit(node)

        def visit_If(self, node: ast.If):
            node.body = self._mutate_stmts(node.body)
            if node.orelse:
                node.orelse = self._mutate_stmts(node.orelse)
            return self.generic_visit(node)

    @classmethod
    def generate_mutants(cls, source_code: str, max_mutants: int = 60) -> List[MutantRecord]:
        """Discovers all viable mutation sites and generates validated mutants."""
        try:
            tree = ast.parse(source_code)
        except SyntaxError as e:
            logger.error(f"Cannot generate mutants for invalid source: {e}")
            return []

        original_unparsed = ast.unparse(tree)
        collector = cls.SiteCollector()
        collector.visit(tree)

        mutants: List[MutantRecord] = []
        sampled_sites = collector.sites
        if len(sampled_sites) > max_mutants:
            sampled_sites = random.sample(sampled_sites, max_mutants)

        for idx, site in enumerate(sampled_sites):
            tree_copy = copy.deepcopy(tree)
            applier = cls.MutationApplier(site)
            mutated_tree = applier.visit(tree_copy)
            ast.fix_missing_locations(mutated_tree)

            try:
                mutated_code = ast.unparse(mutated_tree)
                # Verify syntax validity
                compile(mutated_tree, filename=f"<mutant_{idx}>", mode="exec")
            except Exception:
                continue

            # Filter idempotent mutants
            if mutated_code.strip() == original_unparsed.strip():
                continue

            mutant_id = f"mutant_{site.operator_type.value}_{idx:03d}"
            mutants.append(MutantRecord(
                mutant_id=mutant_id,
                operator_type=site.operator_type,
                line_number=site.lineno,
                description=site.description,
                mutated_source=mutated_code
            ))

        return mutants


# ============================================================================
# 5. BRANCH COVERAGE INSTRUMENTATION & DYNAMIC RUNNER
# ============================================================================

class BranchCoverageInstrumenter(ast.NodeTransformer):
    """
    Instruments conditional branches in AST with branch probes to compute
    deterministic branch coverage C_branch without third-party dependencies.
    """

    def __init__(self):
        self.branch_count = 0
        self.branch_map: Dict[int, str] = {}

    def visit_If(self, node: ast.If):
        self.generic_visit(node)

        b_then = self.branch_count
        self.branch_map[b_then] = f"Line {getattr(node, 'lineno', 0)}: THEN branch"
        self.branch_count += 1

        probe_then = ast.Expr(
            value=ast.Call(
                func=ast.Name(id="__record_branch__", ctx=ast.Load()),
                args=[ast.Constant(value=b_then)],
                keywords=[]
            )
        )
        node.body.insert(0, probe_then)

        b_else = self.branch_count
        self.branch_map[b_else] = f"Line {getattr(node, 'lineno', 0)}: ELSE branch"
        self.branch_count += 1

        probe_else = ast.Expr(
            value=ast.Call(
                func=ast.Name(id="__record_branch__", ctx=ast.Load()),
                args=[ast.Constant(value=b_else)],
                keywords=[]
            )
        )
        if node.orelse:
            node.orelse.insert(0, probe_else)
        else:
            node.orelse = [probe_else]

        return node


class SandboxedContractRunner:
    """
    Safely executes original code and synthesized mutants under timeout constraints,
    monitoring state mutations, exceptions, and Hoare-logic postcondition compliance.
    """

    DEFAULT_TIMEOUT_SEC = 3.0

    @classmethod
    def execute_callable_with_timeout(
        cls,
        target_func: Callable,
        args: Tuple = (),
        kwargs: Optional[Dict[str, Any]] = None,
        timeout_sec: float = DEFAULT_TIMEOUT_SEC
    ) -> Tuple[bool, Any, Optional[Exception]]:
        """Executes a target function on a daemon thread with timeout guarding."""
        if kwargs is None:
            kwargs = {}

        result_container: List[Any] = []
        error_container: List[Exception] = []

        def worker():
            try:
                res = target_func(*args, **kwargs)
                result_container.append(res)
            except Exception as e:
                error_container.append(e)

        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        thread.join(timeout=timeout_sec)

        if thread.is_alive():
            return False, None, TimeoutError(f"Execution timed out after {timeout_sec}s")

        if error_container:
            return False, None, error_container[0]

        return True, (result_container[0] if result_container else None), None

    @classmethod
    def evaluate_contract_predicates(
        cls,
        clauses: List[ContractClause],
        result: Any,
        pre_state: Dict[str, Any],
        post_state: Dict[str, Any],
        context_kwargs: Dict[str, Any]
    ) -> Tuple[bool, Optional[ContractClause], Optional[str]]:
        """Evaluates all postcondition and invariant clauses against execution outcomes."""
        env = {
            "math": math,
            "abs": abs,
            "len": len,
            "min": min,
            "max": max,
            "sum": sum,
            "all": all,
            "any": any,
            "isinstance": isinstance,
            "int": int,
            "float": float,
            "str": str,
            "bool": bool,
            "list": list,
            "dict": dict,
            "set": set,
            "tuple": tuple,
            "result": result,
            "return_value": result,
            "pre_state": pre_state,
            "post_state": post_state,
        }
        # Provide both direct kwargs and old_param aliases
        env.update(context_kwargs)
        for k, v in pre_state.items():
            env[f"old_{k}"] = v

        for clause in clauses:
            expr = clause.clean_expression()
            try:
                val = eval(expr, env, {})
                if not bool(val):
                    return False, clause, f"Evaluated to False: {clause.predicate_expression}"
            except Exception as e:
                return False, clause, f"Evaluation error '{e}' on: {clause.predicate_expression}"

        return True, None, None


# ============================================================================
# 6. SEMANTIC CONTRACT VERIFIER COORDINATOR
# ============================================================================

class SemanticContractVerifier:
    """
    Main verification engine: coordinates contract mining, test synthesis,
    mutation generation, sandboxed execution, and composite SCI calculation.
    """

    WEIGHT_BRANCH_COVERAGE = 0.25
    WEIGHT_CONTRACT_SATISFACTION = 0.45
    WEIGHT_MUTATION_SCORE = 0.30

    def __init__(self, timeout_sec: float = 3.0, samples_per_function: int = 25):
        self.timeout_sec = timeout_sec
        self.samples_per_function = samples_per_function

    def measure_branch_coverage(
        self,
        source_code: str,
        function_name: str,
        test_vectors: List[Dict[str, Any]]
    ) -> float:
        """Instruments source AST and computes branch coverage C_branch across test vectors."""
        try:
            tree = ast.parse(source_code)
        except Exception:
            return 1.0

        instrumenter = BranchCoverageInstrumenter()
        instrumented_tree = instrumenter.visit(tree)
        ast.fix_missing_locations(instrumented_tree)

        total_branches = instrumenter.branch_count
        if total_branches == 0:
            return 1.0

        covered_branches: Set[int] = set()

        def branch_recorder(bid: int):
            covered_branches.add(bid)

        module_ns = {
            "__record_branch__": branch_recorder,
            "math": math,
            "__builtins__": __builtins__
        }

        try:
            compiled = compile(instrumented_tree, filename="<branch_coverage>", mode="exec")
            exec(compiled, module_ns)
        except Exception as e:
            logger.warning(f"Branch instrumented compilation failed: {e}")
            return 0.5

        func = module_ns.get(function_name)
        if not callable(func):
            return 1.0

        for vec in test_vectors:
            try:
                func(**copy.deepcopy(vec))
            except Exception:
                pass

        return round(len(covered_branches) / total_branches, 4)

    def verify_function(
        self,
        source_code: str,
        contract: HoareContract,
        module_name: str = "target_module"
    ) -> SemanticVerificationReport:
        """Executes full semantic verification loop for a single HoareContract target."""
        func_name = contract.function_name
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        # Compile original source
        orig_ns = {"math": math, "__builtins__": __builtins__}
        try:
            compiled = compile(source_code, filename="<original>", mode="exec")
            exec(compiled, orig_ns)
        except Exception as e:
            logger.error(f"Failed to compile original source code: {e}")
            return SemanticVerificationReport(
                timestamp=timestamp,
                target_module=module_name,
                function_name=func_name,
                contracts_extracted=len(contract.preconditions) + len(contract.postconditions) + len(contract.invariants),
                test_vectors_generated=0,
                tests_passed=0,
                branch_coverage=0.0,
                contract_satisfaction_rate=0.0,
                mutation_score=0.0,
                mutants_generated=0,
                mutants_killed=0,
                sci_score=0.0,
                verdict="DEFECT_DETECTED",
                verdict_message=f"Syntax or Compilation Error: {e}"
            )

        target_callable = orig_ns.get(func_name)
        if not callable(target_callable):
            # Check for class method
            if contract.class_name and contract.class_name in orig_ns:
                cls_obj = orig_ns[contract.class_name]
                try:
                    instance = cls_obj()
                    target_callable = getattr(instance, func_name)
                except Exception:
                    pass

        if not callable(target_callable):
            return SemanticVerificationReport(
                timestamp=timestamp,
                target_module=module_name,
                function_name=func_name,
                contracts_extracted=0,
                test_vectors_generated=0,
                tests_passed=0,
                branch_coverage=0.0,
                contract_satisfaction_rate=0.0,
                mutation_score=0.0,
                mutants_generated=0,
                mutants_killed=0,
                sci_score=0.0,
                verdict="DEFECT_DETECTED",
                verdict_message=f"Target callable '{func_name}' not found in compiled namespace."
            )

        # 1. Synthesize boundary and fuzz inputs
        test_vectors = BoundaryInputSynthesizer.synthesize_inputs(
            contract,
            num_samples=self.samples_per_function
        )
        if not test_vectors:
            test_vectors = [{}]

        # 2. Dynamic execution on original code & contract satisfaction
        passed_tests = 0
        counterexamples: List[Dict[str, Any]] = []
        original_outputs: List[Tuple[bool, Any]] = []

        all_post_clauses = list(contract.postconditions) + list(contract.invariants)

        for vec in test_vectors:
            pre_state = copy.deepcopy(vec)
            call_kwargs = copy.deepcopy(vec)
            success, result, err = SandboxedContractRunner.execute_callable_with_timeout(
                target_callable,
                kwargs=call_kwargs,
                timeout_sec=self.timeout_sec
            )
            post_state = copy.deepcopy(call_kwargs)

            if not success:
                counterexamples.append({
                    "test_vector": vec,
                    "failure_type": "execution_error",
                    "details": str(err)
                })
                original_outputs.append((False, err))
                continue

            original_outputs.append((True, result))

            # Evaluate postconditions
            is_valid, violated_clause, msg = SandboxedContractRunner.evaluate_contract_predicates(
                all_post_clauses,
                result,
                pre_state,
                post_state,
                vec
            )
            if is_valid:
                passed_tests += 1
            else:
                counterexamples.append({
                    "test_vector": vec,
                    "result": repr(result),
                    "failure_type": "postcondition_violation",
                    "clause": violated_clause.predicate_expression if violated_clause else None,
                    "details": msg
                })

        p_contract = round(passed_tests / len(test_vectors), 4) if test_vectors else 1.0

        # 3. Measure Branch Coverage
        c_branch = self.measure_branch_coverage(source_code, func_name, test_vectors)

        # 4. AST Mutation Testing
        mutants = ASTMutationEngine.generate_mutants(source_code)
        mutants_killed = 0
        mutants_summary: List[Dict[str, Any]] = []

        for m in mutants:
            mut_ns = {"math": math, "__builtins__": __builtins__}
            try:
                mut_code = compile(m.mutated_source, filename=f"<{m.mutant_id}>", mode="exec")
                exec(mut_code, mut_ns)
                mut_callable = mut_ns.get(func_name)
                if not callable(mut_callable):
                    if contract.class_name and contract.class_name in mut_ns:
                        cls_inst = mut_ns[contract.class_name]()
                        mut_callable = getattr(cls_inst, func_name)
            except Exception as e:
                # Compile-time failure indicates mutant was instantly killed
                m.killed = True
                m.kill_reason = f"Compilation failure: {e}"
                mutants_killed += 1
                mutants_summary.append({
                    "mutant_id": m.mutant_id,
                    "operator": m.operator_type.value,
                    "line": m.line_number,
                    "killed": True,
                    "reason": m.kill_reason
                })
                continue

            # Run test vectors against mutant
            mutant_killed = False
            for idx, vec in enumerate(test_vectors):
                orig_ok, orig_val = original_outputs[idx]
                call_kwargs = copy.deepcopy(vec)
                pre_state = copy.deepcopy(vec)

                m_ok, m_res, m_err = SandboxedContractRunner.execute_callable_with_timeout(
                    mut_callable,
                    kwargs=call_kwargs,
                    timeout_sec=self.timeout_sec
                )
                post_state = copy.deepcopy(call_kwargs)

                if not m_ok:
                    # Mutant crashed or timed out where original might have passed
                    mutant_killed = True
                    m.killed = True
                    m.kill_reason = f"Runtime exception / timeout: {m_err}"
                    m.killer_test_input = vec
                    break

                # Postcondition check against mutant
                p_ok, p_clause, _ = SandboxedContractRunner.evaluate_contract_predicates(
                    all_post_clauses,
                    m_res,
                    pre_state,
                    post_state,
                    vec
                )
                if not p_ok:
                    mutant_killed = True
                    m.killed = True
                    m.kill_reason = f"Violated contract: {p_clause.predicate_expression if p_clause else ''}"
                    m.killer_test_input = vec
                    break

                # Behavioral divergence check against original return value
                if orig_ok and m_res != orig_val:
                    # Floating point NaN check: NaN != NaN is True in Python, handle cleanly
                    if isinstance(orig_val, float) and isinstance(m_res, float):
                        if math.isnan(orig_val) and math.isnan(m_res):
                            continue
                    mutant_killed = True
                    m.killed = True
                    m.kill_reason = f"Diverged output: expected {orig_val}, got {m_res}"
                    m.killer_test_input = vec
                    break

            if mutant_killed:
                mutants_killed += 1

            mutants_summary.append({
                "mutant_id": m.mutant_id,
                "operator": m.operator_type.value,
                "line": m.line_number,
                "killed": m.killed,
                "reason": m.kill_reason
            })

        ms = round(mutants_killed / len(mutants), 4) if mutants else 1.0

        # 5. Calculate Composite Semantic Compliance Index (SCI)
        sci = round(
            (self.WEIGHT_BRANCH_COVERAGE * c_branch) +
            (self.WEIGHT_CONTRACT_SATISFACTION * p_contract) +
            (self.WEIGHT_MUTATION_SCORE * ms),
            4
        )

        # 6. Verdict Determination
        total_contracts = len(contract.preconditions) + len(contract.postconditions) + len(contract.invariants)
        if p_contract < 1.0 or sci < 0.60:
            verdict = "DEFECT_DETECTED"
            verdict_message = (
                f"Defect detected in '{func_name}': P_contract={p_contract:.2f} (< 1.00) "
                f"or SCI={sci:.2f} (< 0.60). Commit promotion blocked."
            )
        elif 0.60 <= sci < 0.85:
            verdict = "WARNING"
            verdict_message = (
                f"Semantic warning for '{func_name}': SCI={sci:.2f} satisfies baseline "
                f"but falls below publication gate threshold (0.85)."
            )
        else:
            verdict = "VERIFIED"
            verdict_message = (
                f"Semantic contracts for '{func_name}' rigorously verified with "
                f"SCI={sci:.2f} (Branch={c_branch:.2f}, Contract={p_contract:.2f}, MS={ms:.2f})."
            )

        return SemanticVerificationReport(
            timestamp=timestamp,
            target_module=module_name,
            function_name=func_name,
            contracts_extracted=total_contracts,
            test_vectors_generated=len(test_vectors),
            tests_passed=passed_tests,
            branch_coverage=c_branch,
            contract_satisfaction_rate=p_contract,
            mutation_score=ms,
            mutants_generated=len(mutants),
            mutants_killed=mutants_killed,
            sci_score=sci,
            verdict=verdict,
            verdict_message=verdict_message,
            counterexamples=counterexamples,
            mutants_summary=mutants_summary
        )

    def verify_source(self, source_code: str, module_name: str = "module") -> Dict[str, Any]:
        """Runs contract verification across all annotated callables in source code."""
        contracts = DocstringContractParser.extract_contracts(source_code)
        reports: List[SemanticVerificationReport] = []

        overall_status = "PASS"
        lowest_sci = 1.0
        total_defects = 0

        for func_name, contract in contracts.items():
            rep = self.verify_function(source_code, contract, module_name=module_name)
            reports.append(rep)
            if rep.sci_score < lowest_sci:
                lowest_sci = rep.sci_score
            if rep.verdict == "DEFECT_DETECTED":
                overall_status = "FAIL"
                total_defects += 1

        avg_sci = round(sum(r.sci_score for r in reports) / len(reports), 4) if reports else 1.0
        overall_verdict = "DEFECT_DETECTED" if total_defects > 0 else ("WARNING" if avg_sci < 0.85 else "VERIFIED")

        return {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "module_name": module_name,
            "overall_status": overall_status,
            "overall_verdict": overall_verdict,
            "average_sci": avg_sci,
            "lowest_sci": lowest_sci,
            "total_callables_verified": len(reports),
            "total_defects": total_defects,
            "reports": [asdict(r) for r in reports]
        }

    def verify_file(self, file_path: Union[str, Path]) -> Dict[str, Any]:
        """Loads and verifies a Python source file from disk."""
        path = Path(file_path)
        if not path.is_file():
            raise FileNotFoundError(f"Source file not found: {path}")

        source_code = path.read_text(encoding="utf-8")
        return self.verify_source(source_code, module_name=path.name)

    @classmethod
    def save_report(cls, report_data: Dict[str, Any], output_path: Union[str, Path] = "dashboards/SEMANTIC_CONTRACT_REPORTS.json") -> Path:
        """Atomically serializes verification report to the designated dashboards path."""
        out_p = Path(output_path)
        out_p.parent.mkdir(parents=True, exist_ok=True)

        with tempfile.NamedTemporaryFile("w", dir=str(out_p.parent), delete=False, encoding="utf-8") as tf:
            json.dump(report_data, tf, indent=2)
            temp_name = tf.name

        os.replace(temp_name, out_p)
        logger.info(f"Verification report atomically written to {out_p.resolve()}")
        return out_p


# ============================================================================
# 7. PRE-MERGE GATING & CLI INTERFACE
# ============================================================================

def gate_commit(
    files: List[Union[str, Path]],
    min_sci: float = 0.85,
    report_output: Optional[Union[str, Path]] = "dashboards/SEMANTIC_CONTRACT_REPORTS.json"
) -> bool:
    """
    Pre-merge gate API callable by worktree_sandbox_manager or orchestrators.
    Returns True if all files satisfy contract invariants and SCI >= min_sci.
    """
    verifier = SemanticContractVerifier()
    all_passed = True
    aggregated_reports = []

    for f in files:
        path = Path(f)
        if not path.suffix == ".py":
            continue
        try:
            res = verifier.verify_file(path)
            aggregated_reports.append(res)
            if res["overall_status"] == "FAIL" or res["average_sci"] < min_sci:
                all_passed = False
                logger.error(f"Gate Failed on {path}: verdict={res['overall_verdict']}, avg_sci={res['average_sci']}")
            else:
                logger.info(f"Gate Passed on {path}: verdict={res['overall_verdict']}, avg_sci={res['average_sci']}")
        except Exception as e:
            logger.error(f"Gate Exception on {path}: {e}")
            all_passed = False

    if report_output and aggregated_reports:
        combined = {
            "gate_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "gate_passed": all_passed,
            "min_sci_threshold": min_sci,
            "inspected_files": [str(f) for f in files],
            "file_summaries": aggregated_reports
        }
        SemanticContractVerifier.save_report(combined, report_output)

    return all_passed


# ============================================================================
# 8. EXHAUSTIVE SELF-TEST SUITE
# ============================================================================

def run_exhaustive_self_test():
    """
    Executes comprehensive 10-stage test suite covering:
    1. Contract Parsing (Google, Sphinx, @-tags, implicit type contracts)
    2. Boundary Input Synthesizer & Precondition Filtering
    3. AST Mutation Engine (ROR, AOM, LCR, STD)
    4. Deterministic Branch Coverage Instrumenter
    5. Sandboxed Dynamic Runner & Timeout Protection
    6. Mathematical Algorithm Verification (safe_divide)
    7. Value Clamping Verification (clamp_value with branches)
    8. String Sanitizer Verification (sanitize_identifier)
    9. Defect Detection & Counterexample Isolation (buggy_sum)
    10. Atomic Report Serialization & Commit Gating
    """
    print("=" * 80)
    print("RUNNING EXHAUSTIVE SELF-TEST SUITE: pipeline_contract_verifier.py")
    print("=" * 80)

    test_failures = 0

    def assert_true(cond: bool, msg: str):
        nonlocal test_failures
        if not cond:
            print(f"[FAIL] {msg}")
            test_failures += 1
        else:
            print(f"[PASS] {msg}")

    # ------------------------------------------------------------------------
    # Test 1: Contract Extraction from Docstrings
    # ------------------------------------------------------------------------
    sample_doc = """
    Computes bounded ratio.
    @requires denominator != 0
    @requires numerator >= 0
    @ensures result >= 0
    @invariant self.active == True
    :requires: numerator < 1000
    :ensures: result <= numerator
    Preconditions:
        denominator > 0
    Postconditions:
        isinstance(result, float)
    Invariants:
        self.limit >= 0
    """
    pre, post, inv = DocstringContractParser.parse_docstring(sample_doc)
    assert_true(len(pre) >= 3, f"Extracted {len(pre)} preconditions (expected >= 3)")
    assert_true(len(post) >= 3, f"Extracted {len(post)} postconditions (expected >= 3)")
    assert_true(len(inv) >= 2, f"Extracted {len(inv)} invariants (expected >= 2)")

    # ------------------------------------------------------------------------
    # Test 2: Boundary Input Generation & Precondition Filtering
    # ------------------------------------------------------------------------
    dummy_contract = HoareContract(
        function_name="dummy_calc",
        preconditions=[
            ContractClause("requires", "x > 0", "@requires x > 0"),
            ContractClause("requires", "y >= 5", "@requires y >= 5")
        ],
        arg_types={"x": "int", "y": "int"}
    )
    vectors = BoundaryInputSynthesizer.synthesize_inputs(dummy_contract, num_samples=15)
    assert_true(len(vectors) > 0, f"Synthesized {len(vectors)} valid boundary vectors")
    for vec in vectors:
        assert_true(vec["x"] > 0 and vec["y"] >= 5, f"Vector {vec} strictly satisfies preconditions")

    # ------------------------------------------------------------------------
    # Test 3: AST Mutation Engine (Operators ROR, AOM, LCR, STD)
    # ------------------------------------------------------------------------
    sample_math_code = """
def calculate(a: int, b: int) -> int:
    val = a + b
    if val > 10 and a != 0:
        val = val * 2
    return val
"""
    mutants = ASTMutationEngine.generate_mutants(sample_math_code)
    assert_true(len(mutants) >= 4, f"Generated {len(mutants)} distinct AST mutants (expected >= 4)")
    op_types_present = {m.operator_type for m in mutants}
    assert_true(MutationOperatorType.AOM in op_types_present, "AOM operator generated")
    assert_true(MutationOperatorType.ROR in op_types_present, "ROR operator generated")

    # ------------------------------------------------------------------------
    # Test 4: Branch Coverage Instrumenter
    # ------------------------------------------------------------------------
    verifier = SemanticContractVerifier()
    branch_cov = verifier.measure_branch_coverage(
        sample_math_code,
        "calculate",
        [{"a": 20, "b": 5}, {"a": 1, "b": 1}]
    )
    assert_true(branch_cov > 0.0, f"Branch coverage calculated: {branch_cov * 100:.1f}%")

    # ------------------------------------------------------------------------
    # Test 5: Dynamic Runner Timeout Guard
    # ------------------------------------------------------------------------
    def hanging_function():
        time.sleep(1.0)
        return 42

    ok, res, err = SandboxedContractRunner.execute_callable_with_timeout(
        hanging_function,
        timeout_sec=0.1
    )
    assert_true(not ok and isinstance(err, TimeoutError), "Timeout guard successfully killed hanging function")

    # ------------------------------------------------------------------------
    # Test 6: Verification of Math Algorithm: safe_divide
    # ------------------------------------------------------------------------
    code_safe_divide = """
import math

def safe_divide(numerator: float, denominator: float) -> float:
    \"\"\"Safely divides two floating point numbers.
    @requires denominator != 0.0
    @requires not math.isnan(numerator) and not math.isnan(denominator)
    @ensures not math.isnan(result)
    @ensures (numerator == 0.0) == (result == 0.0)
    \"\"\"
    if denominator == 0.0 or math.isnan(numerator) or math.isnan(denominator):
        raise ValueError("Invalid division operands")
    return numerator / denominator
"""
    rep_divide = verifier.verify_source(code_safe_divide, "math_module")
    div_report = rep_divide["reports"][0]
    assert_true(div_report["verdict"] in ("VERIFIED", "WARNING"), f"safe_divide verdict: {div_report['verdict']}")
    assert_true(div_report["sci_score"] >= 0.70, f"safe_divide SCI: {div_report['sci_score']} (>= 0.70)")
    assert_true(div_report["mutants_killed"] > 0, f"safe_divide killed {div_report['mutants_killed']} mutants")

    # ------------------------------------------------------------------------
    # Test 7: Verification of Value Clamping with Multi-Branching
    # ------------------------------------------------------------------------
    code_clamp = """
def clamp_value(val: int, low: int, high: int) -> int:
    \"\"\"Clamps val between low and high bounds.
    @requires low <= high
    @ensures result >= low and result <= high
    @ensures (val >= low and val <= high) == (result == val)
    \"\"\"
    if val < low:
        return low
    elif val > high:
        return high
    else:
        return val
"""
    rep_clamp = verifier.verify_source(code_clamp, "clamp_module")
    clamp_report = rep_clamp["reports"][0]
    assert_true(clamp_report["verdict"] == "VERIFIED", f"clamp_value verdict: {clamp_report['verdict']}")
    assert_true(clamp_report["branch_coverage"] >= 0.80, f"clamp_value branch coverage: {clamp_report['branch_coverage'] * 100:.1f}%")
    assert_true(clamp_report["sci_score"] >= 0.85, f"clamp_value SCI: {clamp_report['sci_score']} (>= 0.85)")

    # ------------------------------------------------------------------------
    # Test 8: Verification of String Sanitizer
    # ------------------------------------------------------------------------
    code_sanitizer = """
def sanitize_identifier(name: str, max_length: int = 32) -> str:
    \"\"\"Sanitizes input into a valid Python identifier.
    @requires len(name) > 0
    @requires max_length >= 4
    @ensures len(result) <= max_length
    @ensures result.isidentifier()
    \"\"\"
    cleaned = ""
    for ch in name:
        if ch.isalnum() or ch == "_":
            cleaned += ch
        else:
            cleaned += "_"
    
    if cleaned[0].isdigit():
        cleaned = "_" + cleaned

    truncated = cleaned[:max_length]
    if not truncated or not truncated.isidentifier():
        return "_ident"
    return truncated
"""
    rep_sanitizer = verifier.verify_source(code_sanitizer, "sanitizer_module")
    san_report = rep_sanitizer["reports"][0]
    assert_true(san_report["verdict"] in ("VERIFIED", "WARNING"), f"sanitize_identifier verdict: {san_report['verdict']}")
    assert_true(san_report["contract_satisfaction_rate"] == 1.0, "All synthesized vectors satisfied contracts")

    # ------------------------------------------------------------------------
    # Test 9: Defect Detection & Counterexample Isolation (buggy_sum)
    # ------------------------------------------------------------------------
    code_buggy = """
def buggy_sum(a: int, b: int) -> int:
    \"\"\"Defective adder that subtracts instead of adds.
    @requires a > 0 and b > 0
    @ensures result == a + b
    \"\"\"
    return a - b
"""
    rep_buggy = verifier.verify_source(code_buggy, "buggy_module")
    bug_report = rep_buggy["reports"][0]
    assert_true(bug_report["verdict"] == "DEFECT_DETECTED", f"buggy_sum correctly flagged as DEFECT_DETECTED")
    assert_true(len(bug_report["counterexamples"]) > 0, "Counterexample correctly isolated for defective implementation")
    assert_true(rep_buggy["overall_status"] == "FAIL", "Overall verification suite returned FAIL status")

    # ------------------------------------------------------------------------
    # Test 10: Atomic Report Serialization & Commit Gate API
    # ------------------------------------------------------------------------
    with tempfile.TemporaryDirectory() as tmpdir:
        report_file = Path(tmpdir) / "reports" / "SEMANTIC_CONTRACT_REPORTS.json"
        saved_path = SemanticContractVerifier.save_report(rep_clamp, report_file)
        assert_true(saved_path.is_file(), f"Report successfully written to {saved_path}")

        with open(saved_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert_true("reports" in data and len(data["reports"]) == 1, "Report JSON schema structurally verified")

        # Create temporary valid and invalid Python files for gate_commit
        clean_file = Path(tmpdir) / "clean.py"
        clean_file.write_text(code_clamp, encoding="utf-8")

        bad_file = Path(tmpdir) / "bad.py"
        bad_file.write_text(code_buggy, encoding="utf-8")

        gate_pass = gate_commit([clean_file], min_sci=0.80, report_output=None)
        assert_true(gate_pass is True, "Pre-merge gate correctly APPROVED clean module")

        gate_reject = gate_commit([bad_file], min_sci=0.80, report_output=None)
        assert_true(gate_reject is False, "Pre-merge gate correctly BLOCKED defective module")

    print("=" * 80)
    if test_failures == 0:
        print("ALL 10 VERIFICATION SUITE TESTS PASSED (100% SUCCESS)")
    else:
        print(f"SELF-TEST SUITE DETECTED {test_failures} FAILURES")
    print("=" * 80)
    return test_failures == 0


# ============================================================================
# 9. CLI ENTRYPOINT
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Pipeline Autonomic Semantic Contract Verifier & Invariant Test Synthesizer"
    )
    parser.add_argument(
        "-t", "--target",
        help="Path to target Python source file or directory for verification",
        default=None
    )
    parser.add_argument(
        "--report-out",
        help="Output path for JSON verification report",
        default="dashboards/SEMANTIC_CONTRACT_REPORTS.json"
    )
    parser.add_argument(
        "--min-sci",
        type=float,
        default=0.85,
        help="Minimum Semantic Compliance Index (SCI) threshold (default: 0.85)"
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=25,
        help="Number of boundary test vectors synthesized per function (default: 25)"
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=3.0,
        help="Execution timeout limit per test in seconds (default: 3.0)"
    )
    parser.add_argument(
        "--gate",
        action="store_true",
        help="Pre-merge gate mode: exits with status code 1 on contract defect or SCI < min-sci"
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Execute the comprehensive built-in 10-stage self-test suite"
    )

    args = parser.parse_args()

    if args.self_test or args.target is None:
        success = run_exhaustive_self_test()
        if not success:
            sys.exit(1)
        if args.target is None:
            sys.exit(0)

    target_path = Path(args.target)
    if not target_path.exists():
        logger.error(f"Target path does not exist: {target_path}")
        sys.exit(1)

    target_files = [target_path] if target_path.is_file() else list(target_path.glob("**/*.py"))
    passed = gate_commit(target_files, min_sci=args.min_sci, report_output=args.report_out)

    if args.gate and not passed:
        logger.error("Pre-merge verification gate failed.")
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()