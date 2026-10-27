"""
Pipeline Autonomic Cross-Worktree Concurrency Deconfliction & Distributed Fencing Coordinator.

Implements mathematical resource collision matrices (C and C*), AST blast-radius
transitive caller dependency tracking, monotonic distributed fencing token coordination (nu_k),
dynamic Wait-For Graph (WFG) maintenance, Tarjan strongly connected components (SCC) deadlock
cycle detection and preemption, reader-writer hierarchical locking, and topological execution
tier barriers for multi-agent autonomous engineering pipelines.
"""

from __future__ import annotations

import ast
from dataclasses import asdict, dataclass, field
from enum import Enum
import json
import os
from pathlib import Path
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import uuid


# ==============================================================================
# Core Architecture & Enums
# ==============================================================================

class LockMode(str, Enum):
    """Concurrency lock access modes."""
    SHARED_READ = "SHARED_READ"
    EXCLUSIVE_WRITE = "EXCLUSIVE_WRITE"


class LockStatus(str, Enum):
    """Lifecycle states of a resource lease or lock request."""
    GRANTED = "GRANTED"
    WAITING = "WAITING"
    TIMED_OUT = "TIMED_OUT"
    DEADLOCK_PREEMPTED = "DEADLOCK_PREEMPTED"
    RELEASED = "RELEASED"


class ConflictSeverity(str, Enum):
    """Categorized collision severity for resource overlap."""
    NONE = "NONE"
    READ_WRITE = "READ_WRITE"
    WRITE_WRITE = "WRITE_WRITE"
    AST_BLAST_RADIUS_COLLISION = "AST_BLAST_RADIUS_COLLISION"


class PreemptionPolicy(str, Enum):
    """Victim selection policies for deadlock resolution."""
    LOWEST_PRIORITY = "LOWEST_PRIORITY"
    YOUNGEST_LEASE = "YOUNGEST_LEASE"
    LOWEST_TOKEN_BURN = "LOWEST_TOKEN_BURN"


# ==============================================================================
# Exceptions
# ==============================================================================

class ConcurrencyException(Exception):
    """Base exception for concurrency deconfliction operations."""
    pass


class StaleFencingTokenException(ConcurrencyException):
    """Raised when an operation presents an expired, superseded, or invalidated fencing token."""
    pass


class DeadlockPreemptionException(ConcurrencyException):
    """Raised when an acquisition request is aborted due to deadlock victim preemption."""
    pass


class LockTimeoutException(ConcurrencyException):
    """Raised when lock acquisition exceeds the configured timeout threshold."""
    pass


# ==============================================================================
# Dataclasses
# ==============================================================================

@dataclass
class FencingToken:
    """
    Monotonically increasing distributed fencing token (nu_k).
    Guarantees causal linearizability and epoch fence protection across worktrees.
    """
    worker_id: str
    token_value: int  # Monotonic nu_k
    issued_at: float
    expires_at: float
    worktree_path: str
    is_valid: bool = True

    def is_expired(self, now: Optional[float] = None) -> bool:
        current_time = now if now is not None else time.time()
        return current_time > self.expires_at

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ResourceLease:
    """Active lease held on one or more workspace resources."""
    lease_id: str
    worker_id: str
    resource_paths: List[str]
    lock_mode: LockMode
    fencing_token: FencingToken
    status: LockStatus
    priority: int = 10  # Higher value indicates higher scheduling priority
    accumulated_token_burn: float = 0.0
    created_at: float = field(default_factory=time.time)
    expires_at: float = field(default_factory=lambda: time.time() + 300.0)
    last_heartbeat: float = field(default_factory=time.time)

    def is_expired(self, now: Optional[float] = None) -> bool:
        current_time = now if now is not None else time.time()
        return current_time > self.expires_at

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["lock_mode"] = self.lock_mode.value
        data["status"] = self.status.value
        return data


@dataclass
class ConflictRecord:
    """Record of an identified collision between two worker scopes."""
    timestamp: float
    worker_a: str
    worker_b: str
    severity: ConflictSeverity
    colliding_resources: List[str]
    ast_caller_overlap: List[str]
    resolution: str

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["severity"] = self.severity.value
        return data


@dataclass
class DeadlockDiagnostic:
    """Diagnostic details of a detected and resolved deadlock cycle."""
    detected_at: float
    cycle: List[str]
    victim_worker_id: str
    preemption_policy: str
    reason: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ConcurrencyStateSnapshot:
    """Complete serialization snapshot of the concurrency coordination state."""
    epoch_token: int
    active_leases: List[Dict[str, Any]]
    waiting_requests: List[Dict[str, Any]]
    conflict_history: List[Dict[str, Any]]
    deadlocks_resolved: List[Dict[str, Any]]
    generated_at: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ==============================================================================
# AST Blast-Radius Conflict Analyzer
# ==============================================================================

class ASTBlastRadiusAnalyzer:
    """
    Parses Python Abstract Syntax Trees (AST) to compute transitive symbol references
    and caller graphs across worktree files. Extends conflict detection from physical
    path equality to architectural symbol blast-radius overlap:
    B(w_i) = TransitiveCallers(W_set(w_i)).
    """

    def __init__(self, root_dir: Optional[str] = None):
        self.root_dir = Path(root_dir).resolve() if root_dir else None
        # Maps file path string -> Set of defined symbol names
        self.file_definitions: Dict[str, Set[str]] = {}
        # Maps file path string -> Set of imported module names or symbols
        self.file_imports: Dict[str, Set[str]] = {}
        # Maps file path string -> Set of referenced / called symbol names
        self.file_references: Dict[str, Set[str]] = {}
        # Direct callers: target_file -> set of caller files
        self.caller_graph: Dict[str, Set[str]] = {}

    def _normalize_path(self, path: Union[str, Path]) -> str:
        p = Path(path)
        if self.root_dir and not p.is_absolute():
            p = (self.root_dir / p).resolve()
        return str(p.resolve())

    def analyze_source(self, file_path: str, source_code: str) -> None:
        """Parse source code in-memory and register AST symbols and dependencies."""
        norm_path = self._normalize_path(file_path)
        defined: Set[str] = set()
        imports: Set[str] = set()
        references: Set[str] = set()

        try:
            tree = ast.parse(source_code, filename=norm_path)
        except SyntaxError:
            self.file_definitions[norm_path] = defined
            self.file_imports[norm_path] = imports
            self.file_references[norm_path] = references
            return

        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                defined.add(node.name)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    imports.add(alias.name)
            elif isinstance(node, ast.ImportFrom):
                mod = node.module or ""
                imports.add(mod)
                for alias in node.names:
                    imports.add(f"{mod}.{alias.name}" if mod else alias.name)
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    references.add(node.func.id)
                elif isinstance(node.func, ast.Attribute):
                    references.add(node.func.attr)

        self.file_definitions[norm_path] = defined
        self.file_imports[norm_path] = imports
        self.file_references[norm_path] = references
        self._rebuild_caller_graph()

    def analyze_disk_tree(self, target_dir: Optional[str] = None) -> None:
        """Scan physical directory tree for Python files and build AST caller indices."""
        scan_dir = Path(target_dir).resolve() if target_dir else self.root_dir
        if not scan_dir or not scan_dir.exists():
            return

        for py_path in scan_dir.rglob("*.py"):
            try:
                content = py_path.read_text(encoding="utf-8", errors="replace")
                self.analyze_source(str(py_path), content)
            except Exception:
                continue

    def _rebuild_caller_graph(self) -> None:
        """Reconstruct bidirectional caller mappings across analyzed modules."""
        self.caller_graph = {f: set() for f in self.file_definitions}
        files = list(self.file_definitions.keys())

        for caller_file in files:
            caller_refs = self.file_references.get(caller_file, set())
            caller_imps = self.file_imports.get(caller_file, set())

            for target_file in files:
                if caller_file == target_file:
                    continue

                target_defs = self.file_definitions.get(target_file, set())
                target_stem = Path(target_file).stem

                # Check if caller references target symbols or imports module stem
                has_symbol_overlap = bool(caller_refs.intersection(target_defs))
                has_import_overlap = any(
                    target_stem == imp or imp.startswith(f"{target_stem}.")
                    for imp in caller_imps
                )

                if has_symbol_overlap or has_import_overlap:
                    self.caller_graph.setdefault(target_file, set()).add(caller_file)

    def compute_transitive_callers(self, target_files: List[str]) -> Set[str]:
        """
        Calculates B(files) = TransitiveCallers(files) using breadth-first traversal
        over the inverted AST caller graph.
        """
        transitive_callers: Set[str] = set()
        queue: List[str] = [self._normalize_path(f) for f in target_files]
        visited: Set[str] = set(queue)

        while queue:
            curr = queue.pop(0)
            direct_callers = self.caller_graph.get(curr, set())
            for caller in direct_callers:
                if caller not in visited:
                    visited.add(caller)
                    transitive_callers.add(caller)
                    queue.append(caller)

        return transitive_callers


# ==============================================================================
# Monotonic Distributed Fencing Coordinator
# ==============================================================================

class DistributedFencingCoordinator:
    """
    Issues strictly monotonically increasing fencing tokens (nu_k = nu_{k-1} + 1).
    Persists epoch counters atomically to disk and enforces linearizable lease
    verification at worktree commit boundaries.
    """

    def __init__(self, state_file_path: Optional[str] = None):
        self._lock = threading.RLock()
        self.state_file = Path(state_file_path).resolve() if state_file_path else None
        self._current_epoch: int = 0
        self._tokens: Dict[int, FencingToken] = {}
        self._worker_active_token: Dict[str, int] = {}
        self._load_epoch()

    def _load_epoch(self) -> None:
        if self.state_file and self.state_file.exists():
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self._current_epoch = int(data.get("latest_epoch", 0))
            except Exception:
                self._current_epoch = 0

    def _persist_epoch(self) -> None:
        if not self.state_file:
            return
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        tmp_file = self.state_file.with_suffix(".tmp")
        try:
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump({"latest_epoch": self._current_epoch, "updated_at": time.time()}, f, indent=2)
            tmp_file.replace(self.state_file)
        except Exception:
            if tmp_file.exists():
                tmp_file.unlink()

    def issue_token(self, worker_id: str, worktree_path: str, ttl_sec: float = 300.0) -> FencingToken:
        """Issues monotonically incremented fencing token nu_k."""
        with self._lock:
            self._current_epoch += 1
            now = time.time()
            token = FencingToken(
                worker_id=worker_id,
                token_value=self._current_epoch,
                issued_at=now,
                expires_at=now + ttl_sec,
                worktree_path=worktree_path,
                is_valid=True
            )
            self._tokens[token.token_value] = token
            self._worker_active_token[worker_id] = token.token_value
            self._persist_epoch()
            return token

    def validate_token(self, token: FencingToken, expected_min_epoch: Optional[int] = None) -> bool:
        """
        Validates fencing token validity. Rejects stale or preempted tokens
        with StaleFencingTokenException.
        """
        with self._lock:
            stored = self._tokens.get(token.token_value)
            if not stored or not stored.is_valid or not token.is_valid:
                raise StaleFencingTokenException(
                    f"Token {token.token_value} for worker {token.worker_id} has been invalidated or revoked."
                )

            if token.is_expired():
                raise StaleFencingTokenException(
                    f"Fencing token {token.token_value} for worker {token.worker_id} expired at {token.expires_at}."
                )

            if expected_min_epoch is not None and token.token_value < expected_min_epoch:
                raise StaleFencingTokenException(
                    f"Stale fencing token: presented epoch {token.token_value} < required {expected_min_epoch}."
                )

            # Ensure worker has not been superseded by a higher epoch token
            active_val = self._worker_active_token.get(token.worker_id)
            if active_val is not None and token.token_value < active_val:
                raise StaleFencingTokenException(
                    f"Stale worker token: token {token.token_value} superseded by active token {active_val}."
                )

            return True

    def heartbeat(self, worker_id: str, token_value: int, extend_sec: float = 300.0) -> bool:
        """Extends token validity TTL."""
        with self._lock:
            token = self._tokens.get(token_value)
            if token and token.worker_id == worker_id and token.is_valid:
                token.expires_at = time.time() + extend_sec
                return True
            return False

    def invalidate_token(self, token_value: int) -> None:
        """Revokes token validity."""
        with self._lock:
            if token_value in self._tokens:
                self._tokens[token_value].is_valid = False

    @property
    def current_epoch(self) -> int:
        with self._lock:
            return self._current_epoch


# ==============================================================================
# Tarjan SCC Deadlock Detector & Wait-For Graph (WFG)
# ==============================================================================

class WaitForGraph:
    """
    Maintains dynamic Wait-For Graph G_wait = (W, E_wait).
    Executes Tarjan strongly connected components (SCC) algorithm to identify
    closed dependency cycles and select victims for autonomic preemption.
    """

    def __init__(self):
        self._lock = threading.RLock()
        # Directed edge w_i -> w_j: w_i is blocked waiting on a resource held by w_j
        self._adj: Dict[str, Set[str]] = {}

    def add_wait_edge(self, waiter: str, holder: str) -> None:
        with self._lock:
            if waiter == holder:
                return
            self._adj.setdefault(waiter, set()).add(holder)
            self._adj.setdefault(holder, set())

    def remove_wait_edge(self, waiter: str, holder: str) -> None:
        with self._lock:
            if waiter in self._adj and holder in self._adj[waiter]:
                self._adj[waiter].remove(holder)

    def remove_worker(self, worker_id: str) -> None:
        with self._lock:
            if worker_id in self._adj:
                del self._adj[worker_id]
            for edges in self._adj.values():
                edges.discard(worker_id)

    def get_edges(self) -> List[Tuple[str, str]]:
        with self._lock:
            edges = []
            for u, neighbors in self._adj.items():
                for v in neighbors:
                    edges.append((u, v))
            return edges

    def detect_cycles_tarjan(self) -> List[List[str]]:
        """
        Executes Tarjan's SCC algorithm on the wait-for graph.
        Returns all strongly connected components with length > 1, representing deadlocks.
        """
        with self._lock:
            index = 0
            indices: Dict[str, int] = {}
            lowlinks: Dict[str, int] = {}
            on_stack: Dict[str, bool] = {}
            stack: List[str] = []
            cycles: List[List[str]] = []

            def strongconnect(node: str) -> None:
                nonlocal index
                indices[node] = index
                lowlinks[node] = index
                index += 1
                stack.append(node)
                on_stack[node] = True

                for neighbor in self._adj.get(node, set()):
                    if neighbor not in indices:
                        strongconnect(neighbor)
                        lowlinks[node] = min(lowlinks[node], lowlinks[neighbor])
                    elif on_stack.get(neighbor, False):
                        lowlinks[node] = min(lowlinks[node], indices[neighbor])

                if lowlinks[node] == indices[node]:
                    scc: List[str] = []
                    while True:
                        w = stack.pop()
                        on_stack[w] = False
                        scc.append(w)
                        if w == node:
                            break

                    # An SCC with >1 nodes is a multi-worker deadlock cycle
                    if len(scc) > 1:
                        cycles.append(scc)
                    # Handle direct self-dependency loop if node waits on itself
                    elif len(scc) == 1 and node in self._adj.get(node, set()):
                        cycles.append(scc)

            for vertex in list(self._adj.keys()):
                if vertex not in indices:
                    strongconnect(vertex)

            return cycles


# ==============================================================================
# Hierarchical Concurrency Lock Manager & Deconfliction Coordinator
# ==============================================================================

class PendingRequest:
    """Internal representation of a thread/worker waiting for a lock acquisition."""

    def __init__(
        self,
        request_id: str,
        worker_id: str,
        resource_paths: List[str],
        mode: LockMode,
        priority: int,
        accumulated_token_burn: float,
        timeout_sec: float,
        worktree_path: str
    ):
        self.request_id = request_id
        self.worker_id = worker_id
        self.resource_paths = resource_paths
        self.mode = mode
        self.priority = priority
        self.accumulated_token_burn = accumulated_token_burn
        self.timeout_sec = timeout_sec
        self.worktree_path = worktree_path
        self.requested_at = time.time()
        self.condition = threading.Condition()
        self.granted_lease: Optional[ResourceLease] = None
        self.status = LockStatus.WAITING


class PipelineConcurrencyDeconflictionCoordinator:
    """
    Central autonomic coordinator for:
    1. Hierarchical reader-writer path locking.
    2. Dynamic Wait-For Graph (WFG) maintenance and Tarjan SCC deadlock resolution.
    3. Pairwise resource collision matrix C and AST blast-radius extended matrix C*.
    4. State persistence to dashboards/concurrency_locks.json and CONCURRENCY_COCKPIT.md.
    """

    def __init__(
        self,
        fencing_coordinator: Optional[DistributedFencingCoordinator] = None,
        ast_analyzer: Optional[ASTBlastRadiusAnalyzer] = None,
        state_dir: str = "dashboards",
        preemption_policy: PreemptionPolicy = PreemptionPolicy.LOWEST_PRIORITY
    ):
        self._lock = threading.RLock()
        self.state_dir = Path(state_dir).resolve()
        self.state_dir.mkdir(parents=True, exist_ok=True)

        self.fencing = fencing_coordinator or DistributedFencingCoordinator(
            str(self.state_dir / "fencing_epoch.json")
        )
        self.ast_analyzer = ast_analyzer or ASTBlastRadiusAnalyzer()
        self.wfg = WaitForGraph()
        self.preemption_policy = preemption_policy

        # Active leases: lease_id -> ResourceLease
        self._active_leases: Dict[str, ResourceLease] = {}
        # Resource path -> set of active lease_ids
        self._resource_leases: Dict[str, Set[str]] = {}
        # Pending requests waiting in queue
        self._pending_requests: List[PendingRequest] = []

        self._conflict_history: List[ConflictRecord] = []
        self._deadlock_history: List[DeadlockDiagnostic] = []

    def _paths_collide(self, path_a: str, path_b: str) -> bool:
        """Hierarchical prefix path collision check."""
        norm_a = os.path.normpath(path_a)
        norm_b = os.path.normpath(path_b)
        if norm_a == norm_b:
            return True
        if norm_b.startswith(norm_a + os.sep):
            return True
        if norm_a.startswith(norm_b + os.sep):
            return True
        return False

    def _has_lease_conflict(
        self,
        candidate_paths: List[str],
        candidate_mode: LockMode,
        candidate_worker_id: str
    ) -> Tuple[bool, List[str], Set[str]]:
        """
        Evaluates whether candidate resource paths conflict with currently held leases.
        Returns: (has_conflict, colliding_paths, conflicting_lease_ids).
        """
        colliding_paths: List[str] = []
        conflicting_leases: Set[str] = set()

        for lease in self._active_leases.values():
            if lease.worker_id == candidate_worker_id:
                # Same worker re-entry: allowed for matching or upgraded requests
                continue

            for cand_path in candidate_paths:
                for held_path in lease.resource_paths:
                    if self._paths_collide(cand_path, held_path):
                        # Read-Read never conflicts
                        if candidate_mode == LockMode.SHARED_READ and lease.lock_mode == LockMode.SHARED_READ:
                            continue
                        colliding_paths.append(f"{cand_path} <-> {held_path}")
                        conflicting_leases.add(lease.lease_id)

        return (len(conflicting_leases) > 0), colliding_paths, conflicting_leases

    def compute_collision_matrix(
        self,
        workers: List[Dict[str, Any]]
    ) -> Tuple[List[str], List[List[int]], List[List[int]], List[ConflictRecord]]:
        """
        Computes pairwise resource collision matrix C and AST blast-radius extended C*.
        Let W = {w_1, ..., w_M}.
        C_{ij} = 1 iff (W_set(w_i) cap W_set(w_j) != 0) or (W_set(w_i) cap R_set(w_j) != 0) or ...
        C*_{ij} = C_{ij} or (W_set(w_i) cap B(w_j) != 0) or (W_set(w_j) cap B(w_i) != 0).
        """
        m = len(workers)
        worker_ids = [w["worker_id"] for w in workers]
        c_matrix = [[0 for _ in range(m)] for _ in range(m)]
        c_star_matrix = [[0 for _ in range(m)] for _ in range(m)]
        records: List[ConflictRecord] = []

        # Precompute AST blast radius for each worker's write set
        blast_radii: List[Set[str]] = []
        for w in workers:
            w_set = w.get("write_set", [])
            blast_radii.append(self.ast_analyzer.compute_transitive_callers(w_set))

        now = time.time()
        for i in range(m):
            w_i = workers[i]
            w_set_i = set(os.path.normpath(p) for p in w_i.get("write_set", []))
            r_set_i = set(os.path.normpath(p) for p in w_i.get("read_set", []))
            b_i = blast_radii[i]

            for j in range(m):
                if i == j:
                    continue

                w_j = workers[j]
                w_set_j = set(os.path.normpath(p) for p in w_j.get("write_set", []))
                r_set_j = set(os.path.normpath(p) for p in w_j.get("read_set", []))
                b_j = blast_radii[j]

                ww_overlap = w_set_i.intersection(w_set_j)
                wr_overlap = w_set_i.intersection(r_set_j)
                rw_overlap = r_set_i.intersection(w_set_j)

                has_direct = bool(ww_overlap or wr_overlap or rw_overlap)
                severity = ConflictSeverity.NONE
                colliding_res = []

                if ww_overlap:
                    severity = ConflictSeverity.WRITE_WRITE
                    colliding_res = list(ww_overlap)
                elif wr_overlap or rw_overlap:
                    severity = ConflictSeverity.READ_WRITE
                    colliding_res = list(wr_overlap.union(rw_overlap))

                if has_direct:
                    c_matrix[i][j] = 1
                    c_star_matrix[i][j] = 1

                # Extended AST blast radius collision
                ast_overlap_i = w_set_i.intersection(b_j)
                ast_overlap_j = w_set_j.intersection(b_i)
                ast_overlap = ast_overlap_i.union(ast_overlap_j)

                if ast_overlap and not has_direct:
                    c_star_matrix[i][j] = 1
                    severity = ConflictSeverity.AST_BLAST_RADIUS_COLLISION
                    colliding_res = list(ast_overlap)

                if c_star_matrix[i][j] == 1 and i < j:
                    records.append(ConflictRecord(
                        timestamp=now,
                        worker_a=worker_ids[i],
                        worker_b=worker_ids[j],
                        severity=severity,
                        colliding_resources=colliding_res,
                        ast_caller_overlap=list(ast_overlap),
                        resolution="Serialized via Distributed Lock Manager"
                    ))

        return worker_ids, c_matrix, c_star_matrix, records

    def acquire_lock(
        self,
        worker_id: str,
        resource_paths: List[str],
        mode: LockMode,
        priority: int = 10,
        timeout_sec: float = 30.0,
        worktree_path: str = "",
        accumulated_token_burn: float = 0.0
    ) -> ResourceLease:
        """
        Hierarchical reader-writer lock acquisition. Blocks calling thread until
        lease is granted, timeout occurs, or the request is preempted by deadlock resolution.
        """
        req_id = f"req-{uuid.uuid4().hex[:8]}"
        req = PendingRequest(
            request_id=req_id,
            worker_id=worker_id,
            resource_paths=resource_paths,
            mode=mode,
            priority=priority,
            accumulated_token_burn=accumulated_token_burn,
            timeout_sec=timeout_sec,
            worktree_path=worktree_path
        )

        with self._lock:
            self.reap_expired_leases()
            conflict, col_paths, conflicting_lease_ids = self._has_lease_conflict(resource_paths, mode, worker_id)

            if not conflict:
                # Grant immediately
                lease = self._create_granted_lease(req)
                self.persist_state()
                return lease

            # Lock contended: register wait edges in WFG
            for lid in conflicting_lease_ids:
                holder = self._active_leases[lid]
                self.wfg.add_wait_edge(worker_id, holder.worker_id)

            self._pending_requests.append(req)

            # Record conflict
            self._conflict_history.append(ConflictRecord(
                timestamp=time.time(),
                worker_a=worker_id,
                worker_b=",".join(self._active_leases[lid].worker_id for lid in conflicting_lease_ids),
                severity=ConflictSeverity.WRITE_WRITE if mode == LockMode.EXCLUSIVE_WRITE else ConflictSeverity.READ_WRITE,
                colliding_resources=col_paths,
                ast_caller_overlap=[],
                resolution="Queued in Wait-For Graph"
            ))

            # Trigger Tarjan deadlock cycle check
            self.detect_and_resolve_deadlocks()

        # Block outside the central lock using request condition
        with req.condition:
            deadline = time.time() + timeout_sec
            while req.status == LockStatus.WAITING:
                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                req.condition.wait(timeout=remaining)

            with self._lock:
                if req.status == LockStatus.GRANTED and req.granted_lease:
                    self.wfg.remove_worker(worker_id)
                    self.persist_state()
                    return req.granted_lease

                if req.status == LockStatus.DEADLOCK_PREEMPTED:
                    self.wfg.remove_worker(worker_id)
                    raise DeadlockPreemptionException(
                        f"Worker {worker_id} was preempted to resolve a detected deadlock cycle."
                    )

                # Timeout occurred
                req.status = LockStatus.TIMED_OUT
                if req in self._pending_requests:
                    self._pending_requests.remove(req)
                self.wfg.remove_worker(worker_id)
                self.persist_state()
                raise LockTimeoutException(
                    f"Lock acquisition timed out after {timeout_sec:.2f}s for worker {worker_id}."
                )

    def release_lock(self, lease_id: str) -> None:
        """Releases an active lease and evaluates pending waiters in queue."""
        with self._lock:
            lease = self._active_leases.get(lease_id)
            if not lease:
                return

            lease.status = LockStatus.RELEASED
            self.fencing.invalidate_token(lease.fencing_token.token_value)
            del self._active_leases[lease_id]

            for path in lease.resource_paths:
                if path in self._resource_leases:
                    self._resource_leases[path].discard(lease_id)
                    if not self._resource_leases[path]:
                        del self._resource_leases[path]

            self.wfg.remove_worker(lease.worker_id)
            self._evaluate_pending_requests()
            self.persist_state()

    def _create_granted_lease(self, req: PendingRequest) -> ResourceLease:
        """Internal helper to construct and index a granted lease."""
        token = self.fencing.issue_token(
            worker_id=req.worker_id,
            worktree_path=req.worktree_path,
            ttl_sec=req.timeout_sec * 10
        )
        lease_id = f"lease-{uuid.uuid4().hex[:8]}"
        lease = ResourceLease(
            lease_id=lease_id,
            worker_id=req.worker_id,
            resource_paths=req.resource_paths,
            lock_mode=req.mode,
            fencing_token=token,
            status=LockStatus.GRANTED,
            priority=req.priority,
            accumulated_token_burn=req.accumulated_token_burn,
            created_at=time.time(),
            expires_at=time.time() + 300.0,
            last_heartbeat=time.time()
        )
        self._active_leases[lease_id] = lease
        for path in req.resource_paths:
            self._resource_leases.setdefault(path, set()).add(lease_id)
        return lease

    def _evaluate_pending_requests(self) -> None:
        """Evaluates pending queue in order of priority and grant eligibility."""
        self._pending_requests.sort(key=lambda r: (r.priority, -r.requested_at), reverse=True)
        granted_indices = []

        for idx, req in enumerate(self._pending_requests):
            conflict, _, _ = self._has_lease_conflict(req.resource_paths, req.mode, req.worker_id)
            if not conflict:
                lease = self._create_granted_lease(req)
                req.granted_lease = lease
                req.status = LockStatus.GRANTED
                with req.condition:
                    req.condition.notify_all()
                granted_indices.append(idx)

        for idx in reversed(granted_indices):
            del self._pending_requests[idx]

    def reap_expired_leases(self) -> List[str]:
        """Automatically purges leases exceeding expiration TTL."""
        expired_ids = []
        now = time.time()
        for lease_id, lease in list(self._active_leases.items()):
            if lease.is_expired(now):
                expired_ids.append(lease_id)
                self.release_lock(lease_id)
        return expired_ids

    def detect_and_resolve_deadlocks(self) -> List[DeadlockDiagnostic]:
        """
        Executes Tarjan's SCC cycle detection over the Wait-For Graph.
        Selects and preempts victim workers based on active PreemptionPolicy.
        """
        diagnostics: List[DeadlockDiagnostic] = []
        cycles = self.wfg.detect_cycles_tarjan()

        for cycle in cycles:
            # Find eligible candidate requests or leases belonging to workers in the cycle
            cycle_workers = set(cycle)
            victim = self._select_victim(cycle_workers)
            if not victim:
                continue

            diag = DeadlockDiagnostic(
                detected_at=time.time(),
                cycle=cycle,
                victim_worker_id=victim,
                preemption_policy=self.preemption_policy.value,
                reason=f"Preempted cycle {cycle} via {self.preemption_policy.value}"
            )
            diagnostics.append(diag)
            self._deadlock_history.append(diag)

            # Preempt victim's pending requests
            for req in list(self._pending_requests):
                if req.worker_id == victim:
                    req.status = LockStatus.DEADLOCK_PREEMPTED
                    with req.condition:
                        req.condition.notify_all()
                    self._pending_requests.remove(req)

            # Revoke any leases already held by victim
            for lease_id, lease in list(self._active_leases.items()):
                if lease.worker_id == victim:
                    self.release_lock(lease_id)

            self.wfg.remove_worker(victim)

        if diagnostics:
            self.persist_state()

        return diagnostics

    def _select_victim(self, candidate_worker_ids: Set[str]) -> Optional[str]:
        """Applies preemption policy to select a victim worker from a deadlock cycle."""
        matching_reqs = [r for r in self._pending_requests if r.worker_id in candidate_worker_ids]
        matching_leases = [l for l in self._active_leases.values() if l.worker_id in candidate_worker_ids]

        if not matching_reqs and not matching_leases:
            return next(iter(candidate_worker_ids)) if candidate_worker_ids else None

        if self.preemption_policy == PreemptionPolicy.LOWEST_PRIORITY:
            # Pick worker with minimum priority among pending requests
            if matching_reqs:
                matching_reqs.sort(key=lambda r: (r.priority, r.accumulated_token_burn))
                return matching_reqs[0].worker_id
            if matching_leases:
                matching_leases.sort(key=lambda l: (l.priority, l.accumulated_token_burn))
                return matching_leases[0].worker_id

        elif self.preemption_policy == PreemptionPolicy.YOUNGEST_LEASE:
            # Pick request/lease with most recent creation timestamp
            if matching_reqs:
                matching_reqs.sort(key=lambda r: r.requested_at, reverse=True)
                return matching_reqs[0].worker_id
            if matching_leases:
                matching_leases.sort(key=lambda l: l.created_at, reverse=True)
                return matching_leases[0].worker_id

        elif self.preemption_policy == PreemptionPolicy.LOWEST_TOKEN_BURN:
            # Pick worker with least invested token burn
            if matching_reqs:
                matching_reqs.sort(key=lambda r: r.accumulated_token_burn)
                return matching_reqs[0].worker_id
            if matching_leases:
                matching_leases.sort(key=lambda l: l.accumulated_token_burn)
                return matching_leases[0].worker_id

        return next(iter(candidate_worker_ids))

    def persist_state(self) -> None:
        """Saves concurrency snapshot JSON and exports Cockpit Markdown."""
        snapshot = ConcurrencyStateSnapshot(
            epoch_token=self.fencing.current_epoch,
            active_leases=[l.to_dict() for l in self._active_leases.values()],
            waiting_requests=[
                {
                    "request_id": r.request_id,
                    "worker_id": r.worker_id,
                    "resource_paths": r.resource_paths,
                    "mode": r.mode.value,
                    "priority": r.priority,
                    "status": r.status.value,
                    "requested_at": r.requested_at
                }
                for r in self._pending_requests
            ],
            conflict_history=[c.to_dict() for c in self._conflict_history[-50:]],
            deadlocks_resolved=[d.to_dict() for d in self._deadlock_history[-50:]]
        )

        json_path = self.state_dir / "concurrency_locks.json"
        tmp_json = json_path.with_suffix(".tmp")
        try:
            with open(tmp_json, "w", encoding="utf-8") as f:
                json.dump(snapshot.to_dict(), f, indent=2)
            tmp_json.replace(json_path)
        except Exception:
            if tmp_json.exists():
                tmp_json.unlink()

        self._export_cockpit_markdown()

    def _export_cockpit_markdown(self) -> None:
        """Generates dashboards/CONCURRENCY_COCKPIT.md with ASCII tables and WFG state."""
        cockpit_path = self.state_dir / "CONCURRENCY_COCKPIT.md"
        now_str = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
        edges = self.wfg.get_edges()

        lines = [
            "# CONCURRENCY & FENCING COORDINATION COCKPIT",
            f"**Generated:** {now_str}  |  **Fencing Epoch (nu):** `{self.fencing.current_epoch}`  |  **Policy:** `{self.preemption_policy.value}`",
            "",
            "## 1. System Health & Summary Telemetry",
            "",
            "| Metric | Value | Status |",
            "| :--- | :--- | :--- |",
            f"| Active Leases | `{len(self._active_leases)}` | {'ACTIVE' if self._active_leases else 'IDLE'} |",
            f"| Blocked Waiters | `{len(self._pending_requests)}` | {'CONTENTION' if self._pending_requests else 'CLEAR'} |",
            f"| Wait-For Graph Edges | `{len(edges)}` | {'BLOCKED' if edges else 'UNCONSTRAINED'} |",
            f"| Total Conflicts Handled | `{len(self._conflict_history)}` | {'RECORDED' if self._conflict_history else 'NONE'} |",
            f"| Deadlock Cycles Resolved | `{len(self._deadlock_history)}` | {'ALERT' if self._deadlock_history else 'CLEAN'} |",
            "",
            "## 2. Active Resource Leases & Fencing Tokens",
            "",
        ]

        if self._active_leases:
            lines.extend([
                "| Lease ID | Worker ID | Mode | Priority | Fencing Token (nu) | Resources | Expires In |",
                "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |"
            ])
            now = time.time()
            for l in self._active_leases.values():
                ttl_left = max(0.0, l.expires_at - now)
                res_str = ", ".join(l.resource_paths)
                lines.append(
                    f"| `{l.lease_id}` | `{l.worker_id}` | `{l.lock_mode.value}` | `{l.priority}` | `{l.fencing_token.token_value}` | `{res_str}` | `{ttl_left:.1f}s` |"
                )
        else:
            lines.append("*No active resource leases currently held.*")

        lines.extend([
            "",
            "## 3. Dynamic Wait-For Graph (WFG) & Contention",
            ""
        ])

        if edges:
            lines.extend([
                "```text",
                "+---------------------------------------------------------------+",
                "|               DYNAMIC WAIT-FOR GRAPH DEPENDENCIES             |",
                "+---------------------------------------------------------------+"
            ])
            for u, v in edges:
                lines.append(f"  [Worker {u}] ----> BLOCKED_BY ----> [Worker {v}]")
            lines.extend([
                "+---------------------------------------------------------------+",
                "```"
            ])
        else:
            lines.append("```text",)
            lines.append("[WFG] (empty) - No active blocking dependencies.")
            lines.append("```")

        lines.extend([
            "",
            "## 4. Conflict Resolution & Preemption Audit Log",
            ""
        ])

        if self._conflict_history:
            lines.extend([
                "| Timestamp | Worker A | Worker B | Severity | Colliding Resources | Resolution |",
                "| :--- | :--- | :--- | :--- | :--- | :--- |"
            ])
            for c in reversed(self._conflict_history[-10:]):
                t_str = time.strftime("%H:%M:%S", time.gmtime(c.timestamp))
                res = ", ".join(c.colliding_resources) if c.colliding_resources else "N/A"
                lines.append(
                    f"| `{t_str}` | `{c.worker_a}` | `{c.worker_b}` | `{c.severity.value}` | `{res}` | {c.resolution} |"
                )
        else:
            lines.append("*No conflicts recorded in current epoch session.*")

        lines.append("")
        content = "\n".join(lines)
        tmp_cockpit = cockpit_path.with_suffix(".tmp")
        try:
            with open(tmp_cockpit, "w", encoding="utf-8") as f:
                f.write(content)
            tmp_cockpit.replace(cockpit_path)
        except Exception:
            if tmp_cockpit.exists():
                tmp_cockpit.unlink()


# ==============================================================================
# Topological Tier Barrier Synchronization
# ==============================================================================

class TopologicalBarrier:
    """
    Coordinates execution DAG tiers. Blocks dependent downstream workers
    until all upstream tier workers have verified tasks and released exclusive leases.
    """

    def __init__(self, coordinator: Optional[PipelineConcurrencyDeconflictionCoordinator] = None):
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self.coordinator = coordinator
        # Maps tier_id (int) -> Set of registered worker_ids
        self._tier_expected: Dict[int, Set[str]] = {}
        # Maps tier_id (int) -> Set of completed worker_ids
        self._tier_completed: Dict[int, Set[str]] = {}
        # Maps tier_id (int) -> success boolean status
        self._tier_success: Dict[int, bool] = {}

    def register_tier(self, tier_id: int, expected_worker_ids: List[str]) -> None:
        """Registers expected workers for an execution tier."""
        with self._lock:
            self._tier_expected[tier_id] = set(expected_worker_ids)
            self._tier_completed.setdefault(tier_id, set())
            self._tier_success.setdefault(tier_id, True)

    def worker_finished(
        self,
        tier_id: int,
        worker_id: str,
        success: bool = True,
        held_lease_id: Optional[str] = None
    ) -> None:
        """
        Marks worker completion in tier. Optionally releases held lease and
        signals waiting downstream tiers if the tier is fully unlocked.
        """
        with self._lock:
            if held_lease_id and self.coordinator:
                self.coordinator.release_lock(held_lease_id)

            self._tier_completed.setdefault(tier_id, set()).add(worker_id)
            if not success:
                self._tier_success[tier_id] = False

            if self.is_tier_complete(tier_id):
                self._condition.notify_all()

    def is_tier_complete(self, tier_id: int) -> bool:
        """Checks if all registered workers for a tier have finished."""
        with self._lock:
            expected = self._tier_expected.get(tier_id, set())
            completed = self._tier_completed.get(tier_id, set())
            return expected.issubset(completed)

    def wait_for_tier(self, tier_id: int, timeout_sec: float = 60.0) -> bool:
        """
        Blocks caller until all upstream workers in specified tier have completed.
        Returns True if tier completed successfully, False otherwise or on timeout.
        """
        deadline = time.time() + timeout_sec
        with self._lock:
            while not self.is_tier_complete(tier_id):
                remaining = deadline - time.time()
                if remaining <= 0:
                    return False
                self._condition.wait(timeout=remaining)
            return self._tier_success.get(tier_id, True)


# ==============================================================================
# Self-Test Verification Suite
# ==============================================================================

def run_self_tests() -> None:
    """Executes full unit verification suite for concurrency deconfliction."""
    print("=" * 70)
    print("RUNNING CONCURRENCY DECONFLICTION & FENCING COORDINATOR SELF-TESTS")
    print("=" * 70)

    test_state_dir = Path("dashboards/test_concurrency")
    test_state_dir.mkdir(parents=True, exist_ok=True)

    # --------------------------------------------------------------------------
    # 1. Monotonic Fencing Token Tests
    # --------------------------------------------------------------------------
    print("[TEST 1] Fencing Token Issuance, Monotonicity & Validation...")
    fencing = DistributedFencingCoordinator(str(test_state_dir / "fencing_epoch.json"))
    t1 = fencing.issue_token(worker_id="agent-01", worktree_path="/tmp/worktree1", ttl_sec=60.0)
    t2 = fencing.issue_token(worker_id="agent-02", worktree_path="/tmp/worktree2", ttl_sec=60.0)

    assert t2.token_value > t1.token_value, f"Epoch not strictly monotonic: {t2.token_value} <= {t1.token_value}"
    assert fencing.validate_token(t2), "Valid token was rejected"

    # Invalidate t1 and check rejection
    fencing.invalidate_token(t1.token_value)
    try:
        fencing.validate_token(t1)
        assert False, "Invalidated token did not raise StaleFencingTokenException"
    except StaleFencingTokenException:
        pass
    print("  -> PASSED: Monotonic tokens and stale rejection verified.")

    # --------------------------------------------------------------------------
    # 2. AST Blast Radius & Collision Matrix Tests
    # --------------------------------------------------------------------------
    print("[TEST 2] AST Blast Radius Transitive Overlap & Matrix C*...")
    ast_analyzer = ASTBlastRadiusAnalyzer()

    code_lib = """
def compute_metrics(x, y):
    return x * y + 42

class PipelineGovernor:
    pass
"""
    code_caller = """
from lib import compute_metrics, PipelineGovernor

def execute_stage():
    val = compute_metrics(10, 2)
    return val
"""
    code_unrelated = """
def standalone_task():
    return True
"""
    ast_analyzer.analyze_source("src/lib.py", code_lib)
    ast_analyzer.analyze_source("src/caller.py", code_caller)
    ast_analyzer.analyze_source("src/other.py", code_unrelated)

    callers_of_lib = ast_analyzer.compute_transitive_callers(["src/lib.py"])
    assert any("caller.py" in c for c in callers_of_lib), f"Transitive caller not found: {callers_of_lib}"
    assert not any("other.py" in c for c in callers_of_lib), "Unrelated file incorrectly marked as caller"

    coordinator = PipelineConcurrencyDeconflictionCoordinator(
        fencing_coordinator=fencing,
        ast_analyzer=ast_analyzer,
        state_dir=str(test_state_dir)
    )

    workers_payload = [
        {"worker_id": "w1", "write_set": ["src/lib.py"], "read_set": []},
        {"worker_id": "w2", "write_set": ["src/other.py"], "read_set": ["src/caller.py"]},
        {"worker_id": "w3", "write_set": ["src/unrelated_b.py"], "read_set": []}
    ]
    ids, C, C_star, conflicts = coordinator.compute_collision_matrix(workers_payload)
    assert C[0][1] == 0, "Direct collision detected where only AST overlap exists"
    assert C_star[0][1] == 1, "AST Blast Radius collision not detected in C*"
    print("  -> PASSED: AST transitive blast-radius detection verified.")

    # --------------------------------------------------------------------------
    # 3. Hierarchical Lock Contention (Shared vs Exclusive)
    # --------------------------------------------------------------------------
    print("[TEST 3] Reader-Writer Hierarchical Lock Contention...")
    # Acquire Shared Read on directory
    l_read1 = coordinator.acquire_lock("w_read1", ["src/models/"], LockMode.SHARED_READ, timeout_sec=5.0)
    l_read2 = coordinator.acquire_lock("w_read2", ["src/models/user.py"], LockMode.SHARED_READ, timeout_sec=5.0)
    assert l_read1.status == LockStatus.GRANTED and l_read2.status == LockStatus.GRANTED

    # Exclusive write on subfile should time out while reads are held
    write_timed_out = False
    try:
        coordinator.acquire_lock("w_write", ["src/models/user.py"], LockMode.EXCLUSIVE_WRITE, timeout_sec=0.5)
    except LockTimeoutException:
        write_timed_out = True
    assert write_timed_out, "Exclusive write should have timed out against active shared reads"

    coordinator.release_lock(l_read1.lease_id)
    coordinator.release_lock(l_read2.lease_id)

    # Now exclusive write must succeed
    l_write = coordinator.acquire_lock("w_write", ["src/models/user.py"], LockMode.EXCLUSIVE_WRITE, timeout_sec=2.0)
    assert l_write.status == LockStatus.GRANTED
    coordinator.release_lock(l_write.lease_id)
    print("  -> PASSED: Reader-Writer hierarchical locking verified.")

    # --------------------------------------------------------------------------
    # 4. Tarjan SCC Deadlock Cycle Detection & Preemption
    # --------------------------------------------------------------------------
    print("[TEST 4] Tarjan SCC Deadlock Cycle Detection & Preemption...")
    # Construct artificial cycle in WaitForGraph: A -> B -> C -> A
    wfg = coordinator.wfg
    wfg.add_wait_edge("worker_A", "worker_B")
    wfg.add_wait_edge("worker_B", "worker_C")
    wfg.add_wait_edge("worker_C", "worker_A")

    cycles = wfg.detect_cycles_tarjan()
    assert len(cycles) == 1, f"Expected 1 deadlock cycle, got {len(cycles)}"
    assert set(cycles[0]) == {"worker_A", "worker_B", "worker_C"}

    # Register mock pending requests to test victim selection
    coordinator.preemption_policy = PreemptionPolicy.LOWEST_PRIORITY
    req_a = PendingRequest("rA", "worker_A", ["resA"], LockMode.EXCLUSIVE_WRITE, priority=20, accumulated_token_burn=100.0, timeout_sec=10.0, worktree_path="")
    req_b = PendingRequest("rB", "worker_B", ["resB"], LockMode.EXCLUSIVE_WRITE, priority=5, accumulated_token_burn=50.0, timeout_sec=10.0, worktree_path="")
    req_c = PendingRequest("rC", "worker_C", ["resC"], LockMode.EXCLUSIVE_WRITE, priority=15, accumulated_token_burn=80.0, timeout_sec=10.0, worktree_path="")

    coordinator._pending_requests.extend([req_a, req_b, req_c])
    diags = coordinator.detect_and_resolve_deadlocks()
    assert len(diags) == 1, "Deadlock was not resolved"
    assert diags[0].victim_worker_id == "worker_B", f"Expected worker_B to be victim, got {diags[0].victim_worker_id}"
    assert req_b.status == LockStatus.DEADLOCK_PREEMPTED
    print("  -> PASSED: Tarjan SCC deadlock detection and preemption verified.")

    # Clean up mock requests
    coordinator._pending_requests.clear()
    wfg.remove_worker("worker_A")
    wfg.remove_worker("worker_B")
    wfg.remove_worker("worker_C")

    # --------------------------------------------------------------------------
    # 5. Topological Tier Barrier Synchronization
    # --------------------------------------------------------------------------
    print("[TEST 5] Topological Tier Barrier Synchronization...")
    barrier = TopologicalBarrier(coordinator=coordinator)
    barrier.register_tier(tier_id=0, expected_worker_ids=["worker_tier0_1", "worker_tier0_2"])

    def worker_thread(tid: int, wid: str, delay: float):
        time.sleep(delay)
        barrier.worker_finished(tier_id=tid, worker_id=wid, success=True)

    t_worker1 = threading.Thread(target=worker_thread, args=(0, "worker_tier0_1", 0.1))
    t_worker2 = threading.Thread(target=worker_thread, args=(0, "worker_tier0_2", 0.2))

    t_worker1.start()
    t_worker2.start()

    unlocked = barrier.wait_for_tier(tier_id=0, timeout_sec=2.0)
    t_worker1.join()
    t_worker2.join()

    assert unlocked, "Tier barrier timed out unexpectedly"
    assert barrier.is_tier_complete(0), "Tier 0 should be marked complete"
    print("  -> PASSED: Topological barrier synchronization verified.")

    # --------------------------------------------------------------------------
    # 6. Serialization & Cockpit Verification
    # --------------------------------------------------------------------------
    print("[TEST 6] Dashboard JSON and Cockpit Markdown Export...")
    coordinator.persist_state()

    json_file = test_state_dir / "concurrency_locks.json"
    cockpit_file = test_state_dir / "CONCURRENCY_COCKPIT.md"

    assert json_file.exists(), f"State JSON not written: {json_file}"
    assert cockpit_file.exists(), f"Cockpit markdown not written: {cockpit_file}"

    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)
        assert "epoch_token" in data
        assert "active_leases" in data

    cockpit_md = cockpit_file.read_text(encoding="utf-8")
    assert "CONCURRENCY & FENCING COORDINATION COCKPIT" in cockpit_md
    print("  -> PASSED: Serialization and Cockpit Markdown verified.")

    print("=" * 70)
    print("ALL CONCURRENCY DECONFLICTION SELF-TESTS PASSED PERFECTLY!")
    print("=" * 70)


if __name__ == "__main__":
    run_self_tests()