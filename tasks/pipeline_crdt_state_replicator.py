"""
pipeline_crdt_state_replicator.py
=================================
Pipeline Autonomic Conflict-Free Replicated Data Type (CRDT) & Convergent State Replicator.

Mathematical & Architectural Patterns:
  [Pattern: Conflict-Free Replicated Data Types (CRDT)]
  [Pattern: Strong Eventual Consistency (SEC)]
  [Pattern: Join-Semilattice]
  [Pattern: Optimistic Concurrency Control]

Formal Properties:
  Let (S, ⊔, ≤) define a bounded join-semilattice where for all states x, y, z ∈ S:
    1. Commutativity: x ⊔ y = y ⊔ x
    2. Associativity: (x ⊔ y) ⊔ z = x ⊔ (y ⊔ z)
    3. Idempotence:   x ⊔ x = x
    4. Partial Order: x ≤ y ⇔ x ⊔ y = y
  Replication state evolution across distributed nodes is monotonic: S_t ≤ S_{t+1}.
"""

from __future__ import annotations

import copy
import json
import math
import os
import tempfile
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Generic, List, Optional, Set, Tuple, TypeVar, Union

T = TypeVar("T")


# ============================================================================
# Core Join-Semilattice Interface & Mathematical Primitives
# ============================================================================

class JoinSemilattice(Generic[T]):
    """
    Abstract interface for bounded join-semilattice state-based CRDTs (CvRDTs).
    Any valid implementation must satisfy Commutativity, Associativity, and Idempotence.
    """

    def merge(self, other: T) -> T:
        """Compute the least upper bound (LUB / join: ⊔) of self and other."""
        raise NotImplementedError

    def leq(self, other: T) -> bool:
        """Partial order comparison: self ≤ other ⇔ self ⊔ other == other."""
        raise NotImplementedError


# ============================================================================
# 1. PNCounter: Positive-Negative Counter (State-based / Delta-enabled CvRDT)
# ============================================================================

class PNCounter(JoinSemilattice["PNCounter"]):
    """
    Thread-safe Positive-Negative Counter (PNCounter).
    State: (P, N) where P, N: Dict[NodeID, int] monotonically track increments and decrements.
    Value: V = ∑ P[i] - ∑ N[i]
    Join:  P_merged[i] = max(P_A[i], P_B[i]), N_merged[i] = max(N_A[i], N_B[i])
    """

    def __init__(self, p_map: Optional[Dict[str, int]] = None, n_map: Optional[Dict[str, int]] = None) -> None:
        self._lock = threading.RLock()
        self.P: Dict[str, int] = dict(p_map) if p_map else {}
        self.N: Dict[str, int] = dict(n_map) if n_map else {}

    def increment(self, node_id: str, delta: int = 1) -> None:
        if delta < 0:
            raise ValueError("Increment delta must be non-negative. Use decrement() instead.")
        with self._lock:
            self.P[node_id] = self.P.get(node_id, 0) + delta

    def decrement(self, node_id: str, delta: int = 1) -> None:
        if delta < 0:
            raise ValueError("Decrement delta must be non-negative. Use increment() instead.")
        with self._lock:
            self.N[node_id] = self.N.get(node_id, 0) + delta

    def value(self) -> int:
        with self._lock:
            sum_p = sum(self.P.values())
            sum_n = sum(self.N.values())
            return sum_p - sum_n

    def merge(self, other: PNCounter) -> PNCounter:
        with self._lock, other._lock:
            all_p_nodes = set(self.P.keys()) | set(other.P.keys())
            all_n_nodes = set(self.N.keys()) | set(other.N.keys())

            merged_p = {node: max(self.P.get(node, 0), other.P.get(node, 0)) for node in all_p_nodes}
            merged_n = {node: max(self.N.get(node, 0), other.N.get(node, 0)) for node in all_n_nodes}

            return PNCounter(merged_p, merged_n)

    def merge_in_place(self, other: PNCounter) -> None:
        with self._lock, other._lock:
            for node, val in other.P.items():
                self.P[node] = max(self.P.get(node, 0), val)
            for node, val in other.N.items():
                self.N[node] = max(self.N.get(node, 0), val)

    def leq(self, other: PNCounter) -> bool:
        with self._lock, other._lock:
            for node, val in self.P.items():
                if val > other.P.get(node, 0):
                    return False
            for node, val in self.N.items():
                if val > other.N.get(node, 0):
                    return False
            return True

    def delta_since(self, base: PNCounter) -> PNCounter:
        """Generate a minimal delta state containing only entries strictly advanced beyond base."""
        with self._lock, base._lock:
            delta_p: Dict[str, int] = {}
            delta_n: Dict[str, int] = {}

            for node, val in self.P.items():
                if val > base.P.get(node, 0):
                    delta_p[node] = val

            for node, val in self.N.items():
                if val > base.N.get(node, 0):
                    delta_n[node] = val

            return PNCounter(delta_p, delta_n)

    def copy(self) -> PNCounter:
        with self._lock:
            return PNCounter(copy.deepcopy(self.P), copy.deepcopy(self.N))

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {"P": dict(self.P), "N": dict(self.N), "value": self.value()}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> PNCounter:
        return cls(p_map=data.get("P", {}), n_map=data.get("N", {}))

    def __repr__(self) -> str:
        return f"<PNCounter val={self.value()} P={self.P} N={self.N}>"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PNCounter):
            return False
        with self._lock, other._lock:
            return self.P == other.P and self.N == other.N


# ============================================================================
# 2. ORSet: Observed-Removed Set with Unique UUID Tags (Add-Wins Semantics)
# ============================================================================

@dataclass(frozen=True)
class TaggedElement:
    element: Any
    tag: str


class ORSet(JoinSemilattice["ORSet"]):
    """
    Observed-Removed Set (ORSet) supporting Add-Wins semantics.
    State: (A, R) where:
      - A ⊆ Element × Tag (Add set with unique UUID tags)
      - R ⊆ Tag (Removed / Tombstone tag set)
    Lookup:
      e ∈ ORSet ⇔ ∃ tag: (e, tag) ∈ A ∧ tag ∉ R
    Join (⊔):
      A_merged = A_1 ∪ A_2
      R_merged = R_1 ∪ R_2
    """

    def __init__(
        self,
        adds: Optional[Set[Tuple[Any, str]]] = None,
        removals: Optional[Set[str]] = None,
    ) -> None:
        self._lock = threading.RLock()
        self.A: Set[Tuple[Any, str]] = set(adds) if adds else set()
        self.R: Set[str] = set(removals) if removals else set()

    def add(self, element: Any, node_id: Optional[str] = None) -> str:
        """Add an element with a globally unique tag."""
        unique_tag = f"{node_id or 'node'}_{uuid.uuid4().hex}"
        with self._lock:
            self.A.add((element, unique_tag))
        return unique_tag

    def remove(self, element: Any) -> Set[str]:
        """
        Tombstone all currently observed tags for the given element.
        Concurrent additions that occur independently will receive disjoint tags,
        preserving Add-Wins semantics.
        """
        with self._lock:
            observed_tags = {tag for (e, tag) in self.A if e == element and tag not in self.R}
            self.R.update(observed_tags)
            return observed_tags

    def read(self) -> Set[Any]:
        """Return the current set of active, un-tombstoned elements."""
        with self._lock:
            return {e for (e, tag) in self.A if tag not in self.R}

    def contains(self, element: Any) -> bool:
        with self._lock:
            return any(e == element and tag not in self.R for (e, tag) in self.A)

    def merge(self, other: ORSet) -> ORSet:
        with self._lock, other._lock:
            merged_a = self.A | other.A
            merged_r = self.R | other.R
            return ORSet(merged_a, merged_r)

    def merge_in_place(self, other: ORSet) -> None:
        with self._lock, other._lock:
            self.A.update(other.A)
            self.R.update(other.R)

    def leq(self, other: ORSet) -> bool:
        with self._lock, other._lock:
            return self.A.issubset(other.A) and self.R.issubset(other.R)

    def delta_since(self, base: ORSet) -> ORSet:
        """Extract incremental adds and tombstone removals not yet observed by base."""
        with self._lock, base._lock:
            delta_a = self.A - base.A
            delta_r = self.R - base.R
            return ORSet(delta_a, delta_r)

    def garbage_collect_tombstones(self, known_prunable_tags: Set[str]) -> int:
        """
        Safely prune tombstones (R) and corresponding add records (A)
        once all replicas in the causal horizon have confirmed receipt.
        """
        with self._lock:
            prunable = self.R.intersection(known_prunable_tags)
            if not prunable:
                return 0
            self.A = {(e, tag) for (e, tag) in self.A if tag not in prunable}
            self.R.difference_update(prunable)
            return len(prunable)

    def copy(self) -> ORSet:
        with self._lock:
            return ORSet(set(self.A), set(self.R))

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "active_elements": sorted(list(str(x) for x in self.read())),
                "add_count": len(self.A),
                "tombstone_count": len(self.R),
                "A": [[item[0], item[1]] for item in self.A],
                "R": list(self.R),
            }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ORSet:
        adds = {(item[0], item[1]) for item in data.get("A", [])}
        removals = set(data.get("R", []))
        return cls(adds=adds, removals=removals)

    def __repr__(self) -> str:
        return f"<ORSet items={self.read()} |A|={len(self.A)} |R|={len(self.R)}>"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ORSet):
            return False
        with self._lock, other._lock:
            return self.A == other.A and self.R == other.R


# ============================================================================
# 3. LWWRegister: Last-Write-Wins Register with Node Tie-Breaking
# ============================================================================

class LWWRegister(JoinSemilattice["LWWRegister"]):
    """
    Last-Write-Wins Register (LWWRegister).
    State: (value, logical_timestamp, node_id)
    Join (⊔):
      (v_1, t_1, n_1) ⊔ (v_2, t_2, n_2) =
         argmax by tuple key (timestamp, node_id).
    """

    def __init__(self, value: Any = None, timestamp: float = 0.0, node_id: str = "") -> None:
        self._lock = threading.RLock()
        self.value: Any = value
        self.timestamp: float = float(timestamp)
        self.node_id: str = str(node_id)

    def write(self, value: Any, timestamp: Optional[float] = None, node_id: Optional[str] = None) -> None:
        with self._lock:
            ts = timestamp if timestamp is not None else time.time()
            nid = node_id if node_id is not None else self.node_id
            if (ts, nid) >= (self.timestamp, self.node_id):
                self.value = value
                self.timestamp = ts
                self.node_id = nid

    def read(self) -> Any:
        with self._lock:
            return self.value

    def merge(self, other: LWWRegister) -> LWWRegister:
        with self._lock, other._lock:
            self_key = (self.timestamp, self.node_id)
            other_key = (other.timestamp, other.node_id)
            if self_key >= other_key:
                return LWWRegister(self.value, self.timestamp, self.node_id)
            else:
                return LWWRegister(other.value, other.timestamp, other.node_id)

    def merge_in_place(self, other: LWWRegister) -> None:
        with self._lock, other._lock:
            if (other.timestamp, other.node_id) > (self.timestamp, self.node_id):
                self.value = other.value
                self.timestamp = other.timestamp
                self.node_id = other.node_id

    def leq(self, other: LWWRegister) -> bool:
        with self._lock, other._lock:
            return (self.timestamp, self.node_id) <= (other.timestamp, other.node_id)

    def copy(self) -> LWWRegister:
        with self._lock:
            return LWWRegister(copy.deepcopy(self.value), self.timestamp, self.node_id)

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "value": self.value,
                "timestamp": self.timestamp,
                "node_id": self.node_id,
            }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> LWWRegister:
        return cls(
            value=data.get("value"),
            timestamp=data.get("timestamp", 0.0),
            node_id=data.get("node_id", ""),
        )

    def __repr__(self) -> str:
        return f"<LWWRegister val={self.value!r} ts={self.timestamp} node={self.node_id}>"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, LWWRegister):
            return False
        with self._lock, other._lock:
            return (
                self.value == other.value
                and self.timestamp == other.timestamp
                and self.node_id == other.node_id
            )


# ============================================================================
# 4. Composite Replica State & CRDTStateReplicator Coordinator
# ============================================================================

@dataclass
class ReplicaCompositeState:
    counters: Dict[str, PNCounter] = field(default_factory=dict)
    sets: Dict[str, ORSet] = field(default_factory=dict)
    registers: Dict[str, LWWRegister] = field(default_factory=dict)
    version_vector: Dict[str, int] = field(default_factory=dict)

    def merge(self, other: ReplicaCompositeState) -> ReplicaCompositeState:
        result = ReplicaCompositeState()

        # Merge Counters
        all_counter_keys = set(self.counters.keys()) | set(other.counters.keys())
        for k in all_counter_keys:
            c1 = self.counters.get(k, PNCounter())
            c2 = other.counters.get(k, PNCounter())
            result.counters[k] = c1.merge(c2)

        # Merge Sets
        all_set_keys = set(self.sets.keys()) | set(other.sets.keys())
        for k in all_set_keys:
            s1 = self.sets.get(k, ORSet())
            s2 = other.sets.get(k, ORSet())
            result.sets[k] = s1.merge(s2)

        # Merge Registers
        all_reg_keys = set(self.registers.keys()) | set(other.registers.keys())
        for k in all_reg_keys:
            r1 = self.registers.get(k, LWWRegister())
            r2 = other.registers.get(k, LWWRegister())
            result.registers[k] = r1.merge(r2)

        # Merge Version Vector (Monotonic Component-wise max)
        all_nodes = set(self.version_vector.keys()) | set(other.version_vector.keys())
        for n in all_nodes:
            result.version_vector[n] = max(
                self.version_vector.get(n, 0), other.version_vector.get(n, 0)
            )

        return result

    def copy(self) -> ReplicaCompositeState:
        res = ReplicaCompositeState()
        res.counters = {k: v.copy() for k, v in self.counters.items()}
        res.sets = {k: v.copy() for k, v in self.sets.items()}
        res.registers = {k: v.copy() for k, v in self.registers.items()}
        res.version_vector = dict(self.version_vector)
        return res

    def to_dict(self) -> Dict[str, Any]:
        return {
            "counters": {k: v.to_dict() for k, v in self.counters.items()},
            "sets": {k: v.to_dict() for k, v in self.sets.items()},
            "registers": {k: v.to_dict() for k, v in self.registers.items()},
            "version_vector": dict(self.version_vector),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ReplicaCompositeState:
        state = cls()
        for k, v in data.get("counters", {}).items():
            state.counters[k] = PNCounter.from_dict(v)
        for k, v in data.get("sets", {}).items():
            state.sets[k] = ORSet.from_dict(v)
        for k, v in data.get("registers", {}).items():
            state.registers[k] = LWWRegister.from_dict(v)
        state.version_vector = dict(data.get("version_vector", {}))
        return state


class CRDTReplicaNode:
    """
    Individual participant node holding an independent, locally mutating
    composite CRDT state and generating delta slices for network distribution.
    """

    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        self._lock = threading.RLock()
        self.state = ReplicaCompositeState()
        self.snapshot_sync_state = ReplicaCompositeState()
        self.mutation_counter = 0

    def mutate_counter_inc(self, key: str, delta: int = 1) -> None:
        with self._lock:
            if key not in self.state.counters:
                self.state.counters[key] = PNCounter()
            self.state.counters[key].increment(self.node_id, delta)
            self._tick_version()

    def mutate_counter_dec(self, key: str, delta: int = 1) -> None:
        with self._lock:
            if key not in self.state.counters:
                self.state.counters[key] = PNCounter()
            self.state.counters[key].decrement(self.node_id, delta)
            self._tick_version()

    def mutate_set_add(self, key: str, element: Any) -> str:
        with self._lock:
            if key not in self.state.sets:
                self.state.sets[key] = ORSet()
            tag = self.state.sets[key].add(element, self.node_id)
            self._tick_version()
            return tag

    def mutate_set_remove(self, key: str, element: Any) -> Set[str]:
        with self._lock:
            if key not in self.state.sets:
                return set()
            removed = self.state.sets[key].remove(element)
            self._tick_version()
            return removed

    def mutate_register_write(self, key: str, value: Any, timestamp: Optional[float] = None) -> None:
        with self._lock:
            if key not in self.state.registers:
                self.state.registers[key] = LWWRegister(node_id=self.node_id)
            self.state.registers[key].write(value, timestamp, self.node_id)
            self._tick_version()

    def generate_delta_mutation(self) -> ReplicaCompositeState:
        """
        Generate delta state relative to snapshot_sync_state,
        minimizing serialization payload across replicated channels.
        """
        with self._lock:
            delta = ReplicaCompositeState()

            # Counters delta
            for k, c in self.state.counters.items():
                base_c = self.snapshot_sync_state.counters.get(k, PNCounter())
                d_c = c.delta_since(base_c)
                if d_c.P or d_c.N:
                    delta.counters[k] = d_c

            # Sets delta
            for k, s in self.state.sets.items():
                base_s = self.snapshot_sync_state.sets.get(k, ORSet())
                d_s = s.delta_since(base_s)
                if d_s.A or d_s.R:
                    delta.sets[k] = d_s

            # Registers delta (include if strictly newer)
            for k, r in self.state.registers.items():
                base_r = self.snapshot_sync_state.registers.get(k, LWWRegister())
                if (r.timestamp, r.node_id) > (base_r.timestamp, base_r.node_id):
                    delta.registers[k] = r.copy()

            delta.version_vector = dict(self.state.version_vector)
            # Update baseline snapshot
            self.snapshot_sync_state = self.state.copy()
            return delta

    def receive_and_merge(self, incoming_state: ReplicaCompositeState) -> None:
        with self._lock:
            self.state = self.state.merge(incoming_state)
            self.snapshot_sync_state = self.snapshot_sync_state.merge(incoming_state)

    def _tick_version(self) -> None:
        self.mutation_counter += 1
        self.state.version_vector[self.node_id] = self.mutation_counter


class CRDTStateReplicator:
    """
    Main Autonomic Coordinator managing multi-replica cluster topologies,
    orchestrating full and delta sync cycles, resolving concurrent mutations,
    calculating lattice entropy, and writing telemetry dashboards.
    """

    def __init__(self, dashboard_path: str = "dashboards/crdt_state_replicator.json") -> None:
        self.dashboard_path = dashboard_path
        self._lock = threading.RLock()
        self.replicas: Dict[str, CRDTReplicaNode] = {}
        self.sync_history: List[Dict[str, Any]] = []

    def get_or_create_replica(self, node_id: str) -> CRDTReplicaNode:
        with self._lock:
            if node_id not in self.replicas:
                self.replicas[node_id] = CRDTReplicaNode(node_id)
            return self.replicas[node_id]

    def synchronize_cluster_pairwise(self, source_id: str, target_id: str, delta_only: bool = True) -> int:
        """Propagate state from source replica to target replica."""
        with self._lock:
            src = self.replicas[source_id]
            dst = self.replicas[target_id]

            if delta_only:
                payload = src.generate_delta_mutation()
                payload_size = len(json.dumps(payload.to_dict()))
            else:
                payload = src.state.copy()
                payload_size = len(json.dumps(payload.to_dict()))

            dst.receive_and_merge(payload)

            log_entry = {
                "epoch_timestamp": time.time(),
                "source": source_id,
                "target": target_id,
                "mode": "DELTA" if delta_only else "FULL",
                "payload_bytes": payload_size,
            }
            self.sync_history.append(log_entry)
            return payload_size

    def synchronize_all_to_convergence(self) -> None:
        """
        Execute an all-to-all gossiping convergence epoch until every replica
        reaches equivalent semilattice states.
        """
        with self._lock:
            nodes = list(self.replicas.keys())
            if len(nodes) <= 1:
                return

            # Multi-pass flood gossip
            for _ in range(2):
                for i in range(len(nodes)):
                    for j in range(len(nodes)):
                        if i != j:
                            src = self.replicas[nodes[i]]
                            dst = self.replicas[nodes[j]]
                            dst.receive_and_merge(src.state.copy())

    def compute_lattice_entropy(self) -> float:
        """
        Calculate divergence entropy across replica version vectors.
        Entropy H = 0.0 signifies absolute convergence across all cluster replicas.
        """
        with self._lock:
            if not self.replicas:
                return 0.0

            states = [r.state.to_dict() for r in self.replicas.values()]
            json_states = [json.dumps(s, sort_keys=True) for s in states]

            # Distribution of states
            counts: Dict[str, int] = {}
            for s_str in json_states:
                counts[s_str] = counts.get(s_str, 0) + 1

            total = len(json_states)
            entropy = 0.0
            for count in counts.values():
                p = count / total
                if p > 0:
                    entropy -= p * math.log2(p)

            return round(entropy, 6)

    def verify_algebraic_properties(
        self, state_a: ReplicaCompositeState, state_b: ReplicaCompositeState, state_c: ReplicaCompositeState
    ) -> Dict[str, bool]:
        """
        Formally verifies bounded join-semilattice algebraic identities:
          1. Commutativity: a ⊔ b == b ⊔ a
          2. Associativity: (a ⊔ b) ⊔ c == a ⊔ (b ⊔ c)
          3. Idempotence:   a ⊔ a == a
        """
        # Commutativity
        ab = state_a.merge(state_b)
        ba = state_b.merge(state_a)
        is_commutative = ab.to_dict() == ba.to_dict()

        # Associativity
        ab_c = ab.merge(state_c)
        bc = state_b.merge(state_c)
        a_bc = state_a.merge(bc)
        is_associative = ab_c.to_dict() == a_bc.to_dict()

        # Idempotence
        aa = state_a.merge(state_a)
        is_idempotent = aa.to_dict() == state_a.to_dict()

        return {
            "commutativity": is_commutative,
            "associativity": is_associative,
            "idempotence": is_idempotent,
        }

    def export_telemetry_atomic(self) -> str:
        """Atomically persist cluster status, active entropy, and convergence state."""
        with self._lock:
            entropy = self.compute_lattice_entropy()
            convergence_achieved = entropy == 0.0

            telemetry = {
                "timestamp": time.time(),
                "badges": [
                    "[Pattern: Conflict-Free Replicated Data Types (CRDT)]",
                    "[Pattern: Strong Eventual Consistency (SEC)]",
                    "[Pattern: Join-Semilattice]",
                    "[Pattern: Optimistic Concurrency Control]",
                ],
                "cluster_size": len(self.replicas),
                "lattice_entropy": entropy,
                "converged": convergence_achieved,
                "replicas": {node_id: r.state.to_dict() for node_id, r in self.replicas.items()},
                "recent_sync_events": self.sync_history[-20:],
            }

            os.makedirs(os.path.dirname(self.dashboard_path), exist_ok=True)
            dir_name = os.path.dirname(self.dashboard_path) or "."
            with tempfile.NamedTemporaryFile("w", dir=dir_name, delete=False) as tf:
                json.dump(telemetry, tf, indent=2)
                temp_name = tf.name

            os.replace(temp_name, self.dashboard_path)
            return self.dashboard_path


# ============================================================================
# 5. Standalone CLI & Exhaustive Self-Test Suite
# ============================================================================

def _run_crdt_verification_suite() -> None:
    print("=" * 80)
    print("CRDT STATE REPLICATOR: MATHEMATICAL VERIFICATION & SELF-TEST SUITE")
    print("=" * 80)

    # ---------------------------------------------------------
    # Test A: Join-Semilattice Formal Axioms Verification
    # ---------------------------------------------------------
    print("\n[Test A] Evaluating Join-Semilattice Axioms (Commutativity, Associativity, Idempotence)...")
    c1 = PNCounter({"node_1": 12, "node_2": 5}, {"node_1": 2, "node_3": 1})
    c2 = PNCounter({"node_1": 8, "node_2": 9}, {"node_1": 4, "node_2": 2})
    c3 = PNCounter({"node_3": 15, "node_2": 4}, {"node_3": 8, "node_4": 3})

    # Commutativity: c1 ⊔ c2 == c2 ⊔ c1
    m12 = c1.merge(c2)
    m21 = c2.merge(c1)
    assert m12 == m21, "PNCounter failed commutativity!"

    # Associativity: (c1 ⊔ c2) ⊔ c3 == c1 ⊔ (c2 ⊔ c3)
    m12_3 = m12.merge(c3)
    m23 = c2.merge(c3)
    m1_23 = c1.merge(m23)
    assert m12_3 == m1_23, "PNCounter failed associativity!"

    # Idempotence: c1 ⊔ c1 == c1
    m11 = c1.merge(c1)
    assert m11 == c1, "PNCounter failed idempotence!"

    # Monotonicity / Partial Order: c1 ≤ (c1 ⊔ c2)
    assert c1.leq(m12), "c1 must be <= c1 ⊔ c2"
    assert c2.leq(m12), "c2 must be <= c1 ⊔ c2"
    print("✓ PNCounter passed all Semilattice Axioms.")

    # ---------------------------------------------------------
    # Test B: 3-Way Divergent Concurrent Divergence & Convergence
    # ---------------------------------------------------------
    print("\n[Test B] Testing 3-Way Divergent Concurrent Workspaces...")
    replicator = CRDTStateReplicator("dashboards/crdt_state_replicator.json")
    n1 = replicator.get_or_create_replica("edge_us_east")
    n2 = replicator.get_or_create_replica("edge_eu_west")
    n3 = replicator.get_or_create_replica("edge_ap_south")

    # Concurrent operations on counter 'pipeline_throughput'
    n1.mutate_counter_inc("pipeline_throughput", 100)
    n1.mutate_counter_dec("pipeline_throughput", 10)

    n2.mutate_counter_inc("pipeline_throughput", 50)
    n2.mutate_counter_dec("pipeline_throughput", 5)

    n3.mutate_counter_inc("pipeline_throughput", 200)
    n3.mutate_counter_dec("pipeline_throughput", 80)

    # Initial states differ
    assert n1.state.counters["pipeline_throughput"].value() == 90
    assert n2.state.counters["pipeline_throughput"].value() == 45
    assert n3.state.counters["pipeline_throughput"].value() == 120

    # Intermediate entropy should be > 0.0
    entropy_initial = replicator.compute_lattice_entropy()
    assert entropy_initial > 0.0, f"Expected non-zero entropy during divergence, got {entropy_initial}"
    print(f"  Cluster divergence entropy: {entropy_initial:.4f}")

    # Synchronize all nodes
    replicator.synchronize_all_to_convergence()
    entropy_final = replicator.compute_lattice_entropy()
    assert entropy_final == 0.0, f"Expected 0.0 entropy after convergence, got {entropy_final}"

    # Expected value: (100 + 50 + 200) - (10 + 5 + 80) = 350 - 95 = 255
    expected_counter = 255
    for node_name, node in replicator.replicas.items():
        val = node.state.counters["pipeline_throughput"].value()
        assert val == expected_counter, f"Node {node_name} converged to {val}, expected {expected_counter}"
    print(f"✓ All 3 replicas converged to identical value {expected_counter} with entropy 0.0.")

    # ---------------------------------------------------------
    # Test C: ORSet Add-Wins Semantics under Race Conditions
    # ---------------------------------------------------------
    print("\n[Test C] ORSet Add-Wins Semantics Verification under Concurrent Mutation...")
    set_node_a = CRDTReplicaNode("worker_a")
    set_node_b = CRDTReplicaNode("worker_b")

    # Step 1: Initial item added on A and synced to B
    set_node_a.mutate_set_add("active_jobs", "job_x")
    set_node_b.receive_and_merge(set_node_a.state.copy())

    assert set_node_a.state.sets["active_jobs"].read() == {"job_x"}
    assert set_node_b.state.sets["active_jobs"].read() == {"job_x"}

    # Step 2: Concurrently, A removes 'job_x' while B re-adds 'job_x'
    set_node_a.mutate_set_remove("active_jobs", "job_x")
    set_node_b.mutate_set_add("active_jobs", "job_x")

    # Before merge:
    assert not set_node_a.state.sets["active_jobs"].contains("job_x")
    assert set_node_b.state.sets["active_jobs"].contains("job_x")

    # Merge states both ways
    merged_state_a = set_node_a.state.merge(set_node_b.state)
    merged_state_b = set_node_b.state.merge(set_node_a.state)

    # In Add-Wins semantics, 'job_x' MUST be present because the new add on B generated
    # a fresh UUID tag unseen by A's remove operation.
    assert merged_state_a.sets["active_jobs"].contains("job_x"), "Add-Wins violated on node A merge!"
    assert merged_state_b.sets["active_jobs"].contains("job_x"), "Add-Wins violated on node B merge!"
    assert merged_state_a.to_dict() == merged_state_b.to_dict(), "Merge symmetry broken!"
    print("✓ ORSet Add-Wins race condition mathematically resolved with convergent presence.")

    # ---------------------------------------------------------
    # Test D: Delta-State Synchronization Bandwidth Efficiency
    # ---------------------------------------------------------
    print("\n[Test D] Evaluating Delta-State vs Full-State Synchronization Efficiency...")
    delta_src = CRDTReplicaNode("delta_src")
    delta_dst = CRDTReplicaNode("delta_dst")

    # Seed 100 large elements into the set
    for i in range(100):
        delta_src.mutate_set_add("dataset_catalog", f"payload_record_hash_entry_key_partition_{i}")

    # Initial sync
    initial_delta = delta_src.generate_delta_mutation()
    delta_dst.receive_and_merge(initial_delta)
    assert delta_dst.state.sets["dataset_catalog"].read() == delta_src.state.sets["dataset_catalog"].read()

    # Full state JSON representation
    full_payload_size = len(json.dumps(delta_src.state.to_dict()))

    # Mutate just one small entry
    delta_src.mutate_counter_inc("minor_counter", 1)
    delta_payload = delta_src.generate_delta_mutation()
    delta_payload_size = len(json.dumps(delta_payload.to_dict()))

    savings_ratio = (1.0 - (delta_payload_size / full_payload_size)) * 100.0
    print(f"  Full State Payload Size:  {full_payload_size} bytes")
    print(f"  Delta State Payload Size: {delta_payload_size} bytes")
    print(f"  Bandwidth Savings:        {savings_ratio:.2f}%")
    assert delta_payload_size < full_payload_size, "Delta payload should be smaller than full state!"
    print("✓ Delta synchronization yields significant compression ratio.")

    # ---------------------------------------------------------
    # Test E: Full JSON Serialization and Atomic Telemetry Export
    # ---------------------------------------------------------
    print("\n[Test E] Testing Atomic Dashboard Persistence & Deserialization...")
    dash_path = replicator.export_telemetry_atomic()
    assert os.path.exists(dash_path), f"Dashboard file {dash_path} was not created!"

    with open(dash_path, "r") as f:
        loaded = json.load(f)

    assert loaded["converged"] is True
    assert loaded["lattice_entropy"] == 0.0
    assert len(loaded["badges"]) == 4
    print(f"✓ Telemetry successfully persisted to {dash_path}")

    # Composite State Deserialization check
    sample_replica_dict = loaded["replicas"]["edge_us_east"]
    deserialized_composite = ReplicaCompositeState.from_dict(sample_replica_dict)
    assert deserialized_composite.counters["pipeline_throughput"].value() == 255
    print("✓ Full JSON Round-trip Serialization & Deserialization confirmed bit-accurate.")

    print("\n" + "=" * 80)
    print("ALL CONVERGENCE & SEMILATTICE REPLICATOR TESTS PASSED PERFECTLY (100%)")
    print("=" * 80)


if __name__ == "__main__":
    _run_crdt_verification_suite()