"""Pipeline Autonomic Vector Clock Causality Arbiter & Partial Order Version Vector Tracker.

Academic & Production Architecture:
- [Pattern: Vector Clocks]
- [Pattern: Lamport Logical Timestamps]
- [Pattern: Partial Order Causality]
- [Pattern: Distributed Conflict Detection]

Formal Specification:
1. Vector Clock Algebraic Formalism:
   Let P = {p_1, p_2, ..., p_n} be the set of autonomous agent worker pods.
   Each process p_i maintains vector clock V_i in N^n initialized to [0, 0, ..., 0].
   - Local Event: p_i increments V_i[i] = V_i[i] + 1.
   - Message Send: Carries vector timestamp V_m = V_i.
   - Message Receive: p_i updates V_i[k] = max(V_i[k], V_m[k]) for all k in {1..n},
     followed by local increment V_i[i] = V_i[i] + 1.

2. Partial Order Causality & Concurrency Evaluator:
   Given two event vectors V_A and V_B:
   - Causally Precedes (V_A < V_B): (forall k, V_A[k] <= V_B[k]) AND (exists k, V_A[k] < V_B[k]).
   - Causally Succeeds (V_A > V_B): (forall k, V_A[k] >= V_B[k]) AND (exists k, V_A[k] > V_B[k]).
   - Identical (V_A == V_B): forall k, V_A[k] == V_B[k].
   - Concurrent / Split-Brain (V_A || V_B): NOT(V_A <= V_B) AND NOT(V_B <= V_A).

3. Split-Brain Conflict Detection & DAG Serialization:
   Flag CONCURRENT_MUTATION_HAZARD if V_A || V_B on overlapping repo and target file.
   Constructs topological causal graph G = (Events, CausalEdges).
   Generates deterministic total order linearization via Lamport tie-breaker.
"""

from __future__ import annotations

import copy
import dataclasses
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
import os
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple


class CausalityRelation(str, Enum):
    """Pairwise partial-order causality relationships between distributed events."""
    PRECEDES = "PRECEDES"                    # V_A < V_B (A causally preceded B)
    SUCCEEDS = "SUCCEEDS"                    # V_A > V_B (A causally succeeded B)
    EQUAL = "EQUAL"                          # V_A == V_B (Identical logical state)
    CONCURRENT_CONFLICT = "CONCURRENT_CONFLICT"  # V_A || V_B (Split-brain concurrency)


class VectorClock:
    """Encapsulates a distributed Vector Clock mapping process_id to logical integer counter.

    Supports algebraic partial-order relations (<, <=, >, >=, ==), join/merge lattice
    operations, and pairwise concurrency evaluation (V_A || V_B).
    """

    def __init__(self, clock_dict: Optional[Dict[str, int]] = None) -> None:
        self._clock: Dict[str, int] = {}
        if clock_dict:
            for pid, count in clock_dict.items():
                if count < 0:
                    raise ValueError(f"Vector clock value cannot be negative: {pid}={count}")
                self._clock[str(pid)] = int(count)

    @property
    def clock(self) -> Dict[str, int]:
        """Return an immutable snapshot copy of the internal clock dictionary."""
        return dict(self._clock)

    def get(self, process_id: str, default: int = 0) -> int:
        """Retrieve logical counter for a given process."""
        return self._clock.get(process_id, default)

    def increment(self, process_id: str) -> VectorClock:
        """Monotonically increment the local logical clock for process_id and return self."""
        self._clock[process_id] = self._clock.get(process_id, 0) + 1
        return self

    def merge(self, other: VectorClock) -> VectorClock:
        """Algebraic supremum (join) over two vector clocks: V_new[k] = max(V_self[k], V_other[k])."""
        all_keys = set(self._clock.keys()).union(other._clock.keys())
        merged = {k: max(self.get(k, 0), other.get(k, 0)) for k in all_keys}
        return VectorClock(merged)

    def clone(self) -> VectorClock:
        """Deep copy of the vector clock instance."""
        return VectorClock(copy.deepcopy(self._clock))

    def is_concurrent(self, other: VectorClock) -> bool:
        """Evaluates concurrency V_A || V_B (incomparable under partial order)."""
        return not (self <= other) and not (other <= self)

    def __or__(self, other: Any) -> bool:
        """Syntax sugar for concurrency evaluation: (clock_a | clock_b) -> V_A || V_B."""
        if isinstance(other, VectorClock):
            return self.is_concurrent(other)
        return NotImplemented

    def compare(self, other: VectorClock) -> CausalityRelation:
        """Compute the exact partial-order relation between self and other."""
        if self == other:
            return CausalityRelation.EQUAL
        if self < other:
            return CausalityRelation.PRECEDES
        if self > other:
            return CausalityRelation.SUCCEEDS
        return CausalityRelation.CONCURRENT_CONFLICT

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, VectorClock):
            return False
        all_keys = set(self._clock.keys()).union(other._clock.keys())
        return all(self.get(k, 0) == other.get(k, 0) for k in all_keys)

    def __le__(self, other: VectorClock) -> bool:
        all_keys = set(self._clock.keys()).union(other._clock.keys())
        return all(self.get(k, 0) <= other.get(k, 0) for k in all_keys)

    def __lt__(self, other: VectorClock) -> bool:
        return (self <= other) and (self != other)

    def __ge__(self, other: VectorClock) -> bool:
        return other <= self

    def __gt__(self, other: VectorClock) -> bool:
        return other < self

    def __repr__(self) -> str:
        items = sorted(self._clock.items())
        pairs = ", ".join(f"'{k}': {v}" for k, v in items)
        return f"VectorClock({{{pairs}}})"

    def to_dict(self) -> Dict[str, int]:
        return dict(sorted(self._clock.items()))


@dataclass(frozen=True)
class CausalEvent:
    """Immutable record of an autonomic mutation event in the distributed agent pipeline."""
    event_id: str
    process_id: str
    vector_clock: VectorClock
    repo_id: str
    target_file: str
    mutation_hash: str
    timestamp_epoch: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "process_id": self.process_id,
            "vector_clock": self.vector_clock.to_dict(),
            "repo_id": self.repo_id,
            "target_file": self.target_file,
            "mutation_hash": self.mutation_hash,
            "timestamp_epoch": self.timestamp_epoch,
        }


class VectorClockArbiter:
    """Autonomic Causality Arbiter and Distributed Concurrency Coordinator.

    Coordinates process vector clocks, ingests local and peer sync events, constructs
    topological causal DAGs, detects split-brain file hazards, and outputs linear deterministic orders.
    """

    def __init__(self, state_file: str = "dashboards/vector_clock_causality.json") -> None:
        self.state_file = state_file
        self._lock = threading.RLock()
        self._process_clocks: Dict[str, VectorClock] = {}
        self._events: List[CausalEvent] = []
        self._event_map: Dict[str, CausalEvent] = {}

    def register_process(self, process_id: str) -> None:
        """Register a new autonomous worker process/pod into the coordination mesh."""
        with self._lock:
            if process_id not in self._process_clocks:
                self._process_clocks[process_id] = VectorClock({process_id: 0})

    def get_process_clock(self, process_id: str) -> VectorClock:
        """Return a defensive clone of the process vector clock."""
        with self._lock:
            if process_id not in self._process_clocks:
                self.register_process(process_id)
            return self._process_clocks[process_id].clone()

    def record_event(
        self,
        process_id: str,
        repo_id: str,
        target_file: str,
        payload_hash: str,
        custom_event_id: Optional[str] = None,
    ) -> CausalEvent:
        """Execute Event Rule A (Local Event):

        Increments local clock component V_i[i] = V_i[i] + 1 and records the immutable event.
        """
        with self._lock:
            self.register_process(process_id)
            clock = self._process_clocks[process_id]
            clock.increment(process_id)
            snapshot_clock = clock.clone()

            if not custom_event_id:
                hasher = hashlib.sha256()
                hasher.update(f"{process_id}:{repo_id}:{target_file}:{snapshot_clock}:{time.time()}".encode("utf-8"))
                event_id = f"evt_{hasher.hexdigest()[:12]}"
            else:
                event_id = custom_event_id

            event = CausalEvent(
                event_id=event_id,
                process_id=process_id,
                vector_clock=snapshot_clock,
                repo_id=repo_id,
                target_file=target_file,
                mutation_hash=payload_hash,
                timestamp_epoch=time.time(),
            )
            self._events.append(event)
            self._event_map[event.event_id] = event
            return event

    def receive_sync_event(
        self,
        receiver_process_id: str,
        incoming_event: CausalEvent,
        custom_event_id: Optional[str] = None,
    ) -> CausalEvent:
        """Execute Event Rule C (Message Receive / Commit Integration):

        Upon receiving message with vector clock V_m from peer, receiver p_i:
        1. V_i[k] = max(V_i[k], V_m[k]) for all k.
        2. Increments local component V_i[receiver] = V_i[receiver] + 1.
        """
        with self._lock:
            self.register_process(receiver_process_id)
            local_clock = self._process_clocks[receiver_process_id]

            # Lattice supremum merge
            merged_clock = local_clock.merge(incoming_event.vector_clock)
            # Local receive tick
            merged_clock.increment(receiver_process_id)
            self._process_clocks[receiver_process_id] = merged_clock

            snapshot_clock = merged_clock.clone()

            if not custom_event_id:
                hasher = hashlib.sha256()
                hasher.update(
                    f"sync:{receiver_process_id}:{incoming_event.event_id}:{snapshot_clock}".encode("utf-8")
                )
                event_id = f"sync_{hasher.hexdigest()[:12]}"
            else:
                event_id = custom_event_id

            sync_event = CausalEvent(
                event_id=event_id,
                process_id=receiver_process_id,
                vector_clock=snapshot_clock,
                repo_id=incoming_event.repo_id,
                target_file=incoming_event.target_file,
                mutation_hash=f"merge({incoming_event.mutation_hash})",
                timestamp_epoch=time.time(),
            )
            self._events.append(sync_event)
            self._event_map[sync_event.event_id] = sync_event
            return sync_event

    @staticmethod
    def evaluate_causality(event_a: CausalEvent, event_b: CausalEvent) -> CausalityRelation:
        """Computes the partial-order causality relationship between two causal events."""
        return event_a.vector_clock.compare(event_b.vector_clock)

    def detect_concurrent_conflicts(
        self, events: Optional[List[CausalEvent]] = None
    ) -> List[Tuple[CausalEvent, CausalEvent]]:
        """Identifies all concurrent pairs (V_A || V_B) where target repository file paths intersect.

        Flags CONCURRENT_MUTATION_HAZARD.
        """
        with self._lock:
            ev_list = self._events if events is None else events
            conflicts: List[Tuple[CausalEvent, CausalEvent]] = []
            n = len(ev_list)

            for i in range(n):
                for j in range(i + 1, n):
                    ev_a = ev_list[i]
                    ev_b = ev_list[j]

                    # Target mutation collision criteria
                    if ev_a.repo_id == ev_b.repo_id and ev_a.target_file == ev_b.target_file:
                        relation = self.evaluate_causality(ev_a, ev_b)
                        if relation == CausalityRelation.CONCURRENT_CONFLICT:
                            conflicts.append((ev_a, ev_b))

            return conflicts

    def build_causal_dag(
        self, events: Optional[List[CausalEvent]] = None
    ) -> Dict[str, List[str]]:
        """Constructs a directed acyclic graph G = (V, E) where edge (A -> B) implies A < B."""
        with self._lock:
            ev_list = self._events if events is None else events
            dag: Dict[str, List[str]] = {ev.event_id: [] for ev in ev_list}

            # Find transitive reduction or direct causal precedence
            for a in ev_list:
                for b in ev_list:
                    if a.event_id != b.event_id:
                        if a.vector_clock < b.vector_clock:
                            dag[a.event_id].append(b.event_id)

            return dag

    @staticmethod
    def linearize_causal_history(events: List[CausalEvent]) -> List[CausalEvent]:
        """Produces a deterministic total order linearization of partial-order causal events.

        Resolves concurrent ties using Lamport tie-breaker tuple:
        (Scalar Lamport Logical Time, Sum of Vector Components, Timestamp Epoch, Process ID, Mutation Hash).
        """
        def lamport_sort_key(event: CausalEvent) -> Tuple[int, int, float, str, str]:
            clock_dict = event.vector_clock.clock
            scalar_max = max(clock_dict.values()) if clock_dict else 0
            scalar_sum = sum(clock_dict.values())
            return (
                scalar_max,
                scalar_sum,
                event.timestamp_epoch,
                event.process_id,
                event.mutation_hash,
            )

        return sorted(events, key=lamport_sort_key)

    def export_telemetry_state(self) -> Dict[str, Any]:
        """Generates comprehensive telemetry snapshot and atomically exports to JSON."""
        with self._lock:
            conflicts = self.detect_concurrent_conflicts()
            dag = self.build_causal_dag()
            linearized = self.linearize_causal_history(self._events)

            matrix: Dict[str, Dict[str, int]] = {
                pid: clock.to_dict() for pid, clock in sorted(self._process_clocks.items())
            }

            telemetry = {
                "pattern_badges": [
                    "[Pattern: Vector Clocks]",
                    "[Pattern: Lamport Logical Timestamps]",
                    "[Pattern: Partial Order Causality]",
                    "[Pattern: Distributed Conflict Detection]",
                ],
                "active_processes": sorted(list(self._process_clocks.keys())),
                "process_vector_clock_matrix": matrix,
                "total_events_recorded": len(self._events),
                "causal_dag_edges": dag,
                "concurrent_mutation_hazards": [
                    {
                        "event_a": c[0].to_dict(),
                        "event_b": c[1].to_dict(),
                        "repo_id": c[0].repo_id,
                        "conflicting_target_file": c[0].target_file,
                        "hazard_classification": "CONCURRENT_MUTATION_HAZARD",
                    }
                    for c in conflicts
                ],
                "linearized_total_order": [ev.to_dict() for ev in linearized],
                "export_timestamp_epoch": time.time(),
            }

            # Atomic file persistence
            target_path = os.path.abspath(self.state_file)
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            tmp_path = f"{target_path}.tmp.{os.getpid()}"

            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(telemetry, f, indent=2)

            os.replace(tmp_path, target_path)
            return telemetry


# ==============================================================================
# Comprehensive Self-Test & Verification Suite
# ==============================================================================

def _run_comprehensive_verification() -> None:
    print("=" * 80)
    print("RUNNING EXHAUSTIVE VECTOR CLOCK CAUSALITY ARBITER VERIFICATION")
    print("=" * 80)

    # 1. Monotonic Tick Increments Verification
    print("\n--- Test 1: Monotonic Vector Clock Ticks ---")
    vc1 = VectorClock({"pod_alpha": 0, "pod_beta": 0})
    vc1.increment("pod_alpha")
    assert vc1.get("pod_alpha") == 1, "Failed local tick increment"
    vc1.increment("pod_alpha")
    assert vc1.get("pod_alpha") == 2, "Failed second local tick increment"
    print("✓ Monotonic tick counter assertions passed.")

    # 2. Pairwise Causality Classification (A < B, B > A, A || B, A == B)
    print("\n--- Test 2: Pairwise Partial-Order Evaluation ---")
    v_a = VectorClock({"p1": 2, "p2": 1, "p3": 0})
    v_b = VectorClock({"p1": 2, "p2": 2, "p3": 1})
    v_c = VectorClock({"p1": 1, "p2": 3, "p3": 0})
    v_a_clone = VectorClock({"p1": 2, "p2": 1, "p3": 0})

    assert v_a < v_b, "v_a must causally precede v_b"
    assert v_b > v_a, "v_b must causally succeed v_a"
    assert v_a <= v_b, "v_a <= v_b"
    assert v_a == v_a_clone, "v_a == v_a_clone"
    assert v_a.is_concurrent(v_c), "v_a and v_c must be concurrent"
    assert (v_a | v_c) is True, "v_a | v_c operator must evaluate to True"
    assert v_a.compare(v_b) == CausalityRelation.PRECEDES
    assert v_b.compare(v_a) == CausalityRelation.SUCCEEDS
    assert v_a.compare(v_c) == CausalityRelation.CONCURRENT_CONFLICT
    assert v_a.compare(v_a_clone) == CausalityRelation.EQUAL
    print("✓ Strict partial order mathematical assertions passed.")

    # 3. Multi-Process Distributed Sync & Merge
    print("\n--- Test 3: Distributed Process Synchronization & Lattice Join ---")
    arbiter = VectorClockArbiter(state_file="dashboards/vector_clock_causality.json")
    for pid in ["pod_alpha", "pod_beta", "pod_gamma"]:
        arbiter.register_process(pid)

    # Event 1 on pod_alpha
    ev1 = arbiter.record_event(
        process_id="pod_alpha",
        repo_id="hermes_core",
        target_file="router.py",
        payload_hash="sha_111",
        custom_event_id="ev_alpha_1",
    )
    assert ev1.vector_clock.get("pod_alpha") == 1
    assert ev1.vector_clock.get("pod_beta") == 0

    # Event 2 on pod_beta (independent, concurrent)
    ev2 = arbiter.record_event(
        process_id="pod_beta",
        repo_id="hermes_core",
        target_file="router.py",
        payload_hash="sha_222",
        custom_event_id="ev_beta_1",
    )
    assert ev2.vector_clock.get("pod_beta") == 1
    assert ev2.vector_clock.get("pod_alpha") == 0

    # Sync message: pod_alpha receives ev2 from pod_beta
    ev_sync = arbiter.receive_sync_event(
        receiver_process_id="pod_alpha",
        incoming_event=ev2,
        custom_event_id="ev_alpha_sync_beta",
    )
    # Merged clock should have: alpha=2 (was 1, incremented on receive), beta=1 (from ev2)
    assert ev_sync.vector_clock.get("pod_alpha") == 2
    assert ev_sync.vector_clock.get("pod_beta") == 1
    assert ev2 < ev_sync, "Incoming event must causally precede the integrated sync event"
    print("✓ Multi-process sync and supremum join passed.")

    # 4. Concurrent Mutation Hazard Detection
    print("\n--- Test 4: Split-Brain File Hazard Detection ---")
    # ev1 and ev2 both mutate 'router.py' on 'hermes_core' while being concurrent
    hazards = arbiter.detect_concurrent_conflicts()
    assert len(hazards) == 1, f"Expected 1 hazard, found {len(hazards)}"
    pair = hazards[0]
    assert {pair[0].event_id, pair[1].event_id} == {"ev_alpha_1", "ev_beta_1"}
    print(f"✓ Detected CONCURRENT_MUTATION_HAZARD on {pair[0].target_file}: {pair[0].event_id} || {pair[1].event_id}")

    # 5. Deterministic Total Order Linearization
    print("\n--- Test 5: Deterministic Total Order Linearization ---")
    linearized = arbiter.linearize_causal_history([ev_sync, ev2, ev1])
    ids = [e.event_id for e in linearized]
    print(f"Linearized event order: {ids}")
    # ev_sync has higher scalar time than ev1 and ev2, so it must be last
    assert ids[-1] == "ev_alpha_sync_beta", "Sync event must linearize after its causal predecessors"
    print("✓ Deterministic Lamport linearization passed.")

    # 6. Telemetry Export
    print("\n--- Test 6: Atomic Telemetry Persistence ---")
    telemetry = arbiter.export_telemetry_state()
    assert os.path.exists("dashboards/vector_clock_causality.json"), "Telemetry JSON file missing"
    assert telemetry["total_events_recorded"] == 3
    assert len(telemetry["concurrent_mutation_hazards"]) == 1
    print(f"✓ Atomic telemetry state exported successfully to '{arbiter.state_file}'.")

    print("\n" + "=" * 80)
    print("ALL VECTOR CLOCK CAUSALITY ARBITER SUITES COMPLETED WITH 100% SUCCESS")
    print("=" * 80)


if __name__ == "__main__":
    _run_comprehensive_verification()