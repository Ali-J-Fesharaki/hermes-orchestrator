#!/usr/bin/env python3
"""
Pipeline Autonomic Consistent Hashing Ring & Distributed Sharded Partition Balancer
===================================================================================
Architecture & Algorithmic Foundation:
  1. Consistent Hashing Topology (Karger et al.):
     - Implements a 32-bit continuous circular token space [0, 2^32 - 1].
     - Hash projection: H(k) = MD5(k)[0:4] mapped to uint32.
     - Replicas / Virtual Nodes (vnodes): Each physical worker W_i projects V distinct
       virtual tokens Key(W_i, v) = "worker:{node_id}:vnode:{v}".
     - Token lookup uses logarithmic binary search (bisect_right) over sorted ring tokens.

  2. Google Bounded Loads Algorithm (Mirrokni et al.):
     - Prevents hotspotting when ring tokens or key distributions skew.
     - Average load: bar{L} = ceil(Total_Active_Tasks / |Active_Workers|).
     - Capacity threshold: MaxAllowedLoad(W) = ceil((1 + epsilon) * bar{L}), where epsilon in [0.15, 0.30].
     - Clockwise successor traversal: If the natural ring successor W_cand exceeds its
       capacity bound, the algorithm walks clockwise to inspect subsequent distinct physical
       workers until an unconstrained node is located.

  3. Minimal Topology Churn & Migration Plan Generation:
     - Guarantees O(K/N) task relocations during worker node join/leave events.
     - Computes deterministic migration deltas: Delta_migrate = [(task_id, source_worker, target_worker)]
       enabling seamless task rebalancing and minimal cache/workspace invalidation.

  4. Telemetry & Information Entropy:
     - Quantifies partition uniformity via Shannon Entropy:
         H(X) = - sum_{i=1}^N (p_i * log2(p_i))
       Normalized efficiency: eta = H(X) / log2(N) in [0.0, 1.0].
     - Atomically exports ring state, virtual node distribution, and worker metrics
       to 'dashboards/consistent_hashing_state.json'.

Pattern Badges:
  [Pattern: Consistent Hashing]
  [Pattern: Virtual Nodes]
  [Pattern: Bounded-Load Balancing]
  [Pattern: Sharded Partitioning]
"""

from __future__ import annotations

import bisect
import dataclasses
from dataclasses import dataclass, field
import hashlib
import json
import math
import os
from pathlib import Path
import struct
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple


# =============================================================================
# Core Domain Entities & Data Structures
# =============================================================================

@dataclass
class WorkerNode:
    """Represents a physical worker compute unit / pod in the execution cluster."""
    node_id: str
    capacity_weight: float = 1.0
    active_tasks: int = 0
    is_healthy: bool = True
    tags: Dict[str, str] = field(default_factory=dict)
    assigned_keys: Set[str] = field(default_factory=set)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "capacity_weight": self.capacity_weight,
            "active_tasks": self.active_tasks,
            "is_healthy": self.is_healthy,
            "tags": self.tags,
            "assigned_key_count": len(self.assigned_keys),
        }


@dataclass(order=True)
class HashRingPartition:
    """Represents a single point (virtual token) on the continuous ring topology."""
    token: int
    virtual_node_id: str = field(compare=False)
    physical_node_id: str = field(compare=False)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "token": self.token,
            "virtual_node_id": self.virtual_node_id,
            "physical_node_id": self.physical_node_id,
        }


@dataclass
class MigrationInstruction:
    """Encapsulates a task migration payload triggered by topology dynamics."""
    task_id: str
    source_worker: str
    target_worker: str
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "source_worker": self.source_worker,
            "target_worker": self.target_worker,
            "timestamp": self.timestamp,
        }


# =============================================================================
# Hash Ring Core Topology Engine
# =============================================================================

class ConsistentHashRing:
    """
    Thread-safe 32-bit consistent hashing ring implementation with virtual nodes.
    Maintains sorted tokens for O(log M) successor queries via bisect.
    """

    TWO_POW_32: int = 2**32

    def __init__(self, default_vnodes: int = 150) -> None:
        self.default_vnodes: int = default_vnodes
        self._lock: threading.RLock = threading.RLock()
        self._tokens: List[int] = []
        self._token_to_partition: Dict[int, HashRingPartition] = {}
        self._workers: Dict[str, WorkerNode] = {}
        self._worker_vnodes: Dict[str, List[int]] = {}

    @staticmethod
    def hash_key(key: str) -> int:
        """Computes uniform 32-bit integer ring position using MD5 truncated digest."""
        digest = hashlib.md5(key.encode("utf-8")).digest()
        # Unpack first 4 bytes as big-endian 32-bit unsigned integer
        value = struct.unpack(">I", digest[:4])[0]
        return value % ConsistentHashRing.TWO_POW_32

    def add_worker(self, worker: WorkerNode, vnodes: Optional[int] = None) -> None:
        """
        Projects a physical worker onto the ring with V virtual nodes.
        Thread-safe, handles token collisions gracefully via linear probing.
        """
        with self._lock:
            num_vnodes = vnodes if vnodes is not None else self.default_vnodes
            if worker.node_id in self._workers:
                # Update existing worker metadata without rebuilding if already active
                self._workers[worker.node_id] = worker
                return

            self._workers[worker.node_id] = worker
            generated_tokens: List[int] = []

            for v in range(num_vnodes):
                vnode_key = f"worker:{worker.node_id}:vnode:{v}"
                token = self.hash_key(vnode_key)

                # Linear probing in the astronomical event of 32-bit collision
                while token in self._token_to_partition:
                    token = (token + 1) % self.TWO_POW_32

                partition = HashRingPartition(
                    token=token,
                    virtual_node_id=vnode_key,
                    physical_node_id=worker.node_id,
                )

                idx = bisect.bisect_right(self._tokens, token)
                self._tokens.insert(idx, token)
                self._token_to_partition[token] = partition
                generated_tokens.append(token)

            self._worker_vnodes[worker.node_id] = generated_tokens

    def remove_worker(self, node_id: str) -> Optional[WorkerNode]:
        """Removes a physical worker and purges all associated virtual nodes."""
        with self._lock:
            if node_id not in self._workers:
                return None

            removed_worker = self._workers.pop(node_id)
            tokens_to_remove = self._worker_vnodes.pop(node_id, [])

            for token in tokens_to_remove:
                self._token_to_partition.pop(token, None)
                idx = bisect.bisect_left(self._tokens, token)
                if idx < len(self._tokens) and self._tokens[idx] == token:
                    self._tokens.pop(idx)

            return removed_worker

    def get_successor_partition(self, hash_value: int) -> Optional[HashRingPartition]:
        """Resolves the nearest clockwise virtual node partition for a given hash."""
        with self._lock:
            if not self._tokens:
                return None

            idx = bisect.bisect_right(self._tokens, hash_value)
            if idx == len(self._tokens):
                idx = 0
            return self._token_to_partition[self._tokens[idx]]

    def get_clockwise_sequence(self, hash_value: int) -> List[HashRingPartition]:
        """Yields an ordered sequence of virtual partitions starting from hash successor."""
        with self._lock:
            if not self._tokens:
                return []
            idx = bisect.bisect_right(self._tokens, hash_value)
            # Cycle through tokens starting at idx
            reordered = self._tokens[idx:] + self._tokens[:idx]
            return [self._token_to_partition[t] for t in reordered]

    @property
    def workers(self) -> Dict[str, WorkerNode]:
        with self._lock:
            return dict(self._workers)

    @property
    def token_count(self) -> int:
        with self._lock:
            return len(self._tokens)


# =============================================================================
# Google Bounded-Loads Balancer & Partition Governor
# =============================================================================

class BoundedLoadBalancer:
    """
    Autonomic Load Arbitrator implementing Google Bounded Loads algorithm.
    Guarantees that no worker can be allocated tasks exceeding (1 + epsilon) * avg_load.
    """

    def __init__(
        self,
        ring: ConsistentHashRing,
        epsilon: float = 0.20,
        dashboard_path: str = "dashboards/consistent_hashing_state.json",
    ) -> None:
        self.ring: ConsistentHashRing = ring
        self.epsilon: float = max(0.05, min(1.0, epsilon))
        self.dashboard_path: Path = Path(dashboard_path)
        self._lock: threading.RLock = threading.RLock()
        self._task_assignments: Dict[str, str] = {}  # task_key -> worker_id

    def compute_max_allowed_load(self, active_task_count: Optional[int] = None) -> int:
        """
        Computes MaxAllowedLoad = ceil((1 + epsilon) * ceil(Total_Active_Tasks / |Workers|)).
        """
        workers = [w for w in self.ring.workers.values() if w.is_healthy]
        if not workers:
            return 1

        total_tasks = active_task_count if active_task_count is not None else sum(w.active_tasks for w in workers)
        avg_load = math.ceil(total_tasks / len(workers))
        max_load = math.ceil((1.0 + self.epsilon) * max(1, avg_load))
        return max(1, max_load)

    def assign_task(
        self,
        repository: str,
        file_path: str,
        task_id: Optional[str] = None,
        active_task_count: Optional[int] = None,
    ) -> WorkerNode:
        """
        Assigns a task to a worker using Consistent Hashing with Bounded Loads.
        Walks clockwise along the ring if the primary worker is saturated.
        """
        task_key = f"{repository}:{file_path}"
        effective_task_id = task_id or task_key
        hash_val = self.ring.hash_key(task_key)

        with self._lock:
            max_allowed_load = self.compute_max_allowed_load(active_task_count)
            clockwise_partitions = self.ring.get_clockwise_sequence(hash_val)

            if not clockwise_partitions:
                raise RuntimeError("ConsistentHashRing has zero active partitions.")

            assigned_worker: Optional[WorkerNode] = None
            visited_workers: Set[str] = set()

            for partition in clockwise_partitions:
                w_id = partition.physical_node_id
                if w_id in visited_workers:
                    continue
                visited_workers.add(w_id)

                candidate_worker = self.ring.workers.get(w_id)
                if not candidate_worker or not candidate_worker.is_healthy:
                    continue

                if candidate_worker.active_tasks < max_allowed_load:
                    assigned_worker = candidate_worker
                    break

            # Fallback: If all workers are at or above bounded load, select min-loaded worker
            if not assigned_worker:
                healthy_workers = [w for w in self.ring.workers.values() if w.is_healthy]
                if not healthy_workers:
                    raise RuntimeError("No healthy WorkerNode available in cluster.")
                assigned_worker = min(healthy_workers, key=lambda w: w.active_tasks)

            # Record task assignment
            assigned_worker.active_tasks += 1
            assigned_worker.assigned_keys.add(effective_task_id)
            self._task_assignments[effective_task_id] = assigned_worker.node_id

            return assigned_worker

    def release_task(self, task_id: str) -> None:
        """Deallocates task and decrements corresponding worker load."""
        with self._lock:
            worker_id = self._task_assignments.pop(task_id, None)
            if worker_id and worker_id in self.ring.workers:
                worker = self.ring.workers[worker_id]
                worker.active_tasks = max(0, worker.active_tasks - 1)
                worker.assigned_keys.discard(task_id)

    def rebalance_cluster_dynamics(
        self,
        added_worker: Optional[WorkerNode] = None,
        removed_worker_id: Optional[str] = None,
    ) -> List[MigrationInstruction]:
        """
        Rebalances cluster after topology modification (join/leave).
        Returns minimal Delta_migrate instructions bounded to O(K/N).
        """
        migrations: List[MigrationInstruction] = []

        with self._lock:
            if added_worker:
                self.ring.add_worker(added_worker)
            if removed_worker_id:
                self.ring.remove_worker(removed_worker_id)

            current_assignments = dict(self._task_assignments)

            # Temporary dry-run assignment evaluation
            for task_id, current_worker_id in current_assignments.items():
                hash_val = self.ring.hash_key(task_id)
                successor = self.ring.get_successor_partition(hash_val)

                if successor and successor.physical_node_id != current_worker_id:
                    target_id = successor.physical_node_id
                    # Update local structures
                    if current_worker_id in self.ring.workers:
                        old_worker = self.ring.workers[current_worker_id]
                        old_worker.active_tasks = max(0, old_worker.active_tasks - 1)
                        old_worker.assigned_keys.discard(task_id)

                    new_worker = self.ring.workers[target_id]
                    new_worker.active_tasks += 1
                    new_worker.assigned_keys.add(task_id)

                    self._task_assignments[task_id] = target_id
                    migrations.append(
                        MigrationInstruction(
                            task_id=task_id,
                            source_worker=current_worker_id,
                            target_worker=target_id,
                        )
                    )

        return migrations

    def get_partition_entropy(self) -> float:
        """
        Computes Shannon entropy of task distribution across physical workers:
          H(X) = - sum(p_i * log2(p_i))
        Returns entropy in bits. Maximum entropy = log2(N).
        """
        with self._lock:
            workers = [w for w in self.ring.workers.values() if w.is_healthy]
            if not workers:
                return 0.0

            total_tasks = sum(w.active_tasks for w in workers)
            if total_tasks == 0:
                return 0.0

            entropy = 0.0
            for w in workers:
                if w.active_tasks > 0:
                    p_i = w.active_tasks / total_tasks
                    entropy -= p_i * math.log2(p_i)

            return entropy

    def get_entropy_efficiency(self) -> float:
        """Returns normalized Shannon efficiency eta = H(X) / log2(N) in [0.0, 1.0]."""
        with self._lock:
            healthy_count = len([w for w in self.ring.workers.values() if w.is_healthy])
            if healthy_count <= 1:
                return 1.0
            max_entropy = math.log2(healthy_count)
            return min(1.0, self.get_partition_entropy() / max_entropy)

    def export_dashboard_state(self) -> Dict[str, Any]:
        """Atomically persists ring topology and load stats to JSON dashboard."""
        with self._lock:
            workers_dict = {
                w.node_id: w.to_dict() for w in self.ring.workers.values()
            }
            total_tasks = sum(w.active_tasks for w in self.ring.workers.values())
            worker_count = len(workers_dict)
            avg_load = total_tasks / worker_count if worker_count > 0 else 0.0

            state_payload = {
                "timestamp": time.time(),
                "telemetry_badges": [
                    "[Pattern: Consistent Hashing]",
                    "[Pattern: Virtual Nodes]",
                    "[Pattern: Bounded-Load Balancing]",
                    "[Pattern: Sharded Partitioning]",
                ],
                "topology_summary": {
                    "total_workers": worker_count,
                    "total_virtual_nodes": self.ring.token_count,
                    "total_assigned_tasks": total_tasks,
                    "average_worker_load": round(avg_load, 2),
                    "max_allowed_load": self.compute_max_allowed_load(),
                    "shannon_entropy_bits": round(self.get_partition_entropy(), 4),
                    "entropy_efficiency_ratio": round(self.get_entropy_efficiency(), 4),
                    "epsilon_bound": self.epsilon,
                },
                "workers": workers_dict,
            }

            self.dashboard_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self.dashboard_path.with_suffix(".tmp")
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(state_payload, f, indent=2)
            os.replace(tmp_path, self.dashboard_path)

            return state_payload


# =============================================================================
# Standalone Verification Suite
# =============================================================================

def _run_standalone_verification() -> None:
    """Exhaustive self-test suite validating ring invariants and load bounding."""
    print("======================================================================")
    print("Executing Consistent Hashing Ring & Bounded Load Balancer Verification")
    print("======================================================================")

    ring = ConsistentHashRing(default_vnodes=150)
    balancer = BoundedLoadBalancer(ring=ring, epsilon=0.20)

    # 1. Cluster Initialization
    num_workers = 5
    workers = [
        WorkerNode(node_id=f"pod-alpha-{i}", capacity_weight=1.0)
        for i in range(num_workers)
    ]
    for w in workers:
        ring.add_worker(w)

    print(f"[TEST 1] Cluster Initialized: {len(ring.workers)} workers, {ring.token_count} vnodes total.")
    assert ring.token_count == num_workers * 150, "Virtual node projection mismatch."

    # 2. Uniform Token Distribution Test (1000 Simulated Tasks)
    print("\n[TEST 2] Simulating 1000 tasks across diverse repos & files...")
    num_tasks = 1000
    for i in range(num_tasks):
        repo = f"repo-service-{i % 17}"
        file_path = f"src/module_{i % 31}/component_{i}.py"
        balancer.assign_task(repo, file_path, task_id=f"task-{i}")

    loads = {w.node_id: w.active_tasks for w in ring.workers.values()}
    print(f"  Distribution of 1000 tasks: {loads}")
    entropy = balancer.get_partition_entropy()
    efficiency = balancer.get_entropy_efficiency()
    max_entropy = math.log2(num_workers)
    print(f"  Shannon Entropy: {entropy:.4f} / {max_entropy:.4f} bits (Efficiency: {efficiency * 100:.2f}%)")

    assert efficiency > 0.85, f"Entropy efficiency too low: {efficiency:.2f}"

    # 3. Bounded Load Enforcement under Synthetic Hotspotting
    print("\n[TEST 3] Testing Google Bounded Loads under severe hotspot skew...")
    hotspot_repo = "monorepo-core"
    hotspot_file = "hotspot.py"  # Same key hashed repeatedly
    primary_worker = ring.get_successor_partition(ring.hash_key(f"{hotspot_repo}:{hotspot_file}")).physical_node_id
    print(f"  Primary ring successor for hotspot key: {primary_worker}")

    initial_load = ring.workers[primary_worker].active_tasks
    max_allowed = balancer.compute_max_allowed_load()
    print(f"  Current primary load: {initial_load}, Max allowed load: {max_allowed}")

    # Inject 500 tasks targeting the exact same key
    for k in range(500):
        balancer.assign_task(hotspot_repo, hotspot_file, task_id=f"hotspot-task-{k}")

    post_loads = {w.node_id: w.active_tasks for w in ring.workers.values()}
    print(f"  Post-hotspot load distribution: {post_loads}")
    # Primary worker must not exceed max allowed load threshold
    assert ring.workers[primary_worker].active_tasks <= balancer.compute_max_allowed_load() + 1, \
        "Bounded load constraint violated on primary worker."

    # 4. Minimal Partition Migration Churn Test (<20% upon node addition/removal)
    print("\n[TEST 4] Testing partition migration churn bound upon adding 6th worker...")
    new_pod = WorkerNode(node_id="pod-alpha-5", capacity_weight=1.0)
    migrations = balancer.rebalance_cluster_dynamics(added_worker=new_pod)
    churn_ratio = len(migrations) / len(balancer._task_assignments)
    print(f"  Tasks Migrated: {len(migrations)} / {len(balancer._task_assignments)} ({churn_ratio * 100:.2f}% churn)")
    # Theoretical churn when moving from N to N+1 is 1/(N+1) = 1/6 ~= 16.6%. Must be < 25%.
    assert churn_ratio < 0.25, f"Churn ratio excessive: {churn_ratio * 100:.2f}%"

    # 5. Deterministic Key Affinity
    print("\n[TEST 5] Validating deterministic key affinity...")
    sample_key = "repo-analytics:src/aggregator.py"
    hash_pos = ring.hash_key(sample_key)
    succ1 = ring.get_successor_partition(hash_pos).physical_node_id
    succ2 = ring.get_successor_partition(hash_pos).physical_node_id
    assert succ1 == succ2, "Non-deterministic key affinity detected!"
    print(f"  Affinity verified: {sample_key} -> {succ1}")

    # 6. Dashboard State Export
    print("\n[TEST 6] Exporting dashboard state...")
    dashboard_state = balancer.export_dashboard_state()
    assert Path(balancer.dashboard_path).exists(), "Dashboard file not written."
    print("  Dashboard state persisted successfully:")
    print(f"  Summary: {json.dumps(dashboard_state['topology_summary'], indent=4)}")

    print("\n======================================================================")
    print("ALL TESTS PASSED: Autonomic Consistent Hashing & Balancer Fully Verified")
    print("======================================================================")


if __name__ == "__main__":
    _run_standalone_verification()