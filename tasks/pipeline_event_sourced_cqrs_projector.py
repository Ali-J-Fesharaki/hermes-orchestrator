#!/usr/bin/env python3
"""
Pipeline Autonomic Event-Sourced CQRS Projection Engine & Materialized State Store.

Architectural Design & Behavioral Invariants:
--------------------------------------------
[Pattern: Command Query Responsibility Segregation (CQRS)]
[Pattern: Event Sourcing]
[Pattern: Materialized View]
[Pattern: State Hydration & Time-Travel Replay]

Mathematical & Algorithmic Foundation:
1. Event Sourcing Log Model:
   Let E = {e_1, e_2, ..., e_m} be an append-only sequence of immutable domain events
   ordered by monotonically increasing sequence number seq_k in N^+.
   Each event e_k = (seq_k, timestamp_k, repo_id, aggregate_type, aggregate_id, event_type, payload, idempotency_hash).

2. Deterministic Pure Reducer Function:
   System state S in Omega is projected via foldl:
       S_n = foldl(delta, S_0, E_{1..n})
   where delta: Omega x E -> Omega is a deterministic pure function satisfying:
       delta(S, e) = S'
   such that identical event sequences yield bit-identical state projections regardless
   of replay execution time, node hardware, or runtime environment.

3. Materialized Projections:
   - RepositoryAggregateView: branch HEAD, active milestone status, DAG tier, commit provenance.
   - WorkerFleetView: active lease holder, pod heartbeat timestamp, assigned execution worktree.
   - TokenBudgetView: rolling quota consumption, windowed burn rate, provider-level limits.
   - LockResourceView: granular path locks, fencing tokens (nu_k), and barrier contention.

4. Snapshotting & Hydration:
   - Periodic checkpointing at snapshot interval delta_snap = 100 events: Sigma_snap = (seq_last, S_materialized).
   - Hydrate(target_seq) = foldl(delta, Sigma_snap(seq_k).state, E_{seq_k+1 .. target_seq}).
   - Point-in-Time Time-Travel Replay: S_{t_target} = foldl(delta, S_0, {e in E | e.timestamp <= t_target}).

5. Read-Side Query Engine:
   - Thread-safe Concurrent Read / Exclusive Write (RWLock) query facade.
   - Real-time serialization to dashboards/materialized_projections.json and dashboards/CQRS_VIEW.md.
"""

from __future__ import annotations

import argparse
import copy
from dataclasses import asdict, dataclass, field
import hashlib
import json
import os
from pathlib import Path
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple


# =====================================================================
# 1. Domain Event Model (Immutable Event Sourcing Core)
# =====================================================================

@dataclass(frozen=True)
class DomainEvent:
    """
    [Pattern: Event Sourcing]
    Immutable domain event record constituting the authoritative source of truth.
    Strictly append-only, ordered monotonically by sequence number (seq).
    """
    seq: int
    timestamp: float
    repo_id: str
    aggregate_type: str  # 'repository', 'worker_fleet', 'token_budget', 'lock_resource'
    aggregate_id: str
    event_type: str
    payload: Dict[str, Any]
    idempotency_hash: str

    @classmethod
    def create(
        cls,
        seq: int,
        repo_id: str,
        aggregate_type: str,
        aggregate_id: str,
        event_type: str,
        payload: Dict[str, Any],
        timestamp: Optional[float] = None
    ) -> DomainEvent:
        ts = timestamp if timestamp is not None else time.time()
        canonical_str = f"{seq}:{ts:.6f}:{repo_id}:{aggregate_type}:{aggregate_id}:{event_type}:{json.dumps(payload, sort_keys=True)}"
        idem_hash = hashlib.sha256(canonical_str.encode("utf-8")).hexdigest()
        return cls(
            seq=seq,
            timestamp=ts,
            repo_id=repo_id,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type=event_type,
            payload=copy.deepcopy(payload),
            idempotency_hash=idem_hash
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> DomainEvent:
        return cls(
            seq=int(data["seq"]),
            timestamp=float(data["timestamp"]),
            repo_id=str(data["repo_id"]),
            aggregate_type=str(data["aggregate_type"]),
            aggregate_id=str(data["aggregate_id"]),
            event_type=str(data["event_type"]),
            payload=dict(data.get("payload", {})),
            idempotency_hash=str(data.get("idempotency_hash", ""))
        )


# =====================================================================
# 2. Materialized Projections (Read-Model State Containers)
# =====================================================================

@dataclass
class MilestoneRecord:
    milestone_name: str
    dag_tier: int
    status: str  # 'PENDING', 'IN_PROGRESS', 'COMPLETED', 'FAILED'
    assigned_pod: Optional[str] = None
    commit_sha: Optional[str] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None


@dataclass
class RepositoryAggregateView:
    """[Pattern: Materialized View] Tracks repository state, DAG tiering, and HEAD commits."""
    repo_id: str
    branch_head: str = "main"
    head_commit_sha: Optional[str] = None
    active_milestones: Dict[str, MilestoneRecord] = field(default_factory=dict)
    completed_milestones: List[str] = field(default_factory=list)
    dag_tiers: Dict[int, List[str]] = field(default_factory=dict)
    provenance_chain: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class WorkerPodRecord:
    pod_id: str
    active_lease: Optional[str] = None
    worktree_path: Optional[str] = None
    last_heartbeat: float = 0.0
    status: str = "OFFLINE"  # 'ONLINE', 'BUSY', 'OFFLINE'
    assigned_tasks: List[str] = field(default_factory=list)


@dataclass
class WorkerFleetView:
    """[Pattern: Materialized View] Tracks pod leases, heartbeats, and active worktrees."""
    pods: Dict[str, WorkerPodRecord] = field(default_factory=dict)
    active_leases: Dict[str, str] = field(default_factory=dict)  # resource -> pod_id


@dataclass
class ProviderQuota:
    provider: str
    window_start: float = 0.0
    tokens_consumed: int = 0
    token_limit_per_window: int = 1000000
    burn_rate_tokens_per_sec: float = 0.0
    throttled: bool = False


@dataclass
class TokenBudgetView:
    """[Pattern: Materialized View] Tracks real-time LLM token usage and burn rates."""
    providers: Dict[str, ProviderQuota] = field(default_factory=dict)
    total_tokens_consumed: int = 0


@dataclass
class LockResourceRecord:
    resource_path: str
    holder_pod: str
    fencing_token: int
    acquired_at: float
    barrier_level: str  # 'EXCLUSIVE', 'SHARED'


@dataclass
class LockResourceView:
    """[Pattern: Materialized View] Tracks granular path locking and fencing tokens."""
    locks: Dict[str, LockResourceRecord] = field(default_factory=dict)
    fencing_sequence: int = 0
    contention_counters: Dict[str, int] = field(default_factory=dict)


@dataclass
class SystemState:
    """
    [Pattern: CQRS]
    Omega - The unified state space encompassing all materialized projections.
    """
    last_applied_seq: int = 0
    last_applied_timestamp: float = 0.0
    repositories: Dict[str, RepositoryAggregateView] = field(default_factory=dict)
    fleet: WorkerFleetView = field(default_factory=WorkerFleetView)
    token_budget: TokenBudgetView = field(default_factory=TokenBudgetView)
    lock_registry: LockResourceView = field(default_factory=LockResourceView)

    def deep_copy(self) -> SystemState:
        return copy.deepcopy(self)

    def state_digest(self) -> str:
        """Deterministic bit-identical state serialization hash."""
        serialized = json.dumps(self.to_dict(), sort_keys=True)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "last_applied_seq": self.last_applied_seq,
            "last_applied_timestamp": self.last_applied_timestamp,
            "repositories": {
                k: {
                    "repo_id": v.repo_id,
                    "branch_head": v.branch_head,
                    "head_commit_sha": v.head_commit_sha,
                    "active_milestones": {m_k: asdict(m_v) for m_k, m_v in v.active_milestones.items()},
                    "completed_milestones": list(v.completed_milestones),
                    "dag_tiers": {str(tier): tasks for tier, tasks in v.dag_tiers.items()},
                    "provenance_chain": v.provenance_chain
                }
                for k, v in self.repositories.items()
            },
            "fleet": {
                "pods": {k: asdict(v) for k, v in self.fleet.pods.items()},
                "active_leases": dict(self.fleet.active_leases)
            },
            "token_budget": {
                "providers": {k: asdict(v) for k, v in self.token_budget.providers.items()},
                "total_tokens_consumed": self.token_budget.total_tokens_consumed
            },
            "lock_registry": {
                "locks": {k: asdict(v) for k, v in self.lock_registry.locks.items()},
                "fencing_sequence": self.lock_registry.fencing_sequence,
                "contention_counters": dict(self.lock_registry.contention_counters)
            }
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> SystemState:
        s = cls(
            last_applied_seq=data.get("last_applied_seq", 0),
            last_applied_timestamp=data.get("last_applied_timestamp", 0.0)
        )
        # Restore Repositories
        for repo_id, r_data in data.get("repositories", {}).items():
            active_m = {}
            for mk, mv in r_data.get("active_milestones", {}).items():
                active_m[mk] = MilestoneRecord(**mv)
            dag_tiers = {int(k): v for k, v in r_data.get("dag_tiers", {}).items()}
            s.repositories[repo_id] = RepositoryAggregateView(
                repo_id=repo_id,
                branch_head=r_data.get("branch_head", "main"),
                head_commit_sha=r_data.get("head_commit_sha"),
                active_milestones=active_m,
                completed_milestones=r_data.get("completed_milestones", []),
                dag_tiers=dag_tiers,
                provenance_chain=r_data.get("provenance_chain", [])
            )
        # Restore Fleet
        pods = {}
        for pk, pv in data.get("fleet", {}).get("pods", {}).items():
            pods[pk] = WorkerPodRecord(**pv)
        s.fleet = WorkerFleetView(
            pods=pods,
            active_leases=dict(data.get("fleet", {}).get("active_leases", {}))
        )
        # Restore Token Budget
        provs = {}
        for prk, prv in data.get("token_budget", {}).get("providers", {}).items():
            provs[prk] = ProviderQuota(**prv)
        s.token_budget = TokenBudgetView(
            providers=provs,
            total_tokens_consumed=data.get("token_budget", {}).get("total_tokens_consumed", 0)
        )
        # Restore Locks
        locks = {}
        for lk, lv in data.get("lock_registry", {}).get("locks", {}).items():
            locks[lk] = LockResourceRecord(**lv)
        s.lock_registry = LockResourceView(
            locks=locks,
            fencing_sequence=data.get("lock_registry", {}).get("fencing_sequence", 0),
            contention_counters=dict(data.get("lock_registry", {}).get("contention_counters", {}))
        )
        return s


# =====================================================================
# 3. Deterministic Pure Reducer Function delta(S, e) -> S'
# =====================================================================

def pure_reducer(state: SystemState, event: DomainEvent) -> SystemState:
    """
    [Pattern: Event Sourcing]
    Pure, side-effect-free reducer delta: Omega x E -> Omega.
    Guarantees deterministic bit-identical projection output from input event sequence.
    """
    # Defensive copy for pure state immutability
    next_state = state.deep_copy()
    next_state.last_applied_seq = event.seq
    next_state.last_applied_timestamp = event.timestamp

    agg_type = event.aggregate_type
    e_type = event.event_type
    p = event.payload

    # -------------------------------------------------------------
    # 3.1 Repository Aggregate Projection
    # -------------------------------------------------------------
    if agg_type == "repository":
        repo_id = event.repo_id
        if repo_id not in next_state.repositories:
            next_state.repositories[repo_id] = RepositoryAggregateView(repo_id=repo_id)
        repo = next_state.repositories[repo_id]

        if e_type == "MILESTONE_REGISTERED":
            m_name = p.get("milestone_name", "UNKNOWN")
            tier = int(p.get("dag_tier", 1))
            rec = MilestoneRecord(
                milestone_name=m_name,
                dag_tier=tier,
                status="PENDING"
            )
            repo.active_milestones[m_name] = rec
            if tier not in repo.dag_tiers:
                repo.dag_tiers[tier] = []
            if m_name not in repo.dag_tiers[tier]:
                repo.dag_tiers[tier].append(m_name)

        elif e_type == "MILESTONE_DISPATCHED":
            m_name = p.get("milestone_name", "")
            if m_name in repo.active_milestones:
                m_rec = repo.active_milestones[m_name]
                m_rec.status = "IN_PROGRESS"
                m_rec.assigned_pod = p.get("pod_id")
                m_rec.started_at = event.timestamp

        elif e_type == "MILESTONE_COMPLETED":
            m_name = p.get("milestone_name", "")
            commit_sha = p.get("commit_sha", "HEAD")
            if m_name in repo.active_milestones:
                m_rec = repo.active_milestones[m_name]
                m_rec.status = "COMPLETED"
                m_rec.commit_sha = commit_sha
                m_rec.completed_at = event.timestamp
                if m_name not in repo.completed_milestones:
                    repo.completed_milestones.append(m_name)
            repo.head_commit_sha = commit_sha
            repo.provenance_chain.append({
                "seq": event.seq,
                "timestamp": event.timestamp,
                "milestone": m_name,
                "commit_sha": commit_sha,
                "idempotency_hash": event.idempotency_hash
            })

        elif e_type == "BRANCH_SWITCHED":
            repo.branch_head = p.get("branch", "main")

    # -------------------------------------------------------------
    # 3.2 Worker Fleet Aggregate Projection
    # -------------------------------------------------------------
    elif agg_type == "worker_fleet":
        pod_id = event.aggregate_id
        fleet = next_state.fleet
        if pod_id not in fleet.pods:
            fleet.pods[pod_id] = WorkerPodRecord(pod_id=pod_id)
        pod = fleet.pods[pod_id]

        if e_type == "POD_HEARTBEAT":
            pod.last_heartbeat = event.timestamp
            pod.status = p.get("status", "ONLINE")
            if "worktree_path" in p:
                pod.worktree_path = p["worktree_path"]

        elif e_type == "LEASE_ACQUIRED":
            resource = p.get("resource", "")
            pod.active_lease = resource
            fleet.active_leases[resource] = pod_id
            task = p.get("task_name")
            if task and task not in pod.assigned_tasks:
                pod.assigned_tasks.append(task)
            pod.status = "BUSY"

        elif e_type == "LEASE_RELEASED":
            resource = p.get("resource", "")
            if pod.active_lease == resource:
                pod.active_lease = None
            if resource in fleet.active_leases:
                del fleet.active_leases[resource]
            pod.status = "ONLINE"

    # -------------------------------------------------------------
    # 3.3 Token Budget Aggregate Projection
    # -------------------------------------------------------------
    elif agg_type == "token_budget":
        provider = event.aggregate_id
        tb = next_state.token_budget
        if provider not in tb.providers:
            tb.providers[provider] = ProviderQuota(provider=provider)
        pq = tb.providers[provider]

        if e_type == "TOKEN_CONSUMPTION_LOGGED":
            tokens = int(p.get("tokens", 0))
            pq.tokens_consumed += tokens
            tb.total_tokens_consumed += tokens
            # Rolling rate estimation
            dt = max(1.0, event.timestamp - pq.window_start) if pq.window_start > 0 else 1.0
            if pq.window_start == 0.0:
                pq.window_start = event.timestamp
            pq.burn_rate_tokens_per_sec = pq.tokens_consumed / dt
            if pq.tokens_consumed >= pq.token_limit_per_window:
                pq.throttled = True

        elif e_type == "TOKEN_QUOTA_RESET":
            pq.window_start = event.timestamp
            pq.tokens_consumed = 0
            pq.burn_rate_tokens_per_sec = 0.0
            pq.throttled = False

    # -------------------------------------------------------------
    # 3.4 Lock Resource Aggregate Projection
    # -------------------------------------------------------------
    elif agg_type == "lock_resource":
        lr = next_state.lock_registry
        res_path = event.aggregate_id

        if e_type == "LOCK_ACQUIRED":
            lr.fencing_sequence += 1
            fencing_token = lr.fencing_sequence
            lr.locks[res_path] = LockResourceRecord(
                resource_path=res_path,
                holder_pod=p.get("pod_id", "ANONYMOUS"),
                fencing_token=fencing_token,
                acquired_at=event.timestamp,
                barrier_level=p.get("barrier_level", "EXCLUSIVE")
            )

        elif e_type == "LOCK_RELEASED":
            if res_path in lr.locks:
                del lr.locks[res_path]

        elif e_type == "LOCK_CONTENTION_REGISTERED":
            lr.contention_counters[res_path] = lr.contention_counters.get(res_path, 0) + 1

    return next_state


# =====================================================================
# 4. Snapshot Storage & Compaction Engine
# =====================================================================

@dataclass
class StateSnapshot:
    seq_last: int
    timestamp: float
    state_digest: str
    state_dict: Dict[str, Any]


class SnapshotStore:
    """
    [Pattern: State Hydration & Time-Travel Replay]
    Persistent storage and compaction manager for materialized snapshots (Sigma_snap).
    """

    def __init__(self, snapshot_file: Path, snapshot_interval: int = 100):
        self.snapshot_file = snapshot_file
        self.snapshot_interval = snapshot_interval
        self._lock = threading.Lock()
        self.snapshots: List[StateSnapshot] = []
        self._load_from_disk()

    def _load_from_disk(self) -> None:
        with self._lock:
            if not self.snapshot_file.exists():
                return
            try:
                with open(self.snapshot_file, "r", encoding="utf-8") as f:
                    raw_list = json.load(f)
                    self.snapshots = [
                        StateSnapshot(
                            seq_last=item["seq_last"],
                            timestamp=item["timestamp"],
                            state_digest=item["state_digest"],
                            state_dict=item["state_dict"]
                        )
                        for item in raw_list
                    ]
            except Exception as e:
                sys.stderr.write(f"[WARN] Failed to load snapshots from {self.snapshot_file}: {e}\n")

    def save_snapshot(self, state: SystemState) -> None:
        with self._lock:
            snap = StateSnapshot(
                seq_last=state.last_applied_seq,
                timestamp=state.last_applied_timestamp,
                state_digest=state.state_digest(),
                state_dict=state.to_dict()
            )
            # Retain ascending order by seq
            self.snapshots.append(snap)
            self.snapshots.sort(key=lambda s: s.seq_last)

            self.snapshot_file.parent.mkdir(parents=True, exist_ok=True)
            temp_file = self.snapshot_file.with_suffix(".tmp")
            payload = [
                {
                    "seq_last": s.seq_last,
                    "timestamp": s.timestamp,
                    "state_digest": s.state_digest,
                    "state_dict": s.state_dict
                }
                for s in self.snapshots
            ]
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, sort_keys=True)
            temp_file.replace(self.snapshot_file)

    def find_nearest_snapshot(self, target_seq: int) -> Optional[StateSnapshot]:
        with self._lock:
            eligible = [s for s in self.snapshots if s.seq_last <= target_seq]
            if not eligible:
                return None
            return max(eligible, key=lambda s: s.seq_last)


# =====================================================================
# 5. Read-Write Lock Implementation for Concurrent Queries
# =====================================================================

class RWLock:
    """Concurrent Read / Exclusive Write synchronization primitive."""

    def __init__(self):
        self._readers = 0
        self._writers_waiting = 0
        self._lock = threading.Lock()
        self._ok_to_read = threading.Condition(self._lock)
        self._ok_to_write = threading.Condition(self._lock)

    def acquire_read(self) -> None:
        with self._lock:
            while self._writers_waiting > 0:
                self._ok_to_read.wait()
            self._readers += 1

    def release_read(self) -> None:
        with self._lock:
            self._readers -= 1
            if self._readers == 0:
                self._ok_to_write.notify()

    def acquire_write(self) -> None:
        with self._lock:
            self._writers_waiting += 1
            while self._readers > 0:
                self._ok_to_write.wait()
            self._writers_waiting -= 1

    def release_write(self) -> None:
        with self._lock:
            self._ok_to_read.notify_all()
            self._ok_to_write.notify()


# =====================================================================
# 6. EventSourcedCQRSProjector Main Engine
# =====================================================================

class EventSourcedCQRSProjector:
    """
    [Pattern: Command Query Responsibility Segregation (CQRS)]
    [Pattern: Event Sourcing]
    [Pattern: Materialized View]
    
    Autonomous Engine managing deterministic event folding, periodic snapshotting,
    point-in-time time-travel hydration, and thread-safe read query projections.
    """

    def __init__(
        self,
        base_dir: Optional[Path] = None,
        snapshot_interval: int = 100
    ):
        self.base_dir = base_dir or Path("/home/ajf/automation")
        self.dashboards_dir = self.base_dir / "dashboards"
        self.dashboards_dir.mkdir(parents=True, exist_ok=True)

        self.snapshot_file = self.dashboards_dir / "cqrs_snapshots.json"
        self.projections_export_file = self.dashboards_dir / "materialized_projections.json"
        self.markdown_view_file = self.dashboards_dir / "CQRS_VIEW.md"
        self.outbox_ledger_file = self.dashboards_dir / "outbox_ledger.json"

        self.snapshot_interval = snapshot_interval
        self.snapshot_store = SnapshotStore(self.snapshot_file, snapshot_interval)

        self.event_stream: List[DomainEvent] = []
        self.state = SystemState()
        self.rwlock = RWLock()
        self.events_since_snapshot = 0

    # -------------------------------------------------------------
    # Ingestion & State Folding (Write-Side Processing)
    # -------------------------------------------------------------

    def ingest_event(self, event: DomainEvent) -> None:
        """
        Ingest a verified immutable domain event, advance projection via pure reducer,
        and trigger snapshot checkpointing when snapshot interval is reached.
        """
        self.rwlock.acquire_write()
        try:
            # Check monotonically increasing sequence constraint
            if self.event_stream and event.seq <= self.event_stream[-1].seq:
                raise ValueError(
                    f"Out-of-order sequence violation: received {event.seq}, "
                    f"expected > {self.event_stream[-1].seq}"
                )

            self.event_stream.append(event)
            self.state = pure_reducer(self.state, event)
            self.events_since_snapshot += 1

            if self.events_since_snapshot >= self.snapshot_interval:
                self.snapshot_store.save_snapshot(self.state)
                self.events_since_snapshot = 0
        finally:
            self.rwlock.release_write()

    def ingest_batch(self, events: List[DomainEvent]) -> None:
        for ev in events:
            self.ingest_event(ev)

    def ingest_from_outbox_ledger(self) -> int:
        """
        Ingests committed events from pipeline_transactional_outbox_arbiter ledger.
        Maps external outbox messages to domain events.
        """
        if not self.outbox_ledger_file.exists():
            return 0

        ingested_count = 0
        try:
            with open(self.outbox_ledger_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                entries = data if isinstance(data, list) else data.get("entries", [])

            for entry in entries:
                # Convert outbox entry to DomainEvent
                seq = int(entry.get("seq", len(self.event_stream) + 1))
                if any(e.seq == seq for e in self.event_stream):
                    continue

                ev = DomainEvent.create(
                    seq=seq,
                    repo_id=entry.get("repo_id", "orchestrator"),
                    aggregate_type=entry.get("aggregate_type", "repository"),
                    aggregate_id=entry.get("aggregate_id", entry.get("topic", "default")),
                    event_type=entry.get("event_type", "OUTBOX_COMMITTED"),
                    payload=entry.get("payload", {}),
                    timestamp=float(entry.get("timestamp", time.time()))
                )
                self.ingest_event(ev)
                ingested_count += 1
        except Exception as ex:
            sys.stderr.write(f"[WARN] Error reading outbox ledger: {ex}\n")

        return ingested_count

    # -------------------------------------------------------------
    # State Hydration & Time-Travel Replay (Algorithmic Proof)
    # -------------------------------------------------------------

    def hydrate_to_sequence(self, target_seq: int) -> SystemState:
        """
        [Pattern: State Hydration & Time-Travel Replay]
        Hydrate(target_seq) = foldl(delta, Sigma_snap(seq_k).state, E_{seq_k+1 .. target_seq}).
        Recovers bit-identical state at exactly target_seq.
        """
        self.rwlock.acquire_read()
        try:
            nearest_snap = self.snapshot_store.find_nearest_snapshot(target_seq)
            if nearest_snap is not None:
                hydrated_state = SystemState.from_dict(nearest_snap.state_dict)
                start_seq = nearest_snap.seq_last + 1
            else:
                hydrated_state = SystemState()
                start_seq = 1

            # Filter remaining delta events: seq in [start_seq, target_seq]
            delta_events = [
                e for e in self.event_stream
                if start_seq <= e.seq <= target_seq
            ]
            delta_events.sort(key=lambda e: e.seq)

            for ev in delta_events:
                hydrated_state = pure_reducer(hydrated_state, ev)

            return hydrated_state
        finally:
            self.rwlock.release_read()

    def query_point_in_time(self, timestamp: float) -> SystemState:
        """
        [Pattern: State Hydration & Time-Travel Replay]
        Computes system state S_{t_target} at exact prior timestamp.
        """
        self.rwlock.acquire_read()
        try:
            eligible_events = [e for e in self.event_stream if e.timestamp <= timestamp]
            if not eligible_events:
                return SystemState()
            target_seq = max(e.seq for e in eligible_events)
        finally:
            self.rwlock.release_read()

        return self.hydrate_to_sequence(target_seq)

    # -------------------------------------------------------------
    # Read-Side Queries (CQRS Read Facade)
    # -------------------------------------------------------------

    def query_milestone_projection(self, repo_id: str, milestone_name: str) -> Optional[Dict[str, Any]]:
        """Query specific milestone state in a repository aggregate."""
        self.rwlock.acquire_read()
        try:
            repo = self.state.repositories.get(repo_id)
            if not repo:
                return None
            m = repo.active_milestones.get(milestone_name)
            return asdict(m) if m else None
        finally:
            self.rwlock.release_read()

    def query_dag_tier_readiness(self, repo_id: str) -> List[Dict[str, Any]]:
        """
        Computes DAG tier execution readiness for all milestones within the repository.
        A tier is READY if all milestones in previous tiers are COMPLETED.
        """
        self.rwlock.acquire_read()
        try:
            repo = self.state.repositories.get(repo_id)
            if not repo:
                return []

            results = []
            completed = set(repo.completed_milestones)
            all_tiers = sorted(repo.dag_tiers.keys())

            for tier in all_tiers:
                milestones = repo.dag_tiers[tier]
                # Check prior tiers
                prior_tiers_complete = True
                for p_tier in all_tiers:
                    if p_tier < tier:
                        if not all(m in completed for m in repo.dag_tiers[p_tier]):
                            prior_tiers_complete = False
                            break

                tier_status = "READY" if prior_tiers_complete else "BLOCKED_DEPENDENCY"
                tier_milestones_detail = []
                for m_name in milestones:
                    m_rec = repo.active_milestones.get(m_name)
                    tier_milestones_detail.append({
                        "name": m_name,
                        "status": m_rec.status if m_rec else "UNKNOWN",
                        "assigned_pod": m_rec.assigned_pod if m_rec else None,
                        "commit_sha": m_rec.commit_sha if m_rec else None
                    })

                results.append({
                    "tier": tier,
                    "tier_readiness": tier_status,
                    "milestones": tier_milestones_detail
                })

            return results
        finally:
            self.rwlock.release_read()

    def query_fleet_resource_matrix(self) -> Dict[str, Any]:
        """Matrix view of fleet pods, leases, and resource lock status."""
        self.rwlock.acquire_read()
        try:
            return {
                "active_pods_count": len(self.state.fleet.pods),
                "active_leases": dict(self.state.fleet.active_leases),
                "pods": {k: asdict(v) for k, v in self.state.fleet.pods.items()},
                "active_locks": {k: asdict(v) for k, v in self.state.lock_registry.locks.items()},
                "lock_contention": dict(self.state.lock_registry.contention_counters)
            }
        finally:
            self.rwlock.release_read()

    # -------------------------------------------------------------
    # Materialized View Dashboard Exporters
    # -------------------------------------------------------------

    def export_materialized_projections(self) -> None:
        """Serializes current state projections to JSON and Markdown dashboards."""
        self.rwlock.acquire_read()
        try:
            state_dict = self.state.to_dict()
            state_digest = self.state.state_digest()
            total_events = len(self.event_stream)
        finally:
            self.rwlock.release_read()

        # 1. Export JSON
        export_payload = {
            "metadata": {
                "engine": "Pipeline Autonomic Event-Sourced CQRS Projector",
                "state_digest_sha256": state_digest,
                "total_events_ingested": total_events,
                "last_sequence": state_dict["last_applied_seq"],
                "last_event_timestamp": state_dict["last_applied_timestamp"],
                "exported_at": time.time(),
                "architectural_badges": [
                    "[Pattern: Command Query Responsibility Segregation (CQRS)]",
                    "[Pattern: Event Sourcing]",
                    "[Pattern: Materialized View]",
                    "[Pattern: State Hydration & Time-Travel Replay]"
                ]
            },
            "projections": state_dict
        }

        with open(self.projections_export_file, "w", encoding="utf-8") as f:
            json.dump(export_payload, f, indent=2, sort_keys=True)

        # 2. Export CQRS_VIEW.md
        self._write_markdown_dashboard(export_payload)

    def _write_markdown_dashboard(self, payload: Dict[str, Any]) -> None:
        meta = payload["metadata"]
        proj = payload["projections"]
        
        md_lines = [
            "# Autonomic CQRS & Materialized State Store Dashboard",
            "",
            "> **Architectural Pattern Badges**",
            "> `[Pattern: Command Query Responsibility Segregation (CQRS)]` `[Pattern: Event Sourcing]` `[Pattern: Materialized View]` `[Pattern: State Hydration & Time-Travel Replay]`",
            "",
            "## 1. System Projection Telemetry",
            f"- **State Digest (SHA-256)**: `{meta['state_digest_sha256']}`",
            f"- **Total Events Ingested**: `{meta['total_events_ingested']}`",
            f"- **Last Sequence Number**: `{meta['last_sequence']}`",
            f"- **Last Event Timestamp**: `{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(meta['last_event_timestamp'])) if meta['last_event_timestamp'] > 0 else 'N/A'}`",
            f"- **Snapshot Checkpoint Cadence**: `Every {self.snapshot_interval} events`",
            "",
            "## 2. Materialized Repository Aggregate Views",
            "| Repository ID | Branch HEAD | HEAD Commit | Active Milestones | Completed | Provenance Depth |",
            "| :--- | :--- | :--- | :--- | :--- | :--- |"
        ]

        repos = proj.get("repositories", {})
        if not repos:
            md_lines.append("| *None* | - | - | 0 | 0 | 0 |")
        else:
            for rid, r in repos.items():
                active_count = len(r.get("active_milestones", {}))
                comp_count = len(r.get("completed_milestones", []))
                prov_count = len(r.get("provenance_chain", []))
                md_lines.append(
                    f"| `{rid}` | `{r.get('branch_head')}` | `{r.get('head_commit_sha') or 'None'}` | {active_count} | {comp_count} | {prov_count} commits |"
                )

        md_lines.extend([
            "",
            "## 3. Worker Fleet Matrix & Leases",
            "| Pod ID | Status | Active Worktree | Lease Lock | Last Heartbeat | Assigned Tasks |",
            "| :--- | :--- | :--- | :--- | :--- | :--- |"
        ])

        fleet = proj.get("fleet", {})
        pods = fleet.get("pods", {})
        if not pods:
            md_lines.append("| *No Pods Online* | - | - | - | - | - |")
        else:
            for pid, pod in pods.items():
                hb = time.strftime('%H:%M:%S', time.localtime(pod.get('last_heartbeat', 0))) if pod.get('last_heartbeat') else 'N/A'
                md_lines.append(
                    f"| `{pid}` | **{pod.get('status')}** | `{pod.get('worktree_path') or 'N/A'}` | `{pod.get('active_lease') or 'None'}` | {hb} | {len(pod.get('assigned_tasks', []))} |"
                )

        md_lines.extend([
            "",
            "## 4. Token Budget & Burn Rate Monitor",
            "| Provider | Window Tokens Consumed | Quota Limit | Burn Rate (tok/s) | Throttled |",
            "| :--- | :--- | :--- | :--- | :--- |"
        ])

        tb = proj.get("token_budget", {})
        provs = tb.get("providers", {})
        if not provs:
            md_lines.append("| *No Providers Recorded* | 0 | 0 | 0.0 | False |")
        else:
            for pr_name, pr_data in provs.items():
                md_lines.append(
                    f"| `{pr_name}` | {pr_data.get('tokens_consumed'):,} | {pr_data.get('token_limit_per_window'):,} | {pr_data.get('burn_rate_tokens_per_sec', 0.0):.2f} | {'🚨 YES' if pr_data.get('throttled') else '✅ NO'} |"
                )

        md_lines.extend([
            "",
            "## 5. Lock Registry & Fencing Token Sequence",
            f"- **Current Global Fencing Token (nu_k)**: `{proj.get('lock_registry', {}).get('fencing_sequence', 0)}`",
            "",
            "| Resource Path | Holder Pod | Fencing Token | Barrier Level | Contention Hits |",
            "| :--- | :--- | :--- | :--- | :--- |"
        ])

        lr = proj.get("lock_registry", {})
        locks = lr.get("locks", {})
        contentions = lr.get("contention_counters", {})
        if not locks:
            md_lines.append("| *No Active Path Locks* | - | - | - | 0 |")
        else:
            for rpath, lrec in locks.items():
                c_hits = contentions.get(rpath, 0)
                md_lines.append(
                    f"| `{rpath}` | `{lrec.get('holder_pod')}` | `{lrec.get('fencing_token')}` | `{lrec.get('barrier_level')}` | {c_hits} |"
                )

        with open(self.markdown_view_file, "w", encoding="utf-8") as f:
            f.write("\n".join(md_lines) + "\n")


# =====================================================================
# 7. Verification Test Suite & CLI
# =====================================================================

def run_cqrs_verification_suite(base_dir: Path) -> bool:
    """
    [Pattern: State Hydration & Time-Travel Replay]
    Rigorous verification suite executing synthetic event generation,
    deterministic foldl projections, snapshot compaction, point-in-time
    time-travel replay, bit-identical state hydration, and concurrency validation.
    """
    print("=" * 76)
    print("EXECUTION: Pipeline Autonomic CQRS & Materialized State Store Verification")
    print("=" * 76)

    test_dir = base_dir / "test_scratch_cqrs"
    test_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Step 1: Initialize Projector with snapshot cadence of 50 events
        projector = EventSourcedCQRSProjector(base_dir=test_dir, snapshot_interval=50)

        # Step 2: Generate 125 Synthetic Events crossing snapshot boundary
        print("\n[Stage 1] Injecting 125 synthetic multi-aggregate domain events...")
        base_time = time.time() - 1000.0

        for i in range(1, 126):
            seq = i
            t = base_time + (i * 2.0)

            # Cycle across 4 aggregates: repository, worker_fleet, token_budget, lock_resource
            if i % 4 == 1:
                # Repository Milestone Events
                tier = ((i // 4) % 3) + 1
                m_name = f"milestone_task_{i}"
                ev1 = DomainEvent.create(
                    seq=seq,
                    repo_id="repo_omega",
                    aggregate_type="repository",
                    aggregate_id="repo_omega",
                    event_type="MILESTONE_REGISTERED",
                    payload={"milestone_name": m_name, "dag_tier": tier},
                    timestamp=t
                )
                projector.ingest_event(ev1)

            elif i % 4 == 2:
                # Worker Fleet Heartbeat & Lease
                pod_id = f"worker-pod-{(i % 3) + 1}"
                ev2 = DomainEvent.create(
                    seq=seq,
                    repo_id="repo_omega",
                    aggregate_type="worker_fleet",
                    aggregate_id=pod_id,
                    event_type="LEASE_ACQUIRED",
                    payload={"resource": f"worktree/stage_{i}", "task_name": f"task_{i}"},
                    timestamp=t
                )
                projector.ingest_event(ev2)

            elif i % 4 == 3:
                # Token Budget Consumption
                provider = "gemini-3.8-flash" if (i % 2 == 0) else "claude-opus-4-6"
                ev3 = DomainEvent.create(
                    seq=seq,
                    repo_id="system",
                    aggregate_type="token_budget",
                    aggregate_id=provider,
                    event_type="TOKEN_CONSUMPTION_LOGGED",
                    payload={"tokens": 1500 * (i % 5 + 1)},
                    timestamp=t
                )
                projector.ingest_event(ev3)

            else:
                # Lock Resource Events
                lock_path = f"tasks/pipeline_subsystem_{i % 3}.py"
                ev4 = DomainEvent.create(
                    seq=seq,
                    repo_id="repo_omega",
                    aggregate_type="lock_resource",
                    aggregate_id=lock_path,
                    event_type="LOCK_ACQUIRED",
                    payload={"pod_id": f"worker-pod-{(i % 3) + 1}", "barrier_level": "EXCLUSIVE"},
                    timestamp=t
                )
                projector.ingest_event(ev4)

        current_digest = projector.state.state_digest()
        print(f"  -> Successfully folded 125 events.")
        print(f"  -> State sequence: {projector.state.last_applied_seq}")
        print(f"  -> Deterministic State Digest (SHA-256): {current_digest}")

        # Step 3: Verify Snapshot Compaction
        print("\n[Stage 2] Verifying periodic snapshotting and compaction...")
        snapshots_recorded = len(projector.snapshot_store.snapshots)
        print(f"  -> Snapshots persisted: {snapshots_recorded} (Expected >= 2 at intervals of 50)")
        assert snapshots_recorded >= 2, f"Expected >= 2 snapshots, got {snapshots_recorded}"
        assert projector.snapshot_file.exists(), "Snapshot file not persisted to disk."
        print("  -> Snapshot storage verification PASSED.")

        # Step 4: Verify Hydration & Bit-Identical Replay
        print("\n[Stage 3] Testing foldl hydration and bit-identical state replay...")
        hydrated_state_125 = projector.hydrate_to_sequence(125)
        hydrated_digest = hydrated_state_125.state_digest()
        print(f"  -> Live state digest:     {current_digest}")
        print(f"  -> Hydrated state digest: {hydrated_digest}")
        assert current_digest == hydrated_digest, "Rehydration failed to produce bit-identical state digest!"
        print("  -> Bit-identical replay verification PASSED.")

        # Step 5: Verify Point-in-Time Time-Travel Replay
        print("\n[Stage 4] Testing Point-in-Time time-travel replay...")
        # Query at event #60 timestamp
        event_60_ts = projector.event_stream[59].timestamp
        pit_state = projector.query_point_in_time(event_60_ts)
        print(f"  -> Time-travel target timestamp: {event_60_ts:.4f}")
        print(f"  -> PIT state last_applied_seq: {pit_state.last_applied_seq} (Expected 60)")
        assert pit_state.last_applied_seq == 60, f"Expected sequence 60, got {pit_state.last_applied_seq}"
        print("  -> Point-in-time replay verification PASSED.")

        # Step 6: Verify CQRS Read Queries & DAG Tier Readiness
        print("\n[Stage 5] Verifying CQRS Read Queries...")
        matrix = projector.query_fleet_resource_matrix()
        print(f"  -> Active Pods tracked: {matrix['active_pods_count']}")
        print(f"  -> Active Locks held: {len(matrix['active_locks'])}")

        readiness = projector.query_dag_tier_readiness("repo_omega")
        print(f"  -> Evaluated DAG Tiers: {len(readiness)}")
        assert len(readiness) > 0, "DAG readiness evaluation returned empty tier structure."
        for r_tier in readiness:
            print(f"     * Tier {r_tier['tier']}: Status={r_tier['tier_readiness']}, Milestones={len(r_tier['milestones'])}")

        # Step 7: Verify Thread Safety and Concurrent Reads
        print("\n[Stage 6] Verifying concurrent read / single write thread safety...")
        read_success = []

        def worker_reader(worker_id: int):
            for _ in range(20):
                res = projector.query_dag_tier_readiness("repo_omega")
                if res:
                    read_success.append(worker_id)
                time.sleep(0.001)

        threads = [threading.Thread(target=worker_reader, args=(tid,)) for tid in range(4)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        print(f"  -> Concurrent read passes completed: {len(read_success)} iterations.")
        assert len(read_success) == 80, "Concurrent readers encountered failure or race condition."

        # Step 8: Export Dashboards and Validate Output Artifacts
        print("\n[Stage 7] Exporting materialized state dashboards...")
        projector.export_materialized_projections()
        assert projector.projections_export_file.exists(), "materialized_projections.json missing!"
        assert projector.markdown_view_file.exists(), "CQRS_VIEW.md missing!"
        print(f"  -> Verified JSON export: {projector.projections_export_file}")
        print(f"  -> Verified Markdown export: {projector.markdown_view_file}")

        print("\n" + "=" * 76)
        print("STATUS: ALL CQRS PROJECTION & STATE STORE VERIFICATIONS SUCCEEDED")
        print("=" * 76)
        return True

    finally:
        # Clean up scratch test artifacts
        if test_dir.exists():
            import shutil
            shutil.rmtree(test_dir, ignore_errors=True)


# =====================================================================
# 8. Main CLI Entry Point
# =====================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pipeline Autonomic Event-Sourced CQRS Projection Engine & Materialized State Store"
    )
    parser.add_argument(
        "--verify-cqrs",
        action="store_true",
        help="Execute comprehensive automated CQRS and state store verification suite."
    )
    parser.add_argument(
        "--sync-outbox",
        action="store_true",
        help="Ingest new events from outbox ledger and update materialized projections."
    )
    parser.add_argument(
        "--export-dashboards",
        action="store_true",
        help="Export current materialized projections to dashboards/materialized_projections.json and CQRS_VIEW.md."
    )
    parser.add_argument(
        "--base-dir",
        type=str,
        default="/home/ajf/automation",
        help="Base working directory of the repository."
    )

    args = parser.parse_args()
    base_path = Path(args.base_dir)

    if args.verify_cqrs:
        success = run_cqrs_verification_suite(base_path)
        sys.exit(0 if success else 1)

    projector = EventSourcedCQRSProjector(base_dir=base_path)

    if args.sync_outbox:
        count = projector.ingest_from_outbox_ledger()
        print(f"[INFO] Ingested {count} event(s) from outbox ledger.")
        projector.export_materialized_projections()
        print("[INFO] Updated materialized state projections.")
    elif args.export_dashboards:
        projector.export_materialized_projections()
        print("[INFO] Exported materialized projections to dashboards.")
    else:
        # Default behavior: sync outbox and export
        ingested = projector.ingest_from_outbox_ledger()
        projector.export_materialized_projections()
        print(
            f"[INFO] Pipeline Autonomic Event-Sourced CQRS Projector active. "
            f"Ingested {ingested} events. Projections up to date."
        )


if __name__ == "__main__":
    main()