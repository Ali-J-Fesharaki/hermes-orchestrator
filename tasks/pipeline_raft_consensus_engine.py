#!/usr/bin/env python3
"""
Pipeline Autonomic Raft Consensus Protocol & Replicated State Machine Log Engine
================================================================================

A discrete, event-driven, production-grade distributed consensus state machine
and replicated state machine log engine based on Ongaro & Ousterhout (2014).

Invariants & Core Architectural Guarantees:
1. Election Safety: At most one leader can be elected in a given term.
2. Leader Append-Only: A leader never overwrites or truncates its log; it only appends.
3. Log Matching Property: If two logs contain an entry with the same index and term,
   then the logs are identical in all entries up through the given index.
4. Leader Completeness: If a log entry is committed in a given term, that entry will
   be present in the logs of the leaders for all higher-numbered terms.
5. State Machine Safety: If a server has applied a log entry at a given index to its
   state machine, no other server will ever apply a different log entry for the same index.
6. Monotonic Log Compaction: Snapshotting truncates prefix entries while maintaining
   linearizable state transitions via InstallSnapshot RPC catch-up.

Author: Antigravity Autonomous Systems Architecture Team
License: Apache-2.0
"""

from __future__ import annotations

import asyncio
import copy
from dataclasses import asdict, dataclass, field
from enum import Enum
import json
import logging
from pathlib import Path
import random
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("RaftEngine")


# =====================================================================
# 1. Consensus Role Definitions & Network RPC Dataclasses
# =====================================================================

class Role(str, Enum):
    """Raft consensus roles according to Ongaro & Ousterhout."""
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


@dataclass
class LogEntry:
    """Represents a single replicated command entry within the Raft log."""
    term: int
    index: int
    command: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "term": self.term,
            "index": self.index,
            "command": copy.deepcopy(self.command),
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> LogEntry:
        return cls(
            term=data["term"],
            index=data["index"],
            command=data["command"],
            timestamp=data.get("timestamp", time.time()),
        )


@dataclass
class RequestVoteArgs:
    """RPC arguments dispatched by candidates during leader election."""
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class RequestVoteReply:
    """RPC reply returned by voting nodes."""
    term: int
    vote_granted: bool
    voter_id: str = ""


@dataclass
class AppendEntriesArgs:
    """RPC arguments dispatched by leaders for log replication and heartbeats."""
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[LogEntry]
    leader_commit: int


@dataclass
class AppendEntriesReply:
    """RPC reply returned by followers on AppendEntries."""
    term: int
    success: bool
    match_index: int = 0
    responder_id: str = ""


@dataclass
class InstallSnapshotArgs:
    """RPC arguments dispatched by leader to stream compacted snapshot to lagging peers."""
    term: int
    leader_id: str
    last_included_index: int
    last_included_term: int
    data: Dict[str, Any]


@dataclass
class InstallSnapshotReply:
    """RPC reply returned by follower upon installing a snapshot."""
    term: int
    responder_id: str = ""


@dataclass
class Snapshot:
    """Immutable persistent state capture for log compaction."""
    last_index: int
    last_term: int
    data: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)


# =====================================================================
# 2. Replicated State Machine (Deterministic Key-Value & Lease Store)
# =====================================================================

class ReplicatedStateMachine:
    """
    Deterministic in-memory key-value state machine supporting:
    - Standard key-value operations: SET, DELETE, CAS
    - Fleet Barrier Synchronization: REGISTER_BARRIER, RELEASE_BARRIER
    - Lease & Lock Arbitration: ACQUIRE_LEASE, RELEASE_LEASE
    - Git Branch & Head Manifests: COMMIT_HEAD
    """

    def __init__(self) -> None:
        self._state: Dict[str, Any] = {}
        self._barriers: Dict[str, Set[str]] = {}
        self._leases: Dict[str, Dict[str, Any]] = {}

    def apply(self, command: Dict[str, Any]) -> Dict[str, Any]:
        """Apply a single committed log entry command deterministically."""
        op = command.get("op", "").upper()
        key = command.get("key", "")
        value = command.get("value")
        actor_id = command.get("actor_id", "system")

        if op == "SET":
            self._state[key] = value
            return {"status": "OK", "op": op, "key": key, "value": value}

        elif op == "DELETE":
            existed = key in self._state
            val = self._state.pop(key, None)
            return {"status": "OK" if existed else "NOT_FOUND", "op": op, "key": key, "value": val}

        elif op == "CAS":
            expected = command.get("expected")
            current = self._state.get(key)
            if current == expected:
                self._state[key] = value
                return {"status": "OK", "op": op, "key": key, "value": value, "swapped": True}
            return {"status": "CAS_FAILED", "op": op, "key": key, "current": current, "swapped": False}

        elif op == "REGISTER_BARRIER":
            barrier_name = key
            threshold = int(command.get("threshold", 1))
            if barrier_name not in self._barriers:
                self._barriers[barrier_name] = set()
            self._barriers[barrier_name].add(actor_id)
            tripped = len(self._barriers[barrier_name]) >= threshold
            return {
                "status": "OK",
                "op": op,
                "barrier": barrier_name,
                "members": list(self._barriers[barrier_name]),
                "tripped": tripped,
            }

        elif op == "ACQUIRE_LEASE":
            lease_name = key
            ttl = float(command.get("ttl", 30.0))
            now = time.time()
            existing = self._leases.get(lease_name)
            if existing is None or existing["expires_at"] <= now or existing["owner"] == actor_id:
                self._leases[lease_name] = {"owner": actor_id, "expires_at": now + ttl}
                return {"status": "OK", "lease": lease_name, "owner": actor_id, "granted": True}
            return {"status": "LEASE_HELD", "lease": lease_name, "owner": existing["owner"], "granted": False}

        elif op == "RELEASE_LEASE":
            lease_name = key
            existing = self._leases.get(lease_name)
            if existing and existing["owner"] == actor_id:
                del self._leases[lease_name]
                return {"status": "OK", "lease": lease_name, "released": True}
            return {"status": "NOT_OWNER_OR_EXPIRED", "lease": lease_name, "released": False}

        elif op == "COMMIT_HEAD":
            branch = key
            commit_hash = str(value)
            self._state[f"refs/heads/{branch}"] = commit_hash
            return {"status": "OK", "branch": branch, "commit": commit_hash}

        elif op == "NOOP":
            return {"status": "OK", "op": "NOOP"}

        else:
            return {"status": "UNKNOWN_OP", "op": op}

    def get(self, key: str) -> Any:
        return self._state.get(key)

    def snapshot(self) -> Dict[str, Any]:
        """Create a deep copy representation of the current state machine."""
        return {
            "state": copy.deepcopy(self._state),
            "barriers": {k: list(v) for k, v in self._barriers.items()},
            "leases": copy.deepcopy(self._leases),
        }

    def restore(self, data: Dict[str, Any]) -> None:
        """Deterministic restoration from a snapshot dictionary."""
        self._state = copy.deepcopy(data.get("state", {}))
        self._barriers = {k: set(v) for k, v in data.get("barriers", {}).items()}
        self._leases = copy.deepcopy(data.get("leases", {}))


# =====================================================================
# 3. Raft Cluster Network Simulator
# =====================================================================

class RaftCluster:
    """
    Asynchronous in-process network topology simulator supporting:
    - Node registration and lookup
    - Configurable packet drops and transient latencies
    - Network partition simulation (split-brain & isolated quorum testing)
    - Bidirectional routing arbiter
    """

    def __init__(self) -> None:
        self.nodes: Dict[str, RaftNode] = {}
        self.drop_rate: float = 0.0
        self.partitions: Set[Tuple[str, str]] = set()  # Set of blocked directed edges (src, dst)
        self.rpc_count: int = 0
        self._lock = asyncio.Lock()

    def register_node(self, node: RaftNode) -> None:
        self.nodes[node.node_id] = node
        node.cluster = self

    def isolate_node(self, node_id: str) -> None:
        """Isolate a node completely from the rest of the cluster."""
        for peer in self.nodes:
            if peer != node_id:
                self.partitions.add((node_id, peer))
                self.partitions.add((peer, node_id))
        logger.warning("Network partition created: node '%s' is completely isolated.", node_id)

    def partition_split(self, group_a: List[str], group_b: List[str]) -> None:
        """Create a clean bipartisan network split between Group A and Group B."""
        for a in group_a:
            for b in group_b:
                self.partitions.add((a, b))
                self.partitions.add((b, a))
        logger.warning("Bipartite network partition activated: %s <X> %s", group_a, group_b)

    def heal_partitions(self) -> None:
        """Restore full network mesh connectivity."""
        self.partitions.clear()
        logger.info("All network partitions healed. Full mesh connectivity restored.")

    def is_connected(self, src: str, dst: str) -> bool:
        return (src, dst) not in self.partitions

    async def send_request_vote(self, src: str, dst: str, args: RequestVoteArgs) -> Optional[RequestVoteReply]:
        if not self.is_connected(src, dst):
            return None
        if self.drop_rate > 0.0 and random.random() < self.drop_rate:
            return None
        self.rpc_count += 1
        node = self.nodes.get(dst)
        if not node:
            return None
        return await node.handle_request_vote(args)

    async def send_append_entries(self, src: str, dst: str, args: AppendEntriesArgs) -> Optional[AppendEntriesReply]:
        if not self.is_connected(src, dst):
            return None
        if self.drop_rate > 0.0 and random.random() < self.drop_rate:
            return None
        self.rpc_count += 1
        node = self.nodes.get(dst)
        if not node:
            return None
        return await node.handle_append_entries(args)

    async def send_install_snapshot(self, src: str, dst: str, args: InstallSnapshotArgs) -> Optional[InstallSnapshotReply]:
        if not self.is_connected(src, dst):
            return None
        self.rpc_count += 1
        node = self.nodes.get(dst)
        if not node:
            return None
        return await node.handle_install_snapshot(args)

    def get_cluster_status(self) -> Dict[str, Any]:
        """Aggregate diagnostic cockpit telemetry across all participating nodes."""
        nodes_status = {}
        for nid, node in self.nodes.items():
            nodes_status[nid] = node.get_status()
        return {
            "cluster_size": len(self.nodes),
            "total_rpcs": self.rpc_count,
            "active_partitions": list(self.partitions),
            "nodes": nodes_status,
        }


# =====================================================================
# 4. RaftNode Core Consensus Engine
# =====================================================================

class RaftNode:
    """
    Raft Consensus Node implementing the complete Ongaro & Ousterhout algorithm.
    """

    def __init__(
        self,
        node_id: str,
        peers: List[str],
        cluster: Optional[RaftCluster] = None,
        election_min_ms: int = 150,
        election_max_ms: int = 300,
        heartbeat_interval_ms: int = 50,
        max_log_entries_before_snapshot: int = 100,
    ) -> None:
        self.node_id = node_id
        self.peers = [p for p in peers if p != node_id]
        self.cluster = cluster

        # Election & Heartbeat timing parameters
        self.election_min_ms = election_min_ms
        self.election_max_ms = election_max_ms
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self.max_log_entries = max_log_entries_before_snapshot

        # ----------------- Persistent State (all servers) -----------------
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        # Log entry indexing: 1-indexed conceptually.
        # Self.log stores entries starting after self.last_included_index.
        self.log: List[LogEntry] = []
        self.last_included_index: int = 0
        self.last_included_term: int = 0
        self.latest_snapshot: Optional[Snapshot] = None

        # ----------------- Volatile State (all servers) -------------------
        self.commit_index: int = 0
        self.last_applied: int = 0
        self.role: Role = Role.FOLLOWER
        self.leader_id: Optional[str] = None

        # ----------------- Volatile State (Leaders only) ------------------
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        # ----------------- Internal State & Components --------------------
        self.state_machine = ReplicatedStateMachine()
        self._lock = asyncio.Lock()
        self._election_deadline: float = 0.0
        self._running: bool = False
        self._main_task: Optional[asyncio.Task[None]] = None
        self._heartbeat_task: Optional[asyncio.Task[None]] = None
        self._commit_waiters: Dict[int, asyncio.Future[Any]] = {}

        self._reset_election_deadline()

    def _reset_election_deadline(self) -> None:
        timeout_sec = random.uniform(self.election_min_ms, self.election_max_ms) / 1000.0
        self._election_deadline = time.time() + timeout_sec

    # ---------------- Log Indexing & Bounds Helpers ------------------

    def get_last_log_index(self) -> int:
        if self.log:
            return self.log[-1].index
        return self.last_included_index

    def get_last_log_term(self) -> int:
        if self.log:
            return self.log[-1].term
        return self.last_included_term

    def get_log_entry_term(self, index: int) -> int:
        if index == 0:
            return 0
        if index == self.last_included_index:
            return self.last_included_term
        if index < self.last_included_index:
            return 0
        offset = index - self.last_included_index - 1
        if 0 <= offset < len(self.log):
            return self.log[offset].term
        return 0

    def get_log_slice(self, start_index: int) -> List[LogEntry]:
        """Returns log entries starting from start_index inclusive."""
        if start_index <= self.last_included_index:
            return copy.deepcopy(self.log)
        offset = start_index - self.last_included_index - 1
        if offset < 0:
            offset = 0
        return copy.deepcopy(self.log[offset:])

    # ---------------- Lifecycle Management ---------------------------

    async def start(self) -> None:
        """Start node background tasks (election monitor and heartbeat emitter)."""
        async with self._lock:
            if self._running:
                return
            self._running = True
            self._reset_election_deadline()
            self._main_task = asyncio.create_task(self._election_timer_loop())
            logger.debug("[%s] RaftNode started as FOLLOWER (term=%d)", self.node_id, self.current_term)

    async def stop(self) -> None:
        """Gracefully stop node background routines."""
        async with self._lock:
            self._running = False
            if self._main_task and not self._main_task.done():
                self._main_task.cancel()
            if self._heartbeat_task and not self._heartbeat_task.done():
                self._heartbeat_task.cancel()
            # Cancel any pending client futures
            for idx, fut in list(self._commit_waiters.items()):
                if not fut.done():
                    fut.set_exception(asyncio.CancelledError(f"Node {self.node_id} stopped"))
            self._commit_waiters.clear()
            logger.debug("[%s] RaftNode stopped.", self.node_id)

    # ---------------- Election & Heartbeat Loops ----------------------

    async def _election_timer_loop(self) -> None:
        while self._running:
            await asyncio.sleep(0.02)
            async with self._lock:
                if not self._running:
                    break
                now = time.time()
                if self.role != Role.LEADER and now >= self._election_deadline:
                    logger.info("[%s] Election timer expired! Initiating leader election.", self.node_id)
                    await self._start_election()

    async def _start_election(self) -> None:
        """Convert to CANDIDATE, increment term, vote for self, and broadcast RequestVote."""
        self.role = Role.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.leader_id = None
        self._reset_election_deadline()

        election_term = self.current_term
        last_log_idx = self.get_last_log_index()
        last_log_trm = self.get_last_log_term()

        args = RequestVoteArgs(
            term=election_term,
            candidate_id=self.node_id,
            last_log_index=last_log_idx,
            last_log_term=last_log_trm,
        )

        logger.info(
            "[%s] Candidate started election for Term %d (last_index=%d, last_term=%d)",
            self.node_id,
            election_term,
            last_log_idx,
            last_log_trm,
        )

        # Self-vote guarantees 1 vote
        votes_granted = 1
        total_nodes = len(self.peers) + 1
        majority = (total_nodes // 2) + 1

        if votes_granted >= majority:
            await self._become_leader()
            return

        # Broadcast RequestVote in parallel
        peers = list(self.peers)

        async def _ask_peer(peer_id: str) -> Optional[RequestVoteReply]:
            if not self.cluster:
                return None
            try:
                return await self.cluster.send_request_vote(self.node_id, peer_id, args)
            except Exception as e:
                logger.error("[%s -> %s] RequestVote RPC error: %s", self.node_id, peer_id, e)
                return None

        # Execute requests asynchronously outside lock to allow incoming RPCs
        tasks = [asyncio.create_task(_ask_peer(p)) for p in peers]
        # Release lock during network transit; reacquire to aggregate votes
        # Using a gathering sub-coroutine:
        asyncio.create_task(self._tally_election_votes(election_term, tasks, majority))

    async def _tally_election_votes(
        self,
        election_term: int,
        tasks: List[asyncio.Task[Optional[RequestVoteReply]]],
        majority: int,
    ) -> None:
        votes = 1
        for t in asyncio.as_completed(tasks):
            reply = await t
            async with self._lock:
                if not self._running:
                    return
                if reply is None:
                    continue
                # If peer has higher term, step down immediately
                if reply.term > self.current_term:
                    logger.info("[%s] Observed higher term %d from peer. Reverting to FOLLOWER.", self.node_id, reply.term)
                    self.current_term = reply.term
                    self.role = Role.FOLLOWER
                    self.voted_for = None
                    self._reset_election_deadline()
                    return

                if self.role == Role.CANDIDATE and self.current_term == election_term:
                    if reply.vote_granted:
                        votes += 1
                        logger.debug("[%s] Received vote from %s (total votes: %d/%d)", self.node_id, reply.voter_id, votes, majority)
                        if votes >= majority:
                            await self._become_leader()
                            return

    async def _become_leader(self) -> None:
        """Promote node to LEADER role and immediately dispatch initial empty heartbeat."""
        self.role = Role.LEADER
        self.leader_id = self.node_id
        logger.info("👑 [%s] ELECTED LEADER for Term %d! Reinitializing peer tracking.", self.node_id, self.current_term)

        last_idx = self.get_last_log_index()
        for peer in self.peers:
            self.next_index[peer] = last_idx + 1
            self.match_index[peer] = 0

        # Start periodic heartbeat dispatcher
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        # Dispatch immediate heartbeat
        await self._broadcast_append_entries(is_heartbeat=True)

    async def _heartbeat_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self.heartbeat_interval_ms / 1000.0)
            async with self._lock:
                if not self._running or self.role != Role.LEADER:
                    break
                await self._broadcast_append_entries(is_heartbeat=True)

    # ---------------- RPC Handlers ------------------------------------

    async def handle_request_vote(self, args: RequestVoteArgs) -> RequestVoteReply:
        """
        RequestVote RPC handler enforcing voting safety invariants:
        1. Reply false if term < current_term (§5.1).
        2. If voted_for is null or candidate_id, and candidate's log is at least
           as up-to-date as receiver's log, grant vote (§5.2, §5.4).
        """
        async with self._lock:
            # Rule 1: Term discovery
            if args.term > self.current_term:
                logger.info("[%s] RequestVote term %d > current_term %d. Stepping down to FOLLOWER.", self.node_id, args.term, self.current_term)
                self.current_term = args.term
                self.role = Role.FOLLOWER
                self.voted_for = None
                if self._heartbeat_task and not self._heartbeat_task.done():
                    self._heartbeat_task.cancel()

            reply = RequestVoteReply(term=self.current_term, vote_granted=False, voter_id=self.node_id)

            if args.term < self.current_term:
                logger.debug("[%s] Rejected vote to %s: term %d < current_term %d", self.node_id, args.candidate_id, args.term, self.current_term)
                return reply

            # Up-to-date log check (§5.4.1)
            last_log_idx = self.get_last_log_index()
            last_log_trm = self.get_last_log_term()

            candidate_up_to_date = False
            if args.last_log_term > last_log_trm:
                candidate_up_to_date = True
            elif args.last_log_term == last_log_trm and args.last_log_index >= last_log_idx:
                candidate_up_to_date = True

            can_vote = (self.voted_for is None or self.voted_for == args.candidate_id)

            if can_vote and candidate_up_to_date:
                self.voted_for = args.candidate_id
                self._reset_election_deadline()
                reply.vote_granted = True
                logger.info("[%s] Granted vote to candidate %s for term %d", self.node_id, args.candidate_id, args.term)
            else:
                logger.debug(
                    "[%s] Denied vote to %s for term %d (can_vote=%s, candidate_up_to_date=%s)",
                    self.node_id,
                    args.candidate_id,
                    args.term,
                    can_vote,
                    candidate_up_to_date,
                )

            return reply

    async def handle_append_entries(self, args: AppendEntriesArgs) -> AppendEntriesReply:
        """
        AppendEntries RPC handler enforcing Log Matching Invariants (§5.3):
        1. Reply false if term < current_term (§5.1).
        2. Reply false if log doesn't contain an entry at prev_log_index matching prev_log_term (§5.3).
        3. If an existing entry conflicts with a new one (same index, different terms),
           delete the existing entry and all that follow it (§5.3).
        4. Append any new entries not already in the log.
        5. If leader_commit > commit_index, set commit_index = min(leader_commit, index of last new entry).
        """
        async with self._lock:
            # Rule 1: Term discovery
            if args.term > self.current_term:
                self.current_term = args.term
                self.role = Role.FOLLOWER
                self.voted_for = None
                if self._heartbeat_task and not self._heartbeat_task.done():
                    self._heartbeat_task.cancel()

            reply = AppendEntriesReply(term=self.current_term, success=False, responder_id=self.node_id)

            if args.term < self.current_term:
                return reply

            # Valid leader recognized
            self.role = Role.FOLLOWER
            self.leader_id = args.leader_id
            self._reset_election_deadline()

            # Rule 2: Log matching invariant check at prev_log_index
            if args.prev_log_index > self.get_last_log_index():
                # Receiver is missing entries
                reply.match_index = self.get_last_log_index()
                return reply

            if args.prev_log_index > 0:
                entry_term = self.get_log_entry_term(args.prev_log_index)
                if entry_term != args.prev_log_term:
                    logger.warning(
                        "[%s] Log mismatch at index %d: expected term %d, found term %d",
                        self.node_id,
                        args.prev_log_index,
                        args.prev_log_term,
                        entry_term,
                    )
                    # Conflict resolution hint: return current last log index for quick backoff
                    reply.match_index = min(args.prev_log_index - 1, self.get_last_log_index())
                    return reply

            # Rule 3 & 4: Append new entries and truncate conflicts
            for entry in args.entries:
                if entry.index <= self.last_included_index:
                    continue
                offset = entry.index - self.last_included_index - 1
                if offset < len(self.log):
                    if self.log[offset].term != entry.term:
                        logger.warning(
                            "[%s] Conflict at index %d: local term %d != leader term %d. Truncating.",
                            self.node_id,
                            entry.index,
                            self.log[offset].term,
                            entry.term,
                        )
                        self.log = self.log[:offset]
                        self.log.append(entry)
                else:
                    self.log.append(entry)

            # Rule 5: Advance commit index
            if args.leader_commit > self.commit_index:
                last_new_entry_index = args.entries[-1].index if args.entries else args.prev_log_index
                self.commit_index = min(args.leader_commit, last_new_entry_index)
                await self._apply_entries_to_state_machine()

            reply.success = True
            reply.match_index = self.get_last_log_index()
            return reply

    async def handle_install_snapshot(self, args: InstallSnapshotArgs) -> InstallSnapshotReply:
        """
        InstallSnapshot RPC handler (§7):
        Applies a snapshot streamed by the leader when follower log has fallen behind
        the leader's compacted prefix.
        """
        async with self._lock:
            if args.term > self.current_term:
                self.current_term = args.term
                self.role = Role.FOLLOWER
                self.voted_for = None

            reply = InstallSnapshotReply(term=self.current_term, responder_id=self.node_id)
            if args.term < self.current_term:
                return reply

            self.role = Role.FOLLOWER
            self.leader_id = args.leader_id
            self._reset_election_deadline()

            # If snapshot is older than current snapshot, discard
            if args.last_included_index <= self.last_included_index:
                return reply

            # Discard existing or partially conflicting log entries
            offset = args.last_included_index - self.last_included_index - 1
            if 0 <= offset < len(self.log) and self.log[offset].term == args.last_included_term:
                self.log = self.log[offset + 1:]
            else:
                self.log = []

            self.last_included_index = args.last_included_index
            self.last_included_term = args.last_included_term
            self.state_machine.restore(args.data)
            self.latest_snapshot = Snapshot(args.last_included_index, args.last_included_term, args.data)

            self.commit_index = max(self.commit_index, args.last_included_index)
            self.last_applied = max(self.last_applied, args.last_included_index)

            logger.info(
                "[%s] Successfully installed snapshot (last_included_index=%d, term=%d)",
                self.node_id,
                args.last_included_index,
                args.last_included_term,
            )
            return reply

    # ---------------- Log Replication Dispatcher (Leader) -------------

    async def _broadcast_append_entries(self, is_heartbeat: bool = False) -> None:
        """Leader replicates log entries or dispatches heartbeats to all peers."""
        if self.role != Role.LEADER:
            return

        leader_term = self.current_term
        leader_commit = self.commit_index

        for peer in self.peers:
            prev_idx = self.next_index.get(peer, self.get_last_log_index() + 1) - 1

            # Check if peer requires InstallSnapshot instead of AppendEntries
            if prev_idx < self.last_included_index:
                if self.latest_snapshot:
                    snapshot_args = InstallSnapshotArgs(
                        term=leader_term,
                        leader_id=self.node_id,
                        last_included_index=self.last_included_index,
                        last_included_term=self.last_included_term,
                        data=self.latest_snapshot.data,
                    )
                    asyncio.create_task(self._send_install_snapshot_to_peer(peer, snapshot_args))
                continue

            prev_term = self.get_log_entry_term(prev_idx)
            entries = self.get_log_slice(prev_idx + 1) if not is_heartbeat or (prev_idx < self.get_last_log_index()) else []

            args = AppendEntriesArgs(
                term=leader_term,
                leader_id=self.node_id,
                prev_log_index=prev_idx,
                prev_log_term=prev_term,
                entries=entries,
                leader_commit=leader_commit,
            )

            asyncio.create_task(self._replicate_to_peer(peer, args))

    async def _send_install_snapshot_to_peer(self, peer: str, args: InstallSnapshotArgs) -> None:
        if not self.cluster:
            return
        try:
            reply = await self.cluster.send_install_snapshot(self.node_id, peer, args)
        except Exception as e:
            logger.error("[%s -> %s] InstallSnapshot RPC error: %s", self.node_id, peer, e)
            return

        async with self._lock:
            if not self._running or self.role != Role.LEADER or reply is None:
                return
            if reply.term > self.current_term:
                self.current_term = reply.term
                self.role = Role.FOLLOWER
                self.voted_for = None
                self._reset_election_deadline()
                if self._heartbeat_task and not self._heartbeat_task.done():
                    self._heartbeat_task.cancel()
                return

            self.next_index[peer] = args.last_included_index + 1
            self.match_index[peer] = args.last_included_index

    async def _replicate_to_peer(self, peer: str, args: AppendEntriesArgs) -> None:
        if not self.cluster:
            return
        try:
            reply = await self.cluster.send_append_entries(self.node_id, peer, args)
        except Exception as e:
            logger.error("[%s -> %s] AppendEntries RPC error: %s", self.node_id, peer, e)
            return

        async with self._lock:
            if not self._running or self.role != Role.LEADER or reply is None:
                return

            if reply.term > self.current_term:
                logger.info("[%s] Observed term %d on AppendEntries reply from %s. Reverting to FOLLOWER.", self.node_id, reply.term, peer)
                self.current_term = reply.term
                self.role = Role.FOLLOWER
                self.voted_for = None
                self._reset_election_deadline()
                if self._heartbeat_task and not self._heartbeat_task.done():
                    self._heartbeat_task.cancel()
                return

            if reply.success:
                new_match = max(self.match_index.get(peer, 0), reply.match_index)
                self.match_index[peer] = new_match
                self.next_index[peer] = new_match + 1
                await self._check_and_advance_commit_index()
            else:
                # Decrement next_index and retry log reconciliation
                current_next = self.next_index.get(peer, self.get_last_log_index() + 1)
                self.next_index[peer] = max(1, min(current_next - 1, reply.match_index + 1))
                logger.debug("[%s] Re-attempting log reconciliation with %s (next_index=%d)", self.node_id, peer, self.next_index[peer])

    async def _check_and_advance_commit_index(self) -> None:
        """
        Commitment Invariant (§5.3, §5.4):
        If there exists an N > commit_index such that a majority of match_index[i] >= N,
        and log[N].term == current_term, set commit_index = N.
        """
        if self.role != Role.LEADER:
            return

        last_index = self.get_last_log_index()
        total_nodes = len(self.peers) + 1
        majority = (total_nodes // 2) + 1

        for n in range(last_index, self.commit_index, -1):
            term_n = self.get_log_entry_term(n)
            # Raft safety: Leader cannot commit log entries from previous terms by counting replicas (§5.4.2)
            if term_n != self.current_term:
                continue

            match_count = 1  # Self matches
            for peer in self.peers:
                if self.match_index.get(peer, 0) >= n:
                    match_count += 1

            if match_count >= majority:
                logger.info("🎯 [%s] Leader advancing commit_index from %d to %d (quorum %d/%d)", self.node_id, self.commit_index, n, match_count, majority)
                self.commit_index = n
                await self._apply_entries_to_state_machine()
                break

    async def _apply_entries_to_state_machine(self) -> None:
        """Apply all committed and unapplied entries to the local deterministic state machine."""
        while self.commit_index > self.last_applied:
            self.last_applied += 1
            idx = self.last_applied

            if idx <= self.last_included_index:
                continue

            offset = idx - self.last_included_index - 1
            if 0 <= offset < len(self.log):
                entry = self.log[offset]
                result = self.state_machine.apply(entry.command)
                logger.debug("[%s] Applied entry %d to StateMachine: %s => %s", self.node_id, idx, entry.command, result)

                # Resolve client future if waiting on this node
                waiter = self._commit_waiters.pop(idx, None)
                if waiter and not waiter.done():
                    waiter.set_result(result)

        # Trigger automatic snapshot if log threshold reached
        if len(self.log) >= self.max_log_entries:
            await self.save_snapshot(self.last_applied)

    # ---------------- Client Propose Interface -------------------------

    async def propose(self, command: Dict[str, Any], timeout: float = 3.0) -> Dict[str, Any]:
        """
        Client command ingress. If node is LEADER, appends entry and waits for quorum commit.
        Returns execution result or raises Exception if not leader or timed out.
        """
        future: asyncio.Future[Any] = asyncio.get_event_loop().create_future()
        async with self._lock:
            if self.role != Role.LEADER:
                raise RuntimeError(f"Cannot propose to non-leader node {self.node_id} (leader is {self.leader_id})")

            new_index = self.get_last_log_index() + 1
            entry = LogEntry(term=self.current_term, index=new_index, command=command)
            self.log.append(entry)
            self._commit_waiters[new_index] = future

            logger.info("📥 [%s] Proposed entry %d (term=%d, op=%s)", self.node_id, new_index, self.current_term, command.get("op"))

            # Broadcast immediately to accelerate consensus
            await self._broadcast_append_entries(is_heartbeat=False)

        try:
            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            async with self._lock:
                self._commit_waiters.pop(new_index, None)
            raise TimeoutError(f"Proposal for entry {new_index} timed out after {timeout}s")

    # ---------------- Log Compaction & Snapshotting -------------------

    async def save_snapshot(self, last_index: int) -> Snapshot:
        """
        Compacts the log up to last_index by creating a snapshot and discarding older entries.
        """
        async with self._lock:
            if last_index <= self.last_included_index or last_index > self.last_applied:
                if self.latest_snapshot:
                    return self.latest_snapshot
                return Snapshot(self.last_included_index, self.last_included_term, self.state_machine.snapshot())

            target_term = self.get_log_entry_term(last_index)
            offset = last_index - self.last_included_index - 1
            if 0 <= offset < len(self.log):
                self.log = self.log[offset + 1:]
            else:
                self.log = []

            self.last_included_index = last_index
            self.last_included_term = target_term
            snap_data = self.state_machine.snapshot()
            self.latest_snapshot = Snapshot(last_index, target_term, snap_data)

            logger.info(
                "📸 [%s] Snapshot saved at index %d (term=%d). Remaining log length: %d",
                self.node_id,
                last_index,
                target_term,
                len(self.log),
            )
            return self.latest_snapshot

    def get_status(self) -> Dict[str, Any]:
        """Telemetry diagnostics dictionary."""
        return {
            "node_id": self.node_id,
            "role": self.role.value,
            "current_term": self.current_term,
            "leader_id": self.leader_id,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "last_log_index": self.get_last_log_index(),
            "last_log_term": self.get_last_log_term(),
            "log_entries_count": len(self.log),
            "snapshot_index": self.last_included_index,
            "snapshot_term": self.last_included_term,
            "voted_for": self.voted_for,
        }


# =====================================================================
# 5. ADR-Lite Synthesis Engine (Architectural Decision Record)
# =====================================================================

def get_adr_lite() -> Dict[str, Any]:
    """
    Emits an Architectural Decision Record (ADR-Lite) synthesizing
    trade-offs between Raft, Multi-Paxos, and Zab consensus protocols.
    """
    return {
        "title": "ADR-042: Autonomous Pipeline Raft Consensus Protocol & Replicated State Machine",
        "status": "ACCEPTED",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "context": (
            "Multi-agent autonomous fleet coordination requires linearizable log replication, "
            "deterministic state transitions, barrier synchronization, and leader failover "
            "without split-brain inconsistencies under transient network partitions."
        ),
        "protocol_comparison": {
            "Raft (Ongaro & Ousterhout 2014)": {
                "leader_election": "Randomized election timeouts, single candidate vote quorum, strong leader invariants.",
                "log_structure": "Strict monotonic contiguous indexing with leader append-only semantics. No log gaps.",
                "strengths": "Superior formal verifiability, understandable state transitions, deterministic log repair.",
                "weaknesses": "Strict leader dependency causes transient pipeline pause during leader re-election.",
                "verdict": "CHOSEN - Optimal for fleet arbitration, barrier locks, and autonomous state machine guarantees."
            },
            "Multi-Paxos (Lamport 1998)": {
                "leader_election": "Weak leader or dual-leader prosumption with multi-instance Paxos consensus.",
                "log_structure": "Out-of-order consensus slots allowing log holes that require fill-in noops.",
                "strengths": "Slightly higher throughput when network latency varies across geographically dispersed nodes.",
                "weaknesses": "Extreme implementation complexity, subtle corner-case invariants, difficult membership changes.",
                "verdict": "REJECTED - Non-contiguous log complexity introduces excessive state verification overhead."
            },
            "Zab (ZooKeeper Atomic Broadcast)": {
                "leader_election": "Fast Leader Election with epoch identification (zxid = epoch + counter).",
                "log_structure": "Primary-backup FIFO atomic broadcast channel with recovery synchronization phase.",
                "strengths": "High read-throughput for hierarchical tree nodes and ephemeral node heartbeats.",
                "weaknesses": "Deeply coupled with ZooKeeper directory semantics; non-trivial embedded standard library adoption.",
                "verdict": "REJECTED - Raft provides cleaner modular separation between consensus and state machine."
            }
        },
        "safety_invariants_enforced": [
            "Election Safety: At most one leader per term.",
            "Leader Append-Only: Leaders never truncate or overwrite their own logs.",
            "Log Matching Invariant: Same index & term implies identical prefix logs.",
            "Leader Completeness: Committed entries survive across all subsequent terms.",
            "State Machine Safety: Entries applied at given index are uniquely immutable."
        ]
    }


def save_adr_lite_dashboard(output_path: str = "dashboards/raft_consensus_state.json") -> None:
    """Persist ADR-Lite evaluation to dashboard artifact path."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    adr_data = get_adr_lite()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(adr_data, f, indent=2)
    logger.info("Persisted ADR-Lite Consensus Analysis to %s", path.resolve())


# =====================================================================
# 6. Built-in Verification Suite
# =====================================================================

async def run_self_tests() -> bool:
    """
    Exhaustive verification suite for the Raft Consensus Engine:
    Test 1: Single leader election with 3 and 5 nodes.
    Test 2: Leader log replication and state machine commit across all followers.
    Test 3: Network partition isolating leader, electing new leader in majority partition,
            resolving partition, and asserting convergence (log overwrites and state equality).
    Test 4: Log compaction and snapshot catch-up for lagging nodes.
    """
    print("\n" + "=" * 80)
    print("🚀 PIPELINE AUTONOMIC RAFT CONSENSUS ENGINE: VERIFICATION SUITE")
    print("=" * 80 + "\n")

    # -----------------------------------------------------------------
    # Test 1: Single leader election in 3-node and 5-node clusters
    # -----------------------------------------------------------------
    print("--- [TEST 1] Testing Leader Election Stability (3-Node and 5-Node) ---")
    for cluster_size in [3, 5]:
        cluster = RaftCluster()
        node_ids = [f"node_{i}" for i in range(cluster_size)]
        nodes = [
            RaftNode(
                node_id=nid,
                peers=node_ids,
                cluster=cluster,
                election_min_ms=60,
                election_max_ms=120,
                heartbeat_interval_ms=25,
            )
            for nid in node_ids
        ]
        for n in nodes:
            cluster.register_node(n)
            await n.start()

        # Allow sufficient time for election
        await asyncio.sleep(0.4)

        leaders = [n for n in nodes if n.role == Role.LEADER]
        print(f"Cluster Size {cluster_size}: Detected {len(leaders)} leader(s): {[l.node_id for l in leaders]}")
        assert len(leaders) == 1, f"Expected exactly 1 leader in {cluster_size}-node cluster, found {len(leaders)}"
        leader = leaders[0]
        assert leader.current_term >= 1, "Leader term must be at least 1"

        # Check all followers recognized the leader
        for n in nodes:
            if n.node_id != leader.node_id:
                assert n.role == Role.FOLLOWER, f"Node {n.node_id} should be FOLLOWER"
                assert n.leader_id == leader.node_id, f"Node {n.node_id} leader_id {n.leader_id} != {leader.node_id}"

        for n in nodes:
            await n.stop()

    print("✅ TEST 1 PASSED: Unique leader elected with valid term invariants.\n")

    # -----------------------------------------------------------------
    # Test 2: Leader log replication and state machine commit
    # -----------------------------------------------------------------
    print("--- [TEST 2] Testing Log Replication & State Machine Execution ---")
    cluster = RaftCluster()
    node_ids = ["node_A", "node_B", "node_C"]
    nodes = {
        nid: RaftNode(nid, node_ids, cluster, election_min_ms=60, election_max_ms=120, heartbeat_interval_ms=25)
        for nid in node_ids
    }
    for n in nodes.values():
        cluster.register_node(n)
        await n.start()

    await asyncio.sleep(0.35)
    leader = next(n for n in nodes.values() if n.role == Role.LEADER)
    print(f"Elected Leader: {leader.node_id} (Term {leader.current_term})")

    # Dispatch commands: Key-value, barrier, and lease
    res1 = await leader.propose({"op": "SET", "key": "fleet/status", "value": "ACTIVE"})
    assert res1["status"] == "OK" and res1["value"] == "ACTIVE"

    res2 = await leader.propose({"op": "REGISTER_BARRIER", "key": "build_barrier", "threshold": 2, "actor_id": "worker_1"})
    assert res2["tripped"] is False

    res3 = await leader.propose({"op": "REGISTER_BARRIER", "key": "build_barrier", "threshold": 2, "actor_id": "worker_2"})
    assert res3["tripped"] is True

    res4 = await leader.propose({"op": "ACQUIRE_LEASE", "key": "deploy_lock", "ttl": 10.0, "actor_id": "agent_alpha"})
    assert res4["granted"] is True

    res5 = await leader.propose({"op": "COMMIT_HEAD", "key": "main", "value": "a1b2c3d4e5f6"})
    assert res5["commit"] == "a1b2c3d4e5f6"

    await asyncio.sleep(0.2)

    # Verify replication consistency across all nodes
    for nid, node in nodes.items():
        assert node.commit_index == 5, f"Node {nid} commit_index is {node.commit_index}, expected 5"
        assert node.state_machine.get("fleet/status") == "ACTIVE"
        assert node.state_machine.get("refs/heads/main") == "a1b2c3d4e5f6"
        assert node.get_last_log_index() == 5
        print(f"Node {nid} verified: commitIndex={node.commit_index}, logLen={len(node.log)}")

    for n in nodes.values():
        await n.stop()

    print("✅ TEST 2 PASSED: 5 commands committed across all replicas with linearizable state.\n")

    # -----------------------------------------------------------------
    # Test 3: Network partition, split-brain resistance, and log reconciliation
    # -----------------------------------------------------------------
    print("--- [TEST 3] Testing Network Partition & Uncommitted Overwrites ---")
    cluster = RaftCluster()
    node_ids = ["node_0", "node_1", "node_2", "node_3", "node_4"]
    nodes = {
        nid: RaftNode(nid, node_ids, cluster, election_min_ms=60, election_max_ms=120, heartbeat_interval_ms=25)
        for nid in node_ids
    }
    for n in nodes.values():
        cluster.register_node(n)
        await n.start()

    await asyncio.sleep(0.35)
    initial_leader = next(n for n in nodes.values() if n.role == Role.LEADER)
    print(f"Initial Leader: {initial_leader.node_id} (Term {initial_leader.current_term})")

    # Initial command
    await initial_leader.propose({"op": "SET", "key": "cluster/epoch", "value": 1})

    # Partition network: Minority {initial_leader, peer1} vs Majority {peer2, peer3, peer4}
    minority = [initial_leader.node_id]
    for p in nodes:
        if p != initial_leader.node_id:
            minority.append(p)
            break
    majority = [nid for nid in node_ids if nid not in minority]

    print(f"Injecting Network Partition: Minority {minority} <X> Majority {majority}")
    cluster.partition_split(minority, majority)

    # Attempt propose on partitioned minority leader -> should timeout or fail to commit
    minority_leader = initial_leader
    minority_timed_out = False
    try:
        await minority_leader.propose({"op": "SET", "key": "stale_write", "value": "uncommitted"}, timeout=0.3)
    except TimeoutError:
        minority_timed_out = True
        print(f"Expected behavior: Minority leader {minority_leader.node_id} proposal timed out without quorum.")

    assert minority_timed_out, "Minority leader must not be able to commit without quorum"

    # Allow majority partition to elect a new leader
    await asyncio.sleep(0.5)
    majority_leaders = [nodes[nid] for nid in majority if nodes[nid].role == Role.LEADER]
    assert len(majority_leaders) == 1, f"Expected 1 leader in majority partition, found {len(majority_leaders)}"
    new_leader = majority_leaders[0]
    print(f"Majority Partition Elected New Leader: {new_leader.node_id} (Term {new_leader.current_term})")
    assert new_leader.current_term > initial_leader.current_term

    # Propose on new leader in majority partition -> must succeed
    res_maj = await new_leader.propose({"op": "SET", "key": "cluster/epoch", "value": 2})
    assert res_maj["status"] == "OK"

    # Heal network partition
    print("Healing network partition...")
    cluster.heal_partitions()
    await asyncio.sleep(0.5)

    # Assert old minority leader stepped down and reconciled log
    assert minority_leader.role == Role.FOLLOWER, "Old leader must step down to FOLLOWER"
    active_leaders = [n for n in nodes.values() if n.role == Role.LEADER]
    assert len(active_leaders) == 1, f"Expected exactly 1 leader after healing, found {len(active_leaders)}"

    # All nodes must converge to the same state
    for nid, node in nodes.items():
        assert node.state_machine.get("cluster/epoch") == 2, f"Node {nid} has epoch {node.state_machine.get('cluster/epoch')}, expected 2"
        assert node.state_machine.get("stale_write") is None, f"Node {nid} uncommitted stale write was not rolled back!"

    for n in nodes.values():
        await n.stop()

    print("✅ TEST 3 PASSED: Split-brain prevented, uncommitted entries overwritten, log reconciled.\n")

    # -----------------------------------------------------------------
    # Test 4: Log compaction, snapshot creation, and InstallSnapshot catch-up
    # -----------------------------------------------------------------
    print("--- [TEST 4] Testing Log Compaction & InstallSnapshot RPC ---")
    cluster = RaftCluster()
    node_ids = ["node_fast1", "node_fast2", "node_lagging"]
    nodes = {
        nid: RaftNode(
            nid,
            node_ids,
            cluster,
            election_min_ms=60,
            election_max_ms=120,
            heartbeat_interval_ms=25,
            max_log_entries_before_snapshot=5,  # Compact after 5 entries
        )
        for nid in node_ids
    }
    for n in nodes.values():
        cluster.register_node(n)
        await n.start()

    await asyncio.sleep(0.35)
    leader = next(n for n in nodes.values() if n.role == Role.LEADER)
    lagging_node = nodes["node_lagging"]

    # Isolate the lagging node before log writes
    cluster.isolate_node(lagging_node.node_id)
    print(f"Isolated {lagging_node.node_id}. Replicating 8 commands to trigger snapshot on leader...")

    for i in range(8):
        await leader.propose({"op": "SET", "key": f"key_{i}", "value": f"val_{i}"})

    assert len(leader.log) < 8, f"Leader log should have been compacted (len={len(leader.log)})"
    assert leader.last_included_index > 0, "Leader snapshot must have advanced last_included_index"
    print(f"Leader compacted log: last_included_index={leader.last_included_index}, remaining_log_len={len(leader.log)}")

    # Reconnect lagging node
    print("Reconnecting lagging node to cluster...")
    cluster.heal_partitions()

    # Allow leader to dispatch InstallSnapshot RPC
    await asyncio.sleep(0.5)

    # Verify lagging node caught up through snapshot and state machine matches
    assert lagging_node.last_included_index == leader.last_included_index
    assert lagging_node.commit_index == leader.commit_index
    for i in range(8):
        assert lagging_node.state_machine.get(f"key_{i}") == f"val_{i}", f"Missing key_{i} on lagging node"

    print(f"Lagging node successfully caught up: snapshot_index={lagging_node.last_included_index}, commit_index={lagging_node.commit_index}")

    for n in nodes.values():
        await n.stop()

    print("✅ TEST 4 PASSED: Snapshot compaction and catch-up verified.\n")

    # Persist ADR-Lite specification
    save_adr_lite_dashboard()

    print("=" * 80)
    print("🎉 ALL 4 RAFT CONSENSUS SUITE TESTS PASSED WITH 100% SUCCESS!")
    print("=" * 80 + "\n")
    return True


if __name__ == "__main__":
    asyncio.run(run_self_tests())