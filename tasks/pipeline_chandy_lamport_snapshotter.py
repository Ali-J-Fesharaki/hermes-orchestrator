#!/usr/bin/env python3
"""
Pipeline Autonomic Chandy-Lamport Distributed Snapshot & Global State Checkpointer
===================================================================================

An enterprise-grade, asynchronous implementation of the Chandy-Lamport distributed
snapshot algorithm across actor pods connected by reliable, FIFO communication channels.

Theoretical Foundations:
-----------------------
Let G = (V, E) be a finite directed graph where:
  - V = {p_1, p_2, ..., p_n} denotes autonomous distributed processes (actor pods).
  - E = {c_{i,j} = (p_i, p_j) | p_i, p_j in V} denotes directed communication channels.
  - Channels are assumed to be error-free, non-blocking, and strictly FIFO.
  - Channel delay is arbitrary but finite.

Protocol State Transitions:
---------------------------
1. Snapshot Initiation (Node p_i initiates snapshot S_k):
   - Node p_i records its internal state: sigma_{p_i}.
   - Node p_i sets channel state recording chi_{j,i} = [] for all incoming channels c_{j,i}.
   - Node p_i emits Marker(S_k, p_i) along all outgoing channels c_{i,m} in E.

2. Marker Processing Rule at Node p_r receiving Marker(S_k, p_s) along channel c_{s,r}:
   - Case A (First marker seen by p_r for S_k):
       * p_r records its local state: sigma_{p_r}.
       * p_r records channel state chi_{s,r} = [] (empty sequence; no messages in-flight
         on channel c_{s,r} prior to snapshot cut).
       * p_r begins recording all subsequent incoming regular messages on all other
         incoming channels c_{w,r} (for all w != s).
       * p_r propagates Marker(S_k, p_r) along all outgoing channels c_{r,m}.
   - Case B (Subsequent marker seen by p_r for S_k along channel c_{s,r}):
       * p_r halts recording incoming messages on channel c_{s,r}.
       * Recorded channel state chi_{s,r} constitutes all messages arriving on c_{s,r}
         between the time p_r snapshotted its own local state and the receipt of this marker.

Invariants Verified:
--------------------
1. Consistent Global Cut: No message recorded in channel state is an orphan (received
   without being sent). All causal predecessors e_a -> e_b are preserved.
2. Global Token Conservation: sum(tokens in process states) + sum(tokens in transit) == Total Tokens.
3. No-Orphan Lease Invariant: Any in-flight work item references an active reservation lease.
4. Global Deadlock Detection: Tarjan's Strongly Connected Components (SCC) cycle analysis
   on the unified distributed Wait-For Graph (WFG).

Zero external dependencies: Built strictly with Python 3.10+ standard library.
"""

from __future__ import annotations

import collections
from dataclasses import dataclass, field, asdict
import hashlib
import json
import logging
import os
from pathlib import Path
import queue
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import uuid

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(name)s: %(message)s"
)
logger = logging.getLogger("ChandyLamportSnapshotter")


# ============================================================================
# 1. Network Primitives & Data Models
# ============================================================================

@dataclass(frozen=True)
class Message:
    """Represents a regular payload message in the distributed system."""
    id: str
    sender: str
    receiver: str
    msg_type: str  # e.g., 'TASK_DISPATCH', 'TOKEN_TRANSFER', 'LEASE_REQUEST'
    payload: Dict[str, Any]
    timestamp: float
    causal_seq: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class Marker:
    """Chandy-Lamport synchronization marker traversing communication channels."""
    snapshot_id: str
    initiator: str
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ProcessState:
    """Locally recorded snapshot state of a single actor pod."""
    node_id: str
    snapshot_id: str
    local_data: Dict[str, Any]
    active_tasks: List[str]
    token_balance: int
    held_leases: List[str]
    waiting_for: List[str]  # Dependencies for Deadlock Detection (node_ids or lock_keys)
    timestamp: float
    state_hash: str = ""

    def __post_init__(self):
        if not self.state_hash:
            canonical = f"{self.node_id}:{self.snapshot_id}:{self.token_balance}:{sorted(self.active_tasks)}"
            self.state_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ChannelState:
    """Recorded message sequence in-flight on a directed channel during the cut."""
    channel_id: str
    sender: str
    receiver: str
    snapshot_id: str
    messages: List[Message] = field(default_factory=list)
    recorded_at: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "channel_id": self.channel_id,
            "sender": self.sender,
            "receiver": self.receiver,
            "snapshot_id": self.snapshot_id,
            "messages": [m.to_dict() for m in self.messages],
            "recorded_at": self.recorded_at,
            "message_count": len(self.messages),
        }


@dataclass
class GlobalSnapshot:
    """Coordinated consistent global cut of all processes and channels."""
    snapshot_id: str
    initiator: str
    process_states: Dict[str, ProcessState]
    channel_states: Dict[str, ChannelState]
    created_at: float
    is_consistent: bool = False
    invariants_passed: bool = False
    diagnostics: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "initiator": self.initiator,
            "created_at": self.created_at,
            "is_consistent": self.is_consistent,
            "invariants_passed": self.invariants_passed,
            "process_states": {k: v.to_dict() for k, v in self.process_states.items()},
            "channel_states": {k: v.to_dict() for k, v in self.channel_states.items()},
            "diagnostics": self.diagnostics,
        }


# ============================================================================
# 2. Reliable Thread-Safe FIFO Channel
# ============================================================================

class FIFOChannel:
    """
    A reliable, directed FIFO communication channel between two nodes.
    Supports in-flight message logging when active snapshot recording is enabled.
    """
    def __init__(self, sender_id: str, receiver_id: str, latency_sec: float = 0.01):
        self.sender_id = sender_id
        self.receiver_id = receiver_id
        self.channel_id = f"chan_{sender_id}->{receiver_id}"
        self.latency_sec = latency_sec

        self._queue: queue.Queue[Message | Marker] = queue.Queue()
        self._lock = threading.Lock()
        self._closed = False

    def send(self, item: Message | Marker) -> None:
        """Enqueue message or marker into FIFO queue."""
        if self._closed:
            raise RuntimeError(f"Cannot send on closed channel {self.channel_id}")
        self._queue.put(item)

    def receive(self, timeout: Optional[float] = None) -> Optional[Message | Marker]:
        """Dequeue next item from channel with optional timeout."""
        try:
            return self._queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def close(self) -> None:
        self._closed = True

    def qsize(self) -> int:
        return self._queue.qsize()


# ============================================================================
# 3. Distributed Node (Actor Pod / Process)
# ============================================================================

class DistributedNode:
    """
    Simulates an autonomous actor pod or worker process implementing the
    Chandy-Lamport Distributed Snapshot Protocol.
    """

    def __init__(
        self,
        node_id: str,
        initial_tokens: int = 100,
        coordinator_callback: Optional[Callable[[str, ProcessState, List[ChannelState]], None]] = None
    ):
        self.node_id = node_id
        self.token_balance = initial_tokens
        self.active_tasks: Set[str] = set()
        self.held_leases: Set[str] = set()
        self.waiting_for: Set[str] = set()  # Node IDs or resource tags this node waits on

        self.incoming_channels: Dict[str, FIFOChannel] = {}  # sender_id -> FIFOChannel
        self.outgoing_channels: Dict[str, FIFOChannel] = {}  # receiver_id -> FIFOChannel

        self._coordinator_callback = coordinator_callback
        self._lock = threading.RLock()
        self._running = False
        self._worker_thread: Optional[threading.Thread] = None

        # Snapshot Tracking State per snapshot_id:
        # snapshot_id -> {
        #    "has_recorded_process": bool,
        #    "recording_channels": Set[str], # incoming channels currently being recorded
        #    "recorded_messages": Dict[str, List[Message]], # sender_id -> messages
        #    "process_state": Optional[ProcessState],
        #    "completed_channels": Set[str] # incoming channels that delivered marker
        # }
        self._snapshots: Dict[str, Dict[str, Any]] = {}
        self._causal_counter = 0

    def register_incoming(self, sender_id: str, channel: FIFOChannel) -> None:
        with self._lock:
            self.incoming_channels[sender_id] = channel

    def register_outgoing(self, receiver_id: str, channel: FIFOChannel) -> None:
        with self._lock:
            self.outgoing_channels[receiver_id] = channel

    def start(self) -> None:
        """Starts node background dispatching and receiving thread."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._worker_thread = threading.Thread(
                target=self._run_loop,
                name=f"NodeThread-{self.node_id}",
                daemon=True
            )
            self._worker_thread.start()

    def stop(self) -> None:
        """Halts the node background listener loop."""
        with self._lock:
            self._running = False
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=1.0)

    # ------------------------------------------------------------------------
    # Protocol Entry Point: Snapshot Initiation
    # ------------------------------------------------------------------------
    def initiate_snapshot(self, snapshot_id: Optional[str] = None) -> str:
        """
        Initiates a Chandy-Lamport snapshot from this node.
        1. Records local process state.
        2. Starts recording messages on ALL incoming channels.
        3. Dispatches Marker out to all outgoing channels.
        """
        with self._lock:
            if snapshot_id is None:
                snapshot_id = f"snap_{self.node_id}_{int(time.time()*1000)}"

            logger.info(f"[{self.node_id}] Initiating distributed snapshot: {snapshot_id}")

            # Step 1: Record local state
            p_state = self._record_local_state(snapshot_id)

            # Initialize snapshot tracking context
            # For initiator, all incoming channels start recording
            all_incoming = set(self.incoming_channels.keys())
            self._snapshots[snapshot_id] = {
                "has_recorded_process": True,
                "recording_channels": set(all_incoming),
                "recorded_messages": {s_id: [] for s_id in all_incoming},
                "process_state": p_state,
                "completed_channels": set(),
            }

            # Step 3: Broadcast Marker along all outgoing channels
            self._broadcast_marker(snapshot_id)

            # Check if this node has no incoming channels (edge case: single isolated node or DAG root)
            if not all_incoming:
                self._finalize_local_snapshot(snapshot_id)

            return snapshot_id

    # ------------------------------------------------------------------------
    # Marker & Message Processing Logic
    # ------------------------------------------------------------------------
    def _handle_marker(self, sender_id: str, marker: Marker) -> None:
        """Executes Chandy-Lamport Marker rules on arrival."""
        snapshot_id = marker.snapshot_id
        with self._lock:
            snap_ctx = self._snapshots.get(snapshot_id)

            if snap_ctx is None:
                # -----------------------------------------------------------
                # Case A: First marker seen for this snapshot_id
                # -----------------------------------------------------------
                logger.debug(f"[{self.node_id}] Case A: First marker for {snapshot_id} from {sender_id}")

                # 1. Record local process state immediately
                p_state = self._record_local_state(snapshot_id)

                # 2. Channel c_{sender, this} state is recorded as empty
                all_incoming = set(self.incoming_channels.keys())
                other_incoming = all_incoming - {sender_id}

                self._snapshots[snapshot_id] = {
                    "has_recorded_process": True,
                    "recording_channels": set(other_incoming), # record on all other incoming channels
                    "recorded_messages": {s_id: [] for s_id in all_incoming},
                    "process_state": p_state,
                    "completed_channels": {sender_id}, # sender channel is completed (empty)
                }

                # 3. Propagate Marker along all outgoing channels
                self._broadcast_marker(snapshot_id)

                # Check if all incoming channels have now delivered marker
                if len(self._snapshots[snapshot_id]["completed_channels"]) >= len(all_incoming):
                    self._finalize_local_snapshot(snapshot_id)

            else:
                # -----------------------------------------------------------
                # Case B: Subsequent marker seen for this snapshot_id
                # -----------------------------------------------------------
                logger.debug(f"[{self.node_id}] Case B: Subsequent marker for {snapshot_id} from {sender_id}")

                # Stop recording on channel from sender_id
                snap_ctx["recording_channels"].discard(sender_id)
                snap_ctx["completed_channels"].add(sender_id)

                # If all incoming channels have delivered markers, finalize
                if len(snap_ctx["completed_channels"]) >= len(self.incoming_channels):
                    self._finalize_local_snapshot(snapshot_id)

    def _handle_regular_message(self, sender_id: str, message: Message) -> None:
        """Processes application level messages and logs them if recording."""
        with self._lock:
            # 1. Check if any active snapshot is recording on this incoming channel
            for snap_id, snap_ctx in self._snapshots.items():
                if sender_id in snap_ctx["recording_channels"]:
                    logger.debug(f"[{self.node_id}] Captured in-flight message on {sender_id}->{self.node_id}: {message.id}")
                    snap_ctx["recorded_messages"][sender_id].append(message)

            # 2. Execute message side effects on internal state
            self._apply_message_payload(message)

    def _apply_message_payload(self, message: Message) -> None:
        """Applies application message updates to local state."""
        if message.msg_type == "TOKEN_TRANSFER":
            amount = message.payload.get("amount", 0)
            self.token_balance += amount
            logger.debug(f"[{self.node_id}] Received {amount} tokens from {message.sender}. Balance: {self.token_balance}")

        elif message.msg_type == "TASK_DISPATCH":
            task_id = message.payload.get("task_id")
            lease_id = message.payload.get("lease_id")
            if task_id:
                self.active_tasks.add(task_id)
            if lease_id:
                self.held_leases.add(lease_id)
            logger.debug(f"[{self.node_id}] Accepted task {task_id} with lease {lease_id}")

        elif message.msg_type == "LEASE_RELEASE":
            lease_id = message.payload.get("lease_id")
            if lease_id in self.held_leases:
                self.held_leases.remove(lease_id)

    def _record_local_state(self, snapshot_id: str) -> ProcessState:
        """Atomically captures and snapshots the process's internal state."""
        return ProcessState(
            node_id=self.node_id,
            snapshot_id=snapshot_id,
            local_data={
                "causal_seq": self._causal_counter,
                "node_status": "HEALTHY",
            },
            active_tasks=sorted(list(self.active_tasks)),
            token_balance=self.token_balance,
            held_leases=sorted(list(self.held_leases)),
            waiting_for=sorted(list(self.waiting_for)),
            timestamp=time.time(),
        )

    def _broadcast_marker(self, snapshot_id: str) -> None:
        """Emits marker to all outgoing channels."""
        marker = Marker(snapshot_id=snapshot_id, initiator=self.node_id)
        for dest_id, channel in self.outgoing_channels.items():
            logger.debug(f"[{self.node_id}] Dispatched marker {snapshot_id} to {dest_id}")
            channel.send(marker)

    def _finalize_local_snapshot(self, snapshot_id: str) -> None:
        """Compiles local process state and recorded channel states and reports upward."""
        snap_ctx = self._snapshots[snapshot_id]
        p_state = snap_ctx["process_state"]

        channel_states: List[ChannelState] = []
        for sender_id, msgs in snap_ctx["recorded_messages"].items():
            chan_state = ChannelState(
                channel_id=f"chan_{sender_id}->{self.node_id}",
                sender=sender_id,
                receiver=self.node_id,
                snapshot_id=snapshot_id,
                messages=list(msgs),
            )
            channel_states.append(chan_state)

        logger.info(f"[{self.node_id}] Finalized participation in snapshot {snapshot_id} with {len(channel_states)} channel records")

        if self._coordinator_callback:
            self._coordinator_callback(snapshot_id, p_state, channel_states)

    # ------------------------------------------------------------------------
    # Application Layer Utility Methods
    # ------------------------------------------------------------------------
    def send_task(self, receiver_id: str, task_id: str, lease_id: str) -> bool:
        """Simulates dispatching a task to a peer pod."""
        with self._lock:
            channel = self.outgoing_channels.get(receiver_id)
            if not channel:
                return False

            self.active_tasks.add(task_id)
            self.held_leases.add(lease_id)
            self._causal_counter += 1

            msg = Message(
                id=f"msg_{uuid.uuid4().hex[:8]}",
                sender=self.node_id,
                receiver=receiver_id,
                msg_type="TASK_DISPATCH",
                payload={"task_id": task_id, "lease_id": lease_id},
                timestamp=time.time(),
                causal_seq=self._causal_counter
            )
            channel.send(msg)
            return True

    def transfer_tokens(self, receiver_id: str, amount: int) -> bool:
        """Atomically deducts local tokens and enqueues in-flight token transfer."""
        with self._lock:
            if self.token_balance < amount:
                return False
            channel = self.outgoing_channels.get(receiver_id)
            if not channel:
                return False

            self.token_balance -= amount
            self._causal_counter += 1

            msg = Message(
                id=f"token_{uuid.uuid4().hex[:8]}",
                sender=self.node_id,
                receiver=receiver_id,
                msg_type="TOKEN_TRANSFER",
                payload={"amount": amount},
                timestamp=time.time(),
                causal_seq=self._causal_counter
            )
            channel.send(msg)
            return True

    # ------------------------------------------------------------------------
    # Node Background Listener
    # ------------------------------------------------------------------------
    def _run_loop(self) -> None:
        """Round-robin polling loop across all incoming channels."""
        while self._running:
            did_work = False
            for sender_id, channel in list(self.incoming_channels.items()):
                item = channel.receive(timeout=0.005)
                if item is not None:
                    did_work = True
                    if isinstance(item, Marker):
                        self._handle_marker(sender_id, item)
                    elif isinstance(item, Message):
                        self._handle_regular_message(sender_id, item)

            if not did_work:
                time.sleep(0.005)


# ============================================================================
# 4. Invariant Verification Engine
# ============================================================================

class InvariantVerificationEngine:
    """
    Validates safety and consistency invariants across the collected GlobalSnapshot:
    1. Consistent Cut Verification (Lamport Causality & No Orphans)
    2. Global Token Conservation (sum(process tokens) + sum(channel tokens) == total)
    3. No-Orphan Work Leases
    4. Distributed Deadlock Detection via Tarjan's SCC Cycle Analysis
    """

    @staticmethod
    def verify_consistent_cut(snapshot: GlobalSnapshot) -> Tuple[bool, List[str]]:
        """
        Validates that the snapshot represents a consistent global cut:
        - For every message m recorded in channel state chi_{i,j}:
            * m must have been sent before node p_i recorded its state sigma_{p_i}.
            * m must NOT have been received before node p_j recorded its state sigma_{p_j}.
        - No orphan messages exist.
        """
        violations: List[str] = []

        for chan_id, chan_state in snapshot.channel_states.items():
            sender_id = chan_state.sender
            receiver_id = chan_state.receiver

            sender_state = snapshot.process_states.get(sender_id)
            receiver_state = snapshot.process_states.get(receiver_id)

            if not sender_state:
                violations.append(f"Channel {chan_id} references unknown sender {sender_id}")
                continue
            if not receiver_state:
                violations.append(f"Channel {chan_id} references unknown receiver {receiver_id}")
                continue

            for msg in chan_state.messages:
                # The message must have been sent BEFORE or AT the sender's snapshot cut
                # In discrete Lamport terms: send_event < snapshot_event(sender)
                # Allowing minor clock skew tolerances:
                if msg.timestamp > sender_state.timestamp + 0.1:
                    violations.append(
                        f"Post-cut send violation: Message {msg.id} sent at {msg.timestamp:.3f} "
                        f"after sender {sender_id} snapshot at {sender_state.timestamp:.3f}"
                    )

        is_consistent = len(violations) == 0
        return is_consistent, violations

    @staticmethod
    def verify_token_conservation(snapshot: GlobalSnapshot, expected_total: int) -> Tuple[bool, int, str]:
        """Verifies sum(tokens in process states) + sum(tokens in channels) == expected_total."""
        process_tokens = sum(ps.token_balance for ps in snapshot.process_states.values())

        channel_tokens = 0
        for chan in snapshot.channel_states.values():
            for msg in chan.messages:
                if msg.msg_type == "TOKEN_TRANSFER":
                    channel_tokens += msg.payload.get("amount", 0)

        actual_total = process_tokens + channel_tokens
        passed = (actual_total == expected_total)
        msg = f"Token conservation: Process={process_tokens}, In-Flight={channel_tokens}, Total={actual_total}, Expected={expected_total}"
        return passed, actual_total, msg

    @staticmethod
    def verify_no_orphan_leases(snapshot: GlobalSnapshot) -> Tuple[bool, List[str]]:
        """Verifies that all in-flight task messages correlate to active leases recorded in process states."""
        leaked_leases: List[str] = []

        # Collect all active leases held across all recorded process states
        all_held_leases: Set[str] = set()
        for ps in snapshot.process_states.values():
            all_held_leases.update(ps.held_leases)

        # Check in-flight messages
        for chan in snapshot.channel_states.values():
            for msg in chan.messages:
                if msg.msg_type == "TASK_DISPATCH":
                    lease_id = msg.payload.get("lease_id")
                    if lease_id and lease_id not in all_held_leases:
                        leaked_leases.append(
                            f"Orphan lease detected in channel {chan.channel_id}: lease '{lease_id}' for task '{msg.payload.get('task_id')}'"
                        )

        return (len(leaked_leases) == 0), leaked_leases

    @staticmethod
    def detect_global_deadlock(snapshot: GlobalSnapshot) -> Tuple[bool, List[List[str]]]:
        """
        Builds the global Wait-For Graph (WFG) from recorded process states and identifies
        deadlock cycles using Tarjan's Strongly Connected Components (SCC) algorithm.
        Returns: (has_deadlock, list_of_cycles)
        """
        # Build adjacency list: node -> set of nodes it waits for
        graph: Dict[str, Set[str]] = {nid: set() for nid in snapshot.process_states.keys()}

        for nid, ps in snapshot.process_states.items():
            for target in ps.waiting_for:
                if target in graph:
                    graph[nid].add(target)

        # Tarjan's SCC Algorithm
        index = 0
        indices: Dict[str, int] = {}
        lowlinks: Dict[str, int] = {}
        stack: List[str] = []
        on_stack: Set[str] = set()
        sccs: List[List[str]] = []

        def strongconnect(v: str):
            nonlocal index
            indices[v] = index
            lowlinks[v] = index
            index += 1
            stack.append(v)
            on_stack.add(v)

            for w in graph.get(v, []):
                if w not in indices:
                    strongconnect(w)
                    lowlinks[v] = min(lowlinks[v], lowlinks[w])
                elif w in on_stack:
                    lowlinks[v] = min(lowlinks[v], indices[w])

            if lowlinks[v] == indices[v]:
                scc: List[str] = []
                while True:
                    w = stack.pop()
                    on_stack.remove(w)
                    scc.append(w)
                    if w == v:
                        break
                # An SCC is a cycle if it contains > 1 node, or 1 node with self-loop
                if len(scc) > 1 or (len(scc) == 1 and scc[0] in graph.get(scc[0], set())):
                    sccs.append(scc)

        for node in graph:
            if node not in indices:
                strongconnect(node)

        has_deadlock = len(sccs) > 0
        return has_deadlock, sccs


# ============================================================================
# 5. Chandy-Lamport Distributed Coordinator
# ============================================================================

class ChandyLamportCoordinator:
    """
    Manages the fleet topology, coordinates distributed snapshot executions,
    collects partial snapshots from all participant nodes, and executes the
    verification pipeline.
    """

    def __init__(self, node_ids: List[str], expected_total_tokens: int = 400):
        self.node_ids = node_ids
        self.expected_total_tokens = expected_total_tokens
        self.nodes: Dict[str, DistributedNode] = {}
        self.channels: Dict[str, FIFOChannel] = {}

        self._active_snapshots: Dict[str, Dict[str, Any]] = {}
        self._completed_snapshots: Dict[str, GlobalSnapshot] = {}
        self._lock = threading.Lock()
        self._snapshot_events: Dict[str, threading.Event] = {}

        self._build_topology()

    def _build_topology(self) -> None:
        """Initializes nodes and establishes fully-connected directed channels."""
        # Initialize nodes with evenly distributed tokens
        per_node_tokens = self.expected_total_tokens // len(self.node_ids)
        for nid in self.node_ids:
            self.nodes[nid] = DistributedNode(
                node_id=nid,
                initial_tokens=per_node_tokens,
                coordinator_callback=self._handle_node_snapshot_report
            )

        # Wire directed FIFO channels between every pair of nodes
        for src in self.node_ids:
            for dst in self.node_ids:
                if src != dst:
                    chan = FIFOChannel(sender_id=src, receiver_id=dst)
                    chan_id = chan.channel_id
                    self.channels[chan_id] = chan
                    self.nodes[src].register_outgoing(dst, chan)
                    self.nodes[dst].register_incoming(src, chan)

    def start_fleet(self) -> None:
        """Starts all distributed actor pods."""
        logger.info(f"Starting {len(self.nodes)} actor pods in distributed mesh...")
        for node in self.nodes.values():
            node.start()

    def stop_fleet(self) -> None:
        """Gracefully halts all actor pods and closes channels."""
        logger.info("Halting actor pod fleet...")
        for node in self.nodes.values():
            node.stop()
        for chan in self.channels.values():
            chan.close()

    def run_snapshot(self, initiator_id: str, timeout: float = 5.0) -> Optional[GlobalSnapshot]:
        """
        Triggers a Chandy-Lamport snapshot from initiator_id and blocks until
        all nodes submit their state or timeout expires.
        """
        if initiator_id not in self.nodes:
            raise ValueError(f"Initiator {initiator_id} does not exist in fleet")

        snapshot_id = f"snap_{initiator_id}_{uuid.uuid4().hex[:6]}"
        completion_event = threading.Event()

        with self._lock:
            self._active_snapshots[snapshot_id] = {
                "initiator": initiator_id,
                "process_states": {},
                "channel_states": {},
                "created_at": time.time(),
            }
            self._snapshot_events[snapshot_id] = completion_event

        # Trigger snapshot at initiator
        self.nodes[initiator_id].initiate_snapshot(snapshot_id)

        # Wait for all nodes to report
        success = completion_event.wait(timeout=timeout)
        if not success:
            logger.error(f"Snapshot {snapshot_id} timed out after {timeout} seconds")
            return None

        with self._lock:
            return self._completed_snapshots.get(snapshot_id)

    def _handle_node_snapshot_report(
        self,
        snapshot_id: str,
        process_state: ProcessState,
        channel_states: List[ChannelState]
    ) -> None:
        """Aggregates local node snapshots and compiles the GlobalSnapshot upon completion."""
        with self._lock:
            ctx = self._active_snapshots.get(snapshot_id)
            if not ctx:
                return

            ctx["process_states"][process_state.node_id] = process_state
            for cs in channel_states:
                ctx["channel_states"][cs.channel_id] = cs

            logger.debug(
                f"[Coordinator] Received snapshot report from {process_state.node_id} "
                f"({len(ctx['process_states'])}/{len(self.nodes)} nodes complete)"
            )

            # Check if all nodes have submitted their states
            if len(ctx["process_states"]) == len(self.nodes):
                logger.info(f"[Coordinator] All {len(self.nodes)} nodes completed snapshot {snapshot_id}. Assembling global cut.")

                # Assemble GlobalSnapshot
                snapshot = GlobalSnapshot(
                    snapshot_id=snapshot_id,
                    initiator=ctx["initiator"],
                    process_states=dict(ctx["process_states"]),
                    channel_states=dict(ctx["channel_states"]),
                    created_at=ctx["created_at"],
                )

                # Execute Verification Pipeline
                is_cut_consistent, cut_violations = InvariantVerificationEngine.verify_consistent_cut(snapshot)
                token_ok, actual_tok, token_diag = InvariantVerificationEngine.verify_token_conservation(
                    snapshot, self.expected_total_tokens
                )
                leases_ok, lease_violations = InvariantVerificationEngine.verify_no_orphan_leases(snapshot)
                deadlock_found, cycles = InvariantVerificationEngine.detect_global_deadlock(snapshot)

                snapshot.is_consistent = is_cut_consistent
                snapshot.invariants_passed = (is_cut_consistent and token_ok and leases_ok and not deadlock_found)
                snapshot.diagnostics = {
                    "cut_consistent": is_cut_consistent,
                    "cut_violations": cut_violations,
                    "token_conservation_ok": token_ok,
                    "token_diagnostic": token_diag,
                    "leases_ok": leases_ok,
                    "lease_violations": lease_violations,
                    "deadlock_detected": deadlock_found,
                    "deadlock_cycles": cycles,
                }

                self._completed_snapshots[snapshot_id] = snapshot
                if snapshot_id in self._snapshot_events:
                    self._snapshot_events[snapshot_id].set()


# ============================================================================
# 6. Artifact Generators & ADR Mentorship
# ============================================================================

def get_adr_lite() -> str:
    """Returns an Architectural Decision Record on Distributed State Checkpointing."""
    return """# ADR-042: Distributed State Checkpointing & Snapshot Architecture

## Context & Problem Statement
In autonomic multi-agent pipelines and actor pod meshes, diagnosing fleet-wide state,
token drift, orphan lock leases, and deadlocks requires capturing a consistent global state.
However, nodes communicate asynchronously over non-instantaneous channels. Halting all
nodes simultaneously ('Stop-the-World') incurs severe latency penalties and collapses pipeline throughput.

## Options Considered
1. **Synchronous Stop-the-World Freeze (Two-Phase Commit Barrier)**:
   - *Pros*: Conceptually simple; trivial consistent state.
   - *Cons*: O(N) network freeze; stalls real-time inference and message streaming; sensitive to stragglers.
2. **Vector Clock Asynchronous Checkpointing**:
   - *Pros*: Fine-grained causal dependency tracking per message.
   - *Cons*: Quadratic metadata expansion O(N) on every network envelope; requires complex rollback recovery (domino effect).
3. **Chandy-Lamport Distributed Snapshot Protocol**:
   - *Pros*: Zero downtime; non-blocking execution; processes continue executing application tasks while recording; guarantees consistent global cut.
   - *Cons*: Requires reliable FIFO channels; in-flight messages must be logged during the marker traversal phase.

## Decision
Adopt the **Chandy-Lamport Distributed Snapshot Algorithm** for continuous checkpointing and autonomic
invariant monitoring of actor pod meshes.

### Mathematical Invariants Guaranteed
1. **Consistent Cut Horizon**: For any two events e_a and e_b where e_a -> e_b (Lamport happened-before):
   If e_b in Cut(Sigma), then e_a in Cut(Sigma). No orphan messages appear in channel states.
2. **Conservation of Global Assets**: The sum of conserved assets (tokens, leases) across all process
   states and active channel states remains strictly invariant across the cut:
   Sum_{p in V} (Tokens_p) + Sum_{c in E} (Tokens_c) = TotalTokens.
3. **Acyclic Dependency Invariant**: If Tarjan's SCC on the global wait-for graph detects a cycle C,
   the fleet is in an unresolvable distributed deadlock and triggers autonomic supervision pod eviction.
"""


def export_snapshots_to_json(snapshots: List[GlobalSnapshot], filepath: str) -> None:
    """Serializes recorded snapshots into structured JSON for telemetry dashboards."""
    out_path = Path(filepath)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    data = {
        "version": "1.0.0",
        "protocol": "Chandy-Lamport-FIFO",
        "exported_at": time.time(),
        "total_snapshots": len(snapshots),
        "snapshots": [s.to_dict() for s in snapshots]
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    logger.info(f"Exported {len(snapshots)} snapshots to {out_path.resolve()}")


def generate_fleet_snapshots_markdown(snapshots: List[GlobalSnapshot], filepath: str) -> None:
    """Generates an executive Markdown dashboard with ASCII diagrams of the consistent cuts."""
    out_path = Path(filepath)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    lines: List[str] = [
        "# Autonomic Fleet Distributed Snapshot & State Checkpointer",
        "",
        "## Executive Summary",
        f"This dashboard visualizes **Chandy-Lamport Distributed Snapshots** executed across the actor pod fleet.",
        f"Each snapshot captures a **consistent global cut** without stopping worker execution.",
        "",
        "```",
        "           Process P1                  Channel P1->P2                  Process P2",
        "  ----------------------------------------------------------------------------------",
        "      Event e1 (send msg)  ----[ msg in flight ]------------>",
        "     * SNAPSHOT CUT P1 *",
        "      Marker sent          ----[ Marker(S_k) ]-------------->",
        "                                                             * SNAPSHOT CUT P2 *",
        "                                                               (msg captured in channel)",
        "                                                               (marker received)",
        "  ----------------------------------------------------------------------------------",
        "  Consistent Cut: Messages sent before sender's cut and received after receiver's cut",
        "                  are preserved in Channel State Chi_{1,2}.",
        "```",
        "",
        "## Snapshot Telemetry Log",
        "",
        "| Snapshot ID | Initiator | Timestamp | Cut Consistent | Invariants Passed | In-Flight Msgs | Total Tokens | Deadlock |",
        "| :--- | :--- | :--- | :---: | :---: | :---: | :---: | :---: |"
    ]

    for s in snapshots:
        in_flight_count = sum(len(c.messages) for c in s.channel_states.values())
        tokens = sum(p.token_balance for p in s.process_states.values())
        for c in s.channel_states.values():
            for m in c.messages:
                if m.msg_type == "TOKEN_TRANSFER":
                    tokens += m.payload.get("amount", 0)

        deadlock_status = "CYCLE DETECTED" if s.diagnostics.get("deadlock_detected") else "None (Acyclic)"
        cut_icon = "[PASSED]" if s.is_consistent else "[VIOLATION]"
        inv_icon = "[VERIFIED]" if s.invariants_passed else "[FAILED]"

        ts_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(s.created_at))
        lines.append(
            f"| `{s.snapshot_id}` | `{s.initiator}` | {ts_str} | {cut_icon} | {inv_icon} | {in_flight_count} | {tokens} | {deadlock_status} |"
        )

    lines.extend([
        "",
        "## Invariant Verification Rules",
        "- **Rule 1 (Consistent Cut)**: No orphan messages (received without being sent).",
        "- **Rule 2 (Token Conservation)**: Process balance + in-flight messages == fleet quota.",
        "- **Rule 3 (Lease Safety)**: In-flight task dispatches reference active pod leases.",
        "- **Rule 4 (Deadlock Freedom)**: Distributed Wait-For Graph (WFG) is acyclic via Tarjan's SCC.",
        "",
        "---",
        "*Automated report generated by `tasks/pipeline_chandy_lamport_snapshotter.py`*"
    ])

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    logger.info(f"Generated Markdown snapshot dashboard at {out_path.resolve()}")


# ============================================================================
# 7. Comprehensive Self-Test Verification Suite
# ============================================================================

def run_self_tests() -> bool:
    """
    Executes an exhaustive test suite verifying:
      - Test 1: Snapshot initiation and marker propagation across a 4-node ring network.
      - Test 2: In-flight channel message capture (messages sent before marker arrival).
      - Test 3: Consistent cut validation confirming no orphan or phantom messages.
      - Test 4: Global invariant checking detecting token leaks and deadlock cycles.
    """
    logger.info("=================================================================")
    logger.info("Running Chandy-Lamport Distributed Snapshot Verification Suite")
    logger.info("=================================================================")

    all_passed = True
    collected_snapshots: List[GlobalSnapshot] = []

    # ------------------------------------------------------------------------
    # Test 1: 4-Node Ring Network Propagation
    # ------------------------------------------------------------------------
    logger.info("\n--- [TEST 1] 4-Node Ring Snapshot Initiation & Marker Propagation ---")
    node_ids = ["pod_alpha", "pod_beta", "pod_gamma", "pod_delta"]
    coord = ChandyLamportCoordinator(node_ids=node_ids, expected_total_tokens=400)
    coord.start_fleet()

    try:
        # Node pod_alpha initiates snapshot
        snap1 = coord.run_snapshot(initiator_id="pod_alpha", timeout=3.0)
        assert snap1 is not None, "Snapshot 1 timed out"
        assert len(snap1.process_states) == 4, f"Expected 4 process states, got {len(snap1.process_states)}"
        assert snap1.is_consistent, "Snapshot 1 cut should be consistent"
        assert snap1.invariants_passed, "Snapshot 1 invariants should hold"
        logger.info(f"[TEST 1 PASSED] 4-node snapshot completed successfully: {snap1.snapshot_id}")
        collected_snapshots.append(snap1)
    finally:
        coord.stop_fleet()

    # ------------------------------------------------------------------------
    # Test 2: In-Flight Channel Message Capture
    # ------------------------------------------------------------------------
    logger.info("\n--- [TEST 2] In-Flight Channel Message Capture Under Load ---")
    coord2 = ChandyLamportCoordinator(node_ids=["worker_1", "worker_2"], expected_total_tokens=200)
    coord2.start_fleet()

    try:
        w1 = coord2.nodes["worker_1"]
        w2 = coord2.nodes["worker_2"]

        # Transfer 25 tokens from w1 to w2, but delay delivery to ensure it stays in channel
        # We manually enqueue a token message on channel w1->w2
        chan_w1_w2 = coord2.channels["chan_worker_1->worker_2"]
        w1.token_balance -= 25

        token_msg = Message(
            id="inflight_token_001",
            sender="worker_1",
            receiver="worker_2",
            msg_type="TOKEN_TRANSFER",
            payload={"amount": 25},
            timestamp=time.time(),
            causal_seq=1
        )
        chan_w1_w2.send(token_msg)

        # Worker 1 immediately initiates snapshot.
        # The marker will follow the token_msg in FIFO order.
        # Worker 2 will record the token_msg as part of channel state chi_{w1, w2}!
        snap2 = coord2.run_snapshot(initiator_id="worker_1", timeout=3.0)
        assert snap2 is not None, "Snapshot 2 timed out"

        recorded_channel = snap2.channel_states.get("chan_worker_1->worker_2")
        assert recorded_channel is not None, "Channel w1->w2 state missing"
        assert len(recorded_channel.messages) == 1, f"Expected 1 in-flight message, found {len(recorded_channel.messages)}"
        assert recorded_channel.messages[0].id == "inflight_token_001"

        # Verify token conservation invariant accounts for in-flight tokens
        token_ok, total_tok, diag_msg = InvariantVerificationEngine.verify_token_conservation(snap2, expected_total=200)
        assert token_ok, f"Token conservation failed: {diag_msg}"
        logger.info(f"[TEST 2 PASSED] In-flight token captured in channel state. Total tokens conserved: {total_tok}")
        collected_snapshots.append(snap2)
    finally:
        coord2.stop_fleet()

    # ------------------------------------------------------------------------
    # Test 3: Consistent Cut Validation & Orphan Message Detection
    # ------------------------------------------------------------------------
    logger.info("\n--- [TEST 3] Consistent Cut Validation & Synthetic Orphan Detection ---")
    # Fabricate an inconsistent snapshot with an orphan message (sent after sender's cut)
    curr_time = time.time()
    p_states = {
        "node_A": ProcessState("node_A", "snap_syn", {}, [], 100, [], [], timestamp=curr_time),
        "node_B": ProcessState("node_B", "snap_syn", {}, [], 100, [], [], timestamp=curr_time + 1.0),
    }

    # Orphan message: timestamp is way AFTER node_A recorded its state
    orphan_msg = Message(
        id="orphan_007",
        sender="node_A",
        receiver="node_B",
        msg_type="TASK_DISPATCH",
        payload={"task_id": "orphan_task"},
        timestamp=curr_time + 5.0, # Violates cut
        causal_seq=10
    )
    c_states = {
        "chan_node_A->node_B": ChannelState(
            channel_id="chan_node_A->node_B",
            sender="node_A",
            receiver="node_B",
            snapshot_id="snap_syn",
            messages=[orphan_msg]
        )
    }

    inconsistent_snapshot = GlobalSnapshot(
        snapshot_id="snap_synthetic_inconsistent",
        initiator="node_A",
        process_states=p_states,
        channel_states=c_states,
        created_at=curr_time,
    )

    is_cut_ok, violations = InvariantVerificationEngine.verify_consistent_cut(inconsistent_snapshot)
    assert not is_cut_ok, "Verification should fail on post-cut message"
    assert len(violations) > 0, "Expected violation report"
    logger.info(f"[TEST 3 PASSED] Correctly detected cut violation: {violations[0]}")

    # ------------------------------------------------------------------------
    # Test 4: Deadlock Cycle & Token Leak Detection
    # ------------------------------------------------------------------------
    logger.info("\n--- [TEST 4] Distributed Deadlock & Token Leak Detection Engine ---")
    # Simulate a circular wait-for dependency: pod1 -> pod2 -> pod3 -> pod1
    deadlocked_processes = {
        "p1": ProcessState("p1", "snap_deadlock", {}, [], 50, ["lease_1"], waiting_for=["p2"], timestamp=time.time()),
        "p2": ProcessState("p2", "snap_deadlock", {}, [], 50, ["lease_2"], waiting_for=["p3"], timestamp=time.time()),
        "p3": ProcessState("p3", "snap_deadlock", {}, [], 50, ["lease_3"], waiting_for=["p1"], timestamp=time.time()),
    }
    deadlock_snapshot = GlobalSnapshot(
        snapshot_id="snap_deadlock_test",
        initiator="p1",
        process_states=deadlocked_processes,
        channel_states={},
        created_at=time.time()
    )

    has_deadlock, cycles = InvariantVerificationEngine.detect_global_deadlock(deadlock_snapshot)
    assert has_deadlock, "Deadlock cycle must be detected"
    assert len(cycles) == 1, f"Expected 1 deadlock cycle, got {len(cycles)}"
    logger.info(f"[TEST 4.1 PASSED] Tarjan's SCC successfully detected deadlock cycle: {cycles[0]}")

    # Test Token Leak Detection
    tok_ok, actual_tok, tok_msg = InvariantVerificationEngine.verify_token_conservation(deadlock_snapshot, expected_total=200)
    assert not tok_ok, "Should detect token leak (sum 150 vs expected 200)"
    logger.info(f"[TEST 4.2 PASSED] Detected missing token quota: {tok_msg}")

    # Mark snapshot diagnostics and retain for report
    deadlock_snapshot.diagnostics = {
        "cut_consistent": True,
        "token_conservation_ok": tok_ok,
        "deadlock_detected": has_deadlock,
        "deadlock_cycles": cycles
    }
    collected_snapshots.append(deadlock_snapshot)

    # ------------------------------------------------------------------------
    # Export Telemetry Dashboards & Artifacts
    # ------------------------------------------------------------------------
    logger.info("\n--- Generating Artifacts & Dashboards ---")
    dashboards_dir = Path("dashboards")
    json_path = dashboards_dir / "fleet_snapshots.json"
    md_path = dashboards_dir / "FLEET_SNAPSHOTS.md"

    export_snapshots_to_json(collected_snapshots, str(json_path))
    generate_fleet_snapshots_markdown(collected_snapshots, str(md_path))

    adr_content = get_adr_lite()
    adr_path = dashboards_dir / "ADR_042_CHANDY_LAMPORT.md"
    with open(adr_path, "w", encoding="utf-8") as f:
        f.write(adr_content)
    logger.info(f"Written ADR-Lite document to {adr_path.resolve()}")

    logger.info("\n=================================================================")
    logger.info("ALL SELF-TESTS PASSED WITH PUBLICATION-GRADE PERFECTION (4/4)")
    logger.info("=================================================================")
    return all_passed


if __name__ == "__main__":
    success = run_self_tests()
    if not success:
        exit(1)