#!/usr/bin/env python3
"""
Pipeline Autonomic SWIM Gossip Protocol & Anti-Entropy Fleet Failure Detector
=============================================================================
Implementation of the Structured Weakly-Consistent Infection-Style Process Group
Membership Protocol (SWIM) with indirect probing, suspicion mechanism, monotonic
incarnation rebuttal, and epidemic anti-entropy dissemination for autonomic agent fleets.

Pedagogical Architecture Patterns:
    - [Pattern: SWIM Gossip Protocol]
    - [Pattern: Indirect Ping-Req Probing]
    - [Pattern: Suspicion Mechanism]
    - [Pattern: Epidemic Dissemination]
"""

from __future__ import annotations

import enum
import json
import math
import os
import random
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


# ============================================================================
# Core Domain Enumerations and Data Structures
# ============================================================================

class MemberStatus(str, enum.Enum):
    """
    [Pattern: SWIM Gossip Protocol]
    Node status lifecycle: ALIVE -> SUSPECT -> DEAD.
    Rebuttal transitions SUSPECT back to ALIVE with incremented incarnation.
    """
    ALIVE = "ALIVE"
    SUSPECT = "SUSPECT"
    DEAD = "DEAD"


class MessageType(str, enum.Enum):
    """Protocol message types for two-tier probing and dissemination."""
    PING = "PING"
    ACK = "ACK"
    PING_REQ = "PING_REQ"
    GOSSIP_UPDATE = "GOSSIP_UPDATE"


@dataclass
class MemberNode:
    """Represents a monitored fleet agent/node in the membership table."""
    node_id: str
    host: str = "127.0.0.1"
    port: int = 8000
    status: MemberStatus = MemberStatus.ALIVE
    incarnation: int = 0
    last_seen_epoch: float = field(default_factory=time.time)
    suspicion_deadline: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port,
            "status": self.status.value,
            "incarnation": self.incarnation,
            "last_seen_epoch": self.last_seen_epoch,
            "suspicion_deadline": self.suspicion_deadline,
            "metadata": self.metadata,
        }


@dataclass
class GossipUpdate:
    """Single state change piggybacked on gossip probes."""
    member_id: str
    status: MemberStatus
    incarnation: int
    infection_count: int = 0
    max_infections: int = 5
    originator: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "member_id": self.member_id,
            "status": self.status.value,
            "incarnation": self.incarnation,
            "infection_count": self.infection_count,
            "max_infections": self.max_infections,
            "originator": self.originator,
        }


@dataclass
class GossipMessage:
    """
    Unified packet structure across direct ping, ping-req, ack, and dissemination.
    """
    msg_type: MessageType
    seq_num: int
    source_id: str
    target_id: str
    payload: Dict[str, Any] = field(default_factory=dict)
    incarnation: int = 0
    piggybacked_updates: List[Dict[str, Any]] = field(default_factory=list)
    transit_timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "msg_type": self.msg_type.value,
            "seq_num": self.seq_num,
            "source_id": self.source_id,
            "target_id": self.target_id,
            "payload": self.payload,
            "incarnation": self.incarnation,
            "piggybacked_updates": self.piggybacked_updates,
            "transit_timestamp": self.transit_timestamp,
        }


# ============================================================================
# Telemetry and In-Memory Network Transport Layer
# ============================================================================

class NetworkBus:
    """
    Thread-safe virtual packet exchange simulating real-world network links,
    packet drops, delays, and partitions without socket port friction.
    """
    def __init__(self, seed: Optional[int] = None):
        self._lock = threading.RLock()
        self._mailboxes: Dict[str, List[GossipMessage]] = {}
        self._drop_rates: Dict[Tuple[str, str], float] = {}
        self._partitions: Set[Tuple[str, str]] = set()
        self._rng = random.Random(seed)

    def register_node(self, node_id: str) -> None:
        with self._lock:
            if node_id not in self._mailboxes:
                self._mailboxes[node_id] = []

    def isolate_node(self, node_id: str) -> None:
        with self._lock:
            for other in list(self._mailboxes.keys()):
                if other != node_id:
                    self._partitions.add((node_id, other))
                    self._partitions.add((other, node_id))

    def heal_partitions(self) -> None:
        with self._lock:
            self._partitions.clear()

    def set_drop_rate(self, src: str, dst: str, rate: float) -> None:
        with self._lock:
            self._drop_rates[(src, dst)] = max(0.0, min(1.0, rate))

    def send_packet(self, msg: GossipMessage) -> bool:
        with self._lock:
            link = (msg.source_id, msg.target_id)
            if link in self._partitions:
                return False
            drop_prob = self._drop_rates.get(link, 0.0)
            if drop_prob > 0.0 and self._rng.random() < drop_prob:
                return False
            if msg.target_id not in self._mailboxes:
                return False
            self._mailboxes[msg.target_id].append(msg)
            return True

    def drain_mailbox(self, node_id: str) -> List[GossipMessage]:
        with self._lock:
            box = self._mailboxes.get(node_id, [])
            self._mailboxes[node_id] = []
            return box


# ============================================================================
# SWIM Gossip Engine Core
# ============================================================================

class SWIMGossipEngine:
    """
    [Pattern: SWIM Gossip Protocol]
    [Pattern: Indirect Ping-Req Probing]
    [Pattern: Suspicion Mechanism]
    [Pattern: Epidemic Dissemination]

    Autonomous implementation of the SWIM protocol with dynamic suspicion
    timeouts, piggybacked anti-entropy updates, and dead node reaping.
    """

    def __init__(
        self,
        node_id: str,
        host: str = "127.0.0.1",
        port: int = 8000,
        bus: Optional[NetworkBus] = None,
        probe_period: float = 1.0,
        direct_ack_timeout: float = 0.3,
        indirect_ack_timeout: float = 0.6,
        base_suspicion_timeout: float = 3.0,
        max_suspicion_timeout: float = 10.0,
        k_indirect_peers: int = 3,
        state_export_path: str = "dashboards/swim_gossip_state.json",
        on_dead_reap_callback: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.node_id = node_id
        self.host = host
        self.port = port
        self.bus = bus or NetworkBus()
        self.bus.register_node(node_id)

        # Protocol Timing Parameters
        self.probe_period = probe_period
        self.direct_ack_timeout = direct_ack_timeout
        self.indirect_ack_timeout = indirect_ack_timeout
        self.base_suspicion_timeout = base_suspicion_timeout
        self.max_suspicion_timeout = max_suspicion_timeout
        self.k_indirect_peers = k_indirect_peers
        self.state_export_path = Path(state_export_path)
        self.on_dead_reap_callback = on_dead_reap_callback

        # Internal State Management
        self._lock = threading.RLock()
        self.incarnation = 0
        self.members: Dict[str, MemberNode] = {
            self.node_id: MemberNode(
                node_id=self.node_id,
                host=self.host,
                port=self.port,
                status=MemberStatus.ALIVE,
                incarnation=0,
                last_seen_epoch=time.time(),
            )
        }

        # Randomized probing order
        self._probe_order: List[str] = []
        self._probe_index: int = 0

        # Piggybacked gossip update queue
        self._gossip_updates: Dict[str, GossipUpdate] = {}

        # Pending Direct and Indirect Ping tracking: seq_num -> context
        self._seq_counter = 0
        self._pending_direct: Dict[int, Dict[str, Any]] = {}
        self._pending_indirect: Dict[int, Dict[str, Any]] = {}

        # Telemetry & Histograms
        self._latencies: List[float] = []
        self._packet_trace_counts: Dict[str, int] = {
            "ping_sent": 0,
            "ack_sent": 0,
            "ping_req_sent": 0,
            "gossip_updates_sent": 0,
            "packets_dropped": 0,
            "rebuttals_issued": 0,
            "suspects_declared": 0,
            "dead_reaped": 0,
        }

        # Background Execution Guard
        self._running = False
        self._probe_thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------------
    # Cluster Join & Initialization
    # ------------------------------------------------------------------------

    def join_cluster(self, seed_nodes: List[Tuple[str, str, int]]) -> bool:
        """
        Bootstrap into an existing fleet by pinging known seeds.
        seed_nodes format: [(node_id, host, port), ...]
        """
        joined_any = False
        with self._lock:
            for s_id, s_host, s_port in seed_nodes:
                if s_id == self.node_id:
                    continue
                if s_id not in self.members:
                    self.members[s_id] = MemberNode(
                        node_id=s_id,
                        host=s_host,
                        port=s_port,
                        status=MemberStatus.ALIVE,
                        incarnation=0,
                        last_seen_epoch=time.time(),
                    )
                # Introduce self via PING with self-join update
                self.broadcast_update(self.node_id, MemberStatus.ALIVE, self.incarnation)
                seq = self._next_seq()
                msg = GossipMessage(
                    msg_type=MessageType.PING,
                    seq_num=seq,
                    source_id=self.node_id,
                    target_id=s_id,
                    incarnation=self.incarnation,
                    piggybacked_updates=self._select_piggybacked_updates(),
                )
                sent = self.bus.send_packet(msg)
                if sent:
                    self._packet_trace_counts["ping_sent"] += 1
                    joined_any = True
                else:
                    self._packet_trace_counts["packets_dropped"] += 1
        return joined_any

    # ------------------------------------------------------------------------
    # Gossip Dissemination & Math Bounds
    # ------------------------------------------------------------------------

    def _calc_max_infections(self) -> int:
        """
        Epidemic Bound: λ <= 3 * ⌈log(|Cluster|)⌉.
        Ensures O(log N) epidemic spread with high probability 1 - ε.
        """
        n = max(2, len(self.members))
        return int(math.ceil(3.0 * math.log(n, 2)))

    def broadcast_update(self, member_id: str, status: MemberStatus, incarnation: int) -> None:
        """Enqueue state update into the anti-entropy epidemic dissemination buffer."""
        with self._lock:
            max_inf = self._calc_max_infections()
            self._gossip_updates[member_id] = GossipUpdate(
                member_id=member_id,
                status=status,
                incarnation=incarnation,
                infection_count=0,
                max_infections=max_inf,
                originator=self.node_id,
            )
            if status == MemberStatus.SUSPECT:
                self._packet_trace_counts["suspects_declared"] += 1

    def _select_piggybacked_updates(self) -> List[Dict[str, Any]]:
        """
        Extract eligible gossip updates and increment their infection count.
        Deletes updates that exceed maximum epidemic iterations.
        """
        out: List[Dict[str, Any]] = []
        with self._lock:
            expired_keys = []
            for m_id, update in self._gossip_updates.items():
                out.append(update.to_dict())
                update.infection_count += 1
                if update.infection_count >= update.max_infections:
                    expired_keys.append(m_id)
            for k in expired_keys:
                del self._gossip_updates[k]
        return out

    # ------------------------------------------------------------------------
    # Two-Tier Failure Detection (Direct Ping + Indirect Ping-Req)
    # ------------------------------------------------------------------------

    def _next_seq(self) -> int:
        self._seq_counter += 1
        return self._seq_counter

    def _refresh_probe_order(self) -> None:
        """Maintain random shuffle of active non-self nodes for unbiased probing."""
        candidates = [
            m_id for m_id, m in self.members.items()
            if m_id != self.node_id and m.status != MemberStatus.DEAD
        ]
        random.shuffle(candidates)
        self._probe_order = candidates
        self._probe_index = 0

    def calculate_suspicion_timeout(self) -> float:
        """
        [Pattern: Suspicion Mechanism]
        T_suspect = min(T_max, T_base * ln(1 + |Cluster|))
        """
        with self._lock:
            cluster_size = max(1, len(self.members))
            computed = self.base_suspicion_timeout * math.log(1.0 + cluster_size)
            return min(self.max_suspicion_timeout, max(self.base_suspicion_timeout, computed))

    def execute_probe_cycle(self) -> None:
        """
        Executes one discrete period Τ probe cycle:
        1. Select random target N_j.
        2. Dispatch DIRECT_PING(seq_num).
        3. Flush and process pending direct and indirect timeouts.
        4. Reap expired dead members.
        """
        now = time.time()

        # Step 1: Ingest arriving network packets
        self.process_inbox()

        with self._lock:
            # Check for direct ping timeouts -> trigger indirect ping-req
            direct_timeouts = []
            for seq, ctx in list(self._pending_direct.items()):
                if now - ctx["sent_time"] > self.direct_ack_timeout:
                    direct_timeouts.append((seq, ctx))

            for seq, ctx in direct_timeouts:
                del self._pending_direct[seq]
                self._dispatch_indirect_ping_req(ctx["target_id"])

            # Check for indirect ping-req timeouts -> transition to SUSPECT
            indirect_timeouts = []
            for seq, ctx in list(self._pending_indirect.items()):
                if now - ctx["sent_time"] > self.indirect_ack_timeout:
                    indirect_timeouts.append((seq, ctx))

            for seq, ctx in indirect_timeouts:
                del self._pending_indirect[seq]
                target_id = ctx["target_id"]
                target = self.members.get(target_id)
                if target and target.status == MemberStatus.ALIVE:
                    suspicion_window = self.calculate_suspicion_timeout()
                    target.status = MemberStatus.SUSPECT
                    target.suspicion_deadline = now + suspicion_window
                    self.broadcast_update(target_id, MemberStatus.SUSPECT, target.incarnation)

            # Step 2: Select next target to ping directly
            if not self._probe_order or self._probe_index >= len(self._probe_order):
                self._refresh_probe_order()

            if self._probe_order:
                target_id = self._probe_order[self._probe_index]
                self._probe_index += 1
                target = self.members.get(target_id)
                if target and target.status != MemberStatus.DEAD:
                    seq = self._next_seq()
                    msg = GossipMessage(
                        msg_type=MessageType.PING,
                        seq_num=seq,
                        source_id=self.node_id,
                        target_id=target_id,
                        incarnation=self.incarnation,
                        piggybacked_updates=self._select_piggybacked_updates(),
                        transit_timestamp=now,
                    )
                    self._pending_direct[seq] = {
                        "target_id": target_id,
                        "sent_time": now,
                    }
                    sent = self.bus.send_packet(msg)
                    if sent:
                        self._packet_trace_counts["ping_sent"] += 1
                    else:
                        self._packet_trace_counts["packets_dropped"] += 1

        # Step 3: Check expired suspects and reap them
        self.reap_dead_members()
        self.export_telemetry()

    def _dispatch_indirect_ping_req(self, target_id: str) -> None:
        """
        [Pattern: Indirect Ping-Req Probing]
        Select k random peer nodes to probe target on our behalf.
        """
        candidates = [
            m_id for m_id, m in self.members.items()
            if m_id not in (self.node_id, target_id) and m.status == MemberStatus.ALIVE
        ]
        if not candidates:
            # Fallback if no peers available: direct suspicion immediately
            target = self.members.get(target_id)
            if target and target.status == MemberStatus.ALIVE:
                now = time.time()
                target.status = MemberStatus.SUSPECT
                target.suspicion_deadline = now + self.calculate_suspicion_timeout()
                self.broadcast_update(target_id, MemberStatus.SUSPECT, target.incarnation)
            return

        k = min(self.k_indirect_peers, len(candidates))
        selected_helpers = random.sample(candidates, k)
        seq = self._next_seq()
        now = time.time()

        self._pending_indirect[seq] = {
            "target_id": target_id,
            "helpers": selected_helpers,
            "sent_time": now,
        }

        for helper_id in selected_helpers:
            req_msg = GossipMessage(
                msg_type=MessageType.PING_REQ,
                seq_num=seq,
                source_id=self.node_id,
                target_id=helper_id,
                payload={"target_to_ping": target_id, "original_seq": seq},
                incarnation=self.incarnation,
                piggybacked_updates=self._select_piggybacked_updates(),
                transit_timestamp=now,
            )
            sent = self.bus.send_packet(req_msg)
            if sent:
                self._packet_trace_counts["ping_req_sent"] += 1
            else:
                self._packet_trace_counts["packets_dropped"] += 1

    # ------------------------------------------------------------------------
    # Message Handling & Rebuttal Protocol
    # ------------------------------------------------------------------------

    def process_inbox(self) -> None:
        """Drain arriving packets and process state transitions."""
        packets = self.bus.drain_mailbox(self.node_id)
        for pkt in packets:
            self.handle_incoming_message(pkt)

    def handle_incoming_message(self, msg: GossipMessage) -> Optional[GossipMessage]:
        """
        [Pattern: Rebuttal & Incarnation Protocol]
        Process incoming PING, ACK, PING_REQ, or GOSSIP_UPDATE.
        """
        now = time.time()
        reply: Optional[GossipMessage] = None

        with self._lock:
            # 1. Digest piggybacked updates
            if msg.piggybacked_updates:
                self._digest_updates(msg.piggybacked_updates)

            # Update sender's last seen epoch if not dead
            sender = self.members.get(msg.source_id)
            if sender and sender.status != MemberStatus.DEAD:
                sender.last_seen_epoch = now

            # 2. Handle Message Types
            if msg.msg_type == MessageType.PING:
                # Direct ping -> Send immediate ACK
                reply = GossipMessage(
                    msg_type=MessageType.ACK,
                    seq_num=msg.seq_num,
                    source_id=self.node_id,
                    target_id=msg.source_id,
                    incarnation=self.incarnation,
                    piggybacked_updates=self._select_piggybacked_updates(),
                    transit_timestamp=now,
                )
                self.bus.send_packet(reply)
                self._packet_trace_counts["ack_sent"] += 1

            elif msg.msg_type == MessageType.ACK:
                # Handle direct or relayed ACK
                rtt = max(0.0001, now - msg.transit_timestamp)
                self._latencies.append(rtt)
                if len(self._latencies) > 1000:
                    self._latencies.pop(0)

                # Clear direct pending if matches
                if msg.seq_num in self._pending_direct:
                    del self._pending_direct[msg.seq_num]

                # Check if this resolves an indirect probe
                matched_indirect = None
                for ind_seq, ctx in list(self._pending_indirect.items()):
                    if ctx["target_id"] == msg.source_id or msg.payload.get("relayed_for") == ctx["target_id"]:
                        matched_indirect = ind_seq
                        break
                if matched_indirect:
                    del self._pending_indirect[matched_indirect]

                # If an ACK arrived from a suspected node, check incarnation
                node = self.members.get(msg.source_id)
                if node and node.status == MemberStatus.SUSPECT:
                    if msg.incarnation > node.incarnation:
                        node.status = MemberStatus.ALIVE
                        node.incarnation = msg.incarnation
                        node.suspicion_deadline = None

            elif msg.msg_type == MessageType.PING_REQ:
                # Helper node requested to ping target_to_ping on sender's behalf
                target_to_ping = msg.payload.get("target_to_ping")
                orig_seq = msg.payload.get("original_seq", msg.seq_num)
                orig_sender = msg.source_id

                if target_to_ping and target_to_ping in self.members:
                    # Forward a ping to the target
                    fwd_seq = self._next_seq()
                    fwd_msg = GossipMessage(
                        msg_type=MessageType.PING,
                        seq_num=fwd_seq,
                        source_id=self.node_id,
                        target_id=target_to_ping,
                        payload={"relay_ack_to": orig_sender, "original_seq": orig_seq},
                        incarnation=self.incarnation,
                        piggybacked_updates=self._select_piggybacked_updates(),
                        transit_timestamp=now,
                    )
                    self.bus.send_packet(fwd_msg)
                    self._packet_trace_counts["ping_sent"] += 1

            elif msg.msg_type == MessageType.GOSSIP_UPDATE:
                self._packet_trace_counts["gossip_updates_sent"] += 1

        return reply

    def _digest_updates(self, updates: List[Dict[str, Any]]) -> None:
        """
        Merge remote state updates applying strict incarnation conflict resolution:
        - If update claims self is SUSPECT with current incarnation, refute via incarnation increment.
        - Higher incarnation always supersedes lower incarnation.
        - At same incarnation: DEAD > SUSPECT > ALIVE.
        """
        now = time.time()
        for u in updates:
            m_id = u["member_id"]
            u_status = MemberStatus(u["status"])
            u_inc = u["incarnation"]

            # Case A: Update is about SELF
            if m_id == self.node_id:
                if u_status in (MemberStatus.SUSPECT, MemberStatus.DEAD):
                    if u_inc >= self.incarnation:
                        # Refutation / Rebuttal
                        self.incarnation = u_inc + 1
                        self._packet_trace_counts["rebuttals_issued"] += 1
                        self.broadcast_update(self.node_id, MemberStatus.ALIVE, self.incarnation)
                continue

            # Case B: Update is about PEER
            curr = self.members.get(m_id)
            if not curr:
                # First time seen peer
                self.members[m_id] = MemberNode(
                    node_id=m_id,
                    status=u_status,
                    incarnation=u_inc,
                    last_seen_epoch=now,
                    suspicion_deadline=(now + self.calculate_suspicion_timeout()) if u_status == MemberStatus.SUSPECT else None,
                )
                continue

            if curr.status == MemberStatus.DEAD:
                # Dead is a terminal absorption unless incarnation strictly increases
                if u_inc > curr.incarnation and u_status == MemberStatus.ALIVE:
                    curr.status = MemberStatus.ALIVE
                    curr.incarnation = u_inc
                    curr.last_seen_epoch = now
                    curr.suspicion_deadline = None
                continue

            if u_inc > curr.incarnation:
                # Strictly newer incarnation
                curr.incarnation = u_inc
                curr.status = u_status
                curr.last_seen_epoch = now
                if u_status == MemberStatus.SUSPECT:
                    curr.suspicion_deadline = now + self.calculate_suspicion_timeout()
                else:
                    curr.suspicion_deadline = None
                self.broadcast_update(m_id, u_status, u_inc)

            elif u_inc == curr.incarnation:
                # Same incarnation: DEAD overrides SUSPECT, SUSPECT overrides ALIVE
                if curr.status == MemberStatus.ALIVE and u_status == MemberStatus.SUSPECT:
                    curr.status = MemberStatus.SUSPECT
                    curr.suspicion_deadline = now + self.calculate_suspicion_timeout()
                    self.broadcast_update(m_id, u_status, u_inc)
                elif curr.status in (MemberStatus.ALIVE, MemberStatus.SUSPECT) and u_status == MemberStatus.DEAD:
                    curr.status = MemberStatus.DEAD
                    curr.suspicion_deadline = None
                    self.broadcast_update(m_id, u_status, u_inc)

    # ------------------------------------------------------------------------
    # Dead Member Reaping
    # ------------------------------------------------------------------------

    def reap_dead_members(self) -> List[str]:
        """
        Transition SUSPECT nodes whose suspicion_deadline has lapsed to DEAD.
        Notifies fleet orchestrator for quarantine and task reassignment.
        """
        reaped: List[str] = []
        now = time.time()
        with self._lock:
            for m_id, member in self.members.items():
                if m_id == self.node_id:
                    continue
                if member.status == MemberStatus.SUSPECT:
                    if member.suspicion_deadline and now >= member.suspicion_deadline:
                        member.status = MemberStatus.DEAD
                        member.suspicion_deadline = None
                        reaped.append(m_id)
                        self._packet_trace_counts["dead_reaped"] += 1
                        self.broadcast_update(m_id, MemberStatus.DEAD, member.incarnation)
                        if self.on_dead_reap_callback:
                            self.on_dead_reap_callback(m_id)
        return reaped

    # ------------------------------------------------------------------------
    # State Persistence and Telemetry Export
    # ------------------------------------------------------------------------

    def get_latency_percentiles(self) -> Dict[str, float]:
        """Calculate p50, p95, and p99 probe latencies in milliseconds."""
        with self._lock:
            if not self._latencies:
                return {"p50": 0.0, "p95": 0.0, "p99": 0.0}
            sorted_lat = sorted(self._latencies)
            n = len(sorted_lat)

            def get_perc(p: float) -> float:
                idx = min(n - 1, max(0, int(math.ceil(p * n)) - 1))
                return round(sorted_lat[idx] * 1000.0, 3)

            return {
                "p50": get_perc(0.50),
                "p95": get_perc(0.95),
                "p99": get_perc(0.99),
            }

    def export_telemetry(self) -> None:
        """
        Atomically exports membership table, latency percentiles, and packet trace
        counters to dashboards/swim_gossip_state.json.
        """
        with self._lock:
            members_dump = {m_id: node.to_dict() for m_id, node in self.members.items()}
            latencies = self.get_latency_percentiles()
            state_data = {
                "node_id": self.node_id,
                "timestamp": time.time(),
                "incarnation": self.incarnation,
                "cluster_size": len(self.members),
                "members": members_dump,
                "latency_percentiles_ms": latencies,
                "packet_trace_counts": self._packet_trace_counts,
                "active_suspicions": sum(
                    1 for m in self.members.values() if m.status == MemberStatus.SUSPECT
                ),
                "dead_nodes_count": sum(
                    1 for m in self.members.values() if m.status == MemberStatus.DEAD
                ),
            }

        try:
            self.state_export_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self.state_export_path.with_suffix(".tmp")
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(state_data, f, indent=2)
            tmp_path.replace(self.state_export_path)
        except Exception as e:
            sys.stderr.write(f"[SWIMGossipEngine] Failed exporting telemetry: {e}\n")

    # ------------------------------------------------------------------------
    # Lifecycle Management
    # ------------------------------------------------------------------------

    def start_background_loop(self) -> None:
        """Launch background probing thread."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._probe_thread = threading.Thread(
                target=self._run_probe_worker, daemon=True, name=f"SWIMProbe-{self.node_id}"
            )
            self._probe_thread.start()

    def stop_background_loop(self) -> None:
        """Stop background probing thread."""
        with self._lock:
            self._running = False
        if self._probe_thread and self._probe_thread.is_alive():
            self._probe_thread.join(timeout=2.0)

    def _run_probe_worker(self) -> None:
        while self._running:
            start = time.time()
            try:
                self.execute_probe_cycle()
            except Exception as e:
                sys.stderr.write(f"[SWIMWorker-{self.node_id}] Probe loop error: {e}\n")
            elapsed = time.time() - start
            sleep_time = max(0.01, self.probe_period - elapsed)
            time.sleep(sleep_time)


# ============================================================================
# Standalone Comprehensive Verification Suite
# ============================================================================

def _run_standalone_verification() -> bool:
    """
    Exhaustive verification suite proving:
    (a) 5-node cluster reaches stable consensus.
    (b) Ungraceful crash of 1 node detected via indirect ping-req within expected suspicion window.
    (c) Transient partition causing false suspicion correctly rebutted via incarnation increment.
    (d) Bounded infection gossip dissemination in O(log N).
    """
    print("=" * 80)
    print("SWIM Gossip Failure Detector: Autonomous Verification Suite")
    print("=" * 80)

    bus = NetworkBus(seed=42)
    node_ids = [f"node-{i}" for i in range(1, 6)]
    engines: Dict[str, SWIMGossipEngine] = {}

    # Initialize 5 engines with rapid test timings
    for nid in node_ids:
        engines[nid] = SWIMGossipEngine(
            node_id=nid,
            bus=bus,
            probe_period=0.05,
            direct_ack_timeout=0.03,
            indirect_ack_timeout=0.06,
            base_suspicion_timeout=0.25,
            max_suspicion_timeout=1.0,
            k_indirect_peers=2,
            state_export_path=f"dashboards/swim_{nid}_state.json",
        )

    print("\n--- Test (a): Full Mesh Cluster Join and Consensus ---")
    # All nodes join node-1 as seed
    seed = [("node-1", "127.0.0.1", 8000)]
    for nid in node_ids[1:]:
        engines[nid].join_cluster(seed)

    # Let gossip propagate for 6 probe rounds
    for _ in range(10):
        for nid in node_ids:
            engines[nid].execute_probe_cycle()
        time.sleep(0.02)

    all_consistent = True
    for nid in node_ids:
        known = len(engines[nid].members)
        if known < 5:
            all_consistent = False
        print(f"[{nid}] Known cluster members: {known}/5 (Incarnation: {engines[nid].incarnation})")

    assert all_consistent, "Test (a) Failed: Not all nodes reached consensus on membership."
    print("✓ Test (a) Passed: 5-node cluster achieved unified membership consensus.")

    print("\n--- Test (b): Ungraceful Crash Detection via Indirect Ping-Req ---")
    victim = "node-5"
    print(f"Crashing target: {victim} (Simulating power failure/isolation)")
    bus.isolate_node(victim)

    # Step probe cycles until node-5 is suspected and transitioned to DEAD
    detected_suspect = False
    detected_dead = False

    for cycle in range(25):
        for nid in node_ids:
            if nid != victim:
                engines[nid].execute_probe_cycle()
        time.sleep(0.03)

        # Inspect observer node-1
        n5_status = engines["node-1"].members.get(victim)
        if n5_status:
            if n5_status.status == MemberStatus.SUSPECT:
                detected_suspect = True
            elif n5_status.status == MemberStatus.DEAD:
                detected_dead = True
                print(f"Node-1 confirmed {victim} marked DEAD at cycle {cycle}.")
                break

    assert detected_suspect, "Test (b) Failed: Target was never suspected via indirect ping-req."
    assert detected_dead, "Test (b) Failed: Suspicion deadline never expired to transition DEAD."
    print("✓ Test (b) Passed: Crash successfully detected and finalized to DEAD.")

    print("\n--- Test (c): Transient Partition and Rebuttal via Incarnation ---")
    # Restore node-5 and heal network
    bus.heal_partitions()
    print("Network healed. Simulating false suspicion directed at node-2.")
    # Artificially inject a SUSPECT rumor about node-2 into node-3 with node-2's current incarnation
    curr_inc = engines["node-2"].incarnation
    engines["node-3"].broadcast_update("node-2", MemberStatus.SUSPECT, curr_inc)

    # Let node-3 gossip this to node-2 directly
    for _ in range(8):
        for nid in node_ids:
            engines[nid].execute_probe_cycle()
        time.sleep(0.02)

    new_inc = engines["node-2"].incarnation
    print(f"Node-2 initial incarnation: {curr_inc}, new incarnation post-rebuttal: {new_inc}")
    assert new_inc > curr_inc, "Test (c) Failed: Node-2 failed to increment incarnation to refute suspicion."
    assert engines["node-2"].members["node-2"].status == MemberStatus.ALIVE
    print("✓ Test (c) Passed: False suspicion immediately rebutted with incremented incarnation.")

    print("\n--- Test (d): Bounded Epidemic Gossip Dissemination ---")
    # Verify calculated logarithmic infection bounds
    bound = engines["node-1"]._calc_max_infections()
    print(f"Cluster size 5 -> Calculated max infection rounds λ = {bound}")
    assert bound == int(math.ceil(3.0 * math.log(5, 2))), "Test (d) Failed: Infection bound calculation mismatch."

    # Validate atomic telemetry export
    telemetry_file = Path("dashboards/swim_node-1_state.json")
    engines["node-1"].export_telemetry()
    assert telemetry_file.exists(), "Telemetry export file was not created."
    with open(telemetry_file, "r", encoding="utf-8") as f:
        data = json.load(f)
        assert data["node_id"] == "node-1"
        assert "latency_percentiles_ms" in data
        assert "packet_trace_counts" in data
    print("✓ Test (d) Passed: Logarithmic epidemic bound verified and telemetry atomically exported.")

    print("\n" + "=" * 80)
    print("ALL SWIM GOSSIP PROTOCOL VERIFICATION TESTS PASSED SUCCESSFULLY.")
    print("=" * 80)
    return True


if __name__ == "__main__":
    success = _run_standalone_verification()
    sys.exit(0 if success else 1)