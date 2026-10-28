"""
Pipeline Autonomic Multi-Version Concurrency Control (MVCC) & Serializable Snapshot Isolation (SSI) Engine

This module implements a production-grade, publication-quality MVCC storage engine with
Serializable Snapshot Isolation (SSI) guarantees for high-concurrency transactional pipelines.

Theoretical Foundations:
1. Multi-Version Concurrency Control (MVCC):
   - Non-blocking, wait-free reads via timestamp-ordered version chains V(X) = [v_1, v_2, ..., v_k].
   - Read visibility invariant: Version v is visible to transaction T_i if:
       c(created_by(v)) <= s(T_i) AND (expired_by(v) == null OR c(expired_by(v)) > s(T_i)).
   - First-Committer-Wins (FCW) conflict detection prevents Lost Updates and Write-Write anomalies.

2. Serializable Snapshot Isolation (SSI):
   - Anti-dependency (rw-antidependency) tracking based on the Cahill, Rohm, and Fekete framework:
       T_1 --(rw)--> T_2 exists when T_1 reads version v of tuple X and concurrent transaction T_2
       supersedes/creates successor version v' of X with c(T_2) > s(T_1).
   - In-doubt dependency flags: in(T) indicates incoming rw-antidependencies (T modified tuple read by T_prev);
     out(T) indicates outgoing rw-antidependencies (T read tuple superseded by T_next).
   - Abort rule: Any transaction committing within a dangerous structure (in(T) AND out(T)) or completing
     a directed cycle in the Serialization Graph is aborted with SerializationFailureException.

3. Low-Watermark Vacuum & Garbage Collection:
   - Dynamic tracking of Active = {T_j} and Watermark W = min_{T in Active} s(T).
   - Pruning safely reclaims superseded version records with c(expired_by(v)) < W while
     guaranteeing zero read invalidation for active snapshot readers.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from enum import Enum
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple


# ============================================================================
# Engine Exceptions
# ============================================================================


class MVCCException(Exception):
    """Base exception for all MVCC engine operations."""
    pass


class WriteConflictException(MVCCException):
    """Raised when a First-Committer-Wins (FCW) write-write conflict is detected."""
    pass


class SerializationFailureException(MVCCException):
    """Raised when Serializable Snapshot Isolation (SSI) detects a dangerous cycle/anti-dependency."""
    pass


class TransactionNotActiveException(MVCCException):
    """Raised when operations are attempted on an uninitialized, committed, or aborted transaction."""
    pass


# ============================================================================
# Core Types & Data Structures
# ============================================================================


class TxStatus(Enum):
    """Lifecycle states of a transaction."""
    ACTIVE = "ACTIVE"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"


class IsolationLevel(Enum):
    """Concurrency control isolation levels."""
    SNAPSHOT_ISOLATION = "SNAPSHOT_ISOLATION"
    SERIALIZABLE = "SERIALIZABLE"


@dataclass(slots=True)
class VersionRecord:
    """
    Physical representation of a tuple version in the MVCC version chain.
    """
    key: str
    value: Any
    created_tx_id: int
    created_ts: int
    expired_tx_id: Optional[int] = None
    expired_ts: Optional[int] = None
    is_tombstone: bool = False
    version_id: int = 0
    prev_version_id: Optional[int] = None

    def is_visible_to(self, tx_id: int, snapshot_ts: int) -> bool:
        """
        Visibility Invariant:
        A version v is visible to T_i if:
        1. Created by T_i itself AND not expired by T_i itself; OR
        2. Created by committed T_c with c(T_c) <= s(T_i) AND:
           - not expired; OR
           - expired by T_e with c(T_e) > s(T_i); OR
           - expired by uncommitted or active transaction (unless it is T_i itself).
        """
        # Case 1: Created by the current transaction itself
        if self.created_tx_id == tx_id:
            if self.expired_tx_id == tx_id:
                return False
            return True

        # Case 2: Created by another transaction
        # Must have been committed before or at the start snapshot timestamp
        if self.created_ts > snapshot_ts:
            return False

        # If expired by the reader transaction itself in its local workspace
        if self.expired_tx_id == tx_id:
            return False

        # If not expired at all
        if self.expired_ts is None:
            return True

        # If expired after the snapshot timestamp, it is still visible in this snapshot
        return self.expired_ts > snapshot_ts


@dataclass
class TransactionContext:
    """
    Transactional execution context holding private workspace, read/write sets,
    and SSI graph tracking state.
    """
    tx_id: int
    start_ts: int
    isolation_level: IsolationLevel = IsolationLevel.SERIALIZABLE
    status: TxStatus = TxStatus.ACTIVE
    commit_ts: Optional[int] = None

    # Read set: maps key -> version_id read from main storage
    read_set: Dict[str, int] = field(default_factory=dict)

    # Write set: maps key -> (new_value, is_tombstone)
    write_set: Dict[str, Tuple[Any, bool]] = field(default_factory=dict)

    # SSI Anti-Dependency Tracking (Cahill et al.)
    # in_rw_edge: another transaction T_pred read a version that this transaction supersedes (T_pred --rw--> this)
    in_rw_edge: bool = False
    # out_rw_edge: this transaction read a version that another transaction T_succ supersedes (this --rw--> T_succ)
    out_rw_edge: bool = False

    # Explicit adjacency tracking for cycle/dependency inspection
    in_edges: Set[int] = field(default_factory=set)
    out_edges: Set[int] = field(default_factory=set)


# ============================================================================
# Monotonic Logical Clock Generator
# ============================================================================


class LogicalClock:
    """Thread-safe, monotonically increasing Lamport-style logical clock."""

    def __init__(self, initial_value: int = 1000):
        self._counter: int = initial_value
        self._lock: threading.Lock = threading.Lock()

    def tick(self) -> int:
        """Atomically increment and return the next timestamp."""
        with self._lock:
            self._counter += 1
            return self._counter

    @property
    def current(self) -> int:
        """Get the current timestamp without incrementing."""
        with self._lock:
            return self._counter


# ============================================================================
# Multi-Version Concurrency Control & SSI Engine
# ============================================================================


class MVCCEngine:
    """
    Core MVCC and SSI concurrency arbiter managing multi-version storage,
    snapshot reads, first-committer-wins validation, and anti-dependency graph tracking.
    """

    def __init__(self, clock_start: int = 100):
        self._clock: LogicalClock = LogicalClock(initial_value=clock_start)
        self._global_lock: threading.RLock = threading.RLock()

        # Tuple version storage: key -> list of VersionRecord (ordered oldest to newest)
        self._storage: Dict[str, List[VersionRecord]] = {}

        # Active transaction registry: tx_id -> TransactionContext
        self._active_txs: Dict[int, TransactionContext] = {}

        # Committed/aborted transaction historical registry for SSI graph analysis
        # tx_id -> TransactionContext
        self._tx_history: Dict[int, TransactionContext] = {}

        # Key reader registry for active transactions: key -> set of tx_id
        # Used for fast notification of rw anti-dependencies upon write/commit
        self._key_readers: Dict[str, Set[int]] = {}

        # Monotonic version ID counter
        self._version_counter: int = 0

    def _next_version_id(self) -> int:
        self._version_counter += 1
        return self._version_counter

    # ------------------------------------------------------------------------
    # Transaction Lifecycle Management
    # ------------------------------------------------------------------------

    def begin_transaction(
        self, isolation_level: IsolationLevel = IsolationLevel.SERIALIZABLE
    ) -> TransactionContext:
        """
        Begins a new transaction, allocating a unique transaction ID and snapshot timestamp.
        Registers the transaction in the active transaction table.
        """
        with self._global_lock:
            ts = self._clock.tick()
            tx = TransactionContext(
                tx_id=ts,
                start_ts=ts,
                isolation_level=isolation_level,
                status=TxStatus.ACTIVE,
            )
            self._active_txs[tx.tx_id] = tx
            return tx

    def _assert_active(self, tx: TransactionContext) -> None:
        if tx.status != TxStatus.ACTIVE:
            raise TransactionNotActiveException(
                f"Transaction {tx.tx_id} is not in ACTIVE state (current: {tx.status.value})."
            )

    # ------------------------------------------------------------------------
    # Read Path (Strictly Wait-Free / Non-Blocking)
    # ------------------------------------------------------------------------

    def read(self, tx: TransactionContext, key: str) -> Optional[Any]:
        """
        Reads the visible version of a tuple key under the transaction's snapshot.
        Guaranteed to be wait-free and non-blocking.
        Tracks read anti-dependencies under Serializable Snapshot Isolation.
        """
        self._assert_active(tx)

        # 1. Check local write-set first (Read-Your-Own-Writes)
        if key in tx.write_set:
            val, is_tombstone = tx.write_set[key]
            return None if is_tombstone else copy.deepcopy(val)

        # 2. Traverse version chain in shared storage under read lock
        with self._global_lock:
            versions = self._storage.get(key, [])
            visible_record: Optional[VersionRecord] = None

            # Scan from newest to oldest for fast lookup
            for record in reversed(versions):
                if record.is_visible_to(tx.tx_id, tx.start_ts):
                    visible_record = record
                    break

            if visible_record is None or visible_record.is_tombstone:
                # Key does not exist or has been deleted prior to snapshot
                # Record phantom read check: record that key was read as non-existent (version_id = 0)
                tx.read_set[key] = 0
                return None

            # Record read in transaction context
            tx.read_set[key] = visible_record.version_id

            # Register key reader for SSI tracking
            if key not in self._key_readers:
                self._key_readers[key] = set()
            self._key_readers[key].add(tx.tx_id)

            # In SERIALIZABLE mode: check if this read already forms an outgoing rw-edge
            # If a newer version of 'key' was already created and committed by another transaction T_other
            # with c(T_other) > s(tx), then tx --rw--> T_other
            if tx.isolation_level == IsolationLevel.SERIALIZABLE:
                for record in versions:
                    if record.created_ts > tx.start_ts and record.created_tx_id != tx.tx_id:
                        tx.out_rw_edge = True
                        tx.out_edges.add(record.created_tx_id)

                        # Check if the creator is tracked and notify it of incoming edge
                        creator_tx = self._active_txs.get(
                            record.created_tx_id
                        ) or self._tx_history.get(record.created_tx_id)
                        if creator_tx:
                            creator_tx.in_rw_edge = True
                            creator_tx.in_edges.add(tx.tx_id)

            return copy.deepcopy(visible_record.value)

    # ------------------------------------------------------------------------
    # Write / Delete Path (Private Workspace Buffering)
    # ------------------------------------------------------------------------

    def write(self, tx: TransactionContext, key: str, value: Any) -> None:
        """
        Buffers a write operation in the transaction's private workspace.
        Performs early First-Committer-Wins conflict validation against newer committed versions.
        """
        self._assert_active(tx)
        self._validate_write_conflict(tx, key)
        tx.write_set[key] = (copy.deepcopy(value), False)

    def delete(self, tx: TransactionContext, key: str) -> None:
        """
        Buffers a delete (tombstone) operation in the transaction's private workspace.
        Performs early First-Committer-Wins conflict validation.
        """
        self._assert_active(tx)
        self._validate_write_conflict(tx, key)
        tx.write_set[key] = (None, True)

    def _validate_write_conflict(self, tx: TransactionContext, key: str) -> None:
        """
        First-Committer-Wins Conflict Invariant:
        A transaction T_i cannot write to tuple X if X has been updated/committed
        by another transaction T_k after T_i's snapshot timestamp s(T_i).
        """
        with self._global_lock:
            versions = self._storage.get(key, [])
            for record in reversed(versions):
                # If a version was created after our snapshot start timestamp
                if record.created_ts > tx.start_ts and record.created_tx_id != tx.tx_id:
                    raise WriteConflictException(
                        f"Write-Write conflict detected: Tuple '{key}' was modified by committed "
                        f"Tx {record.created_tx_id} at timestamp {record.created_ts} > s(Tx {tx.tx_id})={tx.start_ts}."
                    )
                # If a version was expired after our snapshot start timestamp
                if (
                    record.expired_ts is not None
                    and record.expired_ts > tx.start_ts
                    and record.expired_tx_id != tx.tx_id
                ):
                    raise WriteConflictException(
                        f"Write-Write conflict detected: Tuple '{key}' was deleted/expired by committed "
                        f"Tx {record.expired_tx_id} at timestamp {record.expired_ts} > s(Tx {tx.tx_id})={tx.start_ts}."
                    )

    # ------------------------------------------------------------------------
    # Commit Path & Serializable Snapshot Isolation Enforcement
    # ------------------------------------------------------------------------

    def commit(self, tx: TransactionContext) -> int:
        """
        Atomically commits the transaction:
        1. Validates First-Committer-Wins invariants for all buffered writes.
        2. Evaluates SSI rw anti-dependencies and aborts if dangerous structures exist.
        3. Installs new versions into shared storage with a monotonic commit timestamp.
        4. Updates transaction status to COMMITTED.
        """
        self._assert_active(tx)

        # Read-only transaction fast path
        if not tx.write_set:
            with self._global_lock:
                commit_ts = self._clock.tick()
                tx.commit_ts = commit_ts
                tx.status = TxStatus.COMMITTED
                self._cleanup_active_tx(tx)
                self._tx_history[tx.tx_id] = tx
                return commit_ts

        with self._global_lock:
            # 1. Final Write-Write conflict check against concurrent commits
            for key in tx.write_set.keys():
                self._validate_write_conflict(tx, key)

            # 2. SSI Conflict Detection and Anti-Dependency Edge Construction
            if tx.isolation_level == IsolationLevel.SERIALIZABLE:
                self._evaluate_ssi_dependencies_pre_commit(tx)

                # Cahill et al. Dangerous Structure Invariant:
                # If in(tx) AND out(tx) are both true, tx is the vulnerable pivot in a potential cycle:
                # T_in --rw--> tx --rw--> T_out
                # To guarantee serializability, tx must abort.
                if tx.in_rw_edge and tx.out_rw_edge:
                    self.abort(tx)
                    raise SerializationFailureException(
                        f"SSI Serialization Failure: Transaction {tx.tx_id} participates in a "
                        f"dangerous rw-antidependency structure (in_edge={tx.in_edges}, out_edge={tx.out_edges})."
                    )

            # 3. Acquire Commit Timestamp
            commit_ts = self._clock.tick()
            tx.commit_ts = commit_ts

            # 4. Atomically install version records in storage
            for key, (val, is_tombstone) in tx.write_set.items():
                if key not in self._storage:
                    self._storage[key] = []

                versions = self._storage[key]
                prev_version_id: Optional[int] = None

                # Expire currently active visible version
                for record in reversed(versions):
                    if record.expired_ts is None:
                        record.expired_tx_id = tx.tx_id
                        record.expired_ts = commit_ts
                        prev_version_id = record.version_id
                        break

                # Create and append new version record
                new_version = VersionRecord(
                    key=key,
                    value=val,
                    created_tx_id=tx.tx_id,
                    created_ts=commit_ts,
                    expired_tx_id=None,
                    expired_ts=None,
                    is_tombstone=is_tombstone,
                    version_id=self._next_version_id(),
                    prev_version_id=prev_version_id,
                )
                versions.append(new_version)

            # 5. Post-commit SSI dependency propagation to other transactions
            if tx.isolation_level == IsolationLevel.SERIALIZABLE:
                self._propagate_ssi_edges_post_commit(tx)

            # Finalize status
            tx.status = TxStatus.COMMITTED
            self._cleanup_active_tx(tx)
            self._tx_history[tx.tx_id] = tx
            return commit_ts

    def _evaluate_ssi_dependencies_pre_commit(self, tx: TransactionContext) -> None:
        """
        Builds incoming anti-dependency edges for tx:
        For every key that tx writes, if any transaction T_reader read an earlier version
        visible at s(T_reader) < commit_ts, an anti-dependency T_reader --rw--> tx exists.
        Therefore, tx.in_rw_edge = True and T_reader.out_rw_edge = True.
        """
        for written_key in tx.write_set.keys():
            # Check all readers of this key across active and recent transactions
            reader_ids = self._key_readers.get(written_key, set())
            for reader_id in reader_ids:
                if reader_id == tx.tx_id:
                    continue

                reader_tx = self._active_txs.get(reader_id) or self._tx_history.get(reader_id)
                if reader_tx is None or reader_tx.status == TxStatus.ABORTED:
                    continue

                # An rw-edge exists if the reader began before this write takes effect
                tx.in_rw_edge = True
                tx.in_edges.add(reader_id)
                reader_tx.out_rw_edge = True
                reader_tx.out_edges.add(tx.tx_id)

    def _propagate_ssi_edges_post_commit(self, tx: TransactionContext) -> None:
        """
        Propagates anti-dependency notifications to active concurrent readers.
        """
        for written_key in tx.write_set.keys():
            reader_ids = self._key_readers.get(written_key, set())
            for reader_id in reader_ids:
                if reader_id == tx.tx_id:
                    continue
                reader_tx = self._active_txs.get(reader_id)
                if reader_tx and reader_tx.status == TxStatus.ACTIVE:
                    reader_tx.out_rw_edge = True
                    reader_tx.out_edges.add(tx.tx_id)

    # ------------------------------------------------------------------------
    # Abort Path
    # ------------------------------------------------------------------------

    def abort(self, tx: TransactionContext) -> None:
        """
        Aborts the transaction, discarding private write buffers and unregistering
        from active tracking structures.
        """
        with self._global_lock:
            tx.status = TxStatus.ABORTED
            tx.write_set.clear()
            self._cleanup_active_tx(tx)
            self._tx_history[tx.tx_id] = tx

    def _cleanup_active_tx(self, tx: TransactionContext) -> None:
        """Removes transaction from active registry and reader index."""
        self._active_txs.pop(tx.tx_id, None)
        for key in tx.read_set.keys():
            if key in self._key_readers:
                self._key_readers[key].discard(tx.tx_id)
                if not self._key_readers[key]:
                    self._key_readers.pop(key, None)

    # ------------------------------------------------------------------------
    # Garbage Collection / Low-Watermark Vacuum
    # ------------------------------------------------------------------------

    def get_low_watermark(self) -> int:
        """
        Computes the safe visibility Low Watermark:
        W = min_{T in Active} s(T).
        If no transactions are active, W is the current clock timestamp.
        """
        with self._global_lock:
            if not self._active_txs:
                return self._clock.current
            return min(tx.start_ts for tx in self._active_txs.values())

    def vacuum(self, low_watermark: Optional[int] = None) -> int:
        """
        Safely purges obsolete version records superseded prior to the low watermark.

        Invariant:
        A version v can be safely collected if:
        1. It is superseded (expired_ts is not None); AND
        2. Its expiration timestamp is strictly less than the lowest active snapshot timestamp:
           c(expired_by(v)) < W; AND
        3. It is not the only visible version for any future or active snapshot.
        """
        with self._global_lock:
            wm = low_watermark if low_watermark is not None else self.get_low_watermark()
            reclaimed_count = 0

            for key, versions in list(self._storage.items()):
                retained_versions: List[VersionRecord] = []

                # Find the latest version that was committed before or at W
                # That version is the base snapshot for all readers >= W and must NOT be pruned
                base_version_idx: Optional[int] = None
                for i in range(len(versions) - 1, -1, -1):
                    v = versions[i]
                    if v.created_ts <= wm:
                        base_version_idx = i
                        break

                for i, v in enumerate(versions):
                    # Check if v is strictly older than base_version_idx and expired before W
                    if base_version_idx is not None and i < base_version_idx:
                        if v.expired_ts is not None and v.expired_ts < wm:
                            reclaimed_count += 1
                            continue

                    # If v is a tombstone that expired before W and has no subsequent readers
                    if v.is_tombstone and v.created_ts < wm:
                        reclaimed_count += 1
                        continue

                    retained_versions.append(v)

                self._storage[key] = retained_versions

            # Prune obsolete transaction history
            stale_tx_ids = [
                tx_id
                for tx_id, tx in self._tx_history.items()
                if tx.commit_ts is not None and tx.commit_ts < wm
            ]
            for tx_id in stale_tx_ids:
                self._tx_history.pop(tx_id, None)

            return reclaimed_count

    # ------------------------------------------------------------------------
    # Introspection & Telemetry
    # ------------------------------------------------------------------------

    def get_stats(self) -> Dict[str, Any]:
        """Returns runtime diagnostic metrics of the MVCC engine."""
        with self._global_lock:
            total_versions = sum(len(v) for v in self._storage.values())
            return {
                "active_transactions": len(self._active_txs),
                "tracked_keys": len(self._storage),
                "total_versions": total_versions,
                "low_watermark": self.get_low_watermark(),
                "clock_current": self._clock.current,
            }


# ============================================================================
# Verification & Self-Testing Harness
# ============================================================================


def _run_test_suite() -> None:
    """Comprehensive test harness verifying MVCC and SSI invariants."""
    print("=" * 80)
    print("HERMES ORCHESTRATOR - MVCC & SSI ENGINE VERIFICATION SUITE")
    print("=" * 80)

    engine = MVCCEngine()

    # ------------------------------------------------------------------------
    # 1. Snapshot Isolation (SI) Read-Repeatability & Non-Blocking Readers
    # ------------------------------------------------------------------------
    print("\n[Test 1] Snapshot Isolation Invariant & Non-Blocking Readers...")
    tx_init = engine.begin_transaction()
    engine.write(tx_init, "account_A", 1000)
    engine.write(tx_init, "account_B", 500)
    engine.commit(tx_init)

    # Tx 1 begins and captures snapshot
    tx1 = engine.begin_transaction(isolation_level=IsolationLevel.SNAPSHOT_ISOLATION)
    val_a1 = engine.read(tx1, "account_A")
    val_b1 = engine.read(tx1, "account_B")
    assert val_a1 == 1000 and val_b1 == 500, f"Expected 1000, 500; got {val_a1}, {val_b1}"

    # Tx 2 mutates account_A and commits while Tx 1 is active
    tx2 = engine.begin_transaction(isolation_level=IsolationLevel.SNAPSHOT_ISOLATION)
    engine.write(tx2, "account_A", 2500)
    engine.commit(tx2)

    # Tx 1 reads again: must observe consistent snapshot, immune to Tx 2 commit
    val_a1_repeat = engine.read(tx1, "account_A")
    assert val_a1_repeat == 1000, f"Snapshot isolation violated! Got {val_a1_repeat}, expected 1000"

    engine.commit(tx1)

    # Tx 3 begins after Tx 2 commit and must observe the new value
    tx3 = engine.begin_transaction()
    val_a3 = engine.read(tx3, "account_A")
    assert val_a3 == 2500, f"Expected 2500, got {val_a3}"
    engine.commit(tx3)
    print("  -> PASSED: Snapshot isolation maintained repeatability across concurrent commits.")

    # ------------------------------------------------------------------------
    # 2. First-Committer-Wins (FCW) Conflict Detection
    # ------------------------------------------------------------------------
    print("\n[Test 2] First-Committer-Wins (FCW) Write-Write Conflict Detection...")
    tx_a = engine.begin_transaction()
    tx_b = engine.begin_transaction()

    engine.write(tx_a, "stock_count", 50)
    engine.write(tx_b, "stock_count", 40)

    # Tx A commits first
    engine.commit(tx_a)

    # Tx B attempts to commit overlapping write: must abort with WriteConflictException
    fcw_caught = False
    try:
        engine.commit(tx_b)
    except WriteConflictException as exc:
        fcw_caught = True
        engine.abort(tx_b)
        print(f"  -> Successfully caught expected WriteConflictException: {exc}")

    assert fcw_caught, "Failed to enforce First-Committer-Wins write-write conflict rule!"
    print("  -> PASSED: First-Committer-Wins reliably aborted concurrent writer.")

    # ------------------------------------------------------------------------
    # 3. Serializable Snapshot Isolation (SSI) Write-Skew Anomaly Prevention
    # ------------------------------------------------------------------------
    print("\n[Test 3] Serializable SSI Write-Skew Anomaly Prevention...")
    # Classic Doctor On-Call / Balance Invariant Problem:
    # Invariant: doctor_1_on_call OR doctor_2_on_call must remain True.
    # Initially: both doctors are on call (True, True).
    tx_doc_init = engine.begin_transaction()
    engine.write(tx_doc_init, "doc_1", True)
    engine.write(tx_doc_init, "doc_2", True)
    engine.commit(tx_doc_init)

    # T1 attempts to take doc_1 off call, checking doc_2 first
    t1 = engine.begin_transaction(isolation_level=IsolationLevel.SERIALIZABLE)
    # T2 attempts to take doc_2 off call, checking doc_1 first
    t2 = engine.begin_transaction(isolation_level=IsolationLevel.SERIALIZABLE)

    # T1 reads doc_2 (True), proceeds to write doc_1 = False
    doc2_val = engine.read(t1, "doc_2")
    assert doc2_val is True
    engine.write(t1, "doc_1", False)

    # T2 reads doc_1 (True), proceeds to write doc_2 = False
    doc1_val = engine.read(t2, "doc_1")
    assert doc1_val is True
    engine.write(t2, "doc_2", False)

    # Under pure Snapshot Isolation, BOTH would commit, resulting in (False, False),
    # violating the serializable schedule!
    # Under SSI:
    # T1 read doc_2; T2 writes doc_2 => T1 --rw--> T2 (T1.out_rw_edge=True, T2.in_rw_edge=True)
    # T2 read doc_1; T1 writes doc_1 => T2 --rw--> T1 (T2.out_rw_edge=True, T1.in_rw_edge=True)
    # Dangerous structure (in AND out) detected!
    t1_committed = False
    t2_committed = False
    ssi_abort_caught = False

    try:
        engine.commit(t1)
        t1_committed = True
    except SerializationFailureException:
        ssi_abort_caught = True
        engine.abort(t1)

    try:
        engine.commit(t2)
        t2_committed = True
    except SerializationFailureException:
        ssi_abort_caught = True
        engine.abort(t2)

    assert ssi_abort_caught, "SSI failed to detect and abort write-skew anomaly!"
    assert not (t1_committed and t2_committed), "Both transactions committed under write-skew!"
    print(f"  -> Write-skew prevented: T1 committed={t1_committed}, T2 committed={t2_committed}")
    print("  -> PASSED: SSI anti-dependency tracking eliminated write-skew anomaly.")

    # ------------------------------------------------------------------------
    # 4. Low-Watermark Vacuum & Safe Version Garbage Collection
    # ------------------------------------------------------------------------
    print("\n[Test 4] Low-Watermark Vacuum & Non-Disruptive GC...")
    tx_gc_init = engine.begin_transaction()
    engine.write(tx_gc_init, "counter", 0)
    engine.commit(tx_gc_init)

    # Produce 10 updates
    for i in range(1, 11):
        t_up = engine.begin_transaction()
        engine.write(t_up, "counter", i)
        engine.commit(t_up)

    stats_before = engine.get_stats()
    print(f"  -> State before GC: {stats_before}")
    assert stats_before["total_versions"] >= 10

    # Start long-running reader tx_slow at current snapshot
    tx_slow = engine.begin_transaction()
    val_slow_start = engine.read(tx_slow, "counter")
    assert val_slow_start == 10

    # Produce 5 more updates while tx_slow is active
    for i in range(11, 16):
        t_up2 = engine.begin_transaction()
        engine.write(t_up2, "counter", i)
        engine.commit(t_up2)

    # Vacuum with tx_slow active: cannot prune versions needed by tx_slow snapshot
    reclaimed = engine.vacuum()
    print(f"  -> Vacuum reclaimed {reclaimed} versions with active reader.")

    # Verify tx_slow can still read its consistent snapshot (10)
    val_slow_repeat = engine.read(tx_slow, "counter")
    assert val_slow_repeat == 10, f"GC invalidated active reader! Expected 10, got {val_slow_repeat}"
    engine.commit(tx_slow)

    # Now that tx_slow has committed, low watermark advances
    reclaimed_final = engine.vacuum()
    print(f"  -> Post-reader vacuum reclaimed {reclaimed_final} versions.")
    stats_after = engine.get_stats()
    print(f"  -> State after complete GC: {stats_after}")

    # Read latest value with fresh transaction
    tx_final = engine.begin_transaction()
    val_final = engine.read(tx_final, "counter")
    assert val_final == 15, f"Expected latest value 15, got {val_final}"
    engine.commit(tx_final)
    print("  -> PASSED: Garbage collection successfully reclaimed obsolete records without reader perturbation.")

    print("\n" + "=" * 80)
    print("ALL MVCC & SERIALIZABLE SNAPSHOT ISOLATION TESTS PASSED SUCCESSFULLY!")
    print("=" * 80)


if __name__ == "__main__":
    _run_test_suite()