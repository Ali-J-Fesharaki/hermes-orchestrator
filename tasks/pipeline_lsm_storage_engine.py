#!/usr/bin/env python3
"""
Pipeline Autonomic Log-Structured Merge (LSM) Storage Engine & SSTable Compaction Arbiter.

Hermes Orchestrator High-Performance Autonomous Storage Subsystem.
Zero external dependencies; pure Python standard library implementation.

Architecture Components:
1. Write-Ahead Log (WAL): Framed binary append-only records with CRC32 integrity verification.
2. SkipList MemTable: Concurrent-safe probabilistic skip list (H=16, p=0.5) with tombstone support.
3. SSTable Storage Format:
   - Data Block: Lexicographically sorted key-value records.
   - Index Block: Sparse key-offset pointers for fast seek targeting.
   - Bloom Filter Block: Optimal bitarray with multi-hash Murmur/FNV derivation (p <= 0.01).
   - Fixed 48-Byte Footer: Metadata offsets, lengths, entry counts, and magic identifier.
4. Point Lookup & Range Scan Algebra: Multi-level fallback with in-memory Bloom pruning and heapq k-way merge.
5. Leveled Compaction Arbiter: Geometric capacity scaling (C_i = C_0 * 10^i) with streaming merge-sort.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass, field
import hashlib
import heapq
import json
import math
import os
from pathlib import Path
import random
import struct
import sys
import threading
import time
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple
import zlib

# ==============================================================================
# Constants, Magic Markers & Struct Signatures
# ==============================================================================

SSTABLE_MAGIC: bytes = b"HLSM_V01"  # 8 bytes magic identifier
SSTABLE_FOOTER_FORMAT: str = "!8sQQQQQ"  # Magic(8), IndexOff(8), IndexLen(8), BloomOff(8), BloomLen(8), NumEntries(8)
SSTABLE_FOOTER_SIZE: int = struct.calcsize(SSTABLE_FOOTER_FORMAT)  # Exact 48 bytes

OP_PUT: int = 0x01
OP_DEL: int = 0x02

# Sentinel object representing deleted keys (tombstone)
TOMBSTONE = object()

DEFAULT_MEMTABLE_THRESHOLD_BYTES: int = 2 * 1024 * 1024  # 2 MB default threshold
DEFAULT_SPARSE_INDEX_INTERVAL: int = 16  # Store sparse index entry every 16 records


# ==============================================================================
# Bloom Filter Implementation
# ==============================================================================

class BloomFilter:
    """
    Space-efficient probabilistic data structure for set membership.
    Calculates optimal bit-vector size (m) and hash function count (k)
    based on expected items (n) and target false positive rate (p = 0.01).
    """

    def __init__(self, expected_items: int = 1000, fp_rate: float = 0.01) -> None:
        self.fp_rate = max(1e-6, min(fp_rate, 0.5))
        self.expected_items = max(1, expected_items)
        
        # m = -(n * ln(p)) / (ln(2)^2)
        m_bits = - (self.expected_items * math.log(self.fp_rate)) / (math.log(2.0) ** 2)
        self.size_bits = max(64, int(math.ceil(m_bits)))
        # Ensure byte alignment
        self.size_bytes = (self.size_bits + 7) // 8
        self.size_bits = self.size_bytes * 8
        
        # k = (m / n) * ln(2)
        k_hashes = (self.size_bits / self.expected_items) * math.log(2.0)
        self.num_hashes = max(1, int(round(k_hashes)))
        
        self.bit_array = bytearray(self.size_bytes)

    def _hashes(self, key_bytes: bytes) -> Iterator[int]:
        """
        Derive k independent hash positions using Kirsch-Mitzenmacher optimization:
        g_i(x) = h1(x) + i * h2(x) mod m
        """
        digest = hashlib.sha256(key_bytes).digest()
        h1, h2 = struct.unpack("!QQ", digest[:16])
        for i in range(self.num_hashes):
            yield (h1 + i * h2) % self.size_bits

    def add(self, key: str) -> None:
        key_bytes = key.encode("utf-8")
        for bit_idx in self._hashes(key_bytes):
            byte_idx = bit_idx // 8
            bit_mask = 1 << (bit_idx % 8)
            self.bit_array[byte_idx] |= bit_mask

    def contains(self, key: str) -> bool:
        key_bytes = key.encode("utf-8")
        for bit_idx in self._hashes(key_bytes):
            byte_idx = bit_idx // 8
            bit_mask = 1 << (bit_idx % 8)
            if not (self.bit_array[byte_idx] & bit_mask):
                return False
        return True

    def serialize(self) -> bytes:
        """
        Serializes filter header and payload:
        [SizeBits: 4 bytes][NumHashes: 4 bytes][BitArray: size_bytes]
        """
        header = struct.pack("!II", self.size_bits, self.num_hashes)
        return header + bytes(self.bit_array)

    @classmethod
    def deserialize(cls, data: bytes) -> BloomFilter:
        size_bits, num_hashes = struct.unpack("!II", data[:8])
        raw_bits = data[8:]
        instance = cls(expected_items=10, fp_rate=0.01)
        instance.size_bits = size_bits
        instance.num_hashes = num_hashes
        instance.size_bytes = len(raw_bits)
        instance.bit_array = bytearray(raw_bits)
        return instance


# ==============================================================================
# SkipList MemTable Implementation
# ==============================================================================

class SkipListNode:
    __slots__ = ("key", "val", "forward")

    def __init__(self, key: Optional[str], val: Any, height: int):
        self.key: Optional[str] = key
        self.val: Any = val
        self.forward: List[Optional[SkipListNode]] = [None] * height


class SkipListMemTable:
    """
    Probabilistic SkipList MemTable with maximum height H = 16 and p = 0.5.
    Thread-safe mutations and point / range queries with dynamic memory tracking.
    """

    MAX_HEIGHT: int = 16
    BRANCH_P: float = 0.5

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.head = SkipListNode(None, None, self.MAX_HEIGHT)
        self.height: int = 1
        self.entry_count: int = 0
        self.estimated_memory_bytes: int = 128  # Base overhead

    def _random_height(self) -> int:
        h = 1
        while h < self.MAX_HEIGHT and random.random() < self.BRANCH_P:
            h += 1
        return h

    def put(self, key: str, val: Any) -> None:
        with self.lock:
            update = [None] * self.MAX_HEIGHT
            curr = self.head
            
            for i in reversed(range(self.height)):
                while curr.forward[i] is not None and curr.forward[i].key < key:
                    curr = curr.forward[i]
                update[i] = curr
                
            curr = curr.forward[0]
            if curr is not None and curr.key == key:
                # Key update
                old_val_size = len(str(curr.val).encode("utf-8")) if curr.val is not TOMBSTONE else 8
                new_val_size = len(str(val).encode("utf-8")) if val is not TOMBSTONE else 8
                self.estimated_memory_bytes += (new_val_size - old_val_size)
                curr.val = val
                return

            # Insert new key
            node_height = self._random_height()
            if node_height > self.height:
                for i in range(self.height, node_height):
                    update[i] = self.head
                self.height = node_height

            new_node = SkipListNode(key, val, node_height)
            for i in range(node_height):
                new_node.forward[i] = update[i].forward[i]
                update[i].forward[i] = new_node

            self.entry_count += 1
            # Memory accounting: key length + val representation + node pointers
            val_size = len(str(val).encode("utf-8")) if val is not TOMBSTONE else 8
            self.estimated_memory_bytes += len(key.encode("utf-8")) + val_size + (node_height * 8) + 64

    def get(self, key: str) -> Tuple[bool, Any]:
        """
        Returns (found, value). If found and value is TOMBSTONE, indicates deleted key.
        """
        with self.lock:
            curr = self.head
            for i in reversed(range(self.height)):
                while curr.forward[i] is not None and curr.forward[i].key < key:
                    curr = curr.forward[i]
            curr = curr.forward[0]
            if curr is not None and curr.key == key:
                return True, curr.val
            return False, None

    def delete(self, key: str) -> None:
        """Sets a tombstone record for the key in MemTable."""
        self.put(key, TOMBSTONE)

    def scan(self, start_key: str, end_key: str) -> List[Tuple[str, Any]]:
        """
        Returns ordered key-value pairs where start_key <= key <= end_key.
        Includes tombstones so caller can handle conflict resolution.
        """
        with self.lock:
            results: List[Tuple[str, Any]] = []
            curr = self.head
            for i in reversed(range(self.height)):
                while curr.forward[i] is not None and curr.forward[i].key < start_key:
                    curr = curr.forward[i]
            curr = curr.forward[0]
            while curr is not None and curr.key <= end_key:
                results.append((curr.key, curr.val))
                curr = curr.forward[0]
            return results

    def iter_all(self) -> Iterator[Tuple[str, Any]]:
        """Iterates sequentially through all elements from head to tail."""
        with self.lock:
            curr = self.head.forward[0]
            while curr is not None:
                yield curr.key, curr.val
                curr = curr.forward[0]

    def memory_footprint(self) -> int:
        with self.lock:
            return self.estimated_memory_bytes

    def __len__(self) -> int:
        with self.lock:
            return self.entry_count


# ==============================================================================
# Write-Ahead Log (WAL)
# ==============================================================================

class WriteAheadLog:
    """
    Append-only framed binary Write-Ahead Log.
    Record Framing Layout:
    [CRC32: 4 bytes][RecordLength: 4 bytes][OpType: 1 byte][KeyLength: 2 bytes][Key: N bytes][ValLength: 4 bytes][Val: M bytes]
    Total Payload = OpType (1) + KeyLen (2) + Key (N) + ValLen (4) + Val (M)
    RecordLength = len(Payload)
    CRC32 = zlib.crc32(Payload)
    """

    def __init__(self, filepath: Path, sync_mode: str = "ALWAYS") -> None:
        self.filepath = filepath
        self.sync_mode = sync_mode.upper()  # "ALWAYS" or "PERIODIC"
        self.lock = threading.Lock()
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.filepath, "a+b")

    def append(self, op_type: int, key: str, val: Any) -> None:
        key_bytes = key.encode("utf-8")
        if val is TOMBSTONE:
            val_bytes = b""
        else:
            val_bytes = json.dumps(val, ensure_ascii=False).encode("utf-8")

        payload_header = struct.pack("!BH", op_type, len(key_bytes))
        val_len_bytes = struct.pack("!I", len(val_bytes))
        payload = payload_header + key_bytes + val_len_bytes + val_bytes

        record_len = len(payload)
        crc = zlib.crc32(payload) & 0xFFFFFFFF
        frame_header = struct.pack("!II", crc, record_len)
        full_record = frame_header + payload

        with self.lock:
            self._file.write(full_record)
            if self.sync_mode == "ALWAYS":
                self._file.flush()
                os.fsync(self._file.fileno())

    def flush(self) -> None:
        with self.lock:
            self._file.flush()
            os.fsync(self._file.fileno())

    def close(self) -> None:
        with self.lock:
            if not self._file.closed:
                self._file.flush()
                os.fsync(self._file.fileno())
                self._file.close()

    @classmethod
    def replay_log(cls, filepath: Path) -> List[Tuple[int, str, Any]]:
        """
        Parses and verifies entire WAL log file. Reconstructs committed mutations.
        Returns list of (op_type, key, value).
        """
        mutations: List[Tuple[int, str, Any]] = []
        if not filepath.exists():
            return mutations

        with open(filepath, "rb") as f:
            while True:
                header = f.read(8)
                if not header or len(header) < 8:
                    break
                expected_crc, record_len = struct.unpack("!II", header)
                payload = f.read(record_len)
                if len(payload) < record_len:
                    # Incomplete record at end of log; stop replay
                    break
                
                actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
                if actual_crc != expected_crc:
                    raise ValueError(f"WAL corruption detected in {filepath} at offset {f.tell() - record_len}")

                op_type, key_len = struct.unpack("!BH", payload[:3])
                offset = 3
                key = payload[offset:offset + key_len].decode("utf-8")
                offset += key_len
                val_len = struct.unpack("!I", payload[offset:offset + 4])[0]
                offset += 4
                raw_val = payload[offset:offset + val_len]
                
                if op_type == OP_DEL:
                    val = TOMBSTONE
                else:
                    val = json.loads(raw_val.decode("utf-8")) if raw_val else None

                mutations.append((op_type, key, val))

        return mutations


# ==============================================================================
# SSTable Writer & Reader
# ==============================================================================

@dataclass
class IndexEntry:
    key: str
    offset: int
    length: int


class SSTableWriter:
    """
    Serializes a sorted sequence of key-value pairs to an immutable disk SSTable.
    Layout:
    1. Data Block (sequential records: [KeyLen: 2B][ValLen: 4B][Key][Val])
    2. Index Block (sparse index entries: [KeyLen: 2B][Key][Offset: 8B][Length: 4B])
    3. Bloom Filter Block (serialized BloomFilter)
    4. Fixed 48-byte Footer
    """

    def __init__(self, filepath: Path, sparse_interval: int = DEFAULT_SPARSE_INDEX_INTERVAL) -> None:
        self.filepath = filepath
        self.sparse_interval = sparse_interval
        self.filepath.parent.mkdir(parents=True, exist_ok=True)

    def write_entries(self, entries: List[Tuple[str, Any]]) -> Tuple[int, int]:
        """
        Writes sorted entries to SSTable file.
        Returns (num_entries, total_file_size).
        """
        bloom = BloomFilter(expected_items=max(len(entries), 10), fp_rate=0.01)
        for k, _ in entries:
            bloom.add(k)

        index_entries: List[IndexEntry] = []
        num_entries = len(entries)

        with open(self.filepath, "wb") as f:
            # 1. Write Data Block
            data_start_offset = f.tell()
            for idx, (k, v) in enumerate(entries):
                record_start = f.tell()
                k_bytes = k.encode("utf-8")
                
                if v is TOMBSTONE:
                    v_bytes = b"__HERMES_TOMBSTONE__"
                else:
                    v_bytes = json.dumps(v, ensure_ascii=False).encode("utf-8")

                record_data = struct.pack("!HI", len(k_bytes), len(v_bytes)) + k_bytes + v_bytes
                f.write(record_data)
                record_len = len(record_data)

                # Record sparse index entry
                if idx % self.sparse_interval == 0:
                    index_entries.append(IndexEntry(key=k, offset=record_start, length=record_len))

            # 2. Write Index Block
            index_block_offset = f.tell()
            # Index format: [NumEntries: 4B] + ( [KeyLen: 2B][Key][Offset: 8B][Length: 4B] * N )
            index_bytes = bytearray(struct.pack("!I", len(index_entries)))
            for ie in index_entries:
                ie_k_bytes = ie.key.encode("utf-8")
                index_bytes.extend(struct.pack("!H", len(ie_k_bytes)))
                index_bytes.extend(ie_k_bytes)
                index_bytes.extend(struct.pack("!QI", ie.offset, ie.length))
            
            f.write(index_bytes)
            index_block_len = f.tell() - index_block_offset

            # 3. Write Bloom Filter Block
            bloom_block_offset = f.tell()
            serialized_bloom = bloom.serialize()
            f.write(serialized_bloom)
            bloom_block_len = f.tell() - bloom_block_offset

            # 4. Write Fixed 48-byte Footer
            footer_data = struct.pack(
                SSTABLE_FOOTER_FORMAT,
                SSTABLE_MAGIC,
                index_block_offset,
                index_block_len,
                bloom_block_offset,
                bloom_block_len,
                num_entries
            )
            assert len(footer_data) == SSTABLE_FOOTER_SIZE, f"Footer size mismatch: {len(footer_data)}"
            f.write(footer_data)
            f.flush()
            os.fsync(f.fileno())

            total_size = f.tell()

        return num_entries, total_size


class SSTableReader:
    """
    Reader for immutable SSTable disk files.
    Caches BloomFilter and sparse Index block in memory for fast negative-pruning
    and binary search boundary seeks (exact point lookup in <= 1 disk seek).
    """

    def __init__(self, filepath: Path) -> None:
        self.filepath = filepath
        self.file_size = filepath.stat().st_size
        if self.file_size < SSTABLE_FOOTER_SIZE:
            raise ValueError(f"Corrupt SSTable file: {filepath} too small")

        self.index_keys: List[str] = []
        self.index_records: List[IndexEntry] = []
        self.bloom: Optional[BloomFilter] = None
        self.num_entries: int = 0
        self.min_key: str = ""
        self.max_key: str = ""

        self._load_metadata()

    def _load_metadata(self) -> None:
        with open(self.filepath, "rb") as f:
            # Read Footer
            f.seek(self.file_size - SSTABLE_FOOTER_SIZE)
            footer_bytes = f.read(SSTABLE_FOOTER_SIZE)
            magic, idx_off, idx_len, bloom_off, bloom_len, num_entries = struct.unpack(
                SSTABLE_FOOTER_FORMAT, footer_bytes
            )
            if magic != SSTABLE_MAGIC:
                raise ValueError(f"Invalid SSTable magic bytes in {self.filepath}")

            self.num_entries = num_entries

            # Read Bloom Filter
            f.seek(bloom_off)
            bloom_bytes = f.read(bloom_len)
            self.bloom = BloomFilter.deserialize(bloom_bytes)

            # Read Sparse Index
            f.seek(idx_off)
            idx_bytes = f.read(idx_len)
            num_idx = struct.unpack("!I", idx_bytes[:4])[0]
            offset = 4
            for _ in range(num_idx):
                k_len = struct.unpack("!H", idx_bytes[offset:offset + 2])[0]
                offset += 2
                k = idx_bytes[offset:offset + k_len].decode("utf-8")
                offset += k_len
                rec_off, rec_len = struct.unpack("!QI", idx_bytes[offset:offset + 12])
                offset += 12
                ie = IndexEntry(key=k, offset=rec_off, length=rec_len)
                self.index_keys.append(k)
                self.index_records.append(ie)

            if self.index_keys:
                self.min_key = self.index_keys[0]

            # Read last record's key to set max_key
            if num_entries > 0:
                f.seek(0)
                # First record gives exact min_key
                k_len, v_len = struct.unpack("!HI", f.read(6))
                self.min_key = f.read(k_len).decode("utf-8")

                # Last sparse index points close to the end
                last_ie = self.index_records[-1]
                f.seek(last_ie.offset)
                while f.tell() < idx_off:
                    header = f.read(6)
                    if not header or len(header) < 6:
                        break
                    k_len, v_len = struct.unpack("!HI", header)
                    current_key = f.read(k_len).decode("utf-8")
                    f.seek(v_len, os.SEEK_CUR)
                    self.max_key = current_key

    def contains_bloom(self, key: str) -> bool:
        if self.bloom is None:
            return True
        return self.bloom.contains(key)

    def get(self, target_key: str) -> Tuple[bool, Any]:
        """
        Attempts point lookup. Returns (found, value).
        Uses Bloom Filter rejection first, then sparse index binary search
        to pinpoint the exact file block.
        """
        # Key range bounds test
        if self.min_key and target_key < self.min_key:
            return False, None
        if self.max_key and target_key > self.max_key:
            return False, None

        # Bloom filter rejection test
        if not self.contains_bloom(target_key):
            return False, None

        # Binary search sparse index
        # bisect_right finds insertion point after target_key
        idx = bisect.bisect_right(self.index_keys, target_key) - 1
        start_offset = 0
        if idx >= 0:
            start_offset = self.index_records[idx].offset

        with open(self.filepath, "rb") as f:
            f.seek(start_offset)
            # Scan forward through the segment until target_key is found or exceeded
            while True:
                header = f.read(6)
                if not header or len(header) < 6:
                    break
                k_len, v_len = struct.unpack("!HI", header)
                key = f.read(k_len).decode("utf-8")
                val_bytes = f.read(v_len)

                if key == target_key:
                    if val_bytes == b"__HERMES_TOMBSTONE__":
                        return True, TOMBSTONE
                    val = json.loads(val_bytes.decode("utf-8")) if val_bytes else None
                    return True, val
                elif key > target_key:
                    break

        return False, None

    def scan_entries(self) -> Iterator[Tuple[str, Any]]:
        """Streaming generator yielding all sorted entries in this SSTable."""
        with open(self.filepath, "rb") as f:
            f.seek(self.file_size - SSTABLE_FOOTER_SIZE)
            footer_bytes = f.read(SSTABLE_FOOTER_SIZE)
            _, idx_off, _, _, _, _ = struct.unpack(SSTABLE_FOOTER_FORMAT, footer_bytes)

            f.seek(0)
            while f.tell() < idx_off:
                header = f.read(6)
                if not header or len(header) < 6:
                    break
                k_len, v_len = struct.unpack("!HI", header)
                key = f.read(k_len).decode("utf-8")
                val_bytes = f.read(v_len)
                if val_bytes == b"__HERMES_TOMBSTONE__":
                    val = TOMBSTONE
                else:
                    val = json.loads(val_bytes.decode("utf-8")) if val_bytes else None
                yield key, val


# ==============================================================================
# Leveled Compaction Arbiter
# ==============================================================================

class CompactionArbiter:
    """
    Manages SSTable levels and drives leveled compaction.
    Level L_i capacity C_i = C_0 * (10 ** i).
    L0: uncompacted flushed SSTables (overlapping key ranges allowed).
    L1+: partitioned SSTables with non-overlapping key ranges.
    """

    def __init__(self, base_dir: Path, c0_bytes: int = 4 * 1024 * 1024, max_levels: int = 5) -> None:
        self.base_dir = base_dir
        self.c0_bytes = c0_bytes
        self.max_levels = max_levels
        self.levels: Dict[int, List[SSTableReader]] = {i: [] for i in range(max_levels)}
        self.table_counter: int = 0
        self.lock = threading.RLock()

    def get_level_capacity(self, level: int) -> int:
        return self.c0_bytes * (10 ** level)

    def get_level_size(self, level: int) -> int:
        with self.lock:
            return sum(reader.file_size for reader in self.levels[level])

    def register_sstable(self, level: int, reader: SSTableReader) -> None:
        with self.lock:
            self.levels[level].append(reader)
            if level > 0:
                # Level 1+ maintain strict sorted order by min_key
                self.levels[level].sort(key=lambda r: r.min_key)

    def compact_level_if_needed(self, level: int) -> List[Path]:
        """
        Checks if level capacity exceeds C_i. If so, triggers merge into level+1.
        Returns list of newly generated SSTable file paths.
        """
        with self.lock:
            if level >= self.max_levels - 1:
                return []

            current_size = self.get_level_size(level)
            capacity = self.get_level_capacity(level)

            # L0 triggers compaction based on file count (>= 4) or byte capacity
            should_compact = (level == 0 and len(self.levels[0]) >= 4) or (current_size > capacity)
            if not should_compact or not self.levels[level]:
                return []

            next_level = level + 1
            # 1. Select victim tables from current level
            if level == 0:
                # In L0, all tables potentially overlap; take all L0 tables
                source_tables = list(self.levels[0])
            else:
                # Select the largest or first table in level
                source_tables = [self.levels[level][0]]

            # Determine key range of source tables
            min_k = min(t.min_key for t in source_tables if t.min_k)
            max_k = max(t.max_key for t in source_tables if t.max_key)

            # 2. Find overlapping tables in next_level
            target_tables: List[SSTableReader] = []
            remaining_next_level: List[SSTableReader] = []
            for t in self.levels[next_level]:
                if not (t.max_key < min_k or t.min_key > max_k):
                    target_tables.append(t)
                else:
                    remaining_next_level.append(t)

            all_input_tables = source_tables + target_tables

            # 3. K-way streaming merge-sort iterator
            # Use iterator heap: (key, priority_order, value, iterator_index)
            # Priority order gives preference to newer versions (lower level or newer index)
            iterators = [t.scan_entries() for t in all_input_tables]
            heap: List[Tuple[str, int, Any, int]] = []

            for idx, it in enumerate(iterators):
                try:
                    k, v = next(it)
                    # Lower idx is newer in source_tables
                    heapq.heappush(heap, (k, idx, v, idx))
                except StopIteration:
                    pass

            merged_entries: List[Tuple[str, Any]] = []
            last_key: Optional[str] = None
            is_highest_level = (next_level == self.max_levels - 1)

            while heap:
                k, priority, v, it_idx = heapq.heappop(heap)

                # Fetch next from the iterator that just popped
                try:
                    next_k, next_v = next(iterators[it_idx])
                    heapq.heappush(heap, (next_k, it_idx, next_v, it_idx))
                except StopIteration:
                    pass

                if k == last_key:
                    # Duplicate key; since smaller priority comes first in heap,
                    # this popped element is older and superseded. Drop it.
                    continue

                last_key = k

                # Drop tombstone if it has reached the highest level (purging)
                if v is TOMBSTONE and is_highest_level:
                    continue

                merged_entries.append((k, v))

            # 4. Partition merged entries into new SSTables in next_level
            new_tables: List[Path] = []
            max_sstable_size = self.c0_bytes
            current_chunk: List[Tuple[str, Any]] = []
            approx_chunk_bytes = 0

            def write_chunk(chunk: List[Tuple[str, Any]]) -> Optional[SSTableReader]:
                if not chunk:
                    return None
                self.table_counter += 1
                out_path = self.base_dir / f"sstable_L{next_level}_{int(time.time()*1000)}_{self.table_counter}.sst"
                writer = SSTableWriter(out_path)
                writer.write_entries(chunk)
                return SSTableReader(out_path)

            for item in merged_entries:
                current_chunk.append(item)
                # approx size of key + json value
                approx_chunk_bytes += len(item[0]) + 32
                if approx_chunk_bytes >= max_sstable_size:
                    new_reader = write_chunk(current_chunk)
                    if new_reader:
                        new_tables.append(new_reader.filepath)
                        remaining_next_level.append(new_reader)
                    current_chunk = []
                    approx_chunk_bytes = 0

            if current_chunk:
                new_reader = write_chunk(current_chunk)
                if new_reader:
                    new_tables.append(new_reader.filepath)
                    remaining_next_level.append(new_reader)

            # 5. Update arbiter state
            # Remove compacted source tables from current level
            if level == 0:
                self.levels[0] = []
            else:
                self.levels[level] = [t for t in self.levels[level] if t not in source_tables]

            remaining_next_level.sort(key=lambda r: r.min_key)
            self.levels[next_level] = remaining_next_level

            # 6. Delete obsolete SSTable files from disk
            for t in all_input_tables:
                try:
                    if t.filepath.exists():
                        t.filepath.unlink()
                except OSError:
                    pass

            return new_tables


# ==============================================================================
# Top-Level LSM-Tree Storage Engine
# ==============================================================================

class LSMTree:
    """
    Autonomic Log-Structured Merge (LSM) Storage Engine.
    Coordinates active MemTable, immutable MemTable flushing, WAL logging,
    multi-level SSTables, and background compaction.
    """

    def __init__(
        self,
        db_dir: str | Path,
        memtable_threshold_bytes: int = DEFAULT_MEMTABLE_THRESHOLD_BYTES,
        wal_sync_mode: str = "ALWAYS",
    ) -> None:
        self.db_dir = Path(db_dir).resolve()
        self.db_dir.mkdir(parents=True, exist_ok=True)
        self.memtable_threshold = memtable_threshold_bytes
        self.wal_sync_mode = wal_sync_mode

        self.lock = threading.RLock()
        self.is_closed = False

        # Metrics & Telemetry
        self.stats = {
            "puts": 0,
            "deletes": 0,
            "gets": 0,
            "memtable_hits": 0,
            "immutable_hits": 0,
            "sstable_hits": 0,
            "bloom_skips": 0,
            "bloom_hits": 0,
            "flushes": 0,
            "compactions": 0,
        }

        # Initialize WAL and MemTable
        self.wal_path = self.db_dir / "active.wal"
        self.active_memtable = SkipListMemTable()
        self.immutable_memtable: Optional[SkipListMemTable] = None

        # Compaction Arbiter
        self.arbiter = CompactionArbiter(self.db_dir)

        # Recover state from existing WAL and SSTables
        self._recover_state()

        # Initialize fresh WAL for subsequent writes
        self.wal = WriteAheadLog(self.wal_path, sync_mode=self.wal_sync_mode)

    def _recover_state(self) -> None:
        """Discovers existing SSTables and replays uncommitted WAL records."""
        # 1. Discover SSTables
        sst_files = sorted(self.db_dir.glob("sstable_L*.sst"))
        for sst_file in sst_files:
            try:
                # Filename pattern: sstable_L{level}_{timestamp}_{counter}.sst
                parts = sst_file.stem.split("_")
                level_str = parts[1]  # 'L0', 'L1', etc.
                level = int(level_str[1:])
                reader = SSTableReader(sst_file)
                self.arbiter.register_sstable(level, reader)
            except Exception as e:
                sys.stderr.write(f"Warning: Failed to load SSTable {sst_file}: {e}\n")

        # 2. Replay active WAL if present
        if self.wal_path.exists():
            records = WriteAheadLog.replay_log(self.wal_path)
            for op_type, key, val in records:
                if op_type == OP_PUT:
                    self.active_memtable.put(key, val)
                elif op_type == OP_DEL:
                    self.active_memtable.delete(key)

    def put(self, key: str, value: Any) -> None:
        if self.is_closed:
            raise RuntimeError("Cannot write to closed LSMTree")
        if not isinstance(key, str):
            raise TypeError("Key must be a string")

        with self.lock:
            # 1. Append to WAL
            self.wal.append(OP_PUT, key, value)
            # 2. Insert into active MemTable
            self.active_memtable.put(key, value)
            self.stats["puts"] += 1

            # 3. Check threshold for flush
            if self.active_memtable.memory_footprint() >= self.memtable_threshold:
                self.flush_memtable()

    def delete(self, key: str) -> None:
        if self.is_closed:
            raise RuntimeError("Cannot delete from closed LSMTree")
        if not isinstance(key, str):
            raise TypeError("Key must be a string")

        with self.lock:
            self.wal.append(OP_DEL, key, TOMBSTONE)
            self.active_memtable.delete(key)
            self.stats["deletes"] += 1

            if self.active_memtable.memory_footprint() >= self.memtable_threshold:
                self.flush_memtable()

    def get(self, key: str) -> Optional[Any]:
        if self.is_closed:
            raise RuntimeError("Cannot read from closed LSMTree")

        with self.lock:
            self.stats["gets"] += 1

            # Phase 1: Query Active MemTable
            found, val = self.active_memtable.get(key)
            if found:
                self.stats["memtable_hits"] += 1
                return None if val is TOMBSTONE else val

            # Phase 2: Query Immutable MemTable
            if self.immutable_memtable is not None:
                found, val = self.immutable_memtable.get(key)
                if found:
                    self.stats["immutable_hits"] += 1
                    return None if val is TOMBSTONE else val

            # Phase 3: Query SSTables from Level 0 to Level L
            # Level 0 SSTables must be searched newest to oldest
            for reader in reversed(self.arbiter.levels[0]):
                if not reader.contains_bloom(key):
                    self.stats["bloom_skips"] += 1
                    continue
                self.stats["bloom_hits"] += 1
                found, val = reader.get(key)
                if found:
                    self.stats["sstable_hits"] += 1
                    return None if val is TOMBSTONE else val

            # Levels 1 to MaxLevel (non-overlapping key ranges per level)
            for level in range(1, self.arbiter.max_levels):
                for reader in self.arbiter.levels[level]:
                    # Check if key falls within SSTable range bounds
                    if reader.min_key and key < reader.min_key:
                        continue
                    if reader.max_key and key > reader.max_key:
                        continue

                    if not reader.contains_bloom(key):
                        self.stats["bloom_skips"] += 1
                        continue
                    self.stats["bloom_hits"] += 1
                    found, val = reader.get(key)
                    if found:
                        self.stats["sstable_hits"] += 1
                        return None if val is TOMBSTONE else val

            return None

    def scan(self, start_key: str, end_key: str) -> List[Tuple[str, Any]]:
        """
        Executes lexicographically ordered range scan [start_key, end_key].
        Merges active MemTable, immutable MemTable, and all SSTables using
        a min-heap priority queue, resolving duplicate keys and tombstones.
        """
        if self.is_closed:
            raise RuntimeError("Cannot scan closed LSMTree")

        with self.lock:
            # Structure min-heap item: (current_key, source_priority, val, iterator)
            # source_priority determines version precedence: lower = newer
            heap: List[Tuple[str, int, Any, Iterator[Tuple[str, Any]]]] = []
            priority_counter = 0

            # 1. Active MemTable items
            mem_items = self.active_memtable.scan(start_key, end_key)
            if mem_items:
                it = iter(mem_items)
                k, v = next(it)
                heapq.heappush(heap, (k, priority_counter, v, it))
            priority_counter += 1

            # 2. Immutable MemTable items
            if self.immutable_memtable:
                imm_items = self.immutable_memtable.scan(start_key, end_key)
                if imm_items:
                    it = iter(imm_items)
                    k, v = next(it)
                    heapq.heappush(heap, (k, priority_counter, v, it))
            priority_counter += 1

            # 3. L0 SSTables (newest to oldest)
            for reader in reversed(self.arbiter.levels[0]):
                entries = [
                    (k, v) for k, v in reader.scan_entries()
                    if start_key <= k <= end_key
                ]
                if entries:
                    it = iter(entries)
                    k, v = next(it)
                    heapq.heappush(heap, (k, priority_counter, v, it))
                priority_counter += 1

            # 4. Levels 1+ SSTables
            for level in range(1, self.arbiter.max_levels):
                for reader in self.arbiter.levels[level]:
                    if reader.max_key and reader.max_key < start_key:
                        continue
                    if reader.min_key and reader.min_key > end_key:
                        continue
                    entries = [
                        (k, v) for k, v in reader.scan_entries()
                        if start_key <= k <= end_key
                    ]
                    if entries:
                        it = iter(entries)
                        k, v = next(it)
                        heapq.heappush(heap, (k, priority_counter, v, it))
                    priority_counter += 1

            # K-Way Merge
            results: List[Tuple[str, Any]] = []
            last_key: Optional[str] = None

            while heap:
                k, prio, val, it = heapq.heappop(heap)

                # Advance iterator
                try:
                    next_k, next_val = next(it)
                    heapq.heappush(heap, (next_k, prio, next_val, it))
                except StopIteration:
                    pass

                if k == last_key:
                    # Older superseded entry; discard
                    continue

                last_key = k
                if val is not TOMBSTONE:
                    results.append((k, val))

            return results

    def flush_memtable(self) -> Optional[str]:
        """
        Freezes active MemTable into ImmutableMemTable, writes sorted entries
        into a Level-0 SSTable, clears WAL, and triggers compaction checks.
        """
        with self.lock:
            if len(self.active_memtable) == 0:
                return None

            # Freeze active to immutable
            self.immutable_memtable = self.active_memtable
            self.active_memtable = SkipListMemTable()

            # Rotate WAL
            self.wal.close()
            rotated_wal = self.db_dir / f"wal_{int(time.time()*1000)}.bak"
            if self.wal_path.exists():
                self.wal_path.rename(rotated_wal)
            self.wal = WriteAheadLog(self.wal_path, sync_mode=self.wal_sync_mode)

            # Write entries to L0 SSTable
            entries = list(self.immutable_memtable.iter_all())
            entries.sort(key=lambda item: item[0])

            self.arbiter.table_counter += 1
            sstable_path = self.db_dir / f"sstable_L0_{int(time.time()*1000)}_{self.arbiter.table_counter}.sst"
            writer = SSTableWriter(sstable_path)
            writer.write_entries(entries)

            reader = SSTableReader(sstable_path)
            self.arbiter.register_sstable(0, reader)

            # Clear immutable memtable and remove rotated WAL
            self.immutable_memtable = None
            if rotated_wal.exists():
                rotated_wal.unlink()

            self.stats["flushes"] += 1

            # Check and trigger background compaction
            self.compact_levels()

            return str(sstable_path)

    def compact_levels(self) -> List[str]:
        """Runs cascade compaction across levels."""
        with self.lock:
            created_sstables: List[str] = []
            for lvl in range(self.arbiter.max_levels - 1):
                new_paths = self.arbiter.compact_level_if_needed(lvl)
                if new_paths:
                    self.stats["compactions"] += 1
                    created_sstables.extend(str(p) for p in new_paths)
            return created_sstables

    def close(self) -> None:
        """Safely flushes and closes active WAL file."""
        with self.lock:
            if not self.is_closed:
                self.wal.close()
                self.is_closed = True


# ==============================================================================
# Self-Testing & Verification Suite
# ==============================================================================

def run_self_test() -> None:
    import shutil
    import tempfile

    test_dir = Path(tempfile.mkdtemp(prefix="hermes_lsm_test_"))
    print(f"[+] Initializing Hermes LSM Storage Engine Test in {test_dir}")

    try:
        # ----------------------------------------------------------------------
        # Test 1: Basic Put, Get, and Delete with Tombstones
        # ----------------------------------------------------------------------
        print("\n--- Test 1: Basic Put, Get, Delete ---")
        engine = LSMTree(test_dir / "db1", memtable_threshold_bytes=1024 * 1024)
        engine.put("node:001", {"ip": "10.0.0.1", "role": "leader"})
        engine.put("node:002", {"ip": "10.0.0.2", "role": "follower"})
        engine.put("node:003", {"ip": "10.0.0.3", "role": "follower"})

        assert engine.get("node:001") == {"ip": "10.0.0.1", "role": "leader"}
        assert engine.get("node:002") == {"ip": "10.0.0.2", "role": "follower"}
        assert engine.get("node:999") is None

        # Delete node:002
        engine.delete("node:002")
        assert engine.get("node:002") is None, "Deleted key must return None"
        engine.close()
        print("[-] Basic Put/Get/Delete verified successfully.")

        # ----------------------------------------------------------------------
        # Test 2: Crash Recovery via Write-Ahead Log (WAL)
        # ----------------------------------------------------------------------
        print("\n--- Test 2: Crash Recovery via WAL Replay ---")
        crash_db_dir = test_dir / "db_crash"
        engine_crash = LSMTree(crash_db_dir, memtable_threshold_bytes=10 * 1024 * 1024)

        for i in range(100):
            engine_crash.put(f"sensor:{i:03d}", {"metric": "temperature", "val": 20.0 + i * 0.1})

        # Simulate ungraceful shutdown without calling close() or flush()
        # Active memtable remains only in memory and WAL
        del engine_crash

        # Restart engine on same directory
        engine_recovered = LSMTree(crash_db_dir)
        for i in range(100):
            val = engine_recovered.get(f"sensor:{i:03d}")
            assert val is not None, f"Key sensor:{i:03d} missing after crash recovery"
            assert val["val"] == 20.0 + i * 0.1

        engine_recovered.close()
        print("[-] Crash recovery verified: 100/100 items recovered from WAL without loss.")

        # ----------------------------------------------------------------------
        # Test 3: Bloom Filter Negative Rejection Efficacy
        # ----------------------------------------------------------------------
        print("\n--- Test 3: Bloom Filter Efficacy & Skip Verification ---")
        bloom_db_dir = test_dir / "db_bloom"
        engine_bloom = LSMTree(bloom_db_dir, memtable_threshold_bytes=1024)  # Small threshold to force flush

        for i in range(200):
            engine_bloom.put(f"key:present:{i:04d}", f"payload_{i}")

        engine_bloom.flush_memtable()  # Ensure data is flushed to SSTable

        initial_skips = engine_bloom.stats["bloom_skips"]
        # Query 100 non-existent keys that fall within the global key space
        for i in range(100):
            val = engine_bloom.get(f"key:absent:{i:04d}")
            assert val is None

        bloom_skips = engine_bloom.stats["bloom_skips"] - initial_skips
        print(f"[-] Bloom filter skips recorded: {bloom_skips}/100 negative lookups pruned before disk seek.")
        assert bloom_skips >= 95, "Bloom filter false negative rate violated expected bounds (p <= 0.01)"

        engine_bloom.close()

        # ----------------------------------------------------------------------
        # Test 4: MemTable Overflow, Multi-Level Compaction & Range Scan
        # ----------------------------------------------------------------------
        print("\n--- Test 4: MemTable Flush, Compaction & Range Scan ---")
        compact_db_dir = test_dir / "db_compact"
        # 512-byte threshold to force frequent flushes into L0 SSTables
        engine_compact = LSMTree(compact_db_dir, memtable_threshold_bytes=512)

        total_keys = 300
        for i in range(total_keys):
            engine_compact.put(f"item:{i:04d}", {"index": i, "seq": i * 10})

        # Overwrite some keys and delete others
        for i in range(0, 50):
            engine_compact.put(f"item:{i:04d}", {"index": i, "seq": i * 999})
        for i in range(50, 100):
            engine_compact.delete(f"item:{i:04d}")

        # Explicitly invoke compaction cascade
        engine_compact.flush_memtable()
        engine_compact.compact_levels()

        # Verify point lookups after compaction
        assert engine_compact.get("item:0010") == {"index": 10, "seq": 9990}
        assert engine_compact.get("item:0075") is None  # Deleted key

        # Verify range scan [item:0040, item:0060]
        # Range should include updated keys 0040..0049, skip deleted keys 0050..0060
        scan_results = engine_compact.scan("item:0040", "item:0060")
        scan_keys = [k for k, _ in scan_results]

        expected_keys = [f"item:{i:04d}" for i in range(40, 50)]
        assert scan_keys == expected_keys, f"Scan keys mismatch: expected {expected_keys}, got {scan_keys}"

        print(f"[-] Compaction and Range Scan verified: {len(scan_results)} items matched expected projection.")
        print(f"[-] Telemetry Stats: {json.dumps(engine_compact.stats, indent=2)}")

        engine_compact.close()
        print("\n[+] ALL HERMES LSM STORAGE ENGINE VERIFICATION TESTS PASSED.")

    finally:
        shutil.rmtree(test_dir, ignore_errors=True)


if __name__ == "__main__":
    run_self_test()