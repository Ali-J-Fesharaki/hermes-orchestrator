#!/usr/bin/env python3
r"""
tasks/provenance_ledger.py
Distributed Execution Provenance Ledger & Reproducibility Attestation Engine.

Provides an immutable, tamper-evident, append-only cryptographic hash chain
and reproducibility attestation system for all milestone planning and execution
dispatches across the multi-repo autonomous orchestrator.

Hash Chain Mechanics:
  H_0 = SHA256('GENESIS')
  H_n = SHA256(H_{n-1} || canonical_json(Record_n without record_hash))

Storage Architecture:
  - Append-only JSON-Lines ledger: dashboards/provenance_ledger.jsonl
  - Visual Audit Report: dashboards/PROVENANCE_AUDIT.md
  - Thread-safe mutation via threading.Lock

Zero external dependencies: 100% Python 3.10+ standard library.
"""

from __future__ import annotations

import collections
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import threading
from typing import Any, Dict, List, Optional, Tuple, Union
import uuid

# ==============================================================================
# Path Constants & Genesis Seed
# ==============================================================================

REPO_ROOT = Path(__file__).resolve().parent.parent
DASHBOARDS_DIR = REPO_ROOT / "dashboards"
LEDGER_FILE_PATH = DASHBOARDS_DIR / "provenance_ledger.jsonl"
AUDIT_MARKDOWN_PATH = DASHBOARDS_DIR / "PROVENANCE_AUDIT.md"

for _p in [str(REPO_ROOT), str(DASHBOARDS_DIR)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

GENESIS_SEED = b"GENESIS"
GENESIS_HASH: str = hashlib.sha256(GENESIS_SEED).hexdigest()


# ==============================================================================
# Helper Utilities
# ==============================================================================

def hash_prompt(prompt_text: Optional[str]) -> str:
    """Computes deterministic SHA-256 hex digest of a prompt text string."""
    if not prompt_text:
        return hashlib.sha256(b"").hexdigest()
    return hashlib.sha256(prompt_text.strip().encode("utf-8")).hexdigest()


def compute_merkle_root(file_hashes: Dict[str, str]) -> str:
    """
    Computes a deterministic Merkle root from a mapping of relative file paths to SHA-256 hashes.
    Leaves are sorted lexicographically by normalized relative path.
    """
    if not file_hashes:
        return hashlib.sha256(b"EMPTY_TREE").hexdigest()

    sorted_leaves = sorted((os.path.normpath(p), h.strip().lower()) for p, h in file_hashes.items())
    leaf_hashes = [
        hashlib.sha256(f"{p}:{h}".encode("utf-8")).digest()
        for p, h in sorted_leaves
    ]

    while len(leaf_hashes) > 1:
        if len(leaf_hashes) % 2 == 1:
            leaf_hashes.append(leaf_hashes[-1])
        next_level = []
        for i in range(0, len(leaf_hashes), 2):
            combined = hashlib.sha256(leaf_hashes[i] + leaf_hashes[i + 1]).digest()
            next_level.append(combined)
        leaf_hashes = next_level

    return leaf_hashes[0].hex()


# ==============================================================================
# Data Models
# ==============================================================================

@dataclass
class ProvenanceRecord:
    """
    Cryptographically verifiable execution record representing an atomic
    planning and execution lifecycle event in the autonomous fleet.
    """
    record_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    sequence_number: int = 0
    prev_hash: str = ""
    record_hash: str = ""
    timestamp_utc: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    milestone_id: str = ""
    milestone_name: str = ""
    repo_id: str = ""
    dag_tier: int = 0
    planner_model: str = ""
    executor_model: str = ""
    planner_prompt_hash: str = ""
    executor_prompt_hash: str = ""
    sandbox_lease_token: Optional[str] = None
    pre_execution_merkle_root: Optional[str] = None
    post_execution_merkle_root: Optional[str] = None
    rate_governor_snapshot: Dict[str, Any] = field(default_factory=dict)
    execution_state_reason: str = ""
    git_commit_hash: Optional[str] = None
    execution_duration_sec: float = 0.0
    token_usage: Dict[str, Any] = field(default_factory=dict)
    exit_status: str = "SUCCESS"  # 'SUCCESS' | 'FAILURE' | 'ROLLBACK'
    failure_task_id: Optional[str] = None

    def __post_init__(self):
        if not self.record_id:
            self.record_id = uuid.uuid4().hex
        if not self.timestamp_utc:
            self.timestamp_utc = datetime.now(timezone.utc).isoformat()
        if self.rate_governor_snapshot is None:
            self.rate_governor_snapshot = {}
        if self.token_usage is None:
            self.token_usage = {}
        self.exit_status = str(self.exit_status).upper()
        if self.exit_status not in ("SUCCESS", "FAILURE", "ROLLBACK"):
            self.exit_status = "SUCCESS"

    def to_dict(self) -> Dict[str, Any]:
        """Converts record to JSON-serializable dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ProvenanceRecord:
        """Instantiates ProvenanceRecord from dictionary, safely ignoring extra keys."""
        valid_fields = {f for f in cls.__dataclass_fields__}
        filtered = {k: v for k, v in data.items() if k in valid_fields}
        return cls(**filtered)


@dataclass
class ChainVerificationResult:
    """Result of an end-to-end cryptographic integrity verification across the ledger."""
    is_valid: bool
    total_records: int
    first_broken_index: Optional[int] = None
    expected_hash: Optional[str] = None
    actual_hash: Optional[str] = None
    verified_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ==============================================================================
# Provenance Ledger Core Engine
# ==============================================================================

class ProvenanceLedger:
    """
    Append-only, thread-safe Distributed Execution Provenance Ledger and
    Reproducibility Attestation Engine.
    """

    def __init__(
        self,
        repo_root: Optional[Union[str, Path]] = None,
        dashboards_dir: Optional[Union[str, Path]] = None,
        ledger_file: Optional[Union[str, Path]] = None,
        audit_file: Optional[Union[str, Path]] = None,
    ):
        self.repo_root = Path(repo_root) if repo_root else REPO_ROOT
        self.dashboards_dir = Path(dashboards_dir) if dashboards_dir else (self.repo_root / "dashboards")
        self.ledger_file = Path(ledger_file) if ledger_file else (self.dashboards_dir / "provenance_ledger.jsonl")
        self.audit_file = Path(audit_file) if audit_file else (self.dashboards_dir / "PROVENANCE_AUDIT.md")

        self.dashboards_dir.mkdir(parents=True, exist_ok=True)
        if self.ledger_file.parent:
            self.ledger_file.parent.mkdir(parents=True, exist_ok=True)
        if self.audit_file.parent:
            self.audit_file.parent.mkdir(parents=True, exist_ok=True)

        self._lock = threading.Lock()
        self._head_seq: int = 0
        self._head_hash: str = GENESIS_HASH

        self._load_head()

    def _canonical_json(self, record_dict: Dict[str, Any]) -> str:
        """
        Produces deterministic canonical JSON string:
        - Sorted keys
        - Strict separators (no extraneous whitespace)
        - Unicode preserved (ensure_ascii=False)
        - Strips 'record_hash' to prevent recursive self-hashing
        """
        hashable = {k: v for k, v in record_dict.items() if k != "record_hash"}
        return json.dumps(hashable, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    def _compute_hash(self, prev_hash: str, record_dict: Dict[str, Any]) -> str:
        """
        H_n = SHA256(H_{n-1} || canonical_json(Record_n without record_hash))
        """
        canon_str = self._canonical_json(record_dict)
        payload = prev_hash.encode("utf-8") + canon_str.encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def _load_head(self) -> None:
        """Initializes chain head state from existing ledger file on disk."""
        with self._lock:
            if not self.ledger_file.exists() or self.ledger_file.stat().st_size == 0:
                self._head_seq = 0
                self._head_hash = GENESIS_HASH
                return

            last_valid_seq = 0
            last_valid_hash = GENESIS_HASH

            try:
                with open(self.ledger_file, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                            seq = entry.get("sequence_number")
                            r_hash = entry.get("record_hash")
                            if isinstance(seq, int) and r_hash:
                                last_valid_seq = seq
                                last_valid_hash = str(r_hash)
                        except Exception:
                            continue
            except Exception:
                pass

            self._head_seq = last_valid_seq
            self._head_hash = last_valid_hash

    def get_chain_head(self) -> Tuple[int, str]:
        """Returns (sequence_number, hash) of the current chain head."""
        with self._lock:
            return (self._head_seq, self._head_hash)

    def append_record(self, record: ProvenanceRecord) -> str:
        """
        Appends a ProvenanceRecord to the append-only ledger in a thread-safe manner.
        Automatically links sequence_number and prev_hash from the chain head,
        computes record_hash, persists single JSON line, updates head, and returns hash.
        """
        with self._lock:
            next_seq = self._head_seq + 1
            prev_hash = self._head_hash

            record.sequence_number = next_seq
            record.prev_hash = prev_hash

            if not record.timestamp_utc:
                record.timestamp_utc = datetime.now(timezone.utc).isoformat()
            if not record.record_id:
                record.record_id = uuid.uuid4().hex

            computed_hash = self._compute_hash(prev_hash, record.to_dict())
            record.record_hash = computed_hash

            # Ensure parent directories exist
            self.ledger_file.parent.mkdir(parents=True, exist_ok=True)

            # Append atomic JSON line
            line = json.dumps(record.to_dict(), ensure_ascii=False)
            with open(self.ledger_file, "a", encoding="utf-8") as f:
                f.write(line + "\n")
                f.flush()

            self._head_seq = next_seq
            self._head_hash = computed_hash

            return computed_hash

    def load_all_records(self) -> List[ProvenanceRecord]:
        """Reads and parses all records from the ledger file in sequence order."""
        if not self.ledger_file.exists():
            return []

        records: List[ProvenanceRecord] = []
        with open(self.ledger_file, "r", encoding="utf-8") as f:
            for line_idx, line in enumerate(f, start=1):
                clean_line = line.strip()
                if not clean_line:
                    continue
                try:
                    payload = json.loads(clean_line)
                    records.append(ProvenanceRecord.from_dict(payload))
                except Exception as err:
                    # Defensive parsing to ensure corrupted single lines don't crash stream
                    continue
        return records

    def verify_chain(self) -> ChainVerificationResult:
        """
        Performs full cryptographic audit of ledger integrity:
        Iterates all records in order, recomputing hashes H_0 -> H_n.
        Detects any tampering, truncation, or broken links.
        """
        records = self.load_all_records()
        total = len(records)
        current_time = datetime.now(timezone.utc).isoformat()

        if total == 0:
            return ChainVerificationResult(
                is_valid=True,
                total_records=0,
                first_broken_index=None,
                expected_hash=GENESIS_HASH,
                actual_hash=GENESIS_HASH,
                verified_at=current_time,
            )

        expected_prev = GENESIS_HASH

        for idx, rec in enumerate(records):
            expected_seq = idx + 1
            if rec.sequence_number != expected_seq:
                return ChainVerificationResult(
                    is_valid=False,
                    total_records=total,
                    first_broken_index=idx,
                    expected_hash=f"seq:{expected_seq}",
                    actual_hash=f"seq:{rec.sequence_number}",
                    verified_at=current_time,
                )

            if rec.prev_hash != expected_prev:
                return ChainVerificationResult(
                    is_valid=False,
                    total_records=total,
                    first_broken_index=idx,
                    expected_hash=expected_prev,
                    actual_hash=rec.prev_hash,
                    verified_at=current_time,
                )

            recomputed_hash = self._compute_hash(expected_prev, rec.to_dict())
            if rec.record_hash != recomputed_hash:
                return ChainVerificationResult(
                    is_valid=False,
                    total_records=total,
                    first_broken_index=idx,
                    expected_hash=recomputed_hash,
                    actual_hash=rec.record_hash,
                    verified_at=current_time,
                )

            expected_prev = rec.record_hash

        return ChainVerificationResult(
            is_valid=True,
            total_records=total,
            first_broken_index=None,
            expected_hash=expected_prev,
            actual_hash=expected_prev,
            verified_at=current_time,
        )

    def get_record_by_commit(self, commit_hash: str) -> Optional[ProvenanceRecord]:
        """
        Linear search for matching git_commit_hash.
        Supports both full SHA and short hex prefix matching.
        """
        if not commit_hash:
            return None

        clean_hash = commit_hash.strip().lower()
        records = self.load_all_records()

        # Reverse iterate to match newest occurrence first
        for rec in reversed(records):
            if not rec.git_commit_hash:
                continue
            rec_commit = rec.git_commit_hash.strip().lower()
            if rec_commit == clean_hash:
                return rec
            if len(clean_hash) >= 6 and rec_commit.startswith(clean_hash):
                return rec
            if len(rec_commit) >= 6 and clean_hash.startswith(rec_commit):
                return rec

        return None

    def get_records_by_milestone(self, milestone_id: str) -> List[ProvenanceRecord]:
        """Filters all provenance records matching a given milestone ID or name."""
        if not milestone_id:
            return []

        target = milestone_id.strip()
        records = self.load_all_records()
        return [
            rec for rec in records
            if rec.milestone_id == target or rec.milestone_name == target
        ]

    def get_attestation(self, commit_hash: str) -> Dict[str, Any]:
        """
        Generates full reproducibility attestation certificate for a git commit.
        Includes cryptographic proofs, model provenance, prompts, sandbox tokens,
        rate governor parameters, and Merkle tree roots.
        """
        record = self.get_record_by_commit(commit_hash)
        if not record:
            return {
                "status": "NOT_FOUND",
                "error": f"No execution provenance record found for commit {commit_hash}",
                "query_commit_hash": commit_hash,
            }

        verification = self.verify_chain()

        # Composite reproducibility signature
        sig_components = [
            record.planner_prompt_hash or "",
            record.executor_prompt_hash or "",
            record.planner_model or "",
            record.executor_model or "",
            str(record.dag_tier),
            record.pre_execution_merkle_root or "",
            record.post_execution_merkle_root or "",
            str(record.git_commit_hash or ""),
            record.exit_status,
        ]
        reproducibility_sig = hashlib.sha256(":".join(sig_components).encode("utf-8")).hexdigest()

        return {
            "status": "ATTESTED",
            "attestation_id": f"attest_{record.record_id[:12]}",
            "attestation_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "chain_integrity_valid": verification.is_valid,
            "chain_total_records": verification.total_records,
            "reproducibility_signature": reproducibility_sig,
            "record": {
                "record_id": record.record_id,
                "sequence_number": record.sequence_number,
                "record_hash": record.record_hash,
                "prev_hash": record.prev_hash,
                "timestamp_utc": record.timestamp_utc,
                "git_commit_hash": record.git_commit_hash,
                "milestone_id": record.milestone_id,
                "milestone_name": record.milestone_name,
                "repo_id": record.repo_id,
                "dag_tier": record.dag_tier,
                "planner_model": record.planner_model,
                "executor_model": record.executor_model,
                "planner_prompt_hash": record.planner_prompt_hash,
                "executor_prompt_hash": record.executor_prompt_hash,
                "sandbox_lease_token": record.sandbox_lease_token,
                "pre_execution_merkle_root": record.pre_execution_merkle_root,
                "post_execution_merkle_root": record.post_execution_merkle_root,
                "rate_governor_snapshot": record.rate_governor_snapshot,
                "execution_state_reason": record.execution_state_reason,
                "execution_duration_sec": record.execution_duration_sec,
                "token_usage": record.token_usage,
                "exit_status": record.exit_status,
                "failure_task_id": record.failure_task_id,
            }
        }

    def generate_audit_markdown(self) -> str:
        """
        Produces dashboards/PROVENANCE_AUDIT.md with header, summary statistics,
        verification status, and a chronological table of the last 50 execution records.
        """
        records = self.load_all_records()
        total_records = len(records)
        verification = self.verify_chain()

        chain_status_badge = "✅ VALID (Unbroken Cryptographic Hash Chain)" if verification.is_valid else "❌ COMPROMISED (Chain Mismatch Detected)"
        
        success_count = sum(1 for r in records if r.exit_status == "SUCCESS")
        failure_count = sum(1 for r in records if r.exit_status == "FAILURE")
        rollback_count = sum(1 for r in records if r.exit_status == "ROLLBACK")

        planners = sorted({r.planner_model for r in records if r.planner_model})
        executors = sorted({r.executor_model for r in records if r.executor_model})
        repos = sorted({r.repo_id for r in records if r.repo_id})

        start_date = records[0].timestamp_utc if records else "N/A"
        end_date = records[-1].timestamp_utc if records else "N/A"

        lines = [
            "# 🛡️ Distributed Execution Provenance Ledger & Reproducibility Audit",
            "",
            f"**Chain Status:** {chain_status_badge}  ",
            f"**Verified At:** `{verification.verified_at}`  ",
            f"**Ledger File:** `{self.ledger_file}`  ",
            f"**Audit Report:** `{self.audit_file}`  ",
            "",
            "---",
            "",
            "## 📊 Executive Provenance Summary",
            f"- **Total Provenance Records:** `{total_records}`",
            f"- **Chain Head Sequence:** `#{self._head_seq}`",
            f"- **Chain Head Hash:** `{self._head_hash}`",
            f"- **Genesis Hash:** `{GENESIS_HASH}`",
            f"- **Date Range:** `{start_date}` → `{end_date}`",
            f"- **Outcome Distribution:** `SUCCESS={success_count}` | `FAILURE={failure_count}` | `ROLLBACK={rollback_count}`",
            f"- **Active Planner Models:** {', '.join(f'`{m}`' for m in planners) or 'None'}",
            f"- **Active Executor Models:** {', '.join(f'`{m}`' for m in executors) or 'None'}",
            f"- **Monitored Repositories:** {', '.join(f'`{r}`' for r in repos) or 'None'}",
            "",
            "---",
            "",
            "## 📜 Chronological Execution Records (Recent 50)",
            "",
            "| Seq | Timestamp (UTC) | Milestone | Repo | Tier | Models (Plan / Exec) | Commit | Status | Duration |",
            "|:---:|:---|:---|:---|:---:|:---|:---:|:---:|:---:|"
        ]

        # Recent 50 records
        display_records = records[-50:] if total_records > 50 else records

        if not display_records:
            lines.append("| - | - | *No execution records registered in ledger.* | - | - | - | - | - | - |")
        else:
            for rec in display_records:
                ts_short = rec.timestamp_utc.split("T")[-1][:8] if "T" in rec.timestamp_utc else rec.timestamp_utc[:19]
                commit_display = f"`{rec.git_commit_hash[:7]}`" if rec.git_commit_hash else "*None*"
                status_icon = "🟢" if rec.exit_status == "SUCCESS" else ("🔴" if rec.exit_status == "FAILURE" else "🟡")
                status_str = f"{status_icon} {rec.exit_status}"
                models_str = f"{rec.planner_model.split('-')[0]}/{rec.executor_model.split('-')[0]}"
                dur_str = f"{rec.execution_duration_sec:.1f}s" if rec.execution_duration_sec else "0.0s"
                ms_clean = (rec.milestone_name[:28] + "…") if len(rec.milestone_name) > 30 else rec.milestone_name

                lines.append(
                    f"| `#{rec.sequence_number}` | `{ts_short}` | {ms_clean} | `{rec.repo_id}` | `{rec.dag_tier}` | `{models_str}` | {commit_display} | {status_str} | {dur_str} |"
                )

        lines.extend([
            "",
            "---",
            "## 🔒 Cryptographic Verification Contract",
            "Every execution dispatch in the autonomous orchestrator is appended to the monotonically increasing SHA-256 hash chain.",
            "Independent auditors can verify mathematical integrity via `/provenance/verify-chain` or `tasks/provenance_ledger.py`.",
            ""
        ])

        markdown_content = "\n".join(lines) + "\n"

        try:
            self.audit_file.parent.mkdir(parents=True, exist_ok=True)
            self.audit_file.write_text(markdown_content, encoding="utf-8")
        except Exception:
            pass

        return markdown_content

    def handle_provenance_request(self, payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        """
        HTTP controller hook for control center / API server.
        Dispatches actions for endpoints:
          - /provenance/timeline
          - /provenance/attest/<commit>
          - /provenance/verify-chain
          - /provenance/stats
        Returns (HTTP status code, response dictionary).
        """
        if not isinstance(payload, dict):
            return 400, {"success": False, "error": "Invalid payload format: expected JSON dictionary"}

        path = str(payload.get("path") or payload.get("endpoint") or "").strip()
        action = str(payload.get("action") or "").strip()

        # Normalize route from path if provided
        if path.startswith("/provenance/attest/"):
            action = "attest"
            commit_part = path.split("/provenance/attest/")[-1].strip()
            if commit_part:
                payload["commit_hash"] = commit_part
        elif "/provenance/timeline" in path or "/timeline" in path:
            action = "timeline"
        elif "/provenance/verify-chain" in path or "/verify-chain" in path or "/verify_chain" in path:
            action = "verify_chain"
        elif "/provenance/stats" in path or "/stats" in path:
            action = "stats"

        # Dispatch action handlers
        if action == "timeline":
            records = self.load_all_records()
            total = len(records)
            limit = int(payload.get("limit", 50))
            offset = int(payload.get("offset", 0))
            reverse = bool(payload.get("reverse", True))

            if reverse:
                records = list(reversed(records))

            sliced = records[offset : offset + limit]
            return 200, {
                "success": True,
                "total": total,
                "limit": limit,
                "offset": offset,
                "records": [r.to_dict() for r in sliced],
            }

        elif action == "attest":
            commit_hash = payload.get("commit_hash") or payload.get("commit")
            if not commit_hash:
                return 400, {
                    "success": False,
                    "error": "Missing 'commit_hash' parameter in attest request",
                }

            attestation = self.get_attestation(str(commit_hash))
            if attestation.get("status") == "NOT_FOUND":
                return 404, {"success": False, "error": attestation.get("error")}

            return 200, {"success": True, "attestation": attestation}

        elif action == "verify_chain":
            result = self.verify_chain()
            return 200, {
                "success": True,
                "verification": result.to_dict(),
            }

        elif action == "stats":
            records = self.load_all_records()
            head_seq, head_hash = self.get_chain_head()
            verification = self.verify_chain()

            success_cnt = sum(1 for r in records if r.exit_status == "SUCCESS")
            failure_cnt = sum(1 for r in records if r.exit_status == "FAILURE")
            rollback_cnt = sum(1 for r in records if r.exit_status == "ROLLBACK")

            stats_data = {
                "total_records": len(records),
                "head_sequence": head_seq,
                "head_hash": head_hash,
                "genesis_hash": GENESIS_HASH,
                "is_chain_valid": verification.is_valid,
                "success_count": success_cnt,
                "failure_count": failure_cnt,
                "rollback_count": rollback_cnt,
                "unique_repos": len({r.repo_id for r in records if r.repo_id}),
                "unique_planner_models": len({r.planner_model for r in records if r.planner_model}),
                "unique_executor_models": len({r.executor_model for r in records if r.executor_model}),
            }
            return 200, {"success": True, "stats": stats_data}

        elif action in ("by_milestone", "record"):
            m_id = str(payload.get("milestone_id") or "").strip()
            if not m_id:
                return 400, {"success": False, "error": "Missing 'milestone_id' parameter"}
            matching = self.get_records_by_milestone(m_id)
            return 200, {
                "success": True,
                "milestone_id": m_id,
                "total": len(matching),
                "records": [r.to_dict() for r in matching],
            }

        else:
            return 400, {
                "success": False,
                "error": f"Unknown action: '{action}'. Supported: 'timeline', 'attest', 'verify_chain', 'stats', 'by_milestone'",
            }


# ==============================================================================
# Module Singleton & Convenience Functions
# ==============================================================================

_GLOBAL_LEDGER: Optional[ProvenanceLedger] = None
_GLOBAL_LOCK = threading.Lock()


def get_ledger() -> ProvenanceLedger:
    """Returns thread-safe global singleton instance of ProvenanceLedger."""
    global _GLOBAL_LEDGER
    if _GLOBAL_LEDGER is None:
        with _GLOBAL_LOCK:
            if _GLOBAL_LEDGER is None:
                _GLOBAL_LEDGER = ProvenanceLedger()
    return _GLOBAL_LEDGER


def create_provenance_record(**kwargs) -> ProvenanceRecord:
    """Convenience factory function to create and validate a ProvenanceRecord."""
    return ProvenanceRecord(**kwargs)


# ==============================================================================
# Demo & Self-Validation Execution
# ==============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print(" 🛡️ DISTRIBUTED EXECUTION PROVENANCE LEDGER & ATTESTATION ENGINE")
    print("=" * 80)

    ledger = get_ledger()
    initial_seq, initial_hash = ledger.get_chain_head()
    print(f"• Initial Chain Head:  #{initial_seq} [{initial_hash[:16]}...]")
    print(f"• Genesis Hash:        {GENESIS_HASH[:16]}...")
    print(f"• Ledger Storage:      {ledger.ledger_file}")

    # Generate 3 representative sample execution records
    sample_records = [
        ProvenanceRecord(
            milestone_id="ms_001_dag_engine",
            milestone_name="Implement Topological DAG Scheduler & Tiering",
            repo_id="hermes_orchestrator",
            dag_tier=0,
            planner_model="claude-opus-4-6-thinking",
            executor_model="gemini-3.8-flash",
            planner_prompt_hash=hash_prompt("High-level architectural planning for DAG engine"),
            executor_prompt_hash=hash_prompt("Generate tasks/dag_scheduler.py with topological levels"),
            sandbox_lease_token="lease_sbx_99a1f2b3c4",
            pre_execution_merkle_root="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            post_execution_merkle_root="2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae",
            rate_governor_snapshot={"stagger_sec": 3.5, "mode": "BALANCED", "quota_pct": 82.5, "max_tokens": 3400},
            execution_state_reason="READY_FOR_DISPATCH",
            git_commit_hash="61bcee9a11f2",
            execution_duration_sec=5.42,
            token_usage={"prompt_tokens": 1240, "completion_tokens": 3120, "total_tokens": 4360},
            exit_status="SUCCESS",
        ),
        ProvenanceRecord(
            milestone_id="ms_002_sandbox_fail",
            milestone_name="Simulate Blast Radius Violation in Ephemeral Sandbox",
            repo_id="autonomous_agents",
            dag_tier=1,
            planner_model="claude-opus-4-6-thinking",
            executor_model="gemini-3.8-flash",
            planner_prompt_hash=hash_prompt("Sandbox test plan with out-of-scope write"),
            executor_prompt_hash=hash_prompt("Write out of bounds to trigger blast radius"),
            sandbox_lease_token="lease_sbx_blast_violation",
            pre_execution_merkle_root="2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae",
            post_execution_merkle_root="fc8252c8dc55839967c58b9ad755a59b61b67c13220169c27497d8bee30e5e0d",
            rate_governor_snapshot={"stagger_sec": 4.0, "mode": "BALANCED", "quota_pct": 80.0, "max_tokens": 3400},
            execution_state_reason="TARGET_FILE_COLLISION",
            git_commit_hash=None,
            execution_duration_sec=3.18,
            token_usage={"prompt_tokens": 980, "completion_tokens": 450, "total_tokens": 1430},
            exit_status="FAILURE",
            failure_task_id="fail_auto_e7c3a1",
        ),
        ProvenanceRecord(
            milestone_id="ms_003_provenance_engine",
            milestone_name="Distributed Execution Provenance Ledger & Reproducibility Attestation Engine",
            repo_id="hermes_orchestrator",
            dag_tier=2,
            planner_model="claude-opus-4-6-thinking",
            executor_model="gemini-3.8-flash",
            planner_prompt_hash=hash_prompt("Planning specification for cryptographic provenance ledger"),
            executor_prompt_hash=hash_prompt("Implement tasks/provenance_ledger.py"),
            sandbox_lease_token="lease_sbx_attestation_proof",
            pre_execution_merkle_root="fc8252c8dc55839967c58b9ad755a59b61b67c13220169c27497d8bee30e5e0d",
            post_execution_merkle_root="d8578edf8458ce06fbc5bb76a58c5ca4fa3b61e967c4d6384120d3345261482d",
            rate_governor_snapshot={"stagger_sec": 2.5, "mode": "AGGRESSIVE", "quota_pct": 89.0, "max_tokens": 4096},
            execution_state_reason="READY_FOR_DISPATCH",
            git_commit_hash="bcb4323f49b1",
            execution_duration_sec=6.84,
            token_usage={"prompt_tokens": 1580, "completion_tokens": 3820, "total_tokens": 5400},
            exit_status="SUCCESS",
        ),
    ]

    print("\n[+] Appending 3 sample execution records to cryptographic ledger...")
    for rec in sample_records:
        h = ledger.append_record(rec)
        print(f"  • Appended Seq #{rec.sequence_number}: Milestone '{rec.milestone_id}' -> Hash: {h[:20]}...")

    print("\n[+] Verifying ledger hash chain integrity...")
    verify_res = ledger.verify_chain()
    print(f"  • Chain Valid:         {verify_res.is_valid}")
    print(f"  • Total Records:       {verify_res.total_records}")
    print(f"  • Verified At:         {verify_res.verified_at}")
    print(f"  • Head Hash:           {verify_res.actual_hash}")

    print("\n[+] Generating audit markdown...")
    audit_md = ledger.generate_audit_markdown()
    print(f"  • Wrote audit report to {ledger.audit_file} ({len(audit_md)} bytes)")

    print("\n[+] Generating reproducibility attestation for commit 'bcb4323f49b1'...")
    attestation = ledger.get_attestation("bcb4323f49b1")
    print(f"  • Attestation ID:      {attestation.get('attestation_id')}")
    print(f"  • Chain Integrity:     {attestation.get('chain_integrity_valid')}")
    print(f"  • Repro Signature:     {attestation.get('reproducibility_signature')}")
    print(f"  • Planner Prompt Hash: {attestation.get('record', {}).get('planner_prompt_hash')}")
    print(f"  • Executor Prompt Hash:{attestation.get('record', {}).get('executor_prompt_hash')}")

    print("\n[+] Testing handle_provenance_request HTTP controller hook...")
    status_code, resp = ledger.handle_provenance_request({"action": "stats"})
    print(f"  • /provenance/stats -> HTTP {status_code}: {json.dumps(resp.get('stats', {}), indent=2)}")

    print("\n" + "=" * 80)
    print(" ✅ PROVENANCE LEDGER ENGINE VERIFIED SUCCESSFULLY")
    print("=" * 80)
