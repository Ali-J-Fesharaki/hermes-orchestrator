#!/usr/bin/env python3
"""
Pipeline Autonomic Multi-Account Quota Reservation Lease Controller & Thundering-Herd Deflector

Milestone:
    Pipeline Autonomic Multi-Account Quota Reservation Lease Controller & Thundering-Herd Deflector

Mathematical Architecture & Governance:
    Let A = {acc_1, acc_2, ..., acc_k} be active cloud model accounts in ~/.antigravity-agent/cloud_accounts.db.

    1. Two-Phase Token Reservation State Machine:
       Sigma_L in {REQUESTED, RESERVED, COMMITTED, RELEASED, EXPIRED, QUARANTINED}
       Request: Req = <worker_id, repo_id, milestone_id, estimated_tokens, max_lease_sec>
       If B_{acc}(t) >= estimated_tokens and account cooldown t_cool == 0:
           Issue lease L = <lease_id, account_id, tokens_reserved, lease_expiry, nonce>
           Deduct tokens_reserved optimistically from active bucket B_{acc}.

    2. Token Debt Amortization & Exponential Smoothing:
       Sliding-window consumption rate model:
           R_hat_t = alpha * R_t + (1 - alpha) * R_hat_{t-1}, with smoothing factor alpha = 0.2
       Token variance:
           Delta_tau = tau_actual - tau_estimated
       Upon commit:
           B_{acc} <- B_{acc} + (tau_estimated - tau_actual)
       If tau_actual > tau_estimated:
           Excess token debt is amortized across window W_debt with backoff multiplier:
           kappa = 1 + (tau_actual - tau_estimated) / tau_estimated.

    3. Thundering-Herd Deflection & Account Arbitration:
       Decorrelated jittered backoff on contention:
           T_wait = min(T_max, Uniform(T_base, T_base * 3 * 2^{retries}))
       Account Selection Score:
           Score(acc_i) = [RemainingQuota(acc_i) / (ActiveLeases(acc_i) + 1)]
                          * (1 - CaptchaPenalty(acc_i))
                          * exp(-PenaltyCooldown(acc_i))
       Arbitrator routes reservation to acc* = argmax_{acc in A} Score(acc).

    4. Diagnostics & Cockpit Persistence:
       - Emits structured diagnostic snapshot to dashboards/quota_reservation_state.json
       - Emits human-readable Markdown summary dashboards/QUOTA_RESERVATION_DIGEST.md
       - Exports diagnostic helper hooks for tasks/execution_state_reasoner.py ('QUOTA_LEASE_WAIT', 'THUNDERING_HERD_DEFLECTED').

Dependencies:
    Zero external dependencies. Pure standard Python library.
"""

import argparse
from dataclasses import dataclass, field, asdict
from enum import Enum
import hashlib
import json
import math
import os
from pathlib import Path
import random
import sqlite3
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Tuple


# ============================================================================
# Domain Models & Enums
# ============================================================================

class LeaseState(str, Enum):
    """Two-Phase Token Reservation State Machine states."""
    REQUESTED = "REQUESTED"
    RESERVED = "RESERVED"
    COMMITTED = "COMMITTED"
    RELEASED = "RELEASED"
    EXPIRED = "EXPIRED"
    QUARANTINED = "QUARANTINED"


class AccountHealthStatus(str, Enum):
    """Operational health state of cloud quota account."""
    HEALTHY = "HEALTHY"
    COOLDOWN = "COOLDOWN"
    CAPTCHA_BLOCKED = "CAPTCHA_BLOCKED"
    EXHAUSTED = "EXHAUSTED"


@dataclass
class QuotaReservationLease:
    """Represents a bounded atomic reservation lease for model tokens."""
    lease_id: str
    account_id: str
    worker_id: str
    repo_id: str
    milestone_id: str
    reserved_tokens: int
    actual_tokens: int = 0
    state: LeaseState = LeaseState.REQUESTED
    granted_at: float = field(default_factory=time.time)
    expires_at: float = 0.0
    committed_at: Optional[float] = None
    nonce: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_active(self) -> bool:
        return self.state == LeaseState.RESERVED and time.time() < self.expires_at

    @property
    def remaining_ttl_sec(self) -> float:
        return max(0.0, self.expires_at - time.time())

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["state"] = self.state.value
        d["remaining_ttl_sec"] = round(self.remaining_ttl_sec, 2)
        return d


@dataclass
class AccountBucketState:
    """In-memory tracking and amortization state for an account bucket."""
    account_id: str
    email: str
    provider: str
    capacity_tokens: int
    available_tokens: int
    active_leases_count: int = 0
    cooldown_until: float = 0.0
    last_used_at: float = 0.0
    smoothed_rate: float = 0.0  # R_hat_t (tokens / sec)
    captcha_penalty: float = 0.0  # [0.0, 1.0]
    debt_pool: int = 0  # Unamortized token debt
    debt_amortization_deadline: float = 0.0
    status: AccountHealthStatus = AccountHealthStatus.HEALTHY
    total_leases_granted: int = 0
    total_tokens_consumed: int = 0

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["status"] = self.status.value
        d["cooldown_remaining_sec"] = max(0.0, round(self.cooldown_until - time.time(), 2))
        return d


# ============================================================================
# Multi-Account SQLite Reader & Fallback Provider
# ============================================================================

class CloudAccountStore:
    """Thread-safe connector for ~/.antigravity-agent/cloud_accounts.db with mock fallbacks."""

    def __init__(self, db_path: Optional[Path] = None):
        if db_path is None:
            self.db_path = Path.home() / ".antigravity-agent" / "cloud_accounts.db"
        else:
            self.db_path = Path(db_path)

    def load_accounts(self) -> List[AccountBucketState]:
        """Loads account quotas and health from SQLite, falling back to 4 Google Pro profiles."""
        accounts: List[AccountBucketState] = []
        if self.db_path.exists():
            try:
                conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, timeout=3.0)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                # Check available tables
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [row["name"] for row in cursor.fetchall()]

                table_name = None
                for candidate in ("cloud_accounts", "accounts", "google_accounts", "quotas"):
                    if candidate in tables:
                        table_name = candidate
                        break

                if table_name:
                    cursor.execute(f"SELECT * FROM {table_name}")
                    rows = cursor.fetchall()
                    for r in rows:
                        row_dict = dict(r)
                        acc_id = str(row_dict.get("account_id") or row_dict.get("id") or f"acc_{len(accounts)+1}")
                        email = str(row_dict.get("email") or f"{acc_id}@antigravity.internal")
                        provider = str(row_dict.get("provider") or "google_gemini_pro")
                        cap = int(row_dict.get("capacity_tokens") or row_dict.get("token_limit") or 1_000_000)
                        avail = int(row_dict.get("available_tokens") or row_dict.get("tokens_remaining") or cap)
                        cooldown = float(row_dict.get("cooldown_until") or 0.0)
                        captcha = 1.0 if bool(row_dict.get("captcha_blocked") or row_dict.get("is_quarantined")) else 0.0

                        status = AccountHealthStatus.HEALTHY
                        now = time.time()
                        if captcha > 0.5:
                            status = AccountHealthStatus.CAPTCHA_BLOCKED
                        elif cooldown > now:
                            status = AccountHealthStatus.COOLDOWN
                        elif avail <= 0:
                            status = AccountHealthStatus.EXHAUSTED

                        accounts.append(
                            AccountBucketState(
                                account_id=acc_id,
                                email=email,
                                provider=provider,
                                capacity_tokens=cap,
                                available_tokens=avail,
                                cooldown_until=cooldown,
                                captcha_penalty=captcha,
                                status=status,
                            )
                        )
                conn.close()
            except Exception as e:
                # Log non-fatal error and fall through to built-in fallbacks
                pass

        if not accounts:
            accounts = self._get_fallback_pro_accounts()

        return accounts

    def _get_fallback_pro_accounts(self) -> List[AccountBucketState]:
        """Provides the standard 4 Google Pro quota accounts (5h rotation cadence)."""
        now = time.time()
        return [
            AccountBucketState(
                account_id="acc_pro_1",
                email="pro.primary@antigravity.internal",
                provider="google_pro",
                capacity_tokens=1_000_000,
                available_tokens=1_000_000,
                last_used_at=now - 300,
            ),
            AccountBucketState(
                account_id="acc_pro_2",
                email="pro.secondary@antigravity.internal",
                provider="google_pro",
                capacity_tokens=1_000_000,
                available_tokens=1_000_000,
                last_used_at=now - 600,
            ),
            AccountBucketState(
                account_id="acc_pro_3",
                email="pro.tertiary@antigravity.internal",
                provider="google_pro",
                capacity_tokens=1_000_000,
                available_tokens=1_000_000,
                last_used_at=now - 900,
            ),
            AccountBucketState(
                account_id="acc_pro_4",
                email="pro.quaternary@antigravity.internal",
                provider="google_pro",
                capacity_tokens=1_000_000,
                available_tokens=1_000_000,
                last_used_at=now - 1200,
            ),
        ]


# ============================================================================
# Quota Reservation Engine & Thundering-Herd Deflector
# ============================================================================

class QuotaReservationEngine:
    """
    Two-Phase Quota Reservation Engine with Decorrelated Jitter Deflection
    and Exponential Token Debt Amortization.
    """

    def __init__(
        self,
        db_path: Optional[Path] = None,
        state_dir: Optional[Path] = None,
        alpha_smoothing: float = 0.2,
        debt_window_sec: float = 60.0,
        t_base_jitter: float = 0.25,
        t_max_jitter: float = 8.0,
    ):
        self.alpha_smoothing = alpha_smoothing
        self.debt_window_sec = debt_window_sec
        self.t_base_jitter = t_base_jitter
        self.t_max_jitter = t_max_jitter

        self.account_store = CloudAccountStore(db_path)
        self._lock = threading.RLock()

        # Paths
        if state_dir is None:
            self.state_dir = Path("/home/ajf/automation/dashboards")
        else:
            self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)

        self.json_path = self.state_dir / "quota_reservation_state.json"
        self.digest_path = self.state_dir / "QUOTA_RESERVATION_DIGEST.md"

        # Internal collections
        self._buckets: Dict[str, AccountBucketState] = {}
        self._leases: Dict[str, QuotaReservationLease] = {}

        # Telemetry metrics
        self._contention_events: int = 0
        self._deflected_retries: int = 0
        self._total_leases_issued: int = 0
        self._total_leases_committed: int = 0
        self._total_leases_expired: int = 0
        self._total_leases_released: int = 0

        # Load initial accounts
        self.reload_accounts()

    def reload_accounts(self) -> None:
        """Loads or synchronizes accounts from the persistence store."""
        with self._lock:
            loaded = self.account_store.load_accounts()
            for acc in loaded:
                if acc.account_id not in self._buckets:
                    self._buckets[acc.account_id] = acc
                else:
                    existing = self._buckets[acc.account_id]
                    existing.capacity_tokens = acc.capacity_tokens
                    existing.email = acc.email
                    existing.provider = acc.provider
                    if acc.captcha_penalty > 0.0:
                        existing.captcha_penalty = acc.captcha_penalty
                    if acc.cooldown_until > existing.cooldown_until:
                        existing.cooldown_until = acc.cooldown_until

    # ------------------------------------------------------------------------
    # Score Arbitration Formula:
    # Score(acc_i) = [RemainingQuota(acc_i) / (ActiveLeases(acc_i) + 1)]
    #                * (1 - CaptchaPenalty(acc_i))
    #                * exp(-PenaltyCooldown(acc_i))
    # ------------------------------------------------------------------------
    def calculate_account_score(self, bucket: AccountBucketState, now: float) -> float:
        """Computes arbitration score for a candidate account bucket."""
        # Remaining net quota after subtracting current debt pool
        remaining_quota = max(0.0, float(bucket.available_tokens - bucket.debt_pool))
        if remaining_quota <= 0:
            return 0.0

        load_factor = float(bucket.active_leases_count + 1)
        base_capacity_score = remaining_quota / load_factor

        # Captcha factor [0.0, 1.0]
        captcha_factor = max(0.0, 1.0 - min(1.0, bucket.captcha_penalty))
        if captcha_factor <= 0.0:
            return 0.0

        # Penalty cooldown factor
        cooldown_penalty = 0.0
        if bucket.cooldown_until > now:
            # Normalized cooldown scale (10 seconds decay scale)
            cooldown_penalty = (bucket.cooldown_until - now) / 10.0

        cooldown_factor = math.exp(-cooldown_penalty)

        return base_capacity_score * captcha_factor * cooldown_factor

    def arbitrate_best_account(self, estimated_tokens: int) -> Optional[AccountBucketState]:
        """Selects optimal account acc* = argmax_{acc in A} Score(acc)."""
        now = time.time()
        best_account: Optional[AccountBucketState] = None
        best_score = -1.0

        for bucket in self._buckets.values():
            # Check availability constraints
            net_available = bucket.available_tokens - bucket.debt_pool
            if net_available < estimated_tokens:
                continue
            if bucket.cooldown_until > now:
                continue
            if bucket.status == AccountHealthStatus.CAPTCHA_BLOCKED:
                continue

            score = self.calculate_account_score(bucket, now)
            if score > best_score:
                best_score = score
                best_account = bucket

        return best_account

    # ------------------------------------------------------------------------
    # Thundering-Herd Deflector: Decorrelated Jittered Backoff
    # T_wait = min(T_max, Uniform(T_base, T_base * 3 * 2^{retries}))
    # ------------------------------------------------------------------------
    def compute_jitter_wait(self, retries: int) -> float:
        """Calculates decorrelated jittered backoff time for contested workers."""
        upper_bound = self.t_base_jitter * 3.0 * (2.0 ** min(retries, 6))
        chosen_wait = random.uniform(self.t_base_jitter, upper_bound)
        return min(self.t_max_jitter, chosen_wait)

    # ------------------------------------------------------------------------
    # Two-Phase Token Reservation State Machine: Acquire
    # ------------------------------------------------------------------------
    def acquire_lease(
        self,
        worker_id: str,
        repo_id: str,
        milestone_id: str,
        estimated_tokens: int = 4000,
        lease_ttl_sec: float = 300.0,
        max_retries: int = 3,
    ) -> Optional[QuotaReservationLease]:
        """
        Attempts to atomically reserve tokens and grant a lease.
        Deflects contention via decorrelated jitter backoff if initial pool is saturated.
        """
        retries = 0
        while retries <= max_retries:
            with self._lock:
                self.sweep_expired_leases()
                self._amortize_active_debts()

                acc = self.arbitrate_best_account(estimated_tokens)
                if acc is not None:
                    # Optimistic deduction
                    acc.available_tokens -= estimated_tokens
                    acc.active_leases_count += 1
                    acc.total_leases_granted += 1
                    acc.last_used_at = time.time()

                    # Generate lease nonce and ID
                    nonce = hashlib.sha256(
                        f"{worker_id}:{repo_id}:{milestone_id}:{time.time()}:{random.random()}".encode("utf-8")
                    ).hexdigest()[:12]
                    lease_id = f"lease_{acc.account_id}_{nonce}"

                    lease = QuotaReservationLease(
                        lease_id=lease_id,
                        account_id=acc.account_id,
                        worker_id=worker_id,
                        repo_id=repo_id,
                        milestone_id=milestone_id,
                        reserved_tokens=estimated_tokens,
                        actual_tokens=0,
                        state=LeaseState.RESERVED,
                        granted_at=time.time(),
                        expires_at=time.time() + lease_ttl_sec,
                        nonce=nonce,
                    )

                    self._leases[lease_id] = lease
                    self._total_leases_issued += 1
                    self._persist_state()
                    return lease

                # Contention occurred
                self._contention_events += 1

            # Outside lock: backoff with decorrelated jitter
            if retries < max_retries:
                wait_sec = self.compute_jitter_wait(retries)
                self._deflected_retries += 1
                time.sleep(wait_sec)
                retries += 1
            else:
                break

        self._persist_state()
        return None

    # ------------------------------------------------------------------------
    # Two-Phase Token Reservation State Machine: Commit
    # ------------------------------------------------------------------------
    def commit_lease(self, lease_id: str, actual_tokens: int) -> bool:
        """
        Finalizes lease execution.
        Applies Delta_tau adjustment, exponential smoothing, and token debt amortization.
        """
        with self._lock:
            lease = self._leases.get(lease_id)
            if not lease:
                return False

            if lease.state != LeaseState.RESERVED:
                return False

            acc = self._buckets.get(lease.account_id)
            now = time.time()
            lease.actual_tokens = actual_tokens
            lease.committed_at = now
            lease.state = LeaseState.COMMITTED
            self._total_leases_committed += 1

            if acc:
                acc.active_leases_count = max(0, acc.active_leases_count - 1)
                acc.total_tokens_consumed += actual_tokens
                acc.last_used_at = now

                # 1. Delta_tau = actual - estimated
                tau_estimated = lease.reserved_tokens
                tau_actual = actual_tokens
                delta_tau = tau_actual - tau_estimated

                # B_{acc} <- B_{acc} + (tau_estimated - tau_actual)
                acc.available_tokens += (tau_estimated - tau_actual)

                # 2. Sliding-window consumption rate model: R_hat_t = alpha*R_t + (1-alpha)*R_hat_{t-1}
                duration = max(1.0, now - lease.granted_at)
                r_instantaneous = float(tau_actual) / duration
                if acc.smoothed_rate <= 0.0:
                    acc.smoothed_rate = r_instantaneous
                else:
                    acc.smoothed_rate = (self.alpha_smoothing * r_instantaneous) + (
                        (1.0 - self.alpha_smoothing) * acc.smoothed_rate
                    )

                # 3. Excess token debt amortization if tau_actual > tau_estimated
                if delta_tau > 0:
                    kappa = 1.0 + (float(delta_tau) / max(1.0, float(tau_estimated)))
                    excess_debt = int(delta_tau * kappa)
                    acc.debt_pool += excess_debt
                    acc.debt_amortization_deadline = now + self.debt_window_sec

            self._persist_state()
            return True

    # ------------------------------------------------------------------------
    # Two-Phase Token Reservation State Machine: Release (Early Abort)
    # ------------------------------------------------------------------------
    def release_lease(self, lease_id: str, reason: str = "ABORTED") -> bool:
        """Restores reserved tokens on task cancellation or non-execution."""
        with self._lock:
            lease = self._leases.get(lease_id)
            if not lease:
                return False

            if lease.state != LeaseState.RESERVED:
                return False

            lease.state = LeaseState.RELEASED
            lease.metadata["release_reason"] = reason
            self._total_leases_released += 1

            acc = self._buckets.get(lease.account_id)
            if acc:
                acc.active_leases_count = max(0, acc.active_leases_count - 1)
                # Full refund of optimistically deducted tokens
                acc.available_tokens += lease.reserved_tokens

            self._persist_state()
            return True

    # ------------------------------------------------------------------------
    # Expiration Sweeper & Debt Amortizer
    # ------------------------------------------------------------------------
    def sweep_expired_leases(self) -> int:
        """Reclaims tokens from crashed or timed-out worker leases."""
        with self._lock:
            now = time.time()
            reclaimed_count = 0

            for lease in self._leases.values():
                if lease.state == LeaseState.RESERVED and now >= lease.expires_at:
                    lease.state = LeaseState.EXPIRED
                    reclaimed_count += 1
                    self._total_leases_expired += 1

                    acc = self._buckets.get(lease.account_id)
                    if acc:
                        acc.active_leases_count = max(0, acc.active_leases_count - 1)
                        acc.available_tokens += lease.reserved_tokens
                        # Set slight penalty cooldown for account due to worker timeout
                        acc.cooldown_until = max(acc.cooldown_until, now + 15.0)

            if reclaimed_count > 0:
                self._persist_state()

            return reclaimed_count

    def _amortize_active_debts(self) -> None:
        """Gradually decays debt pools across accounts as the debt window elapses."""
        now = time.time()
        for acc in self._buckets.values():
            if acc.debt_pool > 0:
                if now >= acc.debt_amortization_deadline:
                    acc.debt_pool = 0
                else:
                    # Linear fractional dissipation
                    time_left = acc.debt_amortization_deadline - now
                    decay_ratio = max(0.0, time_left / self.debt_window_sec)
                    acc.debt_pool = int(acc.debt_pool * decay_ratio)

    # ------------------------------------------------------------------------
    # Diagnostics & Hooks for execution_state_reasoner.py
    # ------------------------------------------------------------------------
    def get_reasoner_telemetry(self, repo_id: str, milestone_id: str) -> Dict[str, Any]:
        """
        Exports diagnostic reason tags and dynamic countdown timers
        for tasks/execution_state_reasoner.py.
        """
        with self._lock:
            now = time.time()
            matched_leases = [
                l for l in self._leases.values()
                if l.repo_id == repo_id and l.milestone_id == milestone_id and l.state == LeaseState.RESERVED
            ]

            if matched_leases:
                active_l = matched_leases[0]
                return {
                    "reason_tag": "QUOTA_LEASE_ACTIVE",
                    "lease_id": active_l.lease_id,
                    "account_id": active_l.account_id,
                    "remaining_ttl_sec": round(active_l.remaining_ttl_sec, 1),
                    "reserved_tokens": active_l.reserved_tokens,
                    "is_contended": False,
                }

            if self._contention_events > 0:
                return {
                    "reason_tag": "THUNDERING_HERD_DEFLECTED",
                    "lease_id": None,
                    "account_id": None,
                    "remaining_ttl_sec": 0.0,
                    "reserved_tokens": 0,
                    "is_contended": True,
                    "contention_count": self._contention_events,
                }

            return {
                "reason_tag": "QUOTA_IDLE",
                "lease_id": None,
                "account_id": None,
                "remaining_ttl_sec": 0.0,
                "reserved_tokens": 0,
                "is_contended": False,
            }

    def get_countdown_timer(self, lease_id: str) -> float:
        """Returns countdown seconds remaining for active lease."""
        with self._lock:
            lease = self._leases.get(lease_id)
            if lease and lease.state == LeaseState.RESERVED:
                return lease.remaining_ttl_sec
            return 0.0

    # ------------------------------------------------------------------------
    # Persistence: JSON Snapshot & Markdown Cockpit Digest
    # ------------------------------------------------------------------------
    def _persist_state(self) -> None:
        """Atomically outputs state JSON and Markdown cockpit digest."""
        now = time.time()
        state_data = {
            "timestamp": now,
            "metrics": {
                "total_leases_issued": self._total_leases_issued,
                "total_leases_committed": self._total_leases_committed,
                "total_leases_released": self._total_leases_released,
                "total_leases_expired": self._total_leases_expired,
                "contention_events": self._contention_events,
                "deflected_retries": self._deflected_retries,
            },
            "accounts": {acc_id: acc.to_dict() for acc_id, acc in self._buckets.items()},
            "active_leases": [
                l.to_dict() for l in self._leases.values() if l.state == LeaseState.RESERVED
            ],
            "recent_leases": [
                l.to_dict() for l in list(self._leases.values())[-20:]
            ],
        }

        # Write atomic JSON
        temp_json = self.json_path.with_suffix(".tmp")
        try:
            with open(temp_json, "w", encoding="utf-8") as f:
                json.dump(state_data, f, indent=2)
            temp_json.replace(self.json_path)
        except Exception:
            if temp_json.exists():
                temp_json.unlink()

        # Write Markdown Digest
        digest_lines = [
            "# Quota Reservation Lease Controller & Thundering-Herd Deflector",
            f"**Last Synchronized:** `{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(now))}`  ",
            f"**Total Issued:** `{self._total_leases_issued}` | "
            f"**Committed:** `{self._total_leases_committed}` | "
            f"**Expired:** `{self._total_leases_expired}` | "
            f"**Contention Deflected:** `{self._deflected_retries}`\n",
            "## 1. Cloud Model Accounts Pool (`A`)",
            "| Account ID | Email | Status | Available Tokens | Debt Pool | Active Leases | Burn Rate (tok/s) | Score |",
            "| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: |",
        ]

        for acc in self._buckets.values():
            score = self.calculate_account_score(acc, now)
            digest_lines.append(
                f"| `{acc.account_id}` | {acc.email} | `{acc.status.value}` | "
                f"{acc.available_tokens:,} / {acc.capacity_tokens:,} | "
                f"{acc.debt_pool:,} | {acc.active_leases_count} | "
                f"{acc.smoothed_rate:.2f} | **{score:.1f}** |"
            )

        digest_lines.append("\n## 2. Active Reservation Leases")
        active_list = [l for l in self._leases.values() if l.state == LeaseState.RESERVED]
        if not active_list:
            digest_lines.append("*No active quota reservation leases currently open.*")
        else:
            digest_lines.append(
                "| Lease ID | Worker ID | Repository | Milestone | Reserved | Remaining TTL | Account |",
            )
            digest_lines.append(
                "| :--- | :--- | :--- | :--- | :---: | :---: | :--- |",
            )
            for l in active_list:
                digest_lines.append(
                    f"| `{l.lease_id}` | `{l.worker_id}` | `{l.repo_id}` | "
                    f"`{l.milestone_id}` | {l.reserved_tokens:,} | "
                    f"`{l.remaining_ttl_sec:.1f}s` | `{l.account_id}` |"
                )

        digest_lines.append("\n## 3. Thundering-Herd Deflector Diagnostics")
        digest_lines.append(
            f"- **Contention Events:** `{self._contention_events}`\n"
            f"- **Jitter Retries Absorbed:** `{self._deflected_retries}`\n"
            f"- **Base Backoff Window (T_base):** `{self.t_base_jitter}s` (Max `{self.t_max_jitter}s`)\n"
            f"- **Exponential Smoothing Alpha (α):** `{self.alpha_smoothing}`\n"
            f"- **Debt Amortization Window (W_debt):** `{self.debt_window_sec}s`\n"
        )

        temp_digest = self.digest_path.with_suffix(".tmp")
        try:
            with open(temp_digest, "w", encoding="utf-8") as f:
                f.write("\n".join(digest_lines) + "\n")
            temp_digest.replace(self.digest_path)
        except Exception:
            if temp_digest.exists():
                temp_digest.unlink()


# ============================================================================
# Demonstration & Self-Test Verification Suite
# ============================================================================

def run_self_test() -> bool:
    """Executes complete in-process self-tests verifying all operational guarantees."""
    print("=" * 78)
    print("RUNNING QUOTA RESERVATION LEASE CONTROLLER SELF-TEST SUITE")
    print("=" * 78)

    test_dir = Path("/tmp/quota_lease_test")
    test_dir.mkdir(parents=True, exist_ok=True)
    engine = QuotaReservationEngine(
        state_dir=test_dir,
        alpha_smoothing=0.2,
        debt_window_sec=2.0,
        t_base_jitter=0.05,
        t_max_jitter=0.5,
    )

    # 1. Verification of Account Arbitration & Lease Acquisition
    print("[TEST 1] Acquiring lease on highest-scored account...")
    lease1 = engine.acquire_lease(
        worker_id="worker_alpha",
        repo_id="hermes_orchestrator",
        milestone_id="M_QUOTA_001",
        estimated_tokens=5000,
        lease_ttl_sec=10.0,
    )
    assert lease1 is not None, "Failed to acquire lease1"
    assert lease1.state == LeaseState.RESERVED
    assert lease1.reserved_tokens == 5000
    acc1 = engine._buckets[lease1.account_id]
    assert acc1.available_tokens == 1_000_000 - 5000
    assert acc1.active_leases_count == 1
    print(f" -> Granted lease {lease1.lease_id} on {lease1.account_id} with {acc1.available_tokens} tokens remaining.")

    # 2. Verification of Commit with Under-Consumption (Refund Delta)
    print("[TEST 2] Committing lease with under-consumption (3,500 tokens used vs 5,000 reserved)...")
    success = engine.commit_lease(lease1.lease_id, actual_tokens=3500)
    assert success, "Commit lease failed"
    assert lease1.state == LeaseState.COMMITTED
    assert acc1.available_tokens == 1_000_000 - 3500
    assert acc1.active_leases_count == 0
    assert acc1.smoothed_rate > 0.0
    print(f" -> Successfully refunded 1,500 tokens. Bucket available: {acc1.available_tokens:,}, Rate: {acc1.smoothed_rate:.2f} tok/s.")

    # 3. Verification of Commit with Over-Consumption & Token Debt Amortization
    print("[TEST 3] Over-consumption debt amortization test (6,000 used vs 4,000 reserved)...")
    lease2 = engine.acquire_lease("worker_beta", "hermes_orchestrator", "M_QUOTA_002", estimated_tokens=4000)
    assert lease2 is not None
    acc2 = engine._buckets[lease2.account_id]
    engine.commit_lease(lease2.lease_id, actual_tokens=6000)
    # Excess = 2000, kappa = 1 + (2000/4000) = 1.5. Debt pool = 2000 * 1.5 = 3000
    assert acc2.debt_pool >= 3000, f"Expected debt pool >= 3000, got {acc2.debt_pool}"
    print(f" -> Token debt pool created: {acc2.debt_pool:,} tokens amortizing across W_debt.")

    # 4. Verification of Lease Release (Task Cancellation)
    print("[TEST 4] Early task cancellation and token release...")
    lease3 = engine.acquire_lease("worker_gamma", "hermes_orchestrator", "M_QUOTA_003", estimated_tokens=10000)
    assert lease3 is not None
    acc3 = engine._buckets[lease3.account_id]
    tokens_before = acc3.available_tokens
    engine.release_lease(lease3.lease_id, reason="CANCELLED_BY_SCHEDULER")
    assert lease3.state == LeaseState.RELEASED
    assert acc3.available_tokens == tokens_before + 10000
    print(" -> Released lease refunded 10,000 reserved tokens completely.")

    # 5. Verification of Sweeper Reclaiming Expired Leases
    print("[TEST 5] Expired lease reclamation sweep...")
    lease4 = engine.acquire_lease("worker_timeout", "hermes_orchestrator", "M_QUOTA_004", estimated_tokens=8000, lease_ttl_sec=0.1)
    assert lease4 is not None
    time.sleep(0.2)
    reclaimed = engine.sweep_expired_leases()
    assert reclaimed >= 1, "Failed to reclaim expired lease"
    assert lease4.state == LeaseState.EXPIRED
    print(f" -> Successfully swept and reclaimed {reclaimed} expired leases.")

    # 6. Verification of Thundering-Herd Deflection (Multi-Threaded Contention)
    print("[TEST 6] Testing concurrent thundering-herd contention deflection (16 parallel workers)...")
    workers_count = 16
    successful_leases: List[QuotaReservationLease] = []
    threads: List[threading.Thread] = []

    def worker_task(w_idx: int):
        l = engine.acquire_lease(
            worker_id=f"concurrent_worker_{w_idx}",
            repo_id="hermes_orchestrator",
            milestone_id=f"MILESTONE_CONCURRENT_{w_idx % 4}",
            estimated_tokens=50_000,
            lease_ttl_sec=5.0,
            max_retries=4,
        )
        if l:
            successful_leases.append(l)
            time.sleep(0.05)
            engine.commit_lease(l.lease_id, actual_tokens=48_000)

    for i in range(workers_count):
        t = threading.Thread(target=worker_task, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print(f" -> Deflected retries absorbed: {engine._deflected_retries}")
    print(f" -> Total concurrent leases satisfied: {len(successful_leases)} / {workers_count}")
    assert len(successful_leases) == workers_count, "All workers should be satisfied via jittered retries"

    # 7. Verification of Reasoner Hooks
    telemetry = engine.get_reasoner_telemetry("hermes_orchestrator", "M_QUOTA_001")
    assert "reason_tag" in telemetry
    print(f" -> Reasoner hook telemetry verified: {telemetry['reason_tag']}")

    # 8. Verification of File Persistence
    assert engine.json_path.exists(), "State JSON was not persisted"
    assert engine.digest_path.exists(), "Digest Markdown was not persisted"
    print(f" -> Artifacts successfully verified at {test_dir}")

    print("=" * 78)
    print("ALL TEST SUITE GUARANTEES SATISFIED (100% PASS)")
    print("=" * 78)
    return True


# ============================================================================
# CLI Command Dispatcher
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Pipeline Autonomic Multi-Account Quota Reservation Lease Controller & Thundering-Herd Deflector"
    )
    parser.add_argument("--status", action="store_true", help="Print active accounts, leases, and deflection metrics")
    parser.add_argument("--reserve", action="store_true", help="Acquire a quota reservation lease")
    parser.add_argument("--commit", action="store_true", help="Commit an existing lease with actual token count")
    parser.add_argument("--release", action="store_true", help="Release an active lease without token consumption")
    parser.add_argument("--sweep", action="store_true", help="Trigger an immediate sweep of expired leases")
    parser.add_argument("--run-demo", action="store_true", help="Run full self-test verification harness")

    parser.add_argument("--worker", type=str, default="cli_worker", help="Worker ID for reservation")
    parser.add_argument("--repo", type=str, default="hermes_orchestrator", help="Repository ID")
    parser.add_argument("--milestone", type=str, default="M_EXEC_001", help="Milestone ID")
    parser.add_argument("--tokens", type=int, default=4000, help="Estimated tokens for reservation")
    parser.add_argument("--ttl", type=float, default=300.0, help="Lease TTL in seconds")
    parser.add_argument("--lease-id", type=str, default="", help="Lease ID for commit/release operations")
    parser.add_argument("--actual-tokens", type=int, default=0, help="Actual tokens consumed on commit")

    args = parser.parse_args()

    engine = QuotaReservationEngine()

    if args.run_demo:
        run_self_test()
        sys.exit(0)

    if args.sweep:
        reclaimed = engine.sweep_expired_leases()
        print(f"[SWEEP] Reclaimed {reclaimed} expired leases.")
        sys.exit(0)

    if args.reserve:
        lease = engine.acquire_lease(
            worker_id=args.worker,
            repo_id=args.repo,
            milestone_id=args.milestone,
            estimated_tokens=args.tokens,
            lease_ttl_sec=args.ttl,
        )
        if lease:
            print(f"[RESERVE GRANTED] Lease ID: {lease.lease_id}")
            print(f"  Account: {lease.account_id}")
            print(f"  Tokens:  {lease.reserved_tokens}")
            print(f"  Expires: {lease.remaining_ttl_sec:.1f}s")
            print(f"  Nonce:   {lease.nonce}")
        else:
            print("[RESERVE REJECTED] Quota unavailable or accounts saturated.")
            sys.exit(1)
        sys.exit(0)

    if args.commit:
        if not args.lease_id:
            print("Error: --lease-id is required for commit.")
            sys.exit(1)
        actual = args.actual_tokens if args.actual_tokens > 0 else args.tokens
        success = engine.commit_lease(args.lease_id, actual_tokens=actual)
        if success:
            print(f"[COMMIT SUCCESS] Lease {args.lease_id} committed with {actual} tokens.")
        else:
            print(f"[COMMIT FAILED] Lease {args.lease_id} not found or not in RESERVED state.")
            sys.exit(1)
        sys.exit(0)

    if args.release:
        if not args.lease_id:
            print("Error: --lease-id is required for release.")
            sys.exit(1)
        success = engine.release_lease(args.lease_id, reason="CLI_MANUAL_RELEASE")
        if success:
            print(f"[RELEASE SUCCESS] Lease {args.lease_id} released and quota restored.")
        else:
            print(f"[RELEASE FAILED] Lease {args.lease_id} not found or not in RESERVED state.")
            sys.exit(1)
        sys.exit(0)

    # Default: print status
    now = time.time()
    print("=" * 78)
    print("PIPELINE QUOTA RESERVATION LEASE CONTROLLER COCKPIT")
    print("=" * 78)
    print(f"Contention Events Deflected: {engine._contention_events}")
    print(f"Jitter Retries Absorbed:     {engine._deflected_retries}")
    print(f"Total Leases (Issued/Committed/Expired): {engine._total_leases_issued} / {engine._total_leases_committed} / {engine._total_leases_expired}\n")
    print("Active Account Buckets:")
    for acc in engine._buckets.values():
        score = engine.calculate_account_score(acc, now)
        print(f"  [{acc.account_id}] {acc.email:32} | {acc.status.value:12} | Rem: {acc.available_tokens:,}/{acc.capacity_tokens:,} | Debt: {acc.debt_pool:,} | Score: {score:.1f}")

    active_leases = [l for l in engine._leases.values() if l.state == LeaseState.RESERVED]
    print(f"\nActive Leases Count: {len(active_leases)}")
    for l in active_leases:
        print(f"  [{l.lease_id}] Worker: {l.worker_id} | Repo: {l.repo_id} | Milestone: {l.milestone_id} | TTL: {l.remaining_ttl_sec:.1f}s")
    print("=" * 78)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        run_self_test()
    else:
        main()