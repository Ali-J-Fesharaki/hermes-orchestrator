#!/usr/bin/env python3
"""
adaptive_rate_governor.py: Empirical Hill-Climbing & Sensitive Boundary Token Flow Governor.

Key Architectural Capabilities:
  1. Empirical Stagger Probing (Trial & Error):
     - Minimum stagger floor allowed down to 0.6s (previously 3.5s).
     - Learns the optimal inter-agent dispatch delay by continuously probing lower delays.
     - Tracks `empirical_danger_threshold` whenever an HTTP 429 boundary is encountered.

  2. Sensitive Non-Linear Delta Scaling Near Critical Thresholds (اعداد حساس):
     - When stagger > 2.5s: steps down by 0.30s per clean call.
     - When 1.6s < stagger <= 2.5s: steps down by 0.15s per clean call.
     - When 1.0s <= stagger <= 1.6s (near 1s sensitive boundary): steps down by 0.06s - 0.08s.
     - When stagger < 1.0s (sub-second frontier): micro-steps down by 0.02s - 0.04s (<= 0.05s).
     - Sensitive backoff: If a 429 occurs near 1.0s, backs off gently (+0.3s to +0.45s) and sets
       a protective floor, avoiding blunt, wild oscillations.

  3. Dynamic Output Token Ceiling Scaling:
     - Scales `max_tokens` up to 8,192 in EXPEDITE mode with continuous success streaks.
     - Dynamically shaped according to account quota depletion:
       * EXPEDITE (Quota >= 75%): 6,000 base -> climbs to 8,192 via success streak rewards.
       * BALANCED (Quota 45%-75%): 4,500 base -> climbs to 6,000.
       * CONSERVE (Quota 20%-45%): 3,200 tokens.
       * ULTRA_LEAN (Quota < 20%): 2,000 tokens.
       * PENALTY_LOCKDOWN: 1,400 tokens.

  4. Proportional Dynamic Jitter:
     - Jitter scales with dispatch delay (e.g. 0.04s - 0.15s near 1.0s) so fast pacing is not delayed.
"""

import os
import sys
import json
import time
import sqlite3
import random
import threading
from pathlib import Path
from datetime import datetime

AUTO_DIR = Path(__file__).resolve().parent.parent
DASHBOARDS_DIR = AUTO_DIR / "dashboards"
GOVERNOR_STATE_FILE = DASHBOARDS_DIR / "rate_governor_state.json"
ACCOUNTS_DB = Path.home() / ".antigravity-agent" / "cloud_accounts.db"

_GOVERNOR_LOCK = threading.Lock()


class AdaptiveRateGovernor:
    """
    Empirical Hill-Climbing & Sensitive Boundary Token Flow Governor.
    Adapts stagger pacing and max_tokens via continuous trial-and-error learning.
    """

    def __init__(self):
        # Dispatch Pacing Bounds
        self.absolute_min_stagger = 0.6      # Minimum physically permissible stagger (seconds)
        self.max_stagger_sec = 10.0          # Upper bound clamp on severe backoff
        self.current_stagger_sec = 1.6       # Initial / default stagger
        self.empirical_danger_threshold = 0.5# Learned lowest point where 429 occurred
        self.max_concurrency_ceiling = 3
        self.current_concurrency = 3

        # Reinforcement & Learning State
        self.penalty_score = 0.0             # Increases on 429, decays on success
        self.reward_score = 0.0              # Increases on clean streaks
        self.consecutive_successes = 0        # Success streak count
        self.total_429_incidents = 0         # Historical 429 count
        self.last_429_timestamp = 0.0        # Epoch of last 429
        self.active_429_cooldown_until = 0.0
        self.last_dispatch_ts = 0.0

        self._load_state()

    def _load_state(self):
        if GOVERNOR_STATE_FILE.exists():
            try:
                data = json.loads(GOVERNOR_STATE_FILE.read_text(encoding="utf-8"))
                loaded_stagger = float(data.get("stagger_delay_sec", 1.6))
                # If old version had large stagger (>3.0s) and clean streak, seed with responsive 1.6s
                if loaded_stagger >= 3.0 and int(data.get("consecutive_successes", 0)) > 5:
                    self.current_stagger_sec = 1.6
                else:
                    self.current_stagger_sec = max(self.absolute_min_stagger, loaded_stagger)
                    
                self.current_concurrency = int(data.get("current_concurrency", data.get("safe_concurrency", 3)))
                self.penalty_score = float(data.get("penalty_score", 0.0))
                self.reward_score = float(data.get("reward_score", 0.0))
                self.consecutive_successes = int(data.get("consecutive_successes", 0))
                self.total_429_incidents = int(data.get("total_429_incidents", 0))
                self.active_429_cooldown_until = float(data.get("cooldown_until", 0.0))
                self.empirical_danger_threshold = float(data.get("empirical_danger_threshold", 0.5))
            except Exception:
                pass

    def get_healthy_account_metrics(self, model_id: str = "gemini-3.8-flash-tiered") -> dict:
        """
        Reads cloud_accounts.db to determine:
          - Number of healthy, active, unblocked accounts.
          - Lowest quota percentage remaining for this model.
          - Average quota percentage remaining.
        """
        if not ACCOUNTS_DB.exists():
            return {"healthy_count": 1, "min_quota_pct": 100.0, "avg_quota_pct": 100.0}

        try:
            con = sqlite3.connect(str(ACCOUNTS_DB), timeout=3)
            cur = con.cursor()

            # 1. Fetch active lockouts from settings
            cur.execute('SELECT value FROM settings WHERE key="proxy_model_availability.v1"')
            row = cur.fetchone()
            locked_account_ids = set()
            now_ms = time.time() * 1000.0
            if row and row[0]:
                try:
                    for l in json.loads(row[0]):
                        if l.get("modelId") == model_id and l.get("unavailableUntil", 0) > now_ms:
                            locked_account_ids.add(l.get("accountId"))
                except Exception:
                    pass

            # 2. Fetch active accounts
            cur.execute("SELECT id, status, health_json, quota_json FROM accounts WHERE status='active'")
            healthy_quotas = []
            for acc_id, status, h_json, q_json in cur.fetchall():
                if acc_id in locked_account_ids:
                    continue
                if h_json:
                    try:
                        h = json.loads(h_json)
                        if h.get("validation", {}).get("status") == "requires_action":
                            continue  # Verification blocked
                    except Exception:
                        pass
                if q_json:
                    try:
                        q = json.loads(q_json)
                        pct = q.get("models", {}).get(model_id, {}).get("percentage", 100)
                        if pct > 0:
                            healthy_quotas.append(float(pct))
                    except Exception:
                        pass

            con.close()
            healthy_count = len(healthy_quotas)
            min_quota = min(healthy_quotas) if healthy_quotas else 0.0
            avg_quota = (sum(healthy_quotas) / len(healthy_quotas)) if healthy_quotas else 0.0

            return {
                "healthy_count": max(1, healthy_count),
                "min_quota_pct": round(min_quota, 1),
                "avg_quota_pct": round(avg_quota, 1)
            }
        except Exception:
            return {"healthy_count": 1, "min_quota_pct": 100.0, "avg_quota_pct": 100.0}

    def compute_policy(self, model_id: str = "gemini-3.8-flash-tiered") -> dict:
        """
        Dynamically computes the active Token Flow Policy based on:
          1. 429 Penalty score and active cooldowns.
          2. Positive reinforcement streak and dynamic token scaling (up to 8,192).
          3. Live lowest account quota percentage and learned empirical stagger.
        """
        metrics = self.get_healthy_account_metrics(model_id)
        healthy_count = metrics["healthy_count"]
        min_quota = metrics["min_quota_pct"]
        now = time.time()

        with _GOVERNOR_LOCK:
            in_cooldown = now < self.active_429_cooldown_until
            cooldown_rem = max(0, int(self.active_429_cooldown_until - now))

            # Tier 1: PENALTY_LOCKDOWN (Active 429 backpressure cooldown or heavy penalty)
            if in_cooldown or self.penalty_score >= 6.0:
                tier = "PENALTY_LOCKDOWN"
                max_tokens = 1400
                concurrency = 1
                directive = (
                    "CRITICAL 429 BACKPRESSURE LOCKDOWN: Quota exhausted or rate limited. "
                    "Output ONLY minimal production logic. Strictly omit all introductory text, conversational pleasantries, and redundant comments."
                )
                details = f"Active 429 cooldown ({cooldown_rem}s remaining). Heavy penalty ({self.penalty_score:.1f} pts). Clamped to 1 worker."

            # Tier 2: ULTRA_LEAN_SURVIVAL (Quota critically low <20% or elevated penalty)
            elif min_quota < 20.0 or self.penalty_score >= 3.0:
                tier = "ULTRA_LEAN_SURVIVAL"
                max_tokens = 2000
                concurrency = 1
                directive = (
                    "LEAN SURVIVAL MODE: Low remaining account quota detected (<20%). "
                    "Implement the complete technical solution with high information density. Omit superfluous explanations to conserve token quota."
                )
                details = f"Low account quota ({min_quota}%). Concurrency clamped to 1, max_tokens tightened to 2000."

            # Tier 3: CONSERVE_ADAPTIVE (Quota moderate 20%-45% or mild penalty)
            elif min_quota < 45.0 or self.penalty_score >= 1.0:
                tier = "CONSERVE_ADAPTIVE"
                max_tokens = 3200
                concurrency = min(2, healthy_count)
                directive = (
                    "CONSERVATION MODE: Moderate quota remaining (20%-45%). "
                    "Write clear, modular, and mathematically rigorous code with compact structure."
                )
                details = f"Moderate quota ({min_quota}%). Balanced concurrency ({concurrency} workers), max_tokens capped at 3200."

            # Tier 4: BALANCED_OPTIMAL (Quota healthy 45%-75%, clean operation)
            elif min_quota < 75.0 or self.consecutive_successes < 4:
                tier = "BALANCED_OPTIMAL"
                # Dynamic token climb: 4500 base + 300 per streak milestone up to 6000
                climb = min(1500, (self.consecutive_successes // 2) * 300)
                max_tokens = 4500 + climb
                concurrency = min(2, healthy_count)
                directive = "BALANCED FLOW: Standard high-quality technical implementation."
                details = f"Healthy flow ({min_quota}% quota, streak: {self.consecutive_successes}). Max tokens {max_tokens}. Paced at {self.current_stagger_sec:.2f}s."

            # Tier 5: EXPEDITE_HIGH (High quota >=75%, sustained 0-429 streak, 0 penalty)
            else:
                tier = "EXPEDITE_HIGH"
                # Dynamic token climb: 6000 base + 400 per 2 successes up to 8192!
                climb = min(2192, (self.consecutive_successes // 2) * 400)
                max_tokens = 6000 + climb
                concurrency = min(self.max_concurrency_ceiling, healthy_count)
                directive = "EXPEDITE MODE: Abundant quota and clean rate headroom. Full architectural synthesis permitted."
                details = f"Peak trial-and-error throughput ({min_quota}% quota, streak: {self.consecutive_successes}). Max tokens {max_tokens} (up to 8,192). Inter-agent delay: {self.current_stagger_sec:.2f}s."

            self.current_concurrency = concurrency

            policy = {
                "tier": tier,
                "max_tokens": max_tokens,
                "safe_concurrency": concurrency,
                "stagger_delay_sec": round(self.current_stagger_sec, 2),
                "empirical_danger_threshold": round(self.empirical_danger_threshold, 2),
                "directive": directive,
                "details": details,
                "penalty_score": round(self.penalty_score, 2),
                "reward_score": round(self.reward_score, 1),
                "consecutive_successes": self.consecutive_successes,
                "total_429_incidents": self.total_429_incidents,
                "min_quota_pct": min_quota,
                "avg_quota_pct": metrics["avg_quota_pct"],
                "healthy_accounts": healthy_count,
                "cooldown_remaining_sec": cooldown_rem,
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

            self._save_state_dict(policy)
            return policy

    def _save_state_dict(self, policy: dict):
        try:
            DASHBOARDS_DIR.mkdir(parents=True, exist_ok=True)
            tmp = GOVERNOR_STATE_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(policy, indent=2, ensure_ascii=False), encoding="utf-8")
            tmp.replace(GOVERNOR_STATE_FILE)
        except Exception:
            pass

    def compute_safe_concurrency(self, requested_max: int = 3, model_id: str = "gemini-3.8-flash-tiered") -> int:
        policy = self.compute_policy(model_id)
        return min(requested_max, policy.get("safe_concurrency", 2))

    def acquire_dispatch_slot(self, worker_index: int = 0):
        """
        Enforces tight, jittered pacing between dispatched workers.
        Paces requests based on empirical, trial-and-error learned stagger interval.
        Uses proportional micro-jitter (0.04s - 0.15s near 1s) to avoid artificial delays.
        """
        with _GOVERNOR_LOCK:
            now = time.time()
            if now < self.active_429_cooldown_until:
                wait_sec = self.active_429_cooldown_until - now
                print(f"[🛑 RATE GOVERNOR] Waiting out active 429 backpressure cooldown ({int(wait_sec)}s)...")
                time.sleep(wait_sec)
                now = time.time()

            # Proportional micro-jitter: scales with current stagger (never blunts fast dispatch)
            max_jitter = min(0.20, max(0.04, self.current_stagger_sec * 0.12))
            jitter = random.uniform(0.03, max_jitter)
            time_since_last = now - self.last_dispatch_ts
            target_delay = (worker_index * self.current_stagger_sec) + jitter

            if time_since_last < target_delay:
                needed_sleep = target_delay - time_since_last
                time.sleep(needed_sleep)

            self.last_dispatch_ts = time.time()

    def record_429_incident(self, model_id: str, error_message: str = "", retry_after_sec: int = 20):
        """
        PENALTY & EMPIRICAL BOUNDARY LEARNING:
          - Marks `empirical_danger_threshold` at the current stagger boundary.
          - If failure occurred near sensitive low-delay zone (stagger <= 1.5s):
            Applies fine-grained, localized backoff (+0.35s to +0.45s) instead of wildly jumping.
          - Sets a temporary safety margin above the failed threshold.
          - If repeated 429s accumulate, escalates backoff proportionally.
        """
        with _GOVERNOR_LOCK:
            cooldown = retry_after_sec
            if "retry after" in error_message.lower():
                try:
                    part = error_message.lower().split("retry after")[1].split()[0].replace("s", "")
                    cooldown = max(10, min(60, int(part)))
                except Exception:
                    cooldown = 20

            now = time.time()
            self.active_429_cooldown_until = now + cooldown
            self.last_429_timestamp = now
            self.total_429_incidents += 1
            self.consecutive_successes = 0

            # Record empirical danger threshold
            self.empirical_danger_threshold = max(self.empirical_danger_threshold, self.current_stagger_sec)

            # Sensitive Backoff (Near sensitive 1s boundary, back off with small fine-grained delta)
            if self.current_stagger_sec <= 1.5:
                # Localized sensitive backoff: add 0.35s to 0.45s and protect above danger point
                self.current_stagger_sec = round(max(self.current_stagger_sec + 0.35, self.empirical_danger_threshold + 0.20), 2)
                self.penalty_score = min(10.0, self.penalty_score + 1.5)
            else:
                # Moderate/High delay backoff
                self.current_stagger_sec = round(min(self.max_stagger_sec, self.current_stagger_sec * 1.35), 2)
                self.penalty_score = min(10.0, self.penalty_score + 2.5)

            if self.penalty_score >= 5.0:
                self.current_concurrency = 1

            details = (
                f"HTTP 429 on '{model_id}'. Empirical boundary marked at {self.empirical_danger_threshold:.2f}s. "
                f"Sensitive backoff -> new stagger: {self.current_stagger_sec:.2f}s. Cooldown {cooldown}s."
            )
            print(f"\n[🛑 RATE GOVERNOR BOUNDARY HIT] {details}")

        self.compute_policy(model_id)

    def record_success(self, tokens_used: int = 0):
        """
        REWARD & EMPIRICAL EXPLORATION (Fine-grained delta near sensitive thresholds):
          - Increments consecutive success streak and reward points.
          - Exponentially decays penalty score (*= 0.82).
          - Non-linear adaptive step size for stagger reduction:
              * stagger > 2.5s:        decrement by 0.30s
              * 1.6s < stagger <= 2.5s: decrement by 0.15s
              * 1.0s <= stagger <= 1.6s: decrement by 0.06s (sensitive boundary!)
              * stagger < 1.0s:        decrement by 0.03s (sub-second micro-step!)
          - Respects empirical danger margin (threshold + 0.10s) unless long clean streak proves recovery.
        """
        with _GOVERNOR_LOCK:
            self.consecutive_successes += 1
            self.reward_score += 1.0

            # Exponential Penalty Decay on success
            if self.penalty_score > 0:
                self.penalty_score = max(0.0, round(self.penalty_score * 0.82, 2))

            # Milestone Streak Bonus
            if self.consecutive_successes % 5 == 0:
                self.reward_score += 5.0
                # After 10 consecutive clean calls, slightly lower the empirical danger threshold to allow re-probing
                if self.consecutive_successes % 10 == 0 and self.empirical_danger_threshold > self.absolute_min_stagger:
                    self.empirical_danger_threshold = max(self.absolute_min_stagger, round(self.empirical_danger_threshold - 0.05, 2))

            # Calculate adaptive delta based on proximity to sensitive 1-second boundary
            curr = self.current_stagger_sec
            if curr > 2.5:
                delta = 0.30
            elif curr > 1.6:
                delta = 0.15
            elif curr >= 1.0:
                delta = 0.06   # Sensitive zone (<= 0.1s as requested)
            else:
                delta = 0.03   # Sub-second frontier micro-step

            # Calculate safe probing floor
            # When danger threshold exists, protect with a slight margin until streak >= 8
            if self.consecutive_successes >= 8:
                safe_floor = self.absolute_min_stagger
            else:
                safe_floor = max(self.absolute_min_stagger, self.empirical_danger_threshold + 0.10)

            # Apply relaxation towards minimum floor
            if curr > safe_floor:
                self.current_stagger_sec = max(safe_floor, round(curr - delta, 3))

        self.compute_policy()


# Global Singleton Instance
_GLOBAL_GOVERNOR = AdaptiveRateGovernor()

def get_governor() -> AdaptiveRateGovernor:
    return _GLOBAL_GOVERNOR


if __name__ == "__main__":
    gov = get_governor()
    policy = gov.compute_policy()
    print("=" * 75)
    print(" 🚦 EMPIRICAL HILL-CLIMBING & SENSITIVE BOUNDARY TOKEN FLOW GOVERNOR")
    print("=" * 75)
    print(f"• Active Policy Tier:         [{policy['tier']}]")
    print(f"• Allocated max_tokens:       {policy['max_tokens']:,} tokens (Scale Ceiling: 8,192)")
    print(f"• Inter-Agent Stagger Delay:  {policy['stagger_delay_sec']}s (Probing floor: {gov.absolute_min_stagger}s)")
    print(f"• Empirical 429 Boundary:     {policy['empirical_danger_threshold']}s")
    print(f"• Safe Concurrency:           {policy['safe_concurrency']} workers")
    print(f"• Penalty vs Reward:          {policy['penalty_score']} pts / +{policy['reward_score']} pts")
    print(f"• Consecutive Clean Streak:   {policy['consecutive_successes']} calls")
    print(f"• Lowest Account Quota:       {policy['min_quota_pct']}%")
    print(f"• Active 429 Cooldown:        {policy['cooldown_remaining_sec']}s remaining")
    print(f"• Policy Details:             {policy['details']}")
    print("=" * 75)
