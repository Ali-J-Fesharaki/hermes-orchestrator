"""
tasks/quota_arbitrage_engine.py
Hermes Orchestrator - Cross-Account Quota Arbitrage & Dynamic Token Rebalancer

Implements multi-account token budgeting, cost estimation, risk-adjusted assignment
('budget quarantine' for high-risk work, headroom maximization for safe work),
in-memory batch assignment to prevent double-booking, consumption logging, and
rebalance analytics.
"""

from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone, timedelta
import json
import logging
import math
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class AccountBudget:
    """
    Represents the token quota and request budget state for a single API account.
    """
    account_id: str
    total_tokens: int
    consumed_tokens: int
    reset_timestamp: str  # ISO 8601 string
    requests_remaining: int
    requests_total: int

    @property
    def remaining_tokens(self) -> int:
        """Returns remaining usable tokens for this account."""
        return max(0, self.total_tokens - self.consumed_tokens)

    @property
    def utilization_ratio(self) -> float:
        """
        Returns fraction of quota consumed (0.0 to 1.0).
        Guards against division by zero.
        """
        if self.total_tokens <= 0:
            return 0.0
        return min(1.0, max(0.0, self.consumed_tokens / self.total_tokens))


@dataclass
class ArbitrageDecision:
    """
    Represents the outcome of a quota arbitrage assignment for a milestone.
    """
    milestone_id: str
    assigned_account: str
    predicted_cost_tokens: int
    account_remaining_before: int
    headroom_ratio: float
    risk_adjusted: bool
    reasoning: str


class QuotaArbitrageEngine:
    """
    Cross-account token arbitrage and dynamic load rebalancer across multi-tenant API accounts.
    """

    DEFAULT_COST_TOKENS: int = 8000
    SAFETY_MARGIN: float = 1.15  # 15% safety headroom requirement

    def __init__(
        self,
        accounts_config_path: str = "config/accounts.json",
        capacity_forecaster: Optional[Any] = None,
        policy_gate: Optional[Any] = None,
        consumption_state_path: str = "dashboards/quota_consumption.json",
        arbitrage_log_path: str = "dashboards/QUOTA_ARBITRAGE_LOG.md",
    ) -> None:
        self.accounts_config_path = Path(accounts_config_path)
        self.consumption_state_path = Path(consumption_state_path)
        self.arbitrage_log_path = Path(arbitrage_log_path)
        self.capacity_forecaster = capacity_forecaster
        self.policy_gate = policy_gate

        self.logger = logging.getLogger("QuotaArbitrageEngine")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter("[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s")
            )
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

        self._ensure_directories()

    def _ensure_directories(self) -> None:
        """Ensures that required directories and dashboard files exist."""
        self.consumption_state_path.parent.mkdir(parents=True, exist_ok=True)
        self.arbitrage_log_path.parent.mkdir(parents=True, exist_ok=True)
        self.accounts_config_path.parent.mkdir(parents=True, exist_ok=True)

        if not self.arbitrage_log_path.exists():
            header = (
                "# Quota Arbitrage Log\n\n"
                "| Timestamp | Milestone | Account | Predicted | Actual | Drift | Headroom After |\n"
                "|---|---|---|---|---|---|---|\n"
            )
            self.arbitrage_log_path.write_text(header, encoding="utf-8")

    def _parse_timestamp(self, ts_str: str) -> float:
        """Parses ISO timestamp string to epoch seconds with UTC fallback."""
        if not ts_str:
            return 0.0
        try:
            cleaned = ts_str.replace("Z", "+00:00")
            return datetime.fromisoformat(cleaned).timestamp()
        except Exception:
            return 0.0

    def refresh_budgets(self) -> List[AccountBudget]:
        """
        Reads account config and persisted consumption state, returning
        a refreshed list of AccountBudget instances.
        If config file is missing, initializes 4 default accounts with 1_000_000 tokens each.
        """
        now = datetime.now(timezone.utc)
        default_accounts = [
            {
                "account_id": f"account_{i+1}",
                "total_tokens": 1_000_000,
                "consumed_tokens": 0,
                "reset_timestamp": (now + timedelta(hours=6 * (i + 1))).isoformat(),
                "requests_remaining": 1000,
                "requests_total": 1000,
            }
            for i in range(4)
        ]

        if not self.accounts_config_path.exists():
            self.logger.warning(
                f"Accounts config {self.accounts_config_path} missing. "
                "Generating default configuration with 4 accounts."
            )
            data = {"accounts": default_accounts}
            with open(self.accounts_config_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            raw_accounts = default_accounts
        else:
            try:
                with open(self.accounts_config_path, "r", encoding="utf-8") as f:
                    config_data = json.load(f)
                    raw_accounts = config_data.get("accounts", [])
            except Exception as e:
                self.logger.error(f"Failed to read {self.accounts_config_path}: {e}")
                raw_accounts = default_accounts

        persisted_consumption: Dict[str, int] = {}
        if self.consumption_state_path.exists():
            try:
                with open(self.consumption_state_path, "r", encoding="utf-8") as f:
                    cons_data = json.load(f)
                    accounts_map = cons_data.get("accounts", {})
                    for acc_id, details in accounts_map.items():
                        persisted_consumption[acc_id] = details.get("consumed_tokens", 0)
            except Exception as e:
                self.logger.warning(f"Failed reading consumption state: {e}")

        budgets: List[AccountBudget] = []
        for item in raw_accounts:
            acc_id = str(item.get("account_id"))
            total = int(item.get("total_tokens", 1_000_000))
            config_consumed = int(item.get("consumed_tokens", 0))
            consumed = max(config_consumed, persisted_consumption.get(acc_id, 0))
            reset_ts = str(item.get("reset_timestamp", (now + timedelta(hours=24)).isoformat()))
            req_rem = int(item.get("requests_remaining", 1000))
            req_tot = int(item.get("requests_total", 1000))

            budgets.append(
                AccountBudget(
                    account_id=acc_id,
                    total_tokens=total,
                    consumed_tokens=consumed,
                    reset_timestamp=reset_ts,
                    requests_remaining=req_rem,
                    requests_total=req_tot,
                )
            )

        return budgets

    def predict_milestone_cost(self, milestone: dict) -> int:
        """
        Estimates token cost for a milestone using capacity forecaster if available,
        falling back to milestone properties or DEFAULT_COST_TOKENS.
        """
        if self.capacity_forecaster is not None:
            if hasattr(self.capacity_forecaster, "estimate_cost") and callable(
                self.capacity_forecaster.estimate_cost
            ):
                try:
                    predicted = self.capacity_forecaster.estimate_cost(milestone)
                    return max(100, int(predicted))
                except Exception as e:
                    self.logger.warning(f"Capacity forecaster estimation failed: {e}")

        if "predicted_cost_tokens" in milestone:
            return int(milestone["predicted_cost_tokens"])
        if "estimated_tokens" in milestone:
            return int(milestone["estimated_tokens"])
        if "cost" in milestone:
            return int(milestone["cost"])

        return self.DEFAULT_COST_TOKENS

    def compute_assignment(
        self,
        milestone: dict,
        budgets: List[AccountBudget],
        risk_score: float = 0.0,
    ) -> ArbitrageDecision:
        """
        Computes optimal account assignment for a milestone:
        (a) filter accounts where remaining_tokens > predicted_cost * 1.15 (15% safety margin);
        (b) if risk_score >= 0.5, prefer account with LOWEST remaining budget (budget quarantine);
        (c) if risk_score < 0.5, prefer account with HIGHEST remaining budget (headroom maximization);
        (d) tie-break by furthest reset_timestamp;
        (e) if no account passes the filter, return assigned_account='DEFER'.
        """
        milestone_id = str(milestone.get("id") or milestone.get("milestone_id") or "milestone-unspecified")
        predicted_cost = self.predict_milestone_cost(milestone)
        required_margin = predicted_cost * self.SAFETY_MARGIN
        is_high_risk = risk_score >= 0.5

        qualifying = [b for b in budgets if b.remaining_tokens > required_margin]

        if not qualifying:
            max_rem = max([b.remaining_tokens for b in budgets], default=0)
            reason = (
                f"Budget shortfall: predicted cost {predicted_cost} with 15% margin requires "
                f"{int(math.ceil(required_margin))} tokens. Max available remaining is {max_rem} tokens. "
                "Execution deferred until next quota reset cycle."
            )
            return ArbitrageDecision(
                milestone_id=milestone_id,
                assigned_account="DEFER",
                predicted_cost_tokens=predicted_cost,
                account_remaining_before=0,
                headroom_ratio=0.0,
                risk_adjusted=is_high_risk,
                reasoning=reason,
            )

        if is_high_risk:
            # Budget Quarantine: lowest remaining tokens first, tie-break by furthest reset timestamp
            qualifying.sort(
                key=lambda b: (b.remaining_tokens, -self._parse_timestamp(b.reset_timestamp))
            )
            strategy = "Budget Quarantine (Lowest Remaining Quota for High-Risk)"
        else:
            # Headroom Maximization: highest remaining tokens first, tie-break by furthest reset timestamp
            qualifying.sort(
                key=lambda b: (-b.remaining_tokens, -self._parse_timestamp(b.reset_timestamp))
            )
            strategy = "Headroom Maximization (Highest Remaining Quota for Safe Work)"

        chosen = qualifying[0]
        rem_before = chosen.remaining_tokens
        rem_after = max(0, rem_before - predicted_cost)
        headroom_ratio = round(rem_after / chosen.total_tokens, 4) if chosen.total_tokens > 0 else 0.0

        reasoning = (
            f"Assigned to '{chosen.account_id}' via {strategy}. "
            f"Pre-allocation: {rem_before} tokens, Post-allocation headroom: {rem_after} "
            f"({headroom_ratio:.2%}). Reset: {chosen.reset_timestamp}. Risk score: {risk_score:.2f}."
        )

        return ArbitrageDecision(
            milestone_id=milestone_id,
            assigned_account=chosen.account_id,
            predicted_cost_tokens=predicted_cost,
            account_remaining_before=rem_before,
            headroom_ratio=headroom_ratio,
            risk_adjusted=is_high_risk,
            reasoning=reasoning,
        )

    def assign_batch(
        self,
        milestones: List[dict],
        pipeline_state: Optional[dict] = None,
    ) -> List[ArbitrageDecision]:
        """
        Sorts pending milestones by predicted cost descending (assign expensive work first),
        computes assignment sequentially, and deducts predicted costs in-memory to prevent double-booking.
        """
        active_budgets = [
            AccountBudget(
                account_id=b.account_id,
                total_tokens=b.total_tokens,
                consumed_tokens=b.consumed_tokens,
                reset_timestamp=b.reset_timestamp,
                requests_remaining=b.requests_remaining,
                requests_total=b.requests_total,
            )
            for b in self.refresh_budgets()
        ]

        indexed_milestones: List[Tuple[int, dict]] = []
        for m in milestones:
            cost = self.predict_milestone_cost(m)
            indexed_milestones.append((cost, m))

        # Sort descending by predicted cost
        indexed_milestones.sort(key=lambda item: item[0], reverse=True)

        decisions: List[ArbitrageDecision] = []
        for cost, m in indexed_milestones:
            risk_score = float(m.get("risk_score", 0.0))
            if self.policy_gate and hasattr(self.policy_gate, "evaluate_risk"):
                try:
                    risk_score = float(self.policy_gate.evaluate_risk(m))
                except Exception as e:
                    self.logger.warning(f"Policy gate risk evaluation failed: {e}")

            decision = self.compute_assignment(m, active_budgets, risk_score=risk_score)
            decisions.append(decision)

            if decision.assigned_account != "DEFER":
                for b in active_budgets:
                    if b.account_id == decision.assigned_account:
                        b.consumed_tokens += decision.predicted_cost_tokens
                        b.requests_remaining = max(0, b.requests_remaining - 1)
                        break

        return decisions

    def record_consumption(
        self,
        account_id: str,
        actual_tokens: int,
        milestone_id: str = "",
        predicted_tokens: Optional[int] = None,
    ) -> None:
        """
        Updates dashboards/quota_consumption.json with actual spend, computes drift,
        and appends a record to dashboards/QUOTA_ARBITRAGE_LOG.md.
        """
        now_iso = datetime.now(timezone.utc).isoformat()
        pred = predicted_tokens if predicted_tokens is not None else actual_tokens
        drift = actual_tokens - pred

        state: Dict[str, Any] = {"accounts": {}, "consumption_history": []}
        if self.consumption_state_path.exists():
            try:
                with open(self.consumption_state_path, "r", encoding="utf-8") as f:
                    state = json.load(f)
            except Exception as e:
                self.logger.warning(f"Failed loading consumption state for update: {e}")

        acc_state = state["accounts"].setdefault(
            account_id, {"consumed_tokens": 0, "total_tokens": 1_000_000}
        )
        acc_state["consumed_tokens"] = acc_state.get("consumed_tokens", 0) + actual_tokens
        acc_state["last_updated"] = now_iso

        history_entry = {
            "timestamp": now_iso,
            "milestone_id": milestone_id or "unspecified",
            "account_id": account_id,
            "tokens": actual_tokens,
            "predicted_tokens": pred,
            "drift": drift,
        }
        state.setdefault("consumption_history", []).append(history_entry)

        with open(self.consumption_state_path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)

        total = acc_state.get("total_tokens", 1_000_000)
        headroom_after = max(0, total - acc_state["consumed_tokens"])

        drift_str = f"+{drift}" if drift > 0 else f"{drift}"
        log_line = (
            f"| {now_iso} | {milestone_id or 'N/A'} | {account_id} | "
            f"{pred} | {actual_tokens} | {drift_str} | {headroom_after} |\n"
        )

        with open(self.arbitrage_log_path, "a", encoding="utf-8") as f:
            f.write(log_line)

        self.logger.info(
            f"Consumption recorded for {account_id}: actual={actual_tokens}, "
            f"predicted={pred}, drift={drift_str}, headroom_after={headroom_after}"
        )

    def get_rebalance_summary(self) -> dict:
        """
        Returns summary metrics including per-account utilization ratios,
        total predicted remaining pipeline cost, and estimated hours until first account exhaustion.
        """
        budgets = self.refresh_budgets()
        utilization = {b.account_id: round(b.utilization_ratio, 4) for b in budgets}
        total_remaining = sum(b.remaining_tokens for b in budgets)

        history: List[dict] = []
        if self.consumption_state_path.exists():
            try:
                with open(self.consumption_state_path, "r", encoding="utf-8") as f:
                    history = json.load(f).get("consumption_history", [])
            except Exception:
                history = []

        avg_tokens_per_hour = 0.0
        if history:
            total_history_tokens = sum(h.get("tokens", 0) for h in history)
            ts_list = [
                self._parse_timestamp(h.get("timestamp", ""))
                for h in history
                if self._parse_timestamp(h.get("timestamp", "")) > 0
            ]
            if len(ts_list) >= 2:
                span_hours = max((max(ts_list) - min(ts_list)) / 3600.0, 0.25)
                avg_tokens_per_hour = total_history_tokens / span_hours
            else:
                avg_tokens_per_hour = float(total_history_tokens)

        # Estimated burn rate per active account
        num_accounts = max(len(budgets), 1)
        account_burn_rate = (avg_tokens_per_hour / num_accounts) if avg_tokens_per_hour > 0 else 0.0

        if account_burn_rate > 0 and budgets:
            min_remaining = min(b.remaining_tokens for b in budgets)
            est_hours = round(min_remaining / account_burn_rate, 2)
        else:
            est_hours = 999.0

        return {
            "account_utilization": utilization,
            "total_remaining_tokens": total_remaining,
            "avg_tokens_per_hour": round(avg_tokens_per_hour, 2),
            "estimated_hours_until_first_exhaustion": est_hours,
            "active_accounts_count": len(budgets),
        }


if __name__ == "__main__":
    import tempfile
    import shutil

    temp_dir = Path(tempfile.mkdtemp(prefix="quota_arbitrage_demo_"))
    try:
        cfg_file = temp_dir / "accounts.json"
        cons_file = temp_dir / "quota_consumption.json"
        log_file = temp_dir / "QUOTA_ARBITRAGE_LOG.md"

        now_utc = datetime.now(timezone.utc)
        demo_accounts = {
            "accounts": [
                {
                    "account_id": "account_alpha",
                    "total_tokens": 1_000_000,
                    "consumed_tokens": 200_000,
                    "reset_timestamp": (now_utc + timedelta(hours=8)).isoformat(),
                    "requests_remaining": 900,
                    "requests_total": 1000,
                },
                {
                    "account_id": "account_beta",
                    "total_tokens": 1_000_000,
                    "consumed_tokens": 850_000,
                    "reset_timestamp": (now_utc + timedelta(hours=4)).isoformat(),
                    "requests_remaining": 150,
                    "requests_total": 1000,
                },
                {
                    "account_id": "account_gamma",
                    "total_tokens": 1_000_000,
                    "consumed_tokens": 500_000,
                    "reset_timestamp": (now_utc + timedelta(hours=16)).isoformat(),
                    "requests_remaining": 600,
                    "requests_total": 1000,
                },
                {
                    "account_id": "account_delta",
                    "total_tokens": 1_000_000,
                    "consumed_tokens": 920_000,
                    "reset_timestamp": (now_utc + timedelta(hours=2)).isoformat(),
                    "requests_remaining": 80,
                    "requests_total": 1000,
                },
            ]
        }

        with open(cfg_file, "w", encoding="utf-8") as f:
            json.dump(demo_accounts, f, indent=2)

        engine = QuotaArbitrageEngine(
            accounts_config_path=str(cfg_file),
            consumption_state_path=str(cons_file),
            arbitrage_log_path=str(log_file),
        )

        test_milestones = [
            {
                "id": "M1_CORE_ENGINE",
                "predicted_cost_tokens": 50000,
                "risk_score": 0.1,  # Safe work -> Headroom Maximization
            },
            {
                "id": "M2_EXPERIMENTAL_REFACTOR",
                "predicted_cost_tokens": 15000,
                "risk_score": 0.85,  # High risk -> Budget Quarantine
            },
            {
                "id": "M3_SYSTEM_TELEMETRY",
                "predicted_cost_tokens": 8000,
                "risk_score": 0.2,  # Safe work -> Headroom Maximization
            },
        ]

        print("=== BATCH ARBITRAGE ASSIGNMENTS ===")
        decisions = engine.assign_batch(test_milestones)
        for d in decisions:
            print(f"Milestone: {d.milestone_id}")
            print(f"  Assigned Account : {d.assigned_account}")
            print(f"  Predicted Cost   : {d.predicted_cost_tokens} tokens")
            print(f"  Risk Adjusted    : {d.risk_adjusted}")
            print(f"  Headroom Ratio   : {d.headroom_ratio:.2%}")
            print(f"  Reasoning        : {d.reasoning}\n")

        print("=== RECORDING ACTUAL CONSUMPTION ===")
        assigned_acc = decisions[1].assigned_account
        engine.record_consumption(
            account_id=assigned_acc,
            actual_tokens=16200,
            milestone_id=decisions[1].milestone_id,
            predicted_tokens=decisions[1].predicted_cost_tokens,
        )

        print("\n=== REBALANCE SUMMARY ===")
        summary = engine.get_rebalance_summary()
        print(json.dumps(summary, indent=2))

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)