"""tasks/pipeline_promotion_gateway.py

Pipeline Autonomic Policy Promotion Gateway & Automated Rollback Controller
Part of the Hermes Orchestrator Autonomic Control Loop.

Manages policy promotion from canary approval through staged rollouts to production
baseline, evaluating live metrics against baseline thresholds and initiating coordinated
automated rollbacks upon telemetry regression.
"""

import datetime
import enum
import json
import logging
import os
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class PromotionStage(enum.Enum):
    """Enumeration of policy promotion lifecycle stages."""
    CANARY_APPROVED = "CANARY_APPROVED"
    STAGED_ROLLOUT_10 = "STAGED_ROLLOUT_10"
    STAGED_ROLLOUT_50 = "STAGED_ROLLOUT_50"
    FULL_PROMOTION = "FULL_PROMOTION"
    MONITORING_HOLD = "MONITORING_HOLD"
    STABLE = "STABLE"
    ROLLBACK = "ROLLBACK"


PROMOTION_SEQUENCE: List[PromotionStage] = [
    PromotionStage.CANARY_APPROVED,
    PromotionStage.STAGED_ROLLOUT_10,
    PromotionStage.STAGED_ROLLOUT_50,
    PromotionStage.FULL_PROMOTION,
    PromotionStage.MONITORING_HOLD,
    PromotionStage.STABLE,
]


@dataclass
class PolicyCandidate:
    """Represents a candidate policy approved for staged autonomic promotion."""
    experiment_id: str
    parameter_vector: Dict[str, Any] = field(default_factory=dict)
    posterior_snapshot: Dict[str, Any] = field(default_factory=dict)
    policy_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    promoted_at: float = field(default_factory=time.time)
    pre_promotion_baseline: Dict[str, float] = field(default_factory=lambda: {
        "error_rate": 0.0,
        "latency_p95": 0.0,
        "throughput": 0.0,
    })


class PromotionLedger:
    """Append-only JSON-lines log recording policy stage transitions atomically."""

    def __init__(self, ledger_path: str = "logs/promotion_ledger.jsonl"):
        self.ledger_path = os.path.abspath(ledger_path)
        parent_dir = os.path.dirname(self.ledger_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)
        self._lock = threading.Lock()

    def record(
        self,
        policy_id: str,
        stage_from: Any,
        stage_to: Any,
        gate_metrics: Dict[str, Any],
        decision: str,
    ) -> Dict[str, Any]:
        """Atomically append a stage transition record using tempfile-rename pattern."""
        stage_from_str = (
            stage_from.name if hasattr(stage_from, "name")
            else (str(stage_from) if stage_from is not None else None)
        )
        stage_to_str = (
            stage_to.name if hasattr(stage_to, "name")
            else (str(stage_to) if stage_to is not None else None)
        )

        entry = {
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "epoch": time.time(),
            "policy_id": policy_id,
            "stage_from": stage_from_str,
            "stage_to": stage_to_str,
            "gate_metrics": gate_metrics,
            "decision": decision,
        }

        parent_dir = os.path.dirname(self.ledger_path)
        with self._lock:
            existing_data = b""
            if os.path.exists(self.ledger_path):
                with open(self.ledger_path, "rb") as f:
                    existing_data = f.read()

            line = (json.dumps(entry) + "\n").encode("utf-8")
            with tempfile.NamedTemporaryFile("wb", dir=parent_dir, delete=False) as tmp:
                tmp_name = tmp.name
                if existing_data:
                    tmp.write(existing_data)
                tmp.write(line)
                tmp.flush()
                os.fsync(tmp.fileno())

            os.replace(tmp_name, self.ledger_path)

        return entry

    def read_history(self, policy_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Read and return promotion records, optionally filtered by policy_id."""
        if not os.path.exists(self.ledger_path):
            return []

        records = []
        with self._lock:
            with open(self.ledger_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        if policy_id is None or record.get("policy_id") == policy_id:
                            records.append(record)
                    except json.JSONDecodeError:
                        continue
        return records


class StagedPromoter:
    """Advances a candidate policy through sequential promotion stages based on gate metrics."""

    def __init__(
        self,
        hold_cycles: int = 2,
        epsilon_err: float = 0.05,
        tau_lat: float = 1.15,
        ledger: Optional[PromotionLedger] = None,
    ):
        self.hold_cycles = hold_cycles
        self.epsilon_err = epsilon_err
        self.tau_lat = tau_lat
        self.ledger = ledger
        self.current_stage: Optional[PromotionStage] = None
        self.stage_entry_cycle: int = 0
        self.active_candidate: Optional[PolicyCandidate] = None

    def begin_promotion(
        self,
        candidate: PolicyCandidate,
        starting_cycle: int = 0,
        baseline_metrics: Optional[Dict[str, float]] = None,
    ) -> None:
        """Initialize candidate at CANARY_APPROVED stage and establish baseline metrics."""
        self.active_candidate = candidate
        self.current_stage = PromotionStage.CANARY_APPROVED
        self.stage_entry_cycle = starting_cycle

        if baseline_metrics:
            self.active_candidate.pre_promotion_baseline = dict(baseline_metrics)
        elif not self.active_candidate.pre_promotion_baseline:
            self.active_candidate.pre_promotion_baseline = {
                "error_rate": 0.0,
                "latency_p95": 1.0,
                "throughput": 100.0,
            }

        if self.ledger:
            self.ledger.record(
                policy_id=candidate.policy_id,
                stage_from=None,
                stage_to=PromotionStage.CANARY_APPROVED.name,
                gate_metrics=self.active_candidate.pre_promotion_baseline,
                decision="BEGIN_PROMOTION",
            )
        logger.info(
            "Candidate %s entered %s at cycle %d with baseline: %s",
            candidate.policy_id,
            self.current_stage.name,
            starting_cycle,
            self.active_candidate.pre_promotion_baseline,
        )

    def evaluate_gate(self, current_cycle: int, live_metrics: Dict[str, float]) -> str:
        """Evaluate gate condition for the current stage.

        Returns:
            'HOLD' if required stage hold cycles have not elapsed or stage is terminal.
            'ADVANCE' if metrics satisfy regression tolerance.
            'ROLLBACK' if error rate delta or latency p95 ratio exceed threshold.
        """
        if self.active_candidate is None or self.current_stage is None:
            return "HOLD"

        if self.current_stage in (PromotionStage.STABLE, PromotionStage.ROLLBACK):
            return "HOLD"

        # Check stage hold duration
        cycles_in_stage = current_cycle - self.stage_entry_cycle
        if cycles_in_stage < self.hold_cycles:
            logger.debug(
                "Gate evaluation HOLD: cycle %d - entry %d = %d < hold_cycles %d",
                current_cycle,
                self.stage_entry_cycle,
                cycles_in_stage,
                self.hold_cycles,
            )
            return "HOLD"

        baseline = self.active_candidate.pre_promotion_baseline
        base_err = baseline.get("error_rate", 0.0)
        base_lat = baseline.get("latency_p95", 1.0)
        live_err = live_metrics.get("error_rate", 0.0)
        live_lat = live_metrics.get("latency_p95", 1.0)

        error_rate_delta = live_err - base_err
        if base_lat <= 0.0:
            latency_ratio = 1.0 if live_lat <= 0.0 else (live_lat / 1e-6)
        else:
            latency_ratio = live_lat / base_lat

        logger.info(
            "Evaluating gate at stage %s (cycle %d): err_delta=%.4f (eps=%.4f), lat_ratio=%.4f (tau=%.4f)",
            self.current_stage.name,
            current_cycle,
            error_rate_delta,
            self.epsilon_err,
            latency_ratio,
            self.tau_lat,
        )

        if error_rate_delta < self.epsilon_err and latency_ratio < self.tau_lat:
            return "ADVANCE"
        else:
            return "ROLLBACK"

    def advance_stage(self, current_cycle: int) -> PromotionStage:
        """Transition current_stage to next stage in sequence, updating cycle marker."""
        if self.current_stage not in PROMOTION_SEQUENCE:
            raise ValueError(f"Cannot advance from non-sequential stage: {self.current_stage}")

        current_idx = PROMOTION_SEQUENCE.index(self.current_stage)
        if current_idx < len(PROMOTION_SEQUENCE) - 1:
            next_stage = PROMOTION_SEQUENCE[current_idx + 1]
            old_stage = self.current_stage
            self.current_stage = next_stage
            self.stage_entry_cycle = current_cycle
            logger.info(
                "Advanced candidate %s: %s -> %s at cycle %d",
                self.active_candidate.policy_id if self.active_candidate else "none",
                old_stage.name,
                next_stage.name,
                current_cycle,
            )
        return self.current_stage

    def get_traffic_weight(self) -> float:
        """Return operational traffic routing weight associated with current stage."""
        if self.current_stage == PromotionStage.STAGED_ROLLOUT_10:
            return 0.1
        elif self.current_stage == PromotionStage.STAGED_ROLLOUT_50:
            return 0.5
        elif self.current_stage in (
            PromotionStage.FULL_PROMOTION,
            PromotionStage.MONITORING_HOLD,
            PromotionStage.STABLE,
        ):
            return 1.0
        return 0.0


class RollbackController:
    """Coordinates autonomic recovery and checkpoint restoration on gate regression."""

    def __init__(self, checkpoint_engine: Optional[Any] = None):
        self.checkpoint_engine = checkpoint_engine

    def execute_rollback(
        self,
        candidate: PolicyCandidate,
        failed_stage: PromotionStage,
        metric_deltas: Dict[str, Any],
        checkpoint_id: str,
    ) -> Dict[str, Any]:
        """Trigger coordinated rollback, restoring system checkpoint state."""
        restore_result = None
        if self.checkpoint_engine is not None:
            try:
                if hasattr(self.checkpoint_engine, "restore"):
                    restore_result = self.checkpoint_engine.restore(checkpoint_id)
                elif hasattr(self.checkpoint_engine, "restore_checkpoint"):
                    restore_result = self.checkpoint_engine.restore_checkpoint(checkpoint_id)
                else:
                    restore_result = {"status": "unsupported_checkpoint_interface"}
            except Exception as exc:
                logger.error("Failed to restore checkpoint %s: %s", checkpoint_id, exc)
                restore_result = {"status": "error", "error": str(exc)}

        failed_stage_name = (
            failed_stage.name if hasattr(failed_stage, "name") else str(failed_stage)
        )

        rollback_record = {
            "timestamp": time.time(),
            "iso_timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "policy_id": candidate.policy_id if candidate else "unknown",
            "experiment_id": candidate.experiment_id if candidate else "unknown",
            "failed_stage": failed_stage_name,
            "metric_deltas": metric_deltas,
            "checkpoint_id": checkpoint_id,
            "checkpoint_restore_result": restore_result,
            "status": "ROLLBACK_INITIATED",
        }

        logger.warning(
            "ROLLBACK_INITIATED for policy %s at stage %s. Restored checkpoint: %s. Deltas: %s",
            rollback_record["policy_id"],
            failed_stage_name,
            checkpoint_id,
            metric_deltas,
        )

        return rollback_record


class PromotionGateway:
    """Orchestrator for autonomic policy promotion, monitoring hold, and rollback."""

    def __init__(
        self,
        checkpoint_engine: Optional[Any] = None,
        telemetry_bridge: Optional[Any] = None,
        hold_cycles: int = 2,
        epsilon_err: float = 0.05,
        tau_lat: float = 1.15,
        ledger_path: str = "logs/promotion_ledger.jsonl",
    ):
        self.telemetry_bridge = telemetry_bridge
        self.checkpoint_engine = checkpoint_engine
        self.ledger = PromotionLedger(ledger_path=ledger_path)
        self.promoter = StagedPromoter(
            hold_cycles=hold_cycles,
            epsilon_err=epsilon_err,
            tau_lat=tau_lat,
            ledger=self.ledger,
        )
        self.rollback_controller = RollbackController(checkpoint_engine=checkpoint_engine)

        self.lock = threading.Lock()
        self.circuit_breaker_active: bool = False
        self.allowed_phases: Set[str] = {"EXPLOIT"}
        self.last_stable_checkpoint_id: str = "ckpt_baseline_stable"

    def set_last_stable_checkpoint(self, checkpoint_id: str) -> None:
        """Update reference to most recent stable checkpoint."""
        with self.lock:
            self.last_stable_checkpoint_id = checkpoint_id

    def submit_candidate(self, candidate: PolicyCandidate, current_cycle: int = 0) -> None:
        """Thread-safe registration of a newly approved PolicyCandidate."""
        with self.lock:
            self.promoter.begin_promotion(candidate, starting_cycle=current_cycle)

    def on_circuit_breaker(self, active: bool) -> None:
        """Update circuit breaker state to freeze or unfreeze in-flight promotions."""
        with self.lock:
            self.circuit_breaker_active = active
            if active:
                logger.warning("Circuit breaker tripped: all policy promotions frozen.")
            else:
                logger.info("Circuit breaker reset: promotions resumed.")

    def on_cycle_tick(
        self,
        current_cycle: int,
        current_phase: str,
        live_metrics: Optional[Dict[str, float]] = None,
    ) -> None:
        """Invoked by Loop Supervisor each cycle to evaluate in-flight promotion."""
        with self.lock:
            candidate = self.promoter.active_candidate
            stage = self.promoter.current_stage

            if candidate is None or stage in (None, PromotionStage.STABLE, PromotionStage.ROLLBACK):
                return

            if self.circuit_breaker_active:
                logger.warning(
                    "Promotion frozen: circuit breaker active at cycle %d", current_cycle
                )
                return

            if current_phase not in self.allowed_phases:
                logger.info(
                    "Promotion frozen: current phase '%s' not in allowed phases %s at cycle %d",
                    current_phase,
                    self.allowed_phases,
                    current_cycle,
                )
                return

            # Query live telemetry if not explicitly provided
            if live_metrics is None:
                if self.telemetry_bridge is not None:
                    try:
                        if hasattr(self.telemetry_bridge, "get_recent_metrics"):
                            live_metrics = self.telemetry_bridge.get_recent_metrics()
                        elif hasattr(self.telemetry_bridge, "query_metrics"):
                            live_metrics = self.telemetry_bridge.query_metrics()
                    except Exception as exc:
                        logger.error("Failed querying telemetry bridge: %s", exc)

            if not live_metrics:
                base = candidate.pre_promotion_baseline
                live_metrics = {
                    "error_rate": base.get("error_rate", 0.01),
                    "latency_p95": base.get("latency_p95", 100.0),
                    "throughput": base.get("throughput", 50.0),
                }

            decision = self.promoter.evaluate_gate(current_cycle, live_metrics)
            base = candidate.pre_promotion_baseline
            base_err = base.get("error_rate", 0.0)
            base_lat = base.get("latency_p95", 1.0)
            metric_deltas = {
                "error_rate_delta": live_metrics.get("error_rate", 0.0) - base_err,
                "latency_p95_ratio": (
                    live_metrics.get("latency_p95", 1.0) / max(base_lat, 1e-6)
                ),
                "live_metrics": live_metrics,
                "baseline_metrics": base,
            }

            if decision == "ADVANCE":
                old_stage = self.promoter.current_stage
                new_stage = self.promoter.advance_stage(current_cycle)
                self.ledger.record(
                    policy_id=candidate.policy_id,
                    stage_from=old_stage,
                    stage_to=new_stage,
                    gate_metrics=metric_deltas,
                    decision="ADVANCE",
                )
                if new_stage == PromotionStage.STABLE:
                    logger.info("Policy %s reached STABLE production stage.", candidate.policy_id)

            elif decision == "ROLLBACK":
                old_stage = self.promoter.current_stage
                self.promoter.current_stage = PromotionStage.ROLLBACK

                self.rollback_controller.execute_rollback(
                    candidate=candidate,
                    failed_stage=old_stage,
                    metric_deltas=metric_deltas,
                    checkpoint_id=self.last_stable_checkpoint_id,
                )

                if self.telemetry_bridge is not None:
                    try:
                        span_attrs = {
                            "policy_id": candidate.policy_id,
                            "failed_stage": old_stage.name if hasattr(old_stage, "name") else str(old_stage),
                            "metric_deltas": json.dumps(metric_deltas),
                            "checkpoint_id_restored": self.last_stable_checkpoint_id,
                        }
                        if hasattr(self.telemetry_bridge, "emit_span"):
                            self.telemetry_bridge.emit_span("ROLLBACK_INITIATED", span_attrs)
                        elif hasattr(self.telemetry_bridge, "record_span"):
                            self.telemetry_bridge.record_span("ROLLBACK_INITIATED", metadata=span_attrs)
                    except Exception as exc:
                        logger.error("Failed emitting rollback telemetry span: %s", exc)

                self.ledger.record(
                    policy_id=candidate.policy_id,
                    stage_from=old_stage,
                    stage_to=PromotionStage.ROLLBACK.name,
                    gate_metrics=metric_deltas,
                    decision="ROLLBACK",
                )

            elif decision == "HOLD":
                logger.debug(
                    "Policy %s held at stage %s (cycle %d)",
                    candidate.policy_id,
                    self.promoter.current_stage.name if self.promoter.current_stage else "NONE",
                    current_cycle,
                )

    def register_with_loop_supervisor(self, supervisor: Any) -> None:
        """Register the promotion gateway phase tick with a Loop Supervisor instance."""
        if hasattr(supervisor, "register_phase_callback"):
            supervisor.register_phase_callback(self.on_cycle_tick)
        elif hasattr(supervisor, "add_callback"):
            supervisor.add_callback(self.on_cycle_tick)
        logger.info("Registered PromotionGateway phase callback with Loop Supervisor.")

    def get_status(self) -> Dict[str, Any]:
        """Return real-time state dictionary for Cockpit UI consumption."""
        with self.lock:
            cand = self.promoter.active_candidate
            stage = self.promoter.current_stage
            return {
                "active_candidate_id": cand.policy_id if cand else None,
                "experiment_id": cand.experiment_id if cand else None,
                "current_stage": stage.name if stage else None,
                "stage_entry_cycle": self.promoter.stage_entry_cycle,
                "traffic_weight": self.promoter.get_traffic_weight(),
                "circuit_breaker_active": self.circuit_breaker_active,
                "allowed_phases": sorted(list(self.allowed_phases)),
                "last_stable_checkpoint_id": self.last_stable_checkpoint_id,
            }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
    )

    test_ledger_path = "logs/test_promotion_ledger.jsonl"
    if os.path.exists(test_ledger_path):
        os.remove(test_ledger_path)

    print("=" * 80)
    print("STARTING PROMOTION GATEWAY INTEGRATION TEST")
    print("=" * 80)

    # Mock Checkpoint Engine & Telemetry Bridge for isolated testing
    class MockCheckpointEngine:
        def __init__(self):
            self.restored_checkpoints = []

        def restore(self, checkpoint_id: str) -> Dict[str, Any]:
            self.restored_checkpoints.append(checkpoint_id)
            print(f"  [MockCheckpointEngine] Restored system state from checkpoint: '{checkpoint_id}'")
            return {"status": "SUCCESS", "checkpoint_id": checkpoint_id}

    class MockTelemetryBridge:
        def __init__(self):
            self.spans = []

        def emit_span(self, name: str, attributes: Dict[str, Any]) -> None:
            self.spans.append({"name": name, "attributes": attributes})
            print(f"  [MockTelemetryBridge] Telemetry span emitted: '{name}'")

    mock_checkpoint_engine = MockCheckpointEngine()
    mock_telemetry_bridge = MockTelemetryBridge()

    gateway = PromotionGateway(
        checkpoint_engine=mock_checkpoint_engine,
        telemetry_bridge=mock_telemetry_bridge,
        hold_cycles=2,
        epsilon_err=0.05,
        tau_lat=1.15,
        ledger_path=test_ledger_path,
    )

    # --------------------------------------------------------------------------
    # SCENARIO 1: Full Successful Promotion Lifecycle to STABLE
    # --------------------------------------------------------------------------
    print("\n[Scenario 1] Submitting candidate for full promotion lifecycle...")
    candidate_1 = PolicyCandidate(
        experiment_id="exp-opt-402",
        parameter_vector={"learning_rate": 0.003, "batch_size": 64},
        posterior_snapshot={"mu": [0.003, 64], "sigma": [0.0001, 2.0]},
        pre_promotion_baseline={"error_rate": 0.02, "latency_p95": 100.0, "throughput": 150.0},
    )
    gateway.submit_candidate(candidate_1, current_cycle=0)
    print(f"Initial Status: {gateway.get_status()}")

    # Healthy telemetry that easily passes gates
    healthy_metrics = {"error_rate": 0.021, "latency_p95": 102.0, "throughput": 152.0}

    # Cycle 1: EXPLOIT -> Should HOLD (1 cycle < 2 hold_cycles)
    gateway.on_cycle_tick(current_cycle=1, current_phase="EXPLOIT", live_metrics=healthy_metrics)
    assert gateway.get_status()["current_stage"] == "CANARY_APPROVED"

    # Cycle 2: EXPLOIT -> Should ADVANCE to STAGED_ROLLOUT_10 (weight 0.1)
    gateway.on_cycle_tick(current_cycle=2, current_phase="EXPLOIT", live_metrics=healthy_metrics)
    assert gateway.get_status()["current_stage"] == "STAGED_ROLLOUT_10"
    assert gateway.get_status()["traffic_weight"] == 0.1

    # Cycle 3: EXPLORE -> Promotion should freeze (phase not in allowed_phases)
    gateway.on_cycle_tick(current_cycle=3, current_phase="EXPLORE", live_metrics=healthy_metrics)
    assert gateway.get_status()["current_stage"] == "STAGED_ROLLOUT_10"

    # Cycle 4: EXPLOIT -> Should ADVANCE to STAGED_ROLLOUT_50 (weight 0.5)
    gateway.on_cycle_tick(current_cycle=4, current_phase="EXPLOIT", live_metrics=healthy_metrics)
    assert gateway.get_status()["current_stage"] == "STAGED_ROLLOUT_50"
    assert gateway.get_status()["traffic_weight"] == 0.5

    # Cycle 6: EXPLOIT -> Should ADVANCE to FULL_PROMOTION (weight 1.0)
    gateway.on_cycle_tick(current_cycle=6, current_phase="EXPLOIT", live_metrics=healthy_metrics)
    assert gateway.get_status()["current_stage"] == "FULL_PROMOTION"
    assert gateway.get_status()["traffic_weight"] == 1.0

    # Cycle 8: EXPLOIT -> Should ADVANCE to MONITORING_HOLD
    gateway.on_cycle_tick(current_cycle=8, current_phase="EXPLOIT", live_metrics=healthy_metrics)
    assert gateway.get_status()["current_stage"] == "MONITORING_HOLD"

    # Cycle 10: EXPLOIT -> Should reach STABLE
    gateway.on_cycle_tick(current_cycle=10, current_phase="EXPLOIT", live_metrics=healthy_metrics)
    assert gateway.get_status()["current_stage"] == "STABLE"
    assert gateway.get_status()["traffic_weight"] == 1.0
    print(f"Scenario 1 Succeeded: Final Status: {gateway.get_status()}")

    # --------------------------------------------------------------------------
    # SCENARIO 2: Regressive Candidate Triggers Automated Rollback
    # --------------------------------------------------------------------------
    print("\n[Scenario 2] Submitting regressive candidate triggering rollback...")
    candidate_2 = PolicyCandidate(
        experiment_id="exp-regress-909",
        parameter_vector={"learning_rate": 0.08, "batch_size": 8},
        posterior_snapshot={"mu": [0.08, 8], "sigma": [0.01, 1.0]},
        pre_promotion_baseline={"error_rate": 0.02, "latency_p95": 100.0, "throughput": 150.0},
    )
    gateway.set_last_stable_checkpoint("ckpt_stable_epoch_99")
    gateway.submit_candidate(candidate_2, current_cycle=20)

    # Advance to STAGED_ROLLOUT_10 at cycle 22
    gateway.on_cycle_tick(current_cycle=22, current_phase="EXPLOIT", live_metrics=healthy_metrics)
    assert gateway.get_status()["current_stage"] == "STAGED_ROLLOUT_10"

    # Advance to STAGED_ROLLOUT_50 at cycle 24
    gateway.on_cycle_tick(current_cycle=24, current_phase="EXPLOIT", live_metrics=healthy_metrics)
    assert gateway.get_status()["current_stage"] == "STAGED_ROLLOUT_50"

    # Cycle 26: Inject severe metric regression (high error rate delta > 0.05 and latency > 1.15 ratio)
    degraded_metrics = {"error_rate": 0.12, "latency_p95": 180.0, "throughput": 60.0}
    gateway.on_cycle_tick(current_cycle=26, current_phase="EXPLOIT", live_metrics=degraded_metrics)

    status_2 = gateway.get_status()
    print(f"Scenario 2 Status after regression: {status_2}")
    assert status_2["current_stage"] == "ROLLBACK"
    assert "ckpt_stable_epoch_99" in mock_checkpoint_engine.restored_checkpoints
    assert len(mock_telemetry_bridge.spans) > 0
    assert mock_telemetry_bridge.spans[-1]["name"] == "ROLLBACK_INITIATED"

    # Verify Ledger Records
    ledger_entries = gateway.ledger.read_history()
    print(f"\nLedger entries logged ({len(ledger_entries)} total):")
    for entry in ledger_entries:
        print(f"  {entry['timestamp']} | {entry['policy_id'][:8]} | {entry['stage_from']} -> {entry['stage_to']} | {entry['decision']}")

    assert len(ledger_entries) >= 8
    print("\n" + "=" * 80)
    print("ALL PROMOTION GATEWAY INTEGRATION TESTS PASSED SUCCESSFULLY!")
    print("=" * 80)