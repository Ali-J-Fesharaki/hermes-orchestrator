"""
Pipeline Autonomic State Persistence & Recovery Checkpoint Engine
Tasks: tasks/pipeline_state_checkpoint.py
Milestone: Pipeline Autonomic State Persistence & Recovery Checkpoint Engine

Provides durable state persistence, atomic checkpointing, validation, diffing,
and crash-recovery capabilities for the Hermes Orchestrator autonomic control loop.
"""

from __future__ import annotations

import copy
import dataclasses
from dataclasses import dataclass, field
import datetime
import json
import logging
import math
import os
import tempfile
from typing import Any, Dict, List, Optional, Tuple, Union
import uuid

logger = logging.getLogger("hermes.autonomic.checkpoint")


class CheckpointCorruptionError(Exception):
    """Raised when a checkpoint payload or manifest is missing, invalid, or corrupted."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def __str__(self) -> str:
        if self.details:
            return f"{self.message} | Details: {json.dumps(self.details, default=str)}"
        return self.message


def _normalize_json_value(val: Any) -> Any:
    """Recursively converts objects (including numpy-like arrays) to pure JSON types."""
    if hasattr(val, "tolist") and callable(val.tolist):
        return val.tolist()
    if isinstance(val, dict):
        return {str(k): _normalize_json_value(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return [_normalize_json_value(v) for v in val]
    if isinstance(val, (int, str, bool)) or val is None:
        return val
    if isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            return str(val)
        return val
    if hasattr(val, "isoformat") and callable(val.isoformat):
        return val.isoformat()
    return str(val)


@dataclass
class StateSnapshot:
    """Represents an atomic, immutable snapshot of the autonomic control loop state."""

    snapshot_id: str
    timestamp: str
    cycle_number: int
    supervisor_phase: str
    supervisor_meta_log: List[Dict[str, Any]]
    learning_engine_state: Dict[str, Any]
    calibration_state: Dict[str, Any]
    experiment_state: Dict[str, Any]
    meta_controller_state: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Recursively converts snapshot fields into a JSON-serializable dictionary."""
        return {
            "snapshot_id": self.snapshot_id,
            "timestamp": self.timestamp,
            "cycle_number": self.cycle_number,
            "supervisor_phase": self.supervisor_phase,
            "supervisor_meta_log": _normalize_json_value(self.supervisor_meta_log),
            "learning_engine_state": _normalize_json_value(self.learning_engine_state),
            "calibration_state": _normalize_json_value(self.calibration_state),
            "experiment_state": _normalize_json_value(self.experiment_state),
            "meta_controller_state": _normalize_json_value(self.meta_controller_state),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> StateSnapshot:
        """Reconstructs and validates a StateSnapshot from a dictionary.

        Raises:
            CheckpointCorruptionError: If validation fails or schema types are invalid.
        """
        if not isinstance(data, dict):
            raise CheckpointCorruptionError(
                "Checkpoint root data must be a dictionary",
                {"provided_type": type(data).__name__},
            )

        required_top_keys = {
            "snapshot_id": str,
            "timestamp": str,
            "cycle_number": int,
            "supervisor_phase": str,
            "supervisor_meta_log": list,
            "learning_engine_state": dict,
            "calibration_state": dict,
            "experiment_state": dict,
            "meta_controller_state": dict,
        }

        for key, expected_type in required_top_keys.items():
            if key not in data:
                raise CheckpointCorruptionError(
                    f"Missing required field in snapshot: '{key}'",
                    {"missing_field": key, "snapshot_keys": list(data.keys())},
                )
            val = data[key]
            # Handle float/int compatibility for cycle_number if integer float
            if expected_type is int and isinstance(val, float) and val.is_integer():
                val = int(val)
                data[key] = val
            if not isinstance(val, expected_type):
                raise CheckpointCorruptionError(
                    f"Invalid type for field '{key}': expected {expected_type.__name__}, got {type(val).__name__}",
                    {"field": key, "expected": expected_type.__name__, "actual": type(val).__name__},
                )

        # Validate learning_engine_state subfields
        le = data["learning_engine_state"]
        le_keys = {
            "policy_parameters": list,
            "posterior_mean": list,
            "posterior_variance": list,
            "proposal_history": list,
        }
        for subkey, expected_type in le_keys.items():
            if subkey not in le:
                raise CheckpointCorruptionError(
                    f"Missing learning_engine_state subfield: '{subkey}'",
                    {"learning_engine_keys": list(le.keys())},
                )
            if not isinstance(le[subkey], expected_type):
                raise CheckpointCorruptionError(
                    f"Invalid type for learning_engine_state.{subkey}: expected {expected_type.__name__}, got {type(le[subkey]).__name__}",
                    {"field": f"learning_engine_state.{subkey}"},
                )

        # Validate vector float elements
        for vec_key in ["policy_parameters", "posterior_mean", "posterior_variance"]:
            for idx, item in enumerate(le[vec_key]):
                if not isinstance(item, (int, float)):
                    raise CheckpointCorruptionError(
                        f"Non-numeric item at {vec_key}[{idx}]: {item}",
                        {"field": f"learning_engine_state.{vec_key}", "index": idx, "value": item},
                    )

        # Validate calibration_state subfields
        cal = data["calibration_state"]
        cal_keys = {
            "drift_baselines": dict,
            "ema_windows": dict,
            "convergence_flags": dict,
            "cumulative_drift_magnitude": (int, float),
        }
        for subkey, expected_type in cal_keys.items():
            if subkey not in cal:
                raise CheckpointCorruptionError(
                    f"Missing calibration_state subfield: '{subkey}'",
                    {"calibration_keys": list(cal.keys())},
                )
            if not isinstance(cal[subkey], expected_type):
                raise CheckpointCorruptionError(
                    f"Invalid type for calibration_state.{subkey}: expected {expected_type}, got {type(cal[subkey]).__name__}",
                    {"field": f"calibration_state.{subkey}"},
                )

        # Validate experiment_state subfields
        exp = data["experiment_state"]
        exp_keys = {
            "active_canaries": list,
            "promotion_history": list,
            "statistical_gate_params": dict,
        }
        for subkey, expected_type in exp_keys.items():
            if subkey not in exp:
                raise CheckpointCorruptionError(
                    f"Missing experiment_state subfield: '{subkey}'",
                    {"experiment_keys": list(exp.keys())},
                )
            if not isinstance(exp[subkey], expected_type):
                raise CheckpointCorruptionError(
                    f"Invalid type for experiment_state.{subkey}: expected {expected_type.__name__}, got {type(exp[subkey]).__name__}",
                    {"field": f"experiment_state.{subkey}"},
                )

        # Validate meta_controller_state subfields
        mc = data["meta_controller_state"]
        mc_keys = {
            "learning_rate": (int, float),
            "ensemble_size": int,
            "significance_threshold": (int, float),
            "circuit_breaker_trip_count": int,
        }
        for subkey, expected_type in mc_keys.items():
            if subkey not in mc:
                raise CheckpointCorruptionError(
                    f"Missing meta_controller_state subfield: '{subkey}'",
                    {"meta_controller_keys": list(mc.keys())},
                )
            if not isinstance(mc[subkey], expected_type):
                raise CheckpointCorruptionError(
                    f"Invalid type for meta_controller_state.{subkey}: expected {expected_type}, got {type(mc[subkey]).__name__}",
                    {"field": f"meta_controller_state.{subkey}"},
                )

        return cls(
            snapshot_id=str(data["snapshot_id"]),
            timestamp=str(data["timestamp"]),
            cycle_number=int(data["cycle_number"]),
            supervisor_phase=str(data["supervisor_phase"]),
            supervisor_meta_log=list(data["supervisor_meta_log"]),
            learning_engine_state=dict(data["learning_engine_state"]),
            calibration_state=dict(data["calibration_state"]),
            experiment_state=dict(data["experiment_state"]),
            meta_controller_state=dict(data["meta_controller_state"]),
        )


class AutonomicCheckpointEngine:
    """Durable state persistence, recovery, and lifecycle manager for autonomic loop snapshots."""

    def __init__(self, checkpoints_dir: str = "checkpoints/", max_retained: int = 50) -> None:
        self.checkpoints_dir = os.path.abspath(checkpoints_dir)
        self.max_retained = max(1, max_retained)
        self.manifest_path = os.path.join(self.checkpoints_dir, "manifest.json")
        self._ensure_storage()

    def _ensure_storage(self) -> None:
        """Initializes the checkpoint directory and ensures manifest.json exists."""
        os.makedirs(self.checkpoints_dir, exist_ok=True)
        if not os.path.exists(self.manifest_path):
            self._write_manifest({"version": 1, "checkpoints": {}})

    def _read_manifest(self) -> Dict[str, Any]:
        """Reads and parses manifest.json safely."""
        if not os.path.exists(self.manifest_path):
            return {"version": 1, "checkpoints": {}}
        try:
            with open(self.manifest_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if not isinstance(data, dict) or "checkpoints" not in data:
                    logger.warning("Manifest schema unaligned, initializing fresh index structure")
                    return {"version": 1, "checkpoints": {}}
                return data
        except Exception as exc:
            logger.warning("Failed to parse manifest.json: %s. Resetting fresh manifest index.", exc)
            return {"version": 1, "checkpoints": {}}

    def _write_manifest(self, manifest_data: Dict[str, Any]) -> None:
        """Atomically persists manifest.json via temporary file + atomic rename."""
        self._atomic_write_json(self.manifest_path, manifest_data)

    def _atomic_write_json(self, target_filepath: str, payload: Any) -> None:
        """Writes JSON payload atomically to target_filepath within the checkpoints directory."""
        directory = os.path.dirname(target_filepath)
        os.makedirs(directory, exist_ok=True)
        fd, temp_path = tempfile.mkstemp(dir=directory, prefix=".tmp_chk_", suffix=".json")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, default=str)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, target_filepath)
        except Exception:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass
            raise

    def checkpoint(self, snapshot: StateSnapshot) -> str:
        """Serializes snapshot to JSON, stores it atomically, updates manifest, and enforces retention.

        Returns:
            str: The persistent snapshot_id.
        """
        if not snapshot.snapshot_id:
            snapshot.snapshot_id = str(uuid.uuid4())
        if not snapshot.timestamp:
            snapshot.timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

        payload = snapshot.to_dict()
        snapshot_filename = f"{snapshot.snapshot_id}.json"
        snapshot_filepath = os.path.join(self.checkpoints_dir, snapshot_filename)

        logger.info(
            "Persisting autonomic checkpoint: cycle=%d, id=%s, phase=%s",
            snapshot.cycle_number,
            snapshot.snapshot_id,
            snapshot.supervisor_phase,
        )

        # Atomic write of the snapshot payload
        self._atomic_write_json(snapshot_filepath, payload)

        # Update manifest index
        manifest = self._read_manifest()
        checkpoints_map = manifest.setdefault("checkpoints", {})
        cycle_key = str(snapshot.cycle_number)

        checkpoints_map[cycle_key] = {
            "snapshot_id": snapshot.snapshot_id,
            "timestamp": snapshot.timestamp,
            "cycle_number": snapshot.cycle_number,
            "supervisor_phase": snapshot.supervisor_phase,
            "filename": snapshot_filename,
        }

        # LRU/Retention Eviction
        self._enforce_retention(manifest)
        self._write_manifest(manifest)

        logger.info("Checkpoint committed successfully. Total indexed: %d", len(manifest["checkpoints"]))
        return snapshot.snapshot_id

    def _enforce_retention(self, manifest: Dict[str, Any]) -> None:
        """Prunes snapshots exceeding max_retained, deleting physical files and manifest entries."""
        checkpoints_map = manifest.get("checkpoints", {})
        if len(checkpoints_map) <= self.max_retained:
            return

        # Sort cycles ascending to evict the oldest
        sorted_cycles = sorted(checkpoints_map.keys(), key=lambda c: int(c))
        eviction_count = len(sorted_cycles) - self.max_retained
        cycles_to_evict = sorted_cycles[:eviction_count]

        for cycle_key in cycles_to_evict:
            entry = checkpoints_map.pop(cycle_key, None)
            if entry:
                filename = entry.get("filename") or f"{entry.get('snapshot_id')}.json"
                file_path = os.path.join(self.checkpoints_dir, filename)
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                        logger.warning(
                            "Evicted old checkpoint for cycle %s (id=%s)",
                            cycle_key,
                            entry.get("snapshot_id"),
                        )
                    except OSError as err:
                        logger.warning("Failed to delete evicted snapshot file %s: %s", file_path, err)

    def restore(self, snapshot_id: Optional[str] = None) -> StateSnapshot:
        """Loads and validates a StateSnapshot. If snapshot_id is None, loads the latest checkpoint.

        Raises:
            CheckpointCorruptionError: If file not found, unparseable, or validation fails.
        """
        manifest = self._read_manifest()
        checkpoints_map = manifest.get("checkpoints", {})

        target_id = snapshot_id
        target_filename = None

        if target_id is None:
            if not checkpoints_map:
                raise CheckpointCorruptionError("No checkpoints found in manifest index")
            latest_cycle = max(checkpoints_map.keys(), key=lambda c: int(c))
            target_entry = checkpoints_map[latest_cycle]
            target_id = target_entry["snapshot_id"]
            target_filename = target_entry.get("filename")
        else:
            # Locate entry in manifest if present
            for entry in checkpoints_map.values():
                if entry.get("snapshot_id") == target_id:
                    target_filename = entry.get("filename")
                    break

        if not target_filename:
            target_filename = f"{target_id}.json"

        snapshot_filepath = os.path.join(self.checkpoints_dir, target_filename)
        if not os.path.isfile(snapshot_filepath):
            # Fallback search if filename was purely guessed
            direct_path = os.path.join(self.checkpoints_dir, f"{target_id}.json")
            if os.path.isfile(direct_path):
                snapshot_filepath = direct_path
            else:
                raise CheckpointCorruptionError(
                    f"Checkpoint file not found on disk: {snapshot_filepath}",
                    {"snapshot_id": target_id, "expected_path": snapshot_filepath},
                )

        try:
            with open(snapshot_filepath, "r", encoding="utf-8") as f:
                raw_data = json.load(f)
        except Exception as exc:
            raise CheckpointCorruptionError(
                f"Failed to read/parse snapshot JSON: {exc}",
                {"snapshot_filepath": snapshot_filepath, "error": str(exc)},
            ) from exc

        snapshot = StateSnapshot.from_dict(raw_data)
        logger.info(
            "Successfully restored checkpoint: cycle=%d, id=%s, phase=%s",
            snapshot.cycle_number,
            snapshot.snapshot_id,
            snapshot.supervisor_phase,
        )
        return snapshot

    def rollback(self, target_cycle: int) -> StateSnapshot:
        """Restores nearest checkpoint <= target_cycle, truncating manifest and deleting later files."""
        manifest = self._read_manifest()
        checkpoints_map = manifest.get("checkpoints", {})

        if not checkpoints_map:
            raise CheckpointCorruptionError("Cannot rollback: manifest is empty")

        available_cycles = sorted([int(c) for c in checkpoints_map.keys()])
        eligible_cycles = [c for c in available_cycles if c <= target_cycle]

        if not eligible_cycles:
            raise CheckpointCorruptionError(
                f"No checkpoint available at or before target cycle {target_cycle}",
                {"target_cycle": target_cycle, "available_cycles": available_cycles},
            )

        chosen_cycle = max(eligible_cycles)
        chosen_entry = checkpoints_map[str(chosen_cycle)]
        restored_snapshot = self.restore(chosen_entry["snapshot_id"])

        # Truncate all cycles strictly greater than chosen_cycle
        cycles_to_purge = [c for c in available_cycles if c > chosen_cycle]
        for c in cycles_to_purge:
            cycle_str = str(c)
            entry = checkpoints_map.pop(cycle_str, None)
            if entry:
                fname = entry.get("filename") or f"{entry.get('snapshot_id')}.json"
                fpath = os.path.join(self.checkpoints_dir, fname)
                if os.path.isfile(fpath):
                    try:
                        os.remove(fpath)
                    except OSError as err:
                        logger.warning("Error removing truncated checkpoint file %s: %s", fpath, err)

        self._write_manifest(manifest)
        logger.info(
            "Rollback executed to cycle %d (purged cycles: %s). Current active state: %s",
            chosen_cycle,
            cycles_to_purge,
            restored_snapshot.snapshot_id,
        )
        return restored_snapshot

    def list_checkpoints(self) -> List[Dict[str, Any]]:
        """Returns all manifest entries sorted by cycle_number descending."""
        manifest = self._read_manifest()
        checkpoints_map = manifest.get("checkpoints", {})
        entries = list(checkpoints_map.values())
        entries.sort(key=lambda item: int(item.get("cycle_number", 0)), reverse=True)
        return entries

    def diff_checkpoints(self, id_a: str, id_b: str) -> Dict[str, Any]:
        """Computes field-level deltas between snapshots id_a (baseline) and id_b (comparison)."""
        snap_a = self.restore(id_a)
        snap_b = self.restore(id_b)

        diff: Dict[str, Any] = {
            "baseline_id": id_a,
            "comparison_id": id_b,
            "cycle_delta": snap_b.cycle_number - snap_a.cycle_number,
            "timestamp_a": snap_a.timestamp,
            "timestamp_b": snap_b.timestamp,
            "learning_engine": {},
            "calibration": {},
            "meta_controller": {},
            "experiment": {},
        }

        # Diff float vectors in learning_engine_state
        le_a = snap_a.learning_engine_state
        le_b = snap_b.learning_engine_state
        for vec_key in ["policy_parameters", "posterior_mean", "posterior_variance"]:
            va = le_a.get(vec_key, [])
            vb = le_b.get(vec_key, [])
            min_len = min(len(va), len(vb))
            elem_diffs = [float(vb[i] - va[i]) for i in range(min_len)]
            l2_norm = math.sqrt(sum(d * d for d in elem_diffs)) if elem_diffs else 0.0
            diff["learning_engine"][vec_key] = {
                "length_a": len(va),
                "length_b": len(vb),
                "element_deltas": elem_diffs,
                "l2_norm_delta": l2_norm,
            }

        # Diff proposal history count
        diff["learning_engine"]["proposal_count_delta"] = len(
            le_b.get("proposal_history", [])
        ) - len(le_a.get("proposal_history", []))

        # Diff calibration_state
        cal_a = snap_a.calibration_state
        cal_b = snap_b.calibration_state

        # Drift baselines
        base_a = cal_a.get("drift_baselines", {})
        base_b = cal_b.get("drift_baselines", {})
        all_metrics = sorted(set(base_a.keys()) | set(base_b.keys()))
        baseline_deltas = {}
        for m in all_metrics:
            val_a = base_a.get(m)
            val_b = base_b.get(m)
            if val_a is not None and val_b is not None:
                baseline_deltas[m] = {
                    "baseline_a": val_a,
                    "baseline_b": val_b,
                    "delta": val_b - val_a,
                }
            elif val_a is None:
                baseline_deltas[m] = {"status": "added", "value": val_b}
            else:
                baseline_deltas[m] = {"status": "removed", "value": val_a}
        diff["calibration"]["drift_baselines"] = baseline_deltas

        # Cumulative drift magnitude
        cdm_a = float(cal_a.get("cumulative_drift_magnitude", 0.0))
        cdm_b = float(cal_b.get("cumulative_drift_magnitude", 0.0))
        abs_change = cdm_b - cdm_a
        pct_change = (abs_change / cdm_a * 100.0) if cdm_a != 0.0 else (0.0 if abs_change == 0 else float("inf"))
        diff["calibration"]["cumulative_drift_magnitude"] = {
            "val_a": cdm_a,
            "val_b": cdm_b,
            "absolute_change": abs_change,
            "percentage_change": pct_change,
        }

        # Convergence flags changes
        cf_a = cal_a.get("convergence_flags", {})
        cf_b = cal_b.get("convergence_flags", {})
        flag_keys = sorted(set(cf_a.keys()) | set(cf_b.keys()))
        flag_diffs = {}
        for fk in flag_keys:
            if cf_a.get(fk) != cf_b.get(fk):
                flag_diffs[fk] = {"from": cf_a.get(fk), "to": cf_b.get(fk)}
        diff["calibration"]["convergence_flag_shifts"] = flag_diffs

        # Diff meta_controller_state
        mc_a = snap_a.meta_controller_state
        mc_b = snap_b.meta_controller_state
        for param in ["learning_rate", "significance_threshold"]:
            pa = float(mc_a.get(param, 0.0))
            pb = float(mc_b.get(param, 0.0))
            delta = pb - pa
            pct = (delta / pa * 100.0) if pa != 0.0 else 0.0
            diff["meta_controller"][param] = {
                "val_a": pa,
                "val_b": pb,
                "absolute_delta": delta,
                "percentage_change": pct,
            }

        diff["meta_controller"]["ensemble_size"] = {
            "val_a": mc_a.get("ensemble_size"),
            "val_b": mc_b.get("ensemble_size"),
            "delta": mc_b.get("ensemble_size", 0) - mc_a.get("ensemble_size", 0),
        }
        diff["meta_controller"]["circuit_breaker_trip_count"] = {
            "val_a": mc_a.get("circuit_breaker_trip_count"),
            "val_b": mc_b.get("circuit_breaker_trip_count"),
            "delta": mc_b.get("circuit_breaker_trip_count", 0) - mc_a.get("circuit_breaker_trip_count", 0),
        }

        # Diff experiment_state
        exp_a = snap_a.experiment_state
        exp_b = snap_b.experiment_state
        diff["experiment"]["active_canaries_count_delta"] = len(
            exp_b.get("active_canaries", [])
        ) - len(exp_a.get("active_canaries", []))
        diff["experiment"]["promotion_history_count_delta"] = len(
            exp_b.get("promotion_history", [])
        ) - len(exp_a.get("promotion_history", []))

        logger.debug(
            "Computed diff between %s and %s (cycle delta=%d)",
            id_a,
            id_b,
            diff["cycle_delta"],
        )
        return diff


# Module-level default singleton engine
_DEFAULT_ENGINE: Optional[AutonomicCheckpointEngine] = None


def get_default_engine() -> AutonomicCheckpointEngine:
    """Returns or initializes the default AutonomicCheckpointEngine instance."""
    global _DEFAULT_ENGINE
    if _DEFAULT_ENGINE is None:
        _DEFAULT_ENGINE = AutonomicCheckpointEngine()
    return _DEFAULT_ENGINE


def checkpoint_after_cycle(
    supervisor_state_dict: Dict[str, Any],
    engine: Optional[AutonomicCheckpointEngine] = None,
) -> str:
    """Integration hook called by the Loop Supervisor to trigger automated checkpointing.

    Accepts the supervisor's exported state dictionary, validates and parses it into
    a StateSnapshot, and persists it.
    """
    active_engine = engine or get_default_engine()

    # Ensure required top-level snapshot fields exist or supply defaults
    snapshot_id = supervisor_state_dict.get("snapshot_id") or str(uuid.uuid4())
    timestamp = supervisor_state_dict.get("timestamp") or datetime.datetime.now(datetime.timezone.utc).isoformat()
    cycle_number = int(supervisor_state_dict.get("cycle_number", 0))
    supervisor_phase = str(supervisor_state_dict.get("supervisor_phase", "IDLE"))
    supervisor_meta_log = list(supervisor_state_dict.get("supervisor_meta_log", []))
    learning_engine_state = dict(supervisor_state_dict.get("learning_engine_state", {}))
    calibration_state = dict(supervisor_state_dict.get("calibration_state", {}))
    experiment_state = dict(supervisor_state_dict.get("experiment_state", {}))
    meta_controller_state = dict(supervisor_state_dict.get("meta_controller_state", {}))

    snapshot = StateSnapshot(
        snapshot_id=snapshot_id,
        timestamp=timestamp,
        cycle_number=cycle_number,
        supervisor_phase=supervisor_phase,
        supervisor_meta_log=supervisor_meta_log,
        learning_engine_state=learning_engine_state,
        calibration_state=calibration_state,
        experiment_state=experiment_state,
        meta_controller_state=meta_controller_state,
    )

    return active_engine.checkpoint(snapshot)


def restore_on_startup(
    engine: Optional[AutonomicCheckpointEngine] = None,
) -> Optional[StateSnapshot]:
    """Integration hook called during orchestrator initialization to recover from the latest valid snapshot.

    Returns:
        StateSnapshot if a valid checkpoint exists; None otherwise (with warnings logged).
    """
    active_engine = engine or get_default_engine()
    try:
        manifest = active_engine._read_manifest()
        checkpoints_map = manifest.get("checkpoints", {})
        if not checkpoints_map:
            logger.info("No existing autonomic checkpoints found during startup initialization.")
            return None

        latest_snapshot = active_engine.restore(snapshot_id=None)
        logger.info(
            "Autonomic loop state restored on startup: cycle=%d, snapshot_id=%s",
            latest_snapshot.cycle_number,
            latest_snapshot.snapshot_id,
        )
        return latest_snapshot
    except CheckpointCorruptionError as err:
        logger.warning("Startup checkpoint restoration failed due to corruption: %s", err)
        return None
    except Exception as exc:
        logger.warning("Unexpected error restoring autonomic state on startup: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Cockpit API Handlers & Endpoint Registry
# ---------------------------------------------------------------------------

def handle_list_checkpoints(params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Cockpit API handler for GET /api/autonomic/checkpoints."""
    engine = get_default_engine()
    items = engine.list_checkpoints()
    return {
        "status": "success",
        "count": len(items),
        "checkpoints": items,
    }


def handle_get_checkpoint(checkpoint_id: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Cockpit API handler for GET /api/autonomic/checkpoints/{checkpoint_id}."""
    engine = get_default_engine()
    try:
        snapshot = engine.restore(checkpoint_id)
        return {
            "status": "success",
            "checkpoint": snapshot.to_dict(),
        }
    except CheckpointCorruptionError as err:
        return {
            "status": "error",
            "error_code": "CHECKPOINT_NOT_FOUND_OR_CORRUPT",
            "message": str(err),
            "details": err.details,
        }


def handle_restore_checkpoint(checkpoint_id: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Cockpit API handler for POST /api/autonomic/checkpoints/{checkpoint_id}/restore."""
    engine = get_default_engine()
    try:
        target_snapshot = engine.restore(checkpoint_id)
        restored = engine.rollback(target_snapshot.cycle_number)
        return {
            "status": "success",
            "action": "rollback",
            "restored_cycle": restored.cycle_number,
            "restored_snapshot_id": restored.snapshot_id,
            "supervisor_phase": restored.supervisor_phase,
        }
    except CheckpointCorruptionError as err:
        return {
            "status": "error",
            "error_code": "ROLLBACK_FAILED",
            "message": str(err),
            "details": err.details,
        }


AUTONOMIC_CHECKPOINT_ENDPOINTS: Dict[str, Any] = {
    "GET /api/autonomic/checkpoints": handle_list_checkpoints,
    "GET /api/autonomic/checkpoints/{checkpoint_id}": handle_get_checkpoint,
    "POST /api/autonomic/checkpoints/{checkpoint_id}/restore": handle_restore_checkpoint,
}


# ---------------------------------------------------------------------------
# Self-Verification & Demonstration Runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    print("================================================================================")
    print("Hermes Autonomic State Checkpoint Engine - Self-Verification")
    print("================================================================================")

    with tempfile.TemporaryDirectory() as temp_dir:
        test_engine = AutonomicCheckpointEngine(checkpoints_dir=temp_dir, max_retained=5)

        # 1. Generate Synthetic State Snapshot (Cycle 1)
        cycle_1_snapshot = StateSnapshot(
            snapshot_id=str(uuid.uuid4()),
            timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            cycle_number=1,
            supervisor_phase="CALIBRATE",
            supervisor_meta_log=[
                {"timestamp": "2026-10-27T10:00:00Z", "action": "ADAPT_LR", "reason": "Loss stabilization"},
                {"timestamp": "2026-10-27T10:05:00Z", "action": "EXPAND_ENSEMBLE", "ensemble_size": 5},
            ],
            learning_engine_state={
                "policy_parameters": [0.12, 0.45, 0.78, -0.34, 0.55, 0.89, -0.11, 0.02, 0.67, 0.31],
                "posterior_mean": [0.10, 0.40, 0.75, -0.30, 0.50, 0.85, -0.10, 0.01, 0.65, 0.30],
                "posterior_variance": [0.01] * 10,
                "proposal_history": [
                    {"proposal_id": "p-101", "score": 0.92, "accepted": True},
                    {"proposal_id": "p-102", "score": 0.88, "accepted": True},
                ],
            },
            calibration_state={
                "drift_baselines": {"latency_p95": 42.5, "error_rate": 0.003, "throughput": 1250.0},
                "ema_windows": {
                    "latency_p95": [40.0, 41.2, 42.5],
                    "error_rate": [0.002, 0.0025, 0.003],
                },
                "convergence_flags": {"policy_converged": True, "covariance_stable": True},
                "cumulative_drift_magnitude": 0.045,
            },
            experiment_state={
                "active_canaries": [
                    {"canary_id": "canary-alpha", "traffic_split": 0.05, "status": "HEALTHY"},
                    {"canary_id": "canary-beta", "traffic_split": 0.10, "status": "EVALUATING"},
                ],
                "promotion_history": [{"canary_id": "canary-prev", "promoted_at": "2026-10-27T09:00:00Z"}],
                "statistical_gate_params": {"p_value_threshold": 0.01, "min_sample_size": 500},
            },
            meta_controller_state={
                "learning_rate": 0.005,
                "ensemble_size": 5,
                "significance_threshold": 0.05,
                "circuit_breaker_trip_count": 0,
            },
        )

        # 2. Checkpoint Cycle 1
        id_1 = test_engine.checkpoint(cycle_1_snapshot)
        print(f"[OK] Checkpoint created for Cycle 1: {id_1}")

        # 3. List Checkpoints
        chk_list = test_engine.list_checkpoints()
        assert len(chk_list) == 1, "Expected 1 checkpoint in manifest"
        print(f"[OK] Manifest validated: found {len(chk_list)} active checkpoint")

        # 4. Restore and Assert Round-Trip Fidelity
        restored_1 = test_engine.restore(id_1)
        assert restored_1.snapshot_id == cycle_1_snapshot.snapshot_id, "Snapshot ID mismatch"
        assert restored_1.cycle_number == cycle_1_snapshot.cycle_number, "Cycle number mismatch"
        assert restored_1.learning_engine_state["policy_parameters"] == cycle_1_snapshot.learning_engine_state["policy_parameters"], "Parameters mismatch"
        assert restored_1.calibration_state["drift_baselines"] == cycle_1_snapshot.calibration_state["drift_baselines"], "Baselines mismatch"
        print("[OK] Round-trip fidelity verified: all dataclass fields identical")

        # 5. Checkpoint Cycle 2 with Drift & Hyperparameter Updates
        cycle_2_snapshot = copy.deepcopy(cycle_1_snapshot)
        cycle_2_snapshot.snapshot_id = str(uuid.uuid4())
        cycle_2_snapshot.cycle_number = 2
        cycle_2_snapshot.supervisor_phase = "FEEDBACK"
        # Mutate parameters
        cycle_2_snapshot.learning_engine_state["policy_parameters"][0] += 0.05
        cycle_2_snapshot.learning_engine_state["policy_parameters"][1] -= 0.08
        cycle_2_snapshot.calibration_state["cumulative_drift_magnitude"] = 0.078
        cycle_2_snapshot.calibration_state["drift_baselines"]["latency_p95"] = 46.8
        cycle_2_snapshot.meta_controller_state["learning_rate"] = 0.004
        id_2 = test_engine.checkpoint(cycle_2_snapshot)
        print(f"[OK] Checkpoint created for Cycle 2: {id_2}")

        # 6. Test Diff Engine
        diff_report = test_engine.diff_checkpoints(id_1, id_2)
        assert diff_report["cycle_delta"] == 1, "Expected cycle delta of 1"
        assert diff_report["learning_engine"]["policy_parameters"]["l2_norm_delta"] > 0, "Expected non-zero L2 norm delta"
        print(f"[OK] Checkpoint diff verified: L2 norm parameter delta = {diff_report['learning_engine']['policy_parameters']['l2_norm_delta']:.4f}")
        print(f"     Latency drift delta: {diff_report['calibration']['drift_baselines']['latency_p95']['delta']:.2f} ms")
        print(f"     Learning rate adjusted: {diff_report['meta_controller']['learning_rate']['percentage_change']:.2f}%")

        # 7. Test Rollback Engine
        rolled_back = test_engine.rollback(target_cycle=1)
        assert rolled_back.cycle_number == 1, "Rollback cycle target failed"
        assert len(test_engine.list_checkpoints()) == 1, "Rollback failed to truncate later checkpoints"
        print(f"[OK] Rollback verified: truncated cycle 2, active cycle now {rolled_back.cycle_number}")

        # 8. Test Integration Hooks
        hook_snap_id = checkpoint_after_cycle(cycle_1_snapshot.to_dict(), engine=test_engine)
        assert hook_snap_id is not None
        startup_restored = restore_on_startup(engine=test_engine)
        assert startup_restored is not None
        print(f"[OK] Integration hooks verified: checkpoint_after_cycle & restore_on_startup succeeded")

    print("================================================================================")
    print("ALL STATE CHECKPOINT VERIFICATIONS PASSED SUCCESSFULLY.")
    print("================================================================================")