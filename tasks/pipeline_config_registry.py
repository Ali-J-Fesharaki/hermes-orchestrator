"""tasks/pipeline_config_registry.py

Pipeline Autonomic Configuration Schema Registry & Versioned Parameter Catalog Engine.
Provides a centralized, single source of truth for all tunable pipeline parameters with
type-safe schema enforcement, cross-parameter invariant constraints, cryptographic
SHA-256 hash-chained version history, parameter-level diff tracking, rollback support,
and thread-safe atomic persistence.
"""

from __future__ import annotations

import os
import sys
import json
import time
import uuid
import math
import hashlib
import tempfile
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Literal, Union


# ============================================================================
# Core Data Models
# ============================================================================

@dataclass
class ParameterSchema:
    """Schema definition for a tunable pipeline parameter.

    Attributes:
        param_key: Unique parameter identifier string.
        dtype: Primitive data type ('int', 'float', 'bool', 'str').
        default_value: Canonical default value conforming to dtype.
        min_value: Optional lower bound (for int/float).
        max_value: Optional upper bound (for int/float).
        step: Optional quantization step increment (for int/float).
        description: Human-readable documentation of parameter semantics.
        owning_subsystem: Subsystem possessing primary governance over parameter.
        tags: Immutable set of metadata tags for taxonomy and filtering.
        constraints: Parameter-specific cross-parameter invariant predicates.
    """
    param_key: str
    dtype: Literal['int', 'float', 'bool', 'str']
    default_value: Any
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    step: Optional[float] = None
    description: str = ""
    owning_subsystem: str = "core"
    tags: frozenset[str] = field(default_factory=frozenset)
    constraints: list[Callable[[dict[str, Any]], bool]] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not isinstance(self.tags, frozenset):
            object.__setattr__(self, 'tags', frozenset(self.tags))


@dataclass
class ValidationResult:
    """Outcome of configuration schema and constraint validation.

    Attributes:
        valid: True if zero fatal errors were encountered.
        errors: List of fatal validation error messages.
        warnings: List of advisory non-fatal warning messages.
    """
    valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def __str__(self) -> str:
        status = "PASSED" if self.valid else "FAILED"
        err_msg = f" | {len(self.errors)} error(s)" if self.errors else ""
        warn_msg = f" | {len(self.warnings)} warning(s)" if self.warnings else ""
        return f"ValidationResult({status}{err_msg}{warn_msg})"


@dataclass
class ConfigVersion:
    """Cryptographically chained immutable snapshot of a pipeline configuration.

    Attributes:
        version_id: Unique UUID identifier string for this configuration version.
        timestamp: Epoch timestamp (seconds) when version was committed.
        parameters: Complete key-value dictionary of configuration values.
        parent_version_id: UUID of direct parent version (None for root genesis).
        diff_from_parent: Map of param_key -> (old_value, new_value) relative to parent.
        applied_by: Identifier of operator, subagent, or autonomic controller.
        rationale: Structured explanation or justification for configuration revision.
        sha256_hash: SHA-256 digest over parent_hash || serialized_canonical_config.
    """
    version_id: str
    timestamp: float
    parameters: dict[str, Any]
    parent_version_id: Optional[str]
    diff_from_parent: dict[str, tuple[Any, Any]]
    applied_by: str
    rationale: str
    sha256_hash: str


@dataclass
class ConfigConstraint:
    """Cross-parameter invariant constraint definition.

    Attributes:
        name: Unique constraint identifier.
        param_keys: List of parameter keys evaluated by the predicate.
        predicate: Callable taking full config dict -> bool (True if valid).
        description: Detailed rationale of invariant failure condition.
        severity: Invariant breach severity level ('error' or 'warning').
    """
    name: str
    param_keys: list[str]
    predicate: Callable[[dict[str, Any]], bool]
    description: str
    severity: Literal['error', 'warning'] = 'error'


# ============================================================================
# Configuration Schema Registry Engine
# ============================================================================

class ConfigSchemaRegistry:
    """Centralized schema registry, invariant validator, and versioned parameter engine."""

    GENESIS_PARENT_HASH: str = "0" * 64

    def __init__(self, version_log_path: str = "logs/config_versions.jsonl") -> None:
        """Initialize the schema registry and load existing version history.

        Args:
            version_log_path: Filesystem path to JSONL version journal.
        """
        self.version_log_path = version_log_path
        self._lock = threading.Lock()
        self._schemas: dict[str, ParameterSchema] = {}
        self._constraints: list[ConfigConstraint] = []
        self._versions: dict[str, ConfigVersion] = {}
        self._version_history: list[ConfigVersion] = []
        self._head_version_id: Optional[str] = None

        # Bootstrap canonical schema definitions and system invariants
        self._register_canonical_parameters()
        self._register_canonical_constraints()

        # Load persisted history if journal file exists
        self._load_version_history()

    # ------------------------------------------------------------------------
    # Registration APIs
    # ------------------------------------------------------------------------

    def register_parameter(self, schema: ParameterSchema) -> None:
        """Register a parameter schema with strict type and boundary verification.

        Args:
            schema: ParameterSchema instance to register.

        Raises:
            ValueError: If parameter key already registered or schema definition is invalid.
        """
        with self._lock:
            key = schema.param_key
            if not key or not isinstance(key, str):
                raise ValueError("Parameter key must be a non-empty string.")

            if key in self._schemas:
                raise ValueError(f"Parameter '{key}' is already registered in registry.")

            valid_dtypes = ('int', 'float', 'bool', 'str')
            if schema.dtype not in valid_dtypes:
                raise ValueError(f"Invalid dtype '{schema.dtype}' for '{key}'. Allowed: {valid_dtypes}")

            # Verify default value matches declared dtype
            self._assert_dtype_compliance(key, schema.default_value, schema.dtype, "default_value")

            # Verify range bounds consistency
            if schema.min_value is not None and schema.max_value is not None:
                if schema.min_value > schema.max_value:
                    raise ValueError(f"Parameter '{key}' min_value ({schema.min_value}) > max_value ({schema.max_value})")

            if schema.min_value is not None and isinstance(schema.default_value, (int, float)):
                if schema.default_value < schema.min_value:
                    raise ValueError(f"Parameter '{key}' default ({schema.default_value}) < min_value ({schema.min_value})")

            if schema.max_value is not None and isinstance(schema.default_value, (int, float)):
                if schema.default_value > schema.max_value:
                    raise ValueError(f"Parameter '{key}' default ({schema.default_value}) > max_value ({schema.max_value})")

            # Verify step specification
            if schema.step is not None:
                if schema.step <= 0:
                    raise ValueError(f"Parameter '{key}' step quantization must be strictly positive.")

            self._schemas[key] = schema

    def register_constraint(
        self,
        name: str,
        param_keys: list[str],
        predicate: Callable[[dict[str, Any]], bool],
        description: str,
        severity: Literal['error', 'warning'] = 'error'
    ) -> None:
        """Register a cross-parameter invariant predicate.

        Args:
            name: Constraint identifier.
            param_keys: Parameter keys relevant to this evaluation.
            predicate: Callable taking dict[str, Any] returning bool.
            description: Narrative explaining why the invariant exists.
            severity: 'error' (prevents commit) or 'warning' (advisory flag).
        """
        with self._lock:
            if severity not in ('error', 'warning'):
                raise ValueError(f"Invalid constraint severity '{severity}'. Allowed: 'error', 'warning'")
            constraint = ConfigConstraint(
                name=name,
                param_keys=list(param_keys),
                predicate=predicate,
                description=description,
                severity=severity
            )
            self._constraints.append(constraint)

    # ------------------------------------------------------------------------
    # Validation APIs
    # ------------------------------------------------------------------------

    def validate_config(self, config: dict[str, Any]) -> ValidationResult:
        """Validate candidate configuration against parameter schemas and invariant constraints.

        Args:
            config: Full or partial parameter configuration dictionary.

        Returns:
            ValidationResult containing status boolean, error list, and warning list.
        """
        with self._lock:
            errors: list[str] = []
            warnings: list[str] = []

            # Check unknown parameters
            for key in config.keys():
                if key not in self._schemas:
                    warnings.append(f"Unrecognized parameter '{key}' not registered in schema catalog.")

            # Evaluate each provided parameter against its registered schema
            for key, val in config.items():
                if key not in self._schemas:
                    continue
                schema = self._schemas[key]

                # Type check
                if schema.dtype == 'int':
                    if not isinstance(val, int) or isinstance(val, bool):
                        errors.append(f"Parameter '{key}' expected int, got {type(val).__name__} ({val!r}).")
                        continue
                elif schema.dtype == 'float':
                    if not isinstance(val, (int, float)) or isinstance(val, bool):
                        errors.append(f"Parameter '{key}' expected float, got {type(val).__name__} ({val!r}).")
                        continue
                elif schema.dtype == 'bool':
                    if not isinstance(val, bool):
                        errors.append(f"Parameter '{key}' expected bool, got {type(val).__name__} ({val!r}).")
                        continue
                elif schema.dtype == 'str':
                    if not isinstance(val, str):
                        errors.append(f"Parameter '{key}' expected str, got {type(val).__name__} ({val!r}).")
                        continue

                # Numeric range checks
                if schema.dtype in ('int', 'float') and isinstance(val, (int, float)) and not isinstance(val, bool):
                    if schema.min_value is not None and val < schema.min_value:
                        errors.append(f"Parameter '{key}' value {val} is below minimum allowed {schema.min_value}.")
                    if schema.max_value is not None and val > schema.max_value:
                        errors.append(f"Parameter '{key}' value {val} exceeds maximum allowed {schema.max_value}.")

                    # Step quantization check
                    if schema.step is not None:
                        base = schema.min_value if schema.min_value is not None else 0.0
                        offset = val - base
                        steps = offset / schema.step
                        is_quantized = math.isclose(steps, round(steps), rel_tol=1e-6, abs_tol=1e-6)
                        if not is_quantized:
                            errors.append(
                                f"Parameter '{key}' value {val} violates step quantization of {schema.step} "
                                f"(offset {offset:.4f} is not an integer multiple of step)."
                            )

                # Schema-level specific predicates
                for p_idx, pred in enumerate(schema.constraints):
                    try:
                        if not pred(config):
                            errors.append(f"Parameter '{key}' failed localized constraint predicate #{p_idx + 1}.")
                    except Exception as exc:
                        errors.append(f"Parameter '{key}' constraint predicate evaluation error: {exc}")

            # Prepare full configuration view with defaults for cross-parameter evaluation
            full_view = self._get_default_config_unlocked()
            full_view.update(config)

            # Evaluate system-wide cross-parameter constraints
            for c in self._constraints:
                try:
                    satisfied = c.predicate(full_view)
                    if not satisfied:
                        msg = f"Constraint '{c.name}' violated: {c.description}"
                        if c.severity == 'warning':
                            warnings.append(msg)
                        else:
                            errors.append(msg)
                except Exception as exc:
                    errors.append(f"Constraint '{c.name}' evaluation encountered unhandled exception: {exc}")

            return ValidationResult(
                valid=(len(errors) == 0),
                errors=errors,
                warnings=warnings
            )

    # ------------------------------------------------------------------------
    # Version Management & Cryptographic Hash-Chaining
    # ------------------------------------------------------------------------

    def commit_version(self, config: dict[str, Any], applied_by: str, rationale: str) -> ConfigVersion:
        """Validate config, compute diff from HEAD, generate SHA-256 hash chain, and commit.

        Args:
            config: Candidate configuration dictionary.
            applied_by: Identifier of entity applying configuration.
            rationale: Contextual rationale for parameter revision.

        Returns:
            Newly created and persisted ConfigVersion instance.

        Raises:
            ValueError: If candidate configuration fails validation.
        """
        with self._lock:
            # Enforce validation
            val_res = self.validate_config(config)
            if not val_res.valid:
                error_summary = "; ".join(val_res.errors)
                raise ValueError(f"Cannot commit invalid configuration: {error_summary}")

            # Merge with default config to ensure complete parameter catalog state
            effective_config = self._get_default_config_unlocked()
            effective_config.update(config)

            # Determine parent version and compute parameter-level diff
            parent_version = self._versions.get(self._head_version_id) if self._head_version_id else None
            parent_version_id = parent_version.version_id if parent_version else None
            parent_hash = parent_version.sha256_hash if parent_version else self.GENESIS_PARENT_HASH

            diff_from_parent: dict[str, tuple[Any, Any]] = {}
            if parent_version is not None:
                parent_params = parent_version.parameters
                all_keys = set(parent_params.keys()) | set(effective_config.keys())
                for k in sorted(all_keys):
                    old_v = parent_params.get(k)
                    new_v = effective_config.get(k)
                    if old_v != new_v:
                        diff_from_parent[k] = (old_v, new_v)
            else:
                # Genesis version diff records initial parameter establishment
                for k in sorted(effective_config.keys()):
                    diff_from_parent[k] = (None, effective_config[k])

            # Generate version metadata and deterministic cryptographic hash
            version_id = str(uuid.uuid4())
            timestamp = time.time()
            canonical_json = json.dumps(effective_config, sort_keys=True, separators=(',', ':'))
            hash_payload = f"{parent_hash}||{canonical_json}".encode('utf-8')
            sha256_hash = hashlib.sha256(hash_payload).hexdigest()

            new_version = ConfigVersion(
                version_id=version_id,
                timestamp=timestamp,
                parameters=effective_config,
                parent_version_id=parent_version_id,
                diff_from_parent=diff_from_parent,
                applied_by=applied_by,
                rationale=rationale,
                sha256_hash=sha256_hash
            )

            # Register in-memory
            self._versions[version_id] = new_version
            self._version_history.append(new_version)
            self._head_version_id = version_id

            # Persist atomically to disk
            self._persist_history_atomic()

            return new_version

    def get_version(self, version_id: str) -> Optional[ConfigVersion]:
        """Retrieve a specific configuration version by UUID."""
        with self._lock:
            return self._versions.get(version_id)

    def get_history(self, n: int = 20) -> list[ConfigVersion]:
        """Retrieve the last N configuration versions in chronological order."""
        with self._lock:
            if n <= 0:
                return []
            return list(self._version_history[-n:])

    def diff_versions(self, v1_id: str, v2_id: str) -> dict[str, tuple[Any, Any]]:
        """Compute parameter-level diff between any two configuration versions.

        Args:
            v1_id: Base version UUID.
            v2_id: Target version UUID.

        Returns:
            Map of parameter key -> (v1_value, v2_value) for all differing parameters.

        Raises:
            KeyError: If either version is not found in registry.
        """
        with self._lock:
            v1 = self._versions.get(v1_id)
            if not v1:
                raise KeyError(f"Configuration version '{v1_id}' not found.")
            v2 = self._versions.get(v2_id)
            if not v2:
                raise KeyError(f"Configuration version '{v2_id}' not found.")

            diff: dict[str, tuple[Any, Any]] = {}
            all_keys = set(v1.parameters.keys()) | set(v2.parameters.keys())
            for k in sorted(all_keys):
                val1 = v1.parameters.get(k)
                val2 = v2.parameters.get(k)
                if val1 != val2:
                    diff[k] = (val1, val2)
            return diff

    def get_current_config(self) -> dict[str, Any]:
        """Return the active HEAD configuration parameters dictionary."""
        with self._lock:
            if self._head_version_id and self._head_version_id in self._versions:
                return dict(self._versions[self._head_version_id].parameters)
            return self._get_default_config_unlocked()

    def rollback_to(self, version_id: str, rationale: str = "") -> ConfigVersion:
        """Create and commit a new configuration version matching target version's parameters.

        Args:
            version_id: UUID of historical target configuration version.
            rationale: Explanation of rollback decision.

        Returns:
            The newly committed ConfigVersion representing the rolled-back state.

        Raises:
            KeyError: If target version_id does not exist.
        """
        with self._lock:
            target = self._versions.get(version_id)
            if not target:
                raise KeyError(f"Target rollback version '{version_id}' does not exist.")
            target_params = dict(target.parameters)

        default_rationale = f"Autonomic rollback to configuration version {version_id}"
        effective_rationale = rationale if rationale else default_rationale

        return self.commit_version(
            config=target_params,
            applied_by="autonomic_rollback_controller",
            rationale=effective_rationale
        )

    def export_schema_catalog(self) -> dict[str, Any]:
        """Export all registered schemas and invariants as a JSON-serializable catalog dictionary."""
        with self._lock:
            subsystems = sorted(list({s.owning_subsystem for s in self._schemas.values()}))
            catalog: dict[str, Any] = {
                "total_parameters": len(self._schemas),
                "subsystems": subsystems,
                "parameters": {},
                "constraints": []
            }

            for key, s in self._schemas.items():
                catalog["parameters"][key] = {
                    "param_key": s.param_key,
                    "dtype": s.dtype,
                    "default_value": s.default_value,
                    "min_value": s.min_value,
                    "max_value": s.max_value,
                    "step": s.step,
                    "description": s.description,
                    "owning_subsystem": s.owning_subsystem,
                    "tags": sorted(list(s.tags)),
                }

            for c in self._constraints:
                catalog["constraints"].append({
                    "name": c.name,
                    "param_keys": c.param_keys,
                    "description": c.description,
                    "severity": c.severity
                })

            return catalog

    def get_default_config(self) -> dict[str, Any]:
        """Return a mapping of all registered parameters to their schema default values."""
        with self._lock:
            return self._get_default_config_unlocked()

    # ------------------------------------------------------------------------
    # Internal Helpers
    # ------------------------------------------------------------------------

    def _get_default_config_unlocked(self) -> dict[str, Any]:
        """Helper to get default config without re-acquiring lock."""
        return {key: schema.default_value for key, schema in self._schemas.items()}

    @staticmethod
    def _assert_dtype_compliance(key: str, val: Any, dtype: str, field_name: str) -> None:
        """Validate primitive type match."""
        if dtype == 'int':
            if not isinstance(val, int) or isinstance(val, bool):
                raise ValueError(f"Parameter '{key}' {field_name} must be int, got {type(val).__name__} ({val!r})")
        elif dtype == 'float':
            if not isinstance(val, (int, float)) or isinstance(val, bool):
                raise ValueError(f"Parameter '{key}' {field_name} must be float, got {type(val).__name__} ({val!r})")
        elif dtype == 'bool':
            if not isinstance(val, bool):
                raise ValueError(f"Parameter '{key}' {field_name} must be bool, got {type(val).__name__} ({val!r})")
        elif dtype == 'str':
            if not isinstance(val, str):
                raise ValueError(f"Parameter '{key}' {field_name} must be str, got {type(val).__name__} ({val!r})")

    def _persist_history_atomic(self) -> None:
        """Persist in-memory version history atomically to disk using tempfile + os.replace."""
        log_dir = os.path.dirname(os.path.abspath(self.version_log_path))
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        temp_file = tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=log_dir,
            delete=False,
            prefix="cfg_ver_",
            suffix=".tmp"
        )
        temp_path = temp_file.name
        try:
            for v in self._version_history:
                record = {
                    "version_id": v.version_id,
                    "timestamp": v.timestamp,
                    "parameters": v.parameters,
                    "parent_version_id": v.parent_version_id,
                    "diff_from_parent": {k: list(pair) for k, pair in v.diff_from_parent.items()},
                    "applied_by": v.applied_by,
                    "rationale": v.rationale,
                    "sha256_hash": v.sha256_hash,
                }
                temp_file.write(json.dumps(record, sort_keys=True) + "\n")
            temp_file.flush()
            os.fsync(temp_file.fileno())
            temp_file.close()
            os.replace(temp_path, self.version_log_path)
        except Exception:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass
            raise

    def _load_version_history(self) -> None:
        """Load and verify existing versions from JSONL journal file."""
        if not os.path.exists(self.version_log_path):
            return

        with self._lock:
            try:
                with open(self.version_log_path, "r", encoding="utf-8") as f:
                    for line_idx, line in enumerate(f):
                        line = line.strip()
                        if not line:
                            continue
                        data = json.loads(line)
                        diff = {
                            k: (pair[0], pair[1])
                            for k, pair in data.get("diff_from_parent", {}).items()
                        }
                        version = ConfigVersion(
                            version_id=data["version_id"],
                            timestamp=float(data["timestamp"]),
                            parameters=data["parameters"],
                            parent_version_id=data.get("parent_version_id"),
                            diff_from_parent=diff,
                            applied_by=data.get("applied_by", "unknown"),
                            rationale=data.get("rationale", ""),
                            sha256_hash=data["sha256_hash"]
                        )
                        self._versions[version.version_id] = version
                        self._version_history.append(version)
                        self._head_version_id = version.version_id
            except Exception as exc:
                sys.stderr.write(f"[ConfigSchemaRegistry] Warning: Failed to load version journal: {exc}\n")

    # ------------------------------------------------------------------------
    # Canonical Subsystem Schemas & Invariant Constraints
    # ------------------------------------------------------------------------

    def _register_canonical_parameters(self) -> None:
        """Register the canonical suite of ~18 tunable pipeline parameters."""
        canonical_params: list[ParameterSchema] = [
            ParameterSchema(
                param_key="CONCURRENCY_LIMIT",
                dtype="int",
                default_value=3,
                min_value=1.0,
                max_value=8.0,
                step=1.0,
                description="Maximum concurrent asynchronous worker tasks executed by pipeline.",
                owning_subsystem="governor",
                tags=frozenset({"concurrency", "governor", "capacity"})
            ),
            ParameterSchema(
                param_key="FEASIBILITY_GATE_PASS_THRESHOLD",
                dtype="float",
                default_value=0.6,
                min_value=0.3,
                max_value=0.9,
                step=0.05,
                description="Minimum composite score threshold required to pass feasibility gate.",
                owning_subsystem="gate",
                tags=frozenset({"feasibility", "gate", "threshold"})
            ),
            ParameterSchema(
                param_key="DECOMPOSITION_MAX_DEPTH",
                dtype="int",
                default_value=3,
                min_value=1.0,
                max_value=5.0,
                step=1.0,
                description="Maximum recursive depth permitted during hierarchical task decomposition.",
                owning_subsystem="planner",
                tags=frozenset({"planner", "decomposition", "recursion"})
            ),
            ParameterSchema(
                param_key="HOLDING_QUEUE_RETRY_INTERVAL_SEC",
                dtype="float",
                default_value=120.0,
                min_value=30.0,
                max_value=600.0,
                step=30.0,
                description="Base delay in seconds between retries of held pipeline tasks.",
                owning_subsystem="queue",
                tags=frozenset({"queue", "retry", "scheduling"})
            ),
            ParameterSchema(
                param_key="GOVERNOR_THROTTLE_BURN_RATIO",
                dtype="float",
                default_value=0.75,
                min_value=0.5,
                max_value=0.95,
                step=0.05,
                description="Quota burn velocity threshold ratio triggering proactive dispatch throttling.",
                owning_subsystem="governor",
                tags=frozenset({"governor", "quota", "throttle"})
            ),
            ParameterSchema(
                param_key="GOVERNOR_PREEMPT_BUDGET_MULTIPLIER",
                dtype="float",
                default_value=2.0,
                min_value=1.2,
                max_value=3.0,
                step=0.1,
                description="Multiplier on mean task cost required to permit preemption bypass.",
                owning_subsystem="governor",
                tags=frozenset({"governor", "preemption", "tokens"})
            ),
            ParameterSchema(
                param_key="PID_KP",
                dtype="float",
                default_value=0.5,
                min_value=0.01,
                max_value=2.0,
                step=0.01,
                description="Proportional gain coefficient for pacing rate controller.",
                owning_subsystem="controller",
                tags=frozenset({"controller", "pid", "pacing"})
            ),
            ParameterSchema(
                param_key="PID_KI",
                dtype="float",
                default_value=0.1,
                min_value=0.0,
                max_value=1.0,
                step=0.01,
                description="Integral gain coefficient for pacing rate controller.",
                owning_subsystem="controller",
                tags=frozenset({"controller", "pid", "pacing"})
            ),
            ParameterSchema(
                param_key="PID_KD",
                dtype="float",
                default_value=0.05,
                min_value=0.0,
                max_value=1.0,
                step=0.01,
                description="Derivative gain coefficient for pacing rate controller.",
                owning_subsystem="controller",
                tags=frozenset({"controller", "pid", "pacing"})
            ),
            ParameterSchema(
                param_key="EWMA_ALPHA",
                dtype="float",
                default_value=0.1,
                min_value=0.01,
                max_value=0.5,
                step=0.01,
                description="Smoothing factor for exponential weighted moving average telemetry filters.",
                owning_subsystem="telemetry",
                tags=frozenset({"telemetry", "ewma", "filter"})
            ),
            ParameterSchema(
                param_key="CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD",
                dtype="float",
                default_value=0.5,
                min_value=0.1,
                max_value=0.9,
                step=0.05,
                description="Failure rate ratio triggering circuit breaker tripped state.",
                owning_subsystem="circuit_breaker",
                tags=frozenset({"resilience", "circuit_breaker", "safety"})
            ),
            ParameterSchema(
                param_key="CIRCUIT_BREAKER_COOLDOWN_SEC",
                dtype="float",
                default_value=120.0,
                min_value=30.0,
                max_value=600.0,
                step=10.0,
                description="Duration in seconds before tripped circuit breaker enters half-open recovery.",
                owning_subsystem="circuit_breaker",
                tags=frozenset({"resilience", "circuit_breaker", "recovery"})
            ),
            ParameterSchema(
                param_key="CONSENSUS_WINDOW_SEC",
                dtype="float",
                default_value=30.0,
                min_value=10.0,
                max_value=120.0,
                step=5.0,
                description="Time window in seconds allowed for gathering multi-agent consensus votes.",
                owning_subsystem="consensus",
                tags=frozenset({"consensus", "quorum", "timing"})
            ),
            ParameterSchema(
                param_key="LEARNING_RATE",
                dtype="float",
                default_value=0.05,
                min_value=0.001,
                max_value=0.5,
                step=0.001,
                description="Step size parameter for autonomic feedback policy adaptation.",
                owning_subsystem="optimizer",
                tags=frozenset({"optimizer", "learning", "adaptation"})
            ),
            ParameterSchema(
                param_key="SIMULATION_ENSEMBLE_SIZE",
                dtype="int",
                default_value=200,
                min_value=50.0,
                max_value=1000.0,
                step=10.0,
                description="Number of Monte Carlo trajectories generated in feasibility simulations.",
                owning_subsystem="simulation",
                tags=frozenset({"simulation", "monte_carlo", "planning"})
            ),
            ParameterSchema(
                param_key="AUTHORITY_WEIGHT_CORE",
                dtype="float",
                default_value=1.0,
                min_value=0.1,
                max_value=2.0,
                step=0.1,
                description="Voting authority weight assigned to core orchestrator node.",
                owning_subsystem="authority",
                tags=frozenset({"authority", "voting", "core"})
            ),
            ParameterSchema(
                param_key="AUTHORITY_WEIGHT_GOVERNOR",
                dtype="float",
                default_value=1.0,
                min_value=0.1,
                max_value=2.0,
                step=0.1,
                description="Voting authority weight assigned to governor throttle node.",
                owning_subsystem="authority",
                tags=frozenset({"authority", "voting", "governor"})
            ),
            ParameterSchema(
                param_key="AUTHORITY_WEIGHT_GATEWAY",
                dtype="float",
                default_value=1.0,
                min_value=0.1,
                max_value=2.0,
                step=0.1,
                description="Voting authority weight assigned to policy promotion gateway.",
                owning_subsystem="authority",
                tags=frozenset({"authority", "voting", "gateway"})
            )
        ]

        for p in canonical_params:
            self._schemas[p.param_key] = p

    def _register_canonical_constraints(self) -> None:
        """Register canonical multi-parameter invariant constraints."""

        def _pid_integral_requires_proportional(cfg: dict[str, Any]) -> bool:
            ki = cfg.get("PID_KI", 0.0)
            kp = cfg.get("PID_KP", 0.0)
            if ki > 0.0 and kp <= 0.0:
                return False
            return True

        self.register_constraint(
            name="pid_integral_requires_proportional",
            param_keys=["PID_KP", "PID_KI"],
            predicate=_pid_integral_requires_proportional,
            description="PID integral gain KI > 0 requires proportional gain KP > 0 to prevent integrator windup.",
            severity="error"
        )

        def _cooldown_exceeds_retry(cfg: dict[str, Any]) -> bool:
            cooldown = cfg.get("CIRCUIT_BREAKER_COOLDOWN_SEC", 120.0)
            retry = cfg.get("HOLDING_QUEUE_RETRY_INTERVAL_SEC", 120.0)
            return cooldown >= retry

        self.register_constraint(
            name="cooldown_exceeds_retry",
            param_keys=["CIRCUIT_BREAKER_COOLDOWN_SEC", "HOLDING_QUEUE_RETRY_INTERVAL_SEC"],
            predicate=_cooldown_exceeds_retry,
            description="CIRCUIT_BREAKER_COOLDOWN_SEC should be >= HOLDING_QUEUE_RETRY_INTERVAL_SEC to prevent premature retries during trip.",
            severity="warning"
        )

        def _concurrency_token_quota_limit(cfg: dict[str, Any]) -> bool:
            # Soft warning: CONCURRENCY_LIMIT * mean_token_cost_per_slot (15,000) <= max_quota_window (100,000)
            concurrency = cfg.get("CONCURRENCY_LIMIT", 3)
            mean_token_cost = 15000
            quota_limit = 100000
            return (concurrency * mean_token_cost) <= quota_limit

        self.register_constraint(
            name="concurrency_token_quota_limit",
            param_keys=["CONCURRENCY_LIMIT"],
            predicate=_concurrency_token_quota_limit,
            description="Estimated concurrency token consumption exceeds window quota budget (CONCURRENCY_LIMIT * 15k > 100k).",
            severity="warning"
        )


# ============================================================================
# Self-Test Suite
# ============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("RUNNING COMPREHENSIVE SELF-TEST: tasks/pipeline_config_registry.py")
    print("=" * 80)

    test_dir = tempfile.mkdtemp(prefix="hermes_cfg_reg_test_")
    log_path = os.path.join(test_dir, "test_config_versions.jsonl")

    try:
        # 1. Initialization and Catalog Registration
        print("\n[1] Testing Registry Initialization & Schema Catalog...")
        registry = ConfigSchemaRegistry(version_log_path=log_path)
        catalog = registry.export_schema_catalog()
        print(f"    - Pre-registered parameters: {catalog['total_parameters']}")
        print(f"    - Subsystems covered: {catalog['subsystems']}")
        assert catalog["total_parameters"] >= 15, "Expected at least 15 pre-registered parameters."
        assert "governor" in catalog["subsystems"]
        assert "circuit_breaker" in catalog["subsystems"]
        print("    [PASS] Schema catalog verified.")

        # 2. Dynamic Parameter Registration & Uniqueness Enforcement
        print("\n[2] Testing Dynamic Parameter Registration & Validation...")
        custom_schema = ParameterSchema(
            param_key="EXPERIMENTAL_FLAG_ALPHA",
            dtype="bool",
            default_value=False,
            description="Experimental autonomic routing switch.",
            owning_subsystem="experimental",
            tags=frozenset({"test", "beta"})
        )
        registry.register_parameter(custom_schema)
        assert "EXPERIMENTAL_FLAG_ALPHA" in registry.get_default_config()

        duplicate_detected = False
        try:
            registry.register_parameter(custom_schema)
        except ValueError:
            duplicate_detected = True
        assert duplicate_detected, "Registry failed to reject duplicate parameter key."

        invalid_type_detected = False
        try:
            registry.register_parameter(
                ParameterSchema(
                    param_key="BAD_FLOAT",
                    dtype="float",
                    default_value="not_a_float",
                    description="Test bad float"
                )
            )
        except ValueError:
            invalid_type_detected = True
        assert invalid_type_detected, "Registry accepted incompatible default value for dtype."
        print("    [PASS] Parameter registration and constraints verified.")

        # 3. Default Configuration & Validation
        print("\n[3] Testing Default Configuration Validation...")
        defaults = registry.get_default_config()
        val_default = registry.validate_config(defaults)
        print(f"    - Default config validation: {val_default}")
        assert val_default.valid, f"Default configuration failed validation: {val_default.errors}"
        assert len(val_default.errors) == 0
        print("    [PASS] Default configuration validated.")

        # 4. Invariant and Boundary Violation Checks
        print("\n[4] Testing Type, Boundary, Step, and Invariant Violations...")

        # Type violation: int given string
        res_type = registry.validate_config({"CONCURRENCY_LIMIT": "four"})
        assert not res_type.valid
        assert any("expected int" in err for err in res_type.errors)

        # Range violation: min / max
        res_min = registry.validate_config({"CONCURRENCY_LIMIT": 0})
        assert not res_min.valid
        assert any("below minimum" in err for err in res_min.errors)

        res_max = registry.validate_config({"CONCURRENCY_LIMIT": 10})
        assert not res_max.valid
        assert any("exceeds maximum" in err for err in res_max.errors)

        # Step violation: FEASIBILITY_GATE_PASS_THRESHOLD step is 0.05
        res_step = registry.validate_config({"FEASIBILITY_GATE_PASS_THRESHOLD": 0.43})
        assert not res_step.valid
        assert any("violates step quantization" in err for err in res_step.errors)

        # Cross-parameter invariant error: PID_KI > 0 without PID_KP > 0
        res_pid_err = registry.validate_config({"PID_KI": 0.5, "PID_KP": 0.0})
        assert not res_pid_err.valid
        assert any("pid_integral_requires_proportional" in err for err in res_pid_err.errors)

        # Cross-parameter invariant warning: cooldown < retry
        res_cooldown_warn = registry.validate_config({
            "CIRCUIT_BREAKER_COOLDOWN_SEC": 30.0,
            "HOLDING_QUEUE_RETRY_INTERVAL_SEC": 120.0
        })
        assert res_cooldown_warn.valid  # Warnings do not invalidate configuration
        assert any("cooldown_exceeds_retry" in warn for warn in res_cooldown_warn.warnings)

        # Soft warning: high concurrency quota consumption
        res_concurrency_warn = registry.validate_config({"CONCURRENCY_LIMIT": 8})
        assert res_concurrency_warn.valid
        assert any("concurrency_token_quota_limit" in warn for warn in res_concurrency_warn.warnings)

        print("    [PASS] All boundary, type, step, and invariant checks verified.")

        # 5. Version Commit, Diff Tracking & Cryptographic Hash Chaining
        print("\n[5] Testing Version Commit, Diffs & SHA-256 Hash Chaining...")
        v1 = registry.commit_version(
            config=defaults,
            applied_by="system_bootstrap",
            rationale="Initial genesis baseline configuration"
        )
        assert v1.parent_version_id is None
        assert v1.sha256_hash is not None
        assert len(v1.sha256_hash) == 64
        print(f"    - Committed V1 (Genesis): ID={v1.version_id[:8]} Hash={v1.sha256_hash[:16]}...")

        # Modify parameters for V2
        v2_params = dict(defaults)
        v2_params["CONCURRENCY_LIMIT"] = 5
        v2_params["FEASIBILITY_GATE_PASS_THRESHOLD"] = 0.70
        v2_params["PID_KP"] = 0.8

        v2 = registry.commit_version(
            config=v2_params,
            applied_by="autonomic_rate_governor",
            rationale="Pacing scale-up for high throughput workload"
        )
        assert v2.parent_version_id == v1.version_id
        assert v2.diff_from_parent["CONCURRENCY_LIMIT"] == (3, 5)
        assert v2.diff_from_parent["FEASIBILITY_GATE_PASS_THRESHOLD"] == (0.6, 0.7)
        assert v2.diff_from_parent["PID_KP"] == (0.5, 0.8)
        print(f"    - Committed V2: ID={v2.version_id[:8]} Parent={v2.parent_version_id[:8]} DiffKeys={list(v2.diff_from_parent.keys())}")

        # Check hash chaining formula
        expected_v2_payload = f"{v1.sha256_hash}||{json.dumps(v2.parameters, sort_keys=True, separators=(',', ':'))}".encode('utf-8')
        assert v2.sha256_hash == hashlib.sha256(expected_v2_payload).hexdigest()
        print("    [PASS] Version commit, diffing, and cryptographic chain verified.")

        # 6. Version Retrieval, History & Arbitrary Version Diffing
        print("\n[6] Testing History Retrieval & Version Diffs...")
        history = registry.get_history(10)
        assert len(history) == 2
        assert history[-1].version_id == v2.version_id

        fetched_v1 = registry.get_version(v1.version_id)
        assert fetched_v1 is not None and fetched_v1.version_id == v1.version_id

        diff_v1_v2 = registry.diff_versions(v1.version_id, v2.version_id)
        assert diff_v1_v2["CONCURRENCY_LIMIT"] == (3, 5)
        assert diff_v1_v2["PID_KP"] == (0.5, 0.8)
        print(f"    - Version diff between V1 and V2: {diff_v1_v2}")
        print("    [PASS] History retrieval and arbitrary version diff verified.")

        # 7. Autonomic Rollback Engine
        print("\n[7] Testing Autonomic Rollback...")
        v3 = registry.rollback_to(v1.version_id, rationale="Workload burst concluded; revert to baseline")
        assert v3.parent_version_id == v2.version_id
        assert v3.parameters["CONCURRENCY_LIMIT"] == defaults["CONCURRENCY_LIMIT"]
        assert v3.parameters["FEASIBILITY_GATE_PASS_THRESHOLD"] == defaults["FEASIBILITY_GATE_PASS_THRESHOLD"]
        assert registry.get_current_config()["CONCURRENCY_LIMIT"] == defaults["CONCURRENCY_LIMIT"]
        print(f"    - Committed V3 (Rollback): ID={v3.version_id[:8]} Hash={v3.sha256_hash[:16]} Target={v1.version_id[:8]}")
        print("    [PASS] Rollback mechanics confirmed.")

        # 8. Persistence Reload Verification
        print("\n[8] Testing Journal Persistence & Reloading...")
        assert os.path.exists(log_path), "Version journal file was not created on disk."
        registry_reloaded = ConfigSchemaRegistry(version_log_path=log_path)
        reloaded_history = registry_reloaded.get_history(10)
        assert len(reloaded_history) == 3
        assert reloaded_history[0].version_id == v1.version_id
        assert reloaded_history[1].version_id == v2.version_id
        assert reloaded_history[2].version_id == v3.version_id
        assert registry_reloaded.get_current_config()["CONCURRENCY_LIMIT"] == defaults["CONCURRENCY_LIMIT"]
        print(f"    - Successfully reloaded {len(reloaded_history)} versions from {log_path}")
        print("    [PASS] Persistence and state recovery verified.")

        # 9. Concurrency & Thread-Safety Stress Test
        print("\n[9] Testing Thread-Safety & Concurrent Commits...")
        num_threads = 8
        errors_collected: list[Exception] = []

        def worker(thread_idx: int) -> None:
            try:
                for step in range(5):
                    cfg = registry_reloaded.get_current_config()
                    cfg["SIMULATION_ENSEMBLE_SIZE"] = 100 + (thread_idx * 20) + step
                    registry_reloaded.commit_version(
                        config=cfg,
                        applied_by=f"thread_{thread_idx}",
                        rationale=f"Concurrent tuning iteration {step}"
                    )
            except Exception as e:
                errors_collected.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors_collected) == 0, f"Thread errors encountered: {errors_collected}"
        final_history = registry_reloaded.get_history(100)
        print(f"    - Total committed versions after concurrency stress: {len(final_history)}")
        assert len(final_history) == 3 + (num_threads * 5)
        print("    [PASS] Thread-safety and atomic journal persistence verified.")

        print("\n" + "=" * 80)
        print("ALL TESTS PASSED SUCCESSFULLY! ZERO ERRORS.")
        print("=" * 80)

    finally:
        # Cleanup temporary test files
        if os.path.exists(log_path):
            try:
                os.remove(log_path)
            except OSError:
                pass
        if os.path.exists(test_dir):
            try:
                os.rmdir(test_dir)
            except OSError:
                pass