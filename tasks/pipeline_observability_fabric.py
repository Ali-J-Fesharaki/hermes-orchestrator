"""
Pipeline Observability Fabric & Unified Telemetry Audit Trail Emitter.

This module provides cross-cutting observability instrumentation, OpenTelemetry
distributed tracing bridges (Arize Phoenix compatible), and a causal audit ledger
for tracking directive lifecycles across the autonomous multi-repo orchestrator.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, Generator, List, Optional, Set, Tuple

# -----------------------------------------------------------------------------
# OpenTelemetry Graceful Degradation Imports
# -----------------------------------------------------------------------------
try:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.trace import Link, SpanKind, Status, StatusCode, TraceFlags

    OPENTELEMETRY_AVAILABLE = True
except ImportError:
    OPENTELEMETRY_AVAILABLE = False
    trace = None
    TracerProvider = None
    BatchSpanProcessor = None
    OTLPSpanExporter = None
    Resource = None
    Status = None
    StatusCode = None
    SpanKind = None
    Link = None
    TraceFlags = None

logger = logging.getLogger("hermes.observability_fabric")


# -----------------------------------------------------------------------------
# Core Enumerations & Data Structures
# -----------------------------------------------------------------------------
class EventType(str, Enum):
    """Categorical types of observable pipeline telemetry events."""

    DIRECTIVE_PROPOSED = "DIRECTIVE_PROPOSED"
    DIRECTIVE_VALIDATED = "DIRECTIVE_VALIDATED"
    CANARY_STARTED = "CANARY_STARTED"
    CANARY_EVALUATED = "CANARY_EVALUATED"
    PROMOTED = "PROMOTED"
    ROLLED_BACK = "ROLLED_BACK"
    RETROSPECTIVE_LINKED = "RETROSPECTIVE_LINKED"
    CYCLE_SCHEDULED = "CYCLE_SCHEDULED"
    CYCLE_EXECUTED = "CYCLE_EXECUTED"
    CYCLE_ANALYZED = "CYCLE_ANALYZED"


class SubsystemName(str, Enum):
    """Enumeration of pipeline subsystems within the orchestrator topology."""

    SCHEDULER = "SCHEDULER"
    MONITOR = "MONITOR"
    RETROSPECTIVE = "RETROSPECTIVE"
    META_OPTIMIZER = "META_OPTIMIZER"
    DIRECTIVE_GOVERNOR = "DIRECTIVE_GOVERNOR"
    OBSERVABILITY_FABRIC = "OBSERVABILITY_FABRIC"


class ImpactAssessment(str, Enum):
    """Classified causal impact of an applied directive configuration."""

    BENEFICIAL = "BENEFICIAL"
    NEUTRAL = "NEUTRAL"
    DETRIMENTAL = "DETRIMENTAL"


@dataclass
class SpanDefinition:
    """Topology descriptor defining semantic attributes and parent relationships."""

    name: str
    parent_name: Optional[str] = None
    attributes: Dict[str, str] = field(default_factory=dict)


@dataclass
class AuditEntry:
    """Immutable causal record in the telemetry audit ledger."""

    entry_id: uuid.UUID = field(default_factory=uuid.uuid4)
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    event_type: EventType = EventType.CYCLE_SCHEDULED
    source_subsystem: SubsystemName = SubsystemName.OBSERVABILITY_FABRIC
    directive_version: Optional[str] = None
    config_snapshot_hash: Optional[str] = None
    causal_parent_id: Optional[uuid.UUID] = None
    payload: Dict[str, Any] = field(default_factory=dict)
    span_context: Optional[Dict[str, str]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert entry to JSON-serializable dictionary."""
        return {
            "entry_id": str(self.entry_id),
            "timestamp": self.timestamp,
            "event_type": self.event_type.value if isinstance(self.event_type, Enum) else str(self.event_type),
            "source_subsystem": self.source_subsystem.value
            if isinstance(self.source_subsystem, Enum)
            else str(self.source_subsystem),
            "directive_version": self.directive_version,
            "config_snapshot_hash": self.config_snapshot_hash,
            "causal_parent_id": str(self.causal_parent_id) if self.causal_parent_id else None,
            "payload": self.payload,
            "span_context": self.span_context,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> AuditEntry:
        """Hydrate AuditEntry from serialized dictionary."""
        entry_id = (
            uuid.UUID(data["entry_id"])
            if isinstance(data.get("entry_id"), str)
            else (data.get("entry_id") or uuid.uuid4())
        )
        causal_parent_id = (
            uuid.UUID(data["causal_parent_id"])
            if data.get("causal_parent_id") and isinstance(data["causal_parent_id"], str)
            else data.get("causal_parent_id")
        )

        event_val = data.get("event_type", EventType.CYCLE_SCHEDULED.value)
        try:
            event_type = EventType(event_val)
        except ValueError:
            event_type = EventType[event_val] if event_val in EventType.__members__ else EventType.CYCLE_SCHEDULED

        subsys_val = data.get("source_subsystem", SubsystemName.OBSERVABILITY_FABRIC.value)
        try:
            subsystem = SubsystemName(subsys_val)
        except ValueError:
            subsystem = (
                SubsystemName[subsys_val]
                if subsys_val in SubsystemName.__members__
                else SubsystemName.OBSERVABILITY_FABRIC
            )

        return cls(
            entry_id=entry_id,
            timestamp=data.get("timestamp", datetime.now(timezone.utc).isoformat()),
            event_type=event_type,
            source_subsystem=subsystem,
            directive_version=data.get("directive_version"),
            config_snapshot_hash=data.get("config_snapshot_hash"),
            causal_parent_id=causal_parent_id,
            payload=data.get("payload", {}),
            span_context=data.get("span_context"),
        )


@dataclass
class DirectiveImpactReport:
    """Holistic impact report measuring canary and post-promotion health deltas."""

    directive_version: str
    parameter_changes: Dict[str, Tuple[Any, Any]]
    canary_conformance_delta: float
    post_promotion_variance_change: float
    net_throughput_impact: float
    causal_chain: List[uuid.UUID]
    assessment: ImpactAssessment

    def to_dict(self) -> Dict[str, Any]:
        """Convert report to JSON-serializable dictionary."""
        return {
            "directive_version": self.directive_version,
            "parameter_changes": {
                k: [v[0], v[1]] for k, v in self.parameter_changes.items()
            },
            "canary_conformance_delta": self.canary_conformance_delta,
            "post_promotion_variance_change": self.post_promotion_variance_change,
            "net_throughput_impact": self.net_throughput_impact,
            "causal_chain": [str(uid) for uid in self.causal_chain],
            "assessment": self.assessment.value,
        }


# -----------------------------------------------------------------------------
# Causal Audit Ledger (Append-Only, Thread-Safe)
# -----------------------------------------------------------------------------
class CausalAuditLedger:
    """
    Append-only in-memory ledger storing immutable AuditEntry records with
    support for causal link traversal, regression detection, and JSON I/O.
    """

    LOWER_IS_BETTER_METRICS = {
        "variance",
        "error_rate",
        "latency",
        "duration",
        "duration_ms",
        "post_promotion_variance",
        "variance_decomposition",
    }

    def __init__(self) -> None:
        self._entries: List[AuditEntry] = []
        self._entries_by_id: Dict[uuid.UUID, AuditEntry] = {}
        self._lock = threading.Lock()

    def append(self, entry: AuditEntry) -> None:
        """Thread-safely append an audit entry to the ledger."""
        with self._lock:
            self._entries.append(entry)
            self._entries_by_id[entry.entry_id] = entry
            logger.debug(
                "Appended audit entry: %s [%s] by %s",
                entry.entry_id,
                entry.event_type.value,
                entry.source_subsystem.value,
            )

    def query_by_directive(self, version: str) -> List[AuditEntry]:
        """Return all entries associated with a specific directive version ordered by timestamp."""
        with self._lock:
            matched = [e for e in self._entries if e.directive_version == version]
            return sorted(matched, key=lambda x: x.timestamp)

    def query_by_cycle(self, cycle_index: int) -> List[AuditEntry]:
        """Return entries whose payload or attributes match the given cycle index."""
        with self._lock:
            matched: List[AuditEntry] = []
            for e in self._entries:
                payload_cycle = e.payload.get("cycle_index")
                if payload_cycle is not None and int(payload_cycle) == int(cycle_index):
                    matched.append(e)
            return sorted(matched, key=lambda x: x.timestamp)

    def query_regressions(self, metric_name: str, threshold: float) -> List[AuditEntry]:
        """
        Identify entries where a metric degraded beyond the threshold relative
        to its causal parent entry's metric value.
        """
        regressions: List[AuditEntry] = []
        with self._lock:
            for entry in self._entries:
                if not entry.causal_parent_id:
                    continue
                parent = self._entries_by_id.get(entry.causal_parent_id)
                if not parent:
                    continue

                curr_val = entry.payload.get(metric_name)
                parent_val = parent.payload.get(metric_name)

                if curr_val is None or parent_val is None:
                    continue

                try:
                    c_num = float(curr_val)
                    p_num = float(parent_val)
                except (ValueError, TypeError):
                    continue

                lower_is_better = any(
                    sub in metric_name.lower() for sub in self.LOWER_IS_BETTER_METRICS
                )

                if lower_is_better:
                    # Degradation means the metric increased beyond threshold
                    degradation = c_num - p_num
                else:
                    # Higher is better: degradation means the metric dropped beyond threshold
                    degradation = p_num - c_num

                if degradation >= threshold:
                    logger.warning(
                        "Telemetry regression detected in entry %s vs parent %s on %s: delta=%.4f (threshold=%.4f)",
                        entry.entry_id,
                        parent.entry_id,
                        metric_name,
                        degradation,
                        threshold,
                    )
                    regressions.append(entry)

        return regressions

    def get_causal_chain(self, entry_id: uuid.UUID) -> List[AuditEntry]:
        """Walk backwards via causal_parent_id and return root-to-leaf chronological chain."""
        chain: List[AuditEntry] = []
        visited: Set[uuid.UUID] = set()

        with self._lock:
            curr_id: Optional[uuid.UUID] = entry_id
            while curr_id and curr_id not in visited:
                visited.add(curr_id)
                entry = self._entries_by_id.get(curr_id)
                if not entry:
                    break
                chain.append(entry)
                curr_id = entry.causal_parent_id

        chain.reverse()
        return chain

    def export_json(self, filepath: str) -> None:
        """Persist the ledger entries to disk as JSON."""
        with self._lock:
            data = [e.to_dict() for e in self._entries]

        os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
        temp_path = f"{filepath}.tmp.{os.getpid()}"
        try:
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            os.replace(temp_path, filepath)
            logger.info("Exported %d audit entries to %s", len(data), filepath)
        except Exception as e:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass
            logger.error("Failed to export audit ledger to %s: %e", filepath, e)
            raise

    def import_json(self, filepath: str) -> None:
        """Import ledger entries from disk, preserving causal integrity."""
        if not os.path.exists(filepath):
            logger.warning("Audit trail file not found: %s", filepath)
            return

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                raw_entries = json.load(f)

            loaded_entries = [AuditEntry.from_dict(d) for d in raw_entries]
            with self._lock:
                for entry in loaded_entries:
                    if entry.entry_id not in self._entries_by_id:
                        self._entries.append(entry)
                        self._entries_by_id[entry.entry_id] = entry
            logger.info("Imported %d audit entries from %s", len(loaded_entries), filepath)
        except Exception as e:
            logger.error("Failed to import audit ledger from %s: %s", filepath, e)
            raise

    def count(self) -> int:
        """Return total entries in ledger."""
        with self._lock:
            return len(self._entries)

    def get_all(self) -> List[AuditEntry]:
        """Return a snapshot of all audit entries."""
        with self._lock:
            return list(self._entries)


# -----------------------------------------------------------------------------
# OpenTelemetry Bridge (Arize Phoenix Compatible)
# -----------------------------------------------------------------------------
class OpenTelemetryBridge:
    """
    OpenTelemetry TracerProvider and exporter bridge configured for Arize Phoenix
    or any standard OTLP HTTP collector, degrading gracefully if dependencies are absent.
    """

    def __init__(
        self,
        endpoint: str = "http://127.0.0.1:6006/v1/traces",
        service_name: str = "hermes-pipeline",
    ) -> None:
        self.endpoint = endpoint
        self.service_name = service_name
        self.available = False
        self.tracer = None
        self.tracer_provider = None

        if not OPENTELEMETRY_AVAILABLE:
            logger.warning(
                "OpenTelemetry packages not installed. Running in ledger-only fallback mode."
            )
            return

        try:
            resource = Resource.create({"service.name": self.service_name})
            self.tracer_provider = TracerProvider(resource=resource)
            exporter = OTLPSpanExporter(endpoint=self.endpoint)
            processor = BatchSpanProcessor(
                exporter,
                max_queue_size=2048,
                max_export_batch_size=512,
                scheduled_delay_millis=5000,
            )
            self.tracer_provider.add_span_processor(processor)
            self.tracer = self.tracer_provider.get_tracer("hermes.observability.fabric")
            self.available = True
            logger.info(
                "OpenTelemetry bridge initialized targeting OTLP endpoint: %s",
                self.endpoint,
            )
        except Exception as e:
            self.available = False
            self.tracer = None
            self.tracer_provider = None
            logger.warning(
                "Could not initialize OpenTelemetry TracerProvider (%s). Degraded to ledger-only.",
                e,
            )

    @contextmanager
    def create_span(
        self,
        name: str,
        attributes: Optional[Dict[str, Any]] = None,
        parent_context: Any = None,
        links: Optional[List[Any]] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Context manager creating an OpenTelemetry span when available or a lightweight
        context tracking duration and status in degraded mode.
        """
        start_time = time.perf_counter()
        span_meta: Dict[str, Any] = {
            "name": name,
            "attributes": dict(attributes or {}),
            "trace_id": None,
            "span_id": None,
            "status": "OK",
            "error": None,
        }

        if self.available and self.tracer:
            otel_span = None
            try:
                otel_span = self.tracer.start_span(
                    name=name,
                    context=parent_context,
                    attributes=span_meta["attributes"],
                    links=links or [],
                )
                ctx = otel_span.get_span_context()
                if ctx and ctx.is_valid:
                    span_meta["trace_id"] = format(ctx.trace_id, "032x")
                    span_meta["span_id"] = format(ctx.span_id, "016x")

                yield span_meta

                if span_meta.get("status") == "ERROR" and Status and StatusCode:
                    otel_span.set_status(Status(StatusCode.ERROR, span_meta.get("error", "Error")))
                elif Status and StatusCode:
                    otel_span.set_status(Status(StatusCode.OK))
            except Exception as exc:
                span_meta["status"] = "ERROR"
                span_meta["error"] = str(exc)
                if otel_span and Status and StatusCode:
                    otel_span.record_exception(exc)
                    otel_span.set_status(Status(StatusCode.ERROR, str(exc)))
                raise
            finally:
                duration_ms = (time.perf_counter() - start_time) * 1000.0
                span_meta["duration_ms"] = duration_ms
                if otel_span:
                    otel_span.set_attribute("duration_ms", duration_ms)
                    otel_span.end()
        else:
            # Fallback mock context
            span_meta["trace_id"] = uuid.uuid4().hex
            span_meta["span_id"] = uuid.uuid4().hex[:16]
            try:
                yield span_meta
            except Exception as exc:
                span_meta["status"] = "ERROR"
                span_meta["error"] = str(exc)
                raise
            finally:
                duration_ms = (time.perf_counter() - start_time) * 1000.0
                span_meta["duration_ms"] = duration_ms

    def create_span_link(self, trace_id_hex: str, span_id_hex: str) -> Optional[Any]:
        """Create an OpenTelemetry Link object referencing a causal predecessor span."""
        if not (self.available and Link and trace):
            return None
        try:
            trace_id = int(trace_id_hex, 16)
            span_id = int(span_id_hex, 16)
            context = trace.SpanContext(
                trace_id=trace_id,
                span_id=span_id,
                is_remote=True,
                trace_flags=TraceFlags(TraceFlags.SAMPLED) if TraceFlags else 1,
            )
            return Link(context)
        except Exception as e:
            logger.debug("Failed creating OpenTelemetry span link: %s", e)
            return None

    def shutdown(self) -> None:
        """Flush pending batches and release tracer resources."""
        if self.available and self.tracer_provider:
            try:
                self.tracer_provider.shutdown()
                logger.info("OpenTelemetry TracerProvider shutdown complete.")
            except Exception as e:
                logger.warning("Error during OpenTelemetry TracerProvider shutdown: %s", e)


# -----------------------------------------------------------------------------
# Pipeline Observability Fabric Main Class
# -----------------------------------------------------------------------------
class PipelineObservabilityFabric:
    """
    Unified telemetry coordinator, span topology governor, and causal audit
    trail emitter for the multi-repo autonomous orchestration loop.
    """

    # Strict Data-Flow DAG execution order for cycle boundary telemetry
    DAG_ORDER: List[SubsystemName] = [
        SubsystemName.SCHEDULER,
        SubsystemName.MONITOR,
        SubsystemName.RETROSPECTIVE,
        SubsystemName.META_OPTIMIZER,
        SubsystemName.DIRECTIVE_GOVERNOR,
    ]

    # Pre-defined Span Topology Model
    SPAN_TOPOLOGY: Dict[str, SpanDefinition] = {
        "pipeline.cycle": SpanDefinition("pipeline.cycle", parent_name=None),
        "pipeline.cycle.schedule": SpanDefinition(
            "pipeline.cycle.schedule", parent_name="pipeline.cycle"
        ),
        "pipeline.cycle.execute": SpanDefinition(
            "pipeline.cycle.execute", parent_name="pipeline.cycle"
        ),
        "pipeline.cycle.retrospective": SpanDefinition(
            "pipeline.cycle.retrospective", parent_name="pipeline.cycle"
        ),
        "pipeline.directive.meta_optimize": SpanDefinition(
            "pipeline.directive.meta_optimize", parent_name="pipeline.cycle"
        ),
        "pipeline.directive.govern": SpanDefinition(
            "pipeline.directive.govern", parent_name="pipeline.cycle"
        ),
    }

    def __init__(
        self,
        otel_endpoint: str = "http://127.0.0.1:6006/v1/traces",
        service_name: str = "hermes-pipeline",
        assessment_thresholds: Optional[Dict[str, float]] = None,
    ) -> None:
        self.ledger = CausalAuditLedger()
        self.otel_bridge = OpenTelemetryBridge(endpoint=otel_endpoint, service_name=service_name)
        self.subsystems: Dict[SubsystemName, Callable[[int], Dict[str, Any]]] = {}

        # Default assessment criteria
        self.assessment_thresholds = {
            "conformance_delta_min": 0.01,
            "variance_change_max": 0.05,
            "throughput_impact_min": 0.00,
            "detrimental_conformance_drop": -0.03,
            "detrimental_variance_spike": 0.15,
            "detrimental_throughput_drop": -0.05,
        }
        if assessment_thresholds:
            self.assessment_thresholds.update(assessment_thresholds)

        # Cross-cycle span link lookup: directive_version -> span_context dict
        self._directive_span_contexts: Dict[str, Dict[str, str]] = {}
        self._lock = threading.Lock()

        logger.info(
            "PipelineObservabilityFabric instantiated with %d topology spans defined.",
            len(self.SPAN_TOPOLOGY),
        )

    def register_subsystem(
        self, name: SubsystemName, telemetry_callback: Callable[[int], Dict[str, Any]]
    ) -> None:
        """Register a pipeline subsystem's boundary telemetry callback."""
        with self._lock:
            self.subsystems[name] = telemetry_callback
            logger.info("Registered subsystem telemetry callback for %s", name.value)

    def record_event(
        self,
        event_type: EventType,
        source_subsystem: SubsystemName,
        directive_version: Optional[str] = None,
        config_snapshot_hash: Optional[str] = None,
        causal_parent_id: Optional[uuid.UUID] = None,
        payload: Optional[Dict[str, Any]] = None,
        span_context: Optional[Dict[str, str]] = None,
    ) -> uuid.UUID:
        """Directly record an event into the causal ledger and emit trace context."""
        entry = AuditEntry(
            event_type=event_type,
            source_subsystem=source_subsystem,
            directive_version=directive_version,
            config_snapshot_hash=config_snapshot_hash,
            causal_parent_id=causal_parent_id,
            payload=payload or {},
            span_context=span_context,
        )
        self.ledger.append(entry)

        if directive_version and span_context:
            with self._lock:
                self._directive_span_contexts[directive_version] = span_context

        return entry.entry_id

    @contextmanager
    def instrument_subsystem(
        self,
        subsystem_name: SubsystemName,
        operation: str,
        attributes: Optional[Dict[str, Any]] = None,
        causal_parent_id: Optional[uuid.UUID] = None,
        directive_version: Optional[str] = None,
        config_snapshot_hash: Optional[str] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Context manager wrapping a subsystem operation in an OpenTelemetry span
        and appending an AuditEntry upon completion.
        """
        span_attrs = {
            "pipeline.subsystem": subsystem_name.value,
            "pipeline.operation": operation,
        }
        if attributes:
            for k, v in attributes.items():
                span_attrs[str(k)] = str(v)
        if directive_version:
            span_attrs["pipeline.directive_version"] = directive_version
        if config_snapshot_hash:
            span_attrs["pipeline.config_snapshot_hash"] = config_snapshot_hash

        # Cross-cycle span link resolution
        links = []
        if directive_version and directive_version in self._directive_span_contexts:
            cached_ctx = self._directive_span_contexts[directive_version]
            t_id = cached_ctx.get("trace_id")
            s_id = cached_ctx.get("span_id")
            if t_id and s_id:
                link = self.otel_bridge.create_span_link(t_id, s_id)
                if link:
                    links.append(link)

        span_name = f"pipeline.{subsystem_name.value.lower()}.{operation}"
        event_type = self._map_operation_to_event(subsystem_name, operation)

        with self.otel_bridge.create_span(
            name=span_name, attributes=span_attrs, links=links
        ) as span_meta:
            yield span_meta

            # Capture timing and payload updates
            payload_data = dict(attributes or {})
            payload_data["duration_ms"] = span_meta.get("duration_ms", 0.0)
            payload_data["status"] = span_meta.get("status", "OK")
            if span_meta.get("error"):
                payload_data["error"] = span_meta.get("error")

            span_ctx = None
            if span_meta.get("trace_id") and span_meta.get("span_id"):
                span_ctx = {
                    "trace_id": span_meta["trace_id"],
                    "span_id": span_meta["span_id"],
                }

            entry_id = self.record_event(
                event_type=event_type,
                source_subsystem=subsystem_name,
                directive_version=directive_version,
                config_snapshot_hash=config_snapshot_hash,
                causal_parent_id=causal_parent_id,
                payload=payload_data,
                span_context=span_ctx,
            )
            span_meta["audit_entry_id"] = str(entry_id)

    def _map_operation_to_event(self, subsystem: SubsystemName, operation: str) -> EventType:
        """Map subsystem operations to the corresponding EventType."""
        op = operation.lower()
        if subsystem == SubsystemName.META_OPTIMIZER or "propose" in op:
            return EventType.DIRECTIVE_PROPOSED
        if subsystem == SubsystemName.DIRECTIVE_GOVERNOR:
            if "validate" in op or "govern" in op:
                return EventType.DIRECTIVE_VALIDATED
            if "promote" in op:
                return EventType.PROMOTED
            if "rollback" in op:
                return EventType.ROLLED_BACK
        if "canary_start" in op:
            return EventType.CANARY_STARTED
        if "canary_eval" in op or "canary" in op:
            return EventType.CANARY_EVALUATED
        if subsystem == SubsystemName.RETROSPECTIVE or "retro" in op:
            return EventType.RETROSPECTIVE_LINKED
        if subsystem == SubsystemName.SCHEDULER or "schedule" in op:
            return EventType.CYCLE_SCHEDULED
        if subsystem == SubsystemName.MONITOR or "execute" in op:
            return EventType.CYCLE_EXECUTED
        return EventType.CYCLE_ANALYZED

    def collect_cycle_telemetry(self, cycle_index: int) -> Dict[str, Any]:
        """
        Invoke registered subsystem callbacks in DAG order
        (Scheduler -> Monitor -> Retrospective -> Meta-Optimizer -> Directive Governor)
        to maintain causal order in the ledger.
        """
        results: Dict[str, Any] = {}
        logger.info("Executing cycle boundary telemetry collection for cycle %d", cycle_index)

        with self.instrument_subsystem(
            subsystem_name=SubsystemName.OBSERVABILITY_FABRIC,
            operation="cycle_boundary_flush",
            attributes={"cycle_index": cycle_index},
        ) as fabric_meta:
            prev_entry_id: Optional[uuid.UUID] = (
                uuid.UUID(fabric_meta["audit_entry_id"])
                if "audit_entry_id" in fabric_meta
                else None
            )

            for subsys in self.DAG_ORDER:
                if subsys not in self.subsystems:
                    continue

                callback = self.subsystems[subsys]
                try:
                    telemetry_payload = callback(cycle_index) or {}
                    results[subsys.value] = telemetry_payload

                    evt_type = self._map_operation_to_event(subsys, f"cycle_{cycle_index}")
                    new_id = self.record_event(
                        event_type=evt_type,
                        source_subsystem=subsys,
                        directive_version=telemetry_payload.get("directive_version"),
                        config_snapshot_hash=telemetry_payload.get("config_snapshot_hash"),
                        causal_parent_id=prev_entry_id,
                        payload=telemetry_payload,
                    )
                    prev_entry_id = new_id
                except Exception as e:
                    logger.error(
                        "Error collecting telemetry from subsystem %s on cycle %d: %s",
                        subsys.value,
                        cycle_index,
                        e,
                    )
                    results[subsys.value] = {"error": str(e)}

        return results

    def correlate_directive_impact(self, directive_version: str) -> DirectiveImpactReport:
        """
        Traverse the audit ledger to reconstruct the full lifecycle of a directive,
        calculating conformance delta, variance decomposition change, and net throughput.
        """
        entries = self.ledger.query_by_directive(directive_version)
        if not entries:
            logger.warning("No audit records found for directive version: %s", directive_version)
            return DirectiveImpactReport(
                directive_version=directive_version,
                parameter_changes={},
                canary_conformance_delta=0.0,
                post_promotion_variance_change=0.0,
                net_throughput_impact=0.0,
                causal_chain=[],
                assessment=ImpactAssessment.NEUTRAL,
            )

        causal_ids: List[uuid.UUID] = [e.entry_id for e in entries]
        parameter_changes: Dict[str, Tuple[Any, Any]] = {}

        # 1. Parameter extraction from proposal or governance
        for e in entries:
            if e.event_type in (EventType.DIRECTIVE_PROPOSED, EventType.DIRECTIVE_VALIDATED):
                changes = e.payload.get("parameter_changes") or e.payload.get("parameter_deltas")
                if isinstance(changes, dict):
                    for param, delta_val in changes.items():
                        if isinstance(delta_val, (list, tuple)) and len(delta_val) == 2:
                            parameter_changes[param] = (delta_val[0], delta_val[1])
                        else:
                            parameter_changes[param] = (None, delta_val)

        # 2. Canary Evaluation & Conformance Delta
        canary_delta = 0.0
        canary_eval_entries = [
            e for e in entries if e.event_type == EventType.CANARY_EVALUATED or "canary" in e.payload
        ]
        if canary_eval_entries:
            latest_canary = canary_eval_entries[-1]
            canary_delta = float(
                latest_canary.payload.get(
                    "canary_delta",
                    latest_canary.payload.get(
                        "conformance_delta",
                        latest_canary.payload.get("conformance_score", 0.0),
                    ),
                )
            )

        # 3. Post-Promotion Retrospective Variance Change
        post_variance_change = 0.0
        retro_entries = [
            e for e in entries if e.event_type == EventType.RETROSPECTIVE_LINKED
        ]
        if retro_entries:
            latest_retro = retro_entries[-1]
            post_variance_change = float(
                latest_retro.payload.get(
                    "variance_change",
                    latest_retro.payload.get(
                        "post_promotion_variance_change",
                        latest_retro.payload.get("variance_decomposition", 0.0),
                    ),
                )
            )

        # 4. Net Throughput Impact
        throughput_impact = 0.0
        for e in entries:
            tp = e.payload.get("net_throughput_impact") or e.payload.get("throughput_delta")
            if tp is not None:
                try:
                    throughput_impact = float(tp)
                except (ValueError, TypeError):
                    pass

        # 5. Threshold-based Assessment
        t = self.assessment_thresholds
        if (
            canary_delta <= t["detrimental_conformance_drop"]
            or post_variance_change >= t["detrimental_variance_spike"]
            or throughput_impact <= t["detrimental_throughput_drop"]
        ):
            assessment = ImpactAssessment.DETRIMENTAL
        elif (
            canary_delta >= t["conformance_delta_min"]
            and post_variance_change <= t["variance_change_max"]
            and throughput_impact >= t["throughput_impact_min"]
        ):
            assessment = ImpactAssessment.BENEFICIAL
        else:
            assessment = ImpactAssessment.NEUTRAL

        return DirectiveImpactReport(
            directive_version=directive_version,
            parameter_changes=parameter_changes,
            canary_conformance_delta=canary_delta,
            post_promotion_variance_change=post_variance_change,
            net_throughput_impact=throughput_impact,
            causal_chain=causal_ids,
            assessment=assessment,
        )

    def get_subsystem_health_summary(self) -> Dict[str, Any]:
        """Compute aggregated metrics (span count, error rate, average duration) per subsystem."""
        entries = self.ledger.get_all()
        summary: Dict[str, Dict[str, Any]] = {}

        for subsys in SubsystemName:
            summary[subsys.value] = {
                "total_events": 0,
                "error_count": 0,
                "error_rate": 0.0,
                "total_duration_ms": 0.0,
                "avg_duration_ms": 0.0,
            }

        for e in entries:
            sub_key = e.source_subsystem.value
            stat = summary[sub_key]
            stat["total_events"] += 1

            dur = e.payload.get("duration_ms")
            if dur is not None:
                try:
                    stat["total_duration_ms"] += float(dur)
                except (ValueError, TypeError):
                    pass

            if (
                e.payload.get("status") == "ERROR"
                or e.payload.get("error")
                or e.event_type == EventType.ROLLED_BACK
            ):
                stat["error_count"] += 1

        for sub_key, stat in summary.items():
            if stat["total_events"] > 0:
                stat["error_rate"] = round(stat["error_count"] / stat["total_events"], 4)
                stat["avg_duration_ms"] = round(
                    stat["total_duration_ms"] / stat["total_events"], 2
                )

        return summary

    def export_audit_trail(self, filepath: str) -> None:
        """Delegate persistence to ledger."""
        self.ledger.export_json(filepath)

    def import_audit_trail(self, filepath: str) -> None:
        """Delegate loading to ledger."""
        self.ledger.import_json(filepath)

    def shutdown(self) -> None:
        """Release OpenTelemetry and background thread resources."""
        self.otel_bridge.shutdown()


# -----------------------------------------------------------------------------
# Module-Level Convenience Factory
# -----------------------------------------------------------------------------
def create_observability_fabric(**kwargs: Any) -> PipelineObservabilityFabric:
    """Instantiate and configure a PipelineObservabilityFabric instance."""
    return PipelineObservabilityFabric(**kwargs)


# -----------------------------------------------------------------------------
# Self-Verification Demo & Lifecycle Simulation
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    )

    print("=" * 80)
    print("Hermes Orchestrator - Pipeline Observability Fabric Demonstration")
    print("=" * 80)

    fabric = create_observability_fabric(
        otel_endpoint="http://127.0.0.1:6006/v1/traces",
        service_name="hermes-pipeline-demo",
    )

    # 1. Register Subsystems in DAG Topology
    def scheduler_cb(cycle: int) -> Dict[str, Any]:
        return {"cycle_index": cycle, "queue_depth": 14, "prioritized_repo": "v2ray_tool"}

    def monitor_cb(cycle: int) -> Dict[str, Any]:
        return {"cycle_index": cycle, "cpu_usage": 0.42, "conformance_score": 0.96}

    def retro_cb(cycle: int) -> Dict[str, Any]:
        return {"cycle_index": cycle, "variance_decomposition": 0.04, "latency_p95": 180.5}

    def meta_opt_cb(cycle: int) -> Dict[str, Any]:
        return {
            "cycle_index": cycle,
            "bayesian_ei_value": 0.88,
            "directive_version": "v2.5.0-opt",
        }

    def governor_cb(cycle: int) -> Dict[str, Any]:
        return {
            "cycle_index": cycle,
            "status": "APPROVED",
            "directive_version": "v2.5.0-opt",
        }

    fabric.register_subsystem(SubsystemName.SCHEDULER, scheduler_cb)
    fabric.register_subsystem(SubsystemName.MONITOR, monitor_cb)
    fabric.register_subsystem(SubsystemName.RETROSPECTIVE, retro_cb)
    fabric.register_subsystem(SubsystemName.META_OPTIMIZER, meta_opt_cb)
    fabric.register_subsystem(SubsystemName.DIRECTIVE_GOVERNOR, governor_cb)

    # 2. Simulate Baseline Cycle (Cycle 10)
    print("\n[Step 1] Collecting baseline cycle boundary telemetry (Cycle 10)...")
    fabric.collect_cycle_telemetry(cycle_index=10)

    # 3. Simulate Directive Lifecycle Across Cycles
    test_directive = "v2.5.0-adaptive-token-governor"
    snapshot_hash = "sha256-e9a184c2f8b5"

    print(f"\n[Step 2] Proposing Directive: {test_directive}")
    prop_id = fabric.record_event(
        event_type=EventType.DIRECTIVE_PROPOSED,
        source_subsystem=SubsystemName.META_OPTIMIZER,
        directive_version=test_directive,
        config_snapshot_hash=snapshot_hash,
        payload={
            "parameter_changes": {
                "max_tokens_per_minute": [40000, 60000],
                "burst_tolerance_ratio": [1.2, 1.5],
            },
            "bayesian_ei_value": 0.892,
        },
    )

    print(f"[Step 3] Validating Directive with Governor (Causal parent: {prop_id})")
    val_id = fabric.record_event(
        event_type=EventType.DIRECTIVE_VALIDATED,
        source_subsystem=SubsystemName.DIRECTIVE_GOVERNOR,
        directive_version=test_directive,
        config_snapshot_hash=snapshot_hash,
        causal_parent_id=prop_id,
        payload={"conformance_check": "PASSED", "safety_margin": 0.98},
    )

    print(f"[Step 4] Starting Canary Deployment in Cycle 11")
    canary_start_id = fabric.record_event(
        event_type=EventType.CANARY_STARTED,
        source_subsystem=SubsystemName.MONITOR,
        directive_version=test_directive,
        config_snapshot_hash=snapshot_hash,
        causal_parent_id=val_id,
        payload={"canary_weight": 0.20, "target_repos": ["v2ray_tool", "PhD_Proposal"]},
    )

    print(f"[Step 5] Evaluating Canary Results")
    canary_eval_id = fabric.record_event(
        event_type=EventType.CANARY_EVALUATED,
        source_subsystem=SubsystemName.MONITOR,
        directive_version=test_directive,
        config_snapshot_hash=snapshot_hash,
        causal_parent_id=canary_start_id,
        payload={"canary_delta": 0.052, "error_rate": 0.001, "conformance_score": 0.99},
    )

    print(f"[Step 6] Promoting Directive to Active Production Topology")
    promo_id = fabric.record_event(
        event_type=EventType.PROMOTED,
        source_subsystem=SubsystemName.DIRECTIVE_GOVERNOR,
        directive_version=test_directive,
        config_snapshot_hash=snapshot_hash,
        causal_parent_id=canary_eval_id,
        payload={"promotion_cycle": 12, "net_throughput_impact": 0.145},
    )

    print(f"[Step 7] Linking Post-Promotion Retrospective Telemetry")
    fabric.record_event(
        event_type=EventType.RETROSPECTIVE_LINKED,
        source_subsystem=SubsystemName.RETROSPECTIVE,
        directive_version=test_directive,
        config_snapshot_hash=snapshot_hash,
        causal_parent_id=promo_id,
        payload={"post_promotion_variance_change": 0.018, "latency_reduction_pct": 12.4},
    )

    # 4. Cross-Subsystem Correlation Engine
    print("\n" + "=" * 80)
    print("Cross-Subsystem Correlation Impact Analysis:")
    print("=" * 80)
    impact_report = fabric.correlate_directive_impact(test_directive)

    print(f"Directive Version:               {impact_report.directive_version}")
    print(f"Parameter Modifications:         {impact_report.parameter_changes}")
    print(f"Canary Conformance Delta:        +{impact_report.canary_conformance_delta:.4f}")
    print(f"Post-Promotion Variance Change:  +{impact_report.post_promotion_variance_change:.4f}")
    print(f"Net Throughput Impact:           +{impact_report.net_throughput_impact:.4f}")
    print(f"Causal Chain Length:             {len(impact_report.causal_chain)} entries")
    print(f"Strategic Assessment:            {impact_report.assessment.value}")

    # 5. Regression Detection Query
    print("\n" + "=" * 80)
    print("Telemetry Regression Auditing:")
    print("=" * 80)
    regressions = fabric.ledger.query_regressions(metric_name="variance", threshold=0.10)
    print(f"Detected Regressions (> 0.10 variance): {len(regressions)}")

    # 6. Subsystem Health Summary
    print("\n" + "=" * 80)
    print("Subsystem Health & Telemetry Metrics Summary:")
    print("=" * 80)
    health = fabric.get_subsystem_health_summary()
    for subsys_name, metrics in health.items():
        if metrics["total_events"] > 0:
            print(
                f"{subsys_name:<22} | Events: {metrics['total_events']:<3} | Errors: {metrics['error_count']:<2} | "
                f"ErrRate: {metrics['error_rate']:.2f} | AvgDuration: {metrics['avg_duration_ms']:.2f}ms"
            )

    fabric.shutdown()
    print("\nPipeline Observability Fabric verification successful.")