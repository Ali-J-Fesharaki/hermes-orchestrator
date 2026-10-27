"""
tasks/lifecycle_state_machine.py

Milestone Lifecycle State Machine & Transition Event Bus.
Authoritative state owner, edge validator, event dispatcher, and persistence engine
for milestone executions across the Hermes Orchestrator fleet.
"""

from __future__ import annotations

import json
import logging
import os
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable

# Setup module logger
logger = logging.getLogger(__name__)

# Dashboard and log paths
DASHBOARDS_DIR = Path("dashboards")
LIFECYCLE_LOG_PATH = DASHBOARDS_DIR / "LIFECYCLE_LOG.md"
EVENT_BUS_ERRORS_PATH = DASHBOARDS_DIR / "EVENT_BUS_ERRORS.md"
STATE_PERSISTENCE_PATH = DASHBOARDS_DIR / "lifecycle_state.json"

LOG_HEADER = (
    "| Milestone ID | Current State | Transitions | Age (s) | Last Transition |\n"
    "|---|---|---|---|---|\n"
)

ERROR_LOG_HEADER = (
    "| Timestamp | Event Type | Milestone ID | Handler | Error |\n"
    "|---|---|---|---|---|\n"
)


def _ensure_dashboards_dir() -> None:
    """Ensure dashboards directory and base log files exist with appropriate headers."""
    try:
        DASHBOARDS_DIR.mkdir(parents=True, exist_ok=True)
        if not LIFECYCLE_LOG_PATH.exists() or LIFECYCLE_LOG_PATH.stat().st_size == 0:
            with open(LIFECYCLE_LOG_PATH, "w", encoding="utf-8") as f:
                f.write(LOG_HEADER)
    except Exception as exc:
        logger.error("Failed to initialize dashboards directory or lifecycle log: %s", exc)


# Initialize dashboard files on module load
_ensure_dashboards_dir()


def _parse_iso(ts_str: str) -> datetime:
    """Safely parse an ISO-8601 string to a timezone-aware datetime."""
    try:
        clean_ts = ts_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(clean_ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return datetime.now(timezone.utc)


class MilestoneState(str, Enum):
    """Authoritative lifecycle states for milestone orchestrations."""
    PENDING = "PENDING"
    GATE_REVIEW = "GATE_REVIEW"
    APPROVED = "APPROVED"
    DEFERRED = "DEFERRED"
    REJECTED = "REJECTED"
    DISPATCHED = "DISPATCHED"
    EXECUTING = "EXECUTING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    ROLLED_BACK = "ROLLED_BACK"
    REQUEUED = "REQUEUED"


class IllegalTransitionError(Exception):
    """Raised when an edge transition violates TRANSITION_TABLE."""

    def __init__(
        self,
        from_state: MilestoneState,
        to_state: MilestoneState,
        message: str | None = None,
    ):
        self.from_state = from_state
        self.to_state = to_state
        err_msg = message or f"Illegal transition from '{from_state.value}' to '{to_state.value}'"
        super().__init__(err_msg)


TRANSITION_TABLE: dict[MilestoneState, set[MilestoneState]] = {
    MilestoneState.PENDING: {MilestoneState.GATE_REVIEW},
    MilestoneState.GATE_REVIEW: {
        MilestoneState.APPROVED,
        MilestoneState.DEFERRED,
        MilestoneState.REJECTED,
    },
    MilestoneState.APPROVED: {MilestoneState.DISPATCHED},
    MilestoneState.DEFERRED: {MilestoneState.GATE_REVIEW},
    MilestoneState.REJECTED: {MilestoneState.PENDING},
    MilestoneState.DISPATCHED: {MilestoneState.EXECUTING},
    MilestoneState.EXECUTING: {MilestoneState.SUCCEEDED, MilestoneState.FAILED},
    MilestoneState.FAILED: {MilestoneState.ROLLED_BACK, MilestoneState.REQUEUED},
    MilestoneState.ROLLED_BACK: {MilestoneState.REQUEUED},
    MilestoneState.REQUEUED: {MilestoneState.GATE_REVIEW},
    MilestoneState.SUCCEEDED: set(),
}


@dataclass
class TransitionEvent:
    """Telemetry record capturing state progression and associated metadata."""
    milestone_id: str
    from_state: MilestoneState
    to_state: MilestoneState
    timestamp: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "milestone_id": self.milestone_id,
            "from_state": self.from_state.value if isinstance(self.from_state, MilestoneState) else str(self.from_state),
            "to_state": self.to_state.value if isinstance(self.to_state, MilestoneState) else str(self.to_state),
            "timestamp": self.timestamp,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TransitionEvent:
        return cls(
            milestone_id=data["milestone_id"],
            from_state=MilestoneState(data["from_state"]),
            to_state=MilestoneState(data["to_state"]),
            timestamp=data["timestamp"],
            metadata=data.get("metadata", {}),
        )


class EventBus:
    """Synchronous publish-subscribe bus with robust per-handler fault isolation."""

    def __init__(self) -> None:
        self._subscribers: dict[str, list[Callable[[TransitionEvent], None]]] = {}

    def subscribe(self, event_type: str, handler: Callable[[TransitionEvent], None]) -> None:
        """Register a callback handler for a specific event type."""
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        self._subscribers[event_type].append(handler)

    def publish(self, event: TransitionEvent, event_type: str = "transition") -> None:
        """
        Invoke all registered handlers for the event synchronously.
        Catches and logs exceptions per handler to dashboards/EVENT_BUS_ERRORS.md
        without aborting the loop.
        """
        handlers = self._subscribers.get(event_type, [])
        for handler in handlers:
            try:
                handler(event)
            except Exception as exc:
                self._log_handler_error(event_type, event, handler, exc)

    def _log_handler_error(
        self,
        event_type: str,
        event: TransitionEvent,
        handler: Callable[[TransitionEvent], None],
        error: Exception,
    ) -> None:
        """Log subscriber handler errors to file and module logger."""
        handler_name = getattr(handler, "__name__", repr(handler))
        err_msg = str(error).replace("\n", " ")
        iso_now = datetime.now(timezone.utc).isoformat()
        logger.error(
            "EventBus error on handler %s for event %s [%s]: %s",
            handler_name,
            event_type,
            event.milestone_id,
            error,
            exc_info=True,
        )

        try:
            DASHBOARDS_DIR.mkdir(parents=True, exist_ok=True)
            if not EVENT_BUS_ERRORS_PATH.exists() or EVENT_BUS_ERRORS_PATH.stat().st_size == 0:
                with open(EVENT_BUS_ERRORS_PATH, "w", encoding="utf-8") as f:
                    f.write(ERROR_LOG_HEADER)

            with open(EVENT_BUS_ERRORS_PATH, "a", encoding="utf-8") as f:
                f.write(
                    f"| {iso_now} | {event_type} | {event.milestone_id} | {handler_name} | {err_msg} |\n"
                )
        except Exception as file_exc:
            logger.error("Failed to append to %s: %s", EVENT_BUS_ERRORS_PATH, file_exc)


class MilestoneLifecycle:
    """
    State machine owner for a single milestone.
    Validates state transitions, manages transition history, dispatches events,
    and maintains persisted telemetry logs and JSON snapshots.
    """

    def __init__(
        self,
        milestone_id: str,
        initial_state: MilestoneState = MilestoneState.PENDING,
        event_bus: EventBus | None = None,
    ) -> None:
        self.milestone_id = milestone_id
        self.initial_state = initial_state
        self.current_state = initial_state
        self.event_bus = event_bus
        self.history: list[TransitionEvent] = []
        now_iso = datetime.now(timezone.utc).isoformat()
        self.created_at = now_iso
        self.last_updated = now_iso

    def transition(self, to_state: MilestoneState, metadata: dict[str, Any] | None = None) -> TransitionEvent:
        """
        Validate edge against TRANSITION_TABLE, advance internal state,
        record transition event, publish to event bus, and persist state.
        """
        if metadata is None:
            metadata = {}

        allowed_targets = TRANSITION_TABLE.get(self.current_state, set())
        if to_state not in allowed_targets:
            raise IllegalTransitionError(self.current_state, to_state)

        timestamp = datetime.now(timezone.utc).isoformat()
        event = TransitionEvent(
            milestone_id=self.milestone_id,
            from_state=self.current_state,
            to_state=to_state,
            timestamp=timestamp,
            metadata=metadata,
        )

        self.current_state = to_state
        self.last_updated = timestamp
        self.history.append(event)

        # Dispatch event synchronously if bus is attached
        if self.event_bus is not None:
            self.event_bus.publish(event, event_type="transition")

        # Persist updated state snapshot
        self._persist_state()

        # Append row to lifecycle log markdown table
        self._append_to_dashboard_log()

        return event

    def get_state(self) -> MilestoneState:
        """Return the current MilestoneState."""
        return self.current_state

    def get_history(self) -> list[TransitionEvent]:
        """Return a copy of the ordered history of transitions."""
        return list(self.history)

    def get_dwell_time(self, state: MilestoneState) -> float:
        """
        Calculate total seconds the milestone has spent in `state`
        across all visits up to the current moment.
        """
        if isinstance(state, str):
            try:
                state = MilestoneState(state)
            except ValueError:
                pass

        total_seconds = 0.0

        # Initial segment start
        current_segment_state = self.initial_state
        current_segment_start = _parse_iso(self.created_at)

        for ev in self.history:
            ev_time = _parse_iso(ev.timestamp)
            if current_segment_state == state:
                duration = max(0.0, (ev_time - current_segment_start).total_seconds())
                total_seconds += duration
            current_segment_state = ev.to_state
            current_segment_start = ev_time

        # If milestone is currently residing in the target state, add time elapsed until now
        if self.current_state == state:
            now_dt = datetime.now(timezone.utc)
            duration = max(0.0, (now_dt - current_segment_start).total_seconds())
            total_seconds += duration

        return total_seconds

    def to_dashboard_row(self) -> str:
        """Produce a pipe-delimited table row for LIFECYCLE_LOG.md."""
        now_dt = datetime.now(timezone.utc)
        created_dt = _parse_iso(self.created_at)
        total_age_seconds = max(0.0, (now_dt - created_dt).total_seconds())
        last_transition_ts = self.history[-1].timestamp if self.history else self.created_at

        return (
            f"| {self.milestone_id} "
            f"| {self.current_state.value} "
            f"| {len(self.history)} "
            f"| {total_age_seconds:.2f} "
            f"| {last_transition_ts} |"
        )

    def _persist_state(self, path: Path | str = STATE_PERSISTENCE_PATH) -> None:
        """Persist full state dictionary to lifecycle_state.json."""
        target_path = Path(path)
        try:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            all_states: dict[str, Any] = {}
            if target_path.exists():
                try:
                    with open(target_path, "r", encoding="utf-8") as f:
                        all_states = json.load(f)
                except Exception:
                    all_states = {}

            all_states[self.milestone_id] = {
                "milestone_id": self.milestone_id,
                "current_state": self.current_state.value,
                "initial_state": self.initial_state.value,
                "created_at": self.created_at,
                "last_updated": self.last_updated,
                "history": [ev.to_dict() for ev in self.history],
            }

            temp_path = target_path.with_suffix(".tmp")
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(all_states, f, indent=2)
            temp_path.replace(target_path)
        except Exception as exc:
            logger.error("Failed persisting lifecycle state for %s: %s", self.milestone_id, exc)

    def _append_to_dashboard_log(self, path: Path | str = LIFECYCLE_LOG_PATH) -> None:
        """Append the latest state transition row to dashboards/LIFECYCLE_LOG.md."""
        target_path = Path(path)
        try:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            if not target_path.exists() or target_path.stat().st_size == 0:
                with open(target_path, "w", encoding="utf-8") as f:
                    f.write(LOG_HEADER)

            with open(target_path, "a", encoding="utf-8") as f:
                f.write(self.to_dashboard_row() + "\n")
        except Exception as exc:
            logger.error("Failed appending row to %s: %s", target_path, exc)

    @classmethod
    def load_all(
        cls,
        path: Path | str = STATE_PERSISTENCE_PATH,
        event_bus: EventBus | None = None,
    ) -> dict[str, MilestoneLifecycle]:
        """Reconstitute all persisted MilestoneLifecycle instances from JSON storage."""
        target_path = Path(path)
        if not target_path.exists():
            return {}

        try:
            with open(target_path, "r", encoding="utf-8") as f:
                raw_data = json.load(f)
        except Exception as exc:
            logger.error("Failed loading lifecycle state from %s: %s", target_path, exc)
            return {}

        reconstituted: dict[str, MilestoneLifecycle] = {}
        for milestone_id, record in raw_data.items():
            curr_state = MilestoneState(record["current_state"])
            init_state = MilestoneState(record.get("initial_state", MilestoneState.PENDING.value))
            instance = cls(milestone_id=milestone_id, initial_state=init_state, event_bus=event_bus)
            instance.current_state = curr_state
            instance.created_at = record.get("created_at", datetime.now(timezone.utc).isoformat())
            instance.last_updated = record.get("last_updated", datetime.now(timezone.utc).isoformat())
            instance.history = [TransitionEvent.from_dict(item) for item in record.get("history", [])]
            reconstituted[milestone_id] = instance

        return reconstituted


if __name__ == "__main__":
    print("=" * 70)
    print("DEMO: Milestone Lifecycle State Machine & Transition Event Bus")
    print("=" * 70)

    # Initialize Event Bus
    bus = EventBus()

    # Register observer handlers
    def audit_listener(event: TransitionEvent) -> None:
        print(f"[EVENT BUS - AUDIT] {event.milestone_id}: {event.from_state.value} -> {event.to_state.value}")

    def failing_listener(event: TransitionEvent) -> None:
        # Deliberately raise exception on EXECUTING to verify fault isolation
        if event.to_state == MilestoneState.EXECUTING:
            raise RuntimeError("Telemetry collector temporarily disconnected")

    bus.subscribe("transition", audit_listener)
    bus.subscribe("transition", failing_listener)

    # Instantiate lifecycle
    milestone_id = "ms-core-telemetry-001"
    lifecycle = MilestoneLifecycle(milestone_id=milestone_id, initial_state=MilestoneState.PENDING, event_bus=bus)
    print(f"Created Milestone {milestone_id} in initial state: {lifecycle.get_state().value}")

    # Canonical execution trace:
    # PENDING -> GATE_REVIEW -> APPROVED -> DISPATCHED -> EXECUTING -> SUCCEEDED
    lifecycle_steps = [
        (MilestoneState.GATE_REVIEW, {"reviewer": "automated_gate_check", "score": 0.98}),
        (MilestoneState.APPROVED, {"sign_off": "lead_architect", "ticket": "HERMES-402"}),
        (MilestoneState.DISPATCHED, {"worker_id": "worker-pool-node-3", "queue": "priority"}),
        (MilestoneState.EXECUTING, {"pid": 18241, "concurrency_tier": "BALANCED_OPTIMAL"}),
        (MilestoneState.SUCCEEDED, {"exit_code": 0, "artifacts": ["dist/bundle.tar.gz"]}),
    ]

    for next_state, metadata in lifecycle_steps:
        lifecycle.transition(next_state, metadata=metadata)

    print("\n" + "=" * 70)
    print("DASHBOARD ROW OUTPUT:")
    print(lifecycle.to_dashboard_row())

    print("\n" + "=" * 70)
    print("TRANSITION HISTORY:")
    for idx, ev in enumerate(lifecycle.get_history(), start=1):
        print(
            f"  {idx}. [{ev.timestamp}] {ev.from_state.value} -> {ev.to_state.value} | Meta: {ev.metadata}"
        )

    print("\n" + "=" * 70)
    print("DWELL TIME ANALYSIS:")
    for state in [
        MilestoneState.PENDING,
        MilestoneState.GATE_REVIEW,
        MilestoneState.APPROVED,
        MilestoneState.DISPATCHED,
        MilestoneState.EXECUTING,
        MilestoneState.SUCCEEDED,
    ]:
        dwell = lifecycle.get_dwell_time(state)
        print(f"  - State {state.value:<12}: {dwell:.6f} seconds")

    print("\n" + "=" * 70)
    print("RECONSTITUTION FROM JSON TEST:")
    all_reconstituted = MilestoneLifecycle.load_all()
    print(f"Found {len(all_reconstituted)} persisted milestones in JSON.")
    if milestone_id in all_reconstituted:
        loaded = all_reconstituted[milestone_id]
        print(f"  Loaded milestone: {loaded.milestone_id}")
        print(f"  Current state   : {loaded.get_state().value}")
        print(f"  Events recorded : {len(loaded.get_history())}")

    print("\n" + "=" * 70)
    print("ILLEGAL TRANSITION VALIDATION TEST:")
    try:
        # SUCCEEDED has no valid outgoing edges
        lifecycle.transition(MilestoneState.EXECUTING)
    except IllegalTransitionError as err:
        print(f"  Successfully caught expected error: {err}")

    print("=" * 70)
    print("Execution verification completed successfully.")
    print("=" * 70)