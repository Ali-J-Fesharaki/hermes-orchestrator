#!/usr/bin/env python3
"""
Pipeline Autonomic Reactive Stream Backpressure & Credit-Based Flow Controller
==============================================================================

An autonomic, mathematically grounded reactive backpressure governor for distributed
task execution pipelines, multi-agent dispatch streams, and asynchronous consumer pools.

Algorithmic & Mathematical Architecture:
----------------------------------------
1. Credit-Based Flow Control Protocol:
   - Consumer C_j maintains dynamic credit pool K_j(t) in [0, B_cap].
   - Producer P_i holds allocated credits c_held >= 0. P_i cannot dispatch or enqueue
     a task without atomically acquiring and consuming 1 credit (c_held <- c_held - 1).
   - Credit Grant Velocity: At evaluation epoch t + Delta t, consumer grants credits:
       Delta K_j = max(0, floor(B_cap - Q_j(t) - alpha * dot{Q}_j(t)))
     where Q_j(t) is instantaneous queue depth, B_cap is capacity ceiling, and
     dot{Q}_j(t) = (Q_j(t) - Q_j(t - Delta t)) / Delta t is queue arrival velocity
     (damping coefficient alpha = 0.5).

2. Dwell-Time Gradient & Congestion Avoidance (TCP Vegas / BBR Inspired):
   - Measures individual task dwell latency:
       tau_dwell = t_dequeue - t_enqueue
   - Tracks minimum base propagation / processing latency:
       tau_base = min_{t' <= t} tau_dwell(t')
   - Computes queue delay gradient:
       Delta tau = tau_dwell - tau_base
   - Congestion Avoidance Policy (AIMD):
       If Delta tau > tau_thresh (default 15.0s):
           Delta t_dispatch(t + 1) = beta * Delta t_dispatch(t)    [Multiplicative Decrease, beta = 1.5]
       If Delta tau <= tau_thresh and Q_j < W_med:
           Delta t_dispatch(t + 1) = max(Delta t_min, Delta t_dispatch(t) - delta)
           [Additive Increase, delta = 0.5s, Delta t_min = 0.05s]

3. Tri-Band Watermark Gating & State Machine:
   - W_low  = 0.30 * B_cap : Resumption boundary (FLOW_NOMINAL, clears throttling).
   - W_med  = 0.70 * B_cap : Warning boundary (FLOW_PRESSURE, activates AIMD rate pacing).
   - W_high = 0.90 * B_cap : Suspension boundary (FLOW_BACKPRESSURED / PAUSED_BACKPRESSURE,
                             freezes credit grants, revokes excess grants, throttles dispatch).
   - Transparent reason token:
       "Upstream Backpressure Gating: Consumer Queue [Q={q}/{cap}] saturated, Dwell Delta={dt:.1f}s, Cooldown={cd:.1f}s"

4. Composite Flow Stability Index:
   - Calculates S_flow in [0.0, 1.0]:
       S_flow = w1 * (1.0 - bar{Q} / B_cap)
              + w2 * exp(-Delta tau / max(1.0, tau_base))
              + w3 * (1.0 - (N_drops + N_pauses) / max(1, N_total))
     with default weights: w1 = 0.35, w2 = 0.35, w3 = 0.30.

5. Architectural Artifacts & Export:
   - Thread-safe synchronization using reentrant locks (threading.RLock).
   - Emits persistent structured telemetry to dashboards/reactive_backpressure.json.
   - Emits human-readable operational Markdown report to dashboards/REACTIVE_BACKPRESSURE.md.
   - Integrates cleanly with tasks/execution_state_reasoner.py and control_center/server.py.
"""

from __future__ import annotations

import collections
import dataclasses
import enum
import json
import math
import os
import pathlib
import sys
import threading
import time
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple


# =====================================================================
# State Enums & Data Structures
# =====================================================================

class BackpressureState(str, enum.Enum):
    """Tri-band operational state for backpressure governance."""
    FLOW_NOMINAL = "FLOW_NOMINAL"              # Q < W_med, Delta tau <= tau_thresh
    FLOW_PRESSURE = "FLOW_PRESSURE"            # W_med <= Q < W_high or Delta tau > tau_thresh
    FLOW_BACKPRESSURED = "FLOW_BACKPRESSURED"  # Q >= W_high, dispatches paused / credits revoked


@dataclasses.dataclass
class TaskEnvelope:
    """Wrapped task item with tracking metadata for dwell time and provenance."""
    task_id: str
    producer_id: str
    payload: Any
    t_enqueue: float = dataclasses.field(default_factory=time.time)
    t_dequeue: Optional[float] = None
    priority: int = 10  # Lower number = higher priority

    @property
    def dwell_time(self) -> float:
        """Computes total queue dwell time (latency before processing)."""
        if self.t_dequeue is None:
            return max(0.0, time.time() - self.t_enqueue)
        return max(0.0, self.t_dequeue - self.t_enqueue)


@dataclasses.dataclass
class FlowMetricsSnapshot:
    """Point-in-time telemetry snapshot of the backpressure engine."""
    timestamp: float
    state: BackpressureState
    queue_depth: int
    capacity_ceiling: int
    credits_available: int
    credits_in_flight: int
    arrival_velocity: float
    current_dwell_time: float
    base_dwell_time: float
    dwell_gradient: float
    dispatch_interval: float
    stability_index: float
    total_enqueued: int
    total_dequeued: int
    total_dropped: int
    total_pauses: int
    reason_token: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "state": self.state.value,
            "queue_depth": self.queue_depth,
            "capacity_ceiling": self.capacity_ceiling,
            "credits_available": self.credits_available,
            "credits_in_flight": self.credits_in_flight,
            "arrival_velocity": round(self.arrival_velocity, 4),
            "current_dwell_time": round(self.current_dwell_time, 4),
            "base_dwell_time": round(self.base_dwell_time, 4),
            "dwell_gradient": round(self.dwell_gradient, 4),
            "dispatch_interval": round(self.dispatch_interval, 4),
            "stability_index": round(self.stability_index, 4),
            "total_enqueued": self.total_enqueued,
            "total_dequeued": self.total_dequeued,
            "total_dropped": self.total_dropped,
            "total_pauses": self.total_pauses,
            "reason_token": self.reason_token,
        }


# =====================================================================
# Reactive Backpressure Controller
# =====================================================================

class ReactiveBackpressureController:
    """
    Autonomic reactive stream flow controller providing dynamic credit issuance,
    TCP Vegas/BBR style latency gradient sensing, and tri-band watermark gating.
    """

    def __init__(
        self,
        capacity_ceiling: int = 100,
        damping_alpha: float = 0.5,
        tau_thresh: float = 15.0,
        aimd_beta: float = 1.5,
        aimd_delta: float = 0.5,
        min_dispatch_interval: float = 0.05,
        max_dispatch_interval: float = 30.0,
        initial_dispatch_interval: float = 0.20,
        weight_queue: float = 0.35,
        weight_dwell: float = 0.35,
        weight_loss: float = 0.30,
        history_window_size: int = 200,
        export_dir: Optional[pathlib.Path] = None,
    ):
        """
        Initialize the reactive backpressure controller.

        Args:
            capacity_ceiling: B_cap, absolute maximum queue capacity.
            damping_alpha: Alpha damping coefficient for arrival velocity.
            tau_thresh: Congestion threshold in seconds for dwell-time gradient.
            aimd_beta: Multiplicative decrease multiplier for dispatch pacing.
            aimd_delta: Additive increase step (seconds) for dispatch pacing.
            min_dispatch_interval: Lower bound Delta t_min for dispatch delay.
            max_dispatch_interval: Upper bound Delta t_max for dispatch delay.
            initial_dispatch_interval: Initial dispatch interval Delta t_dispatch.
            weight_queue: w1 in composite flow stability formulation.
            weight_dwell: w2 in composite flow stability formulation.
            weight_loss: w3 in composite flow stability formulation.
            history_window_size: Rolling window size for moving average calculations.
            export_dir: Target directory for telemetry artifacts (default: dashboards/).
        """
        self._lock = threading.RLock()

        # Capacity & Watermarks
        self.capacity_ceiling = max(10, capacity_ceiling)
        self.w_low = 0.30 * self.capacity_ceiling
        self.w_med = 0.70 * self.capacity_ceiling
        self.w_high = 0.90 * self.capacity_ceiling

        # Algorithmic Parameters
        self.alpha = damping_alpha
        self.tau_thresh = tau_thresh
        self.beta = aimd_beta
        self.delta = aimd_delta
        self.min_dispatch_interval = min_dispatch_interval
        self.max_dispatch_interval = max_dispatch_interval
        self.current_dispatch_interval = initial_dispatch_interval

        # Stability Weights
        self.w1 = weight_queue
        self.w2 = weight_dwell
        self.w3 = weight_loss

        # Core State Buffers
        self._queue: Deque[TaskEnvelope] = collections.deque()
        self._state: BackpressureState = BackpressureState.FLOW_NOMINAL
        self._reason_token: str = "Nominal operating conditions. System healthy."

        # Credit Management
        self._available_credits: int = self.capacity_ceiling
        self._producer_credits: Dict[str, int] = collections.defaultdict(int)

        # Gradient & Velocity Tracking
        self._last_epoch_time: float = time.time()
        self._last_queue_depth: int = 0
        self._tau_base: float = float("inf")
        self._last_dwell_time: float = 0.0
        self._last_dwell_gradient: float = 0.0
        self._cooldown_expiry: float = 0.0

        # Rolling Dwell Time & Queue Statistics
        self._dwell_history: Deque[float] = collections.deque(maxlen=history_window_size)
        self._queue_history: Deque[int] = collections.deque(maxlen=history_window_size)

        # Operational Counters
        self._total_enqueued: int = 0
        self._total_dequeued: int = 0
        self._total_dropped: int = 0
        self._total_pauses: int = 0

        # Export Paths
        if export_dir is None:
            self.export_dir = pathlib.Path(__file__).resolve().parent.parent / "dashboards"
        else:
            self.export_dir = pathlib.Path(export_dir)
        self.export_dir.mkdir(parents=True, exist_ok=True)

        self.json_path = self.export_dir / "reactive_backpressure.json"
        self.md_path = self.export_dir / "REACTIVE_BACKPRESSURE.md"

        # Record initial baseline state
        self._evaluate_state_locked()

    # -----------------------------------------------------------------
    # Credit Protocol Implementation
    # -----------------------------------------------------------------

    def evaluate_credit_grant(self) -> int:
        """
        Computes dynamic credit grant velocity based on queue depth and arrival velocity:
            Delta K_j = max(0, floor(B_cap - Q_j(t) - alpha * dot{Q}_j(t)))

        Returns:
            The number of newly granted credits.
        """
        with self._lock:
            now = time.time()
            delta_t = max(0.001, now - self._last_epoch_time)
            q_current = len(self._queue)

            # Queue arrival velocity dot{Q}(t)
            arrival_velocity = (q_current - self._last_queue_depth) / delta_t

            # Velocity-damped credit allotment
            damped_capacity = self.capacity_ceiling - q_current - (self.alpha * arrival_velocity)
            delta_k = max(0, math.floor(damped_capacity))

            # Respect high watermark constraint: in FLOW_BACKPRESSURED, credit grants are zeroed
            if q_current >= self.w_high:
                delta_k = 0

            # Cap available credits so in-flight + available does not exceed B_cap
            current_allocated = sum(self._producer_credits.values())
            allowable_credits = max(0, self.capacity_ceiling - q_current - current_allocated)
            granted = min(delta_k, allowable_credits)

            self._available_credits = granted
            self._last_epoch_time = now
            self._last_queue_depth = q_current

            return granted

    def request_credits(self, producer_id: str, requested: int = 1) -> int:
        """
        Allocates credits to a specific producer if capacity allows and state != FLOW_BACKPRESSURED.

        Args:
            producer_id: Unique producer identifier.
            requested: Number of credits requested.

        Returns:
            Number of credits actually granted (0 if throttled or unavailable).
        """
        with self._lock:
            self.evaluate_credit_grant()

            if self._state == BackpressureState.FLOW_BACKPRESSURED:
                return 0

            grant = min(requested, self._available_credits)
            if grant > 0:
                self._available_credits -= grant
                self._producer_credits[producer_id] += grant
            return grant

    def consume_credit(self, producer_id: str) -> bool:
        """
        Consumes 1 held credit for task dispatch.

        Returns:
            True if credit was available and consumed; False otherwise.
        """
        with self._lock:
            if self._producer_credits.get(producer_id, 0) > 0:
                self._producer_credits[producer_id] -= 1
                return True
            return False

    def revoke_credits(self, producer_id: Optional[str] = None) -> int:
        """
        Revokes outstanding credits when approaching or exceeding high watermark.

        Args:
            producer_id: If provided, revokes credits for that producer; otherwise revokes all.

        Returns:
            Total credits revoked.
        """
        with self._lock:
            revoked = 0
            if producer_id:
                revoked = self._producer_credits.pop(producer_id, 0)
            else:
                revoked = sum(self._producer_credits.values())
                self._producer_credits.clear()
            self._available_credits = 0
            return revoked

    # -----------------------------------------------------------------
    # Enqueue / Dequeue & Latency Gradient Tracking
    # -----------------------------------------------------------------

    def enqueue_task(self, envelope: TaskEnvelope) -> bool:
        """
        Enqueues an incoming task envelope governed by credit checks and watermarks.

        Returns:
            True if enqueued successfully; False if dropped due to hard saturation.
        """
        with self._lock:
            # Watermark check
            q_depth = len(self._queue)
            if q_depth >= self.capacity_ceiling:
                self._total_dropped += 1
                self._record_reason_locked(f"DROPPED: Capacity ceiling {self.capacity_ceiling} exceeded")
                self._evaluate_state_locked()
                return False

            envelope.t_enqueue = time.time()
            self._queue.append(envelope)
            self._total_enqueued += 1
            self._queue_history.append(len(self._queue))

            self._evaluate_state_locked()
            return True

    def dequeue_task(self) -> Optional[TaskEnvelope]:
        """
        Dequeues the oldest task, records dwell latency, updates base latency,
        and triggers AIMD dispatch interval adjustment.

        Returns:
            The TaskEnvelope, or None if queue is empty.
        """
        with self._lock:
            if not self._queue:
                return None

            envelope = self._queue.popleft()
            now = time.time()
            envelope.t_dequeue = now

            # Dwell latency tracking
            tau_dwell = envelope.dwell_time
            self._last_dwell_time = tau_dwell
            self._dwell_history.append(tau_dwell)

            # Update base propagation latency tau_base = min_{t' <= t} tau_dwell(t')
            if tau_dwell < self._tau_base or math.isinf(self._tau_base):
                # Warmup protection: ignore sub-millisecond anomalies
                self._tau_base = max(0.001, tau_dwell)

            # Dwell gradient: Delta tau = tau_dwell - tau_base
            delta_tau = max(0.0, tau_dwell - self._tau_base)
            self._last_dwell_gradient = delta_tau

            self._total_dequeued += 1

            # AIMD Rate Modulation (TCP Vegas / BBR inspired)
            self._apply_aimd_rate_modulation_locked(delta_tau)

            # Re-evaluate state machine and watermarks
            self._evaluate_state_locked()

            return envelope

    # -----------------------------------------------------------------
    # Congestion Avoidance & Watermark State Machine
    # -----------------------------------------------------------------

    def _apply_aimd_rate_modulation_locked(self, delta_tau: float) -> None:
        """Applies Additive Increase / Multiplicative Decrease to dispatch pacing."""
        q_depth = len(self._queue)

        if delta_tau > self.tau_thresh:
            # Congestion detected -> Multiplicative Decrease (increase interval)
            old_interval = self.current_dispatch_interval
            self.current_dispatch_interval = min(
                self.max_dispatch_interval,
                self.current_dispatch_interval * self.beta,
            )
            self._record_reason_locked(
                f"Multiplicative Decrease: Delay gradient {delta_tau:.2f}s > {self.tau_thresh}s. "
                f"Paced interval: {old_interval:.2f}s -> {self.current_dispatch_interval:.2f}s"
            )
        elif delta_tau <= self.tau_thresh and q_depth < self.w_med:
            # Safe zone -> Additive Increase (decrease interval)
            old_interval = self.current_dispatch_interval
            self.current_dispatch_interval = max(
                self.min_dispatch_interval,
                self.current_dispatch_interval - self.delta,
            )

    def _evaluate_state_locked(self) -> None:
        """Evaluates tri-band watermarks and transitions the backpressure state machine."""
        q_depth = len(self._queue)
        now = time.time()
        cooldown_remaining = max(0.0, self._cooldown_expiry - now)

        # High Watermark -> FLOW_BACKPRESSURED
        if q_depth >= self.w_high:
            if self._state != BackpressureState.FLOW_BACKPRESSURED:
                self._total_pauses += 1
                self.revoke_credits()
                self._cooldown_expiry = now + 5.0  # 5-second mandatory stabilization hysteresis

            self._state = BackpressureState.FLOW_BACKPRESSURED
            self._reason_token = (
                f"Upstream Backpressure Gating: Consumer Queue [Q={q_depth}/{self.capacity_ceiling}] "
                f"saturated, Dwell Delta={self._last_dwell_gradient:.1f}s, Cooldown={cooldown_remaining:.1f}s"
            )

        # Medium Watermark -> FLOW_PRESSURE
        elif q_depth >= self.w_med or self._last_dwell_gradient > self.tau_thresh:
            self._state = BackpressureState.FLOW_PRESSURE
            self._reason_token = (
                f"Flow Pressure Warning: Queue depth {q_depth} >= {int(self.w_med)} or "
                f"Gradient {self._last_dwell_gradient:.1f}s > {self.tau_thresh:.1f}s"
            )

        # Low Watermark & Cooldown Elapsed -> FLOW_NOMINAL
        elif q_depth <= self.w_low and cooldown_remaining <= 0.0:
            self._state = BackpressureState.FLOW_NOMINAL
            self._reason_token = (
                f"Nominal Flow: Queue [Q={q_depth}/{self.capacity_ceiling}] within optimal band, "
                f"Dwell Delta={self._last_dwell_gradient:.1f}s"
            )
        else:
            # Retain existing state if in hysteresis buffer
            pass

    def _record_reason_locked(self, token: str) -> None:
        """Updates internal status reason token."""
        self._reason_token = token

    # -----------------------------------------------------------------
    # Composite Flow Stability Formulation
    # -----------------------------------------------------------------

    def calculate_flow_stability(self) -> float:
        """
        Formulates composite flow stability metric S_flow in [0.0, 1.0]:
            S_flow = w1 * (1 - bar{Q} / B_cap)
                   + w2 * exp(-Delta tau / max(1.0, tau_base))
                   + w3 * (1 - (N_drops + N_pauses) / max(1, N_total))
        """
        with self._lock:
            # 1. Queue depth ratio component
            avg_q = (
                sum(self._queue_history) / len(self._queue_history)
                if self._queue_history
                else float(len(self._queue))
            )
            term_queue = max(0.0, 1.0 - (avg_q / self.capacity_ceiling))

            # 2. Latency gradient decay component
            tau_base_norm = max(1.0, self._tau_base if not math.isinf(self._tau_base) else 1.0)
            term_dwell = math.exp(-self._last_dwell_gradient / tau_base_norm)

            # 3. Drop & pause attrition component
            n_total = max(1, self._total_enqueued)
            attrition = (self._total_dropped + self._total_pauses) / n_total
            term_loss = max(0.0, 1.0 - min(1.0, attrition))

            # Weighted sum
            s_flow = (self.w1 * term_queue) + (self.w2 * term_dwell) + (self.w3 * term_loss)
            return max(0.0, min(1.0, s_flow))

    # -----------------------------------------------------------------
    # Telemetry Snapshot & Dashboard Export
    # -----------------------------------------------------------------

    def get_snapshot(self) -> FlowMetricsSnapshot:
        """Captures a complete thread-safe telemetry snapshot."""
        with self._lock:
            now = time.time()
            delta_t = max(0.001, now - self._last_epoch_time)
            q_len = len(self._queue)
            arrival_vel = (q_len - self._last_queue_depth) / delta_t
            in_flight = sum(self._producer_credits.values())

            return FlowMetricsSnapshot(
                timestamp=now,
                state=self._state,
                queue_depth=q_len,
                capacity_ceiling=self.capacity_ceiling,
                credits_available=self._available_credits,
                credits_in_flight=in_flight,
                arrival_velocity=arrival_vel,
                current_dwell_time=self._last_dwell_time,
                base_dwell_time=0.0 if math.isinf(self._tau_base) else self._tau_base,
                dwell_gradient=self._last_dwell_gradient,
                dispatch_interval=self.current_dispatch_interval,
                stability_index=self.calculate_flow_stability(),
                total_enqueued=self._total_enqueued,
                total_dequeued=self._total_dequeued,
                total_dropped=self._total_dropped,
                total_pauses=self._total_pauses,
                reason_token=self._reason_token,
            )

    def export_dashboards(self) -> Tuple[pathlib.Path, pathlib.Path]:
        """
        Emits structured JSON to dashboards/reactive_backpressure.json
        and human-readable Markdown to dashboards/REACTIVE_BACKPRESSURE.md.
        """
        with self._lock:
            snap = self.get_snapshot()

            # 1. Export JSON artifact
            telemetry_data = {
                "schema_version": "1.0.0",
                "component": "pipeline_reactive_backpressure_controller",
                "snapshot": snap.to_dict(),
                "configuration": {
                    "capacity_ceiling": self.capacity_ceiling,
                    "watermarks": {
                        "w_low": self.w_low,
                        "w_med": self.w_med,
                        "w_high": self.w_high,
                    },
                    "aimd": {
                        "tau_thresh_seconds": self.tau_thresh,
                        "beta_decrease": self.beta,
                        "delta_increase": self.delta,
                        "min_dispatch_interval": self.min_dispatch_interval,
                        "max_dispatch_interval": self.max_dispatch_interval,
                    },
                    "stability_weights": {
                        "w1_queue": self.w1,
                        "w2_dwell": self.w2,
                        "w3_loss": self.w3,
                    },
                },
            }

            temp_json = self.json_path.with_suffix(".tmp")
            with open(temp_json, "w", encoding="utf-8") as f:
                json.dump(telemetry_data, f, indent=2)
            temp_json.replace(self.json_path)

            # 2. Export Markdown dashboard
            state_badge = {
                BackpressureState.FLOW_NOMINAL: "[🟢 FLOW_NOMINAL - HEALTHY]",
                BackpressureState.FLOW_PRESSURE: "[🟡 FLOW_PRESSURE - PACING ACTIVE]",
                BackpressureState.FLOW_BACKPRESSURED: "[🔴 FLOW_BACKPRESSURED - GATING]",
            }.get(snap.state, "[⚪ UNKNOWN]")

            md_content = f"""# Pipeline Reactive Backpressure & Flow Control Cockpit

**System State:** {state_badge}  
**Last Updated:** {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(snap.timestamp))}  
**Reason Token:** `{snap.reason_token}`

---

## 1. Executive Telemetry Overview

| Metric | Current Value | Target / Bounds | Status |
| :--- | :--- | :--- | :--- |
| **Composite Stability Index (S_flow)** | `{snap.stability_index:.4f}` | `[0.8500, 1.0000]` | {'Optimal' if snap.stability_index >= 0.8 else 'Degraded'} |
| **Instantaneous Queue Depth (Q)** | `{snap.queue_depth} / {snap.capacity_ceiling}` | `< {int(self.w_med)}` (Med Watermark) | {'Normal' if snap.queue_depth < self.w_med else 'Pressured'} |
| **Dynamic Credit Pool (K_avail / Held)** | `{snap.credits_available} / {snap.credits_in_flight}` | `<= {snap.capacity_ceiling}` | Dynamic Granting |
| **Paced Dispatch Interval (Δt)** | `{snap.dispatch_interval:.3f}s` | `[{self.min_dispatch_interval:.2f}s, {self.max_dispatch_interval:.2f}s]` | {'Accelerating' if snap.dispatch_interval <= 0.2 else 'Throttling'} |
| **Queue Dwell Gradient (Δτ)** | `{snap.dwell_gradient:.3f}s` | `<= {self.tau_thresh:.1f}s` (τ_thresh) | {'Nominal' if snap.dwell_gradient <= self.tau_thresh else 'Congested'} |
| **Base Propagation Latency (τ_base)** | `{snap.base_dwell_time:.4f}s` | Minimal observed | Baseline Reference |

---

## 2. Watermark & Gating Profile

- **Low Watermark (W_low = 30%):** `{int(self.w_low)}` items -> Flow resumes, credit restrictions lifted.
- **Medium Watermark (W_med = 70%):** `{int(self.w_med)}` items -> Warning boundary, AIMD additive increase throttled.
- **High Watermark (W_high = 90%):** `{int(self.w_high)}` items -> Suspension boundary, all credits revoked, dispatches frozen.

---

## 3. Cumulative Flow Counters

- **Total Tasks Enqueued:** `{snap.total_enqueued}`
- **Total Tasks Dequeued:** `{snap.total_dequeued}`
- **Total Saturation Drops:** `{snap.total_dropped}`
- **Backpressure Pauses Triggered:** `{snap.total_pauses}`
- **Current In-Flight Credits:** `{snap.credits_in_flight}`

---
*Generated autonomously by `tasks/pipeline_reactive_backpressure_controller.py`*
"""
            temp_md = self.md_path.with_suffix(".tmp")
            with open(temp_md, "w", encoding="utf-8") as f:
                f.write(md_content)
            temp_md.replace(self.md_path)

            return self.json_path, self.md_path


# =====================================================================
# Pipeline Producer & Consumer Mock Agents for Integration & Simulation
# =====================================================================

class ManagedBackpressureProducer(threading.Thread):
    """Producer agent participating in credit-based backpressure protocol."""

    def __init__(
        self,
        producer_id: str,
        controller: ReactiveBackpressureController,
        total_tasks: int = 50,
        burst_rate: float = 0.02,
    ):
        super().__init__(name=f"Producer-{producer_id}", daemon=True)
        self.producer_id = producer_id
        self.controller = controller
        self.total_tasks = total_tasks
        self.burst_rate = burst_rate
        self.enqueued_count = 0
        self.stalled_count = 0

    def run(self) -> None:
        for i in range(self.total_tasks):
            # Must acquire credit before dispatching
            acquired = self.controller.request_credits(self.producer_id, requested=1)
            if acquired <= 0:
                self.stalled_count += 1
                time.sleep(self.controller.current_dispatch_interval)
                continue

            # Consume 1 credit
            consumed = self.controller.consume_credit(self.producer_id)
            if not consumed:
                self.stalled_count += 1
                time.sleep(0.05)
                continue

            envelope = TaskEnvelope(
                task_id=f"{self.producer_id}-T{i+1}",
                producer_id=self.producer_id,
                payload={"data": f"payload_{i+1}", "seq": i},
            )

            success = self.controller.enqueue_task(envelope)
            if success:
                self.enqueued_count += 1

            # Dispatch delay with current controller interval
            time.sleep(self.burst_rate)


class ManagedBackpressureConsumer(threading.Thread):
    """Consumer agent draining tasks with artificial service delay."""

    def __init__(
        self,
        consumer_id: str,
        controller: ReactiveBackpressureController,
        service_time_sec: float = 0.05,
        stop_event: Optional[threading.Event] = None,
    ):
        super().__init__(name=f"Consumer-{consumer_id}", daemon=True)
        self.consumer_id = consumer_id
        self.controller = controller
        self.service_time_sec = service_time_sec
        self.stop_event = stop_event or threading.Event()
        self.processed_count = 0

    def run(self) -> None:
        while not self.stop_event.is_set():
            envelope = self.controller.dequeue_task()
            if envelope is None:
                time.sleep(0.01)
                continue

            # Simulate downstream processing
            time.sleep(self.service_time_sec)
            self.processed_count += 1


# =====================================================================
# Integration Hooks with Orchestrator & Execution State Reasoner
# =====================================================================

def create_reactive_controller(
    capacity: int = 100,
    export_dir: Optional[str] = None,
) -> ReactiveBackpressureController:
    """Factory helper for integration with tasks/execution_state_reasoner.py."""
    path = pathlib.Path(export_dir) if export_dir else None
    return ReactiveBackpressureController(capacity_ceiling=capacity, export_dir=path)


# =====================================================================
# Self-Testing Verification Suite
# =====================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("RUNNING EXTENSIVE SELF-TEST SUITE: Reactive Backpressure Controller")
    print("=" * 80)

    # Setup isolated test directory in dashboards
    test_dashboards = pathlib.Path(__file__).resolve().parent.parent / "dashboards"
    test_dashboards.mkdir(parents=True, exist_ok=True)

    controller = ReactiveBackpressureController(
        capacity_ceiling=40,
        tau_thresh=0.10,          # 100ms threshold to trigger AIMD during tests
        initial_dispatch_interval=0.10,
        export_dir=test_dashboards,
    )

    print(f"\n[Test 1] Initializing Controller & Verifying Watermarks:")
    print(f"  B_cap: {controller.capacity_ceiling}")
    print(f"  W_low: {controller.w_low:.1f} (30%)")
    print(f"  W_med: {controller.w_med:.1f} (70%)")
    print(f"  W_high: {controller.w_high:.1f} (90%)")
    assert controller.w_low == 12.0
    assert controller.w_med == 28.0
    assert controller.w_high == 36.0
    print("  -> Watermarks accurately initialized.")

    print("\n[Test 2] Credit Granting & Damped Velocity:")
    granted = controller.evaluate_credit_grant()
    print(f"  Initial Credit Grant: {granted}")
    assert granted > 0
    p1_grant = controller.request_credits("producer_alpha", 5)
    print(f"  Requested 5 credits for producer_alpha: got {p1_grant}")
    assert p1_grant == 5
    consumed = controller.consume_credit("producer_alpha")
    print(f"  Consumed 1 credit: {consumed}")
    assert consumed is True

    print("\n[Test 3] Simulating High Watermark Saturation & Throttling:")
    # Rapidly fill queue up to W_high
    for idx in range(37):
        envelope = TaskEnvelope(
            task_id=f"SYNTH-{idx}",
            producer_id="synth_prod",
            payload={"idx": idx},
        )
        controller.enqueue_task(envelope)

    snap = controller.get_snapshot()
    print(f"  Queue Depth: {snap.queue_depth}")
    print(f"  State: {snap.state.value}")
    print(f"  Reason Token: {snap.reason_token}")
    assert snap.state == BackpressureState.FLOW_BACKPRESSURED
    assert controller.request_credits("producer_beta", 2) == 0
    print("  -> Throttling confirmed: Zero credits granted under FLOW_BACKPRESSURED.")

    print("\n[Test 4] Simulating Consumer Drainage & Low Watermark Resumption:")
    # Drain until below W_low
    while len(controller._queue) > 10:
        dequeued = controller.dequeue_task()
        time.sleep(0.005)

    # Force clear cooldown for deterministic unit testing
    with controller._lock:
        controller._cooldown_expiry = time.time() - 1.0
        controller._evaluate_state_locked()

    snap_drained = controller.get_snapshot()
    print(f"  Queue Depth Post-Drain: {snap_drained.queue_depth}")
    print(f"  State Post-Drain: {snap_drained.state.value}")
    assert snap_drained.state == BackpressureState.FLOW_NOMINAL
    print("  -> Resumption confirmed: Reverted to FLOW_NOMINAL.")

    print("\n[Test 5] AIMD Latency Gradient Modulation (TCP Vegas Style):")
    initial_interval = controller.current_dispatch_interval
    # Simulate high dwell time task to trip tau_thresh (0.10s)
    slow_envelope = TaskEnvelope(task_id="SLOW-1", producer_id="latency_tester", payload={})
    slow_envelope.t_enqueue = time.time() - 0.50  # 500ms dwell time
    controller._queue.append(slow_envelope)
    controller.dequeue_task()

    paced_interval = controller.current_dispatch_interval
    print(f"  Initial Dispatch Interval: {initial_interval:.4f}s")
    print(f"  Paced Dispatch Interval:   {paced_interval:.4f}s")
    assert paced_interval > initial_interval
    print("  -> Multiplicative decrease verified: dispatch interval widened.")

    print("\n[Test 6] Multi-Producer Multi-Consumer Concurrent Live Stream Simulation:")
    stop_event = threading.Event()
    sim_consumers = [
        ManagedBackpressureConsumer(f"C{c}", controller, service_time_sec=0.01, stop_event=stop_event)
        for c in range(3)
    ]
    sim_producers = [
        ManagedBackpressureProducer(f"P{p}", controller, total_tasks=30, burst_rate=0.005)
        for p in range(4)
    ]

    for c in sim_consumers:
        c.start()
    for p in sim_producers:
        p.start()

    for p in sim_producers:
        p.join(timeout=5.0)

    # Allow consumers to drain remaining items
    time.sleep(0.5)
    stop_event.set()
    for c in sim_consumers:
        c.join(timeout=2.0)

    final_snap = controller.get_snapshot()
    print(f"  Live Simulation Results:")
    print(f"    Total Enqueued: {final_snap.total_enqueued}")
    print(f"    Total Dequeued: {final_snap.total_dequeued}")
    print(f"    Total Dropped:  {final_snap.total_dropped}")
    print(f"    Total Pauses:   {final_snap.total_pauses}")
    print(f"    Stability Index: {final_snap.stability_index:.4f}")
    assert final_snap.total_dequeued > 0
    assert 0.0 <= final_snap.stability_index <= 1.0

    print("\n[Test 7] Dashboard & Structured Telemetry File Export:")
    json_out, md_out = controller.export_dashboards()
    print(f"  JSON Telemetry Written: {json_out} (Exists: {json_out.exists()})")
    print(f"  Markdown Cockpit Written: {md_out} (Exists: {md_out.exists()})")
    assert json_out.exists() and json_out.stat().st_size > 0
    assert md_out.exists() and md_out.stat().st_size > 0

    print("\n" + "=" * 80)
    print("ALL VERIFICATION SUITE CHECKS PASSED PERFECTLY!")
    print("=" * 80)