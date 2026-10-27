#!/usr/bin/env python3
"""
Pipeline Autonomic Fleet Watchdog & Ephemeral Worker Heartbeat Supervisor
========================================================================

Author: Hermes Task Executor (gemini-3.8-flash-tiered)
Role: Principal Systems Reliability & OS Concurrency Architect
Repository: hermes_orchestrator
Module: tasks/pipeline_fleet_watchdog.py

Formal Specifications & Heartbeat Lease Model:
Let W = {w_1, w_2, ..., w_K} be active concurrent agent worker processes across repositories.
1. Heartbeat Lease Model:
   Worker lease tuple H(w_i) = <worker_id, pid, repo_id, milestone_id, t_{start}, t_{heartbeat}, tau_{ttl}, quota_lease_id, fencing_token>.
   Worker must invoke heartbeat_ping(worker_id) at interval Delta t <= tau_{ttl} (default 30s).
   If t_{now} - t_{heartbeat} > tau_{ttl}, classify as LEASE_EXPIRED.

2. Deep OS Process Probing & Inactivity Vector:
   For worker w_i with PID p, inspect /proc/p/stat, /proc/p/status, and /proc/p/cmdline:
   Sample vector V(p, t) = <state in {R, S, D, Z, T}, utime + stime, rss_bytes, num_threads, last_pipe_io_timestamp>.
   Compute CPU progression: Delta CPU = (utime(t) + stime(t)) - (utime(t - Delta t) + stime(t - Delta t)).
   Inactivity Score:
   I(w_i) = w_{cpu} * 1_{Delta CPU == 0} + w_{hb} * 1_{Delta t > tau_{ttl}} + w_{zombie} * 1_{state == Z}.
   If I(w_i) >= 1.0 for duration Delta t_{catatonic} > 60s, mark worker as CATATONIC_STALL.

3. Staged Eviction State Machine:
   Sigma_evict in {ACTIVE, STALLED, SIGTERM_SENT, SIGKILL_ESCALATED, REAPED, RECLAIMED}.
   - Step 1: Send SIGTERM to process group os.killpg(p, signal.SIGTERM). Set grace deadline t_{grace} = 15s.
   - Step 2: If process remains in /proc after t_{grace}, escalate to SIGKILL: os.killpg(p, signal.SIGKILL).
   - Step 3: Reap child exit code via os.waitpid(p, os.WNOHANG) to avoid zombie retention.

4. Cascading Resource Reclamation:
   Upon eviction confirmation:
   - Invoke QuotaReservationLeaseController.release_lease(quota_lease_id) in tasks/pipeline_quota_reservation_lease.py.
   - Invoke DistributedFencingCoordinator.revoke_lease(worker_id) in tasks/pipeline_concurrency_deconfliction.py.
   - Force-prune worktree via WorktreeSandboxManager.clean_sandbox(worktree_path) in tasks/worktree_sandbox_manager.py.
   - Emit structured failure post-mortem with reason CATATONIC_WORKER_EVICTION to dashboards/post_mortem_pedagogy.py
     and persist state to dashboards/fleet_watchdog_state.json.
"""

from __future__ import annotations

import os
import sys
import time
import signal
import subprocess
import json
import logging
import threading
import uuid
from dataclasses import dataclass, field, asdict
from enum import Enum
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any

# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------
logger = logging.getLogger("fleet_watchdog")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter(
        fmt="[%(asctime)s] [%(levelname)s] [FleetWatchdog] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# Core Enums & States
# ---------------------------------------------------------------------------
class WorkerHealthState(str, Enum):
    """Health classification states for active and ephemeral agent workers."""
    HEALTHY = "HEALTHY"
    HEARTBEAT_LATE = "HEARTBEAT_LATE"
    LEASE_EXPIRED = "LEASE_EXPIRED"
    CATATONIC_STALL = "CATATONIC_STALL"
    ZOMBIE = "ZOMBIE"
    EVICTING = "EVICTING"
    TERMINATED = "TERMINATED"
    RECLAIMED = "RECLAIMED"


class EvictionSignal(str, Enum):
    """Signals utilized during staged worker eviction."""
    NONE = "NONE"
    SIGTERM = "SIGTERM"
    SIGKILL = "SIGKILL"


class EvictionStage(str, Enum):
    """Formal states in staged eviction state machine Sigma_evict."""
    ACTIVE = "ACTIVE"
    STALLED = "STALLED"
    SIGTERM_SENT = "SIGTERM_SENT"
    SIGKILL_ESCALATED = "SIGKILL_ESCALATED"
    REAPED = "REAPED"
    RECLAIMED = "RECLAIMED"


# ---------------------------------------------------------------------------
# Core Telemetry & Model Dataclasses
# ---------------------------------------------------------------------------
@dataclass
class ProcessSample:
    """Sample vector V(p, t) representing OS-level process state and CPU time."""
    pid: int
    timestamp: float
    exists: bool = False
    state: str = "?"
    utime: float = 0.0
    stime: float = 0.0
    total_cpu_time: float = 0.0
    rss_bytes: int = 0
    num_threads: int = 0
    cmdline: str = ""
    last_pipe_io_timestamp: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class WorkerHeartbeatLease:
    """Worker lease tuple H(w_i) maintaining execution context and lease metrics."""
    worker_id: str
    pid: int
    repo_id: str
    milestone_id: str
    t_start: float
    t_heartbeat: float
    tau_ttl: float = 30.0
    quota_lease_id: str = ""
    fencing_token: str = ""
    worktree_path: Optional[str] = None
    health_state: WorkerHealthState = WorkerHealthState.HEALTHY
    eviction_stage: EvictionStage = EvictionStage.ACTIVE
    last_sample: Optional[ProcessSample] = None
    previous_sample: Optional[ProcessSample] = None
    catatonic_since: Optional[float] = None
    eviction_initiated_at: Optional[float] = None
    eviction_signal_sent: EvictionSignal = EvictionSignal.NONE
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["health_state"] = self.health_state.value
        data["eviction_stage"] = self.eviction_stage.value
        data["eviction_signal_sent"] = self.eviction_signal_sent.value
        return data


@dataclass
class EvictionIncident:
    """Audit incident record emitted when a stalled/expired worker is evicted."""
    incident_id: str
    worker_id: str
    pid: int
    repo_id: str
    milestone_id: str
    reason: str
    timestamp: float
    health_state: WorkerHealthState
    eviction_stage: EvictionStage
    signal_sent: EvictionSignal
    reclaimed_resources: List[str] = field(default_factory=list)
    error_message: Optional[str] = None
    post_mortem_emitted: bool = False

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["health_state"] = self.health_state.value
        data["eviction_stage"] = self.eviction_stage.value
        data["signal_sent"] = self.signal_sent.value
        return data


@dataclass
class FleetWatchdogSnapshot:
    """Serializable snapshot of global fleet health and watchdog state."""
    snapshot_id: str
    timestamp: float
    active_workers_count: int
    healthy_count: int
    stalled_count: int
    evicted_count: int
    workers: List[Dict[str, Any]] = field(default_factory=list)
    recent_incidents: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# OS Probing & Signal Helpers
# ---------------------------------------------------------------------------
def _safe_kill_process_group(pid: int, sig: signal.Signals) -> bool:
    """
    Safely delivers signal to a process group or fallback individual process.
    Prevents self-signaling by verifying supervisor's own process group ID.
    """
    my_pgid = -1
    try:
        my_pgid = os.getpgid(os.getpid())
    except (OSError, ProcessLookupError, PermissionError):
        pass

    target_pgid = -1
    try:
        target_pgid = os.getpgid(pid)
    except (OSError, ProcessLookupError, PermissionError):
        pass

    # If the target process has its own process group separate from supervisor
    if target_pgid > 0 and target_pgid != my_pgid:
        try:
            os.killpg(target_pgid, sig)
            return True
        except (ProcessLookupError, PermissionError):
            pass

    # Direct fallback signal to the single PID
    try:
        os.kill(pid, sig)
        return True
    except ProcessLookupError:
        return False
    except PermissionError as e:
        logger.error(f"Permission denied signaling PID {pid} with {sig.name}: {e}")
        return False


def _try_reap_process(pid: int) -> bool:
    """
    Attempts to reap child process using non-blocking waitpid.
    Handles ChildProcessError gracefully if pid is not an immediate child.
    """
    try:
        reaped_pid, _ = os.waitpid(pid, os.WNOHANG)
        return reaped_pid == pid
    except ChildProcessError:
        # Not a direct child; init system or systemd will reap it
        return True
    except Exception as e:
        logger.debug(f"Exception while reaping PID {pid}: {e}")
        return False


# ---------------------------------------------------------------------------
# Fleet Watchdog Supervisor Engine
# ---------------------------------------------------------------------------
class FleetWatchdog:
    """
    Autonomic supervisor managing worker heartbeat leases, OS inactivity
    vectors, staged process evictions, and cascading resource reclamation.
    """

    def __init__(
        self,
        state_file: Optional[Path | str] = None,
        post_mortem_dir: Optional[Path | str] = None,
        catatonic_threshold_sec: float = 60.0,
        default_ttl_sec: float = 30.0,
    ) -> None:
        self.lock = threading.RLock()
        self.catatonic_threshold_sec = float(catatonic_threshold_sec)
        self.default_ttl_sec = float(default_ttl_sec)

        # Resolve paths
        repo_root = Path(__file__).resolve().parent.parent
        self.state_file = (
            Path(state_file).resolve()
            if state_file
            else repo_root / "dashboards" / "fleet_watchdog_state.json"
        )
        self.post_mortem_dir = (
            Path(post_mortem_dir).resolve()
            if post_mortem_dir
            else repo_root / "dashboards"
        )

        self._workers: Dict[str, WorkerHeartbeatLease] = {}
        self._incidents: List[EvictionIncident] = []
        self._background_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        # Inactivity weights: w_cpu * 1_{Delta CPU == 0} + w_hb * 1_{Delta t > tau} + w_zombie * 1_{state == Z}
        self.w_cpu: float = 0.5
        self.w_hb: float = 0.5
        self.w_zombie: float = 1.0

        # Ensure dashboard directory exists
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.post_mortem_dir.mkdir(parents=True, exist_ok=True)

    def register_worker(
        self,
        worker_id: str,
        pid: int,
        repo_id: str,
        milestone_id: str,
        quota_lease_id: str = "",
        fencing_token: str = "",
        ttl_sec: Optional[float] = None,
        worktree_path: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> WorkerHeartbeatLease:
        """
        Registers worker w_i into the heartbeat lease ledger.
        """
        with self.lock:
            now = time.time()
            effective_ttl = float(ttl_sec) if ttl_sec is not None else self.default_ttl_sec
            lease = WorkerHeartbeatLease(
                worker_id=worker_id,
                pid=pid,
                repo_id=repo_id,
                milestone_id=milestone_id,
                t_start=now,
                t_heartbeat=now,
                tau_ttl=effective_ttl,
                quota_lease_id=quota_lease_id,
                fencing_token=fencing_token,
                worktree_path=worktree_path,
                health_state=WorkerHealthState.HEALTHY,
                eviction_stage=EvictionStage.ACTIVE,
                metadata=metadata or {},
            )
            initial_sample = self.probe_process(pid)
            lease.last_sample = initial_sample
            lease.previous_sample = initial_sample

            self._workers[worker_id] = lease
            logger.info(
                f"Registered worker '{worker_id}' (PID {pid}, repo '{repo_id}', milestone '{milestone_id}', TTL {effective_ttl}s)"
            )
            self.export_snapshot()
            return lease

    def heartbeat(self, worker_id: str) -> bool:
        """
        Refreshes worker heartbeat timestamp t_{heartbeat} = t_{now}.
        Resets catatonic timers and restores HEALTHY status if previously active.
        """
        with self.lock:
            lease = self._workers.get(worker_id)
            if not lease:
                logger.warning(f"Heartbeat received for unknown worker '{worker_id}'")
                return False

            now = time.time()
            lease.t_heartbeat = now
            lease.catatonic_since = None

            if lease.health_state in (
                WorkerHealthState.HEALTHY,
                WorkerHealthState.HEARTBEAT_LATE,
                WorkerHealthState.LEASE_EXPIRED,
            ):
                lease.health_state = WorkerHealthState.HEALTHY
                lease.eviction_stage = EvictionStage.ACTIVE

            logger.debug(f"Heartbeat renewed for worker '{worker_id}' at t={now:.3f}")
            return True

    def probe_process(self, pid: int) -> ProcessSample:
        """
        Safely probes OS process state via /proc/pid/stat, /proc/pid/status,
        and /proc/pid/cmdline. Provides robust fallback for non-Linux / non-/proc environments.
        """
        sample_time = time.time()
        proc_dir = Path(f"/proc/{pid}")

        if proc_dir.exists() and proc_dir.is_dir():
            state = "?"
            utime = 0.0
            stime = 0.0
            num_threads = 1
            rss_bytes = 0
            cmdline = ""
            last_pipe_io = sample_time

            # 1. Inspect /proc/pid/stat
            stat_file = proc_dir / "stat"
            if stat_file.is_file():
                try:
                    content = stat_file.read_text(encoding="utf-8", errors="replace").strip()
                    rparen = content.rfind(")")
                    if rparen != -1 and rparen + 1 < len(content):
                        rest = content[rparen + 1 :].strip().split()
                        # Field mapping:
                        # rest[0] = state (field 3)
                        # rest[11] = utime (field 14)
                        # rest[12] = stime (field 15)
                        # rest[17] = num_threads (field 20)
                        # rest[21] = rss in pages (field 24)
                        state = rest[0]
                        clk_tck = 100.0
                        try:
                            clk_tck = float(os.sysconf("SC_CLK_TCK"))
                        except (AttributeError, ValueError, OSError):
                            pass

                        if len(rest) > 12:
                            utime = float(rest[11]) / clk_tck
                            stime = float(rest[12]) / clk_tck
                        if len(rest) > 17:
                            num_threads = int(rest[17])
                        if len(rest) > 21:
                            page_size = 4096
                            try:
                                page_size = int(os.sysconf("SC_PAGE_SIZE"))
                            except (AttributeError, ValueError, OSError):
                                pass
                            rss_bytes = int(rest[21]) * page_size
                except (FileNotFoundError, ProcessLookupError, PermissionError, ValueError, IndexError):
                    pass

            # 2. Inspect /proc/pid/status for authoritative VmRSS
            status_file = proc_dir / "status"
            if status_file.is_file():
                try:
                    lines = status_file.read_text(encoding="utf-8", errors="replace").splitlines()
                    for line in lines:
                        if line.startswith("VmRSS:"):
                            parts = line.split()
                            if len(parts) >= 2 and parts[1].isdigit():
                                rss_bytes = int(parts[1]) * 1024
                        elif line.startswith("Threads:"):
                            parts = line.split()
                            if len(parts) >= 2 and parts[1].isdigit():
                                num_threads = int(parts[1])
                        elif line.startswith("State:") and state == "?":
                            parts = line.split()
                            if len(parts) >= 2:
                                state = parts[1][0]
                except (FileNotFoundError, ProcessLookupError, PermissionError):
                    pass

            # 3. Inspect /proc/pid/cmdline
            cmdline_file = proc_dir / "cmdline"
            if cmdline_file.is_file():
                try:
                    raw_bytes = cmdline_file.read_bytes()
                    cmdline = raw_bytes.decode("utf-8", errors="replace").replace("\x00", " ").strip()
                except (FileNotFoundError, ProcessLookupError, PermissionError):
                    pass

            # 4. Inspect file descriptor table for pipe/socket I/O
            fd_dir = proc_dir / "fd"
            if fd_dir.is_dir():
                try:
                    latest_io = 0.0
                    for fd_entry in fd_dir.iterdir():
                        try:
                            link = os.readlink(fd_entry)
                            if link.startswith("pipe:") or link.startswith("socket:"):
                                st = fd_entry.stat()
                                if st.st_mtime > latest_io:
                                    latest_io = st.st_mtime
                        except (OSError, FileNotFoundError, ProcessLookupError):
                            continue
                    if latest_io > 0.0:
                        last_pipe_io = latest_io
                except (FileNotFoundError, ProcessLookupError, PermissionError):
                    pass

            return ProcessSample(
                pid=pid,
                timestamp=sample_time,
                exists=True,
                state=state,
                utime=utime,
                stime=stime,
                total_cpu_time=utime + stime,
                rss_bytes=rss_bytes,
                num_threads=num_threads,
                cmdline=cmdline,
                last_pipe_io_timestamp=last_pipe_io,
            )

        # Fallback probe for non-Linux or environments without /proc
        exists = False
        state = "?"
        try:
            os.kill(pid, 0)
            exists = True
            state = "R"
            try:
                ps_output = subprocess.check_output(
                    ["ps", "-p", str(pid), "-o", "state=,time=,rss=,args="],
                    text=True,
                    stderr=subprocess.DEVNULL,
                ).strip()
                if ps_output:
                    parts = ps_output.split(None, 3)
                    if len(parts) >= 1:
                        state = parts[0][0]
                    rss_kb = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else 0
                    cmd = parts[3] if len(parts) >= 4 else ""
                    return ProcessSample(
                        pid=pid,
                        timestamp=sample_time,
                        exists=True,
                        state=state,
                        utime=0.0,
                        stime=0.0,
                        total_cpu_time=0.0,
                        rss_bytes=rss_kb * 1024,
                        num_threads=1,
                        cmdline=cmd,
                        last_pipe_io_timestamp=sample_time,
                    )
            except Exception:
                pass
        except ProcessLookupError:
            exists = False
        except PermissionError:
            exists = True
            state = "R"

        return ProcessSample(
            pid=pid,
            timestamp=sample_time,
            exists=exists,
            state=state,
            utime=0.0,
            stime=0.0,
            total_cpu_time=0.0,
            rss_bytes=0,
            num_threads=0,
            cmdline="",
            last_pipe_io_timestamp=sample_time,
        )

    def compute_inactivity_vector(
        self,
        lease: WorkerHeartbeatLease,
        current_sample: ProcessSample,
        now: float,
    ) -> Tuple[float, float, float, Dict[str, Any]]:
        """
        Computes CPU progression and inactivity score:
        Delta CPU = (utime(t) + stime(t)) - (utime(t - Delta t) + stime(t - Delta t))
        I(w_i) = w_{cpu} * 1_{Delta CPU == 0} + w_{hb} * 1_{Delta t > tau_{ttl}} + w_{zombie} * 1_{state == Z}
        """
        prev_sample = lease.previous_sample or lease.last_sample
        if prev_sample and prev_sample.exists and current_sample.exists:
            delta_cpu = max(0.0, current_sample.total_cpu_time - prev_sample.total_cpu_time)
        else:
            delta_cpu = 0.0

        delta_t_hb = now - lease.t_heartbeat

        is_cpu_frozen = 1.0 if delta_cpu <= 1e-4 else 0.0
        is_hb_expired = 1.0 if delta_t_hb > lease.tau_ttl else 0.0
        is_zombie = 1.0 if current_sample.state == "Z" else 0.0

        inactivity_score = (
            (self.w_cpu * is_cpu_frozen)
            + (self.w_hb * is_hb_expired)
            + (self.w_zombie * is_zombie)
        )

        diagnostics = {
            "delta_cpu": delta_cpu,
            "delta_t_hb": delta_t_hb,
            "cpu_frozen": bool(is_cpu_frozen),
            "hb_expired": bool(is_hb_expired),
            "is_zombie": bool(is_zombie),
            "inactivity_score": inactivity_score,
        }
        return inactivity_score, delta_cpu, delta_t_hb, diagnostics

    def scan_fleet(self) -> List[EvictionIncident]:
        """
        Scans all registered workers, probes OS metrics, assesses lease validity,
        detects catatonic stalls, and triggers staged eviction when required.
        """
        with self.lock:
            now = time.time()
            incidents_created: List[EvictionIncident] = []

            for worker_id, lease in list(self._workers.items()):
                # If already fully reaped/reclaimed, skip active evaluation
                if lease.health_state in (WorkerHealthState.TERMINATED, WorkerHealthState.RECLAIMED):
                    continue

                sample = self.probe_process(lease.pid)
                lease.previous_sample = lease.last_sample
                lease.last_sample = sample

                # 1. Process termination check
                if not sample.exists:
                    lease.health_state = WorkerHealthState.TERMINATED
                    lease.eviction_stage = EvictionStage.REAPED
                    reclaimed = self.reclaim_worker_resources(lease)
                    lease.health_state = WorkerHealthState.RECLAIMED
                    lease.eviction_stage = EvictionStage.RECLAIMED
                    incident = EvictionIncident(
                        incident_id=f"inc-{uuid.uuid4().hex[:8]}",
                        worker_id=worker_id,
                        pid=lease.pid,
                        repo_id=lease.repo_id,
                        milestone_id=lease.milestone_id,
                        reason="PROCESS_DISAPPEARED_ORPHAN",
                        timestamp=now,
                        health_state=WorkerHealthState.TERMINATED,
                        eviction_stage=EvictionStage.RECLAIMED,
                        signal_sent=EvictionSignal.NONE,
                        reclaimed_resources=reclaimed,
                    )
                    self._incidents.append(incident)
                    incidents_created.append(incident)
                    continue

                # 2. Check for Zombie state
                if sample.state == "Z":
                    lease.health_state = WorkerHealthState.ZOMBIE
                    lease.eviction_stage = EvictionStage.STALLED
                    logger.warning(f"Worker '{worker_id}' (PID {lease.pid}) is in ZOMBIE state. Reaping...")
                    _try_reap_process(lease.pid)
                    self.evict_worker(worker_id, force_kill=True, reason="ZOMBIE_PROCESS_REAP")
                    continue

                # 3. Compute Inactivity Vector and Score
                inactivity_score, delta_cpu, delta_t_hb, _ = self.compute_inactivity_vector(
                    lease, sample, now
                )

                # 4. Evaluate Catatonic Stall Timer
                if inactivity_score >= 1.0:
                    if lease.catatonic_since is None:
                        lease.catatonic_since = now
                    catatonic_duration = now - lease.catatonic_since

                    if catatonic_duration >= self.catatonic_threshold_sec:
                        lease.health_state = WorkerHealthState.CATATONIC_STALL
                        lease.eviction_stage = EvictionStage.STALLED
                        logger.error(
                            f"Worker '{worker_id}' reached CATATONIC_STALL (score={inactivity_score:.2f}, "
                            f"catatonic_duration={catatonic_duration:.1f}s > {self.catatonic_threshold_sec}s). Evicting..."
                        )
                        self.evict_worker(
                            worker_id,
                            force_kill=False,
                            synchronous=True,
                            grace_sec=15.0,
                            reason="CATATONIC_WORKER_EVICTION",
                        )
                        continue
                else:
                    # Inactivity resolved or intermittent burst detected
                    lease.catatonic_since = None

                # 5. Heartbeat Lease Expiration classification
                if delta_t_hb > lease.tau_ttl:
                    lease.health_state = WorkerHealthState.LEASE_EXPIRED
                    logger.warning(
                        f"Worker '{worker_id}' lease expired (Delta t={delta_t_hb:.1f}s > tau_ttl={lease.tau_ttl}s)"
                    )
                elif delta_t_hb > (lease.tau_ttl * 0.75):
                    lease.health_state = WorkerHealthState.HEARTBEAT_LATE
                else:
                    if lease.health_state not in (WorkerHealthState.EVICTING,):
                        lease.health_state = WorkerHealthState.HEALTHY

            self.export_snapshot()
            return incidents_created

    def evict_worker(
        self,
        worker_id: str,
        force_kill: bool = False,
        synchronous: bool = True,
        grace_sec: float = 15.0,
        reason: str = "CATATONIC_WORKER_EVICTION",
    ) -> bool:
        """
        Executes staged eviction state machine Sigma_evict:
        Step 1: Send SIGTERM to process group. Wait grace_sec (default 15s).
        Step 2: If process remains, escalate to SIGKILL.
        Step 3: Reap child exit code via waitpid(p, WNOHANG).
        Step 4: Cascading Resource Reclamation and Post-Mortem emission.
        """
        with self.lock:
            lease = self._workers.get(worker_id)
            if not lease:
                logger.error(f"Cannot evict unknown worker '{worker_id}'")
                return False

            now = time.time()
            logger.info(
                f"Starting staged eviction for worker '{worker_id}' (PID {lease.pid}, reason='{reason}', force_kill={force_kill})"
            )

            incident = EvictionIncident(
                incident_id=f"inc-{uuid.uuid4().hex[:8]}",
                worker_id=worker_id,
                pid=lease.pid,
                repo_id=lease.repo_id,
                milestone_id=lease.milestone_id,
                reason=reason,
                timestamp=now,
                health_state=WorkerHealthState.EVICTING,
                eviction_stage=EvictionStage.ACTIVE,
                signal_sent=EvictionSignal.NONE,
            )

            lease.health_state = WorkerHealthState.EVICTING
            lease.eviction_initiated_at = now

            # Step 1: Send SIGTERM or escalate immediately if force_kill
            if force_kill:
                logger.info(f"Escalating directly to SIGKILL for PID {lease.pid}")
                _safe_kill_process_group(lease.pid, signal.SIGKILL)
                lease.eviction_signal_sent = EvictionSignal.SIGKILL
                lease.eviction_stage = EvictionStage.SIGKILL_ESCALATED
                incident.signal_sent = EvictionSignal.SIGKILL
            else:
                logger.info(f"Delivering SIGTERM to process group for PID {lease.pid}")
                _safe_kill_process_group(lease.pid, signal.SIGTERM)
                lease.eviction_signal_sent = EvictionSignal.SIGTERM
                lease.eviction_stage = EvictionStage.SIGTERM_SENT
                incident.signal_sent = EvictionSignal.SIGTERM

            # Handle execution timing
            if synchronous:
                deadline = now + grace_sec
                reaped = False

                # Grace period monitoring loop
                while time.time() < deadline:
                    if not self.probe_process(lease.pid).exists:
                        reaped = True
                        break
                    time.sleep(0.2)

                # Step 2: Escalate to SIGKILL if still alive after grace period
                if not reaped and not force_kill:
                    logger.warning(
                        f"PID {lease.pid} remained active after {grace_sec}s grace. Escalating to SIGKILL..."
                    )
                    _safe_kill_process_group(lease.pid, signal.SIGKILL)
                    lease.eviction_signal_sent = EvictionSignal.SIGKILL
                    lease.eviction_stage = EvictionStage.SIGKILL_ESCALATED
                    incident.signal_sent = EvictionSignal.SIGKILL

                    # Final brief wait for SIGKILL to settle
                    kill_deadline = time.time() + 3.0
                    while time.time() < kill_deadline:
                        if not self.probe_process(lease.pid).exists:
                            break
                        time.sleep(0.1)

                # Step 3: Reap child exit status
                _try_reap_process(lease.pid)
                lease.eviction_stage = EvictionStage.REAPED
                lease.health_state = WorkerHealthState.TERMINATED

            # Step 4: Cascading Resource Reclamation
            reclaimed = self.reclaim_worker_resources(lease, incident)
            incident.reclaimed_resources = reclaimed
            incident.health_state = WorkerHealthState.RECLAIMED
            incident.eviction_stage = EvictionStage.RECLAIMED

            lease.health_state = WorkerHealthState.RECLAIMED
            lease.eviction_stage = EvictionStage.RECLAIMED

            self._incidents.append(incident)
            self.export_snapshot()
            logger.info(
                f"Successfully completed staged eviction for worker '{worker_id}'. Reclaimed: {reclaimed}"
            )
            return True

    def reclaim_worker_resources(
        self,
        lease: WorkerHeartbeatLease,
        incident: Optional[EvictionIncident] = None,
    ) -> List[str]:
        """
        Executes cascading autonomous resource reclamation hooks across
        subsystems (quota leases, concurrency fencing tokens, sandboxes).
        """
        reclaimed: List[str] = []

        # 1. Release Quota Lease
        if lease.quota_lease_id:
            try:
                # Dynamic integration with tasks/pipeline_quota_reservation_lease.py
                from tasks.pipeline_quota_reservation_lease import (
                    QuotaReservationLeaseController,
                )

                if hasattr(QuotaReservationLeaseController, "release_lease"):
                    QuotaReservationLeaseController.release_lease(lease.quota_lease_id)
                elif hasattr(QuotaReservationLeaseController, "get_instance"):
                    QuotaReservationLeaseController.get_instance().release_lease(
                        lease.quota_lease_id
                    )
                reclaimed.append(f"quota_lease:{lease.quota_lease_id}")
                logger.info(f"Reclaimed quota lease '{lease.quota_lease_id}'")
            except ImportError:
                reclaimed.append(f"quota_lease_mock_revoked:{lease.quota_lease_id}")
            except Exception as e:
                logger.warning(f"Error releasing quota lease '{lease.quota_lease_id}': {e}")
                reclaimed.append(f"quota_lease_error:{lease.quota_lease_id}")

        # 2. Revoke Concurrency Fencing Token
        if lease.fencing_token or lease.worker_id:
            token_ref = lease.fencing_token or lease.worker_id
            try:
                # Dynamic integration with tasks/pipeline_concurrency_deconfliction.py
                from tasks.pipeline_concurrency_deconfliction import (
                    DistributedFencingCoordinator,
                )

                if hasattr(DistributedFencingCoordinator, "revoke_lease"):
                    DistributedFencingCoordinator.revoke_lease(lease.worker_id)
                elif hasattr(DistributedFencingCoordinator, "get_instance"):
                    DistributedFencingCoordinator.get_instance().revoke_lease(lease.worker_id)
                reclaimed.append(f"fencing_token:{token_ref}")
                logger.info(f"Revoked concurrency fencing lease '{token_ref}'")
            except ImportError:
                reclaimed.append(f"fencing_token_mock_revoked:{token_ref}")
            except Exception as e:
                logger.warning(f"Error revoking fencing token '{token_ref}': {e}")
                reclaimed.append(f"fencing_token_error:{token_ref}")

        # 3. Clean Worktree Sandbox
        if lease.worktree_path:
            try:
                # Dynamic integration with tasks/worktree_sandbox_manager.py
                from tasks.worktree_sandbox_manager import WorktreeSandboxManager

                if hasattr(WorktreeSandboxManager, "clean_sandbox"):
                    WorktreeSandboxManager.clean_sandbox(lease.worktree_path)
                elif hasattr(WorktreeSandboxManager, "get_instance"):
                    WorktreeSandboxManager.get_instance().clean_sandbox(lease.worktree_path)
                reclaimed.append(f"worktree_sandbox:{lease.worktree_path}")
                logger.info(f"Cleaned worktree sandbox '{lease.worktree_path}'")
            except ImportError:
                # Local fallback pruning
                path = Path(lease.worktree_path)
                if path.exists() and path.is_dir():
                    import shutil

                    shutil.rmtree(path, ignore_errors=True)
                reclaimed.append(f"worktree_sandbox_pruned:{lease.worktree_path}")
            except Exception as e:
                logger.warning(f"Error cleaning worktree sandbox '{lease.worktree_path}': {e}")
                reclaimed.append(f"worktree_sandbox_error:{lease.worktree_path}")

        # 4. Emit Post-Mortem Incident Pedagogy
        self._emit_post_mortem_pedagogy(lease, incident, reclaimed)

        return reclaimed

    def _emit_post_mortem_pedagogy(
        self,
        lease: WorkerHeartbeatLease,
        incident: Optional[EvictionIncident],
        reclaimed: List[str],
    ) -> None:
        """
        Emits structured post-mortem telemetry to dashboards/post_mortem_pedagogy.py
        and appends an immutable JSON record to dashboards/post_mortem_incidents.jsonl.
        """
        incident_reason = incident.reason if incident else "CATATONIC_WORKER_EVICTION"
        payload = {
            "timestamp": time.time(),
            "event": "CATATONIC_WORKER_EVICTION",
            "worker_id": lease.worker_id,
            "pid": lease.pid,
            "repo_id": lease.repo_id,
            "milestone_id": lease.milestone_id,
            "reason": incident_reason,
            "reclaimed_resources": reclaimed,
            "tau_ttl": lease.tau_ttl,
            "last_sample": lease.last_sample.to_dict() if lease.last_sample else None,
        }

        # Try dynamic import of post_mortem_pedagogy
        try:
            from dashboards.post_mortem_pedagogy import emit_post_mortem

            emit_post_mortem(payload)
            if incident:
                incident.post_mortem_emitted = True
        except (ImportError, AttributeError):
            pass
        except Exception as e:
            logger.debug(f"Notice during post_mortem_pedagogy invocation: {e}")

        # Always persist to structured audit ledger
        try:
            audit_file = self.post_mortem_dir / "post_mortem_incidents.jsonl"
            with open(audit_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload) + "\n")
            if incident:
                incident.post_mortem_emitted = True
        except Exception as e:
            logger.warning(f"Failed to append to post_mortem_incidents.jsonl: {e}")

    def export_snapshot(self) -> FleetWatchdogSnapshot:
        """
        Constructs and atomically writes fleet snapshot to dashboards/fleet_watchdog_state.json.
        """
        with self.lock:
            now = time.time()
            workers_list = [w.to_dict() for w in self._workers.values()]
            incidents_list = [inc.to_dict() for inc in self._incidents[-50:]]

            healthy_cnt = sum(
                1 for w in self._workers.values() if w.health_state == WorkerHealthState.HEALTHY
            )
            stalled_cnt = sum(
                1
                for w in self._workers.values()
                if w.health_state
                in (
                    WorkerHealthState.CATATONIC_STALL,
                    WorkerHealthState.LEASE_EXPIRED,
                    WorkerHealthState.ZOMBIE,
                )
            )
            evicted_cnt = sum(
                1
                for w in self._workers.values()
                if w.health_state
                in (WorkerHealthState.TERMINATED, WorkerHealthState.RECLAIMED)
            )

            snapshot = FleetWatchdogSnapshot(
                snapshot_id=f"snap-{uuid.uuid4().hex[:8]}",
                timestamp=now,
                active_workers_count=len(self._workers),
                healthy_count=healthy_cnt,
                stalled_count=stalled_cnt,
                evicted_count=evicted_cnt,
                workers=workers_list,
                recent_incidents=incidents_list,
            )

            try:
                temp_file = self.state_file.with_suffix(".tmp")
                with open(temp_file, "w", encoding="utf-8") as f:
                    json.dump(snapshot.to_dict(), f, indent=2)
                os.replace(temp_file, self.state_file)
            except Exception as e:
                logger.error(f"Failed to persist atomic snapshot to {self.state_file}: {e}")

            return snapshot

    def get_worker(self, worker_id: str) -> Optional[WorkerHeartbeatLease]:
        with self.lock:
            return self._workers.get(worker_id)

    def unregister_worker(self, worker_id: str) -> bool:
        with self.lock:
            if worker_id in self._workers:
                del self._workers[worker_id]
                self.export_snapshot()
                return True
            return False

    def start_background_scanner(self, interval_sec: float = 5.0) -> None:
        """Starts asynchronous watchdog polling thread."""
        with self.lock:
            if self._background_thread and self._background_thread.is_alive():
                return
            self._stop_event.clear()

            def _run():
                while not self._stop_event.is_set():
                    try:
                        self.scan_fleet()
                    except Exception as err:
                        logger.error(f"Unhandled error in fleet scanner thread: {err}")
                    self._stop_event.wait(interval_sec)

            self._background_thread = threading.Thread(
                target=_run, name="FleetWatchdogScanner", daemon=True
            )
            self._background_thread.start()
            logger.info(f"FleetWatchdog background scanner started (interval={interval_sec}s)")

    def stop_background_scanner(self) -> None:
        """Stops asynchronous watchdog polling thread."""
        with self.lock:
            self._stop_event.set()
            if self._background_thread:
                self._background_thread.join(timeout=3.0)
                self._background_thread = None
                logger.info("FleetWatchdog background scanner stopped")


# ---------------------------------------------------------------------------
# Comprehensive Self-Testing Verification Suite
# ---------------------------------------------------------------------------
def _run_self_tests() -> None:
    """
    Executes automated test harness verifying:
    (a) Registration and normal heartbeat renewal
    (b) Lease expiration on missed heartbeat
    (c) Simulated stalled child process eviction and signal escalation
    (d) Atomic reclamation ledger persistence
    """
    logger.info(">>> Launching Pipeline Fleet Watchdog Self-Testing Suite <<<")

    test_dir = Path("/tmp/fleet_watchdog_test")
    test_dir.mkdir(parents=True, exist_ok=True)
    state_file = test_dir / "fleet_watchdog_state.json"
    post_mortem_dir = test_dir

    watchdog = FleetWatchdog(
        state_file=state_file,
        post_mortem_dir=post_mortem_dir,
        catatonic_threshold_sec=0.5,
        default_ttl_sec=0.4,
    )

    # -----------------------------------------------------------------------
    # Test (a): Registration and Normal Heartbeat Renewal
    # -----------------------------------------------------------------------
    logger.info("=== Test (a): Worker Registration & Heartbeat Renewal ===")
    current_pid = os.getpid()
    lease_a = watchdog.register_worker(
        worker_id="worker-node-alpha",
        pid=current_pid,
        repo_id="hermes_orchestrator",
        milestone_id="M-WATCHDOG-001",
        quota_lease_id="quota-lease-xyz",
        fencing_token="fence-token-123",
        ttl_sec=1.0,
    )
    assert lease_a is not None, "Failed to register worker-node-alpha"
    assert lease_a.health_state == WorkerHealthState.HEALTHY
    initial_hb = lease_a.t_heartbeat

    time.sleep(0.1)
    hb_ok = watchdog.heartbeat("worker-node-alpha")
    assert hb_ok, "Heartbeat renewal failed"
    lease_updated = watchdog.get_worker("worker-node-alpha")
    assert lease_updated.t_heartbeat > initial_hb, "Heartbeat timestamp was not incremented"
    logger.info("✓ Test (a) PASSED: Worker registration and heartbeat renewal verified.")

    # -----------------------------------------------------------------------
    # Test (b): Lease Expiration on Missed Heartbeat
    # -----------------------------------------------------------------------
    logger.info("=== Test (b): Lease Expiration on Missed Heartbeat ===")
    watchdog.register_worker(
        worker_id="worker-node-beta",
        pid=current_pid,
        repo_id="hermes_orchestrator",
        milestone_id="M-WATCHDOG-002",
        ttl_sec=0.2,
    )
    # Wait past TTL without sending heartbeat
    time.sleep(0.35)
    watchdog.scan_fleet()
    lease_b = watchdog.get_worker("worker-node-beta")
    assert lease_b.health_state == WorkerHealthState.LEASE_EXPIRED, (
        f"Expected LEASE_EXPIRED, got {lease_b.health_state}"
    )
    logger.info("✓ Test (b) PASSED: Lease classified as LEASE_EXPIRED upon missed heartbeat.")

    # -----------------------------------------------------------------------
    # Test (c): Simulated Stalled Child Process Eviction & Signal Escalation
    # -----------------------------------------------------------------------
    logger.info("=== Test (c): Staged Child Process Eviction & Signal Escalation ===")
    # Spawn child that ignores SIGTERM to force escalation to SIGKILL
    stalled_code = (
        "import signal, time; "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        "time.sleep(60)"
    )
    child_proc = subprocess.Popen(
        [sys.executable, "-c", stalled_code],
        start_new_session=True,
    )
    child_pid = child_proc.pid
    logger.info(f"Spawned test child process (PID {child_pid}) ignoring SIGTERM")

    sandbox_dummy = test_dir / f"sandbox_{child_pid}"
    sandbox_dummy.mkdir(parents=True, exist_ok=True)

    watchdog.register_worker(
        worker_id="worker-node-catatonic",
        pid=child_pid,
        repo_id="hermes_orchestrator",
        milestone_id="M-WATCHDOG-003",
        quota_lease_id="quota-lease-catatonic",
        fencing_token="fence-catatonic",
        ttl_sec=0.3,
        worktree_path=str(sandbox_dummy),
    )

    # Perform synchronous staged eviction with short grace period
    eviction_ok = watchdog.evict_worker(
        worker_id="worker-node-catatonic",
        force_kill=False,
        synchronous=True,
        grace_sec=0.5,
        reason="CATATONIC_WORKER_EVICTION",
    )
    assert eviction_ok, "Eviction sequence returned failure"

    # Confirm child process was terminated and reaped
    time.sleep(0.2)
    sample_after = watchdog.probe_process(child_pid)
    assert not sample_after.exists, f"Child PID {child_pid} still running after eviction"

    lease_c = watchdog.get_worker("worker-node-catatonic")
    assert lease_c.health_state == WorkerHealthState.RECLAIMED, (
        f"Expected RECLAIMED, got {lease_c.health_state}"
    )
    assert lease_c.eviction_signal_sent == EvictionSignal.SIGKILL, (
        "Expected escalation to SIGKILL since child ignored SIGTERM"
    )
    logger.info("✓ Test (c) PASSED: Staged eviction and SIGKILL escalation executed cleanly.")

    # -----------------------------------------------------------------------
    # Test (d): Atomic Reclamation Ledger Persistence
    # -----------------------------------------------------------------------
    logger.info("=== Test (d): Atomic Ledger Persistence Verification ===")
    assert state_file.exists(), f"State file {state_file} does not exist"
    with open(state_file, "r", encoding="utf-8") as f:
        snapshot_data = json.load(f)

    assert "snapshot_id" in snapshot_data
    assert "active_workers_count" in snapshot_data
    assert "recent_incidents" in snapshot_data
    assert len(snapshot_data["recent_incidents"]) >= 1, "Incident was not recorded in ledger"

    incident_entry = snapshot_data["recent_incidents"][-1]
    assert incident_entry["reason"] == "CATATONIC_WORKER_EVICTION"
    assert "quota_lease_catatonic" in str(incident_entry["reclaimed_resources"])

    audit_file = post_mortem_dir / "post_mortem_incidents.jsonl"
    assert audit_file.exists(), "Post-mortem audit log file was not generated"

    # Clean up test artifacts
    import shutil

    shutil.rmtree(test_dir, ignore_errors=True)
    logger.info("✓ Test (d) PASSED: State snapshot and incident post-mortem ledgers validated.")

    logger.info(">>> All Pipeline Fleet Watchdog Self-Tests Completed Successfully (4/4). <<<")


if __name__ == "__main__":
    _run_self_tests()
    sys.exit(0)