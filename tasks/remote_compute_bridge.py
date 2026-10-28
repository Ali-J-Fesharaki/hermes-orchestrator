#!/usr/bin/env python3
"""
remote_compute_bridge.py: Remote University Workstation Simulation & Execution Bridge for Hermes Agents.

Allows Hermes Agents to:
  1. Auto-Wake: Automatically detect if the remote lab workstation is asleep and send Wake-on-LAN (via aut-wol).
  2. Sync Workspace: Fast rsync code from local repo to remote university workstation over campus intranet.
  3. Execute Remotely: Run Gazebo, ROS 2, DRL training, or pytest on the lab machine via SSH.
  4. Fetch Artifacts: Pull back simulation logs, generated worlds, or metrics into local workspace.
"""

from __future__ import annotations

import os
import sys
import json
import time
import socket
import subprocess
from pathlib import Path

CONFIG_PATH = Path.home() / ".config/campus_node.json"

def load_node_config() -> dict:
    if not CONFIG_PATH.exists():
        return {
            "host": "",
            "user": "ajf",
            "port": 22,
            "mac": "",
            "ssh_key": str(Path.home() / ".ssh/id_ed25519"),
            "remote_workspace": "~/remote_simulation_workspace",
            "auto_wake": True
        }
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def is_port_open(host: str, port: int = 22, timeout: float = 1.5) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            s.connect((host, port))
            return True
    except Exception:
        return False

def ensure_workstation_awake(cfg: dict, wait_seconds: int = 35) -> bool:
    """Checks SSH accessibility. If unreachable and MAC is provided, fires aut-wol and waits."""
    host = cfg.get("host", "").strip()
    if not host:
        return False

    if is_port_open(host, cfg.get("port", 22), timeout=1.0):
        return True

    mac = cfg.get("mac", "").strip()
    if not mac or not cfg.get("auto_wake", True):
        return False

    print(f"[*] University workstation at {host} is offline. Transmitting Wake-on-LAN magic packet to {mac}...")
    try:
        subprocess.run(["aut-wol", mac, "-i", host], check=False)
    except Exception as e:
        print(f"[-] Could not invoke aut-wol: {e}")

    print(f"[*] Waiting up to {wait_seconds}s for workstation to boot...")
    t0 = time.time()
    while time.time() - t0 < wait_seconds:
        if is_port_open(host, cfg.get("port", 22), timeout=1.5):
            print(f"[✓] Workstation at {host} is awake and accepting SSH connections!")
            time.sleep(2)  # Allow sshd to stabilize
            return True
        time.sleep(2)

    return False

def run_remote_simulation(
    local_dir: str | Path,
    command: str,
    target_subfolder: str = "simulation_job",
    fetch_patterns: list[str] | None = None,
    timeout_s: int = 600
) -> dict:
    """
    Primary API for Hermes Agents:
    1. Ensures remote workstation is awake.
    2. Synchronizes `local_dir` to remote `remote_workspace / target_subfolder`.
    3. Runs `command` inside the remote workspace directory.
    4. Fetches back any output files matching `fetch_patterns`.
    """
    cfg = load_node_config()
    host = cfg.get("host", "").strip()
    user = cfg.get("user", "ajf").strip()
    port = cfg.get("port", 22)
    key = cfg.get("ssh_key", "")
    base_remote = cfg.get("remote_workspace", "~/remote_simulation_workspace")

    if not host:
        return {
            "status": "CONFIG_ERROR",
            "error": "University workstation IP is not configured in ~/.config/campus_node.json. Please set 'host' and 'user'."
        }

    local_path = Path(local_dir).resolve()
    if not local_path.exists():
        return {"status": "ERROR", "error": f"Local workspace directory '{local_dir}' does not exist."}

    # 1. Wake / Health check
    awake = ensure_workstation_awake(cfg)
    if not awake and not is_port_open(host, port, timeout=2.0):
        return {
            "status": "UNREACHABLE",
            "error": f"Workstation at {host}:{port} is unreachable over campus network. Verify target IP or power state."
        }

    remote_dest_dir = f"{base_remote}/{target_subfolder}"
    ssh_opts = ["-p", str(port), "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=5"]
    if key and Path(key).exists():
        ssh_opts.extend(["-i", key])

    # 2. Sync to remote
    print(f"[*] Synchronizing local workspace '{local_path.name}' to {user}@{host}:{remote_dest_dir}...")
    try:
        # Create remote directory
        mkdir_cmd = ["ssh"] + ssh_opts + [f"{user}@{host}", f"mkdir -p {remote_dest_dir}"]
        subprocess.run(mkdir_cmd, check=True, capture_output=True, timeout=10)

        # Rsync files
        rsync_ssh = f"ssh -p {port}" + (f" -i {key}" if key else "")
        rsync_cmd = [
            "rsync", "-avz", "--delete",
            "--exclude", ".git", "--exclude", "__pycache__", "--exclude", "*.tmp",
            "-e", rsync_ssh,
            f"{local_path}/",
            f"{user}@{host}:{remote_dest_dir}/"
        ]
        sync_res = subprocess.run(rsync_cmd, capture_output=True, text=True, timeout=30)
        if sync_res.returncode != 0:
            return {"status": "SYNC_FAILED", "error": sync_res.stderr}
    except Exception as se:
        return {"status": "SYNC_ERROR", "error": str(se)}

    # 3. Execute remote command
    print(f"[*] Executing remote simulation command: '{command}'...")
    full_remote_cmd = f"cd {remote_dest_dir} && {command}"
    exec_cmd = ["ssh"] + ssh_opts + [f"{user}@{host}", full_remote_cmd]

    t0 = time.time()
    try:
        res = subprocess.run(exec_cmd, capture_output=True, text=True, timeout=timeout_s)
        elapsed = round(time.time() - t0, 1)

        # 4. Fetch results if patterns requested
        fetched_files = []
        if fetch_patterns:
            for pat in fetch_patterns:
                fetch_cmd = [
                    "rsync", "-avz",
                    "-e", rsync_ssh,
                    f"{user}@{host}:{remote_dest_dir}/{pat}",
                    f"{local_path}/"
                ]
                subprocess.run(fetch_cmd, capture_output=True, timeout=30)
                fetched_files.append(pat)

        return {
            "status": "SUCCESS" if res.returncode == 0 else "EXECUTION_FAILED",
            "exit_code": res.returncode,
            "stdout": res.stdout,
            "stderr": res.stderr,
            "elapsed_seconds": elapsed,
            "remote_host": host,
            "fetched_artifacts": fetched_files
        }
    except subprocess.TimeoutExpired:
        return {
            "status": "TIMEOUT",
            "error": f"Remote execution timed out after {timeout_s} seconds."
        }
    except Exception as ee:
        return {
            "status": "ERROR",
            "error": str(ee)
        }

if __name__ == "__main__":
    cfg = load_node_config()
    print("Remote Compute Node Configuration:")
    print(json.dumps(cfg, indent=2))
    if cfg.get("host"):
        print(f"Host {cfg['host']} reachable: {is_port_open(cfg['host'])}")
    else:
        print("Note: Set 'host' in ~/.config/campus_node.json to connect to your university PC.")
