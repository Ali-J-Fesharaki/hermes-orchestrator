#!/usr/bin/env python3
"""
Task Runner for Research & Extensible Workflows
"""

import os
import sys
import json
import sqlite3
import argparse
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
STATE_FILE = Path.home() / "PhD_Proposal/DASHBOARD_STATE.md"
DB_PATH = Path.home() / ".antigravity-agent/cloud_accounts.db"

def check_accounts():
    if not DB_PATH.exists():
        print(f"[-] Account database not found at {DB_PATH}")
        return []
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT id, email, status, last_used FROM accounts")
    rows = cur.fetchall()
    conn.close()
    return rows

def status():
    print("=" * 60)
    print(" Hermes Automation & Multi-Task Scheduler Status")
    print("=" * 60)
    
    # Check LLM Router / Antigravity Gateway
    import subprocess
    svc_check = subprocess.run(["systemctl", "--user", "is-active", "antigravity-manager.service"], capture_output=True, text=True)
    svc_status = svc_check.stdout.strip()
    print(f"[*] Antigravity Manager Service: {svc_status.upper()}")
    
    # Accounts
    accounts = check_accounts()
    print(f"[*] Available Rotation Accounts: {len(accounts)}")
    for acc in accounts:
        print(f"    - ID {acc[0]}: {acc[1]} ({acc[2]}) | Last Used: {acc[3]}")
        
    # Tasks available
    tasks_dir = BASE_DIR / "tasks"
    custom_tasks = list(tasks_dir.glob("*.py")) + list(tasks_dir.glob("*.sh"))
    print(f"\n[*] Registered Custom Tasks in {tasks_dir}:")
    if custom_tasks:
        for t in custom_tasks:
            print(f"    - {t.name}")
    else:
        print("    (None yet. You can add market tracking, backups, etc.)")
    print("=" * 60)

def run_research():
    print("[+] Starting PhD Research & Proposal Automation Pipeline...")
    print(f"    Target Proposal Root: ~/PhD_Proposal")
    print(f"    LaTeX Upstream Rule: ~/PhD_Proposal/proposal/prompt_for_latex_persian_AI_generation/PROMPT_v2.8.md")
    
    # Check if dashboard exists
    if STATE_FILE.exists():
        print(f"[+] Loaded Dashboard State: {STATE_FILE}")
    else:
        print(f"[!] Warning: Dashboard State not found at {STATE_FILE}")
    print("[+] Ready for chunk generation. Dispatching sub-tasks according to quota schedule.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Multi-Task Automated Scheduler")
    parser.add_argument("command", choices=["status", "research", "run"], nargs="?", default="status")
    parser.add_argument("--task", help="Name of custom task inside tasks/")
    args = parser.parse_args()

    if args.command == "status":
        status()
    elif args.command == "research":
        run_research()
    elif args.command == "run":
        if not args.task:
            print("[-] Specify --task <script_name>")
            sys.exit(1)
        task_path = BASE_DIR / "tasks" / args.task
        if not task_path.exists():
            print(f"[-] Task file {task_path} does not exist.")
            sys.exit(1)
        import subprocess
        print(f"[+] Executing custom task: {task_path}")
        subprocess.run([str(task_path)] if task_path.suffix == ".sh" else ["python3", str(task_path)])
