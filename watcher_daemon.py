#!/usr/bin/env python3
"""
watcher_daemon.py: Background monitoring loop that waits for high-level goals
either from DASHBOARD_STATE.md or external events, commits milestones to git,
and transmits updates via Telegram.
"""

import os
import sys
import time
import subprocess
from datetime import datetime
from pathlib import Path

REPO_DIR = Path.home() / "PhD_Proposal"
STATE_FILE = REPO_DIR / "DASHBOARD_STATE.md"
BASE_DIR = Path(__file__).resolve().parent
NOTIFIER = BASE_DIR / "telegram_notifier.py"

def notify_telegram(msg: str):
    if NOTIFIER.exists():
        subprocess.run(["python3", str(NOTIFIER), msg], capture_output=True)

def commit_milestone(branch_name: str, message: str, files_to_add=None):
    try:
        # Check current branch
        curr = subprocess.run(["git", "branch", "--show-current"], cwd=REPO_DIR, capture_output=True, text=True).stdout.strip()
        if curr != branch_name:
            # Switch or create
            subprocess.run(["git", "checkout", "-B", branch_name], cwd=REPO_DIR, check=True)
            
        if files_to_add:
            for f in files_to_add:
                subprocess.run(["git", "add", f], cwd=REPO_DIR, check=True)
        else:
            subprocess.run(["git", "add", "-A"], cwd=REPO_DIR, check=True)
            
        res = subprocess.run(["git", "commit", "-m", message], cwd=REPO_DIR, capture_output=True, text=True)
        if "nothing to commit" not in res.stdout:
            print(f"[+] Committed: {message}")
            notify_telegram(f"📌 *Git Milestone Recorded*\n• *Branch:* `{branch_name}`\n• *Commit:* {message}")
            return True
    except Exception as e:
        print(f"[-] Git commit failed: {e}")
    return False

def check_pending_goals():
    if not STATE_FILE.exists():
        return
    content = STATE_FILE.read_text()
    # Read sections marked with [ ] in Immediate Action Plan
    lines = content.splitlines()
    pending = []
    for line in lines:
        if line.strip().startswith("- [ ]") or "Pending Refinement" in line:
            pending.append(line.strip())
    return pending

def run_loop(poll_interval_sec=30):
    print(f"[*] Hermes Continuous Watcher Daemon started. Polling every {poll_interval_sec}s...")
    last_mtime = STATE_FILE.stat().st_mtime if STATE_FILE.exists() else 0

    while True:
        try:
            if STATE_FILE.exists():
                curr_mtime = STATE_FILE.stat().st_mtime
                if curr_mtime > last_mtime:
                    last_mtime = curr_mtime
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Detected update in DASHBOARD_STATE.md!")
                    pending = check_pending_goals()
                    notify_telegram(f"🔔 *State Dashboard Updated*\nPending items detected: {len(pending) if pending else 0}\nReady for dispatch.")
            time.sleep(poll_interval_sec)
        except KeyboardInterrupt:
            print("Stopping watcher daemon.")
            break
        except Exception as e:
            print(f"Error in watcher loop: {e}")
            time.sleep(poll_interval_sec)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "commit":
        branch = sys.argv[2] if len(sys.argv) > 2 else "main"
        msg = sys.argv[3] if len(sys.argv) > 3 else "chore: automated pipeline update"
        commit_milestone(branch, msg)
    else:
        run_loop()
