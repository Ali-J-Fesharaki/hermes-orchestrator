#!/usr/bin/env python3
"""
sync_roadmap_dates.py: Synchronizes Timeline Dates to GitHub Projects v2 for Roadmap Layout
"""

from __future__ import annotations

import os
import sys
import json
import re
import time
import subprocess
from pathlib import Path
from datetime import datetime, timedelta

AUTO_DIR = Path(__file__).resolve().parent.parent
DASHBOARDS_DIR = AUTO_DIR / "dashboards"
REPOS_FILE = DASHBOARDS_DIR / "repositories.json"

for p in [str(AUTO_DIR), str(DASHBOARDS_DIR), str(AUTO_DIR / "tasks")]:
    if p not in sys.path:
        sys.path.insert(0, p)

from tasks.github_project_sync import (
    execute_graphql,
    DEFAULT_OWNER,
    DEFAULT_PROJECT_NUMBER
)

START_DATE_FIELD_ID = "PVTF_lAHOAtOOcs4Bm_oIzhlmYFY"
TARGET_DATE_FIELD_ID = "PVTF_lAHOAtOOcs4Bm_oIzhlmYFc"
PROJECT_ID = "PVT_kwHOAtOOcs4Bm_oI"


def run_roadmap_sync():
    print("==================================================================", flush=True)
    print(" 🗺️ GITHUB PROJECTS V2 ROADMAP & TIMELINE SYNCHRONIZER", flush=True)
    print("==================================================================", flush=True)

    repos_data = json.load(open(REPOS_FILE, encoding="utf-8"))
    today = datetime.now()

    # Pre-cache commit dates per repo
    commit_date_cache = {}
    for r in repos_data.get("repositories", []):
        r_id = r.get("id")
        repo_path = r.get("path")
        if repo_path and Path(repo_path).exists():
            res = subprocess.run(
                ["git", "log", "-n", "300", "--format=%h %cs"],
                cwd=repo_path,
                capture_output=True,
                text=True
            )
            for line in res.stdout.splitlines():
                parts = line.strip().split()
                if len(parts) >= 2:
                    commit_date_cache[(r_id, parts[0])] = parts[1]

    items_to_update = []
    for r in repos_data.get("repositories", []):
        r_id = r.get("id", "repo")
        ms = r.get("milestones", [])
        future_day = today

        for m in ms:
            item_id = m.get("github_item_id")
            if not item_id:
                continue

            status = m.get("status", "pending")
            start_d_str = None
            target_d_str = None

            if status == "completed":
                c = m.get("commit_hash")
                if c:
                    short_c = c[:7]
                    c_date = commit_date_cache.get((r_id, short_c))
                    if c_date:
                        try:
                            dt = datetime.strptime(c_date, "%Y-%m-%d")
                            start_d_str = (dt - timedelta(days=1)).strftime("%Y-%m-%d")
                            target_d_str = dt.strftime("%Y-%m-%d")
                        except Exception:
                            pass

                if not start_d_str and m.get("added_at"):
                    try:
                        dt = datetime.strptime(m["added_at"].split(" ")[0], "%Y-%m-%d")
                        start_d_str = dt.strftime("%Y-%m-%d")
                        target_d_str = (dt + timedelta(days=1)).strftime("%Y-%m-%d")
                    except Exception:
                        pass

                if not start_d_str:
                    start_d_str = "2026-10-18"
                    target_d_str = "2026-10-20"

            elif status == "in_progress":
                start_d_str = today.strftime("%Y-%m-%d")
                target_d_str = (today + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                # Backlog / Ready
                start_d_str = future_day.strftime("%Y-%m-%d")
                target_d_str = (future_day + timedelta(days=2)).strftime("%Y-%m-%d")
                future_day += timedelta(days=2)

            items_to_update.append((item_id, start_d_str, target_d_str, m.get("name", "")))

    print(f"[*] Total milestones mapped: {len(items_to_update)}", flush=True)

    # Execute in smaller chunks of 15 items with retries
    batch_size = 15
    total_batches = (len(items_to_update) + batch_size - 1) // batch_size
    updated_count = 0
    t0 = time.time()

    for b_idx, i in enumerate(range(0, len(items_to_update), batch_size), 1):
        chunk = items_to_update[i:i + batch_size]
        mut_lines = []
        for idx, (item_id, s_date, t_date, _) in enumerate(chunk):
            mut_lines.append(f"""
  s{idx}: updateProjectV2ItemFieldValue(input: {{
    projectId: "{PROJECT_ID}",
    itemId: "{item_id}",
    fieldId: "{START_DATE_FIELD_ID}",
    value: {{ date: "{s_date}" }}
  }}) {{ projectV2Item {{ id }} }}
  t{idx}: updateProjectV2ItemFieldValue(input: {{
    projectId: "{PROJECT_ID}",
    itemId: "{item_id}",
    fieldId: "{TARGET_DATE_FIELD_ID}",
    value: {{ date: "{t_date}" }}
  }}) {{ projectV2Item {{ id }} }}""")

        mutation = "mutation {\n" + "\n".join(mut_lines) + "\n}"
        
        success = False
        for attempt in range(3):
            res = execute_graphql(mutation)
            if "data" in res and res["data"]:
                success = True
                updated_count += len(chunk)
                pct = round((updated_count / len(items_to_update)) * 100.0, 1)
                print(f"    • Batch {b_idx}/{total_batches}: Injected dates for {len(chunk)} items ({updated_count}/{len(items_to_update)} - {pct}%)...", flush=True)
                break
            else:
                time.sleep(2.0)

        if not success:
            print(f"[-] Batch {b_idx} failed after 3 attempts.", flush=True)

        time.sleep(0.3)

    elapsed = time.time() - t0
    print("==================================================================", flush=True)
    print(f" ✅ ROADMAP TIMELINE SYNC COMPLETE in {elapsed:.1f}s!", flush=True)
    print(f" 🎯 All {updated_count} cards now have 'Start date' and 'Target date' set.", flush=True)
    print(" 🚀 GitHub Projects 'Roadmap' tab now renders all phases on the timeline!", flush=True)
    print("==================================================================", flush=True)


if __name__ == "__main__":
    run_roadmap_sync()
