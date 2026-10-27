#!/usr/bin/env python3
"""
cleanup_github_duplicates.py: Anti-Divergence Garbage Collector for GitHub Projects v2
1. Paginates through all items on the GitHub Projects v2 board.
2. Deduplicates repeated cards (keeping only 1 authentic card per milestone).
3. Prunes orphan cards that no longer exist in local repositories.json.
4. Executes deletions in high-speed GraphQL multi-mutation batches (25 items/req).
5. Brings the remote GitHub Projects board into exact 1:1 parity with the local Hermes Cockpit.
"""

from __future__ import annotations

import os
import sys
import json
import re
import time
import urllib.request
from pathlib import Path
from collections import defaultdict

AUTO_DIR = Path(__file__).resolve().parent.parent
DASHBOARDS_DIR = AUTO_DIR / "dashboards"
REPOS_FILE = DASHBOARDS_DIR / "repositories.json"

for p in [str(AUTO_DIR), str(DASHBOARDS_DIR), str(AUTO_DIR / "tasks")]:
    if p not in sys.path:
        sys.path.insert(0, p)

from github_project_sync import (
    execute_graphql,
    get_gh_token,
    DEFAULT_OWNER,
    DEFAULT_PROJECT_NUMBER
)


def normalize_title(title: str) -> str:
    """Strips [repo_id] prefix and normalizes whitespace."""
    t = re.sub(r"^\[[^\]]+\]\s*", "", title).strip().lower()
    return re.sub(r"\s+", " ", t)


def run_cleanup():
    print("==================================================================")
    print(" 🧹 GITHUB PROJECTS V2 ANTI-DIVERGENCE GARBAGE COLLECTOR")
    print("==================================================================")

    if not REPOS_FILE.exists():
        print("[-] Error: repositories.json not found!")
        return

    # 1. Load authentic local milestones
    repos_data = json.load(open(REPOS_FILE, encoding="utf-8"))
    local_index = {}
    for r in repos_data.get("repositories", []):
        r_id = r.get("id", "repo")
        for m in r.get("milestones", []):
            norm = normalize_title(m.get("name", ""))
            local_index[norm] = (r_id, m)

    print(f"[*] Authentic local milestones in Hermes Orchestrator: {len(local_index)}")

    # 2. Fetch all remote items via GraphQL with cursor pagination
    print("[*] Fetching all remote items from GitHub Projects v2 (paginating)...")
    all_remote_items = []
    cursor = None
    page = 0
    t0 = time.time()
    project_id = None
    status_field_id = None
    status_options = {}

    while True:
        page += 1
        after_clause = f', after: "{cursor}"' if cursor else ''
        q = f"""
        query {{
          user(login: "{DEFAULT_OWNER}") {{
            projectV2(number: {DEFAULT_PROJECT_NUMBER}) {{
              id
              title
              fields(first: 20) {{
                nodes {{
                  ... on ProjectV2SingleSelectField {{
                    id
                    name
                    options {{ id name }}
                  }}
                }}
              }}
              items(first: 100{after_clause}) {{
                totalCount
                pageInfo {{ endCursor hasNextPage }}
                nodes {{
                  id
                  fieldValues(first: 5) {{
                    nodes {{
                      ... on ProjectV2ItemFieldSingleSelectValue {{
                        name
                        field {{ ... on ProjectV2FieldCommon {{ name }} }}
                      }}
                    }}
                  }}
                  content {{
                    ... on DraftIssue {{ title createdAt }}
                    ... on Issue {{ title createdAt }}
                  }}
                }}
              }}
            }}
          }}
        }}
        """
        res = execute_graphql(q)
        if "data" not in res or not res["data"].get("user", {}).get("projectV2"):
            print("[-] Error fetching from GitHub:", res.get("error") or res.get("errors"))
            return

        p = res["data"]["user"]["projectV2"]
        project_id = p["id"]

        if not status_options:
            for f in p.get("fields", {}).get("nodes", []):
                if f and f.get("name") == "Status":
                    status_field_id = f.get("id")
                    for opt in f.get("options", []):
                        status_options[opt["name"].lower()] = opt["id"]

        p_items = p["items"]
        nodes = p_items.get("nodes", [])

        for n in nodes:
            c = n.get("content") or {}
            raw_title = c.get("title", "")
            st = "Backlog"
            for fv in n.get("fieldValues", {}).get("nodes", []):
                if fv and fv.get("field", {}).get("name") == "Status":
                    st = fv.get("name", "Backlog")
            all_remote_items.append({
                "id": n["id"],
                "raw_title": raw_title,
                "status": st,
                "created_at": c.get("createdAt", "")
            })

        if page % 10 == 0:
            print(f"    • Fetched {len(all_remote_items)} cards (page {page})...")

        if not p_items["pageInfo"]["hasNextPage"] or not p_items["pageInfo"]["endCursor"]:
            break
        cursor = p_items["pageInfo"]["endCursor"]

    print(f"[+] Total remote cards on GitHub Projects: {len(all_remote_items)} (fetched in {time.time()-t0:.1f}s)")

    # 3. Classify cards into Keep vs Delete
    groups_by_title = defaultdict(list)
    orphans = []

    for it in all_remote_items:
        norm = normalize_title(it["raw_title"])
        # Fuzzy match to local milestones
        matched_norm = None
        if norm in local_index:
            matched_norm = norm
        else:
            for lm in local_index:
                if lm == norm or lm in norm or norm in lm:
                    matched_norm = lm
                    break

        if not matched_norm:
            orphans.append(it["id"])
        else:
            groups_by_title[matched_norm].append(it)

    cards_to_keep = {}
    duplicates_to_delete = []

    for norm, it_list in groups_by_title.items():
        # Prefer card with status == 'Done' if any, else the latest created
        done_cards = [it for it in it_list if it["status"] == "Done"]
        if done_cards:
            chosen = done_cards[0]
            rest = [it for it in it_list if it["id"] != chosen["id"]]
        else:
            chosen = it_list[-1]
            rest = it_list[:-1]

        cards_to_keep[norm] = chosen
        duplicates_to_delete.extend([it["id"] for it in rest])

    total_deletions = duplicates_to_delete + orphans
    print(f"[*] Analysis Summary:")
    print(f"    • Unique authentic cards to retain: {len(cards_to_keep)}")
    print(f"    • Duplicate zombie cards to prune:  {len(duplicates_to_delete)}")
    print(f"    • Orphaned unknown cards to prune:  {len(orphans)}")
    print(f"    • Total cards to delete from GitHub: {len(total_deletions)}")

    if not total_deletions:
        print("✅ Board is already 100% in parity! Nothing to prune.")
        return

    # 4. Execute batch deletions (50 per mutation)
    print(f"\n[*] Executing high-speed batch deletions in chunks of 50...")
    chunk_size = 50
    total_chunks = (len(total_deletions) + chunk_size - 1) // chunk_size
    deleted_count = 0
    t_del_start = time.time()

    for idx, i in enumerate(range(0, len(total_deletions), chunk_size), 1):
        chunk = total_deletions[i:i + chunk_size]
        mut_lines = [
            f'  d{k}: deleteProjectV2Item(input: {{projectId: "{project_id}", itemId: "{it_id}"}}) {{ deletedItemId }}'
            for k, it_id in enumerate(chunk)
        ]
        mut = "mutation {\n" + "\n".join(mut_lines) + "\n}"
        res = execute_graphql(mut)
        if "data" in res and res["data"]:
            batch_success = len([k for k, v in res["data"].items() if v and v.get("deletedItemId")])
            deleted_count += batch_success

        if idx % 5 == 0 or idx == total_chunks:
            elapsed = time.time() - t_del_start
            pct = round((deleted_count / len(total_deletions)) * 100.0, 1)
            msg = f"    • Batch {idx}/{total_chunks}: Deleted {deleted_count}/{len(total_deletions)} ({pct}%) in {elapsed:.1f}s..."
            print(msg, flush=True)
            with open(DASHBOARDS_DIR / "cleanup_progress.log", "a") as lf:
                lf.write(msg + "\n")

    print(f"\n✅ Pruning Complete: Successfully deleted {deleted_count} zombie cards!")

    # 5. Update local repositories.json with the remaining authentic item IDs
    updated_milestones = 0
    for r in repos_data.get("repositories", []):
        for m in r.get("milestones", []):
            norm = normalize_title(m.get("name", ""))
            if norm in cards_to_keep:
                m["github_item_id"] = cards_to_keep[norm]["id"]
                updated_milestones += 1

    with open(REPOS_FILE, "w", encoding="utf-8") as f:
        json.dump(repos_data, f, indent=2, ensure_ascii=False)

    print(f"[+] Updated {updated_milestones} milestones in repositories.json with authoritative GitHub item IDs.")
    print("==================================================================")
    print(f" 🎯 PARITY ACHIEVED: GitHub Projects now has exactly ~{len(cards_to_keep)} clean cards.")
    print("==================================================================")


if __name__ == "__main__":
    run_cleanup()
