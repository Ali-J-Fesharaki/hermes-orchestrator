#!/usr/bin/env python3
"""
github_project_sync.py: Two-Way Synchronization Bridge between GitHub Projects v2
and Hermes Control Center (Cockpit).

Architecture:
  1. Pull from GitHub Projects:
     - Fetches columns (Backlog, Ready, In progress, In review, Done) and task items.
     - Detects user edits, renames, and drag-and-drop column transitions on GitHub.
     - Syncs updates atomically into dashboards/repositories.json and triggers board re-render.
  2. Push to GitHub Projects:
     - When Opus Planner synthesizes new milestones (aligned with human steering intent),
       they are pushed directly as Draft Issues / Cards on GitHub Projects v2.
     - When Execution Agents finish a milestone, its status on GitHub is transitioned to 'Done'.
  3. Human Steering Injector:
     - Allows the user to provide high-level intent/direction per repository in Cockpit,
       steering Opus 4.6 to generate exactly N (default: 3) focused milestones.
"""

import os
import sys
import json
import subprocess
import urllib.request
import urllib.error
from pathlib import Path
from datetime import datetime

AUTO_DIR = Path(__file__).resolve().parent.parent
DASHBOARDS_DIR = AUTO_DIR / "dashboards"
REPOS_FILE = DASHBOARDS_DIR / "repositories.json"
BOARD_ENGINE = DASHBOARDS_DIR / "board_engine.py"
NOTE_CONVERTER = DASHBOARDS_DIR / "note_converter.py"

DEFAULT_OWNER = "Ali-J-Fesharaki"
DEFAULT_PROJECT_NUMBER = 1

def get_gh_token() -> str:
    """Retrieves GitHub token via local file, environment or gh CLI."""
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if token:
        return token
    token_file = AUTO_DIR / ".github_token"
    if token_file.exists():
        try:
            t = token_file.read_text(encoding="utf-8").strip()
            if t:
                return t
        except Exception:
            pass
    try:
        res = subprocess.run(["gh", "auth", "token"], capture_output=True, text=True, timeout=5)
        if res.returncode == 0 and res.stdout.strip():
            return res.stdout.strip()
    except Exception:
        pass
    return ""

def execute_graphql(query: str, variables: dict | None = None) -> dict:
    """Executes a GraphQL query against GitHub API using gh CLI or curl."""
    token = get_gh_token()
    if token:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "Hermes-Cockpit-Bridge"
        }
        payload = {"query": query, "variables": variables or {}}
        req = urllib.request.Request(
            "https://api.github.com/graphql",
            data=json.dumps(payload).encode("utf-8"),
            headers=headers
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            return {"error": str(e)}

    # Fallback to gh api graphql CLI
    cmd = ["gh", "api", "graphql", "-f", f"query={query}"]
    if variables:
        for k, v in variables.items():
            if isinstance(v, (int, float)):
                cmd.extend(["-F", f"{k}={v}"])
            else:
                cmd.extend(["-f", f"{k}={v}"])
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        if res.returncode == 0:
            return json.loads(res.stdout)
        return {"error": res.stderr}
    except Exception as e:
        return {"error": str(e)}

def fetch_project_v2_data(owner: str = DEFAULT_OWNER, project_number: int = DEFAULT_PROJECT_NUMBER) -> dict:
    """Queries project details, status field options, and all current items."""
    query = """
    query($login: String!, $number: Int!) {
      user(login: $login) {
        projectV2(number: $number) {
          id
          title
          fields(first: 20) {
            nodes {
              ... on ProjectV2SingleSelectField {
                id
                name
                options {
                  id
                  name
                }
              }
            }
          }
          items(first: 100) {
            nodes {
              id
              fieldValues(first: 10) {
                nodes {
                  ... on ProjectV2ItemFieldSingleSelectValue {
                    name
                    field {
                      ... on ProjectV2FieldCommon {
                        name
                      }
                    }
                  }
                }
              }
              content {
                ... on Issue {
                  id
                  title
                  body
                  state
                  repository {
                    name
                    nameWithOwner
                  }
                }
                ... on DraftIssue {
                  id
                  title
                  body
                }
              }
            }
          }
        }
      }
    }
    """
    res = execute_graphql(query, {"login": owner, "number": project_number})
    if "data" in res and res["data"].get("user", {}).get("projectV2"):
        p = res["data"]["user"]["projectV2"]
        # Extract status field & options mapping
        status_field_id = None
        status_options = {}
        for f in p.get("fields", {}).get("nodes", []):
            if f and f.get("name") == "Status":
                status_field_id = f.get("id")
                for opt in f.get("options", []):
                    status_options[opt["name"].lower()] = opt["id"]
        
        items = []
        for item in p.get("items", {}).get("nodes", []):
            if not item:
                continue
            item_id = item.get("id")
            content = item.get("content") or {}
            title = content.get("title", "")
            body = content.get("body", "")
            
            # Find status
            status = "Backlog"
            for fv in item.get("fieldValues", {}).get("nodes", []):
                if fv and fv.get("field", {}).get("name") == "Status":
                    status = fv.get("name", "Backlog")
                    
            items.append({
                "item_id": item_id,
                "title": title,
                "body": body,
                "status": status,
                "raw_content": content
            })
            
        return {
            "project_id": p.get("id"),
            "title": p.get("title"),
            "status_field_id": status_field_id,
            "status_options": status_options,
            "items": items
        }
    return res

def delete_project_v2_item(project_id: str, item_id: str) -> dict:
    """Deletes an item from GitHub Projects v2."""
    mutation = """
    mutation($projectId: ID!, $itemId: ID!) {
      deleteProjectV2Item(input: {projectId: $projectId, itemId: $itemId}) {
        deletedItemId
      }
    }
    """
    return execute_graphql(mutation, {"projectId": project_id, "itemId": item_id})

def delete_repo_items_from_github(repo_id: str, repo_name: str = "", owner: str = DEFAULT_OWNER, project_number: int = DEFAULT_PROJECT_NUMBER) -> int:
    """Finds and deletes all items on GitHub Projects v2 belonging to a deleted repository."""
    data = fetch_project_v2_data(owner, project_number)
    if "error" in data or not data.get("project_id"):
        return 0

    project_id = data["project_id"]
    deleted_count = 0
    prefix1 = f"[{repo_id.lower()}]"
    prefix2 = f"[{repo_name.lower()}]" if repo_name else prefix1

    for item in data.get("items", []):
        t = item.get("title", "").lower().strip()
        if t.startswith(prefix1) or (repo_name and t.startswith(prefix2)) or (repo_id.lower() in t and "[" in t):
            res = delete_project_v2_item(project_id, item["item_id"])
            if "data" in res and res["data"].get("deleteProjectV2Item"):
                deleted_count += 1
                print(f"[+] Deleted from GitHub Projects: {item.get('title')}")
    return deleted_count

def add_draft_item_to_project(project_id: str, title: str, body: str = "") -> dict:
    mutation = """
    mutation($projectId: ID!, $title: String!, $body: String) {
      addProjectV2DraftIssue(input: {projectId: $projectId, title: $title, body: $body}) {
        projectItem {
          id
        }
      }
    }
    """
    return execute_graphql(mutation, {"projectId": project_id, "title": title, "body": body})

def update_project_card_content(item_id: str, title: str, body: str = "") -> dict:
    """Updates the title and body of an item on GitHub Projects (Draft or Issue)."""
    # 1. First fetch item to check if it's DraftIssue or regular Issue
    query = """
    query($itemId: ID!) {
      node(id: $itemId) {
        ... on ProjectV2Item {
          content {
            __typename
            ... on Issue {
              id
            }
            ... on DraftIssue {
              id
            }
          }
        }
      }
    }
    """
    res = execute_graphql(query, {"itemId": item_id})
    node = res.get("data", {}).get("node", {})
    content = node.get("content") or {}
    content_type = content.get("__typename")
    content_id = content.get("id")

    if content_type == "DraftIssue" and content_id:
        mutation = """
        mutation($draftId: ID!, $title: String!, $body: String) {
          updateProjectV2DraftIssue(input: {draftIssueId: $draftId, title: $title, body: $body}) {
            draftIssue {
              id
            }
          }
        }
        """
        return execute_graphql(mutation, {"draftId": content_id, "title": title, "body": body})
    elif content_type == "Issue" and content_id:
        mutation = """
        mutation($issueId: ID!, $title: String!, $body: String) {
          updateIssue(input: {id: $issueId, title: $title, body: $body}) {
            issue {
              id
            }
          }
        }
        """
        return execute_graphql(mutation, {"issueId": content_id, "title": title, "body": body})
    return {"status": "unresolved_type", "type": content_type}

def update_item_status(project_id: str, item_id: str, field_id: str, option_id: str) -> dict:
    """Updates the Status field of an item on GitHub Projects."""
    mutation = """
    mutation($projectId: ID!, $itemId: ID!, $fieldId: ID!, $optionId: String!) {
      updateProjectV2ItemFieldValue(input: {
        projectId: $projectId,
        itemId: $itemId,
        fieldId: $fieldId,
        value: { singleSelectOptionId: $optionId }
      }) {
        projectV2Item {
          id
        }
      }
    }
    """
    return execute_graphql(mutation, {
        "projectId": project_id,
        "itemId": item_id,
        "fieldId": field_id,
        "optionId": option_id
    })

def set_milestone_github_status(
    milestone_name: str,
    target_status: str,
    commit_hash: str | None = None,
    branch: str | None = None,
    commit_url: str | None = None,
    output_file: str | None = None,
    articles: list | None = None,
    skills: list | None = None
) -> bool:
    """
    Moves a milestone card on GitHub Projects v2 to a specific column:
    'In progress', 'Done', 'Ready', or 'Backlog'.
    When moving to 'Done', updates the card body with rich Git and research artifacts.
    """
    data = fetch_project_v2_data()
    if "error" in data or not data.get("project_id"):
        return False

    project_id = data["project_id"]
    status_field_id = data.get("status_field_id")
    status_options = data.get("status_options", {})
    option_id = status_options.get(target_status.lower())

    if not status_field_id or not option_id:
        return False

    target_clean = milestone_name.lower().strip()
    for item in data.get("items", []):
        title = item.get("title", "").lower().strip()
        if target_clean in title or title in target_clean:
            res = update_item_status(project_id, item["item_id"], status_field_id, option_id)
            if "data" in res and res["data"].get("updateProjectV2ItemFieldValue"):
                print(f"[+] [GitHub Projects] Moved '{item.get('title')}' -> '{target_status}'")
                
                # If target is Done and we have Git / research metadata, enrich the card body!
                if target_status.lower() == "done" and (commit_hash or branch or articles):
                    lines = [
                        f"### 🎯 Delivered Milestone\n",
                        f"- **Active Branch:** `{branch or 'main'}`",
                        f"- **Commit SHA:** [{commit_hash}]({commit_url})" if commit_url else f"- **Commit SHA:** `{commit_hash}`",
                        f"- **Artifact File:** `{output_file or 'N/A'}`",
                        f"- **Completed At:** `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`\n"
                    ]
                    if skills and isinstance(skills, list):
                        lines.append("#### 🛠️ Applied Hermes Skills:")
                        for s in skills[:3]:
                            lines.append(f"- `{s}`")
                        lines.append("")
                    if articles and isinstance(articles, list):
                        lines.append("#### 📚 Attached Research Papers & Verified Sources:")
                        for p in articles[:8]:
                            p_title = p.get("title", "Untitled")
                            p_url = p.get("doi_or_url") or p.get("url") or ""
                            p_year = p.get("year", "")
                            p_auth = p.get("authors", "")
                            if p_url:
                                lines.append(f"- [{p_title}]({p_url}) *({p_auth}, {p_year})*")
                            else:
                                lines.append(f"- **{p_title}** *({p_auth}, {p_year})*")
                    card_body = "\n".join(lines)
                    try:
                        update_project_card_content(item["item_id"], item.get("title"), card_body)
                    except Exception:
                        pass
                return True
    return False

def sync_github_to_cockpit(owner: str = DEFAULT_OWNER, project_number: int = DEFAULT_PROJECT_NUMBER) -> dict:
    """Pull changes from GitHub Projects into local repositories.json."""
    data = fetch_project_v2_data(owner, project_number)
    if "error" in data or not data.get("items"):
        return {"status": "skipped_or_offline", "detail": data.get("error", "No items or offline")}
    
    if not REPOS_FILE.exists():
        return {"status": "error", "detail": "repositories.json not found"}
        
    try:
        with open(REPOS_FILE, "r", encoding="utf-8") as f:
            repos_data = json.load(f)
    except Exception as e:
        return {"status": "error", "detail": str(e)}

    updated_count = 0
    github_items = data["items"]

    # Map GitHub items to local milestones by name/title substring matching
    for gh_item in github_items:
        gh_title = gh_item["title"].strip()
        gh_status = gh_item["status"].lower()
        
        # Convert GitHub column to local status
        local_status = "pending"
        if "done" in gh_status:
            local_status = "completed"
        elif "progress" in gh_status or "review" in gh_status:
            local_status = "in_progress"
        elif "backlog" in gh_status:
            local_status = "backlog"
        elif "ready" in gh_status or "todo" in gh_status:
            local_status = "pending"

        matched = False
        for repo in repos_data.get("repositories", []):
            for m in repo.get("milestones", []):
                # Match title
                if m["name"].lower() in gh_title.lower() or gh_title.lower() in m["name"].lower():
                    matched = True
                    has_change = False
                    if m.get("github_item_id") != gh_item["item_id"]:
                        m["github_item_id"] = gh_item["item_id"]
                        has_change = True
                    if m.get("github_column") != gh_item["status"]:
                        m["github_column"] = gh_item["status"]
                        has_change = True
                    if m.get("status") == "completed" and "done" not in gh_status:
                        # Push Done to GitHub rather than overwriting verified local work
                        opt_id = data.get("status_options", {}).get("done")
                        status_field_id = data.get("status_field_id")
                        if status_field_id and opt_id and data.get("project_id"):
                            update_item_status(data["project_id"], gh_item["item_id"], status_field_id, opt_id)
                        m["github_column"] = "Done"
                        has_change = True
                    elif m.get("status") != "completed" and m.get("status") != local_status:
                        m["status"] = local_status
                        has_change = True
                    if has_change:
                        updated_count += 1
                    break
            if matched:
                break
                
    if updated_count > 0:
        with open(REPOS_FILE, "w", encoding="utf-8") as f:
            json.dump(repos_data, f, indent=2, ensure_ascii=False)
        # Re-render local dashboards
        subprocess.run(["python3", str(NOTE_CONVERTER)], capture_output=True, timeout=5)
        
    return {
        "status": "success",
        "github_items_count": len(github_items),
        "local_updated": updated_count
    }

def reconcile_and_prune_github_projects(owner: str = DEFAULT_OWNER, project_number: int = DEFAULT_PROJECT_NUMBER) -> dict:
    """
    Two-way Garbage Collection & Card Alignment Engine:
      1. Prunes stale orphan cards from GitHub Projects v2 that no longer exist in repositories.json.
      2. Deduplicates any duplicate cards with identical titles.
      3. Pushes any missing local milestones to GitHub Projects v2 with the correct status column.
      4. Aligns status columns (Done, In progress, Backlog) so Cockpit and GitHub Projects match 1:1.
    """
    data = fetch_project_v2_data(owner, project_number)
    if "error" in data or not data.get("project_id"):
        return {"status": "offline_or_unauthorized", "detail": data.get("error")}

    project_id = data["project_id"]
    status_field_id = data.get("status_field_id")
    status_options = data.get("status_options", {})
    items = data.get("items", [])

    if not REPOS_FILE.exists():
        return {"status": "error", "detail": "repositories.json not found"}

    try:
        with open(REPOS_FILE, "r", encoding="utf-8") as f:
            repos_data = json.load(f)
    except Exception as e:
        return {"status": "error", "detail": str(e)}

    # Build local milestone name index
    local_milestones = {}
    for r in repos_data.get("repositories", []):
        r_id = r.get("id", "repo")
        for m in r.get("milestones", []):
            clean_name = m.get("name", "").strip().lower()
            local_milestones[clean_name] = (r_id, m)

    # 1. Deduplicate, Prune Orphans, and Align Status from GitHub Projects
    seen_titles = {}
    pruned_count = 0
    aligned_count = 0
    remaining_items = []

    for it in items:
        gh_title = it.get("title", "").strip().lower()
        clean_title = gh_title
        for r_id in ["phd_proposal", "general", "repo"]:
            clean_title = clean_title.replace(f"[{r_id}]", "").strip()

        # Check duplicate
        if clean_title in seen_titles:
            # Duplicate item -> delete from GitHub Projects
            delete_project_v2_item(project_id, it["item_id"])
            pruned_count += 1
            continue

        # Check orphan and find matching local milestone
        matched_m = None
        for local_name, (r_id, m) in local_milestones.items():
            if local_name in clean_title or clean_title in local_name:
                matched_m = m
                break

        if not matched_m:
            delete_project_v2_item(project_id, it["item_id"])
            pruned_count += 1
        else:
            seen_titles[clean_title] = it
            remaining_items.append(it)
            matched_m["github_item_id"] = it["item_id"]

            # Two-way status alignment
            gh_status = it.get("status", "")
            local_status = matched_m.get("status", "pending")

            if local_status == "completed":
                if gh_status != "Done":
                    opt_id = status_options.get("done")
                    if status_field_id and opt_id:
                        update_item_status(project_id, it["item_id"], status_field_id, opt_id)
                        it["status"] = "Done"
                        aligned_count += 1
                matched_m["github_column"] = "Done"
            elif local_status == "in_progress":
                if gh_status != "In progress":
                    opt_id = status_options.get("in progress")
                    if status_field_id and opt_id:
                        update_item_status(project_id, it["item_id"], status_field_id, opt_id)
                        it["status"] = "In progress"
                        aligned_count += 1
                matched_m["github_column"] = "In progress"
            elif local_status == "in_review":
                if gh_status != "In review":
                    opt_id = status_options.get("in review")
                    if status_field_id and opt_id:
                        update_item_status(project_id, it["item_id"], status_field_id, opt_id)
                        it["status"] = "In review"
                        aligned_count += 1
                matched_m["github_column"] = "In review"
            else:
                # Local is pending, todo, or backlog
                # If human changed status on GitHub, adopt GitHub's column
                if gh_status in ["In progress", "In review", "Ready", "Done", "Backlog"]:
                    matched_m["github_column"] = gh_status
                    if gh_status == "Done":
                        matched_m["status"] = "completed"
                    elif gh_status == "In progress":
                        matched_m["status"] = "in_progress"
                    elif gh_status == "In review":
                        matched_m["status"] = "in_review"
                    elif gh_status == "Ready":
                        matched_m["status"] = "pending"
                    elif gh_status == "Backlog":
                        matched_m["status"] = "backlog"
                else:
                    # Sync local to GitHub
                    target_col = "Ready" if local_status in ["pending", "todo"] else "Backlog"
                    opt_id = status_options.get(target_col.lower())
                    if status_field_id and opt_id and gh_status != target_col:
                        update_item_status(project_id, it["item_id"], status_field_id, opt_id)
                        it["status"] = target_col
                        aligned_count += 1
                    matched_m["github_column"] = target_col

    # 2. Push missing milestones from local to GitHub
    pushed_count = 0
    existing_gh_titles = set(seen_titles.keys())

    for r in repos_data.get("repositories", []):
        r_id = r.get("id", "repo")
        for m in r.get("milestones", []):
            m_name = m.get("name", "").strip()
            clean_name = m_name.lower()
            if not any(clean_name in et or et in clean_name for et in existing_gh_titles):
                formatted_title = f"[{r_id}] {m_name}"
                planner_label = m.get("planned_by") or m.get("added_by") or "Claude Opus / Gemini"
                body_text = f"**Planner Model:** `{planner_label}`\n\n**Details:** {m.get('details', '')}\n\n**File Target:** `{m.get('relative_filepath', '—')}`\n\n*Auto-generated via Hermes Autonomous Orchestrator*"
                res = add_draft_item_to_project(project_id, formatted_title, body_text)
                if "data" in res and res["data"].get("addProjectV2DraftIssue"):
                    new_item_id = res["data"]["addProjectV2DraftIssue"]["projectItem"]["id"]
                    m["github_item_id"] = new_item_id
                    pushed_count += 1
                    # Set proper initial column
                    initial_status = "Done" if m.get("status") == "completed" else ("In progress" if m.get("status") == "in_progress" else "Backlog")
                    opt_id = status_options.get(initial_status.lower())
                    if status_field_id and opt_id:
                        update_item_status(project_id, new_item_id, status_field_id, opt_id)

    # 3. Save updated repositories.json
    with open(REPOS_FILE, "w", encoding="utf-8") as f:
        json.dump(repos_data, f, indent=2, ensure_ascii=False)

    return {
        "status": "success",
        "pruned_orphans": pruned_count,
        "pushed_missing": pushed_count,
        "total_github_items": len(remaining_items) + pushed_count,
        "total_local_milestones": sum(len(r.get("milestones", [])) for r in repos_data.get("repositories", []))
    }

def push_milestones_to_github(owner: str = DEFAULT_OWNER, project_number: int = DEFAULT_PROJECT_NUMBER) -> dict:
    """Reconciles, prunes orphans, and pushes local milestones to GitHub Projects."""
    return reconcile_and_prune_github_projects(owner, project_number)

if __name__ == "__main__":
    action = sys.argv[1] if len(sys.argv) > 1 else "status"
    if action == "pull":
        print(sync_github_to_cockpit())
    elif action == "push":
        print(push_milestones_to_github())
    else:
        print("Checking GitHub Project connection:")
        pdata = fetch_project_v2_data()
        if "error" in pdata:
            print("[!] Connection status:", pdata["error"])
        else:
            print(f"[+] Connected to '{pdata.get('title')}' with {len(pdata.get('items', []))} items.")
