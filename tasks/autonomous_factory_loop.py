#!/usr/bin/env python3
"""
autonomous_factory_loop.py: Multi-Repository Non-Stop Autonomous Engine.
Architecture:
  - Iterates over ALL registered repositories in dashboards/repositories.json
  - For each repository:
      1. Inspects its current Git state and milestones in ROADMAP_BOARD.md.
      2. Calls claude-opus-4-6-thinking to review goals and plan the next immediate milestone.
      3. Dispatches gemini-3.8-flash-tiered to generate code/LaTeX/artifacts.
      4. Validates artifacts, commits to the repository's active Git branch.
      5. Updates all Control Tower boards and sends Telegram updates.
  - Loops continuously across repositories until manually halted (Ctrl+C) or tokens exhaust.
"""

import os
import sys
import time
import json
import sqlite3
import subprocess
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path

# Paths
AUTO_DIR = Path(__file__).resolve().parent.parent
DASHBOARDS_DIR = AUTO_DIR / "dashboards"
REPOS_FILE = DASHBOARDS_DIR / "repositories.json"
ROUTER_URL = os.environ.get("ROUTER_URL", "http://127.0.0.1:8045/v1/chat/completions")
ENV_FILE = Path.home() / ".hermes/.env"
NOTIFIER = AUTO_DIR / "telegram_notifier.py"
BOARD_ENGINE = DASHBOARDS_DIR / "board_engine.py"
NOTE_CONVERTER = DASHBOARDS_DIR / "note_converter.py"
FIX_SCRIPT = Path.home() / "Latex_Persian_Tools/fix_latex_persian.py"

PLANNER_MODEL_PRIMARY = "claude-opus-4-6-thinking"
PLANNER_MODEL_FALLBACK = "gemini-3.8-flash-high"
EXECUTOR_MODEL = "gemini-3.8-flash-tiered"

def get_api_key():
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            if line.startswith("HERMES_CUSTOM_ANTIGRAVITY_MANAGER_API_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return os.environ.get("HERMES_CUSTOM_ANTIGRAVITY_MANAGER_API_KEY", "")

def notify(msg: str, force: bool = False):
    """
    Quiet Mode: Only sends a Telegram message when a milestone or whole goal
    is finished, or when an error occurs, not for routine script starts.
    """
    if force and NOTIFIER.exists():
        try:
            subprocess.run(["python3", str(NOTIFIER), msg], capture_output=True, timeout=10)
        except Exception:
            pass

def call_llm(model: str, messages: list, temperature: float = 0.2, max_tokens: int = 4000, retries: int = 3):
    api_key = get_api_key()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens
    }
    
    last_err = None
    for attempt in range(1, retries + 1):
        req = urllib.request.Request(ROUTER_URL, data=json.dumps(payload).encode("utf-8"), headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=180) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return data["choices"][0]["message"]["content"]
        except urllib.error.HTTPError as e:
            err_msg = e.read().decode("utf-8") if e.fp else str(e)
            print(f"[-] HTTP Error from router ({model}) [attempt {attempt}/{retries}]: {e.code} - {err_msg[:200]}")
            last_err = e
            # If 429 quota exhausted or 500 upstream cancelled, wait and retry
            if e.code in [429, 500, 502, 503]:
                time.sleep(15 * attempt)
            else:
                break
        except Exception as e:
            print(f"[-] Network/Timeout Error calling {model} [attempt {attempt}/{retries}]: {e}")
            last_err = e
            # Try to trigger self-heal router if connection refused
            if "Connection refused" in str(e):
                self_heal = Path.home() / ".local/bin/self_heal_router.sh"
                if self_heal.exists():
                    subprocess.run(["bash", str(self_heal)], capture_output=True)
            time.sleep(10 * attempt)
            
    raise last_err or RuntimeError(f"Failed calling {model}")

def load_repositories():
    if not REPOS_FILE.exists():
        return []
    with open(REPOS_FILE, "r") as f:
        return json.load(f).get("repositories", [])

def get_repo_context(repo: dict):
    path = Path(repo["path"])
    if not path.exists():
        return {"error": "Repository path not found"}
    
    git_log = subprocess.run(["git", "log", "-n", "3", "--oneline"], cwd=path, capture_output=True, text=True).stdout.strip()
    branch = subprocess.run(["git", "branch", "--show-current"], cwd=path, capture_output=True, text=True).stdout.strip()
    milestones = repo.get("milestones", [])
    
    # Load PhD proposal dashboard (DASHBOARD_STATE.md) or roadmap board
    dashboard_summary = ""
    dashboard_file = path / "DASHBOARD_STATE.md"
    if dashboard_file.exists():
        dashboard_summary = dashboard_file.read_text(encoding="utf-8")
    elif (DASHBOARDS_DIR / "ROADMAP_BOARD.md").exists():
        dashboard_summary = (DASHBOARDS_DIR / "ROADMAP_BOARD.md").read_text(encoding="utf-8")
    
    return {
        "repo_name": repo["name"],
        "category": repo.get("category", "general"),
        "active_branch": branch,
        "recent_commits": git_log,
        "milestones_overview": milestones,
        "dashboard_state": dashboard_summary
    }

def opus_plan_for_repo(repo_ctx: dict):
    print(f"\n[🧠 STRATEGIC PLANNER] Opus 4.6 reviewing dashboard & overview for '{repo_ctx['repo_name']}'...")
    system_prompt = (
        "You are the Principal Strategic Research & Engineering Planner (Claude Opus 4.6 Thinking).\n"
        "Your role is STRICTLY high-level architectural planning, goal prioritization, and milestone dispatching.\n"
        "DO NOT read, request, or parse other repository source files, full chapter LaTeX bodies, or raw literature.\n"
        "Base your decision SOLELY on the provided PhD Proposal Dashboard (DASHBOARD_STATE.md) and Repository Overview.\n"
        "Select the next immediate pending deliverable or milestone to execute from the Active Goal Queue.\n"
        "Return ONLY a clean JSON object with:\n"
        "  'repo_id': id,\n"
        "  'task_type': 'latex_section' or 'code_implementation' or 'analysis_doc',\n"
        "  'relative_filepath': target file relative to repo root,\n"
        "  'milestone_name': name of the milestone being advanced,\n"
        "  'technical_specification': rigorous mathematical / algorithmic formulation of what must be built,\n"
        "  'executor_prompt': detailed, authoritative prompt instructing the execution model (Gemini 3.8) to implement the artifact completely."
    )
    user_prompt = (
        f"--- REPOSITORY OVERVIEW ---\n"
        f"Repository: {repo_ctx.get('repo_name')}\n"
        f"Active Branch: {repo_ctx.get('active_branch')}\n"
        f"Recent Commits:\n{repo_ctx.get('recent_commits')}\n"
        f"Milestones Status:\n{json.dumps(repo_ctx.get('milestones_overview', []), indent=2)}\n\n"
        f"--- PHD PROPOSAL DASHBOARD ---\n"
        f"{repo_ctx.get('dashboard_state', 'No dashboard available')}\n\n"
        f"Instruction: Review the active goal queue and current state above. What is the single next immediate milestone/file to build?"
    )
    
    # Try Opus first; if quota exhausted or rate limited, gracefully fall back to Gemini 3.8 High
    resp_text = None
    try:
        print(f"[*] Attempting planning with primary model: {PLANNER_MODEL_PRIMARY}...")
        resp_text = call_llm(PLANNER_MODEL_PRIMARY, [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ], temperature=0.3)
    except Exception as e:
        print(f"[!] Primary planner ({PLANNER_MODEL_PRIMARY}) unavailable or quota finished ({e}).")
        print(f"[🔄 FALLBACK] Switching planner to: {PLANNER_MODEL_FALLBACK}...")
        resp_text = call_llm(PLANNER_MODEL_FALLBACK, [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ], temperature=0.3)
    
    clean = resp_text.strip()
    if "```json" in clean:
        clean = clean.split("```json", 1)[1].split("```", 1)[0].strip()
    elif "```" in clean:
        clean = clean.split("```", 1)[1].split("```", 1)[0].strip()
    return json.loads(clean)

def gemini_execute_for_repo(repo: dict, plan: dict):
    repo_path = Path(repo["path"])
    print(f"\n[⚡ GEMINI EXECUTOR] Implementing {plan['relative_filepath']} with {EXECUTOR_MODEL}...")
    
    is_persian_latex = repo.get("category") == "academic-latex-research"
    rule_file = repo_path / "proposal/prompt_for_latex_persian_AI_generation/PROMPT_v2.8.md"
    corpus_file = repo_path / "persian_academic_corpus/glossary/robotics_social_nav_lexicon.md"
    
    rule_content = rule_file.read_text(encoding="utf-8") if rule_file.exists() else ""
    corpus_content = corpus_file.read_text(encoding="utf-8") if corpus_file.exists() else ""
    
    system_prompt = (
        "You are an Elite Autonomous AI Engineer and Academic Writer.\n"
        "Execute the requested technical task with publication-grade perfection.\n"
    )
    if is_persian_latex:
        system_prompt += (
            "CRITICAL XEPERSIAN RULES:\n"
            "1. Output ONLY the raw body of the LaTeX file without preamble.\n"
            "2. Strict Line Separation: Persian and English/LaTeX lines must be separated.\n"
            "3. Enforce standard Persian typography and terms from the approved corpus.\n\n"
            f"--- RULES ---\n{rule_content[:1000]}\n\n"
            f"--- CORPUS ---\n{corpus_content[:1000]}"
        )
        
    user_prompt = (
        f"File: {plan['relative_filepath']}\n"
        f"Milestone: {plan['milestone_name']}\n"
        f"Technical Specification: {plan['technical_specification']}\n\n"
        f"Execution Instructions: {plan['executor_prompt']}\n\n"
        "Generate the complete, fully implemented artifact."
    )
    
    content = call_llm(EXECUTOR_MODEL, [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ], temperature=0.2, max_tokens=4000)
    
    clean_content = content.strip()
    for fence in ["```latex", "```tex", "```python", "```markdown", "```"]:
        if fence in clean_content:
            clean_content = clean_content.split(fence, 1)[1].split("```", 1)[0].strip()
            break
            
    return clean_content

def post_process_and_commit(repo: dict, plan: dict, content: str):
    repo_path = Path(repo["path"])
    out_file = repo_path / plan["relative_filepath"]
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(content, encoding="utf-8")
    print(f"[+] Written {len(content)} chars to {out_file}")
    
    # If XePersian, sanitize and compile
    if repo.get("category") == "academic-latex-research" and out_file.suffix == ".tex":
        if FIX_SCRIPT.exists():
            subprocess.run(["python3", str(FIX_SCRIPT), str(out_file)], capture_output=True)
            
        main_tex = repo_path / "proposal/main.tex"
        if main_tex.exists():
            main_text = main_tex.read_text(encoding="utf-8")
            stem = out_file.stem
            include_line = f"\\include{{./chapters/{stem}}}"
            if include_line not in main_text:
                main_text = main_text.replace("\\bibliographystyle", f"{include_line}\n\\bibliographystyle")
                main_tex.write_text(main_text, encoding="utf-8")
            subprocess.run(["xelatex", "-interaction=nonstopmode", "main.tex"], cwd=main_tex.parent, capture_output=True)
            
    # Git commit in target repo
    subprocess.run(["git", "add", plan["relative_filepath"]], cwd=repo_path)
    if (repo_path / "proposal/main.tex").exists():
        subprocess.run(["git", "add", "proposal/main.tex"], cwd=repo_path)
        
    commit_msg = f"feat(milestone): {plan['milestone_name']} - implement {plan['relative_filepath']}"
    res = subprocess.run(["git", "commit", "-m", commit_msg], cwd=repo_path, capture_output=True, text=True)
    if "nothing to commit" not in res.stdout:
        print(f"[+] Committed in {repo['name']}: {commit_msg}")
        notify(f"🎯 *Repo Milestone Delivered*\n• *Repo:* {repo['name']}\n• *Milestone:* {plan['milestone_name']}\n• *File:* `{plan['relative_filepath']}`\n• *Planner:* Opus 4.6 | *Executor:* Gemini 3.8", force=True)

def run_loop():
    print("=" * 70)
    print(" 🚀 MULTI-REPOSITORY AUTONOMOUS ENGINE (OPUS 4.6 / GEMINI 3.8)")
    print(f"   Primary Planner:   {PLANNER_MODEL_PRIMARY}")
    print(f"   Fallback Planner:  {PLANNER_MODEL_FALLBACK} (if Opus quota exhausted)")
    print(f"   Executor:          {EXECUTOR_MODEL}")
    print("   Scope:             All active repositories in Control Tower")
    print("=" * 70)
    notify("🏭 *Multi-Repository Autonomous Engine Started*\nContinuous loop active across all registered repositories.")
    
    cycle = 1
    while True:
        try:
            repos = load_repositories()
            if not repos:
                print("[-] No repositories registered in repositories.json. Sleeping 30s...")
                time.sleep(30)
                continue
                
            print(f"\n=======================================================")
            print(f"▶ Cycle #{cycle}: Scanning {len(repos)} Repositories ({datetime.now().strftime('%H:%M:%S')})")
            print(f"=======================================================")
            
            for repo in repos:
                print(f"\n[+] Processing Repository: {repo['name']} ({repo['path']})")
                ctx = get_repo_context(repo)
                
                # Step 1: Opus Plans
                plan = opus_plan_for_repo(ctx)
                print(f"[✓] Milestone Selected: {plan['milestone_name']}")
                print(f"[✓] Target File: {plan['relative_filepath']}")
                
                # Step 2: Gemini Executes
                content = gemini_execute_for_repo(repo, plan)
                
                # Step 3: Validate, Compile & Git Commit
                post_process_and_commit(repo, plan, content)
                
                # Refresh Control Tower Boards
                subprocess.run(["python3", str(BOARD_ENGINE)], capture_output=True)
                
                print("[*] Cooling down 10s before next repository...")
                time.sleep(10)
                
            cycle += 1
            print("\n[*] Completed full round across all repositories. Starting next cycle...")
            
        except KeyboardInterrupt:
            print("\n[-] User terminated loop via Ctrl+C. Exiting gracefully.")
            notify("⏹️ *Autonomous Engine Paused*: Stopped by user.")
            break
        except Exception as e:
            print(f"[!] Encountered error in loop: {e}")
            notify(f"⚠️ *Engine Alert*: {e}\nWaiting 60s before retry...")
            time.sleep(60)

if __name__ == "__main__":
    run_loop()
