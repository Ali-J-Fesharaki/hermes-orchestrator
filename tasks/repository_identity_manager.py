#!/usr/bin/env python3
"""
repository_identity_manager.py: Dynamic Living Identity Charter for GitHub Repositories.

Core Functions:
  1. Generates and maintains an authoritative 'IDENTITY.md' charter in each repository root.
  2. Clearly defines:
     - Core Purpose & Mission
     - What This Repo IS Designed For (In-Scope Capabilities)
     - What This Repo IS NOT Designed For (Anti-Goals / Negative Boundaries)
  3. Dynamic Living Updates: Automatically absorbs human instructions, custom tasks,
     and steering notes to evolve the charter continuously.
  4. Horizon Grounding Guard: Validates that AI-proposed roadmap expansions strictly conform
     to the In-Scope capabilities and never violate Anti-Goals.
"""

from __future__ import annotations

import re
import json
from pathlib import Path
from datetime import datetime

DEFAULT_CHARTERS = {
    "hermes_orchestrator": {
        "title": "Hermes Multi-Repository Autonomous Orchestrator",
        "mission": "Provide a high-throughput, asynchronous, decentralized multi-agent development cockpit and execution runtime powered natively by Hermes Agent CLI and local model routing.",
        "in_scope": [
            "Autonomous multi-repo Kanban board and live agent monitoring cockpit.",
            "Subprocess execution management via authentic Hermes Agent (`hermes -z`).",
            "Decentralized per-repository Actor Pods with fair-share cloud account token scheduling.",
            "Visual product features, live tool trace drawers, and non-blocking human approval gates.",
            "Dynamic telemetry, rate governance, and closed-loop worker capacity forecasting."
        ],
        "out_of_scope": [
            "DO NOT build standalone toy algorithms or academic computer science simulations (e.g. Raft consensus, Chandy-Lamport snapshots, LSM trees) unless directly wired to user-facing product features.",
            "DO NOT introduce foreign proprietary APIs (e.g. Cursor, external closed runners); all workflows must run via native Hermes Harness.",
            "DO NOT overwrite user-configured roadmaps or plan tasks without human authorization when current roadmap goals finish."
        ]
    },
    "ali_resume__cv_engine": {
        "title": "Ali Resume & Professional Career Engineering Pipeline",
        "mission": "Automate tailored, high-impact bilingual curriculum vitae, targeted LinkedIn outreach dossiers, cover letters, and tech portfolio assets.",
        "in_scope": [
            "Bilingual (English/Persian) professional LaTeX and Markdown resume generation.",
            "ATS keyword resonance matching and skill gap visualizers.",
            "Tailored job application dossiers, cover letters, and outreach personas.",
            "GitHub and LinkedIn profile SVG banner & skill badge generators."
        ],
        "out_of_scope": [
            "DO NOT run 40+ minute unbounded compilation loops for repetitive resume variants.",
            "DO NOT introduce heavy unrelated backend server frameworks or distributed database logic.",
            "DO NOT fabricate fake work experience or inaccurate biographical data."
        ]
    },
    "phd_proposal": {
        "title": "PhD Proposal & Robotics DRL Research Engine",
        "mission": "Develop mathematical, algorithmic, and experimental research documentation for Multi-Agent Reinforcement Learning and Social Air-Gap Navigation in Smart Wheelchairs.",
        "in_scope": [
            "Rigorous XePersian LaTeX chapter writing adhering to University of Tehran thesis standards.",
            "Deep Reinforcement Learning (DRL) reward formulations, stability proofs, and mechatronic specs.",
            "Connected citation literature reviews, bibliography management, and defense slide decks.",
            "Social force models, occupancy grid simulations, and crowd interaction metrics."
        ],
        "out_of_scope": [
            "DO NOT overwrite or restructure the root entry file `proposal/main.tex`.",
            "DO NOT produce standalone web applications or unrelated cloud infrastructure inside the thesis tree.",
            "DO NOT break XePersian LaTeX typography rules or corrupt bidirectional Persian typesetting."
        ]
    },
    "map2gz": {
        "title": "Map2Gz (2D ROS Occupancy Grid to Gazebo SDF World Converter)",
        "mission": "Convert 2D ROS occupancy grid map images into fully 3D Gazebo simulation worlds (.world / .sdf) with collision meshes.",
        "in_scope": [
            "Image-to-mesh geometric extrusions and ROS 2 map yaml parsing.",
            "Gazebo classic (.world) and Gazebo Sim / Ignition (.sdf) model generation.",
            "Wall collision meshes, material texturing, and spawn point configurations."
        ],
        "out_of_scope": [
            "DO NOT build general web dashboards or unrelated social media automation inside this package.",
            "DO NOT break ROS 2 navigation compatibility."
        ]
    }
}

def format_charter_markdown(charter: dict) -> str:
    in_scope_md = "\n".join([f"- [x] **{item}**" for item in charter.get("in_scope", [])])
    out_scope_md = "\n".join([f"- [ ] 🚫 **{item}**" for item in charter.get("out_of_scope", [])])
    history_md = "\n".join(charter.get("history", ["- Initial charter instantiated by Hermes Orchestrator."]))

    return f"""# 🛡️ Repository Identity Charter: {charter.get('title', 'Autonomous Repository')}

> **Authoritative Scope & Identity Specification**  
> This living document dictates the boundaries, core mission, and non-negotiable negative constraints
> for autonomous AI development agents and human architects.

---

## 🎯 Core Mission & Purpose (هویت و ماموریت بنیادین)
{charter.get('mission', 'Autonomous multi-agent development.')}

---

## ✅ What This Repo IS Designed For (In-Scope Capabilities)
{in_scope_md}

---

## 🚫 What This Repo IS NOT Designed For (Anti-Goals & Negative Boundaries)
{out_scope_md}

---

## 📈 Living Evolution History (Learned from Human Steering & Tasks)
{history_md}
"""

def get_repository_identity(repo_path: Path, repo_id: str, repo_name: str = "") -> dict:
    """Loads or creates the living IDENTITY.md for a repository."""
    identity_file = repo_path / "IDENTITY.md"
    default_info = DEFAULT_CHARTERS.get(repo_id, {
        "title": repo_name or repo_id,
        "mission": f"Dedicated autonomous engineering repository for '{repo_name or repo_id}'.",
        "in_scope": ["Core repository deliverables, tests, and documentation."],
        "out_of_scope": ["Unrelated frameworks, non-conforming mocks, and out-of-scope abstractions."],
        "history": [f"- Initial charter created on {datetime.now().strftime('%Y-%m-%d')}."]
    })

    if not identity_file.exists():
        try:
            content = format_charter_markdown(default_info)
            identity_file.write_text(content, encoding="utf-8")
        except Exception as e:
            print(f"[-] Could not write initial IDENTITY.md in {repo_path}: {e}")
        return default_info

    try:
        raw = identity_file.read_text(encoding="utf-8")
        # Parse existing sections
        mission_match = re.search(r'## 🎯 Core Mission & Purpose.*?\n(.*?)\n---', raw, re.DOTALL)
        in_scope = re.findall(r'- \[[ xX]\] \*\*(.*?)\*\*', raw)
        out_scope = re.findall(r'- \[[ xX]\] 🚫 \*\*(.*?)\*\*', raw)
        history_match = re.search(r'## 📈 Living Evolution History.*?\n(.*)', raw, re.DOTALL)
        history = [line.strip() for line in (history_match.group(1).splitlines() if history_match else []) if line.strip().startswith("-")]

        return {
            "title": default_info.get("title", repo_name or repo_id),
            "mission": mission_match.group(1).strip() if mission_match else default_info.get("mission", ""),
            "in_scope": in_scope if in_scope else default_info.get("in_scope", []),
            "out_of_scope": out_scope if out_scope else default_info.get("out_of_scope", []),
            "history": history if history else default_info.get("history", [])
        }
    except Exception as e:
        print(f"[-] Error parsing IDENTITY.md in {repo_path}: {e}")
        return default_info

def update_identity_from_human_action(repo_path: Path, repo_id: str, action_type: str, text_content: str):
    """
    Dynamically evolves the repository's IDENTITY.md when the human architect:
    - Creates a new custom milestone
    - Issues a steering directive
    - Adds an explicit constraint
    """
    if not text_content or len(text_content.strip()) < 5:
        return

    charter = get_repository_identity(repo_path, repo_id)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    clean_text = text_content.strip().replace("\n", " ")[:120]

    # Check for negative constraints (e.g. "don't", "never", "no", "نباید", "نکن")
    lower_t = text_content.lower()
    is_negative = any(w in lower_t for w in ["do not", "don't", "never", "avoid", "no ", "نباید", "نکن", "حذف"])

    if is_negative and clean_text not in charter["out_of_scope"]:
        charter["out_of_scope"].append(f"Learned Negative Boundary: {clean_text}")
    elif not is_negative and len(charter["in_scope"]) < 12:
        if any(w in lower_t for w in ["focus", "build", "add", "implement", "میخوام", "بساز"]):
            charter["in_scope"].append(f"Human Directed Capability: {clean_text}")

    charter.setdefault("history", []).append(f"- **{timestamp}** [{action_type}]: Absorbed human guidance: *\"{clean_text}\"*")
    
    # Save back to IDENTITY.md
    try:
        identity_file = repo_path / "IDENTITY.md"
        identity_file.write_text(format_charter_markdown(charter), encoding="utf-8")
        print(f"[+] 🛡️ [IDENTITY UPDATED] Evolved IDENTITY.md for '{repo_id}' from human action ({action_type}).")
    except Exception as e:
        print(f"[-] Failed to update IDENTITY.md for {repo_id}: {e}")

def validate_proposal_against_identity(charter: dict, proposal_title: str, product_visual: str) -> tuple[bool, str]:
    """
    Validates whether an AI-proposed roadmap phase violates any Anti-Goals
    defined in the repository's IDENTITY.md.
    """
    combined = f"{proposal_title} {product_visual}".lower()
    for anti_goal in charter.get("out_of_scope", []):
        keywords = [w.lower() for w in re.findall(r'\b[A-Za-z]{4,}\b', anti_goal) if w.lower() not in ("repo", "build", "make", "with")]
        matches = [kw for kw in keywords if kw in combined]
        if len(matches) >= 2:
            return False, f"Violates Repository Identity Anti-Goal: '{anti_goal}' (matched: {', '.join(matches)})"
    return True, "Aligned with Repository Identity Charter"

if __name__ == "__main__":
    p = Path("/home/ajf/automation")
    c = get_repository_identity(p, "hermes_orchestrator", "hermes_orchestrator")
    print("Charter loaded:\n", json.dumps(c, indent=2))
