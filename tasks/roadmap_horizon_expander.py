#!/usr/bin/env python3
"""
roadmap_horizon_expander.py: Self-Expanding Roadmap & Hermes-Native Project Grounder with Human Approval Gate.

Responsibilities:
  1. Detects when a repository's existing roadmap phases are completed or exhausted.
  2. Proposes future evolution strictly grounded in the Hermes Harness (Hermes CLI, Hermes Skills, Hermes Router).
  3. Enforces a STRICT HUMAN APPROVAL GATE: Proposes new phases as drafts. Does NOT auto-write or plan
     until the human architect explicitly authorizes the horizon proposal in the UI or via API.
  4. Formulates application-centric, concise milestones that explicitly define:
     - product_visual: What the human will actually see/experience in the product/UI/CLI/PDF.
     - inspiration: The Hermes-native capability or cutting-edge open-source workflow inspiring it.
     - details: 1-2 plain, concise sentences strictly in English (NO academic jargon).
"""

from __future__ import annotations

import os
import re
import json
import time
from pathlib import Path
from datetime import datetime

AUTO_DIR = Path(__file__).resolve().parent.parent
DASH_DIR = AUTO_DIR / "dashboards"
PROPOSALS_FILE = DASH_DIR / "pending_horizons.json"

# Grounding Knowledge: Hermes-Native & Open-Source Workflows per repository category
SOTA_INSPIRATION_CATALOG = {
    "hermes_orchestrator": [
        {
            "inspiration": "Hermes Subprocess Streaming & Live Tool Logs",
            "feature": "Live Subprocess Output Drawer & Step-by-Step Tool Trace",
            "product_visual": "A slide-out drawer in the Cockpit streaming live terminal output, tool calls, and syntax checks from active Hermes worker subprocesses.",
            "target_file": "control_center/index.html"
        },
        {
            "inspiration": "Hermes Multi-Model Gateway & Dynamic Router Discovery",
            "feature": "Dynamic Model Catalog Explorer & One-Click Model Routing",
            "product_visual": "A visual model discovery dropdown querying the local Hermes router (:8045) allowing dynamic per-agent model reassignment.",
            "target_file": "control_center/index.html"
        },
        {
            "inspiration": "Hermes Native TTS & Audio Briefing Engine",
            "feature": "Daily Speech Briefing via Hermes text_to_speech Tool",
            "product_visual": "An audio player button in the Cockpit header that plays a 15-second synthesized speech summary of today's completed milestones.",
            "target_file": "tasks/fleet_audio_briefing.py"
        },
        {
            "inspiration": "Hermes Skill Ecosystem & Dynamic Capability Injection",
            "feature": "Hermes Skill Manager & Active Skill Badges",
            "product_visual": "An interactive modal listing available Hermes skills in ~/.hermes/skills/ with one-click injection into repository developer prompts.",
            "target_file": "tasks/skill_matcher.py"
        }
    ],
    "ali_resume__cv_engine": [
        {
            "inspiration": "Hermes Web Search & Reactive Resume ATS Engine",
            "feature": "ATS Keyword Match Matrix & Skill Gap Visualizer",
            "product_visual": "A visual score gauge (0-100%) and missing keyword badge list comparing the resume against live employer job requirements.",
            "target_file": "scripts/ats_score_matrix_analyzer.py"
        },
        {
            "inspiration": "Hermes Agent Dynamic Document Synthesis",
            "feature": "One-Click Contextual Cover Letter Compiler",
            "product_visual": "A compiled, ready-to-send bilingual LaTeX and Markdown cover letter tailored specifically to the hiring manager.",
            "target_file": "scripts/contextual_cover_letter_compiler.py"
        },
        {
            "inspiration": "Hermes Markdown/SVG Generation Engine",
            "feature": "Dynamic SVG Skill Badge Banner for GitHub & LinkedIn",
            "product_visual": "A clean dark-themed SVG banner containing verified tech badges and publication counts for profile embeds.",
            "target_file": "scripts/generate_svg_profile_banner.py"
        }
    ],
    "phd_proposal": [
        {
            "inspiration": "Hermes Research CLI & Connected Citation Graphs",
            "feature": "Interactive Literature Citation Resonance Graph",
            "product_visual": "An interactive network graph showing relationships, clusters, and citation linkages between all thesis bibliography references.",
            "target_file": "proposal/figures/citation_resonance_network.html"
        },
        {
            "inspiration": "Hermes XePersian LaTeX Multi-Target Compiler",
            "feature": "Automated Defense Slide Deck Synthesis from Thesis Chapters",
            "product_visual": "A compiled 15-slide Beamer defense presentation PDF generated directly from Chapter 3 & 4 mathematical findings.",
            "target_file": "proposal/slides/defense_presentation.tex"
        }
    ]
}

def load_proposals() -> dict:
    if not PROPOSALS_FILE.exists():
        return {}
    try:
        with open(PROPOSALS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_proposals(data: dict):
    temp_f = PROPOSALS_FILE.with_suffix(".tmp")
    with open(temp_f, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")
    temp_f.replace(PROPOSALS_FILE)

def is_roadmap_exhausted(roadmap_text: str, milestones: list) -> bool:
    """
    Evaluates whether the repository roadmap is completed or has no remaining unfulfilled tasks.
    """
    if not roadmap_text or len(roadmap_text.strip()) < 50:
        return True
    
    # Check if active uncompleted milestones exist in repositories.json
    active_ms = [m for m in milestones if m.get("status") not in ("completed", "cancelled")]
    if len(active_ms) == 0:
        return True

    return False

def get_pending_horizon_proposal(repo_id: str) -> dict | None:
    """Returns an unapproved horizon proposal for this repository if one exists."""
    proposals = load_proposals()
    prop = proposals.get(repo_id)
    if prop and prop.get("status") == "AWAITING_HUMAN_APPROVAL":
        return prop
    return None

def propose_repository_horizon(repo_path: Path, repo_id: str, repo_name: str) -> dict:
    """
    Formulates a proposed Horizon Phase grounded in Hermes-native workflows.
    STRICTLY HOLDS in AWAITING_HUMAN_APPROVAL state until user authorizes it.
    """
    proposals = load_proposals()
    existing_prop = proposals.get(repo_id)
    if existing_prop and existing_prop.get("status") == "AWAITING_HUMAN_APPROVAL":
        return existing_prop

    roadmap_path = repo_path / "ROADMAP.md"
    if not roadmap_path.exists():
        roadmap_path = repo_path / "FUTURE_ROADMAP.md"

    sota_ideas = SOTA_INSPIRATION_CATALOG.get(repo_id) or SOTA_INSPIRATION_CATALOG["hermes_orchestrator"]
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")

    current_text = ""
    if roadmap_path.exists():
        try:
            current_text = roadmap_path.read_text(encoding="utf-8")
        except Exception:
            current_text = ""

    phase_num = len(re.findall(r'##\s+Phase\s+(\d+)', current_text, re.IGNORECASE)) + 1
    selected_idea = sota_ideas[(phase_num - 1) % len(sota_ideas)]

    # Validate against Repository Identity Charter (Anti-Goals Guard)
    try:
        try:
            from tasks.repository_identity_manager import get_repository_identity, validate_proposal_against_identity
        except ImportError:
            from repository_identity_manager import get_repository_identity, validate_proposal_against_identity  # type: ignore
        charter = get_repository_identity(repo_path, repo_id, repo_name)
        valid, reason = validate_proposal_against_identity(charter, selected_idea["feature"], selected_idea["product_visual"])
        if not valid:
            print(f"[!] [CHARTER GUARD] Horizon candidate '{selected_idea['feature']}' rejected: {reason}")
            # select alternative conforming idea
            for alt_idea in sota_ideas:
                v, _ = validate_proposal_against_identity(charter, alt_idea["feature"], alt_idea["product_visual"])
                if v:
                    selected_idea = alt_idea
                    break
    except Exception as ce:
        print(f"[-] Identity validation notice: {ce}")

    proposal = {
        "repo_id": repo_id,
        "repo_name": repo_name,
        "phase_number": phase_num,
        "feature_title": selected_idea["feature"],
        "inspiration": selected_idea["inspiration"],
        "product_visual": selected_idea["product_visual"],
        "target_file": selected_idea["target_file"],
        "status": "AWAITING_HUMAN_APPROVAL",
        "proposed_at": timestamp
    }

    proposals[repo_id] = proposal
    save_proposals(proposals)
    print(f"[!] ⏳ [HORIZON PROPOSAL] Prototyped Phase {phase_num} for '{repo_name}': '{selected_idea['feature']}'. Held waiting for Human Approval.")
    return proposal

def approve_repository_horizon(repo_id: str) -> bool:
    """
    Human Architect approves the proposed horizon phase.
    Appends the approved phase into ROADMAP.md and clears the gate.
    """
    proposals = load_proposals()
    prop = proposals.get(repo_id)
    if not prop or prop.get("status") != "AWAITING_HUMAN_APPROVAL":
        return False

    repos_file = DASH_DIR / "repositories.json"
    repo_path = None
    if repos_file.exists():
        try:
            with open(repos_file, "r", encoding="utf-8") as f:
                rdata = json.load(f)
            for r in rdata.get("repositories", []):
                if r.get("id") == repo_id:
                    repo_path = Path(r.get("path", ""))
                    break
        except Exception:
            pass

    if not repo_path or not repo_path.exists():
        repo_path = Path.home() / "automation"

    roadmap_path = repo_path / "ROADMAP.md"
    if not roadmap_path.exists():
        roadmap_path = repo_path / "FUTURE_ROADMAP.md"

    new_section = f"""

---

## 🌟 Phase {prop['phase_number']}: {prop['feature_title']} (Approved)
* **Approved by Human Architect:** {datetime.now().strftime("%Y-%m-%d %H:%M")}
* **Inspiration:** `{prop['inspiration']}`
* **What You Will See in the Product:**
  - {prop['product_visual']}
* **Target Delivery File:** `{prop['target_file']}`
"""
    try:
        with open(roadmap_path, "a", encoding="utf-8") as f:
            f.write(new_section)
        prop["status"] = "APPROVED"
        prop["approved_at"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        save_proposals(proposals)
        print(f"[+] ✅ [HORIZON APPROVED] Human authorized Phase {prop['phase_number']} for '{repo_id}' into {roadmap_path.name}.")
        return True
    except Exception as e:
        print(f"[-] Error writing approved horizon: {e}")
        return False

def reject_repository_horizon(repo_id: str) -> bool:
    """Rejects or dismisses a proposed horizon phase."""
    proposals = load_proposals()
    if repo_id in proposals:
        del proposals[repo_id]
        save_proposals(proposals)
        return True
    return False

if __name__ == "__main__":
    test_path = Path("/home/ajf/automation")
    prop = propose_repository_horizon(test_path, "hermes_orchestrator", "hermes_orchestrator")
    print("Pending Proposal:", json.dumps(prop, indent=2))
