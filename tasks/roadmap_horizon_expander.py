#!/usr/bin/env python3
"""
roadmap_horizon_expander.py: Self-Expanding Roadmap & State-of-the-Art Project Grounder.

Responsibilities:
  1. Detects when a repository's existing roadmap phases are completed or exhausted.
  2. Grounds future evolution in cutting-edge open-source GitHub repositories, real-world products, and research papers.
  3. Proactively writes/appends new, concrete 'Self-Evolution Horizons' directly into the repository's ROADMAP.md.
  4. Formulates application-centric, concise milestones that explicitly define:
     - product_visual: What the human will actually see/experience in the product/UI/CLI/PDF.
     - inspiration: The real-world project/repo inspiring it (e.g. Cursor, v0, Reactive Resume, Connected Papers).
     - details: 1-2 plain, concise sentences (NO academic jargon or bloated word salad).
"""

from __future__ import annotations

import os
import re
import json
import time
from pathlib import Path
from datetime import datetime

# Reference Grounding Knowledge: State-of-the-Art inspirations per repository category
SOTA_INSPIRATION_CATALOG = {
    "hermes_orchestrator": [
        {
            "inspiration": "Cursor Composer & Devin AI",
            "feature": "Interactive Artifact Diff Viewer & Instant Rollback",
            "product_visual": "A visual split-pane modal in the Kanban showing exact code changes before & after with a 1-click 'Rollback' button.",
            "target_file": "control_center/index.html"
        },
        {
            "inspiration": "v0 by Vercel & WebContainers",
            "feature": "Instant In-Browser HTML/SVG Live Preview Widget",
            "product_visual": "A live interactive preview iframe directly inside Kanban card modals for HTML/SVG deliverables.",
            "target_file": "control_center/preview_server.py"
        },
        {
            "inspiration": "Linear & Superhuman",
            "feature": "Lightning Keyboard Command Palette (Ctrl+K)",
            "product_visual": "A sleek dark modal opened with Ctrl+K to instantly filter repos, approve cards, or trigger passes without mouse clicks.",
            "target_file": "control_center/index.html"
        },
        {
            "inspiration": "OpenHands & AutoGPT Fleet Telemetry",
            "feature": "Fleet Audio/TTS Briefing Synthesis",
            "product_visual": "A 'Listen to Daily Briefing' audio player in the header playing a 15-second speech recap of completed tasks.",
            "target_file": "tasks/fleet_audio_briefing.py"
        }
    ],
    "ali_resume__cv_engine": [
        {
            "inspiration": "Reactive Resume & TealHQ",
            "feature": "ATS Keyword Match Matrix & Score Gauge",
            "product_visual": "A visual radial progress gauge (0-100%) and missing keyword badge list comparing the resume against job descriptions.",
            "target_file": "scripts/ats_score_matrix_analyzer.py"
        },
        {
            "inspiration": "CoverLetter AI & Kickresume",
            "feature": "One-Click Contextual Cover Letter Generator",
            "product_visual": "A generated ready-to-send bilingual LaTeX and Markdown cover letter tailored to the hiring manager.",
            "target_file": "scripts/contextual_cover_letter_compiler.py"
        },
        {
            "inspiration": "GitHub Readme Stats & Devicons",
            "feature": "Dynamic SVG Skill Badge Matrix for GitHub/LinkedIn",
            "product_visual": "A clean dark-themed SVG banner containing verified tech badges and publication counts for profile embeds.",
            "target_file": "scripts/generate_svg_profile_banner.py"
        }
    ],
    "phd_proposal": [
        {
            "inspiration": "Connected Papers & ResearchRabbit",
            "feature": "Interactive Literature Citation Resonance Graph",
            "product_visual": "An interactive network graph showing relationships, clusters, and citations between all cited papers.",
            "target_file": "proposal/figures/citation_resonance_network.html"
        },
        {
            "inspiration": "Marp & Pandoc Beamer",
            "feature": "Automated Slide Deck Synthesis from LaTeX Chapters",
            "product_visual": "A compiled 15-slide Beamer defense presentation PDF generated directly from Chapter 3 & 4 mathematical findings.",
            "target_file": "proposal/slides/defense_presentation.tex"
        }
    ]
}

def is_roadmap_exhausted(roadmap_text: str, milestones: list) -> bool:
    """
    Evaluates whether the repository roadmap is completed or has no remaining unfulfilled phases.
    """
    if not roadmap_text or len(roadmap_text.strip()) < 50:
        return True
    
    # Check if all phases mentioned are checked [x] or marked completed
    lines = roadmap_text.splitlines()
    checkboxes = [l for l in lines if re.search(r'^\s*-\s*\[[ xX]\]', l)]
    if checkboxes:
        uncompleted = [c for c in checkboxes if '[ ]' in c]
        if not uncompleted:
            return True
            
    # Check if active uncompleted milestones exist in repositories.json
    active_ms = [m for m in milestones if m.get("status") not in ("completed", "cancelled")]
    if len(active_ms) == 0:
        return True

    return False

def expand_repository_horizon(repo_path: Path, repo_id: str, repo_name: str) -> str:
    """
    Appends a new State-of-the-Art inspired Horizon Phase to the repository's ROADMAP.md.
    Returns the newly added phase description.
    """
    roadmap_path = repo_path / "ROADMAP.md"
    if not roadmap_path.exists():
        roadmap_path = repo_path / "FUTURE_ROADMAP.md"
    if not roadmap_path.exists():
        roadmap_path = repo_path / "ROADMAP.md"

    sota_ideas = SOTA_INSPIRATION_CATALOG.get(repo_id) or SOTA_INSPIRATION_CATALOG["hermes_orchestrator"]
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")

    # Pick the next phase idea
    current_text = ""
    if roadmap_path.exists():
        try:
            current_text = roadmap_path.read_text(encoding="utf-8")
        except Exception:
            current_text = ""

    phase_num = len(re.findall(r'##\s+Phase\s+(\d+)', current_text, re.IGNORECASE)) + 1
    selected_idea = sota_ideas[(phase_num - 1) % len(sota_ideas)]

    new_section = f"""

---

## 🌟 Phase {phase_num}: Self-Evolution Horizon — {selected_idea['feature']}
* **Added Automatically:** {timestamp} (Grounded via `{selected_idea['inspiration']}`)
* **Why this Excites:** Brings real product-centric visual capabilities inspired by state-of-the-art open-source ecosystems.
* **What You Will See in the Product:**
  - {selected_idea['product_visual']}
* **Concrete Application Target:** `{selected_idea['target_file']}`
"""
    try:
        with open(roadmap_path, "a", encoding="utf-8") as f:
            f.write(new_section)
        print(f"[+] 🚀 [HORIZON EXPANSION] Expanded '{repo_name}' ROADMAP.md with Phase {phase_num}: '{selected_idea['feature']}' (Inspiration: {selected_idea['inspiration']})")
        return new_section
    except Exception as e:
        print(f"[-] Failed to expand roadmap horizon: {e}")
        return ""

if __name__ == "__main__":
    test_path = Path("/home/ajf/automation")
    res = expand_repository_horizon(test_path, "hermes_orchestrator", "hermes_orchestrator")
    print("Result:\n", res)
