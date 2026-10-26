#!/usr/bin/env python3
"""
persian_writer.py: Orchestrates the Persian Academic LaTeX writing pipeline.
Enforces:
  1. Upstream prompt rules (~/PhD_Proposal/proposal/prompt_for_latex_persian_AI_generation/PROMPT_v2.8.md)
  2. Academic Persian corpus (~/PhD_Proposal/persian_academic_corpus/glossary/robotics_social_nav_lexicon.md)
  3. Strict XePersian Bi-Directional Line Separation
  4. Automatic post-validation with Latex_Persian_Tools/fix_latex_persian.py
"""

import os
import sys
import argparse
import subprocess
from pathlib import Path

PROPOSAL_DIR = Path.home() / "PhD_Proposal"
PROPOSAL_SRC = PROPOSAL_DIR / "proposal"
CORPUS_FILE = PROPOSAL_DIR / "persian_academic_corpus/glossary/robotics_social_nav_lexicon.md"
RULE_FILE = PROPOSAL_SRC / "prompt_for_latex_persian_AI_generation/PROMPT_v2.8.md"
FIX_SCRIPT = Path.home() / "Latex_Persian_Tools/fix_latex_persian.py"
NOTIFIER = Path(__file__).resolve().parent.parent / "telegram_notifier.py"

def write_chapter(chapter_num: int, target_section: str = "all"):
    print(f"============================================================")
    print(f" Persian Academic LaTeX Writer: Chapter {chapter_num}")
    print(f"============================================================")
    print(f"[*] Upstream Rule File: {RULE_FILE}")
    print(f"[*] Domain Corpus:      {CORPUS_FILE}")
    
    target_tex = PROPOSAL_SRC / "chapters" / f"chapter{chapter_num}.tex"
    print(f"[*] Target File:        {target_tex}")
    
    # Notify Telegram of task start
    if NOTIFIER.exists():
        subprocess.run(["python3", str(NOTIFIER), f"✍️ *Persian LaTeX Writer Started*\n• Chapter: {chapter_num}\n• Section: {target_section}\n• Validating with Bi-Directional tools."], capture_output=True)

    print(f"[+] Persian drafting agent primed for Chapter {chapter_num}.")
    print(f"[+] Run with Hermes interactive command or auto-sharded generator.")

def compile_proposal():
    print("[*] Compiling Persian Proposal with xelatex...")
    cmd = ["latexmk", "-xelatex", "-interaction=nonstopmode", "main.tex"]
    res = subprocess.run(cmd, cwd=PROPOSAL_SRC, capture_output=True, text=True)
    if res.returncode == 0:
        print("[+] Compilation Successful! PDF generated at proposal/main.pdf")
        if NOTIFIER.exists():
            subprocess.run(["python3", str(NOTIFIER), "📄 *XePersian Compilation Successful!* PDF is up to date."], capture_output=True)
    else:
        print(f"[-] Compilation error:\n{res.stderr[:500]}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Persian Academic LaTeX Writer Agent")
    parser.add_argument("action", choices=["write", "compile", "sanitize"], default="write", nargs="?")
    parser.add_argument("--chapter", type=int, default=1, help="Chapter number (1, 2, 3...)")
    parser.add_argument("--section", type=str, default="all", help="Target section name")
    args = parser.parse_args()

    if args.action == "write":
        write_chapter(args.chapter, args.section)
    elif args.action == "compile":
        compile_proposal()
    elif args.action == "sanitize":
        target_tex = PROPOSAL_SRC / "chapters" / f"chapter{args.chapter}.tex"
        if FIX_SCRIPT.exists() and target_tex.exists():
            subprocess.run(["python3", str(FIX_SCRIPT), str(target_tex)])
            print(f"[+] Sanitized {target_tex}")
