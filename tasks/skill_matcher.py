"""
skill_matcher.py: Authoritative Hermes Skill Classifier & Tagger.
Infers and assigns the top 3 specific Hermes skills for any task/milestone
based on domain, target file extensions, technical specs, and executor prompts.
"""

import re
from typing import Dict, Any, List


def infer_task_skills(m: Dict[str, Any], repo_id: str = "", repo_cat: str = "") -> List[str]:
    """
    Infers exactly 3 authoritative Hermes skills for a milestone.
    """
    # If the milestone already carries custom defined skills (>= 3), preserve them
    existing = m.get("skills")
    if isinstance(existing, list) and len(existing) >= 3 and all(isinstance(s, str) and s.strip() for s in existing):
        return [s.strip() for s in existing[:3]]

    name = str(m.get("name") or "").lower()
    details = str(m.get("details") or "").lower()
    filepath = str(m.get("relative_filepath") or "").lower()
    tech_spec = str(m.get("technical_specification") or "").lower()
    prompt = str(m.get("executor_prompt") or "").lower()
    category = str(repo_cat or "").lower()
    r_id = str(repo_id or "").lower()

    combined = f"{name} {details} {filepath} {tech_spec} {prompt} {r_id} {category}"

    # 1. Intranet Portal / CAS / SSO Automation
    if re.search(r'\b(cas|sso|intranet|portal|dining|dorm|violentmonkey|userscript|tampermonkey)\b', combined):
        return ["intranet-portal-automation-v5", "systematic-debugging", "test-driven-development"]

    # 2. Multi-Repo Kanban Cockpit / Fleet Telemetry / Orchestrator
    if re.search(r'\b(kanban|cockpit|orchestrator|telemetry|quota optimizer|failed_tasks|github projects)\b', combined):
        return ["multi-repo-kanban-cockpit-v25", "github-projects-workflow", "adaptive-rate-and-token-governor"]

    # 3. Campus Network / DNS / Proxy Routing
    if re.search(r'\b(campus network|singbox|v2ray|resolvectl|dns routing|captive portal|ip rule)\b', combined):
        return ["campus-network-routing-v2", "systematic-debugging", "campus-network-routing"]

    # 4. Map2gz / ROS Occupancy Grid to Gazebo 3D World / SDF
    if re.search(r'\b(map2gz|gazebo|sdformat|sdf|pgm_to_sdf|occupancy grid|denoising|vector polygon)\b', combined):
        return ["ros-gazebo-world-generation", "codebase-inspection", "test-driven-development"]

    # 5. Persian Academic LaTeX & Thesis / Proposal Engine
    if filepath.endswith(".tex") or r_id == "phd_proposal" or "xepersian" in combined or "latex" in combined or "proposal" in combined or "chapter" in combined:
        if re.search(r'\b(fix|align|error|typograph|overflow|prompt v2\.8|correct|reconcile|clean|lint)\b', combined):
            return ["persian-academic-latex", "systematic-debugging", "codebase-inspection"]
        elif re.search(r'\b(literature|survey|paper|ingestion|taxonomy|review|bibtex|citation|abstract)\b', combined):
            return ["persian-academic-latex", "grounded-citations", "arxiv"]
        elif re.search(r'\b(ros 2|ros2|costmap|lifecycle|cbf|shield|hardware|driver|uart|can|hil|benchmarking)\b', combined):
            return ["persian-academic-latex", "ros-gazebo-world-generation", "codebase-inspection"]
        elif re.search(r'\b(drl|reinforcement|reward|policy|actor-critic|pomdp|q-learning|ppo)\b', combined):
            return ["persian-academic-latex", "research-paper-writing", "codebase-inspection"]
        else:
            return ["persian-academic-latex", "research-paper-writing", "grounded-citations"]

    # 6. ROS 2 Navigation & Mechatronic Robotics (Python/C++)
    if re.search(r'\b(ros 2|ros2|nav2|costmap|cbf|sensor ring|wheelchair|lifecycle node|kinodynamics)\b', combined):
        return ["ros-gazebo-world-generation", "codebase-inspection", "test-driven-development"]

    # 7. Deep Reinforcement Learning & Machine Learning
    if re.search(r'\b(drl|ppo|reinforcement learning|actor-critic|neural|reward engine|gym)\b', combined):
        return ["research-paper-writing", "codebase-inspection", "test-driven-development"]

    # 8. GitHub Pull Request & Issue Workflow
    if re.search(r'\b(pull request|pr workflow|code review|github issue|ci/cd|pipeline)\b', combined):
        return ["github-pr-workflow", "github-code-review", "github-issues"]

    # Default technical skills
    return ["systematic-debugging", "test-driven-development", "codebase-inspection"]


def tag_milestone_skills(m: Dict[str, Any], repo_id: str = "", repo_cat: str = "") -> List[str]:
    skills = infer_task_skills(m, repo_id=repo_id, repo_cat=repo_cat)
    m["skills"] = skills
    return skills
