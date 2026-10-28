"""Pipeline DAG Visualizer & Synchronization Hook.

Synchronizes multi-repository milestones into a directed acyclic graph (DAG),
resolves cross-repository dependency topology, detects file write lock conflicts,
and atomically exports standalone SVG and HTML minimap visual widgets for the
Hermes Cockpit and status dashboards.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# Defensive import of DAGScheduler and MilestoneNode with local heuristics fallback
try:
    from tasks.dag_scheduler import DAGScheduler, MilestoneNode
except ImportError:
    try:
        from dag_scheduler import DAGScheduler, MilestoneNode
    except ImportError:
        # Standalone fallback implementation if tasks.dag_scheduler is not in PYTHONPATH
        @dataclass
        class MilestoneNode:  # type: ignore[no-redef]
            id: str
            title: str
            status: str = "pending"
            prerequisites: List[str] = field(default_factory=list)
            target_file: Optional[str] = None
            repo_id: Optional[str] = None
            repo_name: Optional[str] = None
            tier: int = 0
            metadata: Dict[str, Any] = field(default_factory=dict)

        class DAGScheduler:  # type: ignore[no-redef]
            def __init__(self, allow_missing_prereqs: bool = True) -> None:
                self.allow_missing_prereqs = allow_missing_prereqs
                self.nodes: Dict[str, MilestoneNode] = {}
                self.edges: Dict[str, Set[str]] = {}
                self.reverse_edges: Dict[str, Set[str]] = {}

            def add_node(self, node: MilestoneNode) -> None:
                self.nodes[node.id] = node
                if node.id not in self.edges:
                    self.edges[node.id] = set()
                if node.id not in self.reverse_edges:
                    self.reverse_edges[node.id] = set()

            def add_dependency(self, parent_id: str, child_id: str) -> None:
                if parent_id in self.nodes and child_id in self.nodes:
                    self.edges[parent_id].add(child_id)
                    self.reverse_edges[child_id].add(parent_id)

            def compute_tiers(self) -> Dict[str, int]:
                in_degree: Dict[str, int] = {
                    node_id: len(self.reverse_edges.get(node_id, set()))
                    for node_id in self.nodes
                }
                current_tier = 0
                assigned_tiers: Dict[str, int] = {}
                queue = [n for n, deg in in_degree.items() if deg == 0]

                while queue:
                    next_queue: List[str] = []
                    for n in queue:
                        assigned_tiers[n] = current_tier
                        self.nodes[n].tier = current_tier
                        for child in self.edges.get(n, set()):
                            in_degree[child] -= 1
                            if in_degree[child] == 0:
                                next_queue.append(child)
                    queue = next_queue
                    current_tier += 1

                for n in self.nodes:
                    if n not in assigned_tiers:
                        assigned_tiers[n] = current_tier
                        self.nodes[n].tier = current_tier
                return assigned_tiers

            def export_svg_minimap(self) -> str:
                tiers = self.compute_tiers()
                max_tier = max(tiers.values()) if tiers else 0
                width = max(600, (max_tier + 1) * 180 + 40)
                tier_counts: Dict[int, int] = {}
                for t in tiers.values():
                    tier_counts[t] = tier_counts.get(t, 0) + 1
                max_in_tier = max(tier_counts.values()) if tier_counts else 1
                height = max(300, max_in_tier * 70 + 80)

                svg = [
                    f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
                    f'width="100%" height="100%" style="background:#0d1117;font-family:monospace;">'
                ]
                svg.append(
                    '<style>.node{stroke:#30363d;stroke-width:1.5px;rx:6px;}'
                    '.completed{fill:#238636;stroke:#2ea043;}'
                    '.in_progress{fill:#1f6feb;stroke:#388bfd;}'
                    '.pending{fill:#21262d;stroke:#30363d;}'
                    '.failed{fill:#da3633;stroke:#f85149;}'
                    '.text{fill:#c9d1d9;font-size:11px;font-family:sans-serif;}'
                    '.edge{stroke:#484f58;stroke-width:1.5;stroke-dasharray:3,3;fill:none;}</style>'
                )

                positions: Dict[str, Tuple[float, float]] = {}
                tier_tracker: Dict[int, int] = {}
                for node_id, node in self.nodes.items():
                    t = tiers.get(node_id, 0)
                    idx = tier_tracker.get(t, 0)
                    tier_tracker[t] = idx + 1
                    cx = 30 + t * 180 + 75
                    cy = 50 + idx * 65
                    positions[node_id] = (cx, cy)

                for parent, children in self.edges.items():
                    px, py = positions.get(parent, (0, 0))
                    for child in children:
                        cx, cy = positions.get(child, (0, 0))
                        svg.append(
                            f'<path d="M {px + 60} {py + 18} C {px + 100} {py + 18}, '
                            f'{cx - 20} {cy + 18}, {cx} {cy + 18}" class="edge" />'
                        )

                for node_id, node in self.nodes.items():
                    x, y = positions.get(node_id, (0, 0))
                    status_class = (
                        node.status.lower()
                        if node.status.lower() in ("completed", "in_progress", "failed")
                        else "pending"
                    )
                    clean_title = (node.title[:18] + "..") if len(node.title) > 20 else node.title
                    clean_id = (node.id[:10] + "..") if len(node.id) > 12 else node.id
                    svg.append(f'<g id="node-{node_id}">')
                    svg.append(f'<rect x="{x}" y="{y}" width="140" height="38" class="node {status_class}" />')
                    svg.append(f'<text x="{x + 8}" y="{y + 16}" class="text" font-weight="bold">{clean_title}</text>')
                    svg.append(f'<text x="{x + 8}" y="{y + 30}" class="text" fill="#8b949e">{clean_id}</text>')
                    svg.append('</g>')

                svg.append('</svg>')
                return "\n".join(svg)

            def render_html_stage_minimap(self) -> str:
                svg_content = self.export_svg_minimap()
                tiers = self.compute_tiers()
                total_nodes = len(self.nodes)
                completed_count = sum(1 for n in self.nodes.values() if n.status.lower() == "completed")
                in_prog_count = sum(1 for n in self.nodes.values() if n.status.lower() == "in_progress")
                pending_count = sum(1 for n in self.nodes.values() if n.status.lower() not in ("completed", "in_progress"))

                return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Pipeline DAG Minimap</title>
    <style>
        body {{
            margin: 0;
            padding: 16px;
            background: #090d13;
            color: #c9d1d9;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
        }}
        .header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 12px;
            border-bottom: 1px solid #21262d;
            padding-bottom: 8px;
        }}
        .title {{
            font-size: 16px;
            font-weight: 600;
            color: #58a6ff;
        }}
        .metrics {{
            font-size: 12px;
            display: flex;
            gap: 12px;
        }}
        .badge {{
            padding: 2px 8px;
            border-radius: 12px;
            background: #21262d;
            border: 1px solid #30363d;
        }}
        .minimap-wrapper {{
            overflow: auto;
            border: 1px solid #30363d;
            border-radius: 8px;
            background: #0d1117;
            padding: 8px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <div class="title">Multi-Repo Pipeline Execution DAG</div>
        <div class="metrics">
            <span class="badge">Nodes: {total_nodes}</span>
            <span class="badge" style="color: #3fb950;">Done: {completed_count}</span>
            <span class="badge" style="color: #58a6ff;">Active: {in_prog_count}</span>
            <span class="badge" style="color: #8b949e;">Pending: {pending_count}</span>
            <span class="badge">Tiers: {max(tiers.values(), default=-1) + 1}</span>
        </div>
    </div>
    <div class="minimap-wrapper">
        {svg_content}
    </div>
</body>
</html>"""

logger = logging.getLogger("pipeline_dag_visualizer")


def atomic_write(filepath: Path, content: str, mode: str = "w", encoding: str = "utf-8") -> None:
    """Atomically writes content to a file via a temporary file and os.replace."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    temp_dir = filepath.parent
    prefix = f".{filepath.name}."
    with tempfile.NamedTemporaryFile(
        mode=mode, dir=temp_dir, prefix=prefix, suffix=".tmp", delete=False, encoding=encoding
    ) as tf:
        tf.write(content)
        tf.flush()
        os.fsync(tf.fileno())
        temp_name = tf.name

    os.replace(temp_name, filepath)


class PipelineDAGVisualizer:
    """Multi-Repo Pipeline DAG Live Synchronization and Cockpit Visualizer Hook."""

    def __init__(self, dashboards_dir: Path) -> None:
        self.dashboards_dir = dashboards_dir
        self.primary_manifest = self.dashboards_dir / "repositories.json"
        self.fallback_manifest = self.dashboards_dir / "repositories.example.json"
        self.svg_output = self.dashboards_dir / "dag_minimap.svg"
        self.html_output = self.dashboards_dir / "dag_minimap.html"

    def load_manifest(self) -> Dict[str, Any]:
        """Safely loads repositories.json with fallback to repositories.example.json."""
        manifest_path = self.primary_manifest
        if not manifest_path.exists():
            if self.fallback_manifest.exists():
                manifest_path = self.fallback_manifest
            else:
                logger.warning(
                    "Neither repositories.json nor repositories.example.json found at %s. Returning empty manifest.",
                    self.dashboards_dir,
                )
                return {"repositories": []}

        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if not isinstance(data, dict):
                    logger.error("Manifest root must be an object: %s", manifest_path)
                    return {"repositories": []}
                return data
        except (json.JSONDecodeError, OSError) as e:
            logger.error("Failed to read manifest %s: %s", manifest_path, e)
            if manifest_path != self.fallback_manifest and self.fallback_manifest.exists():
                try:
                    with open(self.fallback_manifest, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        return data if isinstance(data, dict) else {"repositories": []}
                except Exception as ex:
                    logger.error("Fallback manifest also failed: %s", ex)
            return {"repositories": []}

    def build_graph(self, manifest_data: Dict[str, Any]) -> Tuple[DAGScheduler, List[Dict[str, Any]]]:
        """Parses milestones, builds dependency DAG, and detects file write lock conflicts."""
        scheduler = DAGScheduler(allow_missing_prereqs=True)
        repositories = manifest_data.get("repositories", [])
        if not isinstance(repositories, list):
            repositories = []

        all_nodes: List[MilestoneNode] = []
        id_lookup: Dict[str, str] = {}  # alias -> canonical node id
        file_to_active_nodes: Dict[str, List[str]] = {}
        conflicts: List[Dict[str, Any]] = []

        for repo in repositories:
            if not isinstance(repo, dict):
                continue
            repo_id = str(repo.get("id", repo.get("name", "unknown")))
            repo_name = str(repo.get("name", repo_id))
            milestones = repo.get("milestones", [])
            if not isinstance(milestones, list):
                continue

            for m in milestones:
                if not isinstance(m, dict):
                    continue

                m_name = str(m.get("name", m.get("title", "Untitled Milestone"))).strip()
                m_id = str(m.get("id", f"{repo_id}:{m_name}")).strip()
                status = str(m.get("status", "pending")).strip()
                prereqs = m.get("prerequisites", [])
                if not isinstance(prereqs, list):
                    prereqs = []
                target_file = m.get("relative_filepath") or m.get("target_file")
                if target_file is not None:
                    target_file = str(target_file).strip()

                node = MilestoneNode(
                    id=m_id,
                    title=m_name,
                    status=status,
                    prerequisites=[str(p).strip() for p in prereqs],
                    target_file=target_file,
                    repo_id=repo_id,
                    repo_name=repo_name,
                    metadata={"raw": m},
                )
                all_nodes.append(node)
                scheduler.add_node(node)

                # Map potential aliases
                id_lookup[m_id] = m_id
                id_lookup[m_name.lower()] = m_id
                if target_file:
                    base_name = Path(target_file).name.lower()
                    id_lookup[base_name] = m_id
                    id_lookup[target_file.lower()] = m_id

                    # Track potential concurrent write conflicts for active workers
                    if status.lower() in ("in_progress", "active", "running"):
                        file_key = str(Path(target_file))
                        file_to_active_nodes.setdefault(file_key, []).append(m_id)

        # Detect active file write lock conflicts
        for filepath, active_m_ids in file_to_active_nodes.items():
            if len(active_m_ids) > 1:
                conflicts.append(
                    {
                        "target_file": filepath,
                        "conflicting_milestones": active_m_ids,
                        "type": "CONCURRENT_WRITE_LOCK_HAZARD",
                    }
                )

        # Connect graph dependencies
        for node in all_nodes:
            for p in node.prerequisites:
                p_clean = p.strip()
                p_lower = p_clean.lower()
                target_id = None

                if p_clean in scheduler.nodes:
                    target_id = p_clean
                elif p_lower in id_lookup:
                    target_id = id_lookup[p_lower]
                elif Path(p_clean).name.lower() in id_lookup:
                    target_id = id_lookup[Path(p_clean).name.lower()]

                if target_id and target_id in scheduler.nodes:
                    scheduler.add_dependency(target_id, node.id)
                elif not scheduler.allow_missing_prereqs:
                    logger.warning("Unresolved prerequisite '%s' for milestone '%s'", p, node.id)

        return scheduler, conflicts

    def synchronize(self) -> Dict[str, Any]:
        """Executes full DAG sync, calculates metrics, and persists SVG/HTML artifacts atomically."""
        manifest_data = self.load_manifest()
        scheduler, conflicts = self.build_graph(manifest_data)

        # Calculate topological metrics
        tiers = scheduler.compute_tiers() if hasattr(scheduler, "compute_tiers") else {}
        total_milestones = len(scheduler.nodes)
        tier_count = (max(tiers.values()) + 1) if tiers else 0

        # Critical path length heuristic (max tier depth + 1)
        critical_path_length = tier_count

        # Render artifacts
        svg_content = ""
        if hasattr(scheduler, "export_svg_minimap"):
            svg_content = scheduler.export_svg_minimap()
        elif hasattr(scheduler, "render_svg_minimap"):
            svg_content = scheduler.render_svg_minimap()

        html_content = ""
        if hasattr(scheduler, "render_html_stage_minimap"):
            html_content = scheduler.render_html_stage_minimap()
        elif hasattr(scheduler, "render_html"):
            html_content = scheduler.render_html()
        else:
            html_content = f"<html><body>{svg_content}</body></html>"

        # Atomic persistence
        if svg_content:
            atomic_write(self.svg_output, svg_content)
        if html_content:
            atomic_write(self.html_output, html_content)

        summary = {
            "total_milestones": total_milestones,
            "tiers": tier_count,
            "critical_path_length": critical_path_length,
            "conflicts": conflicts,
            "svg_path": str(self.svg_output),
            "html_path": str(self.html_output),
        }
        return summary


def main() -> None:
    """CLI entrypoint."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Synchronize multi-repo DAG and generate live minimap widgets.")
    parser.add_argument(
        "--output-dir",
        default="dashboards",
        help="Path to dashboards directory containing repositories.json (default: dashboards)",
    )
    args = parser.parse_args()

    dashboards_path = Path(args.output_dir).resolve()
    visualizer = PipelineDAGVisualizer(dashboards_path)
    result = visualizer.synchronize()

    print(json.dumps(result, indent=2))
    if result["conflicts"]:
        logger.warning("Detected %d file write lock conflict(s)!", len(result["conflicts"]))
        for c in result["conflicts"]:
            logger.warning("  Conflict on '%s': %s", c["target_file"], c["conflicting_milestones"])


if __name__ == "__main__":
    # Built-in self-test harness
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_path = Path(tmp_dir)
            mock_repos = {
                "repositories": [
                    {
                        "id": "repo-core",
                        "name": "Core Engine",
                        "milestones": [
                            {
                                "id": "M1",
                                "name": "Scaffold Infrastructure",
                                "status": "completed",
                                "prerequisites": [],
                                "relative_filepath": "src/core/base.py",
                            },
                            {
                                "id": "M2",
                                "name": "Build Event Loop",
                                "status": "in_progress",
                                "prerequisites": ["M1"],
                                "relative_filepath": "src/core/loop.py",
                            },
                        ],
                    },
                    {
                        "id": "repo-worker",
                        "name": "Worker Fleet",
                        "milestones": [
                            {
                                "id": "W1",
                                "name": "Worker Client",
                                "status": "in_progress",
                                "prerequisites": ["base.py"],
                                "relative_filepath": "src/core/loop.py",  # Intentional conflict with M2
                            },
                            {
                                "id": "W2",
                                "name": "Telemetry Reporter",
                                "status": "pending",
                                "prerequisites": ["M2", "Worker Client"],
                                "relative_filepath": "src/worker/telemetry.py",
                            },
                        ],
                    },
                ]
            }

            test_manifest = temp_path / "repositories.json"
            with open(test_manifest, "w", encoding="utf-8") as tf:
                json.dump(mock_repos, tf)

            vis = PipelineDAGVisualizer(temp_path)
            res = vis.synchronize()

            assert res["total_milestones"] == 4, f"Expected 4 milestones, got {res['total_milestones']}"
            assert res["tiers"] >= 2, f"Expected at least 2 tiers, got {res['tiers']}"
            assert len(res["conflicts"]) == 1, f"Expected 1 write conflict, got {len(res['conflicts'])}"
            assert (temp_path / "dag_minimap.svg").exists(), "SVG minimap artifact missing"
            assert (temp_path / "dag_minimap.html").exists(), "HTML minimap artifact missing"
            assert (temp_path / "dag_minimap.svg").stat().st_size > 0, "SVG artifact empty"
            assert (temp_path / "dag_minimap.html").stat().st_size > 0, "HTML artifact empty"

            print("Self-test passed successfully: Visualizer generated valid SVG/HTML with conflict detection.")
            sys.exit(0)

    main()