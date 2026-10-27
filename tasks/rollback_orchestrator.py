"""
tasks/rollback_orchestrator.py

Provenance-Linked Rollback Orchestrator & Cascading Recovery Engine.
Provides automated rollback of failed milestones with cascading dependency resolution,
reversion of file overlaps, orphan requeueing with elevated risk modifiers, and
audit journal rendering.
"""

from __future__ import annotations

import argparse
import collections
from collections import deque
import datetime
import enum
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Dict, List, NamedTuple, Optional, Set, Tuple


class CascadeClassification(str, enum.Enum):
    """Classification states for nodes in a cascading rollback tree."""
    REVERT = "REVERT"
    ORPHAN = "ORPHAN"
    SAFE = "SAFE"


class RollbackTarget(NamedTuple):
    """Specification of the primary root milestone target for rollback."""
    milestone_id: str
    commit_hash: str
    files_modified: list[str]
    provenance_entry: dict


class CascadeNode(NamedTuple):
    """Represents a downstream dependency node evaluated during cascade resolution."""
    milestone_id: str
    classification: CascadeClassification
    commit_hash: str | None
    reason: str


class RollbackEvent(NamedTuple):
    """Structured audit record representing an executed or simulated rollback event."""
    timestamp: str
    trigger_source: str
    root_target: RollbackTarget
    cascade_nodes: list[CascadeNode]
    revert_commit_hash: str | None
    requeued_ids: list[str]


class RollbackOrchestrator:
    """
    Orchestrates provenance-linked rollback operations across git revisions and
    dependency graphs with cascading blast-radius mitigation.
    """

    def __init__(
        self,
        repo_root: str,
        provenance_path: str = "dashboards/provenance_ledger.json",
        dependency_graph_path: str = "dashboards/dependency_edges.json",
        rollback_journal_path: str = "dashboards/rollback_journal.json",
    ) -> None:
        """
        Initialize the rollback orchestrator with repository root and artifact paths.

        Args:
            repo_root: Root path to the git repository.
            provenance_path: Relative or absolute path to provenance ledger JSON.
            dependency_graph_path: Relative or absolute path to dependency edges JSON.
            rollback_journal_path: Relative or absolute path to rollback journal JSON.
        """
        self.repo_root = Path(repo_root).resolve()
        self.provenance_path = provenance_path
        self.dependency_graph_path = dependency_graph_path
        self.rollback_journal_path = rollback_journal_path
        self._provenance_cache: Optional[Dict[str, dict]] = None

    def _resolve_path(self, path_str: str) -> Path:
        """Resolve a path against repository root if relative."""
        p = Path(path_str)
        if not p.is_absolute():
            return self.repo_root / p
        return p

    def _load_provenance_ledger(self) -> Dict[str, dict]:
        """Lazy loader for the provenance ledger mapped by milestone_id."""
        if self._provenance_cache is not None:
            return self._provenance_cache

        ledger_file = self._resolve_path(self.provenance_path)
        ledger_map: Dict[str, dict] = {}

        if not ledger_file.exists():
            self._provenance_cache = ledger_map
            return self._provenance_cache

        try:
            with open(ledger_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            if isinstance(data, dict):
                if "entries" in data:
                    entries = data["entries"]
                    if isinstance(entries, list):
                        for item in entries:
                            if isinstance(item, dict) and "milestone_id" in item:
                                ledger_map[str(item["milestone_id"])] = item
                    elif isinstance(entries, dict):
                        for k, v in entries.items():
                            if isinstance(v, dict):
                                ledger_map[str(v.get("milestone_id", k))] = v
                else:
                    for k, v in data.items():
                        if isinstance(v, dict):
                            ledger_map[str(v.get("milestone_id", k))] = v
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        m_id = item.get("milestone_id") or item.get("id")
                        if m_id:
                            ledger_map[str(m_id)] = item
        except Exception as exc:
            sys.stderr.write(f"Warning: Failed to parse provenance ledger at {ledger_file}: {exc}\n")

        self._provenance_cache = ledger_map
        return self._provenance_cache

    def _get_files_modified_for_commit(self, commit_hash: str) -> list[str]:
        """
        Executes git diff-tree to extract list of files modified in a commit.

        Args:
            commit_hash: Commit SHA to inspect.

        Returns:
            List of repository-relative modified file paths.
        """
        cmd = ["git", "diff-tree", "--no-commit-id", "--name-only", "-r", commit_hash]
        try:
            result = subprocess.run(
                cmd,
                cwd=str(self.repo_root),
                capture_output=True,
                text=True,
                check=True,
            )
            files = [line.strip() for line in result.stdout.splitlines() if line.strip()]
            return files
        except subprocess.CalledProcessError as err:
            sys.stderr.write(
                f"Error retrieving modified files for commit {commit_hash}: {err.stderr}\n"
            )
            return []

    def analyze_target(self, milestone_id: str) -> RollbackTarget:
        """
        Looks up milestone_id in the provenance ledger, extracts its commit hash,
        and retrieves the list of modified files.

        Args:
            milestone_id: The unique identifier of the target milestone.

        Returns:
            Populated RollbackTarget named tuple.

        Raises:
            ValueError: If the milestone is not found or lacks a valid commit hash.
        """
        ledger = self._load_provenance_ledger()
        entry = ledger.get(str(milestone_id))
        if entry is None:
            raise ValueError(
                f"Milestone '{milestone_id}' not found in provenance ledger ({self.provenance_path})."
            )

        commit_hash = entry.get("commit_hash") or entry.get("commit") or entry.get("hash")
        if not commit_hash:
            raise ValueError(
                f"Milestone '{milestone_id}' entry in provenance ledger does not specify a commit hash."
            )

        commit_hash = str(commit_hash).strip()
        files_modified = self._get_files_modified_for_commit(commit_hash)

        return RollbackTarget(
            milestone_id=str(milestone_id),
            commit_hash=commit_hash,
            files_modified=files_modified,
            provenance_entry=entry,
        )

    def resolve_cascade(self, target: RollbackTarget) -> list[CascadeNode]:
        """
        Loads dependency edges and performs forward BFS from target.milestone_id
        to collect all transitive dependents. Classifies each dependent as:
        - REVERT: File overlap exists with target.files_modified.
        - ORPHAN: Dependent is reachable via dependency graph but has no file overlap.
        - SAFE: Unreachable (omitted from cascade return).

        Args:
            target: The RollbackTarget being rolled back.

        Returns:
            List of CascadeNode objects representing all affected downstream milestones.
        """
        graph_file = self._resolve_path(self.dependency_graph_path)
        edges: list = []

        if graph_file.exists():
            try:
                with open(graph_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    edges = data
                elif isinstance(data, dict):
                    edges = data.get("edges") or data.get("dependency_edges") or []
            except Exception as exc:
                sys.stderr.write(f"Warning: Failed to load dependency edges from {graph_file}: {exc}\n")

        # Build forward adjacency list: dependency (upstream) -> dependent (downstream)
        adj: Dict[str, List[str]] = collections.defaultdict(list)
        for edge in edges:
            u, v = None, None
            if isinstance(edge, (list, tuple)) and len(edge) >= 2:
                u, v = str(edge[0]), str(edge[1])
            elif isinstance(edge, dict):
                u = edge.get("from") or edge.get("source") or edge.get("parent") or edge.get("dependency")
                v = edge.get("to") or edge.get("target") or edge.get("child") or edge.get("dependent")
            if u and v:
                adj[str(u)].append(str(v))

        # BFS starting from target.milestone_id to find all transitive dependents
        queue: deque[str] = deque([target.milestone_id])
        visited: Set[str] = {target.milestone_id}
        transitive_dependents: List[str] = []

        while queue:
            curr = queue.popleft()
            for neighbor in adj.get(curr, []):
                if neighbor not in visited:
                    visited.add(neighbor)
                    transitive_dependents.append(neighbor)
                    queue.append(neighbor)

        target_files_set = set(target.files_modified)
        ledger = self._load_provenance_ledger()
        cascade_nodes: list[CascadeNode] = []

        for dep_id in transitive_dependents:
            dep_entry = ledger.get(dep_id, {})
            dep_commit = dep_entry.get("commit_hash") or dep_entry.get("commit") or dep_entry.get("hash")
            dep_files: list[str] = []
            if dep_commit:
                dep_files = self._get_files_modified_for_commit(str(dep_commit))

            overlap = set(dep_files) & target_files_set
            if overlap:
                classification = CascadeClassification.REVERT
                reason = f"File overlap with target {target.milestone_id}: {', '.join(sorted(overlap))}"
            else:
                classification = CascadeClassification.ORPHAN
                reason = f"Transitive dependent of {target.milestone_id} without direct file overlap"

            cascade_nodes.append(
                CascadeNode(
                    milestone_id=dep_id,
                    classification=classification,
                    commit_hash=str(dep_commit) if dep_commit else None,
                    reason=reason,
                )
            )

        return cascade_nodes

    def _get_commit_timestamp(self, commit_hash: str) -> int:
        """Retrieves unix timestamp for a commit SHA."""
        cmd = ["git", "log", "-1", "--format=%ct", commit_hash]
        try:
            res = subprocess.run(
                cmd,
                cwd=str(self.repo_root),
                capture_output=True,
                text=True,
                check=True,
            )
            return int(res.stdout.strip())
        except Exception:
            return 0

    def execute_rollback(
        self,
        target: RollbackTarget,
        cascade: list[CascadeNode],
        dry_run: bool = False,
        trigger_source: str = "manual_execution",
        requeued_ids: Optional[list[str]] = None,
    ) -> RollbackEvent:
        """
        Executes git revert for the target and all REVERT cascade nodes in descending
        chronological order, committing the unified rollback patch.

        Args:
            target: Root RollbackTarget.
            cascade: Resolved cascade dependency nodes.
            dry_run: If True, plans actions without executing git modifications.
            trigger_source: Origin of the rollback trigger.
            requeued_ids: Pre-computed list of requeued orphan milestone IDs.

        Returns:
            RollbackEvent representing the executed or simulated rollback.
        """
        # Gather all commit hashes to revert: target commit + REVERT nodes
        hashes_to_revert: List[str] = []
        if target.commit_hash:
            hashes_to_revert.append(target.commit_hash)

        for node in cascade:
            if node.classification == CascadeClassification.REVERT and node.commit_hash:
                if node.commit_hash not in hashes_to_revert:
                    hashes_to_revert.append(node.commit_hash)

        # Sort commits by timestamp descending (newest commits reverted first)
        hashes_to_revert.sort(key=self._get_commit_timestamp, reverse=True)

        cascaded_revert_count = max(0, len(hashes_to_revert) - (1 if target.commit_hash in hashes_to_revert else 0))
        revert_msg = (
            f"revert(rollback): rollback milestone {target.milestone_id} and "
            f"{cascaded_revert_count} cascaded dependents [trigger: {trigger_source}]"
        )

        revert_commit_hash: Optional[str] = None

        if dry_run:
            sys.stdout.write("[DRY-RUN] Rollback execution plan:\n")
            for h in hashes_to_revert:
                sys.stdout.write(f"  [DRY-RUN] git revert --no-commit {h}\n")
            sys.stdout.write(f"  [DRY-RUN] git commit -m \"{revert_msg}\"\n")
            revert_commit_hash = None
        else:
            for h in hashes_to_revert:
                try:
                    subprocess.run(
                        ["git", "revert", "--no-commit", h],
                        cwd=str(self.repo_root),
                        capture_output=True,
                        text=True,
                        check=True,
                    )
                except subprocess.CalledProcessError as err:
                    raise RuntimeError(
                        f"Failed during git revert --no-commit {h}: {err.stderr}"
                    ) from err

            try:
                subprocess.run(
                    ["git", "commit", "-m", revert_msg],
                    cwd=str(self.repo_root),
                    capture_output=True,
                    text=True,
                    check=True,
                )
                res = subprocess.run(
                    ["git", "rev-parse", "HEAD"],
                    cwd=str(self.repo_root),
                    capture_output=True,
                    text=True,
                    check=True,
                )
                revert_commit_hash = res.stdout.strip()
            except subprocess.CalledProcessError as err:
                raise RuntimeError(f"Failed to commit rollback patch: {err.stderr}") from err

        now_str = datetime.datetime.now(datetime.timezone.utc).isoformat()
        return RollbackEvent(
            timestamp=now_str,
            trigger_source=trigger_source,
            root_target=target,
            cascade_nodes=cascade,
            revert_commit_hash=revert_commit_hash,
            requeued_ids=requeued_ids or [],
        )

    def requeue_orphans(
        self,
        cascade: list[CascadeNode],
        pipeline_state: Optional[list[dict]] = None,
    ) -> list[str]:
        """
        Inserts ORPHAN nodes back into pipeline state with status='pending' and risk_modifier=1.5.

        Args:
            cascade: Resolved cascade nodes.
            pipeline_state: Optional list to append the requeued milestone entries into.

        Returns:
            List of milestone IDs that were requeued.
        """
        if pipeline_state is None:
            pipeline_state = []

        ledger = self._load_provenance_ledger()
        requeued_ids: list[str] = []

        for node in cascade:
            if node.classification == CascadeClassification.ORPHAN:
                original_entry = ledger.get(node.milestone_id, {"milestone_id": node.milestone_id})
                requeued_entry = dict(original_entry)
                requeued_entry["status"] = "pending"
                requeued_entry["risk_modifier"] = 1.5
                requeued_entry["requeue_reason"] = node.reason
                pipeline_state.append(requeued_entry)
                requeued_ids.append(node.milestone_id)

        return requeued_ids

    def record_event(self, event: RollbackEvent) -> None:
        """
        Appends the serialized RollbackEvent into the rollback journal JSON.

        Args:
            event: RollbackEvent to record.
        """
        journal_path = self._resolve_path(self.rollback_journal_path)
        journal_data: list = []

        if journal_path.exists():
            try:
                with open(journal_path, "r", encoding="utf-8") as f:
                    content = json.load(f)
                    if isinstance(content, list):
                        journal_data = content
            except Exception as exc:
                sys.stderr.write(f"Warning: Failed to read journal file {journal_path}: {exc}\n")

        serialized_event = {
            "timestamp": event.timestamp,
            "trigger_source": event.trigger_source,
            "root_target": {
                "milestone_id": event.root_target.milestone_id,
                "commit_hash": event.root_target.commit_hash,
                "files_modified": event.root_target.files_modified,
                "provenance_entry": event.root_target.provenance_entry,
            },
            "cascade_nodes": [
                {
                    "milestone_id": node.milestone_id,
                    "classification": node.classification.value
                    if isinstance(node.classification, CascadeClassification)
                    else str(node.classification),
                    "commit_hash": node.commit_hash,
                    "reason": node.reason,
                }
                for node in event.cascade_nodes
            ],
            "revert_commit_hash": event.revert_commit_hash,
            "requeued_ids": event.requeued_ids,
        }

        journal_data.append(serialized_event)
        journal_path.parent.mkdir(parents=True, exist_ok=True)

        with open(journal_path, "w", encoding="utf-8") as f:
            json.dump(journal_data, f, indent=2)

    def render_history(self) -> str:
        """
        Reads the rollback journal and writes dashboards/ROLLBACK_HISTORY.md
        with a reverse-chronological markdown table.

        Returns:
            Rendered markdown history content.
        """
        journal_path = self._resolve_path(self.rollback_journal_path)
        journal_data: list = []

        if journal_path.exists():
            try:
                with open(journal_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        journal_data = data
            except Exception as exc:
                sys.stderr.write(f"Warning: Failed to read journal for rendering: {exc}\n")

        lines: list[str] = [
            "# Provenance-Linked Rollback History & Recovery Ledger",
            "",
            "Autonomous audit trail of milestone rollbacks, cascading blast radius assessments, and requeued orphans.",
            "",
            "| Timestamp | Trigger | Root Milestone | Reverted Count | Requeued Count | Revert Commit |",
            "| :--- | :--- | :--- | :---: | :---: | :--- |",
        ]

        # Reverse chronological ordering
        for entry in reversed(journal_data):
            ts = entry.get("timestamp", "N/A")
            trigger = entry.get("trigger_source", "manual")
            root = entry.get("root_target", {}).get("milestone_id", "Unknown")
            cascade = entry.get("cascade_nodes", [])
            revert_nodes = [
                n for n in cascade if n.get("classification") == CascadeClassification.REVERT.value
            ]
            reverted_count = len(revert_nodes) + 1
            requeued_count = len(entry.get("requeued_ids", []))
            revert_commit = entry.get("revert_commit_hash")
            commit_display = f"`{revert_commit[:7]}`" if revert_commit else "*Dry Run / None*"

            lines.append(
                f"| {ts} | {trigger} | `{root}` | {reverted_count} | {requeued_count} | {commit_display} |"
            )

        lines.append("")
        rendered_md = "\n".join(lines)

        output_path = self._resolve_path("dashboards/ROLLBACK_HISTORY.md")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(rendered_md)

        return rendered_md

    def orchestrate(
        self,
        milestone_id: str,
        trigger_source: str,
        dry_run: bool = False,
    ) -> RollbackEvent:
        """
        Top-level orchestration API:
        1. Analyzes root milestone target and extracts file changes.
        2. Resolves cascading dependency graph and classifies nodes.
        3. Identifies and requeues orphan dependents with risk modifiers.
        4. Executes git reverts and generates atomic rollback commit.
        5. Records structured audit event in rollback journal.
        6. Renders rollback history dashboard.

        Args:
            milestone_id: ID of the failed milestone to rollback.
            trigger_source: Source label triggering the rollback.
            dry_run: Whether to simulate without committing git reverts.

        Returns:
            Completed RollbackEvent.
        """
        target = self.analyze_target(milestone_id)
        cascade = self.resolve_cascade(target)

        pipeline_state: list[dict] = []
        requeued_ids = self.requeue_orphans(cascade, pipeline_state=pipeline_state)

        event = self.execute_rollback(
            target=target,
            cascade=cascade,
            dry_run=dry_run,
            trigger_source=trigger_source,
            requeued_ids=requeued_ids,
        )

        self.record_event(event)
        self.render_history()

        return event


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Provenance-Linked Rollback Orchestrator & Cascading Recovery Engine"
    )
    parser.add_argument(
        "--milestone-id",
        type=str,
        required=True,
        help="Milestone ID to rollback",
    )
    parser.add_argument(
        "--trigger",
        type=str,
        default="manual_cli",
        help="Trigger source for the rollback (default: manual_cli)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate rollback and log planned git actions without executing them",
    )
    parser.add_argument(
        "--repo-root",
        type=str,
        default=".",
        help="Repository root path (default: .)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    orchestrator = RollbackOrchestrator(repo_root=args.repo_root)
    try:
        rollback_event = orchestrator.orchestrate(
            milestone_id=args.milestone_id,
            trigger_source=args.trigger,
            dry_run=args.dry_run,
        )
        sys.stdout.write(
            f"Successfully completed rollback for {rollback_event.root_target.milestone_id} "
            f"(Trigger: {rollback_event.trigger_source}, Dry-Run: {args.dry_run})\n"
        )
        sys.stdout.write(f"Revert Commit: {rollback_event.revert_commit_hash}\n")
        sys.stdout.write(f"Requeued Orphans: {rollback_event.requeued_ids}\n")
    except Exception as error:
        sys.stderr.write(f"Rollback failed: {error}\n")
        sys.exit(1)