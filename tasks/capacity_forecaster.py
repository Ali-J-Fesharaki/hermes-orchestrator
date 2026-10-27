"""Predictive Capacity Forecasting & Token Budget Planner.

Consumes rolling statistical baselines (from Telemetry Anomaly Engine),
git churn complexity heuristics, and critical-path DAG topology
(from Dependency Graph) to forecast token budget consumption, wall-time
estimates, and failure probability for upcoming pipeline milestones.

Uses only the Python standard library.
"""

from __future__ import annotations

from collections import defaultdict, deque
import json
import math
import os
from pathlib import Path
import statistics
import subprocess
from typing import Any, Dict, List, NamedTuple, Optional, Set, Tuple


# ============================================================================
# Core Data Models
# ============================================================================

class CostEstimate(NamedTuple):
    """Resource and risk predictions for a single milestone."""
    predicted_tokens: int
    predicted_wall_time_s: float
    error_probability: float
    complexity_multiplier: float


class PipelineForecast(NamedTuple):
    """Aggregate capacity and feasibility forecast for an entire pipeline."""
    milestone_estimates: list[tuple[str, CostEstimate]]
    critical_path_total_tokens: int
    critical_path_wall_time_s: float
    budget_feasibility_score: float
    per_account_drawdown: dict[str, int]
    gantt_ascii: str


class EMABaseline(NamedTuple):
    """Exponential Moving Average baseline metrics for a repository / task type."""
    ema_tokens: float
    ema_wall_time: float
    ema_error_rate: float
    stddev_tokens: float = 0.0
    stddev_wall_time: float = 0.0
    stddev_error_rate: float = 0.0
    sample_count: int = 1


# ============================================================================
# Capacity Forecaster Engine
# ============================================================================

class CapacityForecaster:
    """Predictive capacity forecasting and token budget planner.

    Evaluates pending milestones against historical telemetry baselines, git churn
    complexity metrics, and DAG critical paths to allocate account quotas and
    determine pipeline feasibility.
    """

    FEASIBILITY_COMFORTABLE = 0.7
    FEASIBILITY_TIGHT = 0.3

    DEFAULT_GLOBAL_BASELINE = EMABaseline(
        ema_tokens=3500.0,
        ema_wall_time=45.0,
        ema_error_rate=0.05,
        stddev_tokens=500.0,
        stddev_wall_time=12.0,
        stddev_error_rate=0.03,
        sample_count=10,
    )

    def __init__(
        self,
        anomaly_registry_path: str = "dashboards/anomaly_registry.json",
        baselines_path: str = "dashboards/telemetry_baselines.json",
        quota_state_path: str = "dashboards/quota_state.json",
        repo_root: Optional[str] = None,
    ) -> None:
        """Initializes the capacity forecaster with state paths and loads baselines.

        Args:
            anomaly_registry_path: Path to persistent anomaly registry JSON.
            baselines_path: Path to telemetry baselines JSON.
            quota_state_path: Path to account quota state JSON.
            repo_root: Default root repository directory for git operations.
        """
        self.anomaly_registry_path = Path(anomaly_registry_path)
        self.baselines_path = Path(baselines_path)
        self.quota_state_path = Path(quota_state_path)
        self.repo_root = str(repo_root or os.getcwd())

        # Loaded states
        self._baselines: Dict[str, Dict[str, EMABaseline]] = self._load_baselines()
        self._anomaly_registry: List[Dict[str, Any]] = self._load_anomaly_registry()
        self._quota_state: Dict[str, Dict[str, int]] = self._load_quota_state()

        # Cache for milestone account assignments from last forecast
        self._last_assignments: Dict[str, str] = {}
        self._last_critical_path_ids: Set[str] = set()

    def _load_baselines(self) -> dict[str, dict[str, EMABaseline]]:
        """Loads baselines from JSON.

        Returns a dictionary mapping repo -> {task_type -> EMABaseline}.
        Reads baselines JSON keyed by 'repo::task_type' with ema_tokens,
        ema_wall_time, ema_error_rate, and stddev fields.
        """
        baselines: Dict[str, Dict[str, EMABaseline]] = defaultdict(dict)

        if self.baselines_path.is_file():
            try:
                with open(self.baselines_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                if isinstance(data, dict):
                    for key, val in data.items():
                        if isinstance(val, dict):
                            repo = key
                            task_type = "default"
                            if "::" in key:
                                repo, task_type = key.split("::", 1)

                            # Format 1: Direct EMABaseline dict
                            if "ema_tokens" in val:
                                baselines[repo][task_type] = EMABaseline(
                                    ema_tokens=float(val.get("ema_tokens", 3500.0)),
                                    ema_wall_time=float(val.get("ema_wall_time", 45.0)),
                                    ema_error_rate=float(val.get("ema_error_rate", 0.05)),
                                    stddev_tokens=float(val.get("stddev_tokens", 0.0)),
                                    stddev_wall_time=float(val.get("stddev_wall_time", 0.0)),
                                    stddev_error_rate=float(val.get("stddev_error_rate", 0.0)),
                                    sample_count=int(val.get("sample_count", 1)),
                                )
                            # Format 2: TelemetryAnomalyEngine metric dicts with 'ema'
                            elif "ema" in val:
                                baselines[repo][task_type] = EMABaseline(
                                    ema_tokens=float(val.get("ema", 3500.0)),
                                    ema_wall_time=45.0,
                                    ema_error_rate=0.05,
                                    sample_count=int(val.get("sample_count", 1)),
                                )
            except Exception:
                baselines = defaultdict(dict)

        # Default fallbacks if baselines file is empty or missing
        if not baselines:
            default_entries = [
                ("hermes_orchestrator", "python_module"),
                ("hermes_orchestrator", "academic_latex"),
                ("hermes_orchestrator", "test_suite"),
                ("hermes_orchestrator", "dashboard_ui"),
                ("phd_proposal", "academic_latex"),
                ("ali_cv_resume", "academic_latex"),
                ("map2gz", "robotics_module"),
            ]
            for repo, task_type in default_entries:
                baselines[repo][task_type] = self.DEFAULT_GLOBAL_BASELINE

        return dict(baselines)

    def _load_anomaly_registry(self) -> List[Dict[str, Any]]:
        """Loads anomaly records from dashboards/anomaly_registry.json if present."""
        if self.anomaly_registry_path.is_file():
            try:
                with open(self.anomaly_registry_path, "r", encoding="utf-8") as f:
                    records = json.load(f)
                    if isinstance(records, list):
                        return records
            except Exception:
                pass
        return []

    def _load_quota_state(self) -> Dict[str, Dict[str, int]]:
        """Loads account token quotas from JSON or falls back to default 4-account rotation."""
        if self.quota_state_path.is_file():
            try:
                with open(self.quota_state_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                if isinstance(data, dict):
                    accounts_raw = data.get("accounts", data)
                    normalized: Dict[str, Dict[str, int]] = {}
                    for acc, details in accounts_raw.items():
                        if isinstance(details, dict):
                            rem = details.get("remaining_tokens", details.get("tokens_remaining", 500000))
                            normalized[acc] = {"remaining_tokens": int(rem)}
                        elif isinstance(details, (int, float)):
                            normalized[acc] = {"remaining_tokens": int(details)}
                    if normalized:
                        return normalized
            except Exception:
                pass

        # Standard 4 Google Pro accounts rotation fallback (500k token window per account)
        return {
            "google_pro_account_1": {"remaining_tokens": 500000},
            "google_pro_account_2": {"remaining_tokens": 500000},
            "google_pro_account_3": {"remaining_tokens": 500000},
            "google_pro_account_4": {"remaining_tokens": 500000},
        }

    def _get_global_median_baseline(self) -> EMABaseline:
        """Computes the global median baseline across all loaded baselines."""
        all_b = [
            baseline
            for task_dict in self._baselines.values()
            for baseline in task_dict.values()
        ]
        if not all_b:
            return self.DEFAULT_GLOBAL_BASELINE

        med_tokens = statistics.median([b.ema_tokens for b in all_b])
        med_wall = statistics.median([b.ema_wall_time for b in all_b])
        med_err = statistics.median([b.ema_error_rate for b in all_b])

        return EMABaseline(
            ema_tokens=float(med_tokens),
            ema_wall_time=float(med_wall),
            ema_error_rate=float(med_err),
            sample_count=sum(b.sample_count for b in all_b),
        )

    def _estimate_complexity(self, repo_root: str, filepath: str) -> float:
        """Estimates code complexity multiplier based on historical churn in git.

        Runs 'git log --numstat -5 -- <filepath>' in repo_root, parses added+deleted lines
        across the last 5 commits, and clamps total_changed / 500.0 to [0.5, 3.0].
        Falls back gracefully if git command fails or file has no churn.
        """
        if not filepath:
            return 1.0

        root = repo_root or self.repo_root
        total_changed = 0
        git_succeeded = False

        try:
            cmd = ["git", "log", "--numstat", "-5", "--", filepath]
            proc = subprocess.run(
                cmd,
                cwd=root if os.path.isdir(root) else None,
                capture_output=True,
                text=True,
                timeout=10,
            )
            if proc.returncode == 0 and proc.stdout:
                git_succeeded = True
                for line in proc.stdout.splitlines():
                    parts = line.strip().split()
                    if len(parts) >= 3:
                        added_str, deleted_str = parts[0], parts[1]
                        if added_str.isdigit() and deleted_str.isdigit():
                            total_changed += int(added_str) + int(deleted_str)
        except Exception:
            git_succeeded = False

        if not git_succeeded or total_changed == 0:
            full_path = Path(root) / filepath if root else Path(filepath)
            if full_path.is_file():
                try:
                    lines = len(full_path.read_text(encoding="utf-8", errors="replace").splitlines())
                    total_changed = max(lines, 250)
                except Exception:
                    total_changed = 500
            else:
                total_changed = 500

        multiplier = total_changed / 500.0
        return max(0.5, min(3.0, multiplier))

    def _detect_task_type(self, milestone: dict) -> str:
        """Infers the task type category for a milestone."""
        if milestone.get("task_type"):
            return str(milestone["task_type"]).lower()

        filepath = str(milestone.get("relative_filepath") or milestone.get("filepath") or "").lower()
        category = str(milestone.get("category") or "").lower()
        name = str(milestone.get("name") or "").lower()

        if filepath.endswith(".tex") or "latex" in category or "latex" in name:
            return "academic_latex"
        if "test" in filepath or "test" in name:
            return "test_suite"
        if filepath.startswith("dashboards/") or "dashboard" in name or "cockpit" in name:
            return "dashboard_ui"
        if "ros" in category or "robotics" in category or "map2gz" in filepath:
            return "robotics_module"
        if filepath.endswith(".py"):
            return "python_module"

        return "python_module"

    def _query_anomaly_risk_boost(self, repo: str, filepath: str) -> float:
        """Queries the anomaly registry to compute an additive risk modifier boost."""
        if not self._anomaly_registry:
            return 0.0

        boost = 0.0
        count = 0
        norm_fp = filepath.replace("\\", "/").lower()

        for rec in self._anomaly_registry:
            r_repo = str(rec.get("repo") or "").lower()
            r_fp = str(rec.get("target_file") or rec.get("filepath") or "").replace("\\", "/").lower()

            if r_repo == repo.lower() or (r_fp and r_fp in norm_fp):
                count += 1
                classification = str(rec.get("classification") or "NOMINAL").upper()
                if classification == "ANOMALOUS":
                    boost += 0.15
                elif classification == "ELEVATED":
                    boost += 0.08
                else:
                    boost += 0.03

        # Clamp additive boost to [0.0, 0.5]
        return min(0.5, boost)

    def estimate_milestone(self, milestone: dict) -> CostEstimate:
        """Estimates token cost, wall time, and error probability for a milestone.

        Matches repository and filepath against baselines, computes complexity
        via git churn, and incorporates anomaly risk modifiers.
        """
        repo = str(
            milestone.get("repository")
            or milestone.get("repo_id")
            or milestone.get("repo")
            or "hermes_orchestrator"
        )
        filepath = str(milestone.get("relative_filepath") or milestone.get("filepath") or "")
        repo_root = str(milestone.get("repo_root") or milestone.get("repo_path") or self.repo_root)

        task_type = self._detect_task_type(milestone)

        # Baseline lookup
        baseline: Optional[EMABaseline] = None

        if repo in self._baselines:
            repo_baselines = self._baselines[repo]
            if task_type in repo_baselines:
                baseline = repo_baselines[task_type]
            elif repo_baselines:
                # Pick first baseline available for this repo
                baseline = next(iter(repo_baselines.values()))

        if baseline is None:
            # Check any repo with matching task_type
            for repo_dict in self._baselines.values():
                if task_type in repo_dict:
                    baseline = repo_dict[task_type]
                    break

        if baseline is None:
            baseline = self._get_global_median_baseline()

        # Complexity multiplier
        complexity = self._estimate_complexity(repo_root, filepath)

        # Anomaly and risk modifier calculation
        base_risk_mod = float(milestone.get("risk_modifier", 1.0))
        anomaly_boost = self._query_anomaly_risk_boost(repo, filepath)
        risk_modifier_from_anomaly_engine = base_risk_mod * (1.0 + anomaly_boost)

        predicted_tokens = int(round(baseline.ema_tokens * complexity))
        predicted_wall_time_s = float(round(baseline.ema_wall_time * complexity, 2))
        error_probability = min(1.0, max(0.0, float(baseline.ema_error_rate * risk_modifier_from_anomaly_engine)))

        return CostEstimate(
            predicted_tokens=predicted_tokens,
            predicted_wall_time_s=predicted_wall_time_s,
            error_probability=round(error_probability, 4),
            complexity_multiplier=round(complexity, 3),
        )

    def forecast_pipeline(
        self,
        pending_milestones: list[dict],
        dependency_edges: list[tuple],
        quota_state: Optional[dict] = None,
    ) -> PipelineForecast:
        """Performs end-to-end predictive capacity forecasting for a list of milestones.

        Topologically sorts milestones via dependency edges, accumulates resource
        consumption along the critical path, distributes token drawdown across
        available accounts using quota-weighted round-robin, and calculates the
        budget feasibility score.
        """
        if not pending_milestones:
            return PipelineForecast(
                milestone_estimates=[],
                critical_path_total_tokens=0,
                critical_path_wall_time_s=0.0,
                budget_feasibility_score=1.0,
                per_account_drawdown={},
                gantt_ascii="[No pending milestones]",
            )

        active_quota = quota_state if quota_state is not None else self._quota_state
        accounts: Dict[str, int] = {}
        if "accounts" in active_quota and isinstance(active_quota["accounts"], dict):
            for acc, det in active_quota["accounts"].items():
                if isinstance(det, dict):
                    accounts[acc] = int(det.get("remaining_tokens", 500000))
                else:
                    accounts[acc] = int(det)
        elif isinstance(active_quota, dict):
            for acc, det in active_quota.items():
                if isinstance(det, dict) and "remaining_tokens" in det:
                    accounts[acc] = int(det["remaining_tokens"])
                elif isinstance(det, (int, float)):
                    accounts[acc] = int(det)

        if not accounts:
            accounts = {
                "google_pro_account_1": 500000,
                "google_pro_account_2": 500000,
                "google_pro_account_3": 500000,
                "google_pro_account_4": 500000,
            }

        # 1. Identifier mapping & DAG setup
        milestone_by_id: Dict[str, dict] = {}
        for idx, m in enumerate(pending_milestones):
            m_id = str(m.get("name") or m.get("id") or f"milestone_{idx}")
            milestone_by_id[m_id] = m

        node_ids = list(milestone_by_id.keys())
        adj: Dict[str, List[str]] = defaultdict(list)
        preds: Dict[str, List[str]] = defaultdict(list)
        in_degree: Dict[str, int] = {m_id: 0 for m_id in node_ids}

        for edge in dependency_edges:
            if isinstance(edge, (list, tuple)) and len(edge) >= 2:
                u, v = str(edge[0]), str(edge[1])
                if u in in_degree and v in in_degree:
                    adj[u].append(v)
                    preds[v].append(u)
                    in_degree[v] += 1

        # 2. Topological Sort (Kahn's algorithm)
        queue = deque([m_id for m_id, deg in in_degree.items() if deg == 0])
        topo_order: List[str] = []

        while queue:
            curr = queue.popleft()
            topo_order.append(curr)
            for neighbor in adj[curr]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        # Append remaining nodes in case of cycles or isolated subgraphs
        for m_id in node_ids:
            if m_id not in topo_order:
                topo_order.append(m_id)

        # 3. Compute CostEstimate for each milestone
        milestone_estimates: List[Tuple[str, CostEstimate]] = []
        estimates_map: Dict[str, CostEstimate] = {}

        for m_id in topo_order:
            estimate = self.estimate_milestone(milestone_by_id[m_id])
            milestone_estimates.append((m_id, estimate))
            estimates_map[m_id] = estimate

        # 4. Critical Path Dynamic Programming (Longest Path by wall-time)
        dp_time: Dict[str, float] = {m: estimates_map[m].predicted_wall_time_s for m in topo_order}
        parent: Dict[str, Optional[str]] = {m: None for m in topo_order}

        for u in topo_order:
            for v in adj[u]:
                cand_time = dp_time[u] + estimates_map[v].predicted_wall_time_s
                if cand_time > dp_time[v]:
                    dp_time[v] = cand_time
                    parent[v] = u

        if dp_time:
            end_node = max(dp_time.keys(), key=lambda k: dp_time[k])
            crit_path: List[str] = []
            curr_node: Optional[str] = end_node
            while curr_node is not None:
                crit_path.append(curr_node)
                curr_node = parent[curr_node]
            crit_path.reverse()

            critical_path_wall_time_s = sum(estimates_map[m].predicted_wall_time_s for m in crit_path)
            critical_path_total_tokens = sum(estimates_map[m].predicted_tokens for m in crit_path)
            self._last_critical_path_ids = set(crit_path)
        else:
            critical_path_wall_time_s = 0.0
            critical_path_total_tokens = 0
            self._last_critical_path_ids = set()

        # 5. Account Allocation (Weighted Round-Robin by remaining quota)
        per_account_drawdown: Dict[str, int] = {acc: 0 for acc in accounts}
        self._last_assignments = {}

        for m_id, estimate in milestone_estimates:
            # Pick account with maximum available headroom after current drawdown
            chosen_acc = max(
                accounts.keys(),
                key=lambda acc: accounts[acc] - per_account_drawdown[acc],
            )
            self._last_assignments[m_id] = chosen_acc
            per_account_drawdown[chosen_acc] += estimate.predicted_tokens

        # 6. Budget Feasibility Score
        total_available_tokens = sum(accounts.values())
        if total_available_tokens <= 0:
            budget_feasibility_score = 0.0
        else:
            score = (total_available_tokens - critical_path_total_tokens) / float(total_available_tokens)
            budget_feasibility_score = max(0.0, min(1.0, score))

        # 7. Render ASCII Gantt Chart
        gantt_ascii = self._generate_ascii_gantt(
            topo_order=topo_order,
            estimates_map=estimates_map,
            preds=preds,
            crit_path_ids=self._last_critical_path_ids,
        )

        return PipelineForecast(
            milestone_estimates=milestone_estimates,
            critical_path_total_tokens=critical_path_total_tokens,
            critical_path_wall_time_s=round(critical_path_wall_time_s, 2),
            budget_feasibility_score=round(budget_feasibility_score, 4),
            per_account_drawdown=per_account_drawdown,
            gantt_ascii=gantt_ascii,
        )

    def _generate_ascii_gantt(
        self,
        topo_order: List[str],
        estimates_map: Dict[str, CostEstimate],
        preds: Dict[str, List[str]],
        crit_path_ids: Set[str],
        width: int = 40,
    ) -> str:
        """Constructs an ASCII Gantt timeline using unicode block characters."""
        if not topo_order:
            return ""

        # Calculate earliest start & finish offsets
        start_offsets: Dict[str, float] = {}
        end_offsets: Dict[str, float] = {}

        for m_id in topo_order:
            pred_ends = [end_offsets[p] for p in preds.get(m_id, []) if p in end_offsets]
            s = max(pred_ends) if pred_ends else 0.0
            start_offsets[m_id] = s
            end_offsets[m_id] = s + estimates_map[m_id].predicted_wall_time_s

        total_span = max(end_offsets.values()) if end_offsets else 1.0
        if total_span <= 0:
            total_span = 1.0

        lines = []
        header_title = "Milestone Task"
        lines.append(f"{header_title:<34} |{'Timeline Progress':^{width}}| Span (s)")
        lines.append(f"{'-'*34}-+-{'-'*width}-+-{'-'*10}")

        for m_id in topo_order:
            s = start_offsets[m_id]
            e = end_offsets[m_id]
            is_crit = m_id in crit_path_ids

            s_col = min(width - 1, int((s / total_span) * width))
            e_col = max(s_col + 1, min(width, int((e / total_span) * width)))
            bar_len = e_col - s_col

            # Block character styling: █ for critical path, ▓ for parallel
            char = "█" if is_crit else "▓"
            bar_str = (" " * s_col) + (char * bar_len) + (" " * (width - e_col))

            crit_marker = "⚡" if is_crit else " "
            display_name = f"{crit_marker} {m_id[:31]}"
            lines.append(f"{display_name:<34} |{bar_str}| {s:>4.1f} - {e:>4.1f}s")

        # Time axis bottom
        lines.append(f"{'-'*34}-+-{'-'*width}-+-{'-'*10}")
        axis_label = f"0.0s{' ' * (width - 10)}{total_span:.1f}s"
        lines.append(f"{'Total Span: ' + f'{total_span:.1f}s':<34} |{axis_label:^{width}}|")

        return "\n".join(lines)

    def get_forecast(
        self,
        pipeline_state: Optional[List[dict]] = None,
        quota_state: Optional[dict] = None,
        dependency_edges: Optional[List[tuple]] = None,
    ) -> PipelineForecast:
        """Exposes forecast API consumed by autonomous orchestrator.

        If pipeline_state is omitted, attempts to load pending/ready milestones
        from dashboards/repositories.json.
        """
        pending = pipeline_state
        if pending is None:
            pending = self._load_pending_from_repositories()

        edges = dependency_edges or []
        return self.forecast_pipeline(pending, edges, quota_state)

    def _load_pending_from_repositories(self) -> List[dict]:
        """Loads pending/ready milestones from dashboards/repositories.json."""
        repos_file = Path("dashboards/repositories.json")
        if not repos_file.is_file():
            repos_file = Path("/home/ajf/automation/dashboards/repositories.json")

        collected: List[dict] = []
        if repos_file.is_file():
            try:
                with open(repos_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                for repo in data.get("repositories", []):
                    r_id = repo.get("id", "repo")
                    r_root = repo.get("path", "")
                    for m in repo.get("milestones", []):
                        if m.get("status") in ("ready", "pending", "in_progress"):
                            m_copy = dict(m)
                            m_copy["repository"] = r_id
                            m_copy["repo_root"] = r_root
                            collected.append(m_copy)
            except Exception:
                pass
        return collected

    def render_dashboard(
        self,
        forecast: PipelineForecast,
        assigned_accounts: Optional[Dict[str, str]] = None,
    ) -> str:
        """Renders Markdown table + ASCII Gantt from a PipelineForecast.

        Enforces budget feasibility thresholds:
        - COMFORTABLE > 0.7
        - TIGHT in [0.3, 0.7]
        - INFEASIBLE < 0.3
        """
        score = forecast.budget_feasibility_score
        if score > self.FEASIBILITY_COMFORTABLE:
            feasibility_badge = "🟢 COMFORTABLE"
            feasibility_desc = "Optimal headroom. Pipeline execution fully sustainable across accounts."
        elif score >= self.FEASIBILITY_TIGHT:
            feasibility_badge = "🟡 TIGHT"
            feasibility_desc = "Constrained quota headroom. Pacing or milestone deferral recommended."
        else:
            feasibility_badge = "🔴 INFEASIBLE"
            feasibility_desc = "Quota deficit alert. Critical path tokens exceed safe reserve threshold."

        assignments = assigned_accounts or self._last_assignments

        md_lines = [
            "# 📊 Predictive Capacity & Token Budget Forecast",
            "",
            f"**Budget Feasibility Score:** {score:.1%} ({feasibility_badge})  ",
            f"**Feasibility Assessment:** {feasibility_desc}  ",
            f"**Critical-Path Tokens:** `{forecast.critical_path_total_tokens:,}`  ",
            f"**Critical-Path Wall-Time:** `{forecast.critical_path_wall_time_s:.1f}s` ({round(forecast.critical_path_wall_time_s / 60.0, 1)} min)  ",
            "",
            "---",
            "",
            "## 💼 Fleet Token Quota Drawdown Schedule",
            "",
            "| Account | Assigned Drawdown | Status |",
            "| :--- | :---: | :---: |",
        ]

        for acc, drawdown in forecast.per_account_drawdown.items():
            stat_icon = "🟢 NOMINAL" if drawdown < 400000 else ("🟡 HEAVY" if drawdown < 500000 else "🔴 SATURATED")
            md_lines.append(f"| `{acc}` | {drawdown:,} tokens | {stat_icon} |")

        md_lines.extend([
            "",
            "---",
            "",
            "## 📋 Milestone Capacity Schedule",
            "",
            "| Milestone | Predicted Tokens | Wall-Time | Error Prob | Account | Status |",
            "| :--- | :---: | :---: | :---: | :---: | :---: |",
        ])

        for m_id, est in forecast.milestone_estimates:
            acc = assignments.get(m_id, "unassigned")
            is_crit = m_id in self._last_critical_path_ids

            if is_crit:
                status_str = "⚡ CRITICAL PATH"
            elif est.error_probability > 0.25:
                status_str = "⚠️ HIGH RISK"
            else:
                status_str = "✅ FEASIBLE"

            md_lines.append(
                f"| **{m_id}** | {est.predicted_tokens:,} | {est.predicted_wall_time_s:.1f}s | "
                f"{est.error_probability * 100:.1f}% | `{acc}` | {status_str} |"
            )

        md_lines.extend([
            "",
            "---",
            "",
            "## ⏱️ Critical-Path Execution Gantt Chart",
            "",
            "```text",
            forecast.gantt_ascii,
            "```",
            "",
            "---",
            "*Legend: `⚡` = Critical Path item (`█`), `▓` = Parallel / Buffered milestone.*",
            "",
        ])

        return "\n".join(md_lines)

    def write_dashboard(
        self,
        forecast: PipelineForecast,
        output_path: str = "dashboards/CAPACITY_FORECAST.md",
    ) -> str:
        """Renders the capacity forecast dashboard and writes it atomically to disk."""
        content = self.render_dashboard(forecast)
        out_p = Path(output_path)
        out_p.parent.mkdir(parents=True, exist_ok=True)
        out_p.write_text(content, encoding="utf-8")
        return content


# ============================================================================
# Main CLI Execution
# ============================================================================

if __name__ == "__main__":
    print("[*] Initializing CapacityForecaster with default state paths...")
    forecaster = CapacityForecaster()

    # Load pending milestones from repositories.json or build demonstration set
    pending = forecaster._load_pending_from_repositories()
    if not pending:
        # Fallback sample milestones if repositories.json has no pending items
        pending = [
            {
                "name": "Predictive Capacity Forecasting & Token Budget Planner",
                "repository": "hermes_orchestrator",
                "relative_filepath": "tasks/capacity_forecaster.py",
                "risk_modifier": 1.0,
            },
            {
                "name": "Provenance-Linked Rollback Orchestrator & Cascading Recovery Engine",
                "repository": "hermes_orchestrator",
                "relative_filepath": "tasks/rollback_orchestrator.py",
                "risk_modifier": 1.2,
            },
            {
                "name": "Literature Survey Section: Social Navigation & Pedestrian Trajectory",
                "repository": "phd_proposal",
                "relative_filepath": "proposal/chapters/02_literature_survey.tex",
                "risk_modifier": 1.0,
            },
            {
                "name": "ROS 2 Occupancy Grid to Gazebo World Generator",
                "repository": "map2gz",
                "relative_filepath": "map2gz/converter.py",
                "risk_modifier": 1.1,
            },
        ]

    # Sample dependencies linking capacity forecaster -> rollback orchestrator
    sample_edges = [
        (
            "Predictive Capacity Forecasting & Token Budget Planner",
            "Provenance-Linked Rollback Orchestrator & Cascading Recovery Engine",
        )
    ]

    print(f"[*] Forecasting capacity for {len(pending)} pending milestone(s)...")
    forecast = forecaster.forecast_pipeline(
        pending_milestones=pending,
        dependency_edges=sample_edges,
    )

    dashboard_path = "dashboards/CAPACITY_FORECAST.md"
    forecaster.write_dashboard(forecast, output_path=dashboard_path)
    print(f"[+] Wrote capacity forecast dashboard to {dashboard_path}")

    print("\n" + forecaster.render_dashboard(forecast))
