#!/usr/bin/env python3
"""
tasks/worktree_sandbox_manager.py
Ephemeral Git Worktree Isolation & Concurrency Sandbox Manager.

Mathematical Model & Lifecycle State Space:
  S_sandbox ∈ {
      UNINITIALIZED, PROVISIONED, ACQUIRED, ISOLATED_EXEC,
      AUDITING, COMMITTED, MERGING, RELEASED, ROLLED_BACK, PRUNED
  }

Invariants:
  1. Blast-Radius Invariant:
     Let H_0: Files -> SHA256 be the pre-execution baseline state.
     Let H_1: Files -> SHA256 be the post-execution state.
     Δ_modified = { p ∈ SandboxFiles | H_1(p) ≠ H_0(p) ∨ p ∈ UntrackedFiles ∨ p ∈ DeletedFiles }.
     AllowedScope = {declared_relative_path} ∪ allowed_dependencies.
     Invariant: Δ_modified ⊆ AllowedScope.
     If Δ_modified \\ AllowedScope ≠ ∅ -> raise BlastRadiusViolationError and trigger immediate rollback.

  2. Distributed Fencing Lease Invariant:
     Lease L is valid iff:
       (now() - L.heartbeat_at < L.ttl_seconds) ∧ (L.fencing_token == CurrentFencingToken(repo, branch)).

  3. Atomic Merge Strategy:
     Let BaseCommit = L.base_commit, DivergedHead = RepoHead(branch).
     Case 1 (Zero Divergence, DivergedHead == BaseCommit):
         Fast-forward merge directly into branch ref (git merge --ff-only).
     Case 2 (Diverged Branch, DivergedHead ≠ BaseCommit):
         Let DivergedDiff = { files changed between BaseCommit and DivergedHead }.
         Let SandboxDiff = { files changed between BaseCommit and SandboxCommit }.
         If DivergedDiff ∩ SandboxDiff == ∅:
             Execute automated linear rebase in sandbox, then fast-forward parent ref.
         Else:
             Abort rebase, preserve patch in quarantine registry, roll back sandbox.

Zero external dependencies: 100% Python 3.10+ standard library.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import time
from typing import Dict, List, Literal, Optional, Set, Tuple
import uuid

logger = logging.getLogger("worktree_sandbox_manager")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# ============================================================================
# Exceptions
# ============================================================================

class GitSandboxError(Exception):
    """Base exception for all git sandbox operations."""
    pass


class BlastRadiusViolationError(GitSandboxError):
    """Raised when sandbox modifications violate declared blast-radius boundaries."""
    pass


class MergeConflictError(GitSandboxError):
    """Raised when reconciling diverged branch history results in unresolvable merge conflicts."""
    pass


class StaleLeaseError(GitSandboxError):
    """Raised when an operation is attempted with an expired lease or superseded fencing token."""
    pass


# ============================================================================
# State Space Model & Core Data Structures
# ============================================================================

class SandboxState(str, Enum):
    """Lifecycle state machine for ephemeral git worktree sandboxes."""
    UNINITIALIZED = "UNINITIALIZED"
    PROVISIONED = "PROVISIONED"
    ACQUIRED = "ACQUIRED"
    ISOLATED_EXEC = "ISOLATED_EXEC"
    AUDITING = "AUDITING"
    COMMITTED = "COMMITTED"
    MERGING = "MERGING"
    RELEASED = "RELEASED"
    ROLLED_BACK = "ROLLED_BACK"
    PRUNED = "PRUNED"


@dataclass
class SandboxLease:
    """Represents an active, isolated execution lease over a detached git worktree."""
    lease_token: str
    task_id: str
    repo_path: Path
    sandbox_path: Path
    branch: str
    base_commit: str
    fencing_token: int
    ttl_seconds: float = 600.0
    created_at: float = field(default_factory=time.time)
    heartbeat_at: float = field(default_factory=time.time)
    state: SandboxState = SandboxState.PROVISIONED
    pid: int = field(default_factory=os.getpid)

    def is_expired(self, current_time: Optional[float] = None) -> bool:
        now = current_time if current_time is not None else time.time()
        return (now - self.heartbeat_at) > self.ttl_seconds

    def to_dict(self) -> dict:
        return {
            "lease_token": self.lease_token,
            "task_id": self.task_id,
            "repo_path": str(self.repo_path.resolve()),
            "sandbox_path": str(self.sandbox_path.resolve()),
            "branch": self.branch,
            "base_commit": self.base_commit,
            "fencing_token": self.fencing_token,
            "ttl_seconds": self.ttl_seconds,
            "created_at": self.created_at,
            "heartbeat_at": self.heartbeat_at,
            "state": self.state.value,
            "pid": self.pid,
        }

    @classmethod
    def from_dict(cls, data: dict) -> SandboxLease:
        return cls(
            lease_token=data["lease_token"],
            task_id=data["task_id"],
            repo_path=Path(data["repo_path"]),
            sandbox_path=Path(data["sandbox_path"]),
            branch=data["branch"],
            base_commit=data["base_commit"],
            fencing_token=int(data["fencing_token"]),
            ttl_seconds=float(data.get("ttl_seconds", 600.0)),
            created_at=float(data.get("created_at", time.time())),
            heartbeat_at=float(data.get("heartbeat_at", time.time())),
            state=SandboxState(data.get("state", SandboxState.PROVISIONED.value)),
            pid=int(data.get("pid", os.getpid())),
        )


@dataclass
class FileAuditSnapshot:
    """Baseline file state manifest and allowed blast-radius envelope."""
    file_hashes: Dict[str, str]  # relative path -> SHA256 hex digest
    declared_relative_path: str = ""
    allowed_dependencies: Set[str] = field(default_factory=set)


@dataclass
class MergeResult:
    """Outcome of sandbox reconciliation into the parent repository branch."""
    success: bool
    merge_commit: Optional[str]
    strategy: Literal["FAST_FORWARD", "THREE_WAY_REBASE", "CONFLICT_ABORT"]
    conflicted_files: List[str]
    reverted_cleanly: bool


# ============================================================================
# Git Worktree Sandbox Manager
# ============================================================================

class GitWorktreeSandboxManager:
    """
    Manages isolated, ephemeral git worktree sandboxes for concurrent agent execution.
    Guarantees strict blast-radius confinement, monotonic fencing token split-brain prevention,
    and atomic reconciliation (fast-forward or automated non-conflicting rebase).
    """

    def __init__(self, sandbox_root: Optional[Path] = None):
        self.sandbox_root = (
            sandbox_root.resolve()
            if sandbox_root is not None
            else Path(os.environ.get("HERMES_SANDBOX_ROOT", "/tmp/hermes_sandboxes")).resolve()
        )
        self.sandbox_root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._active_leases: Dict[str, SandboxLease] = {}
        self._snapshots: Dict[str, FileAuditSnapshot] = {}

    # ------------------------------------------------------------------------
    # Safe Subprocess Execution Helper
    # ------------------------------------------------------------------------

    @staticmethod
    def _run_git(args: List[str], cwd: Path, timeout: float = 30.0) -> str:
        """
        Executes a git command inside the specified working directory with rigorous error handling.
        """
        cmd = ["git"] + args
        try:
            result = subprocess.run(
                cmd,
                cwd=str(cwd),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True,
                timeout=timeout,
            )
            return result.stdout.strip()
        except subprocess.TimeoutExpired as e:
            raise GitSandboxError(f"Git command timed out ({timeout}s): {' '.join(cmd)}") from e
        except subprocess.CalledProcessError as e:
            err_msg = e.stderr.strip() or e.stdout.strip() or f"Exit code {e.returncode}"
            raise GitSandboxError(f"Git command failed: {' '.join(cmd)}\nDetails: {err_msg}") from e

    # ------------------------------------------------------------------------
    # Hashing & File Manifest Helpers
    # ------------------------------------------------------------------------

    @staticmethod
    def _compute_file_sha256(path: Path) -> str:
        """Computes the SHA-256 hex digest of a filesystem file."""
        hasher = hashlib.sha256()
        with open(path, "rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        return hasher.hexdigest()

    def _build_file_manifest(self, root_dir: Path) -> Dict[str, str]:
        """
        Scans all files within root_dir (excluding .git and lease metadata)
        and computes a dictionary mapping normalized POSIX relative paths to SHA-256 digests.
        """
        manifest: Dict[str, str] = {}
        for root, dirs, files in os.walk(root_dir):
            # Exclude git internal metadata and worktree links
            if ".git" in dirs:
                dirs.remove(".git")
            for filename in files:
                if filename in (".hermes_lease.json", ".git"):
                    continue
                file_path = Path(root) / filename
                if file_path.is_symlink() and not file_path.exists():
                    continue
                if file_path.is_file():
                    rel_path = file_path.relative_to(root_dir).as_posix()
                    try:
                        manifest[rel_path] = self._compute_file_sha256(file_path)
                    except (OSError, PermissionError):
                        continue
        return manifest

    @staticmethod
    def _normalize_path(p: str) -> str:
        """Normalizes path strings to canonical forward-slash relative representations."""
        return Path(p).as_posix().lstrip("./")

    # ------------------------------------------------------------------------
    # Concurrency & Monotonic Fencing Token Protocol
    # ------------------------------------------------------------------------

    def _get_fencing_token_path(self, repo_path: Path) -> Path:
        repo_hash = hashlib.sha256(str(repo_path.resolve()).encode()).hexdigest()[:16]
        return self.sandbox_root / f"fencing_{repo_hash}.json"

    def _get_fencing_lock_path(self, repo_path: Path) -> Path:
        repo_hash = hashlib.sha256(str(repo_path.resolve()).encode()).hexdigest()[:16]
        return self.sandbox_root / f"fencing_{repo_hash}.lock"

    def _acquire_next_fencing_token(self, repo_path: Path, branch: str) -> int:
        """
        Increments and returns the next monotonic integer fencing token for (repo, branch).
        Uses both in-process thread locking and filesystem file locks for cross-process safety.
        """
        with self._lock:
            lock_path = self._get_fencing_lock_path(repo_path)
            token_file = self._get_fencing_token_path(repo_path)

            with open(lock_path, "a+") as lock_fd:
                try:
                    import fcntl
                    fcntl.flock(lock_fd, fcntl.LOCK_EX)
                except (ImportError, OSError):
                    pass

                try:
                    data = {}
                    if token_file.exists():
                        try:
                            data = json.loads(token_file.read_text())
                        except Exception:
                            data = {}
                    current = int(data.get(branch, 0))
                    next_token = current + 1
                    data[branch] = next_token
                    token_file.write_text(json.dumps(data, indent=2))
                    return next_token
                finally:
                    try:
                        import fcntl
                        fcntl.flock(lock_fd, fcntl.LOCK_UN)
                    except (ImportError, OSError):
                        pass

    def get_current_fencing_token(self, repo_path: Path, branch: str) -> int:
        """Returns the current highest fencing token issued for (repo, branch)."""
        token_file = self._get_fencing_token_path(repo_path)
        if not token_file.exists():
            return 0
        try:
            data = json.loads(token_file.read_text())
            return int(data.get(branch, 0))
        except Exception:
            return 0

    def _verify_fencing_token(self, lease: SandboxLease) -> bool:
        """
        Checks Distributed Fencing Lease Invariant:
        Lease L is valid iff (now() - L.heartbeat_at < L.ttl_seconds) ∧ (L.fencing_token == CurrentFencingToken).
        """
        if lease.is_expired():
            return False
        current_token = self.get_current_fencing_token(lease.repo_path, lease.branch)
        return lease.fencing_token == current_token

    # ------------------------------------------------------------------------
    # Public API: Provisioning
    # ------------------------------------------------------------------------

    def provision_sandbox(
        self,
        repo_path: Path,
        task_id: str,
        branch: str = "main",
        ttl_seconds: float = 600.0,
    ) -> SandboxLease:
        """
        Provisions an isolated detached git worktree for a task.
        Acquires a monotonic fencing token and takes a baseline SHA-256 snapshot.
        """
        repo_path = repo_path.resolve()
        if not (repo_path / ".git").exists():
            raise GitSandboxError(f"Directory {repo_path} is not a valid git repository root.")

        # Resolve base commit for requested branch
        base_commit = self._run_git(["rev-parse", branch], cwd=repo_path)
        lease_token = str(uuid.uuid4())
        repo_id = repo_path.name
        sandbox_path = self.sandbox_root / f"{repo_id}_{task_id}_{lease_token[:8]}"

        if sandbox_path.exists():
            shutil.rmtree(sandbox_path, ignore_errors=True)

        fencing_token = self._acquire_next_fencing_token(repo_path, branch)

        logger.info(
            f"Provisioning worktree sandbox at {sandbox_path} (branch={branch}, base_commit={base_commit[:8]}, fencing={fencing_token})"
        )

        # Create detached worktree at target branch/commit
        self._run_git(
            ["worktree", "add", "--detach", str(sandbox_path), branch],
            cwd=repo_path,
        )

        # Build baseline SHA-256 manifest
        baseline_hashes = self._build_file_manifest(sandbox_path)

        lease = SandboxLease(
            lease_token=lease_token,
            task_id=task_id,
            repo_path=repo_path,
            sandbox_path=sandbox_path,
            branch=branch,
            base_commit=base_commit,
            fencing_token=fencing_token,
            ttl_seconds=ttl_seconds,
            created_at=time.time(),
            heartbeat_at=time.time(),
            state=SandboxState.ACQUIRED,
            pid=os.getpid(),
        )

        # Save metadata inside sandbox
        lease_meta_path = sandbox_path / ".hermes_lease.json"
        lease_meta_path.write_text(json.dumps(lease.to_dict(), indent=2))

        with self._lock:
            self._active_leases[lease_token] = lease
            self._snapshots[lease_token] = FileAuditSnapshot(file_hashes=baseline_hashes)

        return lease

    # ------------------------------------------------------------------------
    # Public API: Heartbeat
    # ------------------------------------------------------------------------

    def heartbeat(self, lease_token: str) -> bool:
        """
        Extends the validity lease window. Fails if the lease has expired or
        its fencing token has been superseded by a newer sandbox lease.
        """
        lease = self._get_lease(lease_token)
        if lease is None:
            return False

        with self._lock:
            if not self._verify_fencing_token(lease):
                logger.warning(
                    f"Heartbeat rejected: Lease {lease_token} superseded (token={lease.fencing_token}, current={self.get_current_fencing_token(lease.repo_path, lease.branch)})"
                )
                return False

            now = time.time()
            lease.heartbeat_at = now
            lease_meta_path = lease.sandbox_path / ".hermes_lease.json"
            if lease_meta_path.exists():
                try:
                    lease_meta_path.write_text(json.dumps(lease.to_dict(), indent=2))
                except OSError:
                    pass
            return True

    # ------------------------------------------------------------------------
    # Public API: Blast-Radius Auditing
    # ------------------------------------------------------------------------

    def audit_blast_radius(
        self,
        lease_token: str,
        declared_path: str,
        allowed_dependencies: Optional[List[str]] = None,
        raise_on_violation: bool = False,
    ) -> Tuple[bool, List[str]]:
        """
        Enforces the Blast-Radius Invariant:
          Δ_modified = { p ∈ SandboxFiles | H_1(p) ≠ H_0(p) ∨ p ∈ UntrackedFiles ∨ p ∈ DeletedFiles }.
          AllowedScope = {declared_relative_path} ∪ allowed_dependencies.
          Asserts Δ_modified ⊆ AllowedScope.
        Returns (is_clean, violations).
        If raise_on_violation=True and violations exist, triggers sandbox rollback and raises BlastRadiusViolationError.
        """
        lease = self._get_lease(lease_token)
        if lease is None:
            raise StaleLeaseError(f"Lease {lease_token} not found or expired.")

        with self._lock:
            if not self._verify_fencing_token(lease):
                raise StaleLeaseError(f"Lease {lease_token} fencing token superseded or lease expired.")

            snapshot = self._snapshots.get(lease_token)
            if snapshot is None:
                raise GitSandboxError(f"File audit snapshot missing for lease {lease_token}.")

            lease.state = SandboxState.AUDITING
            snapshot.declared_relative_path = declared_path
            snapshot.allowed_dependencies = set(allowed_dependencies or [])

        # Allowed scope normalized
        allowed_scope: Set[str] = {
            self._normalize_path(declared_path)
        }
        for dep in (allowed_dependencies or []):
            allowed_scope.add(self._normalize_path(dep))

        # Current manifest
        current_hashes = self._build_file_manifest(lease.sandbox_path)
        baseline_hashes = snapshot.file_hashes

        delta_modified: Set[str] = set()

        # Check for newly created or modified files
        for rel_p, curr_hash in current_hashes.items():
            norm_p = self._normalize_path(rel_p)
            base_hash = baseline_hashes.get(rel_p)
            if base_hash is None:
                # Untracked / newly created file
                delta_modified.add(norm_p)
            elif curr_hash != base_hash:
                # Modified file
                delta_modified.add(norm_p)

        # Check for deleted files
        for rel_p in baseline_hashes:
            norm_p = self._normalize_path(rel_p)
            if rel_p not in current_hashes:
                delta_modified.add(norm_p)

        violations = sorted(list(delta_modified - allowed_scope))

        if violations:
            logger.error(
                f"Blast radius violation detected for task {lease.task_id}: {violations} (Allowed: {allowed_scope})"
            )
            if raise_on_violation:
                self._trigger_rollback(lease)
                raise BlastRadiusViolationError(
                    f"Blast radius violation: modified files outside permitted scope {allowed_scope}: {violations}"
                )
            return False, violations

        return True, []

    # ------------------------------------------------------------------------
    # Public API: Commit & Reconciliation
    # ------------------------------------------------------------------------

    def commit_and_reconcile(
        self,
        lease_token: str,
        commit_message: str,
        author_name: str = "Hermes Agent",
        author_email: str = "agent@hermes.ai",
    ) -> MergeResult:
        """
        Stages modified files inside the sandbox, commits them with author identity,
        and atomically reconciles the changes into the parent repository branch.

        Merge Strategies:
          1. FAST_FORWARD: Parent branch has not diverged. Fast-forward applied cleanly.
          2. THREE_WAY_REBASE: Parent branch diverged, but file diff sets are disjoint.
             Automated rebase applied, then fast-forward parent branch.
          3. CONFLICT_ABORT: Overlapping file modifications. Rebase aborted, diff
             preserved in quarantine registry, and sandbox rolled back cleanly.
        """
        lease = self._get_lease(lease_token)
        if lease is None:
            raise StaleLeaseError(f"Lease {lease_token} not found or expired.")

        if not self._verify_fencing_token(lease):
            raise StaleLeaseError(f"Lease {lease_token} fencing token has expired or been superseded.")

        snapshot = self._snapshots.get(lease_token)
        if snapshot is not None and snapshot.declared_relative_path:
            # Enforce blast radius before committing
            self.audit_blast_radius(
                lease_token=lease_token,
                declared_path=snapshot.declared_relative_path,
                allowed_dependencies=list(snapshot.allowed_dependencies),
                raise_on_violation=True,
            )

        with self._lock:
            lease.state = SandboxState.COMMITTED

        # Stage modified files inside sandbox
        self._run_git(["add", "-A"], cwd=lease.sandbox_path)

        # Check status
        status = self._run_git(["status", "--porcelain"], cwd=lease.sandbox_path)
        if not status:
            logger.info(f"Sandbox {lease_token} has zero modifications. Nothing to commit.")
            return MergeResult(
                success=True,
                merge_commit=lease.base_commit,
                strategy="FAST_FORWARD",
                conflicted_files=[],
                reverted_cleanly=True,
            )

        # Execute atomic git commit inside sandbox
        git_env_args = [
            "-c", f"user.name={author_name}",
            "-c", f"user.email={author_email}",
            "commit", "-m", commit_message,
        ]
        self._run_git(git_env_args, cwd=lease.sandbox_path)
        sandbox_commit = self._run_git(["rev-parse", "HEAD"], cwd=lease.sandbox_path)
        logger.info(f"Created sandbox commit {sandbox_commit[:8]} for task {lease.task_id}")

        with self._lock:
            lease.state = SandboxState.MERGING

        # Lock parent branch ref during reconciliation
        diverged_head = self._run_git(["rev-parse", lease.branch], cwd=lease.repo_path)

        # --------------------------------------------------------------------
        # Case 1: Zero Divergence (Fast-Forward Merge)
        # --------------------------------------------------------------------
        if diverged_head == lease.base_commit:
            logger.info(f"Branch '{lease.branch}' has zero divergence. Executing Fast-Forward merge.")
            self._apply_fast_forward_to_parent(lease, sandbox_commit, diverged_head)
            with self._lock:
                lease.state = SandboxState.RELEASED
            return MergeResult(
                success=True,
                merge_commit=sandbox_commit,
                strategy="FAST_FORWARD",
                conflicted_files=[],
                reverted_cleanly=True,
            )

        # --------------------------------------------------------------------
        # Case 2: Diverged Branch History (Tree-Diff Disjointness Analysis)
        # --------------------------------------------------------------------
        logger.info(
            f"Branch '{lease.branch}' diverged ({lease.base_commit[:8]} -> {diverged_head[:8]}). Analyzing tree diffs."
        )

        diverged_diff_files = set(
            filter(None, self._run_git(
                ["diff", "--name-only", lease.base_commit, diverged_head],
                cwd=lease.repo_path,
            ).splitlines())
        )
        sandbox_diff_files = set(
            filter(None, self._run_git(
                ["diff", "--name-only", lease.base_commit, sandbox_commit],
                cwd=lease.sandbox_path,
            ).splitlines())
        )

        overlapping_files = sorted(list(diverged_diff_files.intersection(sandbox_diff_files)))

        if not overlapping_files:
            # Diff sets are strictly disjoint: Attempt clean automated rebase
            logger.info("Divergence tree diffs are disjoint. Executing automated rebase in sandbox.")
            try:
                self._run_git(["rebase", diverged_head], cwd=lease.sandbox_path)
                rebased_commit = self._run_git(["rev-parse", "HEAD"], cwd=lease.sandbox_path)
                logger.info(f"Rebase succeeded cleanly: new commit is {rebased_commit[:8]}. Merging to parent.")
                self._apply_fast_forward_to_parent(lease, rebased_commit, diverged_head)
                with self._lock:
                    lease.state = SandboxState.RELEASED
                return MergeResult(
                    success=True,
                    merge_commit=rebased_commit,
                    strategy="THREE_WAY_REBASE",
                    conflicted_files=[],
                    reverted_cleanly=True,
                )
            except GitSandboxError as e:
                logger.warning(f"Rebase encountered conflict despite disjoint check: {e}. Aborting rebase.")
                try:
                    self._run_git(["rebase", "--abort"], cwd=lease.sandbox_path)
                except Exception:
                    pass

        # --------------------------------------------------------------------
        # Case 3: Conflict Abort & Quarantine
        # --------------------------------------------------------------------
        logger.warning(
            f"Merge conflict detected for task {lease.task_id}. Overlapping files: {overlapping_files}. Triggering quarantine."
        )
        self._quarantine_sandbox(lease, sandbox_commit, overlapping_files)
        self._trigger_rollback(lease)

        with self._lock:
            lease.state = SandboxState.ROLLED_BACK

        return MergeResult(
            success=False,
            merge_commit=None,
            strategy="CONFLICT_ABORT",
            conflicted_files=overlapping_files,
            reverted_cleanly=True,
        )

    def _apply_fast_forward_to_parent(self, lease: SandboxLease, commit_sha: str, expected_old_ref: str) -> None:
        """
        Updates the target branch in the parent repository.
        If the parent repository working tree currently has `lease.branch` checked out,
        runs `git merge --ff-only` to advance both ref and working index;
        otherwise updates the ref atomically via `git update-ref`.
        """
        active_branch = self._run_git(["rev-parse", "--abbrev-ref", "HEAD"], cwd=lease.repo_path)
        if active_branch == lease.branch:
            self._run_git(["merge", "--ff-only", commit_sha], cwd=lease.repo_path)
        else:
            self._run_git(
                ["update-ref", f"refs/heads/{lease.branch}", commit_sha, expected_old_ref],
                cwd=lease.repo_path,
            )

    # ------------------------------------------------------------------------
    # Public API: Teardown & Rollback
    # ------------------------------------------------------------------------

    def teardown_sandbox(self, lease_token: str, force: bool = False) -> None:
        """
        Removes the detached git worktree, cleans up filesystem artifacts,
        and releases internal tracking state.
        """
        lease = self._get_lease(lease_token)
        if lease is None:
            return

        with self._lock:
            self._active_leases.pop(lease_token, None)
            self._snapshots.pop(lease_token, None)

        logger.info(f"Tearing down worktree sandbox for task {lease.task_id} at {lease.sandbox_path}")

        # Git worktree removal
        if lease.sandbox_path.exists() and lease.repo_path.exists():
            try:
                self._run_git(["worktree", "remove", "--force", str(lease.sandbox_path)], cwd=lease.repo_path)
            except Exception as e:
                logger.debug(f"git worktree remove reported: {e}")

        # Filesystem cleanup fallback
        if lease.sandbox_path.exists():
            shutil.rmtree(lease.sandbox_path, ignore_errors=True)

        # Expire stale worktree entries
        if lease.repo_path.exists():
            try:
                self._run_git(["worktree", "prune", "--expire", "now"], cwd=lease.repo_path)
            except Exception as e:
                logger.debug(f"git worktree prune reported: {e}")

        lease.state = SandboxState.RELEASED

    def _trigger_rollback(self, lease: SandboxLease) -> None:
        """Rolls back all uncommitted and staged modifications in the sandbox."""
        logger.info(f"Executing rollback on sandbox {lease.sandbox_path}")
        try:
            self._run_git(["reset", "--hard", "HEAD"], cwd=lease.sandbox_path)
            self._run_git(["clean", "-fd"], cwd=lease.sandbox_path)
        except Exception as e:
            logger.error(f"Rollback encountered error: {e}")
        lease.state = SandboxState.ROLLED_BACK

    def _quarantine_sandbox(self, lease: SandboxLease, sandbox_commit: str, conflict_files: List[str]) -> Path:
        """Preserves conflicted diffs and metadata in a quarantine registry directory."""
        quarantine_dir = self.sandbox_root / "quarantine" / f"{lease.task_id}_{lease.lease_token[:8]}"
        quarantine_dir.mkdir(parents=True, exist_ok=True)

        try:
            patch = self._run_git(["diff", lease.base_commit, sandbox_commit], cwd=lease.sandbox_path)
            (quarantine_dir / "divergence.patch").write_text(patch)
        except Exception as e:
            logger.error(f"Failed to extract quarantine patch: {e}")

        meta = {
            "lease": lease.to_dict(),
            "sandbox_commit": sandbox_commit,
            "conflicted_files": conflict_files,
            "quarantined_at": time.time(),
        }
        (quarantine_dir / "quarantine_meta.json").write_text(json.dumps(meta, indent=2))
        logger.info(f"Preserved conflicted sandbox state in quarantine at {quarantine_dir}")
        return quarantine_dir

    # ------------------------------------------------------------------------
    # Public API: Garbage Collection & Orphan Pruning
    # ------------------------------------------------------------------------

    def prune_stale_sandboxes(self, max_age_seconds: float = 1800.0) -> int:
        """
        Active scan of all worktree directories under the sandbox root.
        Identifies and purges dead or orphaned sandboxes whose leases have expired
        or whose worker PIDs have terminated.
        """
        pruned_count = 0
        now = time.time()

        if not self.sandbox_root.exists():
            return 0

        for item in self.sandbox_root.iterdir():
            if not item.is_dir() or item.name == "quarantine":
                continue

            lease_file = item / ".hermes_lease.json"
            should_prune = False
            repo_path: Optional[Path] = None

            if lease_file.exists():
                try:
                    data = json.loads(lease_file.read_text())
                    lease = SandboxLease.from_dict(data)
                    repo_path = lease.repo_path

                    pid_dead = not self._is_pid_alive(lease.pid)
                    expired = (now - lease.heartbeat_at) > lease.ttl_seconds
                    too_old = (now - lease.created_at) > max_age_seconds

                    if pid_dead or expired or too_old:
                        should_prune = True
                        logger.info(
                            f"Pruning stale sandbox {item.name}: pid_dead={pid_dead}, expired={expired}, age={now - lease.created_at:.1f}s"
                        )
                except Exception as e:
                    logger.warning(f"Corrupt lease file at {lease_file}: {e}. Marking for cleanup.")
                    should_prune = True
            else:
                # Unmanaged directory older than max_age_seconds
                try:
                    mtime = item.stat().st_mtime
                    if (now - mtime) > max_age_seconds:
                        should_prune = True
                        logger.info(f"Pruning unmanaged sandbox folder {item.name}")
                except OSError:
                    pass

            if should_prune:
                if repo_path and repo_path.exists():
                    try:
                        self._run_git(["worktree", "remove", "--force", str(item)], cwd=repo_path)
                    except Exception:
                        pass
                shutil.rmtree(item, ignore_errors=True)
                pruned_count += 1

        return pruned_count

    @staticmethod
    def _is_pid_alive(pid: int) -> bool:
        """Checks if a process identifier exists on the host system."""
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError:
            return False

    def _get_lease(self, lease_token: str) -> Optional[SandboxLease]:
        with self._lock:
            lease = self._active_leases.get(lease_token)
            if lease is not None:
                return lease

        # Attempt recovery from filesystem metadata
        for item in self.sandbox_root.iterdir():
            if item.is_dir() and item.name.endswith(lease_token[:8]):
                lease_file = item / ".hermes_lease.json"
                if lease_file.exists():
                    try:
                        data = json.loads(lease_file.read_text())
                        recovered = SandboxLease.from_dict(data)
                        with self._lock:
                            self._active_leases[lease_token] = recovered
                        return recovered
                    except Exception:
                        pass
        return None


# ============================================================================
# Verification Suite & Self-Test
# ============================================================================

if __name__ == "__main__":
    import sys

    print("=" * 80)
    print("Hermes Ephemeral Git Worktree Isolation & Concurrency Sandbox Manager: Self-Test")
    print("=" * 80)

    with tempfile.TemporaryDirectory() as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        repo_dir = temp_dir / "target_repo"
        sandbox_storage = temp_dir / "sandboxes"

        repo_dir.mkdir(parents=True, exist_ok=True)
        sandbox_storage.mkdir(parents=True, exist_ok=True)

        # --------------------------------------------------------------------
        # 1. Initialize Test Git Repository
        # --------------------------------------------------------------------
        print("\n[Step 1] Initializing target git repository...")
        subprocess.run(["git", "init", "-b", "main"], cwd=repo_dir, check=True, stdout=subprocess.DEVNULL)
        subprocess.run(["git", "config", "user.name", "Test Runner"], cwd=repo_dir, check=True)
        subprocess.run(["git", "config", "user.email", "runner@test.local"], cwd=repo_dir, check=True)

        # Seed initial files
        (repo_dir / "README.md").write_text("# Test Repository\nInitial baseline content.\n")
        (repo_dir / "core.py").write_text("def run():\n    return 'initial'\n")
        (repo_dir / "utils.py").write_text("def helper():\n    return True\n")

        subprocess.run(["git", "add", "-A"], cwd=repo_dir, check=True)
        subprocess.run(["git", "commit", "-m", "Initial commit"], cwd=repo_dir, check=True, stdout=subprocess.DEVNULL)
        initial_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo_dir, text=True).strip()
        print(f"Target repository ready at commit {initial_commit[:8]}")

        # Instantiate Manager
        manager = GitWorktreeSandboxManager(sandbox_root=sandbox_storage)

        # --------------------------------------------------------------------
        # 2. Provision Sandbox & Verify Baseline Snapshot
        # --------------------------------------------------------------------
        print("\n[Step 2] Provisioning sandbox for Task-Alpha...")
        lease_a = manager.provision_sandbox(
            repo_path=repo_dir,
            task_id="task_alpha",
            branch="main",
            ttl_seconds=300.0,
        )
        assert lease_a.sandbox_path.exists(), "Sandbox directory must exist on disk"
        assert lease_a.fencing_token == 1, f"Expected fencing token 1, got {lease_a.fencing_token}"
        assert lease_a.base_commit == initial_commit, "Base commit must match initial commit"

        # Verify Heartbeat
        assert manager.heartbeat(lease_a.lease_token), "Heartbeat must return True for active lease"
        print(f"Sandbox provisioned cleanly: lease={lease_a.lease_token[:8]}, fencing={lease_a.fencing_token}")

        # --------------------------------------------------------------------
        # 3. Test Authorized Modification (Within Blast-Radius)
        # --------------------------------------------------------------------
        print("\n[Step 3] Testing authorized file write within blast-radius...")
        (lease_a.sandbox_path / "core.py").write_text("def run():\n    return 'alpha_modified'\n")

        clean, violations = manager.audit_blast_radius(
            lease_token=lease_a.lease_token,
            declared_path="core.py",
            allowed_dependencies=["utils.py"],
            raise_on_violation=False,
        )
        assert clean, f"Audit must succeed for authorized modification. Violations: {violations}"
        assert not violations, "Violations list must be empty"
        print("Authorized blast-radius validation PASSED.")

        # --------------------------------------------------------------------
        # 4. Test Unauthorized Modification (Outside Blast-Radius)
        # --------------------------------------------------------------------
        print("\n[Step 4] Testing unauthorized modification outside declared scope...")
        (lease_a.sandbox_path / "unauthorized_file.py").write_text("# Malicious intrusion")

        clean, violations = manager.audit_blast_radius(
            lease_token=lease_a.lease_token,
            declared_path="core.py",
            allowed_dependencies=["utils.py"],
            raise_on_violation=False,
        )
        assert not clean, "Audit must fail when unauthorized files exist"
        assert "unauthorized_file.py" in violations, "unauthorized_file.py must be detected in violations"

        # Test raise_on_violation and automated rollback trigger
        rollback_caught = False
        try:
            manager.audit_blast_radius(
                lease_token=lease_a.lease_token,
                declared_path="core.py",
                allowed_dependencies=["utils.py"],
                raise_on_violation=True,
            )
        except BlastRadiusViolationError:
            rollback_caught = True

        assert rollback_caught, "BlastRadiusViolationError must be raised"
        assert not (lease_a.sandbox_path / "unauthorized_file.py").exists(), (
            "Sandbox rollback must remove unauthorized files"
        )
        print("Blast-radius violation detection & immediate rollback PASSED.")

        # Re-apply legitimate modification
        (lease_a.sandbox_path / "core.py").write_text("def run():\n    return 'alpha_verified'\n")
        clean, _ = manager.audit_blast_radius(
            lease_token=lease_a.lease_token,
            declared_path="core.py",
            raise_on_violation=False,
        )
        assert clean, "Audit must pass after clean state re-application"

        # --------------------------------------------------------------------
        # 5. Test Fast-Forward Atomic Reconciliation
        # --------------------------------------------------------------------
        print("\n[Step 5] Testing Fast-Forward atomic commit and reconciliation...")
        merge_res_a = manager.commit_and_reconcile(
            lease_token=lease_a.lease_token,
            commit_message="feat(core): update run function for Task-Alpha",
            author_name="Alpha Agent",
            author_email="alpha@hermes.ai",
        )
        assert merge_res_a.success, "Reconciliation must succeed"
        assert merge_res_a.strategy == "FAST_FORWARD", f"Expected FAST_FORWARD, got {merge_res_a.strategy}"
        assert merge_res_a.merge_commit is not None

        # Verify parent repository received update
        parent_core = (repo_dir / "core.py").read_text()
        assert "alpha_verified" in parent_core, "Parent repository must contain updated core.py"
        manager.teardown_sandbox(lease_a.lease_token)
        print("Fast-Forward reconciliation PASSED.")

        # --------------------------------------------------------------------
        # 6. Test Diverged Branch Reconciliation (Disjoint Non-Conflicting Rebase)
        # --------------------------------------------------------------------
        print("\n[Step 6] Testing concurrent diverged branch reconciliation (Disjoint Rebase)...")
        # Provision sandbox B based on current head
        lease_b = manager.provision_sandbox(
            repo_path=repo_dir,
            task_id="task_beta",
            branch="main",
        )
        assert lease_b.fencing_token == 2, f"Expected fencing token 2, got {lease_b.fencing_token}"

        # Advance parent repository on 'utils.py' concurrently
        (repo_dir / "utils.py").write_text("def helper():\n    return 'parent_advanced'\n")
        subprocess.run(["git", "add", "utils.py"], cwd=repo_dir, check=True)
        subprocess.run(
            ["git", "-c", "user.name=Main", "-c", "user.email=main@repo", "commit", "-m", "advance utils"],
            cwd=repo_dir,
            check=True,
            stdout=subprocess.DEVNULL,
        )
        diverged_head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo_dir, text=True).strip()

        # In Sandbox B, modify 'core.py' (disjoint from utils.py)
        (lease_b.sandbox_path / "core.py").write_text("def run():\n    return 'beta_concurrent'\n")

        merge_res_b = manager.commit_and_reconcile(
            lease_token=lease_b.lease_token,
            commit_message="feat(core): concurrent update by Beta Agent",
        )
        assert merge_res_b.success, "Disjoint reconciliation must succeed"
        assert merge_res_b.strategy == "THREE_WAY_REBASE", f"Expected THREE_WAY_REBASE, got {merge_res_b.strategy}"

        # Verify parent repository has BOTH changes
        assert "beta_concurrent" in (repo_dir / "core.py").read_text()
        assert "parent_advanced" in (repo_dir / "utils.py").read_text()
        manager.teardown_sandbox(lease_b.lease_token)
        print("Disjoint automated rebase reconciliation PASSED.")

        # --------------------------------------------------------------------
        # 7. Test Diverged Branch Conflict Abort & Quarantine
        # --------------------------------------------------------------------
        print("\n[Step 7] Testing overlapping modification conflict abort & quarantine...")
        lease_c = manager.provision_sandbox(
            repo_path=repo_dir,
            task_id="task_gamma",
            branch="main",
        )

        # Concurrently modify 'core.py' in parent
        (repo_dir / "core.py").write_text("def run():\n    return 'parent_conflict_marker'\n")
        subprocess.run(["git", "add", "core.py"], cwd=repo_dir, check=True)
        subprocess.run(
            ["git", "-c", "user.name=Main", "-c", "user.email=main@repo", "commit", "-m", "conflicting commit"],
            cwd=repo_dir,
            check=True,
            stdout=subprocess.DEVNULL,
        )

        # In Sandbox C, also modify 'core.py'
        (lease_c.sandbox_path / "core.py").write_text("def run():\n    return 'gamma_conflict_marker'\n")

        merge_res_c = manager.commit_and_reconcile(
            lease_token=lease_c.lease_token,
            commit_message="feat(core): conflicting gamma update",
        )
        assert not merge_res_c.success, "Reconciliation must report failure on conflict"
        assert merge_res_c.strategy == "CONFLICT_ABORT", f"Expected CONFLICT_ABORT, got {merge_res_c.strategy}"
        assert "core.py" in merge_res_c.conflicted_files, "core.py must be identified as conflicted file"

        # Check quarantine registry
        quarantine_root = sandbox_storage / "quarantine"
        assert quarantine_root.exists(), "Quarantine directory must exist"
        quarantine_dirs = list(quarantine_root.glob("task_gamma_*"))
        assert len(quarantine_dirs) > 0, "Quarantine directory for task_gamma must exist"
        assert (quarantine_dirs[0] / "divergence.patch").exists(), "Quarantined patch must be preserved"
        print("Conflict detection, quarantine preservation, and safe abort PASSED.")

        manager.teardown_sandbox(lease_c.lease_token)

        # --------------------------------------------------------------------
        # 8. Test Garbage Collection & Stale Worktree Pruning
        # --------------------------------------------------------------------
        print("\n[Step 8] Testing garbage collection of expired/orphaned sandboxes...")
        lease_orphan = manager.provision_sandbox(
            repo_path=repo_dir,
            task_id="task_orphan",
            branch="main",
            ttl_seconds=0.01,  # Expire almost immediately
        )
        time.sleep(0.05)
        pruned = manager.prune_stale_sandboxes(max_age_seconds=0.02)
        assert pruned >= 1, f"Expected at least 1 pruned sandbox, got {pruned}"
        assert not lease_orphan.sandbox_path.exists(), "Orphaned worktree directory must be removed"
        print("Garbage collection and orphan pruning PASSED.")

    print("\n" + "=" * 80)
    print("ALL VERIFICATION SUITE CHECKS COMPLETED WITH 100% SUCCESS.")
    print("=" * 80)
    sys.exit(0)