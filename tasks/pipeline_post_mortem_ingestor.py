#!/usr/bin/env python3
"""
Pipeline Post-Mortem Ingestor & Pedagogical Lesson Dispatcher
=============================================================
Autonomic pipeline component connecting failed task registries with the
PostMortemPedagogyEngine. Ingests unanalyzed execution failures, normalizes
stack traces, deduplicates failure signatures, categorizes anti-patterns,
and records architectural pedagogical lessons atomically.

Module: tasks.pipeline_post_mortem_ingestor
Milestone: Scaffold: Autonomic Post-Mortem Ingestion Pipeline & Lesson Dispatcher
"""

import argparse
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import sys
import tempfile
import time
from typing import Any, Dict, List, Optional, Set, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [PostMortemIngestor] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("PostMortemIngestor")

# ANSI escape sequence stripper regex
ANSI_ESCAPE_RE = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
HEX_MEMORY_ADDR_RE = re.compile(r"0x[0-9a-fA-F]{4,16}")
TIMESTAMP_RE = re.compile(r"\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?")

# Try importing PostMortemPedagogyEngine with fallback for standalone/test environments
try:
    from dashboards.post_mortem_pedagogy import PostMortemPedagogyEngine
except ImportError:
    logger.warning("dashboards.post_mortem_pedagogy not found in PYTHONPATH; initializing built-in fallback engine.")

    class PostMortemPedagogyEngine:
        """Robust fallback pedagogy engine implementing standard lesson recording."""

        def __init__(self, lessons_path: Optional[Path] = None):
            self.lessons_path = Path(lessons_path) if lessons_path else Path("dashboards/ARCHITECTURAL_LESSONS.md")
            self.recorded_lessons: List[Dict[str, Any]] = []

        def analyze_failure(self, task_id: str, error_trace: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
            context = context or {}
            anti_pattern = "UNKNOWN_RUNTIME_EXCEPTION"
            lower_trace = error_trace.lower()

            if "rate limit" in lower_trace or "429" in lower_trace:
                anti_pattern = "UNTHROTTLED_TOKEN_BURST"
            elif "timeout" in lower_trace or "deadline exceeded" in lower_trace:
                anti_pattern = "BOUNDLESS_SYNCHRONOUS_BLOCK"
            elif "filenotfound" in lower_trace or "no such file" in lower_trace:
                anti_pattern = "UNRESOLVED_PATH_ANCHOR"
            elif "memoryerror" in lower_trace or "out of memory" in lower_trace:
                anti_pattern = "EXCESSIVE_CONTEXT_ACCUMULATION"
            elif "assertionerror" in lower_trace or "schema" in lower_trace:
                anti_pattern = "CONTRACT_SCHEMA_DIVERGENCE"
            elif "import" in lower_trace or "modulenotfound" in lower_trace:
                anti_pattern = "UNBOUND_DEPENDENCY_DRIFT"

            lesson = {
                "lesson_id": f"LES-{hashlib.sha256(f'{task_id}:{anti_pattern}'.encode()).hexdigest()[:8].upper()}",
                "task_id": task_id,
                "anti_pattern": anti_pattern,
                "signature": context.get("signature", "N/A"),
                "summary": f"Detected {anti_pattern} during execution of {task_id}.",
                "remediation": f"Implement defensive guarding, fallback heuristics, and circuit breakers for {anti_pattern}.",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%SZ", time.gmtime()),
            }
            return lesson

        def record_lesson(self, lesson: Dict[str, Any], dry_run: bool = False) -> bool:
            self.recorded_lessons.append(lesson)
            if dry_run:
                logger.info(f"[DRY-RUN] Staged lesson {lesson['lesson_id']} for anti-pattern {lesson['anti_pattern']}")
                return True

            self.lessons_path.parent.mkdir(parents=True, exist_ok=True)
            markdown_entry = (
                f"\n### [{lesson['lesson_id']}] {lesson['anti_pattern']} ({lesson['timestamp']})\n"
                f"- **Task Reference:** `{lesson['task_id']}`\n"
                f"- **Signature:** `{lesson['signature']}`\n"
                f"- **Root Cause Summary:** {lesson['summary']}\n"
                f"- **Remediation Strategy:** {lesson['remediation']}\n"
            )
            try:
                with open(self.lessons_path, "a", encoding="utf-8") as f:
                    f.write(markdown_entry)
                return True
            except IOError as err:
                logger.error(f"Failed to record lesson to {self.lessons_path}: {err}")
                return False


class PostMortemIngestor:
    """Ingestion, normalization, and pedagogical dispatch orchestrator."""

    def __init__(
        self,
        registry_path: Path,
        lessons_path: Optional[Path] = None,
        limit: int = 100,
        dry_run: bool = False,
    ):
        self.registry_path = Path(registry_path)
        self.lessons_path = Path(lessons_path) if lessons_path else Path("dashboards/ARCHITECTURAL_LESSONS.md")
        self.limit = limit
        self.dry_run = dry_run
        self.engine = PostMortemPedagogyEngine(lessons_path=self.lessons_path)
        self.processed_signatures: Set[str] = set()

    @staticmethod
    def strip_ansi(text: str) -> str:
        """Strip ANSI color codes and control sequences from traceback text."""
        return ANSI_ESCAPE_RE.sub("", text)

    @classmethod
    def normalize_traceback(cls, raw_trace: str) -> Tuple[str, str]:
        """Normalize stack traces to eliminate volatile memory addresses, line variances,

        and timestamps, generating a deterministic SHA-256 failure signature.
        """
        clean_text = cls.strip_ansi(raw_trace)
        # Scrub transient timestamps and pointers
        normalized = TIMESTAMP_RE.sub("<TIMESTAMP>", clean_text)
        normalized = HEX_MEMORY_ADDR_RE.sub("0x<ADDR>", normalized)

        # Normalize line formatting and whitespace
        lines = [line.strip() for line in normalized.splitlines() if line.strip()]
        canonical_representation = "\n".join(lines)

        # Compute deterministic signature
        signature = hashlib.sha256(canonical_representation.encode("utf-8")).hexdigest()
        return canonical_representation, signature

    def load_registry(self) -> List[Dict[str, Any]]:
        """Load failed task incidents from JSON registry or Python fallback."""
        if not self.registry_path.exists():
            logger.warning(f"Failure registry path does not exist: {self.registry_path}")
            return []

        try:
            with open(self.registry_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                # Common registry schema formats: {'tasks': [...]}, {'failures': [...]}, {'data': [...]}
                for key in ("tasks", "failures", "failed_tasks", "records", "data"):
                    if key in data and isinstance(data[key], list):
                        return data[key]
                # If dictionary is keyed by task_id
                return [{"task_id": k, **v} if isinstance(v, dict) else {"task_id": k, "error": str(v)} for k, v in data.items()]
            else:
                logger.error(f"Unexpected registry structure in {self.registry_path}: {type(data)}")
                return []
        except Exception as err:
            logger.error(f"Error parsing failure registry {self.registry_path}: {err}")
            return []

    def run_pipeline(self) -> Dict[str, Any]:
        """Execute full ingest, normalization, deduplication, pedagogy analysis,

        and lesson recording cycle.
        """
        start_time = time.time()
        raw_records = self.load_registry()
        logger.info(f"Loaded {len(raw_records)} total records from {self.registry_path}")

        metrics: Dict[str, Any] = {
            "total_ingested": len(raw_records),
            "analyzed_count": 0,
            "deduplicated_count": 0,
            "skipped_already_analyzed": 0,
            "anti_pattern_counts": {},
            "generated_lessons": [],
            "dry_run": self.dry_run,
            "duration_seconds": 0.0,
            "status": "SUCCESS",
        }

        unique_incidents: List[Tuple[Dict[str, Any], str, str]] = []

        for record in raw_records:
            if metrics["analyzed_count"] >= self.limit:
                logger.info(f"Reached processing limit of {self.limit} tasks.")
                break

            # Skip records already tagged as analyzed in the registry
            if record.get("analyzed") is True or record.get("status") == "analyzed":
                metrics["skipped_already_analyzed"] += 1
                continue

            task_id = record.get("task_id") or record.get("id") or f"TASK-{metrics['analyzed_count'] + 1}"
            raw_trace = (
                record.get("traceback")
                or record.get("error_trace")
                or record.get("error")
                or record.get("message")
                or "Unknown failure"
            )

            canonical_trace, signature = self.normalize_traceback(str(raw_trace))

            # Deduplication: check if signature has been encountered in this run
            if signature in self.processed_signatures:
                metrics["deduplicated_count"] += 1
                continue

            self.processed_signatures.add(signature)
            unique_incidents.append((record, canonical_trace, signature))

        logger.info(f"Identified {len(unique_incidents)} unique unanalyzed failure signatures.")

        # Batch-process incidents with pedagogy engine
        for record, canonical_trace, signature in unique_incidents:
            task_id = record.get("task_id") or record.get("id") or "UNKNOWN_TASK"
            context = {
                "signature": signature,
                "metadata": record.get("metadata", {}),
                "timestamp": record.get("timestamp"),
            }

            lesson = self.engine.analyze_failure(
                task_id=task_id,
                error_trace=canonical_trace,
                context=context,
            )

            # Record lesson
            success = self.engine.record_lesson(lesson, dry_run=self.dry_run)
            if success:
                metrics["analyzed_count"] += 1
                anti_pattern = lesson.get("anti_pattern", "UNKNOWN")
                metrics["anti_pattern_counts"][anti_pattern] = metrics["anti_pattern_counts"].get(anti_pattern, 0) + 1
                metrics["generated_lessons"].append(lesson)
                record["analyzed"] = True
                record["lesson_id"] = lesson.get("lesson_id")

        # Atomic registry update if not in dry-run mode and items were analyzed
        if not self.dry_run and metrics["analyzed_count"] > 0:
            self._sync_registry(raw_records)

        metrics["duration_seconds"] = round(time.time() - start_time, 4)
        logger.info(
            f"Pipeline completed: {metrics['analyzed_count']} analyzed, "
            f"{metrics['deduplicated_count']} deduplicated, "
            f"{metrics['skipped_already_analyzed']} skipped in {metrics['duration_seconds']}s"
        )
        return metrics

    def _sync_registry(self, records: List[Dict[str, Any]]) -> None:
        """Atomically persist updated failure records with analysis metadata."""
        temp_file = self.registry_path.with_suffix(".tmp")
        try:
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(records, f, indent=2)
            temp_file.replace(self.registry_path)
            logger.info(f"Atomically updated failure registry at {self.registry_path}")
        except Exception as err:
            logger.error(f"Failed to atomically update registry {self.registry_path}: {err}")
            if temp_file.exists():
                temp_file.unlink()


def parse_args(args: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Autonomous Post-Mortem Ingestion Pipeline and Pedagogical Lesson Dispatcher."
    )
    parser.add_argument(
        "--registry-path",
        type=str,
        default="dashboards/failed_tasks_registry.json",
        help="Path to failure records JSON registry.",
    )
    parser.add_argument(
        "--lessons-path",
        type=str,
        default="dashboards/ARCHITECTURAL_LESSONS.md",
        help="Path to architectural lessons markdown file.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum number of failure records to process in a single batch.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform normalization and analysis without persisting changes to disk.",
    )
    parser.add_argument(
        "--output-metrics",
        type=str,
        default=None,
        help="Optional path to write structured execution metrics JSON.",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Execute self-contained unit tests and validation checks.",
    )
    return parser.parse_args(args)


def run_self_tests() -> int:
    """Execute end-to-end self-tests covering normalization, deduplication,

    pedagogy engine integration, and atomic sync.
    """
    logger.info("Executing self-test suite for Pipeline Post-Mortem Ingestor...")

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        test_registry = tmp_path / "failed_tasks_registry.json"
        test_lessons = tmp_path / "ARCHITECTURAL_LESSONS.md"
        metrics_file = tmp_path / "metrics.json"

        # 1. Test ANSI code stripping and normalization
        raw_trace_1 = "\x1b[31mException in task:\x1b[0m HTTP 429 Rate limit exceeded at 0x7f8841a2bc00"
        raw_trace_2 = "Exception in task: HTTP 429 Rate limit exceeded at 0x7f8841a5df20"

        norm_1, sig_1 = PostMortemIngestor.normalize_traceback(raw_trace_1)
        norm_2, sig_2 = PostMortemIngestor.normalize_traceback(raw_trace_2)

        assert "\x1b[31m" not in norm_1, "ANSI escape sequences were not removed"
        assert "0x<ADDR>" in norm_1, "Memory address was not scrubbed"
        assert sig_1 == sig_2, f"Expected identical signatures for duplicate traces, got {sig_1} vs {sig_2}"
        logger.info("Test 1: Traceback normalization and signature hashing passed.")

        # 2. Test mock failure records with duplicate signatures and varying errors
        sample_records = [
            {
                "task_id": "TASK-101",
                "error": raw_trace_1,
                "analyzed": False,
                "timestamp": "2026-10-28T12:00:00Z",
            },
            {
                "task_id": "TASK-102",
                "error": raw_trace_2,  # Duplicate of TASK-101
                "analyzed": False,
                "timestamp": "2026-10-28T12:01:00Z",
            },
            {
                "task_id": "TASK-103",
                "error": "FileNotFoundError: [Errno 2] No such file or directory: 'configs/model.yaml'",
                "analyzed": False,
                "timestamp": "2026-10-28T12:02:00Z",
            },
            {
                "task_id": "TASK-104",
                "error": "TimeoutError: Synchronous lock acquisition deadline exceeded after 30s",
                "analyzed": True,  # Already analyzed, should be skipped
                "timestamp": "2026-10-28T11:59:00Z",
            },
        ]

        with open(test_registry, "w", encoding="utf-8") as f:
            json.dump(sample_records, f, indent=2)

        # 3. Execute ingestor in dry-run mode
        dry_ingestor = PostMortemIngestor(
            registry_path=test_registry,
            lessons_path=test_lessons,
            limit=10,
            dry_run=True,
        )
        dry_metrics = dry_ingestor.run_pipeline()
        assert dry_metrics["analyzed_count"] == 2, f"Expected 2 analyzed, got {dry_metrics['analyzed_count']}"
        assert dry_metrics["deduplicated_count"] == 1, f"Expected 1 deduplicated, got {dry_metrics['deduplicated_count']}"
        assert dry_metrics["skipped_already_analyzed"] == 1, f"Expected 1 skipped, got {dry_metrics['skipped_already_analyzed']}"
        assert not test_lessons.exists(), "Dry-run should not write lessons file"
        logger.info("Test 2: Dry-run execution and metric counting passed.")

        # 4. Execute ingestor in write mode
        live_ingestor = PostMortemIngestor(
            registry_path=test_registry,
            lessons_path=test_lessons,
            limit=10,
            dry_run=False,
        )
        live_metrics = live_ingestor.run_pipeline()
        assert live_metrics["analyzed_count"] == 2
        assert test_lessons.exists(), "Lessons file was not created"

        # Verify markdown content written
        with open(test_lessons, "r", encoding="utf-8") as f:
            content = f.read()
            assert "UNTHROTTLED_TOKEN_BURST" in content
            assert "UNRESOLVED_PATH_ANCHOR" in content
        logger.info("Test 3: Live execution and pedagogical markdown generation passed.")

        # 5. Verify registry persistence state
        with open(test_registry, "r", encoding="utf-8") as f:
            updated_records = json.load(f)
            assert updated_records[0].get("analyzed") is True
            assert updated_records[0].get("lesson_id") is not None
        logger.info("Test 4: Atomic registry state sync passed.")

        # 6. Verify idempotent second run
        rerun_ingestor = PostMortemIngestor(
            registry_path=test_registry,
            lessons_path=test_lessons,
            limit=10,
            dry_run=False,
        )
        rerun_metrics = rerun_ingestor.run_pipeline()
        assert rerun_metrics["analyzed_count"] == 0
        assert rerun_metrics["skipped_already_analyzed"] >= 2
        logger.info("Test 5: Idempotent rerun check passed.")

    logger.info("All unit tests and safety assertions passed successfully.")
    return 0


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()

    if args.test or len(sys.argv) == 1:
        # Default to self-test when run directly without custom args or with --test
        return run_self_tests()

    ingestor = PostMortemIngestor(
        registry_path=Path(args.registry_path),
        lessons_path=Path(args.lessons_path),
        limit=args.limit,
        dry_run=args.dry_run,
    )

    metrics = ingestor.run_pipeline()

    if args.output_metrics:
        out_path = Path(args.output_metrics)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2)
        logger.info(f"Exported metrics to {out_path}")

    # Print summary JSON to stdout
    print(json.dumps(metrics, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())