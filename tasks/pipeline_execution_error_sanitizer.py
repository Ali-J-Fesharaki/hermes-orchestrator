"""Execution Error Sanitizer & Pre-Ingestion Filter Module.

Autonomic pipeline component for cleaning, normalizing, and categorizing raw execution
failure traces, isolating root exceptions from extraneous logging/terminal wrappers,
and rejecting stub-echo outputs before downstream post-mortem ingestion.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
import json
import logging
import pathlib
import re
import sys
from typing import Any, Dict, List, Optional, Pattern, Tuple

logger = logging.getLogger("execution_error_sanitizer")


# Standard Error Formats Categorization
FORMAT_PYTHON_TRACEBACK = "PYTHON_TRACEBACK"
FORMAT_LATEX_ERROR = "LATEX_ERROR"
FORMAT_SYNTAX_CORRUPTION = "SYNTAX_CORRUPTION"
FORMAT_STUB_HEADER_ECHO = "STUB_HEADER_ECHO"
FORMAT_TIMEOUT_EXHAUSTION = "TIMEOUT_EXHAUSTION"
FORMAT_GIT_OPERATION_ERROR = "GIT_OPERATION_ERROR"
FORMAT_EMPTY_OR_WHITESPACE = "EMPTY_OR_WHITESPACE"
FORMAT_UNKNOWN_ERROR = "UNKNOWN_ERROR"


@dataclass
class SanitizedTrace:
    """Structured representation of a sanitized and classified execution error trace."""

    raw_length: int
    sanitized_text: str
    detected_format: str
    root_exception_type: Optional[str] = None
    root_cause_message: Optional[str] = None
    confidence_score: float = 1.0
    is_actionable: bool = True
    actionable_remediation: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert dataclass to JSON-serializable dictionary."""
        return dataclasses.asdict(self)

    def to_json(self, indent: int = 2) -> str:
        """Serialize dataclass to formatted JSON."""
        return json.dumps(self.to_dict(), indent=indent)


class ExecutionErrorSanitizer:
    """Autonomous sanitizer and classifier for execution error traces.

    Provides defensive log filtering, terminal escape code removal, boundary
    isolation, and pre-ingestion heuristics against degenerate stub echoes.
    """

    # ANSI escape regex: CSI (e.g. \x1b[...m) and OSC (e.g. \x1b]...\x07) sequences
    ANSI_ESCAPE_RE: Pattern[str] = re.compile(
        r"(?:\x1B[@-Z\\-_]|[\x80-\x9A\x9C-\x9F]|(?:\x1B\[|\x9B)[0-?]*[ -/]*[@-~])"
    )

    # Standard timestamp and log-level prefix regexes
    LOG_TIMESTAMP_PREFIX_RE: Pattern[str] = re.compile(
        r"^(?:\[?\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[,\.]\d+)?(?:Z|[+-]\d{2}:?\d{2})?\]?|"
        r"\[?\d{2}:\d{2}:\d{2}(?:[,\.]\d+)?\]?)\s*"
        r"(?:\[?(?:DEBUG|INFO|WARNING|WARN|ERROR|CRITICAL|FATAL)\]?[:\s]*)?",
        re.MULTILINE | re.IGNORECASE,
    )

    # Max trace length threshold for safe memory & regex handling (512 KB)
    MAX_PROCESSING_BUFFER_BYTES: int = 512 * 1024
    TRUNCATION_HEAD_BYTES: int = 128 * 1024
    TRUNCATION_TAIL_BYTES: int = 256 * 1024

    # Pre-compiled error pattern discriminators
    PYTHON_TRACEBACK_RE: Pattern[str] = re.compile(
        r"Traceback\s+\(most\s+recent\s+call\s+last\):\s*\n.*?(?:^(\w+(?:Error|Exception|Interrupt|Exit|Warning|Fault))(?::\s*(.*))?$)",
        re.MULTILINE | re.DOTALL,
    )

    PYTHON_SYNTAX_ERROR_RE: Pattern[str] = re.compile(
        r"^\s*(SyntaxError|IndentationError|TabError):\s*(.*)$",
        re.MULTILINE,
    )

    LATEX_FATAL_RE: Pattern[str] = re.compile(
        r"(?:^!\s+(?:LaTeX Error:\s*(.+)|Undefined control sequence.*|Emergency stop.*|"
        r"Fatal error occurred, no output PDF file produced!.*|.*?line\s+\d+.*))",
        re.MULTILINE | re.IGNORECASE,
    )

    GIT_LOCK_ERROR_RE: Pattern[str] = re.compile(
        r"fatal:\s*(Unable to create '[^']*index\.lock': File exists|"
        r"not a git repository|"
        r"Your local changes to the following files would be overwritten by merge|"
        r"Could not resolve host:.*)",
        re.IGNORECASE,
    )

    TIMEOUT_ERROR_RE: Pattern[str] = re.compile(
        r"(?:TimeoutExpired|Command '.*?' timed out after \d+|Execution timed out|"
        r"timed out after \d+ seconds|SIGKILL: process exceeded timeout limit)",
        re.IGNORECASE,
    )

    def __init__(self, stub_char_threshold: int = 120) -> None:
        """Initialize sanitizer with configurable heuristics threshold."""
        self.stub_char_threshold = stub_char_threshold

    @classmethod
    def strip_ansi_escapes(cls, text: str) -> str:
        """Remove ANSI terminal color, style, and cursor control codes."""
        if not text:
            return ""
        return cls.ANSI_ESCAPE_RE.sub("", text)

    @classmethod
    def strip_timestamps_and_log_prefixes(cls, text: str) -> str:
        """Remove timestamps, log level tags, and syslog-style headers."""
        if not text:
            return ""
        return cls.LOG_TIMESTAMP_PREFIX_RE.sub("", text)

    def is_stub_echo(self, text: str, target_file: str = "") -> bool:
        """Heuristic check to determine if the payload is merely a file path echo or stub header.

        Flags outputs that consist only of target path echoes, docstrings, empty
        markdown fences, or trivial comment banners without functional body code.
        """
        if not text:
            return True

        cleaned = self.strip_ansi_escapes(text).strip()

        # If after stripping markdown formatting and path echoes, meaningful chars < threshold
        # Strip common markdown wrapper markers
        content = re.sub(r"^```[a-zA-Z0-9_-]*", "", cleaned, flags=re.MULTILINE)
        content = re.sub(r"```$", "", content, flags=re.MULTILINE)

        # Remove single-line comments, docstring delimiters, and whitespace
        content_no_comments = re.sub(r"^\s*#.*$", "", content, flags=re.MULTILINE)
        content_no_comments = re.sub(r'"""[\s\S]*?"""|\'\'\'[\s\S]*?\'\'\'', "", content_no_comments)
        stripped_meaningful = re.sub(r"\s+", "", content_no_comments)

        # Check target filename echo
        if target_file:
            target_norm = pathlib.Path(target_file).name.lower()
            cleaned_lower = cleaned.lower()
            # If text only echoes the filename or path in quotes/fences
            echo_patterns = [
                f"# {target_norm}",
                f"// {target_norm}",
                f"tasks/{target_norm}",
                f"{target_norm}",
            ]
            for ep in echo_patterns:
                cleaned_lower = cleaned_lower.replace(ep, "")
            residual = re.sub(r"[#\s`\'\"\-:]+", "", cleaned_lower)
            if len(residual) == 0:
                return True

        return len(stripped_meaningful) < self.stub_char_threshold

    def extract_root_exception(self, trace: str) -> Tuple[Optional[str], Optional[str]]:
        """Isolate root exception name and descriptive message from trace."""
        if not trace:
            return None, None

        # 1. Check Python syntax/indentation errors
        syntax_match = self.PYTHON_SYNTAX_ERROR_RE.search(trace)
        if syntax_match:
            return syntax_match.group(1).strip(), syntax_match.group(2).strip()

        # 2. Check full Python traceback tail
        lines = [line.strip() for line in trace.splitlines() if line.strip()]
        for line in reversed(lines):
            # Look for ExceptionType: Message
            match = re.match(r"^([a-zA-Z_]\w*(?:Error|Exception|Interrupt|Exit|Warning|Fault))(?::\s*(.*))?$", line)
            if match:
                return match.group(1).strip(), (match.group(2) or "").strip()

        # 3. Check Git Errors
        git_match = self.GIT_LOCK_ERROR_RE.search(trace)
        if git_match:
            return "GitOperationError", git_match.group(1).strip()

        # 4. Check LaTeX Errors
        latex_match = self.LATEX_FATAL_RE.search(trace)
        if latex_match:
            err_msg = latex_match.group(0).strip()
            return "LaTeXCompilationError", err_msg

        # 5. Check Timeouts
        timeout_match = self.TIMEOUT_ERROR_RE.search(trace)
        if timeout_match:
            return "TimeoutExpired", timeout_match.group(0).strip()

        return None, None

    def clean_execution_trace(self, raw_trace: Optional[str], target_file: str = "") -> SanitizedTrace:
        """Sanitize raw execution trace, classify failure mode, and determine actionability."""
        if raw_trace is None or not str(raw_trace).strip():
            return SanitizedTrace(
                raw_length=0,
                sanitized_text="",
                detected_format=FORMAT_EMPTY_OR_WHITESPACE,
                root_exception_type=None,
                root_cause_message="Execution output is empty or whitespace only.",
                confidence_score=1.0,
                is_actionable=False,
                actionable_remediation="Inspect process spawning and stdio redirection; command produced zero bytes.",
            )

        raw_str = str(raw_trace)
        raw_len = len(raw_str)

        # Defensive memory bounds: truncate extremely large or runaway log outputs
        if raw_len > self.MAX_PROCESSING_BUFFER_BYTES:
            head = raw_str[: self.TRUNCATION_HEAD_BYTES]
            tail = raw_str[-self.TRUNCATION_TAIL_BYTES :]
            truncated_marker = (
                f"\n... [SANITIZER WARNING: LOG EXCEEDED {self.MAX_PROCESSING_BUFFER_BYTES} BYTES. "
                f"TRUNCATED {raw_len - (self.TRUNCATION_HEAD_BYTES + self.TRUNCATION_TAIL_BYTES)} BYTES] ...\n"
            )
            working_text = head + truncated_marker + tail
        else:
            working_text = raw_str

        # Step 1: Strip ANSI color codes and control sequences
        sanitized = self.strip_ansi_escapes(working_text)

        # Step 2: Evaluate Stub Header / Pre-Ingestion Echo Heuristics
        if self.is_stub_echo(sanitized, target_file=target_file):
            return SanitizedTrace(
                raw_length=raw_len,
                sanitized_text=sanitized.strip(),
                detected_format=FORMAT_STUB_HEADER_ECHO,
                root_exception_type="StubHeaderEchoArtifact",
                root_cause_message="Output contains only filename echoes, docstrings, or empty fences without runnable implementation.",
                confidence_score=0.98,
                is_actionable=True,
                actionable_remediation="Re-prompt generator with strict non-stubbing enforcement; verify complete function/class definitions are returned.",
                metadata={"target_file": target_file},
            )

        # Step 3: Strip redundant log timestamps for cleaner root-cause isolation
        cleaned_no_timestamps = self.strip_timestamps_and_log_prefixes(sanitized)

        # Step 4: Classification & Root Exception Extraction
        # Priority A: Git lock / concurrency contention
        git_match = self.GIT_LOCK_ERROR_RE.search(sanitized)
        if git_match:
            root_type, root_msg = "GitOperationError", git_match.group(1).strip()
            remediation = "Release git lock via removal of .git/index.lock or synchronize parallel commit workers."
            return SanitizedTrace(
                raw_length=raw_len,
                sanitized_text=sanitized.strip(),
                detected_format=FORMAT_GIT_OPERATION_ERROR,
                root_exception_type=root_type,
                root_cause_message=root_msg,
                confidence_score=0.99,
                is_actionable=True,
                actionable_remediation=remediation,
            )

        # Priority B: Python Traceback / Syntax Error
        if "Traceback (most recent call last):" in sanitized or self.PYTHON_SYNTAX_ERROR_RE.search(sanitized):
            is_syntax = bool(self.PYTHON_SYNTAX_ERROR_RE.search(sanitized))
            root_type, root_msg = self.extract_root_exception(sanitized)
            format_name = FORMAT_SYNTAX_CORRUPTION if is_syntax else FORMAT_PYTHON_TRACEBACK
            remediation = (
                f"Fix syntax corruption: {root_msg}"
                if is_syntax
                else f"Patch exception '{root_type}' in the offending source frame."
            )
            return SanitizedTrace(
                raw_length=raw_len,
                sanitized_text=sanitized.strip(),
                detected_format=format_name,
                root_exception_type=root_type or "UnknownPythonException",
                root_cause_message=root_msg or "Python traceback detected without trailing message.",
                confidence_score=0.95,
                is_actionable=True,
                actionable_remediation=remediation,
            )

        # Priority C: LaTeX Compilation Failures
        if self.LATEX_FATAL_RE.search(sanitized) or "! LaTeX Error:" in sanitized or "! Emergency stop." in sanitized:
            root_type, root_msg = self.extract_root_exception(sanitized)
            return SanitizedTrace(
                raw_length=raw_len,
                sanitized_text=sanitized.strip(),
                detected_format=FORMAT_LATEX_ERROR,
                root_exception_type=root_type or "LaTeXCompilationError",
                root_cause_message=root_msg or "LaTeX build halted with errors.",
                confidence_score=0.96,
                is_actionable=True,
                actionable_remediation="Resolve undefined LaTeX control sequences, mismatched environments, or missing packages.",
            )

        # Priority D: Timeout Exhaustion
        if self.TIMEOUT_ERROR_RE.search(sanitized):
            root_type, root_msg = self.extract_root_exception(sanitized)
            return SanitizedTrace(
                raw_length=raw_len,
                sanitized_text=sanitized.strip(),
                detected_format=FORMAT_TIMEOUT_EXHAUSTION,
                root_exception_type=root_type or "TimeoutExpired",
                root_cause_message=root_msg or "Process execution timed out.",
                confidence_score=0.92,
                is_actionable=True,
                actionable_remediation="Increase task timeout budget or optimize computational complexity of the target script.",
            )

        # Fallback: Generic / Unknown Failure Trace
        root_type, root_msg = self.extract_root_exception(cleaned_no_timestamps)
        return SanitizedTrace(
            raw_length=raw_len,
            sanitized_text=sanitized.strip(),
            detected_format=FORMAT_UNKNOWN_ERROR,
            root_exception_type=root_type or "UnknownError",
            root_cause_message=root_msg or "Unclassified execution failure detected.",
            confidence_score=0.60,
            is_actionable=True,
            actionable_remediation="Inspect sanitized log text manually to isolate non-standard failure semantics.",
        )


# =====================================================================
# Standalone Self-Test Harness
# =====================================================================
def _run_self_tests() -> None:
    """Comprehensive test harness verifying sanitization against all required failure modes."""
    sanitizer = ExecutionErrorSanitizer(stub_char_threshold=120)

    # 1. Test ANSI Escape Stripping
    raw_ansi = "\x1b[31;1m[ERROR]\x1b[0m \x1b[33mCritical failure in pipeline\x1b[0m"
    stripped = sanitizer.strip_ansi_escapes(raw_ansi)
    assert "\x1b[" not in stripped, "ANSI codes were not completely stripped"
    assert "[ERROR] Critical failure in pipeline" in stripped

    # 2. Test Python Traceback Cleaning & Root Isolation
    py_trace = """
    2026-10-28 14:02:11,501 [ERROR] Worker failed unexpectedly:
    Traceback (most recent call last):
      File "/app/tasks/worker.py", line 45, in execute_task
        result = compute_metrics(dataset)
      File "/app/tasks/worker.py", line 82, in compute_metrics
        raise ZeroDivisionError("division by zero in sample normalization")
    ZeroDivisionError: division by zero in sample normalization
    """
    res_py = sanitizer.clean_execution_trace(py_trace)
    assert res_py.detected_format == FORMAT_PYTHON_TRACEBACK
    assert res_py.root_exception_type == "ZeroDivisionError"
    assert res_py.root_cause_message == "division by zero in sample normalization"
    assert res_py.is_actionable is True

    # 3. Test Python Syntax Corruption
    syntax_trace = """
      File "tasks/model.py", line 12
        def run(
               ^
    SyntaxError: unexpected EOF while parsing
    """
    res_syntax = sanitizer.clean_execution_trace(syntax_trace)
    assert res_syntax.detected_format == FORMAT_SYNTAX_CORRUPTION
    assert res_syntax.root_exception_type == "SyntaxError"
    assert "unexpected EOF while parsing" in (res_syntax.root_cause_message or "")

    # 4. Test Git Lock & Concurrency Contention
    git_trace = "fatal: Unable to create '/home/ajf/automation/.git/index.lock': File exists."
    res_git = sanitizer.clean_execution_trace(git_trace)
    assert res_git.detected_format == FORMAT_GIT_OPERATION_ERROR
    assert res_git.root_exception_type == "GitOperationError"
    assert "index.lock" in (res_git.root_cause_message or "")

    # 5. Test LaTeX Compilation Error
    latex_trace = """
    This is pdfTeX, Version 3.141592653-2.6-1.40.24 (TeX Live 2022/Debian)
    entering extended mode
    ! LaTeX Error: File 'nonexistent_package.sty' not found.

    Type X to quit or <RETURN> to proceed,
    or enter new name. (Default extension: sty)
    ! Emergency stop.
    <read *> 
    l.6 \\usepackage
                   {nonexistent_package}^^M
    Fatal error occurred, no output PDF file produced!
    """
    res_latex = sanitizer.clean_execution_trace(latex_trace)
    assert res_latex.detected_format == FORMAT_LATEX_ERROR
    assert res_latex.is_actionable is True
    assert "LaTeX" in (res_latex.root_exception_type or "")

    # 6. Test Timeout Exhaustion
    timeout_trace = "subprocess.TimeoutExpired: Command '['pytest', 'tests/']' timed out after 300 seconds"
    res_timeout = sanitizer.clean_execution_trace(timeout_trace)
    assert res_timeout.detected_format == FORMAT_TIMEOUT_EXHAUSTION
    assert res_timeout.root_exception_type == "TimeoutExpired"

    # 7. Test Stub Header & Filename Echo Detection (< 120 meaningful non-whitespace chars)
    stub_snippet = """```python
    # tasks/pipeline_execution_error_sanitizer.py
    # TODO: Add implementation here
    ```"""
    res_stub = sanitizer.clean_execution_trace(stub_snippet, target_file="tasks/pipeline_execution_error_sanitizer.py")
    assert res_stub.detected_format == FORMAT_STUB_HEADER_ECHO
    assert res_stub.root_exception_type == "StubHeaderEchoArtifact"
    assert res_stub.is_actionable is True

    # 8. Test Empty & Null Handling
    res_empty = sanitizer.clean_execution_trace("")
    assert res_empty.detected_format == FORMAT_EMPTY_OR_WHITESPACE
    assert res_empty.is_actionable is False

    res_none = sanitizer.clean_execution_trace(None)
    assert res_none.detected_format == FORMAT_EMPTY_OR_WHITESPACE
    assert res_none.is_actionable is False

    # 9. Test Defensive Multi-Megabyte Truncation Handling
    large_garbage = "A" * (600 * 1024) + "\nZeroDivisionError: integer division or modulo by zero"
    res_large = sanitizer.clean_execution_trace(large_garbage)
    assert res_large.raw_length > 600 * 1024
    assert res_large.root_exception_type == "ZeroDivisionError"
    assert "TRUNCATED" in res_large.sanitized_text

    # 10. Test JSON Serialization
    json_repr = res_py.to_json()
    assert "ZeroDivisionError" in json_repr
    parsed_json = json.loads(json_repr)
    assert parsed_json["detected_format"] == FORMAT_PYTHON_TRACEBACK


if __name__ == "__main__":
    _run_self_tests()
    print("ExecutionErrorSanitizer: All self-tests passed successfully.")
    sys.exit(0)