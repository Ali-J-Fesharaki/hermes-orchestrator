#!/usr/bin/env python3
"""
phoenix_tracer.py: Official Arize Phoenix OpenTelemetry tracing for Hermes Orchestrator.
Sends LLM spans, prompt/completion tokens, reasoning, and latencies directly to Phoenix UI on port 6006.
"""

import os
import sys
import time

_REGISTERED = False
_TRACER_PROVIDER = None

def init_phoenix_tracing(project_name="default"):
    global _REGISTERED, _TRACER_PROVIDER
    if _REGISTERED:
        return True

    try:
        from phoenix.otel import register
        _TRACER_PROVIDER = register(
            project_name=project_name,
            endpoint=os.environ.get("PHOENIX_COLLECTOR_ENDPOINT", "http://localhost:6006/v1/traces")
        )
        _REGISTERED = True
        return True
    except Exception as e:
        print(f"[-] Phoenix tracer init note: {e}")
        return False

def record_llm_span(model: str, role: str, repo_name: str, prompt_text: str, response_text: str, usage: dict, latency_s: float, session_id: str | None = None):
    """Logs a first-class LLM span directly into Phoenix trace view."""
    if not _REGISTERED:
        init_phoenix_tracing("default")

    try:
        from opentelemetry import trace
        from openinference.semconv.trace import SpanAttributes, OpenInferenceSpanKindValues

        tracer = trace.get_tracer("hermes-orchestrator")
        span_name = f"{role}: {model}"
        with tracer.start_as_current_span(span_name) as span:
            span.set_attribute(SpanAttributes.OPENINFERENCE_SPAN_KIND, OpenInferenceSpanKindValues.LLM.value)
            span.set_attribute(SpanAttributes.LLM_MODEL_NAME, model)
            span.set_attribute(SpanAttributes.LLM_SYSTEM, "hermes-agent")
            span.set_attribute("repository.name", repo_name)
            span.set_attribute("agent.role", role)
            if session_id:
                span.set_attribute("hermes.session_id", session_id)
                span.set_attribute(SpanAttributes.SESSION_ID, session_id)
            span.set_attribute(SpanAttributes.INPUT_VALUE, str(prompt_text)[:4000])
            span.set_attribute(SpanAttributes.OUTPUT_VALUE, str(response_text)[:4000])

            in_tok = usage.get("prompt_tokens", 0)
            out_tok = usage.get("completion_tokens", 0)
            tot_tok = usage.get("total_tokens", in_tok + out_tok)

            span.set_attribute(SpanAttributes.LLM_TOKEN_COUNT_PROMPT, in_tok)
            span.set_attribute(SpanAttributes.LLM_TOKEN_COUNT_COMPLETION, out_tok)
            span.set_attribute(SpanAttributes.LLM_TOKEN_COUNT_TOTAL, tot_tok)
            span.set_attribute("latency.seconds", round(latency_s, 2))
    except Exception as e:
        pass

def record_error_span(model: str, role: str, repo_name: str, task_name: str, error_message: str, session_id: str | None = None):
    """Logs an explicit ERROR span to Phoenix showing in red on the dashboard."""
    if not _REGISTERED:
        init_phoenix_tracing("default")

    try:
        from opentelemetry import trace
        from opentelemetry.trace import StatusCode
        from openinference.semconv.trace import SpanAttributes, OpenInferenceSpanKindValues

        tracer = trace.get_tracer("hermes-orchestrator")
        span_name = f"ERROR: {role} ({task_name[:30]})"
        with tracer.start_as_current_span(span_name) as span:
            span.set_status(StatusCode.ERROR, error_message[:500])
            span.set_attribute(SpanAttributes.OPENINFERENCE_SPAN_KIND, OpenInferenceSpanKindValues.CHAIN.value)
            span.set_attribute(SpanAttributes.LLM_MODEL_NAME, model)
            span.set_attribute(SpanAttributes.LLM_SYSTEM, "hermes-agent")
            span.set_attribute("repository.name", repo_name)
            span.set_attribute("agent.role", role)
            span.set_attribute("task.name", task_name)
            span.set_attribute("error.message", str(error_message)[:2000])
            if session_id:
                span.set_attribute("hermes.session_id", session_id)
                span.set_attribute(SpanAttributes.SESSION_ID, session_id)
            span.set_attribute(SpanAttributes.INPUT_VALUE, f"Task: {task_name}")
            span.set_attribute(SpanAttributes.OUTPUT_VALUE, f"FAILED: {error_message[:2000]}")
    except Exception:
        pass

if __name__ == "__main__":
    ok = init_phoenix_tracing()
    print("[+] Phoenix tracing registered:", ok)
    record_llm_span(
        model="gemini-3.8-flash-tiered",
        role="Task Executor",
        repo_name="PhD Proposal & Research Engine",
        prompt_text="Formulate Virtual Air-Gap boundaries",
        response_text="Generated chapter LaTeX",
        usage={"prompt_tokens": 100, "completion_tokens": 500, "total_tokens": 600},
        latency_s=2.3
    )
    print("[+] Sample LLM span recorded to Phoenix!")
