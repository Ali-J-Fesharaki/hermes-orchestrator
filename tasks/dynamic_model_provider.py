#!/usr/bin/env python3
"""
dynamic_model_provider.py: Dynamic LLM Model Discovery & Router Integration.
Discovers available models in real-time from whichever LLM server or provider
Hermes Agent is currently configured with (~/.hermes/config.yaml or environment).
"""

import os
import sys
import json
import urllib.request
import urllib.error
from pathlib import Path

# Paths
HOME = Path.home()
CONFIG_FILE = HOME / ".hermes" / "config.yaml"
ENV_FILE = HOME / ".hermes" / ".env"

def load_hermes_env() -> dict:
    """Loads environment variables defined in ~/.hermes/.env."""
    env = dict(os.environ)
    if ENV_FILE.exists():
        try:
            for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip().strip("'\"")
        except Exception:
            pass
    return env

def get_active_llm_server_info() -> dict:
    """
    Parses ~/.hermes/config.yaml to extract the active base_url and API key.
    Adapts automatically if Hermes switches to OpenRouter, local llama.cpp/vLLM, or another gateway.
    """
    env_vars = load_hermes_env()
    base_url = "http://127.0.0.1:8045/v1"
    api_key = ""
    provider_name = "antigravity_tools"
    static_models = []

    if CONFIG_FILE.exists():
        try:
            import yaml
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                conf = yaml.safe_load(f) or {}

            model_conf = conf.get("model", {})
            provider_name = model_conf.get("provider") or provider_name
            base_url = model_conf.get("base_url") or base_url
            key_env = model_conf.get("key_env")

            if provider_name and "providers" in conf:
                prov = conf["providers"].get(provider_name, {})
                base_url = prov.get("base_url") or base_url
                key_env = prov.get("key_env") or key_env
                if "models" in prov and isinstance(prov["models"], dict):
                    static_models = list(prov["models"].keys())

            if key_env:
                api_key = env_vars.get(key_env, "")
        except Exception:
            # Fallback regex parser for config.yaml without pyyaml
            try:
                text = CONFIG_FILE.read_text(encoding="utf-8")
                import re
                m_base = re.search(r"base_url:\s*([^\s#]+)", text)
                if m_base:
                    base_url = m_base.group(1).strip()
                m_key = re.search(r"key_env:\s*([^\s#]+)", text)
                if m_key:
                    key_env = m_key.group(1).strip()
                    api_key = env_vars.get(key_env, "")
            except Exception:
                pass

    # Fallbacks for API key if empty
    if not api_key:
        for possible_key in [
            "HERMES_ANTIGRAVITY_TOOLS_API_KEY",
            "HERMES_CUSTOM_ANTIGRAVITY_MANAGER_API_KEY",
            "OPENROUTER_API_KEY",
            "OPENAI_API_KEY"
        ]:
            if env_vars.get(possible_key):
                api_key = env_vars[possible_key]
                break

    return {
        "provider": provider_name,
        "base_url": base_url.rstrip("/"),
        "api_key": api_key,
        "static_models": static_models
    }

def fetch_live_models() -> dict:
    """
    Queries the active LLM server's /models endpoint dynamically.
    Returns sorted list of available models with display labels and metadata.
    """
    server_info = get_active_llm_server_info()
    base_url = server_info["base_url"]
    api_key = server_info["api_key"]
    models_url = f"{base_url}/models" if not base_url.endswith("/models") else base_url

    req = urllib.request.Request(models_url)
    if api_key:
        req.add_header("Authorization", f"Bearer {api_key}")
    req.add_header("User-Agent", "Hermes-Multi-Repo-Orchestrator/1.0")

    models_list = []
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            raw_models = data.get("data", [])
            for m in raw_models:
                m_id = m.get("id")
                if not m_id:
                    continue
                display_name = m.get("display_name") or m.get("name") or m_id
                # Categorize / tag
                is_thinking = bool(m.get("supports_thinking") or "thinking" in m_id.lower() or "reasoning" in m_id.lower())
                models_list.append({
                    "id": m_id,
                    "name": display_name,
                    "supports_thinking": is_thinking,
                    "recommended": bool(m.get("recommended", False))
                })
    except Exception as e:
        print(f"[-] Dynamic model discovery from {models_url} failed: {e}")
        # Fallback to static list from config.yaml or standard fallbacks
        fallback_ids = server_info.get("static_models", []) or [
            "gemini-3.8-flash-tiered",
            "claude-opus-4-6-thinking",
            "claude-sonnet-4-6",
            "gemini-3.8-flash-high",
            "gemini-3.1-pro-high"
        ]
        for mid in fallback_ids:
            models_list.append({
                "id": mid,
                "name": mid.replace("-", " ").title(),
                "supports_thinking": "thinking" in mid or "3.8" in mid,
                "recommended": True
            })

    # Sort models: Claude & Gemini frontier models first, then alphabetical
    def sort_key(item):
        mid = item["id"].lower()
        if "opus" in mid:
            return (0, mid)
        if "sonnet" in mid:
            return (1, mid)
        if "3.8-flash-tiered" in mid:
            return (2, mid)
        if "3.8-flash" in mid:
            return (3, mid)
        if "3.1-pro" in mid:
            return (4, mid)
        if "gemini" in mid:
            return (5, mid)
        return (6, mid)

    models_list.sort(key=sort_key)

    return {
        "status": "success",
        "server_url": base_url,
        "models_count": len(models_list),
        "models": models_list
    }

if __name__ == "__main__":
    res = fetch_live_models()
    print(f"Discovered {res['models_count']} models from {res['server_url']}:")
    for m in res["models"][:10]:
        print(f" - {m['id']} ({m['name']}) [thinking: {m['supports_thinking']}]")
