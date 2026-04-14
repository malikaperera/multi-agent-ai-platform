"""
LLM provider factory.

Provider is always Ollama (local, free). Anthropic support exists but must be
explicitly opted in by setting LLM_PROVIDER=anthropic in the environment.
There is no automatic fallback to Anthropic based on key presence.

Usage:
    build_llm(config)                    # uses OLLAMA_MODEL env var or config default
    build_llm(config, model="qwen3:14b") # override model for this instance
"""
import logging
import os

logger = logging.getLogger(__name__)


def build_llm(config: dict, model: str | None = None):
    """
    Return an LLM provider instance.

    model parameter overrides both env var and config — used to give each
    agent its own model without changing the global config.
    """
    llm_cfg = config.get("llm", {})

    provider = os.environ.get("LLM_PROVIDER", llm_cfg.get("provider", "ollama")).lower()

    if provider == "anthropic":
        from shared.llm.anthropic_provider import AnthropicProvider
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            raise RuntimeError(
                "LLM_PROVIDER=anthropic but ANTHROPIC_API_KEY is not set. "
                "Set LLM_PROVIDER=ollama to use the local model instead."
            )
        m = model or llm_cfg.get("model", "claude-sonnet-4-6")
        logger.info("LLM provider: Anthropic | model: %s", m)
        return AnthropicProvider(api_key=api_key, model=m, max_tokens=llm_cfg.get("max_tokens", 4096))

    # Default: Ollama
    from shared.llm.ollama_provider import OllamaProvider
    host = os.environ.get("OLLAMA_HOST", llm_cfg.get("ollama_host", "http://host.docker.internal:11434"))
    m = model or os.environ.get("OLLAMA_MODEL", llm_cfg.get("ollama_model", "qwen3:14b"))
    timeout = int(os.environ.get("OLLAMA_TIMEOUT_SECONDS", llm_cfg.get("timeout_seconds", 600)))
    logger.info("LLM provider: Ollama @ %s | model: %s", host, m)
    return OllamaProvider(host=host, model=m, max_tokens=llm_cfg.get("max_tokens", 4096), timeout=timeout)
