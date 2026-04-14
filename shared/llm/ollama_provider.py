"""
Ollama LLM provider — free, local, no API key required.

Talks to the Ollama HTTP API (compatible with OpenAI /api/chat format).
Tools are converted from Anthropic input_schema format to OpenAI function format.

Recommended models (pull with: docker exec ollama ollama pull <model>):
  llama3.1:8b    — best balance of speed and tool-use capability  ← default
  qwen2.5:7b     — strong tool use and reasoning
  mistral:7b     — fast, good general purpose
  llama3.2:3b    — smallest, fastest (limited tool use)
"""
from __future__ import annotations

import json
import logging
import os
import socket
import time
import urllib.error
import urllib.request
from typing import Callable, Optional

logger = logging.getLogger(__name__)

_langfuse: Optional[object] = None

def _get_langfuse():
    global _langfuse
    if _langfuse is not None:
        return _langfuse
    pub  = os.environ.get("LANGFUSE_PUBLIC_KEY", "")
    sec  = os.environ.get("LANGFUSE_SECRET_KEY", "")
    host = os.environ.get("LANGFUSE_HOST", "http://langfuse:3000")
    if not pub or not sec:
        return None
    try:
        from langfuse import Langfuse
        _langfuse = Langfuse(public_key=pub, secret_key=sec, host=host)
        logger.info("Langfuse tracing enabled for Ollama (host=%s)", host)
    except Exception as e:
        logger.warning("Langfuse unavailable: %s", e)
        _langfuse = False
    return _langfuse if _langfuse else None


def _start_generation(name: str, model: str, input_data: dict, max_tokens: int):
    lf = _get_langfuse()
    if not lf or not hasattr(lf, "start_observation"):
        return None, None
    try:
        generation = lf.start_observation(
            name=name,
            as_type="generation",
            input=input_data,
            model=model,
            model_parameters={"num_predict": max_tokens},
        )
        return lf, generation
    except Exception as e:
        logger.debug("Langfuse generation start failed: %s", e)
        return None, None


def _finish_generation(lf, generation, output: str, usage: dict, metadata: Optional[dict] = None, error: Optional[Exception] = None) -> None:
    if not generation:
        return
    try:
        update: dict = {
            "output": output[:1000] if isinstance(output, str) else output,
            "usage_details": usage,
            "metadata": metadata or {},
        }
        if error:
            update["level"] = "ERROR"
            update["status_message"] = str(error)[:500]
        generation.update(**update)
        generation.end()
        if lf and hasattr(lf, "flush"):
            lf.flush()
    except Exception as e:
        logger.debug("Langfuse generation finish failed: %s", e)


class OllamaProvider:
    def __init__(self, host: str, model: str, max_tokens: int = 4096, timeout: int = 600):
        self.host = host.rstrip("/")
        self.model = model
        self.max_tokens = max_tokens
        self.timeout = timeout
        self.retry_attempts = max(1, int(os.environ.get("OLLAMA_RETRY_ATTEMPTS", "3")))
        self.retry_backoff_seconds = max(1.0, float(os.environ.get("OLLAMA_RETRY_BACKOFF_SECONDS", "2")))

    # ── Public interface (same as AnthropicProvider) ──────────────────────────

    def complete(
        self,
        messages: list[dict],
        system: str,
        name: str = "complete",
        timeout: Optional[int] = None,
    ) -> str:
        payload = {
            "model": self.model,
            "messages": [{"role": "system", "content": system}] + messages,
            "stream": False,
            "options": {"num_predict": self.max_tokens},
        }
        input_data = {"system": system[:500], "messages": messages[-6:]}
        lf, generation = _start_generation(name, self.model, input_data, self.max_tokens)
        t0 = time.time()
        try:
            resp = self._post("/api/chat", payload, timeout=timeout)
        except Exception as e:
            _finish_generation(lf, generation, "", {}, {"provider": "ollama", "duration_s": round(time.time() - t0, 2)}, e)
            raise
        elapsed = time.time() - t0
        content = resp.get("message", {}).get("content", "")
        tokens = resp.get("eval_count", 0) + resp.get("prompt_eval_count", 0)
        logger.info("LLM call: model=%s name=%s duration=%.1fs tokens=%d", self.model, name, elapsed, tokens)
        _finish_generation(
            lf,
            generation,
            content,
            {
                "input": int(resp.get("prompt_eval_count", 0) or 0),
                "output": int(resp.get("eval_count", 0) or 0),
                "total": int(tokens or 0),
            },
            {"provider": "ollama", "duration_s": round(elapsed, 2)},
        )
        return content

    def run_agentic_loop(
        self,
        messages: list[dict],
        system: str,
        tools: list[dict],
        tool_executor: Callable[[str, dict], str],
        name: str = "agentic_loop",
        timeout: Optional[int] = None,
    ) -> str:
        msgs: list[dict] = [{"role": "system", "content": system}] + list(messages)
        ollama_tools = _to_openai_tools(tools)
        max_turns = 10

        for turn in range(max_turns):
            payload: dict = {
                "model": self.model,
                "messages": msgs,
                "stream": False,
                "options": {"num_predict": self.max_tokens},
            }
            if ollama_tools:
                payload["tools"] = ollama_tools

            lf, generation = _start_generation(
                f"{name}_turn{turn}",
                self.model,
                {"messages": msgs[-4:] if len(msgs) >= 4 else msgs, "tools": [t.get("function", {}).get("name") for t in ollama_tools]},
                self.max_tokens,
            )
            t0 = time.time()
            try:
                resp = self._post("/api/chat", payload, timeout=timeout)
            except Exception as e:
                _finish_generation(lf, generation, "", {}, {"provider": "ollama", "turn": turn, "duration_s": round(time.time() - t0, 2)}, e)
                raise
            elapsed = time.time() - t0
            tokens = resp.get("eval_count", 0) + resp.get("prompt_eval_count", 0)
            logger.info("LLM call: model=%s name=%s turn=%d duration=%.1fs tokens=%d", self.model, name, turn, elapsed, tokens)
            message = resp.get("message", {})
            tool_calls = message.get("tool_calls") or []

            _finish_generation(
                lf,
                generation,
                message.get("content", "") or f"[{len(tool_calls)} tool calls]",
                {
                    "input": int(resp.get("prompt_eval_count", 0) or 0),
                    "output": int(resp.get("eval_count", 0) or 0),
                    "total": int(tokens or 0),
                },
                {"provider": "ollama", "turn": turn, "tool_calls": len(tool_calls), "duration_s": round(elapsed, 2)},
            )

            if not tool_calls:
                return message.get("content", "")

            # Add assistant message with tool calls
            msgs.append(message)

            # Execute each tool and add results
            for tc in tool_calls:
                fn = tc.get("function", {})
                tool_name = fn.get("name", "")
                raw_args = fn.get("arguments", {})
                # Ollama may return arguments as a JSON string
                if isinstance(raw_args, str):
                    try:
                        raw_args = json.loads(raw_args)
                    except json.JSONDecodeError:
                        raw_args = {}
                logger.info("Ollama tool call: %s(%s)", tool_name, raw_args)
                result = tool_executor(tool_name, raw_args)
                msgs.append({"role": "tool", "content": str(result)})

        logger.warning("Ollama agentic loop hit max_turns=%d", max_turns)
        return ""

    # ── HTTP ─────────────────────────────────────────────────────────────────

    def _post(self, path: str, payload: dict, timeout: Optional[int] = None) -> dict:
        url = f"{self.host}{path}"
        body = json.dumps(payload).encode()
        effective_timeout = timeout if timeout is not None else self.timeout
        last_error: Exception | None = None
        for attempt in range(1, self.retry_attempts + 1):
            req = urllib.request.Request(
                url,
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=effective_timeout) as resp:
                    return json.loads(resp.read())
            except urllib.error.HTTPError as e:
                last_error = e
                if e.code not in {500, 502, 503, 504} or attempt >= self.retry_attempts:
                    raise RuntimeError(
                        f"Ollama HTTP {e.code} at {self.host}{path}. "
                        "The daemon may be warming up or overloaded."
                    ) from e
                sleep_for = self.retry_backoff_seconds * attempt
                logger.warning(
                    "Ollama transient HTTP %s for model=%s path=%s (attempt %s/%s); retrying in %.1fs",
                    e.code,
                    self.model,
                    path,
                    attempt,
                    self.retry_attempts,
                    sleep_for,
                )
                time.sleep(sleep_for)
            except (urllib.error.URLError, TimeoutError, socket.timeout) as e:
                last_error = e
                if attempt >= self.retry_attempts:
                    break
                sleep_for = self.retry_backoff_seconds * attempt
                logger.warning(
                    "Ollama transient connectivity issue for model=%s path=%s (attempt %s/%s); retrying in %.1fs: %s",
                    self.model,
                    path,
                    attempt,
                    self.retry_attempts,
                    sleep_for,
                    e,
                )
                time.sleep(sleep_for)
        raise RuntimeError(
            f"Ollama not reachable at {self.host}. "
            "Is the ollama service running? Check: docker compose ps"
        ) from last_error


# ── Format conversion ─────────────────────────────────────────────────────────

def _to_openai_tools(anthropic_tools: list[dict]) -> list[dict]:
    """Convert Anthropic input_schema tools → OpenAI function tools (Ollama format)."""
    result = []
    for tool in anthropic_tools:
        result.append({
            "type": "function",
            "function": {
                "name": tool["name"],
                "description": tool.get("description", ""),
                "parameters": tool.get("input_schema", {"type": "object", "properties": {}}),
            },
        })
    return result
