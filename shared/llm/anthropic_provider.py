import logging
import os
import time
from typing import Callable, Optional

try:
    import anthropic
    _anthropic_import_error: Optional[Exception] = None
except ImportError as _e:
    anthropic = None  # type: ignore[assignment]
    _anthropic_import_error = ImportError(
        "The 'anthropic' package is not installed. "
        "It is only required when LLM_PROVIDER=anthropic. "
        "Install it with: pip install anthropic>=0.40.0"
    )

logger = logging.getLogger(__name__)

# Langfuse tracing — optional. Enabled only when LANGFUSE_PUBLIC_KEY + LANGFUSE_SECRET_KEY are set.
_langfuse: Optional[object] = None

def _get_langfuse():
    global _langfuse
    if _langfuse is not None:
        return _langfuse
    pub = os.environ.get("LANGFUSE_PUBLIC_KEY", "")
    sec = os.environ.get("LANGFUSE_SECRET_KEY", "")
    host = os.environ.get("LANGFUSE_HOST", "http://langfuse:3000")
    if not pub or not sec:
        return None
    try:
        from langfuse import Langfuse
        _langfuse = Langfuse(public_key=pub, secret_key=sec, host=host)
        logger.info("Langfuse tracing enabled (host=%s)", host)
    except Exception as e:
        logger.warning("Langfuse unavailable: %s", e)
        _langfuse = False  # Don't retry
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
            model_parameters={"max_tokens": max_tokens},
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


class AnthropicProvider:
    def __init__(self, api_key: str, model: str, max_tokens: int = 4096):
        if _anthropic_import_error:
            raise _anthropic_import_error
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = model
        self.max_tokens = max_tokens

    def complete(self, messages: list[dict], system: str, name: str = "complete") -> str:
        """Single-shot LLM call. No tools."""
        lf, generation = _start_generation(name, self.model, {"system": system[:500], "messages": messages[-6:]}, self.max_tokens)
        t0 = time.monotonic()
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                system=system,
                messages=messages,
            )
        except Exception as e:
            _finish_generation(lf, generation, "", {}, {"provider": "anthropic", "duration_s": round(time.monotonic() - t0, 2)}, e)
            raise
        elapsed = time.monotonic() - t0
        text = response.content[0].text if response.content else ""
        _finish_generation(
            lf,
            generation,
            text,
            {"input": response.usage.input_tokens, "output": response.usage.output_tokens, "total": response.usage.input_tokens + response.usage.output_tokens},
            {"provider": "anthropic", "duration_s": round(elapsed, 2)},
        )

        return text

    def run_agentic_loop(
        self,
        messages: list[dict],
        system: str,
        tools: list[dict],
        tool_executor: Callable[[str, dict], str],
        name: str = "agentic_loop",
    ) -> str:
        """Drive agentic tool-use loop. tool_executor(name, inputs) -> str result."""
        msgs = list(messages)
        turn = 0
        while True:
            turn += 1
            lf, generation = _start_generation(
                f"{name}_turn_{turn}",
                self.model,
                {"system": system[:300], "messages_count": len(msgs), "tools": [t.get("name") for t in tools]},
                self.max_tokens,
            )

            t0 = time.monotonic()
            try:
                response = self.client.messages.create(
                    model=self.model,
                    max_tokens=self.max_tokens,
                    system=system,
                    tools=tools,
                    messages=msgs,
                )
            except Exception as e:
                _finish_generation(lf, generation, "", {}, {"provider": "anthropic", "turn": turn, "duration_s": round(time.monotonic() - t0, 2)}, e)
                raise

            text_parts = [b.text for b in response.content if b.type == "text"]
            tool_uses = [b for b in response.content if b.type == "tool_use"]
            _finish_generation(
                lf,
                generation,
                "\n".join(text_parts) if text_parts else f"{len(tool_uses)} tool calls",
                {"input": response.usage.input_tokens, "output": response.usage.output_tokens, "total": response.usage.input_tokens + response.usage.output_tokens},
                {"provider": "anthropic", "turn": turn, "tool_calls": len(tool_uses), "duration_s": round(time.monotonic() - t0, 2)},
            )

            if not tool_uses:
                return "\n".join(text_parts) if text_parts else ""

            msgs.append({"role": "assistant", "content": response.content})

            tool_results = []
            for tu in tool_uses:
                logger.info("Tool: %s | input: %s", tu.name, tu.input)
                result = tool_executor(tu.name, tu.input)
                logger.debug("Tool result (%d chars): %s", len(str(result)), str(result)[:200])
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tu.id,
                    "content": str(result),
                })

            msgs.append({"role": "user", "content": tool_results})
