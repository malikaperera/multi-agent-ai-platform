from typing import Callable, Protocol, runtime_checkable


@runtime_checkable
class LLMAdapter(Protocol):
    def complete(self, messages: list[dict], system: str) -> str:
        """Single-shot completion. No tool loop."""
        ...

    def run_agentic_loop(
        self,
        messages: list[dict],
        system: str,
        tools: list[dict],
        tool_executor: Callable[[str, dict], str],
    ) -> str:
        """Drive tool-use loop until no more tool calls. Returns final text."""
        ...
