import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from shared.db.messages import get_unread_messages, mark_message_read, send_agent_message

logger = logging.getLogger(__name__)


def recent_inbox_context(db_path: str, agent_name: str, limit: int = 5) -> str:
    messages = get_unread_messages(db_path, agent_name, limit=limit)
    if not messages:
        return "(no unread inter-agent messages)"
    lines = []
    for msg in messages:
        lines.append(f"- From {msg.from_agent} [{msg.priority}]: {msg.message[:600]}")
        if msg.id is not None:
            mark_message_read(db_path, msg.id)
    return "\n".join(lines)


def reflect_after_task(
    *,
    llm,
    db_path: str,
    data_dir: str,
    agent_name: str,
    task,
    result: dict[str, Any] | None,
    owner_context: str = "",
) -> dict[str, Any]:
    """Ask an agent's own LLM what it learned and who else should know."""
    context = {
        "agent": agent_name,
        "task": {
            "id": task.id,
            "from_agent": task.from_agent,
            "to_agent": task.to_agent,
            "task_type": task.task_type,
            "description": task.description,
            "priority": task.priority,
            "urgency": task.urgency,
            "domain": task.domain,
        },
        "result": result or {},
        "inbox": recent_inbox_context(db_path, agent_name),
        "owner_context": owner_context,
    }
    raw = llm.complete(
        messages=[{"role": "user", "content": json.dumps(context, ensure_ascii=False, indent=2)}],
        system=(
            f"You are {agent_name}, a specialist agent in the Roderick ecosystem. "
            "Reflect on the completed task so the ecosystem gets smarter and more useful. "
            "Be passionate about improving the owner's quality of life, but stay practical and safe. "
            "Return valid JSON only with keys: learned, memory_note, messages, forge_improvement. "
            "messages must be a list of objects: {to_agent, priority, message}. "
            "forge_improvement is either null or {title, rationale, requested_change, priority}. "
            "Only propose Forge work if there is a concrete system improvement worth the owner approving."
        ),
        name=f"{agent_name}_reflection",
    )
    try:
        reflection = json.loads(raw.strip())
    except json.JSONDecodeError:
        reflection = {
            "learned": raw[:500],
            "memory_note": raw[:1000],
            "messages": [],
            "forge_improvement": None,
        }

    _append_learning(data_dir, agent_name, task.id, reflection)
    _send_reflection_messages(db_path, agent_name, reflection)
    return reflection


def try_reflect_after_task(**kwargs) -> dict[str, Any] | None:
    try:
        return reflect_after_task(**kwargs)
    except Exception as e:
        logger.warning("Agent reflection failed for %s: %s", kwargs.get("agent_name"), e)
        return None


def _append_learning(data_dir: str, agent_name: str, task_id: int | None, reflection: dict[str, Any]) -> None:
    learning_dir = Path(data_dir) / "agent_learning"
    learning_dir.mkdir(parents=True, exist_ok=True)
    path = learning_dir / f"{agent_name}.md"
    timestamp = datetime.now(timezone.utc).isoformat()
    memory_note = reflection.get("memory_note") or reflection.get("learned") or ""
    text = (
        f"\n## {timestamp} — task #{task_id}\n\n"
        f"**Learned:** {reflection.get('learned', '')}\n\n"
        f"**Memory note:** {memory_note}\n"
    )
    with path.open("a", encoding="utf-8") as f:
        f.write(text)


def _send_reflection_messages(db_path: str, agent_name: str, reflection: dict[str, Any]) -> None:
    for msg in reflection.get("messages", [])[:5]:
        if not isinstance(msg, dict):
            continue
        to_agent = str(msg.get("to_agent", "")).strip()
        message = str(msg.get("message", "")).strip()
        if not to_agent or not message:
            continue
        priority = str(msg.get("priority", "normal")).strip() or "normal"
        try:
            send_agent_message(db_path, agent_name, to_agent, message, priority)
        except Exception as e:
            logger.warning("Failed to send reflection message from %s to %s: %s", agent_name, to_agent, e)
