"""
Agent registry — tracks all known agents and their status.
Bootstrapped from config/agents.json; live state in SQLite.
"""
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from shared.db.agents import get_all_agents, upsert_agent
from shared.schemas.agent import AgentRecord

logger = logging.getLogger(__name__)
_DISPLAY_TZ = ZoneInfo("UTC")

_STATUS_EMOJI = {
    "online": "🟢",
    "idle": "🟡",
    "busy": "🔵",
    "offline": "🔴",
    "unknown": "⚪",
}


class AgentRegistryManager:
    def __init__(self, db_path: str, agents_config_path: str):
        self.db_path = db_path
        self.agents_config_path = agents_config_path

    def sync_from_config(self) -> None:
        """Load agents.json and upsert each agent into the registry."""
        try:
            data = json.loads(Path(self.agents_config_path).read_text(encoding="utf-8"))
        except Exception as e:
            logger.error("Could not load agents config: %s", e)
            return

        for entry in data.get("agents", []):
            agent = AgentRecord(
                name=entry["name"],
                display_name=entry["display_name"],
                purpose=entry.get("purpose", ""),
                status=entry.get("status", "unknown"),
                autonomy_level=entry.get("autonomy_level", "supervised"),
                model_used=entry.get("model_used", "claude-sonnet-4-6"),
                task_types_accepted=entry.get("task_types_accepted", []),
                report_types_produced=entry.get("report_types_produced", []),
                config=entry.get("config", {}),
            )
            upsert_agent(self.db_path, agent)
            logger.debug("Agent synced: %s", agent.name)

        logger.info("Agent registry synced (%d agents)", len(data.get("agents", [])))

    def get_status_summary(self) -> str:
        """Return formatted HTML summary of all registered agents."""
        agents = get_all_agents(self.db_path)
        if not agents:
            return "No agents registered."

        lines = ["<b>Agent Status</b>\n"]
        for a in agents:
            emoji = _STATUS_EMOJI.get(a.status, "⚪")
            model = a.current_model or a.model_used or ""
            model_str = f" <code>{model}</code>" if model else ""
            task_str = f" · task #{a.current_task_id}" if a.current_task_id and a.status == "busy" else ""
            lines.append(f"{emoji} <b>{a.display_name}</b> [{a.status}]{model_str}{task_str}")
            if a.last_message:
                lines.append(f"   <i>{a.last_message[:80]}</i>")
            if a.last_heartbeat:
                lines.append(f"   <i>heartbeat: {self._format_display_time(a.last_heartbeat)} AEST</i>")
            lines.append("")

        return "\n".join(lines).strip()

    def _format_display_time(self, value: str) -> str:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(_DISPLAY_TZ).strftime("%H:%M:%S")
        except Exception:
            return str(value)[11:19] if value else "unknown"

    def update_status(self, name: str, status: str, message: str = None) -> None:
        from shared.db.agents import update_agent_status
        update_agent_status(self.db_path, name, status, message)
