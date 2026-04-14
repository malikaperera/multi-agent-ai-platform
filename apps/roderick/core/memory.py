"""
Persistent context store for Roderick.
JSON files in data/context/ — human-editable, loaded fresh from disk on each access.
"""
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_DEFAULTS = {
    "preferences.json": {
        "name": "owner",
        "timezone": "UTC",
        "response_style": "concise",
        "pronouns": "they/them",
    },
    "projects.json": {},
    "routines.json": {
        "morning_briefing": "08:00",
        "notes": "Configure your background and role in data/context/routines.json.",
    },
}


class MemoryManager:
    def __init__(self, data_dir: str):
        self.context_dir = Path(data_dir) / "context"
        self.context_dir.mkdir(parents=True, exist_ok=True)
        self._ensure_defaults()

    def _ensure_defaults(self) -> None:
        for filename, default in _DEFAULTS.items():
            p = self.context_dir / filename
            if not p.exists():
                p.write_text(json.dumps(default, indent=2), encoding="utf-8")

    def _load(self, filename: str) -> dict:
        p = self.context_dir / filename
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning("Could not load %s: %s", filename, e)
            return {}

    def _save(self, filename: str, data: dict) -> None:
        p = self.context_dir / filename
        p.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    def get_preferences(self) -> dict:
        return self._load("preferences.json")

    def get_projects(self) -> dict:
        return self._load("projects.json")

    def get_routines(self) -> dict:
        return self._load("routines.json")

    def update_preference(self, key: str, value) -> None:
        prefs = self._load("preferences.json")
        prefs[key] = value
        self._save("preferences.json", prefs)
        logger.info("Preference updated: %s = %s", key, value)

    def add_project(self, name: str, details: dict) -> None:
        projects = self._load("projects.json")
        projects[name] = details
        self._save("projects.json", projects)
        logger.info("Project added: %s", name)

    def get_context_summary(self) -> str:
        """Compact text injected into every system prompt."""
        prefs = self.get_preferences()
        projects = self.get_projects()
        routines = self.get_routines()

        lines = [
            f"User: {prefs.get('name', 'owner')} ({prefs.get('pronouns', 'he/him')})",
            f"Timezone: {prefs.get('timezone', 'UTC')}",
            f"Response style: {prefs.get('response_style', 'concise')}",
        ]
        if routines.get("notes"):
            lines.append(f"Notes: {routines['notes']}")
        if projects:
            project_names = ", ".join(list(projects.keys())[:5])
            lines.append(f"Active projects: {project_names}")

        return "\n".join(lines)
