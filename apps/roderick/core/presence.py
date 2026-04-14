"""
User presence mode.

At PC     → dashboard-first; Telegram for critical alerts and approvals only
Away      → Telegram-first; all updates via Telegram
DND       → critical alerts only; no briefings or routine messages
Focus     → reduced noise; batch non-critical; essential updates only

Stored in data/context/presence.json, readable by all components.
Dashboard (Phase 5) writes to this file via the API.
"""
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

VALID_MODES = {"at_pc", "away", "dnd", "focus"}
DEFAULT_MODE = "at_pc"


class PresenceManager:
    def __init__(self, data_dir: str):
        self._path = Path(data_dir) / "context" / "presence.json"
        self._path.parent.mkdir(parents=True, exist_ok=True)
        if not self._path.exists():
            self._write(DEFAULT_MODE)

    def get_mode(self) -> str:
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
            mode = data.get("mode", DEFAULT_MODE)
            return mode if mode in VALID_MODES else DEFAULT_MODE
        except Exception:
            return DEFAULT_MODE

    def set_mode(self, mode: str) -> None:
        if mode not in VALID_MODES:
            raise ValueError(f"Invalid presence mode: {mode}. Valid: {VALID_MODES}")
        self._write(mode)
        logger.info("Presence mode → %s", mode)

    def should_send_telegram(self, priority: str = "normal") -> bool:
        """
        Returns True if a message of the given priority should be sent via Telegram
        in the current presence mode.
        """
        mode = self.get_mode()
        if priority == "critical":
            return True  # Always send critical
        if mode == "at_pc":
            return priority == "high"   # Only high+ on Telegram when at PC
        if mode == "away":
            return True                 # All messages when away
        if mode == "dnd":
            return False                # Nothing in DND (critical handled above)
        if mode == "focus":
            return priority in ("high", "critical")
        return True

    def _write(self, mode: str) -> None:
        self._path.write_text(
            json.dumps({"mode": mode}, indent=2), encoding="utf-8"
        )
