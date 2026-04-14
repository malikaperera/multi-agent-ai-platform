"""
FieldClassifier — classifies form fields into safety tiers before filling.

SAFE    → auto-fill silently (name, email, phone, location, URLs)
CONFIRM → auto-fill with profile value, flagged in pre-submit review (salary, notice)
STOP    → never auto-fill; alert user and pause apply (legal, assessments, background)
"""
import json
import logging
from pathlib import Path

log = logging.getLogger(__name__)

# Path relative to this file's location
ANSWERS_PATH = Path(__file__).parent.parent / "config" / "approved_answers.json"


class FieldClassifier:
    def __init__(self):
        self._answers = self._load_answers()
        self._never   = [k.lower() for k in self._answers.get("never_auto_fill", [])]
        self._confirm = {k.lower(): v for k, v in self._answers.get("confirm_before_fill", {}).items()}
        self._safe    = {k.lower(): v for k, v in self._answers.get("safe_auto_fill", {}).items()}

    def _load_answers(self) -> dict:
        try:
            return json.loads(ANSWERS_PATH.read_text(encoding="utf-8"))
        except Exception as exc:
            log.warning("[Classifier] Could not load approved_answers.json: %s", exc)
            return {}

    def classify(self, label: str) -> str:
        """Returns: 'safe' | 'confirm' | 'stop' | 'unknown'"""
        l = label.lower().strip()

        for pattern in self._never:
            if pattern in l:
                return "stop"

        for pattern in self._confirm:
            if pattern in l:
                return "confirm"

        for pattern in self._safe:
            if pattern in l:
                return "safe"

        safe_patterns = [
            "first name", "last name", "full name", "name",
            "email", "phone", "mobile", "telephone",
            "city", "suburb", "postcode", "state", "country", "location", "address",
            "linkedin", "github", "portfolio", "website",
            "cover letter", "message", "additional information",
        ]
        for p in safe_patterns:
            if p in l:
                return "safe"

        return "unknown"

    def get_answer(self, label: str) -> str | None:
        """Get pre-approved answer for a field label, or None."""
        l = label.lower().strip()
        for pattern, answer in self._confirm.items():
            if pattern in l:
                return str(answer)
        for pattern, answer in self._safe.items():
            if pattern in l:
                return str(answer)
        return None

    def stop_reason(self, label: str) -> str | None:
        l = label.lower().strip()
        for pattern in self._never:
            if pattern in l:
                return pattern
        return None
