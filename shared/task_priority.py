"""Shared task priority policy for cross-agent dispatch."""
from __future__ import annotations


PRIORITY_ORDER = {"critical": 0, "high": 1, "normal": 2, "low": 3}
URGENCY_ORDER = {"immediate": 0, "today": 1, "this_week": 2, "backlog": 3}

SECURITY_KEYWORDS = (
    "security",
    "secret",
    "token",
    "api key",
    "credential",
    "vulnerability",
    "exploit",
    "injection",
    "permission",
    "unsafe",
    "exposed",
    "breach",
)

CRITICAL_KEYWORDS = (
    "critical",
    "urgent",
    "immediately",
    "asap",
    "blocker",
    "production down",
    "broken",
    "breach",
    "leaked",
)


def priority_rank(priority: str | None) -> int:
    return PRIORITY_ORDER.get(str(priority or "normal").lower(), PRIORITY_ORDER["normal"])


def urgency_rank(urgency: str | None) -> int:
    return URGENCY_ORDER.get(str(urgency or "this_week").lower(), URGENCY_ORDER["this_week"])


def sort_key(task) -> tuple[int, int, str]:
    return (
        priority_rank(getattr(task, "priority", "normal")),
        urgency_rank(getattr(task, "urgency", "this_week")),
        str(getattr(task, "created_at", "") or ""),
    )


def infer_priority(text: str, *, default_priority: str = "normal", default_urgency: str = "this_week") -> tuple[str, str, bool]:
    """Infer priority from user or agent text.

    Returns (priority, urgency, security_related). This is intentionally conservative:
    it only promotes obvious security/urgent language and otherwise keeps caller defaults.
    """
    haystack = (text or "").lower()
    security_related = any(keyword in haystack for keyword in SECURITY_KEYWORDS)
    critical = any(keyword in haystack for keyword in CRITICAL_KEYWORDS)
    if security_related and critical:
        return "critical", "immediate", True
    if security_related:
        return "high", "today", True
    if critical:
        return "high", "today", False
    return default_priority, default_urgency, False
