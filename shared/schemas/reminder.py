from dataclasses import dataclass
from typing import Optional


@dataclass
class Reminder:
    text: str
    id: Optional[int] = None
    due: Optional[str] = None  # ISO datetime string (UTC)
    category: str = "personal"  # personal | work | devops | research | other
    done: bool = False
    recurring: Optional[str] = None  # reserved for future cron expression
    created_at: Optional[str] = None
