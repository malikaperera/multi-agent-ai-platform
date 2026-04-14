from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Task:
    to_agent: str
    task_type: str  # research | build | reminder_planning | agent_status | personal_admin | direct_answer | opportunity
    description: str
    id: Optional[int] = None
    from_agent: str = "roderick"
    # Status lifecycle:
    #   pending → approved → in_progress → (plan_ready → plan_approved) → awaiting_validation
    #   → completed | live | rolled_back | failed | rejected
    status: str = "pending"
    priority: str = "normal"    # low | normal | high | critical
    urgency: str = "this_week"  # immediate | today | this_week | backlog
    domain: str = "operations"  # life | career | business | build | research | operations
    payload: dict = field(default_factory=dict)
    result: Optional[dict] = None
    approval_required: bool = False
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
