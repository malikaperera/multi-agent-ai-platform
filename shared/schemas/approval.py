from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ApprovalRequest:
    request_type: str  # task_approval | plan_approval
    description: str
    id: Optional[int] = None
    task_id: Optional[int] = None
    payload: dict = field(default_factory=dict)
    status: str = "pending"  # pending | approved | rejected | deferred
    telegram_message_id: Optional[int] = None
    # Base key stored in DB; buttons use "{callback_data}:approve" etc.
    callback_data: Optional[str] = None
    created_at: Optional[str] = None
    resolved_at: Optional[str] = None
