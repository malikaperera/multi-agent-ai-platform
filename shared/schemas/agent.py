from dataclasses import dataclass, field
from typing import Optional


@dataclass
class AgentRecord:
    name: str
    display_name: str
    purpose: str
    id: Optional[int] = None
    status: str = "unknown"            # online | offline | idle | busy | unknown
    autonomy_level: str = "supervised" # manual | supervised | autonomous
    model_used: str = "claude-sonnet-4-6"
    task_types_accepted: list = field(default_factory=list)   # JSON list
    report_types_produced: list = field(default_factory=list) # JSON list
    last_run: Optional[str] = None
    last_heartbeat: Optional[str] = None
    last_success: Optional[str] = None
    last_error: Optional[str] = None
    last_message: Optional[str] = None
    last_report: Optional[str] = None
    config: dict = field(default_factory=dict)
    updated_at: Optional[str] = None
    # introspection fields (v3)
    current_task_id: Optional[int] = None
    current_model: Optional[str] = None
    state_confidence: float = 1.0
