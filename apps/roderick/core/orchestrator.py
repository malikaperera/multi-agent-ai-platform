"""
Orchestrator â€” classifies incoming messages and routes them.
Maintains conversational history for direct-answer mode.
"""
import asyncio
import json
import logging
import re
import subprocess
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from apps.roderick.bot.approvals import send_approval_request
from apps.roderick.core.agent_registry import AgentRegistryManager
from apps.roderick.core.memory import MemoryManager
from shared.db.messages import get_unread_messages, mark_message_read
from shared.db.reminders import (
    get_due_reminders,
    list_reminders,
    mark_done,
    save_reminder,
)
from shared.db.tasks import enqueue_task, list_tasks
from shared.llm.anthropic_provider import AnthropicProvider
from shared.memory.founder import OwnerMemory
from shared.schemas.reminder import Reminder
from shared.schemas.task import Task
from shared.task_priority import infer_priority

logger = logging.getLogger(__name__)

# â”€â”€ Tool definitions â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

TOOLS = [
    {
        "name": "save_reminder",
        "description": "Save a reminder, note, or to-do item for the system owner.",
        "input_schema": {
            "type": "object",
            "properties": {
                "text": {"type": "string"},
                "due":  {"type": "string", "description": "ISO datetime string (UTC) or natural description"},
                "category": {
                    "type": "string",
                    "enum": ["personal", "work", "devops", "research", "other"],
                },
            },
            "required": ["text"],
        },
    },
    {
        "name": "list_reminders",
        "description": "List upcoming or recent reminders.",
        "input_schema": {
            "type": "object",
            "properties": {
                "category":     {"type": "string", "description": "Filter by category (optional)"},
                "include_done": {"type": "boolean", "description": "Include completed reminders"},
            },
            "required": [],
        },
    },
    {
        "name": "mark_reminder_done",
        "description": "Mark a reminder as done by its ID.",
        "input_schema": {
            "type": "object",
            "properties": {
                "reminder_id": {"type": "integer"},
            },
            "required": ["reminder_id"],
        },
    },
    {
        "name": "get_agent_status",
        "description": "Get the current status of all registered agents.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "run_command",
        "description": "Run a shell command (docker, git, etc.) in the DevOps directory.",
        "input_schema": {
            "type": "object",
            "properties": {
                "command":     {"type": "string"},
                "working_dir": {"type": "string", "description": "Relative to devops root"},
                "timeout":     {"type": "integer", "default": 30},
            },
            "required": ["command"],
        },
    },
    {
        "name": "list_files",
        "description": "List files in a directory under the DevOps root.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Relative to devops root (empty = root)"},
            },
            "required": [],
        },
    },
    {
        "name": "read_file",
        "description": "Read a file from the DevOps directory.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string"},
            },
            "required": ["path"],
        },
    },
]

SAFE_TOOLS = [tool for tool in TOOLS if tool["name"] in {"save_reminder", "list_reminders", "mark_reminder_done", "get_agent_status"}]
DEVOPS_TOOL_NAMES = {"run_command", "list_files", "read_file"}

# â”€â”€ Classification prompt â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

_CLASSIFY_SYSTEM = """Classify the following message into exactly one category.

Categories:
- reminder_planning  : creating/checking/deleting reminders, scheduling, deadlines, “remind me”
- research           : “find”, “research”, “best way to”, “how do I”, comparisons, investigations
- opportunity        : business ideas, side income, revenue opportunities, investment, “could I make money”, market ideas
- build              : “create”, “build”, “make”, “set up”, “code”, development requests
- agent_status       : asking what agents are doing, job agent updates, system status
- investigate        : “diagnose”, “investigate why”, “what's wrong”, “ask Merlin to”, “why is X slow”, “ecosystem health”
- show_improvements  : “improvement candidates”, “what changed”, “pipeline”, “recent improvements”, “Forge proposals”
- personal_admin     : general notes, preferences, admin items, ongoing tracking
- behavior_change    : "Zuko should", "Merlin should", "Forge should", "Sentinel should", "Atlas should", "Venture should", agent behavior instructions, scan more often, stop sending, focus on
- direct_answer      : conversation, questions, anything not fitting the above

Respond with JSON only - no markdown, no explanation:
{"category": "<category>", "confidence": 0.0, "summary": "<10-word summary>"}"""


# ── Rules-first fast path ──────────────────────────────────────────────────────
# These patterns bypass LLM classification entirely — instant response.
import re as _re

_STATUS_PATTERNS = _re.compile(
    r'(?i)((agent[s]?\s+(status|state|states|doing|working))|'
    r'(status\s+of\s+agent[s]?)|'
    r'(what.*(agent|running|doing))|'
    r'system\s*status|who.*(running|working|active)|status\s*report|'
    r'/agents|show\s*agents|list\s*agents|are.*agents|^\s*status\s*$|'
    r'what[\'’]?s\s+happening|what\s+is\s+happening|are\s+you\s+down)',
)

_PENDING_PATTERNS = _re.compile(
    r'(?i)(pending|approvals?|waiting\s*for|what.*(approve|approval))',
)

_INVESTIGATE_PATTERNS = _re.compile(
    r'(?i)(diagnos|investigat|why.*(slow|broken|fail|stuck|wrong)|'
    r'ask\s*merlin.*(diagnos|check|look)|ecosystem\s*health|'
    r'what.*(wrong|broken|issue)|merlin\s*diagnos|'
    r'(security|secret|token|credential|vulnerability|exploit|unsafe).*(check|review|investigat|diagnos|fix|urgent))',
)

_IMPROVEMENTS_PATTERNS = _re.compile(
    r'(?i)(improvement\s*candidate|show.*improvement|pipeline\s*status|'
    r'forge\s*proposal|what\s*changed|recent\s*improvement|'
    r'what\s*did.*sentinel.*valid|validation\s*result)',
)

_WORK_STATUS_PATTERNS = _re.compile(
    r'(?i)((what|where|show|give).*(status|progress|update)|'
    r'(status|progress|update).*(of|on|for)|'
    r'how.*going|is.*being.*addressed|already.*working|already.*fix|'
    r'already.*patch|being.*worked.*on|existing.*approval|existing.*task)'
)

_BEHAVIOR_PATTERNS = _re.compile(
    r'(?i)(zuko\s*should|merlin\s*should|forge\s*should|sentinel\s*should|atlas\s*should|venture\s*should|'
    r'tell\s+(zuko|merlin|forge|sentinel|atlas|venture)\s+to|'
    r'make\s+(zuko|merlin|forge|sentinel|atlas|venture)|change\s+how\s+(zuko|merlin|forge|sentinel|atlas|venture)|'
    r'(agent)\s+behavior|scan\s+more\s+often|focus\s+more\s+on|stop\s+sending|watch\s+.*more\s+closely|'
    r'(less|fewer)\s+messages?\s+from\s+(zuko|merlin|forge|sentinel|atlas|venture)|'
    r'(zuko|merlin|forge|sentinel|atlas|venture).*?(less|fewer|more|maximum|max|no\s+more\s+than|without\s+my\s+approval).*?'
    r'(messages?|scans?|approvals?|reports?|research|tasks?))',
)

_RESEARCH_PATTERNS = _re.compile(
    r'(?i)\b(research|find out|look up|investigate|compare|best way|how do i|how can i|what is the best|study)\b',
)

_BUILD_PATTERNS = _re.compile(
    r'(?i)\b(build|create|make|implement|code|add|set up|wire|fix|patch|improve|refactor)\b',
)

_OPPORTUNITY_PATTERNS = _re.compile(
    r'(?i)\b(business|opportunit|revenue|money|income|side hustle|market idea|startup|product idea|sell|moneti[sz]e)\b',
)

_REMINDER_PATTERNS = _re.compile(
    r'(?i)\b(remind me|reminder|schedule|deadline|todo|to-do|follow up|follow-up)\b',
)

_GREETING_PATTERNS = _re.compile(
    r'(?i)^\s*(hi|hello|hey|yo|sup|good\s+(morning|afternoon|evening))[\s!.?]*$'
)

_DEVOPS_TOOL_PATTERNS = _re.compile(
    r"(?i)(\bfile\b|\bfiles\b|\bread\b|\bopen\b|\bpath\b|\bdirectory\b|\bfolder\b|\bpdf\b|"
    r"\blog\b|\blogs\b|\bdocker\b|\bgit\b|\brepo\b|\brepository\b|\bsource\b|\bcode\b|"
    r"\bconfig\b|\bterminal\b|\bcommand\b|\bshell\b|localhost|http://|https://|\.md\b|\.json\b|\.py\b|/devops|F:)"
)

def _rules_classify(message: str) -> tuple[str, str] | None:
    """Return (category, summary) if message matches a fast-path rule, else None."""
    if _GREETING_PATTERNS.search(message):
        return "greeting", message[:40]
    if _BEHAVIOR_PATTERNS.search(message):
        return "behavior_change", message[:40]
    if _WORK_STATUS_PATTERNS.search(message):
        return "existing_work_status", message[:40]
    if _REMINDER_PATTERNS.search(message):
        return "reminder_planning", message[:40]
    if _STATUS_PATTERNS.search(message):
        return "agent_status", message[:40]
    if _PENDING_PATTERNS.search(message):
        return "pending_approvals", message[:40]
    if _INVESTIGATE_PATTERNS.search(message):
        return "investigate", message[:40]
    if _IMPROVEMENTS_PATTERNS.search(message):
        return "show_improvements", message[:40]
    if _OPPORTUNITY_PATTERNS.search(message):
        return "opportunity", message[:40]
    if _BUILD_PATTERNS.search(message):
        return "build", message[:40]
    if _RESEARCH_PATTERNS.search(message):
        return "research", message[:40]
    return None


class Orchestrator:
    def __init__(
        self,
        llm: AnthropicProvider,
        db_path: str,
        memory: MemoryManager,
        registry: AgentRegistryManager,
        config: dict,
        owner_memory: Optional[OwnerMemory] = None,
        coordinator_llm: Optional[AnthropicProvider] = None,
    ):
        self.llm = llm
        self.coordinator_llm = coordinator_llm or llm
        self.db_path = db_path
        self.memory = memory
        self.registry = registry
        self.config = config
        self.roderick_config = config.get("roderick", {})
        self.owner_memory = owner_memory
        self.devops_root = config.get("devops_root", "/devops")
        self.history: list[dict] = []
        self._history_lock = threading.Lock()

    def clear_history(self) -> None:
        with self._history_lock:
            self.history = []

    async def morning_briefing_text(self) -> str:
        """Scheduler compatibility wrapper for the daily briefing."""
        due = get_due_reminders(self.db_path, as_of=datetime.now(timezone.utc).isoformat())
        recent_tasks = list_tasks(self.db_path, limit=8)
        pending = [
            task for task in recent_tasks
            if task.status in {"pending", "approved", "plan_ready", "awaiting_validation"}
        ]
        lines = [
            "<b>Morning briefing</b>",
            "",
            f"Verified reminders due: {len(due)}",
            f"Recent tracked tasks: {len(recent_tasks)}",
            f"Tasks needing attention: {len(pending)}",
        ]
        if pending:
            lines.append("")
            lines.append("<b>Attention</b>")
            for task in pending[:5]:
                lines.append(f"- Task #{task.id} [{task.to_agent}/{task.status}] {task.description[:80]}")
        return "\n".join(lines)

    async def ecosystem_council_text(self) -> str:
        """Scheduler compatibility wrapper for the ecosystem council."""
        return await asyncio.get_event_loop().run_in_executor(None, self._run_ecosystem_council)

    async def propose_ecosystem_improvement(self, bot=None, chat_id: Optional[int] = None) -> str:
        """Create an approval-gated Forge task from the ecosystem council."""
        proposal = await asyncio.get_event_loop().run_in_executor(None, self._ecosystem_council_proposal)
        if not proposal.get("should_propose"):
            return (
                "<b>Ecosystem council</b>\n\n"
                f"{proposal.get('summary', 'No concrete build proposal right now.')}"
            )

        from shared.db.improvements import Improvement, upsert_improvement

        requested_change = proposal.get("requested_change") or proposal.get("summary") or proposal.get("title")
        affected_agents = proposal.get("affected_agents", []) or []
        imp = upsert_improvement(
            self.db_path,
            Improvement(
                title=(proposal.get("title") or "Ecosystem council proposal")[:160],
                description=requested_change,
                origin_agent="roderick",
                origin_signal="ecosystem_council",
                status="proposed",
                evidence={
                    "summary": proposal.get("summary", ""),
                    "rationale": proposal.get("rationale", ""),
                    "risks": proposal.get("risks", []),
                    "success_criteria": proposal.get("success_criteria", []),
                },
                priority=proposal.get("priority", "normal"),
                risk_level="medium" if proposal.get("risks") else "low",
                affected_components=affected_agents,
                forge_recommended=True,
            ),
        )
        forge_task = enqueue_task(
            self.db_path,
            Task(
                to_agent="forge",
                from_agent="roderick",
                task_type="system_improvement",
                description=requested_change,
                status="pending",
                priority=proposal.get("priority", "normal"),
                urgency=proposal.get("urgency", "this_week"),
                domain="operations",
                payload={
                    "improvement_id": imp.id,
                    "verified_facts": [proposal.get("summary", "")],
                    "likely_causes": [proposal.get("rationale", "")] if proposal.get("rationale") else [],
                    "unknowns": [],
                    "affected_components": affected_agents,
                    "recommended_actions": proposal.get("success_criteria", []),
                    "risk_level": imp.risk_level,
                    "source": "ecosystem_council",
                },
                approval_required=True,
            ),
        )
        from shared.db.improvements import advance_improvement
        advance_improvement(self.db_path, imp.id, "proposed", forge_task_id=forge_task.id)

        approval_text = (
            f"{self._format_ecosystem_proposal(proposal)}\n\n"
            f"Forge task #{forge_task.id} is ready for first approval. "
            "Approve to let Forge create a plan; implementation still requires plan approval."
        )
        if bot is not None and chat_id is not None:
            await send_approval_request(
                db_path=self.db_path,
                bot=bot,
                chat_id=chat_id,
                description=approval_text,
                task_id=forge_task.id,
                request_type="task_approval",
                payload={"improvement_id": imp.id},
            )
            return (
                "<b>Ecosystem council proposal created.</b>\n\n"
                f"Improvement #{imp.id} → Forge task #{forge_task.id}. "
                "Approval request sent."
            )
        return (
            "<b>Ecosystem council proposal created.</b>\n\n"
            f"Improvement #{imp.id} → Forge task #{forge_task.id}. "
            "Approve it from the dashboard."
        )

    # â”€â”€ Main entry point â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    async def handle(self, message: str, bot=None, chat_id: Optional[int] = None) -> str:
        category, summary = await asyncio.get_event_loop().run_in_executor(
            None, self._classify, message
        )
        logger.info("Classified '%s' â†’ %s", summary, category)

        if category == "greeting":
            return self._fast_greeting()
        elif category == "research":
            return await self._route_research(message)
        elif category == "opportunity":
            return await self._route_opportunity(message)
        elif category == "build":
            return await self._route_build(message, bot, chat_id)
        elif category == "agent_status":
            return self.registry.get_status_summary()
        elif category == "existing_work_status":
            return await self._route_existing_work_status(message)
        elif category == "pending_approvals":
            return await self._route_pending_approvals()
        elif category == "investigate":
            return await self._route_investigate(message)
        elif category == "show_improvements":
            return await self._route_show_improvements()
        elif category == "behavior_change":
            return await self._route_behavior_change(message)
        else:
            # reminder_planning, personal_admin, direct_answer all go through agentic loop
            return await asyncio.get_event_loop().run_in_executor(
                None, self._run_agentic, message
            )

    async def _route_research(self, message: str) -> str:
        existing = self._find_related_open_work(message)
        if existing["tasks"] or existing["improvements"] or existing["approvals"]:
            return self._format_existing_work_status(message, existing, preface="This already appears to be in flight.")
        priority, urgency, security_related = infer_priority(message, default_priority="normal", default_urgency="this_week")
        task = enqueue_task(self.db_path, Task(
            to_agent="merlin", from_agent="roderick", task_type="deep_research",
            description=message, approval_required=False, priority=priority, urgency=urgency,
            domain="operations" if security_related else "research",
        ))
        return (f"\U0001f50d <b>Research task queued for Merlin.</b>\n\n"
                f"<i>{message[:100]}</i>\n\nTask #{task.id} [deep_research] · {priority}/{urgency}")

    def _fast_greeting(self) -> str:
        status = self.registry.get_status_summary()
        first_line = status.splitlines()[0] if status else "System status unavailable."
        return f"Hey — I'm online.\n\n{first_line}"

    async def _route_opportunity(self, message: str) -> str:
        existing = self._find_related_open_work(message)
        if existing["tasks"] or existing["improvements"]:
            return self._format_existing_work_status(message, existing, preface="This already appears to be tracked.")
        task = enqueue_task(self.db_path, Task(
            to_agent="venture", from_agent="roderick", task_type="opportunity_eval",
            description=message, approval_required=False, priority="normal", domain="venture",
        ))
        return (f"\U0001f4bc <b>Opportunity queued for Venture.</b>\n\n"
                f"<i>{message[:100]}</i>\n\nTask #{task.id} [opportunity_eval]")

    async def _route_build(self, message: str, bot=None, chat_id: int = None) -> str:
        existing = self._find_related_open_work(message)
        if existing["tasks"] or existing["improvements"] or existing["approvals"]:
            return self._format_existing_work_status(message, existing, preface="I found related work already in progress, so I did not create a new Forge request.")
        priority, urgency, security_related = infer_priority(message, default_priority="normal", default_urgency="this_week")
        task = enqueue_task(self.db_path, Task(
            to_agent="forge", from_agent="roderick", task_type="build_feature",
            description=message, approval_required=True, priority=priority, urgency=urgency,
            domain="security" if security_related else "development",
            payload={
                "priority_source": "roderick_inferred",
                "security_related": security_related,
            },
        ))
        if bot is not None and chat_id is not None:
            await send_approval_request(
                db_path=self.db_path,
                bot=bot,
                chat_id=chat_id,
                description=(
                    f"Forge build request [{priority}/{urgency}]:\n\n{message[:1000]}\n\n"
                    "Approve to let Forge create a plan. A second approval is required before implementation."
                ),
                task_id=task.id,
                request_type="task_approval",
                payload={},
            )
        else:
            from shared.db.approvals import create_approval
            from shared.schemas.approval import ApprovalRequest
            create_approval(
                self.db_path,
                ApprovalRequest(
                    request_type="task_approval",
                    description=(
                        f"Forge build request [{priority}/{urgency}]:\n\n{message[:1000]}\n\n"
                        "Approve to let Forge create a plan. A second approval is required before implementation."
                    ),
                    task_id=task.id,
                    payload={},
                ),
            )
        return (f"\U0001f528 <b>Build request queued for Forge (approval required).</b>\n\n"
                f"<i>{message[:100]}</i>\n\nTask #{task.id} [build_feature] · {priority}/{urgency} \u2014 Forge will propose a plan before proceeding.")

    async def _route_pending_approvals(self) -> str:
        from shared.db.approvals import list_pending_approvals
        pending = list_pending_approvals(self.db_path)
        if not pending:
            return "No pending approvals."
        lines = [f"<b>{len(pending)} pending approval(s):</b>"]
        for a in pending:
            lines.append(f"\u2022 [{a.request_type}] {a.description[:100]}")
        return "\n".join(lines)

    async def _route_investigate(self, message: str) -> str:
        from shared.db.tasks import enqueue_task, Task
        existing = self._find_related_open_work(message)
        if existing["tasks"] or existing["improvements"] or existing["approvals"]:
            return self._format_existing_work_status(message, existing, preface="This issue already has related work attached to it.")
        msg_lower = message.lower()
        priority, urgency, security_related = infer_priority(message, default_priority="high", default_urgency="today")
        if "performance" in msg_lower or "slow" in msg_lower or "latency" in msg_lower:
            task_type = "performance_research"
        elif "diagnos" in msg_lower or "health" in msg_lower or "ecosystem" in msg_lower:
            task_type = "agent_diagnostics"
        else:
            task_type = "system_research"
        task = enqueue_task(self.db_path, Task(
            to_agent="merlin", from_agent="roderick", task_type=task_type,
            description=message, approval_required=False, priority=priority, urgency=urgency, domain="operations",
            payload={"priority_source": "roderick_inferred", "security_related": security_related},
        ))
        return (f"\U0001f52c <b>Investigation queued for Merlin.</b>\n\n"
                f"<i>{message[:100]}</i>\n\nTask #{task.id} [{task_type}] · {priority}/{urgency} \u2014 I'll report back with evidence-backed findings.")

    async def _route_show_improvements(self) -> str:
        from shared.db.improvements import list_improvements
        active = list_improvements(self.db_path, limit=20)
        if not active:
            return ("<b>Improvement Pipeline</b>\n\nNo improvements tracked yet.\n\n"
                    "<i>Say \"investigate why X\" to start a Merlin investigation.</i>")
        lines = ["<b>Improvement Pipeline</b>\n"]
        status_order = ["signal", "investigating", "proposed", "approved", "implementing",
                        "validating", "complete", "failed", "rejected", "rolled_back"]
        by_status = {}
        for imp in active:
            by_status.setdefault(imp.status, []).append(imp)
        for st in status_order:
            items = by_status.get(st, [])
            if items:
                lines.append(f"<b>{st.upper()} ({len(items)})</b>")
                for imp in items[:3]:
                    risk = f" [{imp.risk_level}]" if imp.risk_level and imp.risk_level != "unknown" else ""
                    forge = " \U0001f528" if imp.forge_recommended else ""
                    lines.append(f"  #{imp.id} {imp.title[:70]}{risk}{forge}")
                lines.append("")
        return "\n".join(lines).strip()

    async def _route_existing_work_status(self, message: str) -> str:
        existing = self._find_related_open_work(message)
        if existing["tasks"] or existing["improvements"] or existing["approvals"]:
            return self._format_existing_work_status(message, existing)
        return (
            "<b>No matching in-flight work found.</b>\n\n"
            "I could not verify an existing task, approval, or improvement for that issue from current state.\n\n"
            "<i>If you want, ask me to investigate it and I’ll queue the right agent.</i>"
        )

    def _extract_issue_keywords(self, message: str) -> list[str]:
        text = re.sub(r"[^a-z0-9\s_-]", " ", message.lower())
        stop = {
            "what", "whats", "status", "progress", "update", "the", "of", "on", "for", "is", "are", "a", "an",
            "to", "me", "please", "show", "give", "where", "how", "being", "already", "with", "that", "this",
            "task", "approval", "issue", "fix", "patch", "work", "worked", "working",
        }
        words = [w for w in text.split() if len(w) >= 3 and w not in stop]
        ordered: list[str] = []
        for word in words:
            if word not in ordered:
                ordered.append(word)
        return ordered[:8]

    def _find_related_open_work(self, message: str) -> dict:
        from shared.db.approvals import list_pending_approvals
        from shared.db.improvements import list_improvements

        keywords = self._extract_issue_keywords(message)
        open_statuses = {"pending", "approved", "plan_ready", "plan_approved", "in_progress", "awaiting_validation"}
        tasks = [
            task for task in list_tasks(self.db_path, limit=120)
            if task.status in open_statuses
        ]
        improvements = [
            imp for imp in list_improvements(self.db_path, limit=120)
            if imp.status not in {"complete", "failed", "rejected", "rolled_back"}
        ]
        approvals = list_pending_approvals(self.db_path)

        def score_text(text: str) -> int:
            haystack = text.lower()
            return sum(1 for kw in keywords if kw in haystack)

        scored_tasks = []
        for task in tasks:
            payload_blob = json.dumps(task.payload or {}, ensure_ascii=False)
            result_blob = json.dumps(task.result or {}, ensure_ascii=False)
            score = score_text(task.description) + score_text(payload_blob) + score_text(result_blob)
            if score > 0:
                scored_tasks.append((score, task))

        scored_improvements = []
        for imp in improvements:
            evidence_blob = json.dumps(imp.evidence or {}, ensure_ascii=False)
            text = " ".join([
                imp.title or "",
                imp.description or "",
                evidence_blob,
                " ".join(imp.affected_components or []),
            ])
            score = score_text(text)
            if score > 0:
                scored_improvements.append((score, imp))

        scored_approvals = []
        for approval in approvals:
            payload_blob = json.dumps(approval.payload or {}, ensure_ascii=False)
            text = " ".join([approval.description or "", payload_blob])
            score = score_text(text)
            if score > 0:
                scored_approvals.append((score, approval))

        scored_tasks.sort(key=lambda pair: (-pair[0], pair[1].id or 0))
        scored_improvements.sort(key=lambda pair: (-pair[0], pair[1].id or 0))
        scored_approvals.sort(key=lambda pair: (-pair[0], pair[1].id or 0))

        return {
            "keywords": keywords,
            "tasks": [task for _, task in scored_tasks[:5]],
            "improvements": [imp for _, imp in scored_improvements[:5]],
            "approvals": [approval for _, approval in scored_approvals[:5]],
        }

    def _format_existing_work_status(self, message: str, existing: dict, preface: Optional[str] = None) -> str:
        lines = ["<b>Existing related work</b>"]
        if preface:
            lines.extend(["", preface])

        tasks = existing.get("tasks", [])
        improvements = existing.get("improvements", [])
        approvals = existing.get("approvals", [])

        if tasks:
            lines.extend(["", "<b>Tasks</b>"])
            for task in tasks[:4]:
                lines.append(
                    f"- Task #{task.id} [{task.to_agent}/{task.status}] {task.description[:120]}"
                )
        if improvements:
            lines.extend(["", "<b>Improvements</b>"])
            for imp in improvements[:4]:
                lines.append(
                    f"- Improvement #{imp.id} [{imp.status}] {imp.title[:120]}"
                )
        if approvals:
            lines.extend(["", "<b>Pending approvals</b>"])
            for approval in approvals[:4]:
                task_part = f" · task #{approval.task_id}" if approval.task_id else ""
                lines.append(
                    f"- Approval #{approval.id} [{approval.request_type}]{task_part} {approval.description[:120]}"
                )

        lines.extend([
            "",
            "I did not create new work for this question.",
            "<i>Ask for more detail if you want the exact task chain or why it is blocked.</i>",
        ])
        return "\n".join(lines)

    def synthesize_investigation(self, investigation_result: dict, task_id: int):
        if not investigation_result.get("forge_recommended"):
            return None
        scope = investigation_result.get("forge_scope", "unknown")
        if scope == "large_refactor":
            return None
        facts = investigation_result.get("verified_facts", [])
        causes = investigation_result.get("likely_causes", [])
        if not facts and not causes:
            return None
        description = investigation_result.get("forge_description", "") or investigation_result.get("summary", "")
        return {
            "title": f"System improvement: {description[:80]}",
            "description": description,
            "problem_statement": investigation_result.get("summary", ""),
            "evidence_task_id": task_id,
            "verified_facts": facts[:6],
            "likely_causes": causes[:4],
            "affected_components": investigation_result.get("affected_components", [])[:8],
            "scope": scope,
            "risk_level": investigation_result.get("risk_level", "medium"),
            "priority": investigation_result.get("priority", "normal"),
            "requires_approval": True,
            "suggested_validation": "Sentinel should run compile checks, config sanity, and relevant tests on affected components.",
        }

    # â”€â”€ Classification â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _classify(self, message: str) -> tuple[str, str]:
        # Fast path: no LLM call needed for obvious status queries
        fast = _rules_classify(message)
        if fast:
            logger.info("Fast-path classification: %s", fast[0])
            return fast

        try:
            raw = self.coordinator_llm.complete(
                messages=[{"role": "user", "content": message}],
                system=_CLASSIFY_SYSTEM,
                name="classify",
                timeout=int(self.roderick_config.get("classification_timeout_seconds", 12)),
            )
            data = json.loads(raw.strip())
            return data.get("category", "direct_answer"), data.get("summary", message[:40])
        except Exception as e:
            logger.warning("Classification failed (%s), defaulting to direct_answer", e)
            return "direct_answer", message[:40]

    # â”€â”€ Agentic loop (direct answers, reminders, personal admin) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _run_agentic(self, message: str) -> str:
        max_history = self.config.get("llm", {}).get("max_history", 20)
        with self._history_lock:
            self.history.append({"role": "user", "content": message})
            if len(self.history) > max_history * 2:
                self.history = self.history[-(max_history * 2):]
            history_snapshot = list(self.history)

        system = self._build_system_prompt()
        tools = TOOLS if self._needs_devops_tools(message) else SAFE_TOOLS
        response = self.llm.run_agentic_loop(
            messages=history_snapshot,
            system=system,
            tools=tools,
            tool_executor=self._execute_tool,
        )
        with self._history_lock:
            self.history.append({"role": "assistant", "content": response})
        logger.info("Agentic response (%d chars)", len(response))
        return response

    def _needs_devops_tools(self, message: str) -> bool:
        return bool(_DEVOPS_TOOL_PATTERNS.search(message))

    def _build_system_prompt(self) -> str:
        context = self.memory.get_context_summary()
        owner_ctx = self.owner_memory.get_context() if self.owner_memory else ""
        owner_section = ("\nOwner context:\n" + owner_ctx + "\n") if owner_ctx else ""
        agent_status = self.registry.get_status_summary()
        try:
            from shared.db.approvals import list_pending_approvals
            _pend = list_pending_approvals(self.db_path)
            pending_section = ("\nPending approvals (" + str(len(_pend)) + "): " +
                "; ".join(a.description[:60] for a in _pend[:3])) if _pend else ""
        except Exception:
            pending_section = ""
        return (
            "TRUTHFULNESS PROTOCOL: Only state what you can verify from the current context or tool results. "
            "Label uncertain information as [inferred] or [unverified]. "
            "Never claim an action was taken unless you see confirmation. "
            "If you don\'t know something, say so directly.\n\n"
            "You are Roderick, the personal AI assistant and orchestrator for the system owner "
            ".\n\n"
            "You report directly to the system owner. You coordinate specialist agents:\n"
            "\u2022 Merlin (research) \u2022 Forge (builder, requires approval) \u2022 Zuko (job search)\n"
            "\u2022 Venture (business opportunities) \u2022 Atlas (skill tutor) \u2022 Sentinel (QA)\n\n"
            "Your role: manage the owner\'s life admin, schedule, reminders, "
            "work oversight, business opportunities, and agent coordination.\n\n"
            "Communication style: concise and direct. Lead with the answer. "
            "Use HTML formatting for structured output.\n\n"
            + owner_section
            + "Current context:\n" + context
            + "\nAgent status:\n" + agent_status
            + pending_section
        )

    def _run_ecosystem_council(self) -> str:
        proposal = self._ecosystem_council_proposal()
        if not proposal.get("should_propose"):
            return (
                "<b>Ecosystem council</b>\n\n"
                f"{proposal.get('summary', 'No concrete build proposal right now.')}"
            )
        return (
            "<b>Ecosystem council</b>\n\n"
            f"{self._format_ecosystem_proposal(proposal)}\n\n"
            "<i>Ask Roderick to propose the ecosystem improvement when you want Forge to review it.</i>"
        )

    def _ecosystem_council_proposal(self) -> dict:
        context = {
            "agent_status": self.registry.get_status_summary(),
            "recent_tasks": [t.__dict__ for t in list_tasks(self.db_path, limit=12)],
            "learning_notes": self._read_learning_notes(),
            "roderick_inbox": self._read_roderick_inbox(),
            "owner_context": self.owner_memory.get_context() if self.owner_memory else "",
        }
        raw = self.llm.complete(
            messages=[{"role": "user", "content": json.dumps(context, ensure_ascii=False, indent=2)}],
            system=(
                "You are Roderick running an ecosystem council for the owner's agent system. "
                "Review what the agents learned and decide whether Forge should propose a concrete system improvement. "
                "Be ambitious about improving the owner's quality of life, but only propose safe, scoped work. "
                "Return valid JSON only with keys: should_propose, title, summary, rationale, requested_change, "
                "priority, urgency, affected_agents, risks, success_criteria. "
                "If no clear build is worth approval, set should_propose=false and explain in summary."
            ),
        )
        try:
            proposal = json.loads(raw.strip())
        except json.JSONDecodeError:
            proposal = {
                "should_propose": False,
                "summary": raw[:500],
                "title": "",
                "rationale": "",
                "requested_change": "",
                "priority": "normal",
                "urgency": "this_week",
                "affected_agents": [],
                "risks": [],
                "success_criteria": [],
            }
        proposal.setdefault("priority", "normal")
        proposal.setdefault("urgency", "this_week")
        return proposal

    def _read_learning_notes(self) -> dict:
        learning_dir = Path(self.config["data_dir"]) / "agent_learning"
        if not learning_dir.exists():
            return {}
        notes = {}
        for path in sorted(learning_dir.glob("*.md")):
            try:
                notes[path.stem] = path.read_text(encoding="utf-8")[-3000:]
            except Exception:
                notes[path.stem] = "(unreadable)"
        return notes

    def _read_roderick_inbox(self) -> list[dict]:
        messages = get_unread_messages(self.db_path, "roderick", limit=20)
        result = []
        for msg in messages:
            result.append(msg.__dict__)
            if msg.id is not None:
                mark_message_read(self.db_path, msg.id)
        return result

    def _format_ecosystem_proposal(self, proposal: dict) -> str:
        lines = [
            f"<b>{proposal.get('title') or 'System improvement proposal'}</b>",
            proposal.get("summary", "")[:700],
        ]
        rationale = proposal.get("rationale")
        if rationale:
            lines.append(f"\n<b>Why:</b> {rationale[:700]}")
        requested = proposal.get("requested_change")
        if requested:
            lines.append(f"\n<b>Forge request:</b> {requested[:1000]}")
        criteria = proposal.get("success_criteria", [])
        if criteria:
            lines.append("\n<b>Success criteria:</b>")
            for item in criteria[:5]:
                lines.append(f"  • {str(item)[:160]}")
        risks = proposal.get("risks", [])
        if risks:
            lines.append("\n<b>Risks:</b>")
            for item in risks[:3]:
                lines.append(f"  • {str(item)[:160]}")
        return "\n".join(lines)

    # â”€â”€ Tool implementations â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _execute_tool(self, name: str, inputs: dict) -> str:
        try:
            if name == "save_reminder":
                return self._tool_save_reminder(inputs)
            elif name == "list_reminders":
                return self._tool_list_reminders(inputs)
            elif name == "mark_reminder_done":
                mark_done(self.db_path, int(inputs["reminder_id"]))
                return f"Reminder #{inputs['reminder_id']} marked done."
            elif name == "get_agent_status":
                return self.registry.get_status_summary()
            elif name == "run_command":
                return self._tool_run_command(inputs)
            elif name == "list_files":
                return self._tool_list_files(inputs.get("path", ""))
            elif name == "read_file":
                return self._tool_read_file(inputs["path"])
            else:
                return f"Unknown tool: {name}"
        except Exception as e:
            logger.error("Tool '%s' error: %s", name, e)
            return f"Tool error ({name}): {e}"

    def _tool_save_reminder(self, inputs: dict) -> str:
        r = save_reminder(
            self.db_path,
            Reminder(
                text=inputs["text"],
                due=inputs.get("due"),
                category=inputs.get("category", "personal"),
            ),
        )
        due_str = f" (due: {r.due})" if r.due else ""
        return f"Reminder #{r.id} saved [{r.category}]{due_str}: {r.text}"

    def _tool_list_reminders(self, inputs: dict) -> str:
        include_done = inputs.get("include_done", False)
        reminders = list_reminders(
            self.db_path,
            category=inputs.get("category"),
            done=None if include_done else False,
            limit=15,
        )
        if not reminders:
            return "No reminders found."
        lines = []
        for r in reminders:
            status = "âœ“" if r.done else "â€¢"
            due = f" [{r.due[:16]}]" if r.due else ""
            lines.append(f"{status} #{r.id} [{r.category}]{due} {r.text}")
        return "\n".join(lines)

    def _tool_run_command(self, inputs: dict) -> str:
        command = inputs["command"]
        working_dir = inputs.get("working_dir", "")
        timeout = min(int(inputs.get("timeout", 30)), 120)

        base = Path(self.devops_root)
        cwd = base / working_dir if working_dir else base
        if not cwd.exists():
            return f"Error: directory '{cwd}' does not exist"

        blocked = ["rm -rf /", "mkfs", "dd if=", "> /dev/sd"]
        for b in blocked:
            if b in command:
                return f"Blocked: command contains '{b}'"

        try:
            result = subprocess.run(
                command, shell=True, cwd=str(cwd),
                capture_output=True, text=True, timeout=timeout,
            )
            output = (result.stdout + result.stderr).strip()
            if not output:
                output = f"(exit code {result.returncode}, no output)"
            if len(output) > 3000:
                output = output[:3000] + "\nâ€¦ (truncated)"
            return output
        except subprocess.TimeoutExpired:
            return f"Command timed out after {timeout}s"
        except Exception as e:
            return f"Error: {e}"

    def _tool_list_files(self, rel_path: str) -> str:
        target = Path(self.devops_root) / rel_path if rel_path else Path(self.devops_root)
        if not target.exists():
            return f"Path does not exist: {target}"
        try:
            entries = sorted(target.iterdir(), key=lambda p: p.name)
            return "\n".join(
                f"[{'dir' if e.is_dir() else 'file'}] {e.name}" for e in entries
            ) or "(empty)"
        except Exception as e:
            return f"Error: {e}"

    def _tool_read_file(self, rel_path: str) -> str:
        target = Path(self.devops_root) / rel_path
        if not target.is_file():
            return f"File not found: {target}"
        try:
            content = target.read_text(encoding="utf-8", errors="replace")
            if len(content) > 4000:
                content = content[:4000] + "\nâ€¦ (truncated)"
            return content
        except Exception as e:
            return f"Error: {e}"

    async def _route_behavior_change(self, message: str) -> str:
        """Translate a natural-language behavior instruction into a policy proposal."""
        from shared.db.behavior import BehaviorPolicy, upsert_policy

        # Ask LLM to parse the instruction into a structured policy proposal
        parse_prompt = (
            "You are Roderick, translating a user instruction into a structured agent behavior policy.\n\n"
            "User said: " + repr(message) + "\n\n"
            "Return JSON with:\n"
            "  agent (str): one of merlin|forge|venture|atlas|sentinel|zuko\n"
            "  policy_key (str): snake_case key e.g. source_preference, scan_cadence, quality_gate\n"
            "  policy_value (str): the new value\n"
            "  description (str): plain-English description of the change\n"
            "  requires_approval (bool): true if this affects external actions, shell exec, job apps, high-freq 30B usage, Git, spending\n"
            "  risk_level (str): low|medium|high\n"
            "Return valid JSON only."
        )
        try:
            import json as _json
            raw = self.coordinator_llm.complete(
                messages=[{"role": "user", "content": parse_prompt}],
                system="You are Roderick. Return valid JSON only.",
                name="behavior_parse",
                timeout=int(self.roderick_config.get("behavior_parse_timeout_seconds", 20)),
            )
            proposal = _json.loads(raw.strip())
        except Exception as e:
            return f"⚠️ Could not parse behavior instruction: {e}\n\n<i>Try: \"Zuko should scan LinkedIn more often\"</i>"

        agent = proposal.get("agent", "")
        policy_key = proposal.get("policy_key", "")
        policy_value = proposal.get("policy_value", "")
        description = proposal.get("description", "")
        requires_approval = bool(proposal.get("requires_approval", False))
        risk = proposal.get("risk_level", "low")

        if not agent or not policy_key:
            return "⚠️ Could not identify agent or policy key from instruction."

        status = "proposed" if requires_approval else "applied"
        pol = upsert_policy(self.db_path, BehaviorPolicy(
            agent=agent,
            policy_key=policy_key,
            policy_value=policy_value,
            description=description,
            status=status,
            origin="user",
            changed_by="roderick",
            requires_approval=requires_approval,
        ))

        from shared.db.events import emit_event as _emit
        _emit(self.db_path, "behavior_policy_proposed", "roderick", {
            "agent": agent, "policy_key": policy_key, "status": status, "risk_level": risk,
        })

        if requires_approval:
            return (
                f"📋 <b>Behavior change proposed (approval required)</b>\n\n"
                f"<b>Agent:</b> {agent.capitalize()}\n"
                f"<b>Policy:</b> <code>{policy_key}</code> = <code>{policy_value}</code>\n"
                f"<b>Description:</b> {description}\n"
                f"<b>Risk:</b> {risk}\n\n"
                f"<i>Review on the dashboard → Behaviors tab and approve to apply.</i>"
            )
        else:
            return (
                f"✅ <b>Behavior applied immediately</b>\n\n"
                f"<b>Agent:</b> {agent.capitalize()}\n"
                f"<b>Policy:</b> <code>{policy_key}</code> = <code>{policy_value}</code>\n"
                f"<b>Description:</b> {description}\n\n"
                f"<i>Low-risk change — applied without approval. {agent.capitalize()} will use this at the next task boundary.</i>"
            )
