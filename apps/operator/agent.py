"""
Operator — business execution agent.

Sits downstream of Venture. Takes approved business initiatives and turns them
into concrete, trackable actions. Coordinates Forge, Merlin, Sentinel, and Venture
for execution work on configurable business ventures.

Approval gates enforced (no exceptions):
  - Any spend of money → requires approval before acting
  - Any external outreach → requires approval before acting
  - Any identity/branding action → requires approval before acting
  - Any vendor engagement → requires approval before acting

Operator does NOT:
  - Reinvent strategy (Venture owns strategy)
  - Bypass approval controls
  - Conflate personal identity with business identity
  - Act as a general chatbot
"""
import asyncio
import contextlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Coroutine, Optional

from shared.db.agents import emit_heartbeat, record_agent_error, record_agent_success, update_agent_status
from shared.db.events import emit_event
from shared.db.messages import get_unread_messages, mark_message_read, send_agent_message
from shared.db.tasks import get_next_task, requeue_in_progress_tasks, touch_task, update_task_status
from shared.agent_learning import try_reflect_after_task
from shared.llm.anthropic_provider import AnthropicProvider
from shared.memory.founder import OwnerMemory
from shared.schemas.task import Task

logger = logging.getLogger(__name__)

AGENT_NAME = "operator"

# Task types that require approval before producing any external-facing action
_OUTREACH_TYPES = {"outreach_preparation", "vendor_setup"}
# All task types accepted by Operator
_TASK_TYPES = {
    "initiative_execution",
    "business_ops",
    "launch_checklist",
    "vendor_setup",
    "outreach_preparation",
    "decision_brief",
    "execution_followup",
    "cashflow_action",
    "milestone_review",
}


class OperatorAgent:
    def __init__(
        self,
        llm: AnthropicProvider,
        db_path: str,
        data_dir: str,
        config: dict,
        owner_memory: OwnerMemory,
    ):
        self.llm = llm
        self.db_path = db_path
        self.reports_dir = Path(data_dir) / "reports"
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self.owner_memory = owner_memory

        opcfg = config.get("operator", {})
        self.prompt_template = opcfg.get(
            "execution_prompt_template",
            (
                "You are Operator, a business execution agent working for the system owner.\n"
                "Your role is to manage approved business initiatives and produce concrete, "
                "trackable action plans. Venture owns strategy — you own execution.\n\n"
                "Approval gates (no exceptions): any spend, outreach, branding, or vendor "
                "engagement requires approval before action.\n\n"
                "Owner context:\n{owner_context}\n\n"
                "Business operations context:\n{business_context}\n\n"
                "Active initiatives:\n{initiatives_context}\n\n"
                "Task: {task}"
            ),
        )
        self.approval_gates = opcfg.get(
            "approval_gates",
            {
                "spend_threshold": 0,
                "outreach_requires_approval": True,
                "identity_actions_require_approval": True,
                "vendor_engagement_requires_approval": True,
            },
        )
        self.poll_interval = int(config.get("scheduler", {}).get("worker_poll_interval_seconds", 10))
        self.keepalive_seconds = max(10, int(config.get("observability", {}).get("task_keepalive_seconds", 20)))

        self._notify: Optional[Callable[[str], Coroutine]] = None
        self._send_approval: Optional[Callable[[str, int, str, dict], Coroutine]] = None

    def set_notify(self, fn: Callable[[str], Coroutine]) -> None:
        self._notify = fn

    def set_approval_sender(self, fn: Callable[[str, int, str, dict], Coroutine]) -> None:
        """fn(description, task_id, request_type, payload={})"""
        self._send_approval = fn

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def run(self) -> None:
        logger.info("Operator worker started")
        recovered = requeue_in_progress_tasks(self.db_path, AGENT_NAME)
        if recovered:
            logger.info("Operator recovered %d abandoned in-progress task(s)", recovered)
        update_agent_status(self.db_path, AGENT_NAME, "idle")
        emit_heartbeat(self.db_path, AGENT_NAME, current_task_id=None, current_model=self.llm.model)

        while True:
            try:
                unread = get_unread_messages(self.db_path, AGENT_NAME, limit=5)
                if unread:
                    for message in unread:
                        await self._handle_message(message)
                    continue
                task = get_next_task(self.db_path, AGENT_NAME, "pending")
                if task:
                    await self._process(task)
                else:
                    await asyncio.sleep(self.poll_interval)
            except Exception as e:
                logger.error("Operator worker error: %s", e, exc_info=True)
                await asyncio.sleep(self.poll_interval)

    async def _process(self, task: Task) -> None:
        logger.info("Operator processing task #%d: %s (%s)", task.id, task.task_type, task.description[:60])
        update_agent_status(self.db_path, AGENT_NAME, "busy", f"Executing: {task.description[:60]}")
        emit_heartbeat(self.db_path, AGENT_NAME, current_task_id=task.id, current_model=self.llm.model)
        update_task_status(self.db_path, task.id, "in_progress")

        try:
            result = await self._run_with_keepalive(
                task,
                f"Executing: {task.description[:60]}",
                lambda: self._execute(task),
                self.llm.model,
            )

            report_path = self.reports_dir / f"operator_{task.id}.json"
            report_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
            update_task_status(self.db_path, task.id, "completed", result)
            update_agent_status(self.db_path, AGENT_NAME, "idle", f"Completed task #{task.id}")
            record_agent_success(self.db_path, AGENT_NAME, f"Task #{task.id} done: {result.get('task_summary', '')[:60]}")
            emit_event(
                self.db_path,
                "operator_task_complete",
                AGENT_NAME,
                {"task_id": task.id, "initiative": result.get("initiative", ""), "status": result.get("status", "")},
            )

            # Send approval request if the result says one is needed
            if result.get("approval_required") and self._send_approval:
                reason = result.get("approval_reason", "Operator requires approval before proceeding.")
                summary = result.get("task_summary", task.description)[:200]
                await self._send_approval(
                    f"Operator — {summary}\n\nReason: {reason}",
                    task.id,
                    "operator_approval",
                    {"result": result},
                )

            if self._notify:
                await self._notify(self._format_result(task, result))

            try_reflect_after_task(
                llm=self.llm,
                db_path=self.db_path,
                data_dir=str(self.reports_dir.parent),
                agent_name=AGENT_NAME,
                task=task,
                result=result,
                owner_context=self.owner_memory.get_context(),
            )

        except Exception as e:
            logger.error("Operator task #%d failed: %s", task.id, e, exc_info=True)
            update_task_status(self.db_path, task.id, "failed", {"error": str(e)})
            update_agent_status(self.db_path, AGENT_NAME, "idle")
            record_agent_error(self.db_path, AGENT_NAME, str(e))
            if self._notify:
                await self._notify(
                    f"⚠️ <b>Operator — Task #{task.id} failed</b>\n\nError: {e}"
                )

        finally:
            emit_heartbeat(self.db_path, AGENT_NAME, current_task_id=None, current_model=self.llm.model)

    async def _handle_message(self, message) -> None:
        update_agent_status(self.db_path, AGENT_NAME, "busy", "Responding to dashboard chat")
        emit_heartbeat(self.db_path, AGENT_NAME, current_task_id=None, current_model=self.llm.model)
        try:
            response = await asyncio.get_event_loop().run_in_executor(None, lambda: self._respond_to_message(message.message))
        except Exception as exc:
            logger.error("Operator message #%s failed: %s", message.id, exc, exc_info=True)
            response = "I hit an internal error while reading that. The task queue is still intact, so I can pick this back up."
        send_agent_message(self.db_path, AGENT_NAME, "dashboard", response, priority="normal")
        mark_message_read(self.db_path, message.id)
        emit_event(
            self.db_path,
            "operator_dashboard_reply",
            AGENT_NAME,
            {"message_id": message.id, "summary": response[:300]},
        )
        update_agent_status(self.db_path, AGENT_NAME, "idle", "Dashboard reply sent")

    # ── Execution ──────────────────────────────────────────────────────────────

    def _execute(self, task: Task) -> dict:
        """
        Single LLM call for all task types. The prompt template differentiates
        behaviour by task type via the task description. Returns structured JSON result.
        """
        # Pre-flight approval gate check
        if self._requires_pre_approval(task):
            return {
                "task_summary": f"Approval required before executing: {task.description[:100]}",
                "initiative": task.payload.get("initiative", "unknown"),
                "status": "needs_approval",
                "actions_taken": [],
                "next_actions": ["Await owner approval before proceeding"],
                "blockers": ["Approval required before this action can be taken"],
                "approval_required": True,
                "approval_reason": self._pre_approval_reason(task),
                "agents_to_coordinate": [],
                "capital_impact": "None until approved",
                "confidence": "high",
            }

        owner_context = self.owner_memory.get_context()
        business_context = self.owner_memory.get_business_context()
        # Limit initiatives context to avoid prompt bloat
        initiatives_context = business_context[:3000] if len(business_context) > 3000 else business_context

        task_desc = f"[{task.task_type}] {task.description}"
        if task.payload:
            relevant = {k: v for k, v in task.payload.items() if k not in ("notify_user", "source")}
            if relevant:
                task_desc += f"\n\nAdditional context:\n{json.dumps(relevant, indent=2)}"

        prompt = self.prompt_template.format(
            owner_context=owner_context,
            business_context=business_context,
            initiatives_context=initiatives_context,
            task=task_desc,
        )

        raw = self.llm.complete(
            messages=[{"role": "user", "content": prompt}],
            system=(
                "You are Operator, a business execution agent. "
                "Respond with valid JSON only. No markdown, no preamble. "
                "Every action that involves spending money, external outreach, or identity "
                "decisions MUST set approval_required=true."
            ),
            name="operator_execute",
        )

        try:
            result = json.loads(raw.strip())
        except json.JSONDecodeError:
            # Graceful fallback — keep task traceable
            result = {
                "task_summary": task.description[:200],
                "initiative": task.payload.get("initiative", "unknown"),
                "status": "completed",
                "actions_taken": [raw[:500]],
                "next_actions": [],
                "blockers": [],
                "approval_required": False,
                "approval_reason": "",
                "agents_to_coordinate": [],
                "capital_impact": "none",
                "confidence": "low",
                "_parse_warning": "LLM did not return valid JSON; raw response captured in actions_taken",
            }

        result["_task_id"] = task.id
        result["_task_type"] = task.task_type
        result["_executed_at"] = datetime.now(timezone.utc).isoformat()

        # Enforce approval gate: if result mentions spend/outreach/identity and
        # doesn't set approval_required, set it defensively.
        if not result.get("approval_required"):
            capital_impact = str(result.get("capital_impact", "")).lower()
            if capital_impact and capital_impact not in {"none", "nil", "n/a", "0", "$0"}:
                result["approval_required"] = True
                result["approval_reason"] = (
                    result.get("approval_reason")
                    or "Capital impact detected — approval required before proceeding."
                )

        return result

    def _respond_to_message(self, message: str) -> str:
        owner_context = self.owner_memory.get_context()
        business_context = self.owner_memory.get_business_context()
        prompt = (
            "You are Operator, the owner's business execution operator.\n"
            "Reply in clean, direct prose with short sections if helpful.\n"
            "Do not return JSON. Do not act like a generic assistant.\n"
            "Acknowledge what is already in motion, note any approvals/blockers, and name the next concrete action.\n\n"
            f"Owner context:\n{owner_context[:2500]}\n\n"
            f"Business context:\n{business_context[:2500]}\n\n"
            f"Dashboard message:\n{message}"
        )
        response = self.llm.complete(
            messages=[{"role": "user", "content": prompt}],
            system=(
                "You are Operator. Reply in plain English only. "
                "Use concise execution language, mention approvals if needed, and never return JSON."
            ),
            name="operator_dashboard_chat",
        )
        return response.strip() or "I’ve logged that and I’m ready to move it into the execution queue when you want."

    def _requires_pre_approval(self, task: Task) -> bool:
        """
        Block certain high-risk task types at the gate before even calling the LLM.
        Only applies when the task explicitly signals an external action is imminent.
        """
        if task.task_type in _OUTREACH_TYPES:
            # These task types always require pre-approval if payload flags immediate action
            return task.payload.get("execute_immediately", False)
        return False

    def _pre_approval_reason(self, task: Task) -> str:
        if task.task_type == "outreach_preparation":
            return "Outreach to external parties requires explicit owner approval before sending."
        if task.task_type == "vendor_setup":
            return "Vendor engagement and any associated spend requires explicit owner approval."
        return "This action requires explicit approval before execution."

    # ── Formatting ─────────────────────────────────────────────────────────────

    def _format_result(self, task: Task, result: dict) -> str:
        initiative = result.get("initiative", "")
        status = result.get("status", "")
        summary = result.get("task_summary", task.description)[:200]
        next_actions = result.get("next_actions", [])
        blockers = result.get("blockers", [])
        approval_required = result.get("approval_required", False)

        lines = [f"⚙️ <b>Operator — Task #{task.id}</b>"]
        if initiative:
            lines.append(f"<b>Initiative:</b> {initiative}")
        lines.append(f"<b>Status:</b> {status}")
        lines.append(f"<b>Summary:</b> {summary}")

        if next_actions:
            lines.append("\n<b>Next actions:</b>")
            for a in next_actions[:4]:
                lines.append(f"  • {str(a)[:120]}")

        if blockers:
            lines.append("\n<b>Blockers:</b>")
            for b in blockers[:3]:
                lines.append(f"  ⚠ {str(b)[:120]}")

        if approval_required:
            reason = result.get("approval_reason", "")[:200]
            lines.append(f"\n🔐 <b>Approval required:</b> {reason}")

        return "\n".join(lines)

    # ── Keepalive ──────────────────────────────────────────────────────────────

    async def _run_with_keepalive(self, task: Task, label: str, fn, current_model: str):
        keepalive = asyncio.create_task(self._keepalive_loop(task.id, label, current_model))
        try:
            return await asyncio.get_event_loop().run_in_executor(None, fn)
        finally:
            keepalive.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await keepalive

    async def _keepalive_loop(self, task_id: int, label: str, current_model: str) -> None:
        while True:
            await asyncio.sleep(self.keepalive_seconds)
            update_agent_status(self.db_path, AGENT_NAME, "busy", label)
            emit_heartbeat(self.db_path, AGENT_NAME, current_task_id=task_id, current_model=current_model)
            touch_task(self.db_path, task_id)
