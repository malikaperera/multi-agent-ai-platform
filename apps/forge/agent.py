"""
Forge — builder agent.

Two-stage approval flow:
  Stage 1: Task arrives with status='approved' (first approval done by user).
           Forge creates an implementation plan. Status → 'plan_ready'.
           Roderick sends plan to user for second approval.

  Stage 2: Task arrives with status='plan_approved' (second approval done by user).
           Forge may now create/edit files. Status → 'completed'.

Forge NEVER creates files before plan_approved status.
"""
import asyncio
import contextlib
import json
import logging
import os
import re
import string
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Coroutine, Optional

from shared.db.agents import emit_heartbeat, update_agent_status
from shared.db.artifacts import ForgeArtifact, record_forge_artifact
from shared.db.context import format_approval_decisions, get_recent_approval_decisions
from shared.db.events import emit_event
from shared.db.messages import get_unread_messages, mark_message_read
from shared.db.tasks import enqueue_task, get_next_task, get_task, list_tasks, touch_task, update_task_status
from shared.agent_learning import try_reflect_after_task
from shared.llm.anthropic_provider import AnthropicProvider
from shared.memory.founder import OwnerMemory
from shared.schemas.task import Task
from shared.task_priority import sort_key

logger = logging.getLogger(__name__)

AGENT_NAME = "forge"


class ForgeAgent:
    def __init__(
        self,
        llm: AnthropicProvider,
        db_path: str,
        data_dir: str,
        config: dict,
        owner_memory: Optional["OwnerMemory"] = None,
        coder_llm: Optional[AnthropicProvider] = None,
        planner_model: str = "qwen3:30b",
        coder_model: str = "qwen2.5-coder:14b",
    ):
        # planner_llm = the reasoning model (qwen3:30b) — planning, tradeoffs, approval summaries
        # coder_llm   = the code model (qwen2.5-coder:14b) — code gen, patches, self-review
        self.planner_llm = llm
        self.coder_llm = coder_llm if coder_llm is not None else llm
        self.llm = llm  # kept for backward-compat (heartbeat model reporting)
        self.planner_model = planner_model
        self.coder_model = coder_model if coder_llm is not None else llm.model
        self.config = config
        self.db_path = db_path
        self.reports_dir = Path(data_dir) / "reports"
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir = Path(data_dir) / "forge_artifacts"
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.repo_root = self._resolve_repo_root()
        self.compose_timeout_seconds = int(
            config.get("forge", {}).get("deployment_timeout_seconds", 1200)
        )
        self.owner_memory = owner_memory
        self.planning_template = config.get("forge", {}).get(
            "planning_prompt_template",
            "Create a detailed implementation plan for: {request}\n\nReturn valid JSON only.",
        )
        self.poll_interval = int(
            config.get("scheduler", {}).get("worker_poll_interval_seconds", 10)
        )
        self.keepalive_seconds = max(10, int(config.get("observability", {}).get("task_keepalive_seconds", 20)))
        self._notify: Optional[Callable[[str], Coroutine]] = None
        self._send_approval: Optional[Callable[[str, int], Coroutine]] = None
        # When True, Forge auto-advances system_improvement tasks through both approval
        # gates (pending→approved and plan_ready→plan_approved) without user intervention.
        self.auto_approve_improvements = bool(
            config.get("forge", {}).get("auto_approve_improvements", False)
        )

    def set_notify(self, fn: Callable[[str], Coroutine]) -> None:
        self._notify = fn

    def set_approval_sender(self, fn: Callable[[str, int], Coroutine]) -> None:
        """Inject async fn(description, task_id) that sends a plan_approval keyboard."""
        self._send_approval = fn

    async def run(self) -> None:
        logger.info("Forge worker started (poll interval %ds)", self.poll_interval)
        self._recover_abandoned_work()
        self._collapse_duplicate_pending_improvements()
        update_agent_status(self.db_path, AGENT_NAME, "idle")
        emit_heartbeat(self.db_path, AGENT_NAME, current_task_id=None, current_model=f"{self.planner_model}|{self.coder_model}")

        while True:
            try:
                # Auto-approval gate: advance system_improvement tasks that are
                # stuck at pending or plan_ready without requiring user input.
                if self.auto_approve_improvements:
                    now_iso = datetime.now(timezone.utc).isoformat()
                    for _t in list_tasks(self.db_path, to_agent=AGENT_NAME, limit=50):
                        if _t.task_type != "system_improvement":
                            continue
                        if _t.status == "pending":
                            update_task_status(self.db_path, _t.id, "approved", {
                                "auto_approved_by": "forge",
                                "auto_approved_at": now_iso,
                            })
                            emit_event(self.db_path, "forge_auto_approved", AGENT_NAME, {"task_id": _t.id, "from_status": "pending"})
                            logger.info("Forge auto-approved pending system_improvement #%d", _t.id)
                        elif _t.status == "plan_ready":
                            update_task_status(self.db_path, _t.id, "plan_approved", {
                                **(_t.result or {}),
                                "auto_plan_approved_by": "forge",
                                "auto_plan_approved_at": now_iso,
                            })
                            emit_event(self.db_path, "forge_auto_approved", AGENT_NAME, {"task_id": _t.id, "from_status": "plan_ready"})
                            logger.info("Forge auto-approved plan for system_improvement #%d", _t.id)

                # Stage 0: critical/high Sentinel feedback should not wait behind routine planning.
                msgs = get_unread_messages(self.db_path, AGENT_NAME, limit=5)
                urgent_revision_msgs = [
                    m for m in msgs
                    if m.priority in ("critical", "high")
                    and ("revision" in m.message.lower() or "blocked" in m.message.lower())
                ]
                if urgent_revision_msgs:
                    await self._handle_revision_messages(urgent_revision_msgs)
                    continue

                # Stage 1/2: choose the highest-priority actionable task across planning and implementation.
                # This prevents urgent security fixes from sitting behind routine approved plans.
                task = self._next_actionable_task()
                if task:
                    if task.status == "approved":
                        if task.task_type == "system_improvement":
                            await self._plan_system_improvement(task)
                        else:
                            await self._plan(task)
                    elif task.status == "plan_approved":
                        if task.task_type == "system_improvement":
                            await self._implement_system_improvement(task)
                        else:
                            await self._implement(task)
                    else:
                        logger.warning("Forge skipped unexpected actionable task #%s status=%s", task.id, task.status)
                    continue

                # Stage 3: check for non-urgent Sentinel revision messages
                revision_msgs = [m for m in msgs if "revision" in m.message.lower() or m.priority in ("high", "critical")]
                if revision_msgs:
                    await self._handle_revision_messages(revision_msgs)
                    continue

                live_patch = self._next_deployable_live_task()
                if live_patch:
                    await self._deploy_promoted_system_improvement(live_patch)
                    continue

                await asyncio.sleep(self.poll_interval)

            except Exception as e:
                logger.error("Forge worker error: %s", e, exc_info=True)
                await asyncio.sleep(self.poll_interval)

    def _next_actionable_task(self) -> Optional[Task]:
        tasks: list[Task] = []
        for status in ("approved", "plan_approved"):
            task = get_next_task(self.db_path, AGENT_NAME, status)
            if task:
                tasks.append(task)
        if not tasks:
            return None
        return sorted(tasks, key=sort_key)[0]

    def _resolve_repo_root(self) -> Path:
        configured = self.config.get("forge", {}).get("repo_root") or os.environ.get("REPO_ROOT", "")
        candidates = [
            Path(configured) if configured else None,
            Path(os.environ.get("DEVOPS_ROOT", "/devops")) / "telegram-claude-agent",
            Path.cwd(),
        ]
        for candidate in candidates:
            if candidate and candidate.exists():
                return candidate
        return Path.cwd()

    def _resolve_repo_path(self, file_path_str: str) -> Path:
        fp = Path(file_path_str)
        if fp.is_absolute():
            return fp
        return (self.repo_root / fp).resolve()

    @staticmethod
    def _merge_result(task: Task, **updates) -> dict:
        result = task.result.copy() if isinstance(task.result, dict) else {}
        result.update(updates)
        return result

    def _deployment_state(self, task: Task) -> str:
        result = task.result if isinstance(task.result, dict) else {}
        deployment = result.get("deployment", {})
        if isinstance(deployment, dict):
            return str(deployment.get("state", "pending"))
        return "pending"

    def _next_deployable_live_task(self) -> Optional[Task]:
        live_tasks = [
            task for task in list_tasks(self.db_path, to_agent=AGENT_NAME, limit=120)
            if task.task_type == "system_improvement" and task.status == "live"
        ]
        for task in live_tasks:
            if self._deployment_state(task) not in {"deploying", "deployed", "not_required"}:
                return task
        return None

    async def _run_with_keepalive(self, task: Task, label: str, fn):
        keepalive = asyncio.create_task(self._keepalive_loop(task.id, label))
        try:
            return await asyncio.get_event_loop().run_in_executor(None, fn)
        finally:
            keepalive.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await keepalive

    async def _keepalive_loop(self, task_id: int, label: str) -> None:
        while True:
            await asyncio.sleep(self.keepalive_seconds)
            update_agent_status(self.db_path, AGENT_NAME, "busy", label)
            emit_heartbeat(
                self.db_path,
                AGENT_NAME,
                current_task_id=task_id,
                current_model=f"{self.planner_model}|{self.coder_model}",
            )
            touch_task(self.db_path, task_id)

    def _recover_abandoned_work(self) -> None:
        """Repair stale in-progress Forge rows after a worker/container restart.

        Forge has a multi-stage lifecycle, so blindly requeueing in-progress work
        can hide a finished plan or bypass the next approval gate. On startup we
        only repair work that has been stale long enough to prove it is not this
        worker's current task.
        """
        recovery_cfg = self.config.get("forge", {}).get("recovery", {})
        stale_minutes = max(10, int(recovery_cfg.get("stale_in_progress_minutes", 30)))
        now = datetime.now(timezone.utc)
        recovered = 0
        for task in list_tasks(self.db_path, to_agent=AGENT_NAME, limit=200):
            if task.status != "in_progress":
                continue
            updated_at = self._parse_dt(task.updated_at or task.created_at)
            if updated_at and now - updated_at < timedelta(minutes=stale_minutes):
                continue

            if self._looks_like_plan_result(task.result):
                update_task_status(
                    self.db_path,
                    task.id,
                    "plan_ready",
                    {
                        **(task.result or {}),
                        "_recovered_by": "forge_startup_recovery",
                        "_recovered_at": now.isoformat(),
                        "_previous_status": "in_progress",
                        "_recovery_reason": "Forge found a completed plan result for a stale in-progress task.",
                    },
                )
                emit_event(self.db_path, "forge_recovered_plan_ready", AGENT_NAME, {
                    "task_id": task.id,
                    "stale_minutes": stale_minutes,
                })
                recovered += 1
                continue

            update_task_status(
                self.db_path,
                task.id,
                "pending",
                {
                    "recovered_by": "forge_startup_recovery",
                    "recovered_at": now.isoformat(),
                    "previous_status": "in_progress",
                    "reason": (
                        "Forge worker restarted or lost the active planning step before producing a plan; "
                        "task returned to pending so Roderick can request a fresh approval instead of showing fake active work."
                    ),
                },
            )
            emit_event(self.db_path, "forge_recovered_pending", AGENT_NAME, {
                "task_id": task.id,
                "stale_minutes": stale_minutes,
            })
            recovered += 1

        if recovered:
            logger.warning("Forge startup recovery repaired %s stale in-progress task(s)", recovered)
        self._reconcile_validation_backlog()

    def _reconcile_validation_backlog(self) -> None:
        """Clear stale awaiting_validation Forge rows once Sentinel has already decided."""
        repaired = 0
        for task in list_tasks(self.db_path, to_agent=AGENT_NAME, limit=200):
            if task.status != "awaiting_validation":
                continue
            result = task.result or {}
            sentinel_task_id = result.get("sentinel_task_id")
            if not sentinel_task_id:
                continue
            sentinel_task = get_task(self.db_path, int(sentinel_task_id))
            if not sentinel_task or sentinel_task.status not in {"completed", "failed", "rolled_back"}:
                continue
            sentinel_result = sentinel_task.result or {}
            merged = {
                **result,
                "sentinel_report": sentinel_result,
                "sentinel_validated_at": sentinel_task.updated_at or sentinel_task.created_at,
            }
            if sentinel_task.status == "completed" and sentinel_result.get("passed") is True:
                merged["validation_state"] = "passed_waiting_promotion"
                update_task_status(self.db_path, task.id, "awaiting_validation", merged)
                continue
            merged["validation_state"] = "failed"
            merged["blocked_by"] = "sentinel"
            update_task_status(self.db_path, task.id, "failed", merged)
            emit_event(
                self.db_path,
                "forge_validation_reconciled",
                AGENT_NAME,
                {
                    "task_id": task.id,
                    "sentinel_task_id": sentinel_task.id,
                    "status": "failed",
                },
            )
            repaired += 1
        if repaired:
            logger.warning("Forge validation recovery cleared %s stale validation task(s)", repaired)

    def _collapse_duplicate_pending_improvements(self) -> None:
        """Cancel obviously duplicated pending Forge work so the queue stays actionable."""
        tasks = [
            task for task in list_tasks(self.db_path, to_agent=AGENT_NAME, limit=200)
            if task.task_type == "system_improvement" and task.status in {"pending", "plan_ready"}
        ]
        if len(tasks) < 2:
            return

        pending = [task for task in tasks if task.status == "pending"]
        plan_ready = [task for task in tasks if task.status == "plan_ready"]
        cancelled = 0
        superseded_by_plan_ready: set[int] = set()

        # If a matching plan is already ready, keep the plan and drop duplicate pending asks.
        for ready in plan_ready:
            ready_tokens = self._task_fingerprint(ready.description)
            for task in pending:
                if task.id in superseded_by_plan_ready:
                    continue
                if self._descriptions_similar(ready_tokens, self._task_fingerprint(task.description)):
                    superseded_by_plan_ready.add(task.id)
                    self._cancel_superseded_task(task, superseded_by=ready.id, reason="matching_plan_ready")
                    cancelled += 1

        # For the remaining pending tasks, keep the strongest/newest representative per cluster.
        remaining = [task for task in pending if task.id not in superseded_by_plan_ready]
        consumed: set[int] = set()
        for task in remaining:
            if task.id in consumed:
                continue
            group = [task]
            tokens = self._task_fingerprint(task.description)
            for other in remaining:
                if other.id == task.id or other.id in consumed:
                    continue
                if self._descriptions_similar(tokens, self._task_fingerprint(other.description)):
                    group.append(other)
            if len(group) < 2:
                consumed.add(task.id)
                continue
            keep = sorted(group, key=self._task_rank, reverse=True)[0]
            for dup in group:
                consumed.add(dup.id)
                if dup.id == keep.id:
                    continue
                self._cancel_superseded_task(dup, superseded_by=keep.id, reason="duplicate_pending_cluster")
                cancelled += 1

        if cancelled:
            logger.warning("Forge dedupe cancelled %s duplicate pending system-improvement task(s)", cancelled)

    def _cancel_superseded_task(self, task: Task, *, superseded_by: int, reason: str) -> None:
        update_task_status(
            self.db_path,
            task.id,
            "cancelled",
            {
                "cancelled_by": "forge_dedupe",
                "superseded_by_task_id": superseded_by,
                "reason": reason,
            },
        )
        emit_event(self.db_path, "forge_duplicate_task_cancelled", AGENT_NAME, {
            "task_id": task.id,
            "superseded_by_task_id": superseded_by,
            "reason": reason,
        })

    @staticmethod
    def _task_rank(task: Task) -> tuple[int, str]:
        priority_order = {"critical": 3, "high": 2, "normal": 1, "low": 0}
        return (
            priority_order.get(task.priority, 0),
            task.updated_at or task.created_at or "",
        )

    @staticmethod
    def _task_fingerprint(text: str) -> set[str]:
        stop = {
            "add", "implement", "create", "task", "system", "improvement", "the", "and", "with",
            "for", "that", "this", "from", "into", "before", "after", "should", "must", "error",
            "handling", "logic", "validation", "required", "fields", "mandatory",
        }
        cleaned = text.lower().translate(str.maketrans("", "", string.punctuation))
        return {token for token in cleaned.split() if len(token) > 3 and token not in stop}

    @staticmethod
    def _descriptions_similar(a: set[str], b: set[str]) -> bool:
        if not a or not b:
            return False
        intersection = len(a & b)
        union = len(a | b)
        if union == 0:
            return False
        return intersection >= 4 and (intersection / union) >= 0.45

    @staticmethod
    def _parse_dt(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            return None

    @staticmethod
    def _looks_like_plan_result(result: Optional[dict]) -> bool:
        if not isinstance(result, dict):
            return False
        plan_keys = {
            "implementation_plan",
            "steps",
            "patches",
            "files_to_create",
            "_artifact_root",
            "_artifact_plan_dir",
        }
        return any(key in result for key in plan_keys)

    @staticmethod
    def _revision_priority(feedback_items: list[dict], messages=None) -> tuple[str, str]:
        priorities = [getattr(msg, "priority", "normal") for msg in (messages or [])]
        text = json.dumps(feedback_items, ensure_ascii=False).lower()
        if any(p == "critical" for p in priorities) or "critical" in text:
            return "critical", "immediate"
        if any(p == "high" for p in priorities) or any(word in text for word in ("high", "blocked", "security")):
            return "high", "today"
        return "normal", "today"

    # ── Stage 1: plan ─────────────────────────────────────────────────────────

    async def _plan(self, task: Task) -> None:
        logger.info("Forge planning task #%d: %s", task.id, task.description[:80])
        update_agent_status(self.db_path, AGENT_NAME, "busy", f"Planning: {task.description[:60]}")
        emit_heartbeat(self.db_path, AGENT_NAME, current_task_id=task.id, current_model=f"{self.planner_model}|{self.coder_model}")
        update_task_status(self.db_path, task.id, "in_progress")

        try:
            plan = await self._run_with_keepalive(
                task,
                f"Planning: {task.description[:60]}",
                lambda: self._create_plan(task),
            )
            plan = self._attach_artifact_workspace(task, plan)
            self._write_plan_artifacts(task, plan, "build_plan")
            report_path = self.reports_dir / f"forge_{task.id}_plan.json"
            report_path.write_text(
                json.dumps(plan, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            if self._is_auto_markdown_artifact(task, plan):
                update_task_status(self.db_path, task.id, "plan_approved", plan)
                update_agent_status(self.db_path, AGENT_NAME, "idle", f"Markdown artifact plan approved #{task.id}")
                emit_event(self.db_path, "forge_markdown_artifact_auto_approved", AGENT_NAME, {"task_id": task.id})
                if self._notify:
                    await self._notify(
                        f"ðŸ“„ <b>Forge â€” Markdown artifact queued (Task #{task.id})</b>\n\n"
                        "Non-code Markdown artifact approved for managed workspace generation."
                    )
                return

            update_task_status(self.db_path, task.id, "plan_ready", plan)
            update_agent_status(self.db_path, AGENT_NAME, "idle", f"Plan ready for task #{task.id}")
            # Send plan to user for second approval
            description = self._format_plan_for_approval(task, plan)
            if self._send_approval:
                await self._send_approval(description, task.id)
            if self._notify:
                await self._notify(self._format_plan_notification(task, plan))
            try_reflect_after_task(
                llm=self.llm,
                db_path=self.db_path,
                data_dir=str(self.reports_dir.parent),
                agent_name=AGENT_NAME,
                task=task,
                result=plan,
                owner_context=self.owner_memory.get_context() if self.owner_memory else "",
            )

        except Exception as e:
            logger.error("Forge planning task #%d failed: %s", task.id, e, exc_info=True)
            update_task_status(self.db_path, task.id, "failed", {"error": str(e)})
            update_agent_status(self.db_path, AGENT_NAME, "idle")
            if self._notify:
                await self._notify(
                    f"⚠️ <b>Forge — Planning failed (Task #{task.id})</b>\n\nError: {e}"
                )

    def _create_plan(self, task: Task) -> dict:
        if task.payload.get("approval_policy") == "markdown_artifact_auto":
            return {
                "project_name": "Merlin Research Note",
                "target_path": "/devops/forge-artifacts",
                "summary": "Create a non-code Markdown research note from Merlin findings for dashboard review.",
                "implementation_plan": task.description,
                "steps": [
                    "Create a single Markdown note from Merlin's summary and findings.",
                    "Store it only in the managed Forge artifact workspace.",
                    "Do not modify source code, config, scripts, secrets, or repo documentation.",
                ],
                "estimated_scope": "small",
                "files_to_create": task.payload.get("suggested_files") or ["research_note.md"],
                "test_strategy": "Confirm the Markdown artifact exists in the managed Forge workspace and is visible through the Forge Files dashboard tab.",
                "requires_approval": False,
                "artifact_only": True,
            }
        prompt = self.planning_template.format(request=task.description)
        raw = self.planner_llm.complete(
            messages=[{"role": "user", "content": prompt}],
            system=(
                "You are Forge, a builder agent. "
                "Always respond with valid JSON only — no markdown fences, no preamble."
            ),
            name="forge_plan",
        )
        try:
            plan = json.loads(raw.strip())
        except json.JSONDecodeError:
            plan = {
                "project_name": "unnamed",
                "target_path": "/devops/new-project",
                "summary": raw[:300],
                "implementation_plan": raw,
                "steps": [],
                "estimated_scope": "unknown",
                "files_to_create": [],
                "requires_approval": True,
            }
        plan["_task_id"] = task.id
        plan["_planned_at"] = datetime.now(timezone.utc).isoformat()
        return plan

    def _is_auto_markdown_artifact(self, task: Task, plan: dict) -> bool:
        if task.payload.get("approval_policy") != "markdown_artifact_auto":
            return False
        files = plan.get("files_to_create") or []
        return bool(files) and all(str(path).lower().endswith(".md") for path in files)

    # ── Stage 2: implement ────────────────────────────────────────────────────

    async def _implement(self, task: Task) -> None:
        logger.info("Forge implementing task #%d", task.id)
        update_agent_status(self.db_path, AGENT_NAME, "busy", f"Implementing task #{task.id}")
        emit_heartbeat(self.db_path, AGENT_NAME, current_task_id=task.id, current_model=f"{self.planner_model}|{self.coder_model}")
        update_task_status(self.db_path, task.id, "in_progress")

        try:
            plan = task.result or {}
            files_created = await self._run_with_keepalive(
                task,
                f"Implementing task #{task.id}",
                lambda: self._execute_plan(task, plan),
            )

            # Hand off to Sentinel for validation before marking as live
            sentinel_task = enqueue_task(
                self.db_path,
                Task(
                    to_agent="sentinel",
                    from_agent=AGENT_NAME,
                    task_type="validate_build",
                    description=task.description,
                    payload={
                        "forge_task_id": task.id,
                        "project_dir": plan.get("target_path", ""),
                        "files_created": files_created,
                    },
                ),
            )
            update_task_status(
                self.db_path, task.id, "awaiting_validation",
                {"sentinel_task_id": sentinel_task.id}
            )
            emit_event(self.db_path, "forge_awaiting_validation", AGENT_NAME, {
                "task_id": task.id,
                "sentinel_task_id": sentinel_task.id,
            })
            update_agent_status(self.db_path, AGENT_NAME, "idle", f"Task #{task.id} → Sentinel")
            if self._notify:
                project_name = plan.get("project_name", f"task-{task.id}")
                target = plan.get("target_path", "unknown path")
                await self._notify(
                    f"🔨 <b>Forge — Implementation done, validating…</b>\n\n"
                    f"<b>Project:</b> {project_name}\n"
                    f"<b>Location:</b> <code>{target}</code>\n\n"
                    f"<i>Sentinel is running validation (task #{sentinel_task.id}).</i>"
                )
            try_reflect_after_task(
                llm=self.llm,
                db_path=self.db_path,
                data_dir=str(self.reports_dir.parent),
                agent_name=AGENT_NAME,
                task=task,
                result={"files_created": files_created, "sentinel_task_id": sentinel_task.id},
                owner_context=self.owner_memory.get_context() if self.owner_memory else "",
            )

        except Exception as e:
            logger.error("Forge implementation task #%d failed: %s", task.id, e, exc_info=True)
            update_task_status(self.db_path, task.id, "failed", {"error": str(e)})
            update_agent_status(self.db_path, AGENT_NAME, "idle")
            if self._notify:
                await self._notify(
                    f"⚠️ <b>Forge — Implementation failed (Task #{task.id})</b>\n\nError: {e}"
                )

    def _execute_plan(self, task: Task, plan: dict) -> list[str]:
        """
        Create the project structure as described in the plan.
        Only called after plan_approved.
        Returns list of created file paths (relative to target).
        """
        target_path_str = plan.get("_artifact_files_dir") or plan.get("target_path", "")
        if not target_path_str:
            plan = self._attach_artifact_workspace(task, plan)
            target_path_str = plan.get("_artifact_files_dir") or plan.get("target_path", "")
        if not target_path_str:
            raise ValueError("Plan has no target_path — cannot implement.")

        target = Path(target_path_str)
        target.mkdir(parents=True, exist_ok=True)
        logger.info("Forge staging project artifacts at %s", target)

        files_to_create = plan.get("files_to_create", [])
        steps = plan.get("steps", [])
        created: list[str] = []

        # Write a runbook/scaffold from the plan
        runbook = target / "FORGE_PLAN.md"
        lines = [
            f"# {plan.get('project_name', 'Project')}\n",
            f"> Generated by Forge on {datetime.now(timezone.utc).isoformat()[:16]} UTC\n",
            f"## Summary\n{plan.get('summary', '')}\n",
            f"## Implementation Plan\n{plan.get('implementation_plan', '')}\n",
            f"## Steps\n",
        ]
        for i, step in enumerate(steps, 1):
            lines.append(f"{i}. {step}")
        lines.append(f"\n## Files to Create\n")
        for f in files_to_create:
            lines.append(f"- `{f}`")

        runbook.write_text("\n".join(lines), encoding="utf-8")
        created.append("FORGE_PLAN.md")
        self._record_artifact(
            task=task,
            artifact_type="runbook",
            root=Path(plan.get("_artifact_root", target.parent)),
            path=runbook,
            summary=f"Forge implementation runbook for task #{task.id}",
            approval_state="approved",
            validation_state="pending",
        )
        logger.info("Forge wrote FORGE_PLAN.md at %s", runbook)

        # Generate and write each file using the LLM
        for rel_path in files_to_create:
            fp = target / rel_path
            fp.parent.mkdir(parents=True, exist_ok=True)
            if not fp.exists():
                try:
                    content = self._generate_file_content(plan, rel_path)
                    fp.write_text(content, encoding="utf-8")
                    logger.info("Forge generated: %s (%d chars)", fp, len(content))
                except Exception as e:
                    logger.warning("Forge LLM generation failed for %s, writing stub: %s", rel_path, e)
                    fp.write_text(f"# {rel_path}\n# TODO: implement\n", encoding="utf-8")
                self._record_artifact(
                    task=task,
                    artifact_type="generated_file",
                    root=Path(plan.get("_artifact_root", target.parent)),
                    path=fp,
                    summary=f"Generated file for {plan.get('project_name', f'task-{task.id}')}",
                    approval_state="approved",
                    validation_state="pending",
                    metadata={"requested_path": rel_path},
                )
            created.append(rel_path)

        return created

    def _generate_file_content(self, plan: dict, rel_path: str) -> str:
        """Ask the LLM to write the actual content for a single file from the plan."""
        ext = Path(rel_path).suffix.lower()
        lang_hint = {
            ".py": "Python", ".ts": "TypeScript", ".js": "JavaScript",
            ".sh": "bash shell script", ".md": "Markdown", ".json": "JSON",
            ".yaml": "YAML", ".yml": "YAML", ".toml": "TOML",
            ".dockerfile": "Dockerfile", "": "plain text",
        }.get(ext, ext.lstrip(".").upper())

        steps_text = "\n".join(f"{i+1}. {s}" for i, s in enumerate(plan.get("steps", [])))
        prompt = (
            f"You are implementing a file as part of a software project.\n\n"
            f"Project: {plan.get('project_name', 'unnamed')}\n"
            f"Summary: {plan.get('summary', '')}\n"
            f"Implementation plan: {plan.get('implementation_plan', '')}\n"
            f"Steps:\n{steps_text}\n\n"
            f"Write the complete contents of the file: {rel_path}\n"
            f"Language: {lang_hint}\n\n"
            f"Return ONLY the raw file content. No markdown fences, no explanation."
        )
        return self.coder_llm.complete(
            messages=[{"role": "user", "content": prompt}],
            system=(
                "You are Forge, a precise software builder. "
                "Respond with raw file content only — no markdown, no commentary."
            ),
            name=f"forge_generate_{Path(rel_path).name}",
        )

    # ── Formatting helpers ────────────────────────────────────────────────────

    # Artifact workspace helpers

    def _attach_artifact_workspace(self, task: Task, plan: dict) -> dict:
        """Attach a managed local Forge artifact workspace to a plan."""
        plan = dict(plan or {})
        project_name = str(plan.get("project_name") or f"task-{task.id}")
        slug = self._slug(project_name or task.description)
        root = self.artifacts_dir / f"forge_task_{task.id}_{slug}"
        plan_dir = root / "plan"
        files_dir = root / "files"
        reports_dir = root / "reports"
        validation_dir = root / "validation"
        handoff_dir = root / "handoff"
        for path in (plan_dir, files_dir, reports_dir, validation_dir, handoff_dir):
            path.mkdir(parents=True, exist_ok=True)

        requested_target = plan.get("target_path")
        if requested_target and not plan.get("_requested_target_path"):
            plan["_requested_target_path"] = requested_target
        plan["_artifact_root"] = str(root)
        plan["_artifact_plan_dir"] = str(plan_dir)
        plan["_artifact_files_dir"] = str(files_dir)
        plan["_artifact_reports_dir"] = str(reports_dir)
        plan["_artifact_validation_dir"] = str(validation_dir)
        plan["_artifact_handoff_dir"] = str(handoff_dir)
        plan["target_path"] = str(files_dir)
        return plan

    def _write_plan_artifacts(self, task: Task, plan: dict, artifact_type: str) -> None:
        root = Path(plan.get("_artifact_root", self.artifacts_dir / f"forge_task_{task.id}"))
        plan_dir = Path(plan.get("_artifact_plan_dir", root / "plan"))
        plan_dir.mkdir(parents=True, exist_ok=True)

        plan_json = plan_dir / "plan.json"
        plan_json.write_text(json.dumps(plan, indent=2, ensure_ascii=False), encoding="utf-8")
        self._record_artifact(task, artifact_type, root, plan_json, f"Forge plan for task #{task.id}", "pending", "not_started")

        plan_md = plan_dir / "plan.md"
        plan_md.write_text(self._plan_markdown(task, plan), encoding="utf-8")
        self._record_artifact(task, f"{artifact_type}_markdown", root, plan_md, f"Human-readable Forge plan for task #{task.id}", "pending", "not_started")

        manifest = root / "manifest.json"
        manifest.write_text(json.dumps({
            "task_id": task.id,
            "description": task.description,
            "project_name": plan.get("project_name", f"task-{task.id}"),
            "artifact_root": str(root),
            "requested_target_path": plan.get("_requested_target_path"),
            "managed_files_dir": plan.get("_artifact_files_dir"),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "approval_state": "pending",
            "validation_state": "not_started",
        }, indent=2, ensure_ascii=False), encoding="utf-8")
        self._record_artifact(task, "manifest", root, manifest, f"Forge artifact manifest for task #{task.id}", "pending", "not_started")

    def _record_artifact(
        self,
        task: Task,
        artifact_type: str,
        root: Path,
        path: Path,
        summary: str,
        approval_state: str,
        validation_state: str,
        metadata: Optional[dict] = None,
    ) -> None:
        try:
            relative_path = str(path.relative_to(root))
        except ValueError:
            relative_path = path.name
        record_forge_artifact(
            self.db_path,
            ForgeArtifact(
                task_id=task.id or 0,
                artifact_type=artifact_type,
                artifact_root=str(root),
                relative_path=relative_path,
                path=str(path),
                summary=summary,
                approval_state=approval_state,
                validation_state=validation_state,
                metadata=metadata or {},
            ),
        )
        emit_event(self.db_path, "forge_artifact_created", AGENT_NAME, {
            "task_id": task.id,
            "artifact_type": artifact_type,
            "path": str(path),
            "relative_path": relative_path,
        })

    def _plan_markdown(self, task: Task, plan: dict) -> str:
        steps = plan.get("steps", [])
        files = plan.get("files_to_create") or [
            p.get("file") for p in plan.get("patches", []) if isinstance(p, dict)
        ]
        lines = [
            f"# Forge Task {task.id}: {plan.get('project_name', 'Plan')}",
            "",
            f"Description: {task.description}",
            "",
            f"Summary: {plan.get('summary', '')}",
            f"Scope: {plan.get('estimated_scope', 'unknown')}",
            f"Risk: {plan.get('risk_level', 'unknown')}",
            "",
            "## Managed Artifact Workspace",
            f"- Root: `{plan.get('_artifact_root', '')}`",
            f"- Files: `{plan.get('_artifact_files_dir', '')}`",
        ]
        requested = plan.get("_requested_target_path")
        if requested:
            lines.append(f"- Requested target from plan: `{requested}`")
        if steps:
            lines.extend(["", "## Steps"])
            lines.extend(f"{i}. {step}" for i, step in enumerate(steps, 1))
        if files:
            lines.extend(["", "## Files"])
            lines.extend(f"- `{file}`" for file in files if file)
        lines.append("")
        return "\n".join(lines)

    def _slug(self, value: str) -> str:
        slug = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip().lower()).strip("_")
        return (slug[:48] or "artifact")

    def _format_plan_for_approval(self, task: Task, plan: dict) -> str:
        project = plan.get("project_name", f"task-{task.id}")
        target = plan.get("target_path", "unknown")
        scope = plan.get("estimated_scope", "unknown")
        steps = plan.get("steps", [])
        step_preview = "\n".join(f"  {i+1}. {s}" for i, s in enumerate(steps[:5]))
        return (
            f"Forge has a plan for: {task.description[:80]}\n\n"
            f"Project: {project}\nTarget: {target}\nScope: {scope}\n\n"
            f"Steps preview:\n{step_preview}\n\n"
            f"Approve to allow Forge to create files."
        )

    # ── System improvement mode ───────────────────────────────────────────────

    async def _plan_system_improvement(self, task: Task) -> None:
        """Stage 1 for system_improvement: inspect files, produce a targeted patch plan."""
        logger.info("Forge planning system improvement #%d", task.id)
        update_agent_status(self.db_path, AGENT_NAME, "busy", f"Planning improvement: {task.description[:60]}")
        emit_heartbeat(self.db_path, AGENT_NAME, current_task_id=task.id, current_model=f"{self.planner_model}|{self.coder_model}")
        update_task_status(self.db_path, task.id, "in_progress")

        try:
            plan = await self._run_with_keepalive(
                task,
                f"Planning improvement: {task.description[:60]}",
                lambda: self._create_system_improvement_plan(task),
            )
            plan = self._attach_artifact_workspace(task, plan)
            self._write_plan_artifacts(task, plan, "system_improvement_plan")
            report_path = self.reports_dir / f"forge_{task.id}_sysimprovement_plan.json"
            report_path.write_text(json.dumps(plan, indent=2, ensure_ascii=False), encoding="utf-8")
            update_task_status(self.db_path, task.id, "plan_ready", plan)
            update_agent_status(self.db_path, AGENT_NAME, "idle", f"Sys-improvement plan ready #{task.id}")
            if self.auto_approve_improvements:
                # Skip user approval gate — immediately advance to plan_approved.
                update_task_status(self.db_path, task.id, "plan_approved", plan)
                emit_event(self.db_path, "forge_auto_approved", AGENT_NAME, {"task_id": task.id, "from_status": "plan_ready"})
                logger.info("Forge auto-approved plan for system_improvement #%d", task.id)
                if self._notify:
                    await self._notify(self._format_system_improvement_notification(task, plan))
            else:
                description = self._format_system_improvement_approval(task, plan)
                if self._send_approval:
                    await self._send_approval(description, task.id)
                if self._notify:
                    await self._notify(self._format_system_improvement_notification(task, plan))
        except Exception as e:
            logger.error("Forge sys-improvement planning #%d failed: %s", task.id, e, exc_info=True)
            update_task_status(self.db_path, task.id, "failed", {"error": str(e)})
            update_agent_status(self.db_path, AGENT_NAME, "idle")
            if self._notify:
                await self._notify(f"⚠️ <b>Forge — System improvement plan failed (Task #{task.id})</b>\n\nError: {e}")

    def _create_system_improvement_plan(self, task: Task) -> dict:
        payload = task.payload or {}
        affected = payload.get("affected_components", [])
        evidence_facts = payload.get("verified_facts", [])
        likely_causes = payload.get("likely_causes", [])

        # Read relevant files to include as context
        file_snippets = []
        for component in affected[:4]:
            # component may be a file path relative to repo root or a module name
            candidates = [
                self._resolve_repo_path(component),
                self._resolve_repo_path(str(Path("apps") / component)),
                self._resolve_repo_path(str(Path("shared") / component)),
            ]
            for c in candidates:
                if c.exists() and c.is_file():
                    try:
                        text = c.read_text(encoding="utf-8", errors="replace")
                        file_snippets.append(f"### {c}\n```\n{text[:2000]}\n```")
                    except Exception:
                        pass
                    break

        context_block = "\n\n".join(file_snippets) if file_snippets else "(no files read)"
        facts_block = "\n".join(f"- {f}" for f in evidence_facts) or "(none)"
        causes_block = "\n".join(f"- {c}" for c in likely_causes) or "(none)"

        prompt = (
            f"You are Forge, an expert software improvement agent.\n\n"
            f"A Merlin investigation produced the following findings:\n\n"
            f"**Problem:** {task.description}\n\n"
            f"**Verified facts:**\n{facts_block}\n\n"
            f"**Likely causes:**\n{causes_block}\n\n"
            f"**Affected components:** {', '.join(affected) or 'unknown'}\n\n"
            f"**File contents:**\n{context_block}\n\n"
            f"Produce a focused patch plan with SMALL, TARGETED changes only. "
            f"No large refactors. No new files unless essential. "
            f"Return valid JSON with these fields:\n"
            f"  project_name (str), summary (str), risk_level (low|medium|high), "
            f"estimated_scope (small|medium), patches (list of {{file, description, change_type: add|modify|delete}}), "
            f"steps (list of str), rollback_notes (str), requires_approval (true)."
        )
        raw = self.planner_llm.complete(
            messages=[{"role": "user", "content": prompt}],
            system="You are Forge. Return valid JSON only — no markdown, no preamble.",
            name="forge_sysimprovement_plan",
        )
        try:
            plan = json.loads(raw.strip())
        except json.JSONDecodeError:
            plan = {
                "project_name": f"system-improvement-{task.id}",
                "summary": raw[:300],
                "risk_level": payload.get("risk_level", "medium"),
                "estimated_scope": "small",
                "patches": [],
                "steps": [raw[:200]],
                "rollback_notes": "Manual rollback required — review git diff.",
                "requires_approval": True,
            }
        plan["_task_id"] = task.id
        plan["_improvement_id"] = payload.get("improvement_id")
        plan["_planned_at"] = datetime.now(timezone.utc).isoformat()
        return plan

    async def _implement_system_improvement(self, task: Task) -> None:
        """Stage 2: apply targeted patches from the system improvement plan."""
        logger.info("Forge implementing system improvement #%d", task.id)
        update_agent_status(self.db_path, AGENT_NAME, "busy", f"Applying system improvement #{task.id}")
        emit_heartbeat(self.db_path, AGENT_NAME, current_task_id=task.id, current_model=f"{self.planner_model}|{self.coder_model}")
        update_task_status(self.db_path, task.id, "in_progress")

        try:
            plan = task.result or {}
            patches_applied = await self._run_with_keepalive(
                task,
                f"Applying system improvement #{task.id}",
                lambda: self._apply_patches(task, plan),
            )
            forge_review = await self._run_with_keepalive(
                task,
                f"Reviewing system improvement #{task.id}",
                lambda: self._review_system_improvement(task, plan, patches_applied),
            )

            sentinel_task = enqueue_task(
                self.db_path,
                Task(
                    to_agent="sentinel",
                    from_agent=AGENT_NAME,
                    task_type="validate_system_improvement",
                    description=task.description,
                    payload={
                        "forge_task_id": task.id,
                        "improvement_id": plan.get("_improvement_id"),
                        "patches_applied": patches_applied,
                        "forge_review": forge_review,
                        "affected_components": [p.get("file") for p in plan.get("patches", [])],
                    },
                ),
            )
            update_task_status(
                self.db_path,
                task.id,
                "awaiting_validation",
                self._merge_result(
                    task,
                    sentinel_task_id=sentinel_task.id,
                    patches_applied=patches_applied,
                    forge_review=forge_review,
                    affected_components=[p.get("file") for p in plan.get("patches", [])],
                    deployment={"state": "awaiting_sentinel"},
                ),
            )
            update_agent_status(self.db_path, AGENT_NAME, "idle", f"Sys-improvement #{task.id} → Sentinel")
            if self._notify:
                await self._notify(
                    f"🔨 <b>Forge — System improvement applied, validating…</b>\n\n"
                    f"<b>Summary:</b> {plan.get('summary', '')[:100]}\n"
                    f"<b>Patches:</b> {len(patches_applied)}\n\n"
                    f"<b>Forge review:</b> {forge_review.get('recommendation', 'unknown')}\n\n"
                    f"<i>Sentinel is running checks (task #{sentinel_task.id}).</i>"
                )
            try_reflect_after_task(
                llm=self.llm,
                db_path=self.db_path,
                data_dir=str(self.reports_dir.parent),
                agent_name=AGENT_NAME,
                task=task,
                result={"patches_applied": patches_applied, "forge_review": forge_review, "sentinel_task_id": sentinel_task.id},
                owner_context=self.owner_memory.get_context() if self.owner_memory else "",
            )
        except Exception as e:
            logger.error("Forge sys-improvement implement #%d failed: %s", task.id, e, exc_info=True)
            update_task_status(self.db_path, task.id, "failed", {"error": str(e)})
            update_agent_status(self.db_path, AGENT_NAME, "idle")
            if self._notify:
                await self._notify(f"⚠️ <b>Forge — System improvement failed (Task #{task.id})</b>\n\nError: {e}")

    def _apply_patches(self, task: Task, plan: dict) -> list[dict]:
        """Apply each patch from the plan. Returns list of applied patch summaries."""
        patches = plan.get("patches", [])
        applied = []
        for patch in patches:
            file_path_str = patch.get("file", "")
            if not file_path_str:
                continue
            change_type = patch.get("change_type", "modify")
            description = patch.get("description", "")
            fp = self._resolve_repo_path(file_path_str)

            try:
                if change_type == "delete" and fp.exists():
                    fp.unlink()
                    applied.append({"file": str(fp), "change_type": "delete", "status": "ok"})
                elif change_type in ("add", "modify"):
                    # Ask LLM to produce the new content for this file
                    if fp.exists():
                        existing = fp.read_text(encoding="utf-8", errors="replace")[:3000]
                    else:
                        existing = "(new file)"
                    prompt = (
                        f"You are applying a system improvement patch.\n\n"
                        f"File: {file_path_str}\n"
                        f"Change: {description}\n\n"
                        f"Current content (truncated at 3000 chars):\n```\n{existing}\n```\n\n"
                        f"Return ONLY the complete updated file content. No markdown, no explanation."
                    )
                    new_content = self.coder_llm.complete(
                        messages=[{"role": "user", "content": prompt}],
                        system="You are Forge. Return raw file content only.",
                        name=f"forge_patch_{fp.name}",
                    )
                    fp.parent.mkdir(parents=True, exist_ok=True)
                    fp.write_text(new_content, encoding="utf-8")
                    applied.append({"file": str(fp), "change_type": change_type, "status": "ok", "chars": len(new_content)})
                else:
                    applied.append({"file": str(fp), "change_type": change_type, "status": "skipped"})
            except Exception as e:
                logger.error("Patch failed for %s: %s", file_path_str, e)
                applied.append({"file": str(fp), "change_type": change_type, "status": "error", "error": str(e)})

        return applied

    def _find_related_sentinel_task(self, forge_task_id: int) -> Optional[Task]:
        for task in list_tasks(self.db_path, to_agent="sentinel", limit=160):
            payload = task.payload if isinstance(task.payload, dict) else {}
            if payload.get("forge_task_id") == forge_task_id:
                return task
        return None

    def _infer_services_for_paths(self, paths: list[str]) -> list[str]:
        services: set[str] = set()
        normalized = [str(Path(p)).replace("\\", "/").lower() for p in paths if p]
        if not normalized:
            return ["roderick"]
        for path in normalized:
            rel = path
            repo_root_norm = str(self.repo_root).replace("\\", "/").lower()
            if rel.startswith(repo_root_norm):
                rel = rel[len(repo_root_norm):].lstrip("/")
            if rel.startswith("dashboard/"):
                services.add("dashboard")
                continue
            if rel.startswith("apps/api/") or rel.startswith("docker/dockerfile.api"):
                services.add("api")
            if rel.startswith("apps/zuko/") or rel.startswith("docker/dockerfile.zuko"):
                services.add("zuko")
            if (
                rel.startswith("apps/roderick/")
                or rel.startswith("apps/forge/")
                or rel.startswith("apps/merlin/")
                or rel.startswith("apps/sentinel/")
                or rel.startswith("apps/venture/")
                or rel.startswith("apps/operator/")
                or rel.startswith("shared/")
                or rel.startswith("config/")
                or rel.startswith("docker/dockerfile.roderick")
                or rel == "requirements.txt"
            ):
                services.update({"roderick", "api", "zuko"})
        if not services:
            services.add("roderick")
        ordered = ["roderick", "api", "zuko", "dashboard"]
        return [service for service in ordered if service in services]

    def _run_compose(self, args: list[str]) -> tuple[bool, str]:
        cmd = ["docker", "compose", *args]
        result = subprocess.run(
            cmd,
            cwd=self.repo_root,
            capture_output=True,
            text=True,
            timeout=self.compose_timeout_seconds,
        )
        output = "\n".join(part for part in (result.stdout.strip(), result.stderr.strip()) if part).strip()
        return (result.returncode == 0, output[:5000])

    async def _deploy_promoted_system_improvement(self, task: Task) -> None:
        update_agent_status(self.db_path, AGENT_NAME, "busy", f"Deploying promoted patch #{task.id}")
        emit_heartbeat(
            self.db_path,
            AGENT_NAME,
            current_task_id=task.id,
            current_model=f"{self.planner_model}|{self.coder_model}",
        )
        update_task_status(
            self.db_path,
            task.id,
            "live",
            self._merge_result(task, deployment={"state": "deploying", "started_at": datetime.now(timezone.utc).isoformat()}),
        )
        try:
            sentinel_task = self._find_related_sentinel_task(task.id)
            sentinel_payload = sentinel_task.payload if sentinel_task and isinstance(sentinel_task.payload, dict) else {}
            patches = sentinel_payload.get("patches_applied") or []
            affected = sentinel_payload.get("affected_components") or [p.get("file") for p in patches if isinstance(p, dict)]
            affected_paths = [str(p.get("file")) for p in patches if isinstance(p, dict) and p.get("file")] or [str(p) for p in affected if p]
            services = self._infer_services_for_paths(affected_paths)
            build_ok, build_output = self._run_compose(["build", *services])
            if not build_ok:
                raise RuntimeError(f"Docker build failed: {build_output[:500]}")
            up_ok, up_output = self._run_compose(["up", "-d", "--force-recreate", *services])
            if not up_ok:
                raise RuntimeError(f"Docker refresh failed: {up_output[:500]}")
            result = self._merge_result(
                task,
                deployment={
                    "state": "deployed",
                    "repo_root": str(self.repo_root),
                    "services": services,
                    "affected_paths": affected_paths[:40],
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                    "build_output": build_output[-2000:],
                    "up_output": up_output[-2000:],
                },
            )
            update_task_status(self.db_path, task.id, "live", result)
            emit_event(
                self.db_path,
                "forge_live_deployed",
                AGENT_NAME,
                {"task_id": task.id, "services": services, "affected_paths": affected_paths[:20]},
            )
            if self._notify:
                await self._notify(
                    f"🚀 <b>Forge — Patch deployed (Task #{task.id})</b>\n\n"
                    f"<b>Services:</b> {', '.join(services)}\n"
                    f"<b>Files:</b> {len(affected_paths)} touched\n"
                    f"<i>Docker services were rebuilt and refreshed from the promoted workspace.</i>"
                )
        except Exception as exc:
            logger.error("Forge deployment for live task #%d failed: %s", task.id, exc, exc_info=True)
            update_task_status(
                self.db_path,
                task.id,
                "live",
                self._merge_result(
                    task,
                    deployment={
                        "state": "failed",
                        "failed_at": datetime.now(timezone.utc).isoformat(),
                        "error": str(exc),
                    },
                ),
            )
            emit_event(self.db_path, "forge_live_deploy_failed", AGENT_NAME, {"task_id": task.id, "error": str(exc)[:500]})
            if self._notify:
                await self._notify(
                    f"⚠️ <b>Forge — Deployment failed (Task #{task.id})</b>\n\n"
                    f"{str(exc)[:300]}"
                )
        finally:
            update_agent_status(self.db_path, AGENT_NAME, "idle")

    def _review_system_improvement(self, task: Task, plan: dict, patches_applied: list[dict]) -> dict:
        """Forge performs a self-review before handing patches to Sentinel."""
        root = Path(plan.get("_artifact_root", self.artifacts_dir / f"forge_task_{task.id}"))
        validation_dir = root / "validation"
        validation_dir.mkdir(parents=True, exist_ok=True)
        patch_summary = "\n".join(
            f"- {p.get('status', '?')} {p.get('change_type', '?')} {p.get('file', '?')}: {p.get('error', '')}"
            for p in patches_applied
        ) or "(no patches applied)"
        prompt = (
            "You are Forge doing your own pre-Sentinel code review.\n\n"
            f"Task #{task.id}: {task.description}\n\n"
            f"Plan summary: {plan.get('summary', '')}\n\n"
            f"Patches applied:\n{patch_summary}\n\n"
            "Return valid JSON only with keys: recommendation (pass|revise|block), "
            "review_summary (str), risks (list of str), follow_up_checks (list of str). "
            "Be strict: if any patch failed, recommend revise or block."
        )
        try:
            raw = self.coder_llm.complete(
                messages=[{"role": "user", "content": prompt}],
                system="You are Forge. Return valid JSON only - no markdown, no preamble.",
                name="forge_self_review",
            )
            review = json.loads(raw.strip())
        except Exception as e:
            review = {
                "recommendation": "revise",
                "review_summary": f"Forge self-review could not parse a clean result: {e}",
                "risks": ["Self-review output was not valid JSON."],
                "follow_up_checks": ["Sentinel must perform compile, config, and security checks."],
            }
        review["_task_id"] = task.id
        review["_reviewed_at"] = datetime.now(timezone.utc).isoformat()
        review_path = validation_dir / "forge_self_review.json"
        review_path.write_text(json.dumps(review, indent=2, ensure_ascii=False), encoding="utf-8")
        self._record_artifact(
            task,
            "forge_self_review",
            root,
            review_path,
            f"Forge pre-Sentinel self-review for task #{task.id}",
            "approved",
            "pre_sentinel",
            {"recommendation": review.get("recommendation", "unknown")},
        )
        return review

    def _format_system_improvement_approval(self, task: Task, plan: dict) -> str:
        patches = plan.get("patches", [])
        patch_list = "\n".join(f"  [{p.get('change_type','?')}] {p.get('file','?')}: {p.get('description','')[:60]}" for p in patches[:6])
        return (
            f"Forge has a system improvement plan for: {task.description[:80]}\n\n"
            f"Risk: {plan.get('risk_level','unknown')}  Scope: {plan.get('estimated_scope','?')}\n\n"
            f"Patches:\n{patch_list or '  (no patches listed)'}\n\n"
            f"Rollback: {plan.get('rollback_notes','')[:100]}\n\n"
            f"Approve to allow Forge to apply these patches."
        )

    def _format_system_improvement_notification(self, task: Task, plan: dict) -> str:
        patches = plan.get("patches", [])
        lines = [
            f"🔨 <b>Forge — System Improvement Plan Ready (Task #{task.id})</b>\n",
            f"<b>Summary:</b> {plan.get('summary', '')[:120]}",
            f"<b>Risk:</b> {plan.get('risk_level','?')}  <b>Scope:</b> {plan.get('estimated_scope','?')}\n",
        ]
        if patches:
            lines.append("<b>Patches:</b>")
            for p in patches[:6]:
                lines.append(f"  [{p.get('change_type','?')}] <code>{p.get('file','?')}</code>")
        lines.append("\n<i>See approval request above to review and approve.</i>")
        return "\n".join(lines)

    def _format_plan_notification(self, task: Task, plan: dict) -> str:
        project = plan.get("project_name", f"task-{task.id}")
        summary = plan.get("summary", "")
        scope = plan.get("estimated_scope", "unknown")
        steps = plan.get("steps", [])

        lines = [
            f"🔨 <b>Forge — Plan Ready (Task #{task.id})</b>\n",
            f"<b>Project:</b> {project}",
            f"<b>Scope:</b> {scope}",
            f"<b>Summary:</b> {summary}\n",
        ]
        if steps:
            lines.append("<b>Steps:</b>")
            for i, s in enumerate(steps[:6], 1):
                lines.append(f"  {i}. {s}")
        lines.append("\n<i>See the approval request above to review and approve implementation.</i>")
        return "\n".join(lines)

    # ── Self-healing: Sentinel revision loop ──────────────────────────────────

    async def _handle_revision_messages(self, msgs) -> None:
        """Batch Sentinel feedback into one approval-gated Forge revision task."""
        feedback_items = []
        for msg in msgs:
            mark_message_read(self.db_path, msg.id)
            try:
                feedback = json.loads(msg.message) if msg.message.strip().startswith("{") else {"summary": msg.message}
            except json.JSONDecodeError:
                feedback = {"summary": msg.message}
            feedback["_message_id"] = msg.id
            feedback_items.append(feedback)

        if not feedback_items:
            return

        open_digest = [
            task for task in list_tasks(self.db_path, to_agent=AGENT_NAME, limit=80)
            if task.task_type == "system_improvement"
            and task.status in {"pending", "approved", "plan_ready", "plan_approved", "in_progress", "awaiting_validation"}
            and task.payload.get("source") == "sentinel_revision_digest"
        ]
        if open_digest:
            emit_event(self.db_path, "forge_revision_feedback_held", AGENT_NAME, {
                "existing_revision_task_id": open_digest[0].id,
                "message_ids": [item.get("_message_id") for item in feedback_items],
                "count": len(feedback_items),
            })
            logger.info(
                "Forge held %d Sentinel revision message(s); revision digest task #%s is still %s",
                len(feedback_items),
                open_digest[0].id,
                open_digest[0].status,
            )
            return

        summaries = []
        original_task_ids = []
        sentinel_task_ids = []
        issues = []
        for item in feedback_items:
            summary = item.get("summary", "Sentinel requested revision.")
            summaries.append(str(summary))
            original = item.get("forge_task_id") or item.get("task_id")
            if original:
                original_task_ids.append(original)
            if item.get("sentinel_task_id"):
                sentinel_task_ids.append(item.get("sentinel_task_id"))
            for issue in item.get("warnings", []) or item.get("issues", []):
                issues.append(str(issue))

        revision_desc = (
            "Consolidated Sentinel revision package. "
            f"Sentinel sent {len(feedback_items)} feedback item(s); Forge should create one coherent revision plan "
            "instead of many overlapping follow-up tasks.\n\n"
            "Feedback summaries:\n"
            + "\n".join(f"- {summary[:220]}" for summary in summaries[:10])
        )
        priority, urgency = self._revision_priority(feedback_items, msgs)
        revision_task = enqueue_task(
            self.db_path,
            Task(
                to_agent=AGENT_NAME,
                from_agent="sentinel",
                task_type="system_improvement",
                description=revision_desc,
                approval_required=True,
                priority=priority,
                urgency=urgency,
                domain="operations",
                payload={
                    "source": "sentinel_revision_digest",
                    "is_revision": True,
                    "original_task_ids": original_task_ids[:20],
                    "sentinel_task_ids": sentinel_task_ids[:20],
                    "sentinel_feedback_items": feedback_items[:20],
                    "issues": issues[:20],
                },
            ),
        )
        emit_event(self.db_path, "forge_revision_digest_queued", AGENT_NAME, {
            "revision_task_id": revision_task.id,
            "message_ids": [item.get("_message_id") for item in feedback_items],
            "count": len(feedback_items),
            "priority": priority,
            "urgency": urgency,
        })

        if self._notify:
            await self._notify(
                f"Forge revision digest queued (Task #{revision_task.id}). "
                f"Sentinel sent {len(feedback_items)} revision item(s); Forge bundled them into one approval-gated task."
            )

    async def _handle_revision_message(self, msg) -> None:
        """Read a Sentinel revision feedback message and create a revision task."""
        from shared.db.messages import mark_message_read
        mark_message_read(self.db_path, msg.id)

        try:
            feedback = json.loads(msg.message) if msg.message.strip().startswith("{") else {"summary": msg.message}
        except json.JSONDecodeError:
            feedback = {"summary": msg.message}

        original_task_id = feedback.get("forge_task_id") or feedback.get("task_id")
        summary = feedback.get("summary", "Sentinel requested revision.")
        issues = feedback.get("warnings", []) or feedback.get("issues", [])
        sentinel_task_id = feedback.get("sentinel_task_id")

        logger.info("Forge received Sentinel revision feedback (msg #%d): %s", msg.id, summary[:120])

        # Create a new revision task so the two-stage approval still applies
        revision_desc = (
            f"[REVISION] Sentinel blocked task #{original_task_id}. "
            f"Summary: {summary[:200]}. "
            f"Issues: {'; '.join(str(i) for i in issues[:5])}."
        )
        revision_task = enqueue_task(
            self.db_path,
            Task(
                to_agent=AGENT_NAME,
                from_agent="sentinel",
                task_type="system_improvement",
                description=revision_desc,
                approval_required=True,
                priority="high",
                domain="operations",
                payload={
                    "is_revision": True,
                    "original_task_id": original_task_id,
                    "sentinel_task_id": sentinel_task_id,
                    "sentinel_feedback": feedback,
                },
            ),
        )
        emit_event(self.db_path, "forge_revision_queued", AGENT_NAME, {
            "revision_task_id": revision_task.id,
            "original_task_id": original_task_id,
            "sentinel_task_id": sentinel_task_id,
        })

        if self._notify:
            await self._notify(
                f"🔄 <b>Forge — Revision queued (Task #{revision_task.id})</b>\n\n"
                f"Sentinel blocked task #{original_task_id}.\n"
                f"<b>Issue:</b> {summary[:150]}\n\n"
                f"<i>Revision task #{revision_task.id} queued — approval required before Forge will re-attempt.</i>"
            )
