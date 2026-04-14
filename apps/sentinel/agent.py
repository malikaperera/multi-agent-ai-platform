"""
Sentinel — QA and validation agent.

Sits between Forge and "live": validates every build task before promotion.

Three-gate flow:
  Gate 1: Generate test scaffold (pytest files + smoke test spec)
  Gate 2: Run tests (pytest, docker build, smoke command if provided)
  Gate 3: Produce validation report → send approval request

Sentinel warns loudly when it cannot generate meaningful tests.
No test scaffold → no promotion.

Task types accepted:
  - validate_build    : called automatically by Forge after plan_approved completion
  - run_tests         : manual trigger
  - generate_scaffold : generate test files only, no execution

Validation report fields:
  - scaffold_generated: bool
  - tests_run: bool
  - tests_passed: bool
  - docker_build_passed: bool | None
  - smoke_passed: bool | None
  - warnings: list[str]
  - test_files: list[str]
  - summary: str
"""
import asyncio
import contextlib
import json
import logging
import os
import re
import sqlite3
import stat
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Coroutine, Optional

from shared.db.agents import emit_heartbeat, record_agent_error, record_agent_success, update_agent_status
from shared.db.events import emit_event
from shared.db.messages import send_agent_message
from shared.db.tasks import enqueue_task, get_next_task, list_tasks, touch_task, update_task_status
from shared.agent_learning import try_reflect_after_task
from shared.llm.anthropic_provider import AnthropicProvider
from shared.memory.founder import OwnerMemory
from shared.schemas.task import Task
from shared.task_priority import infer_priority

logger = logging.getLogger(__name__)

AGENT_NAME = "sentinel"


class SentinelAgent:
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

        scfg = config.get("sentinel", {})
        self.prompt_template = scfg.get(
            "validation_prompt_template",
            "Generate a test scaffold for:\n{description}\n\nFiles created:\n{files_created}\n\nOwner context:\n{owner_context}",
        )
        self.poll_interval = int(config.get("scheduler", {}).get("worker_poll_interval_seconds", 10))
        self.max_concurrent_validations = max(1, int(scfg.get("max_concurrent_validations", 1)))
        self.keepalive_seconds = max(10, int(config.get("observability", {}).get("task_keepalive_seconds", 20)))

        self._notify: Optional[Callable[[str], Coroutine]] = None
        self._send_approval: Optional[Callable[[str, int, str], Coroutine]] = None

    def set_notify(self, fn: Callable[[str], Coroutine]) -> None:
        self._notify = fn

    def set_approval_sender(self, fn: Callable[[str, int, str, dict], Coroutine]) -> None:
        """fn(description, task_id, request_type, payload)"""
        self._send_approval = fn

    # ── Worker loop ───────────────────────────────────────────────────────────

    async def run(self) -> None:
        logger.info(
            "Sentinel worker started (poll interval %ds, concurrency=%d)",
            self.poll_interval,
            self.max_concurrent_validations,
        )
        update_agent_status(self.db_path, AGENT_NAME, "idle")
        emit_heartbeat(self.db_path, AGENT_NAME, current_task_id=None, current_model=self.llm.model)
        active: set[asyncio.Task] = set()

        while True:
            try:
                if active:
                    done = {t for t in active if t.done()}
                    for finished in done:
                        active.remove(finished)
                        exc = finished.exception()
                        if exc:
                            logger.error("Sentinel concurrent validation failed: %s", exc, exc_info=exc)
                    if not active:
                        update_agent_status(self.db_path, AGENT_NAME, "idle", "Sentinel validation queue is clear")
                        emit_heartbeat(self.db_path, AGENT_NAME, current_task_id=None, current_model=self.llm.model)

                task = get_next_task(self.db_path, AGENT_NAME, "pending")
                if task and len(active) < self.max_concurrent_validations:
                    update_task_status(self.db_path, task.id, "in_progress")
                    active.add(asyncio.create_task(self._process(task)))
                    if len(active) >= self.max_concurrent_validations:
                        logger.info("Sentinel validation concurrency full (%d/%d)", len(active), self.max_concurrent_validations)
                elif not active:
                    await asyncio.sleep(self.poll_interval)
                else:
                    await asyncio.sleep(1)
            except Exception as e:
                logger.error("Sentinel worker error: %s", e, exc_info=True)
                await asyncio.sleep(self.poll_interval)

    async def _process(self, task: Task) -> None:
        logger.info("Sentinel processing task #%d: %s", task.id, task.task_type)
        update_agent_status(self.db_path, AGENT_NAME, "busy", task.description[:60])
        emit_heartbeat(self.db_path, AGENT_NAME, current_task_id=task.id, current_model=self.llm.model)
        update_task_status(self.db_path, task.id, "in_progress")

        try:
            if task.task_type in ("validate_build", "run_tests"):
                await self._validate(task)
            elif task.task_type == "validate_system_improvement":
                await self._validate_system_improvement(task)
            elif task.task_type == "generate_scaffold":
                await self._scaffold_only(task)
            elif task.task_type == "health_check":
                await self._health_check(task)
            else:
                update_task_status(self.db_path, task.id, "completed")

            record_agent_success(self.db_path, AGENT_NAME, f"Processed {task.task_type} #{task.id}")

        except Exception as e:
            logger.error("Sentinel task #%d failed: %s", task.id, e, exc_info=True)
            update_task_status(self.db_path, task.id, "failed", {"error": str(e)})
            record_agent_error(self.db_path, AGENT_NAME, str(e))
            if self._notify:
                await self._notify(f"⚠️ <b>Sentinel — Task #{task.id} failed</b>\n\nError: {e}")
        finally:
            emit_heartbeat(self.db_path, AGENT_NAME, current_task_id=None, current_model=self.llm.model)

    # ── Gate 1+2: full validation ─────────────────────────────────────────────

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
            emit_heartbeat(self.db_path, AGENT_NAME, current_task_id=task_id, current_model=self.llm.model)
            touch_task(self.db_path, task_id)

    async def _validate(self, task: Task) -> None:
        report = await self._run_with_keepalive(
            task,
            f"Validating build #{task.id}",
            lambda: self._run_validation(task),
        )

        report_path = self.reports_dir / f"sentinel_{task.id}.json"
        report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        update_task_status(self.db_path, task.id, "completed", report)
        emit_event(self.db_path, "validation_complete", AGENT_NAME, {
            "task_id": task.id,
            "passed": report.get("tests_passed", False),
            "warnings": report.get("warnings", []),
        })
        self._escalate_to_merlin_if_needed(task, report, "build validation")
        if self._notify:
            await self._notify(self._format_report(task, report))

        # Promotion gate: tests AND security must both pass
        forge_task_id = task.payload.get("forge_task_id")
        tests_ok = report.get("tests_passed") and report.get("scaffold_generated")
        security_ok = report.get("security_passed", True)

        if tests_ok and security_ok:
            self._update_forge_validation_state(
                forge_task_id,
                "awaiting_validation",
                report,
                validation_state="passed_waiting_promotion",
            )
            if self._send_approval:
                await self._send_approval(
                    self._format_approval_description(task, report),
                    task.id,
                    "sentinel_approval",
                    {"forge_task_id": forge_task_id} if forge_task_id else {},
                )
        else:
            block_reasons = []
            if not tests_ok:
                block_reasons.append("Tests did not pass.")
            if not security_ok:
                crit = report.get("security_critical", 0)
                high = report.get("security_high", 0)
                block_reasons.append(f"Security: {crit} critical, {high} high finding(s).")
            all_warnings = report.get("warnings", [])
            sec_findings = [
                f for f in report.get("security_findings", [])
                if f.get("severity") in ("critical", "high")
            ]
            if self._notify:
                lines = ["🚨 <b>Sentinel BLOCKED promotion</b> — Task #{}\n".format(task.id)]
                for r in block_reasons:
                    lines.append(f"  ✗ {r}")
                if sec_findings:
                    lines.append("\n<b>Security findings blocking promotion:</b>")
                    for f in sec_findings[:5]:
                        lines.append(f"  [{f['severity'].upper()}] {f['category']}: {f['message'][:100]}")
                elif all_warnings:
                    lines.append("\n<b>Warnings:</b>")
                    for w in all_warnings[:3]:
                        lines.append(f"  ⚠ {w[:100]}")
                await self._notify("\n".join(lines))
            self._update_forge_validation_state(
                forge_task_id,
                "failed",
                report,
                validation_state="failed",
            )
            self._send_revision_feedback_to_forge(task, report, "build validation")

    # ── Gate 1 only: scaffold generation ──────────────────────────────────────

        try_reflect_after_task(
            llm=self.llm,
            db_path=self.db_path,
            data_dir=str(self.reports_dir.parent),
            agent_name=AGENT_NAME,
            task=task,
            result=report,
            owner_context=self.owner_memory.get_context(),
        )

    async def _scaffold_only(self, task: Task) -> None:
        scaffold = await self._run_with_keepalive(
            task,
            f"Generating scaffold #{task.id}",
            lambda: self._generate_scaffold(task.description, task.payload.get("files_created", [])),
        )
        update_task_status(self.db_path, task.id, "completed", scaffold)
        if self._notify and scaffold.get("test_files"):
            files = "\n".join(f"  • {f}" for f in scaffold["test_files"])
            await self._notify(
                f"🧪 <b>Sentinel — Scaffold generated (Task #{task.id})</b>\n\n{files}"
            )

    # ── System improvement validation ─────────────────────────────────────────

    async def _validate_system_improvement(self, task: Task) -> None:
        """Lightweight validation for system improvement patches: compile + config + security scan."""
        report = await self._run_with_keepalive(
            task,
            f"Validating improvement #{task.id}",
            lambda: self._run_improvement_checks(task),
        )
        report_path = self.reports_dir / f"sentinel_{task.id}_sysimprovement.json"
        report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        update_task_status(self.db_path, task.id, "completed", report)
        emit_event(self.db_path, "system_improvement_validated", AGENT_NAME, {
            "task_id": task.id,
            "passed": report.get("passed", False),
            "improvement_id": task.payload.get("improvement_id"),
        })

        # Advance improvement status if we have an improvement_id
        improvement_id = task.payload.get("improvement_id")
        if improvement_id:
            try:
                from shared.db.improvements import advance_improvement
                new_status = "validating" if report.get("passed") else "failed"
                advance_improvement(
                    self.db_path, int(improvement_id), new_status,
                    evidence_update={"sentinel_report": report},
                )
            except Exception as e:
                logger.warning("Could not advance improvement %s: %s", improvement_id, e)

        if self._notify:
            await self._notify(self._format_improvement_report(task, report))
        self._escalate_to_merlin_if_needed(task, report, "system improvement validation")
        if report.get("passed"):
            self._update_forge_validation_state(
                task.payload.get("forge_task_id"),
                "awaiting_validation",
                report,
                validation_state="passed_waiting_promotion",
            )
            if self._send_approval:
                await self._send_approval(
                    self._format_improvement_approval_description(task, report),
                    task.id,
                    "sentinel_approval",
                    {
                        "forge_task_id": task.payload.get("forge_task_id"),
                        "improvement_id": task.payload.get("improvement_id"),
                    },
                )
        else:
            self._update_forge_validation_state(
                task.payload.get("forge_task_id"),
                "failed",
                report,
                validation_state="failed",
            )
            self._send_revision_feedback_to_forge(task, report, "system improvement validation")

        try_reflect_after_task(
            llm=self.llm,
            db_path=self.db_path,
            data_dir=str(self.reports_dir.parent),
            agent_name=AGENT_NAME,
            task=task,
            result=report,
            owner_context=self.owner_memory.get_context(),
        )

    def _run_improvement_checks(self, task: Task) -> dict:
        """Run compile check, JSON config sanity, and LLM security review on patched files."""
        payload = task.payload or {}
        affected = payload.get("affected_components", [])
        patches_applied = payload.get("patches_applied", [])
        forge_review = payload.get("forge_review", {}) or {}
        warnings = []
        compile_passed = True
        config_passed = True
        forge_review_passed = forge_review.get("recommendation", "pass") == "pass"
        if not forge_review_passed:
            warnings.append(
                f"Forge self-review requested {forge_review.get('recommendation', 'revision')}: "
                f"{forge_review.get('review_summary', '')[:160]}"
            )

        # 1. Compile check all Python files touched
        py_files = [f for f in affected if f and str(f).endswith(".py")]
        for fp_str in py_files:
            fp = Path(fp_str)
            if fp.exists():
                r = subprocess.run(
                    [sys.executable, "-m", "py_compile", str(fp)],
                    capture_output=True, text=True,
                )
                if r.returncode != 0:
                    compile_passed = False
                    warnings.append(f"Compile error in {fp_str}: {r.stderr.strip()[:120]}")

        # 2. JSON config sanity for any .json files touched
        json_files = [f for f in affected if f and str(f).endswith(".json")]
        for fp_str in json_files:
            fp = Path(fp_str)
            if fp.exists():
                try:
                    json.loads(fp.read_text(encoding="utf-8", errors="replace"))
                except json.JSONDecodeError as e:
                    config_passed = False
                    warnings.append(f"Invalid JSON in {fp_str}: {e}")

        # 3. LLM security review of patched content
        security_passed = True
        security_findings = []
        if affected:
            security_passed, security_findings = self._llm_improvement_security_review(
                task.description, affected, patches_applied
            )
            for f in security_findings:
                if f.get("severity") in ("critical", "high"):
                    warnings.append(f"[SECURITY {f['severity'].upper()}] {f.get('message','')[:100]}")

        passed = compile_passed and config_passed and security_passed and forge_review_passed
        return {
            "passed": passed,
            "compile_passed": compile_passed,
            "config_passed": config_passed,
            "forge_review_passed": forge_review_passed,
            "forge_review": forge_review,
            "security_passed": security_passed,
            "security_findings": security_findings,
            "warnings": warnings,
            "py_files_checked": len(py_files),
            "json_files_checked": len(json_files),
            "patches_applied": len(patches_applied),
            "summary": ("All checks passed." if passed else f"Checks FAILED: {'; '.join(warnings[:3])}"),
        }

    def _llm_improvement_security_review(
        self, description: str, affected: list, patches_applied: list
    ) -> tuple[bool, list]:
        """Ask LLM to review patches for security issues."""
        patches_text = "\n".join(
            f"  [{p.get('change_type','?')}] {p.get('file','?')}: {p.get('description','')[:80]}"
            for p in patches_applied[:8]
        ) or "(no patches)"

        try:
            raw = self.llm.complete(
                messages=[{"role": "user", "content": (
                    f"Security review of system improvement patches:\n\n"
                    f"Change: {description[:200]}\n"
                    f"Affected: {', '.join(str(f) for f in affected[:6])}\n"
                    f"Patches:\n{patches_text}\n\n"
                    f"Return JSON: {{\"passed\": bool, \"findings\": [{{\"severity\": \"low|medium|high|critical\", \"category\": str, \"message\": str}}]}}\n"
                    f"Focus on: hardcoded secrets, command injection, path traversal, dangerous evals."
                )}],
                system="You are Sentinel security scanner. Return valid JSON only.",
                name="sentinel_improvement_security",
            )
            result = json.loads(raw.strip())
            return result.get("passed", True), result.get("findings", [])
        except Exception as e:
            logger.warning("LLM security review failed: %s", e)
            return True, []

    def _format_improvement_report(self, task: Task, report: dict) -> str:
        icon = "✅" if report.get("passed") else "❌"
        lines = [
            f"{icon} <b>Sentinel — System Improvement Validation (Task #{task.id})</b>\n",
            f"<b>Result:</b> {'PASSED' if report.get('passed') else 'FAILED'}",
            f"<b>Compile:</b> {'✓' if report.get('compile_passed') else '✗'}  "
            f"<b>Config:</b> {'✓' if report.get('config_passed') else '✗'}  "
            f"<b>Security:</b> {'✓' if report.get('security_passed') else '✗'}",
        ]
        if report.get("warnings"):
            lines.append("\n<b>Issues:</b>")
            for w in report["warnings"][:5]:
                lines.append(f"  ⚠ {w[:100]}")
        lines.append(f"\n<i>{report.get('summary','')}</i>")
        return "\n".join(lines)

    def _format_improvement_approval_description(self, task: Task, report: dict) -> str:
        forge_task_id = task.payload.get("forge_task_id")
        improvement_id = task.payload.get("improvement_id")
        forge_review = task.payload.get("forge_review") or {}
        return (
            f"🛡 <b>Sentinel — System Improvement Promotion Approval</b>\n\n"
            f"Sentinel task #{task.id}; Forge task #{forge_task_id or 'unknown'}; "
            f"Improvement #{improvement_id or 'unknown'}.\n\n"
            f"<b>Validation:</b> {report.get('summary', '')}\n"
            f"<b>Compile:</b> {'✓' if report.get('compile_passed') else '✗'}  "
            f"<b>Config:</b> {'✓' if report.get('config_passed') else '✗'}  "
            f"<b>Security:</b> {'✓' if report.get('security_passed') else '✗'}\n"
            f"<b>Forge self-review:</b> {forge_review.get('recommendation', 'unknown')}\n\n"
            "Approve only if you want this validated system improvement treated as promotable."
        )

    def _update_forge_validation_state(
        self,
        forge_task_id: Optional[int],
        status: str,
        report: dict,
        *,
        validation_state: str,
    ) -> None:
        if not forge_task_id:
            return
        merged_result: dict = {}
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute("SELECT result FROM tasks WHERE id=?", (forge_task_id,)).fetchone()
            if row and row["result"]:
                try:
                    merged_result = json.loads(row["result"]) or {}
                except Exception:
                    merged_result = {}
        except Exception as exc:
            logger.warning("Could not read Forge task #%s before Sentinel update: %s", forge_task_id, exc)
        merged_result["validation_state"] = validation_state
        merged_result["sentinel_report"] = report
        merged_result["sentinel_validated_at"] = datetime.now(timezone.utc).isoformat()
        update_task_status(self.db_path, int(forge_task_id), status, merged_result)
        emit_event(
            self.db_path,
            "forge_validation_state_updated",
            AGENT_NAME,
            {
                "forge_task_id": int(forge_task_id),
                "status": status,
                "validation_state": validation_state,
                "passed": bool(report.get("passed")),
            },
        )

    def _send_revision_feedback_to_forge(self, task: Task, report: dict, context: str) -> None:
        forge_task_id = task.payload.get("forge_task_id")
        warnings = report.get("warnings", []) or []
        summary = report.get("summary", "Sentinel found issues requiring Forge revision.")
        crit = int(report.get("security_critical", 0) or 0)
        high = int(report.get("security_high", 0) or 0)
        priority = "critical" if crit > 0 else "high" if high > 0 or not report.get("passed", True) else "normal"
        message = (
            f"Sentinel blocked {context} for Sentinel task #{task.id}"
            f"{f' / Forge task #{forge_task_id}' if forge_task_id else ''}.\n"
            f"Summary: {summary}\n"
            f"Issues: {'; '.join(str(w)[:180] for w in warnings[:6]) or 'No structured warnings captured.'}\n"
            "Do not promote. Revise the Forge plan/patches, then return for Sentinel validation."
        )
        try:
            send_agent_message(self.db_path, AGENT_NAME, "forge", message, priority=priority)
            emit_event(self.db_path, "sentinel_revision_feedback_sent", AGENT_NAME, {
                "task_id": task.id,
                "forge_task_id": forge_task_id,
                "context": context,
                "priority": priority,
            })
        except Exception as e:
            logger.warning("Could not send Sentinel revision feedback to Forge: %s", e)

    # ── Core validation logic (sync, run in executor) ─────────────────────────

    async def _health_check(self, task: Task) -> None:
        report = await asyncio.get_event_loop().run_in_executor(
            None, self._generate_health_report, task
        )
        report_path = self.reports_dir / f"sentinel_{task.id}_health.json"
        report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        update_task_status(self.db_path, task.id, "completed", report)
        emit_event(self.db_path, "health_check_complete", AGENT_NAME, {"task_id": task.id})
        self._escalate_to_merlin_if_needed(task, report, "health check")
        if self._notify:
            await self._notify(self._format_health_report(task, report))
        try_reflect_after_task(
            llm=self.llm,
            db_path=self.db_path,
            data_dir=str(self.reports_dir.parent),
            agent_name=AGENT_NAME,
            task=task,
            result=report,
            owner_context=self.owner_memory.get_context(),
        )

    def _generate_health_report(self, task: Task) -> dict:
        snapshot = self._health_snapshot()
        raw = self.llm.complete(
            messages=[{"role": "user", "content": json.dumps(snapshot, ensure_ascii=False, indent=2)}],
            system=(
                "You are Sentinel, the QA and validation agent for the Roderick ecosystem. "
                "Assess the system health snapshot and return valid JSON only with keys: "
                "summary, overall_status, risks, agent_observations, recommended_actions, "
                "needs_human_attention. Be practical and concise."
            ),
            name="sentinel_health_check",
        )
        try:
            report = json.loads(raw.strip())
        except json.JSONDecodeError:
            report = {
                "summary": raw[:500],
                "overall_status": "unknown",
                "risks": [],
                "agent_observations": [],
                "recommended_actions": [],
                "needs_human_attention": True,
            }
        report["_task_id"] = task.id
        report["_checked_at"] = datetime.now(timezone.utc).isoformat()
        report["_snapshot"] = snapshot
        return report

    def _health_snapshot(self) -> dict:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            agents = [
                dict(r) for r in conn.execute(
                    "SELECT name, status, model_used, last_success, last_error, last_message, updated_at "
                    "FROM agent_registry ORDER BY name"
                ).fetchall()
            ]
            recent_tasks = [
                dict(r) for r in conn.execute(
                    "SELECT id, to_agent, task_type, status, description, updated_at "
                    "FROM tasks ORDER BY id DESC LIMIT 12"
                ).fetchall()
            ]
            pending_approvals = [
                dict(r) for r in conn.execute(
                    "SELECT id, task_id, request_type, description, status, created_at "
                    "FROM approval_requests WHERE status='pending' ORDER BY id DESC LIMIT 12"
                ).fetchall()
            ]
        finally:
            conn.close()

        return {
            "agents": agents,
            "recent_tasks": recent_tasks,
            "pending_approvals": pending_approvals,
            "owner_context": self.owner_memory.get_context(),
        }

    def _run_validation(self, task: Task) -> dict:
        description = task.description
        files_created: list[str] = task.payload.get("files_created", [])
        smoke_cmd: Optional[str] = task.payload.get("smoke_command")
        project_dir: Optional[str] = task.payload.get("project_dir")

        warnings: list[str] = []
        test_files: list[str] = []

        # Gate 1: generate scaffold
        scaffold = self._generate_scaffold(
            description,
            files_created,
            project_path=project_dir or task.payload.get("project_path", ""),
            build_report=task.payload,
        )
        scaffold_generated = bool(scaffold.get("test_files"))
        test_files = scaffold.get("test_files", [])

        if not scaffold_generated:
            warnings.append("Could not generate meaningful test scaffold — no testable code identified.")

        # Write scaffold files to disk if project_dir provided
        if scaffold_generated and project_dir and scaffold.get("test_content"):
            for fname, content in scaffold["test_content"].items():
                fpath = Path(project_dir) / fname
                fpath.parent.mkdir(parents=True, exist_ok=True)
                try:
                    fpath.write_text(content, encoding="utf-8")
                    logger.info("Sentinel wrote scaffold: %s", fpath)
                except Exception as ex:
                    warnings.append(f"Failed to write {fname}: {ex}")

        # Gate 2a: pytest
        tests_run = False
        tests_passed = False
        if scaffold_generated and project_dir:
            pytest_result = self._run_pytest(project_dir, test_files, warnings)
            tests_run = pytest_result["ran"]
            tests_passed = pytest_result["passed"]
        elif not scaffold_generated:
            warnings.append("Tests skipped — no scaffold available.")
        else:
            # scaffold generated but no project_dir — mark as not run
            warnings.append("Test scaffold generated but not executed (no project_dir provided).")
            tests_run = False
            tests_passed = False

        # Gate 2b: docker build
        docker_build_passed = None
        if project_dir and self._has_dockerfile(project_dir):
            docker_build_passed = self._run_docker_build(project_dir, warnings)

        # Gate 2c: smoke command
        smoke_passed = None
        if smoke_cmd:
            smoke_passed = self._run_smoke(smoke_cmd, project_dir, warnings)

        # Gate 3: Security gates (only when we have a project directory to scan)
        security: dict = {"security_passed": True, "security_findings": [], "hardening_recommendations": []}
        if project_dir:
            try:
                security = self._run_security_gates(project_dir, files_created, description)
            except Exception as e:
                logger.error("Security gates error: %s", e, exc_info=True)
                warnings.append(f"Security gate error (non-blocking): {e}")

        return {
            "scaffold_generated": scaffold_generated,
            "tests_run": tests_run,
            "tests_passed": tests_passed,
            "docker_build_passed": docker_build_passed,
            "smoke_passed": smoke_passed,
            "warnings": warnings,
            "test_files": test_files,
            # Security fields
            "security_passed": security.get("security_passed", True),
            "security_findings": security.get("security_findings", []),
            "hardening_recommendations": security.get("hardening_recommendations", []),
            "security_critical": security.get("critical_count", 0),
            "security_high": security.get("high_count", 0),
            "summary": self._build_summary(
                tests_passed, docker_build_passed, smoke_passed, warnings,
                security.get("security_passed", True),
                security.get("critical_count", 0), security.get("high_count", 0),
            ),
            "_task_id": task.id,
            "_validated_at": datetime.now(timezone.utc).isoformat(),
        }

    def _generate_scaffold(
        self,
        description: str,
        files_created: list[str],
        project_path: str = "",
        build_report: Optional[dict] = None,
    ) -> dict:
        owner_context = self.owner_memory.get_context()
        prompt = self.prompt_template.format(
            description=description,
            files_created="\n".join(files_created) if files_created else "(no files listed)",
            owner_context=owner_context,
            project_path=project_path or "(no project path provided)",
            build_report=json.dumps(build_report or {}, ensure_ascii=False)[:3000],
        )
        raw = self.llm.complete(
            messages=[{"role": "user", "content": prompt}],
            system=(
                "You are Sentinel, a QA and validation agent for the Roderick ecosystem. "
                "Generate a pytest test scaffold for the described build task. "
                "Return valid JSON only — no markdown, no preamble. "
                "Format: {\"test_files\": [\"tests/test_x.py\"], \"test_content\": {\"tests/test_x.py\": \"<content>\"}, \"notes\": \"...\"}. "
                "If no meaningful tests are possible (e.g., config-only change), return {\"test_files\": [], \"test_content\": {}, \"notes\": \"reason\"}. "
                "Never invent passing tests — if you can't test it, say so clearly."
            ),
        )
        try:
            return json.loads(raw.strip())
        except json.JSONDecodeError:
            return {"test_files": [], "test_content": {}, "notes": raw[:200]}

    def _run_pytest(self, project_dir: str, test_files: list[str], warnings: list[str]) -> dict:
        try:
            cmd = ["python", "-m", "pytest", "--tb=short", "-q"] + test_files
            result = subprocess.run(
                cmd,
                cwd=project_dir,
                capture_output=True,
                text=True,
                timeout=120,
            )
            if result.returncode != 0:
                excerpt = (result.stdout + result.stderr)[-500:]
                warnings.append(f"pytest failed:\n{excerpt}")
                return {"ran": True, "passed": False}
            return {"ran": True, "passed": True}
        except FileNotFoundError:
            warnings.append("pytest not found — skipping test run.")
            return {"ran": False, "passed": False}
        except subprocess.TimeoutExpired:
            warnings.append("pytest timed out after 120s.")
            return {"ran": True, "passed": False}
        except Exception as ex:
            warnings.append(f"pytest error: {ex}")
            return {"ran": False, "passed": False}

    def _has_dockerfile(self, project_dir: str) -> bool:
        return (Path(project_dir) / "Dockerfile").exists()

    def _run_docker_build(self, project_dir: str, warnings: list[str]) -> bool:
        try:
            result = subprocess.run(
                ["docker", "build", "-t", "sentinel-test-build", "."],
                cwd=project_dir,
                capture_output=True,
                text=True,
                timeout=300,
            )
            if result.returncode != 0:
                warnings.append(f"docker build failed:\n{(result.stderr or result.stdout)[-300:]}")
                return False
            return True
        except FileNotFoundError:
            warnings.append("docker not found — skipping build check.")
            return False
        except subprocess.TimeoutExpired:
            warnings.append("docker build timed out after 300s.")
            return False
        except Exception as ex:
            warnings.append(f"docker build error: {ex}")
            return False

    def _run_smoke(self, smoke_cmd: str, project_dir: Optional[str], warnings: list[str]) -> bool:
        try:
            result = subprocess.run(
                smoke_cmd,
                shell=True,
                cwd=project_dir,
                capture_output=True,
                text=True,
                timeout=60,
            )
            if result.returncode != 0:
                warnings.append(f"smoke command failed:\n{(result.stdout + result.stderr)[-300:]}")
                return False
            return True
        except subprocess.TimeoutExpired:
            warnings.append("smoke command timed out after 60s.")
            return False
        except Exception as ex:
            warnings.append(f"smoke command error: {ex}")
            return False

    # ── Security gates ────────────────────────────────────────────────────────

    def _run_security_gates(
        self, project_dir: str, files_created: list[str], description: str
    ) -> dict:
        """Run all security checks. Returns structured security report."""
        findings: list[dict] = []
        recommendations: list[str] = []

        self._scan_secrets(project_dir, findings)
        self._run_bandit(project_dir, findings, recommendations)
        self._scan_dependencies(project_dir, findings, recommendations)
        self._check_dockerfile_security(project_dir, findings, recommendations)
        self._check_permissions(project_dir, findings, recommendations)
        self._llm_security_review(description, files_created, project_dir, findings, recommendations)

        critical = [f for f in findings if f["severity"] == "critical"]
        high     = [f for f in findings if f["severity"] == "high"]
        medium   = [f for f in findings if f["severity"] == "medium"]
        low      = [f for f in findings if f["severity"] == "low"]

        return {
            "security_passed": len(critical) == 0 and len(high) == 0,
            "security_findings": findings,
            "hardening_recommendations": recommendations,
            "critical_count": len(critical),
            "high_count": len(high),
            "medium_count": len(medium),
            "low_count": len(low),
        }

    # Secret patterns: (compiled_regex, severity, category, description)
    _SECRET_PATTERNS = [
        (re.compile(r'AKIA[0-9A-Z]{16}'), "critical", "secrets", "AWS Access Key ID"),
        (re.compile(r'-----BEGIN (?:RSA|EC|DSA|OPENSSH) PRIVATE KEY-----'), "critical", "secrets", "Private key in file"),
        (re.compile(r'\b\d{10}:[A-Za-z0-9_-]{35}\b'), "critical", "secrets", "Telegram bot token"),
        (re.compile(r'ghp_[A-Za-z0-9]{36}'), "critical", "secrets", "GitHub personal access token"),
        (re.compile(r'github_pat_[A-Za-z0-9_]{82}'), "critical", "secrets", "GitHub fine-grained PAT"),
        (re.compile(r'(?:password|passwd|pwd)\s*=\s*["\'][^"\']{8,}["\']', re.I), "high", "secrets", "Hardcoded password"),
        (re.compile(r'(?:secret|api_?key|token)\s*=\s*["\'][A-Za-z0-9+/=_-]{16,}["\']', re.I), "high", "secrets", "Hardcoded secret/token"),
        (re.compile(r'sk-[A-Za-z0-9]{32,}'), "critical", "secrets", "OpenAI/Anthropic API key"),
    ]
    _SKIP_EXTENSIONS = {'.pyc', '.png', '.jpg', '.jpeg', '.gif', '.ico', '.woff', '.ttf', '.pdf', '.zip'}
    _SKIP_DIRS = {'__pycache__', '.git', 'node_modules', '.venv', 'venv', '.mypy_cache'}

    def _scan_secrets(self, project_dir: str, findings: list[dict]) -> None:
        root = Path(project_dir)
        if not root.is_dir():
            return
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            if any(part in self._SKIP_DIRS for part in path.parts):
                continue
            if path.suffix.lower() in self._SKIP_EXTENSIONS:
                continue
            # Flag sensitive filenames regardless of content
            if path.name in (".env", ".env.local", ".env.production") and path.stat().st_size > 0:
                findings.append({
                    "severity": "high", "category": "secrets",
                    "message": f"Sensitive file present in project: {path.relative_to(root)}",
                })
                continue
            if path.suffix.lower() in (".pem", ".key", ".p12", ".pfx"):
                findings.append({
                    "severity": "high", "category": "secrets",
                    "message": f"Credential/key file in project: {path.relative_to(root)}",
                })
                continue
            try:
                content = path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            for pattern, severity, category, description in self._SECRET_PATTERNS:
                if pattern.search(content):
                    findings.append({
                        "severity": severity, "category": category,
                        "message": f"{description} detected in {path.relative_to(root)}",
                    })
                    break  # one finding per file is enough

    def _run_bandit(self, project_dir: str, findings: list[dict], recommendations: list[str]) -> None:
        """Run bandit static analysis on Python files."""
        root = Path(project_dir)
        if not any(root.rglob("*.py")):
            return
        try:
            result = subprocess.run(
                ["python", "-m", "bandit", "-r", str(root), "-f", "json", "--exit-zero", "-ll"],
                capture_output=True, text=True, timeout=120,
            )
            data = json.loads(result.stdout or "{}")
        except (FileNotFoundError, subprocess.TimeoutExpired, json.JSONDecodeError) as e:
            logger.warning("bandit unavailable or failed: %s", e)
            return

        severity_map = {"HIGH": "high", "MEDIUM": "medium", "LOW": "low"}
        seen: set[str] = set()
        for issue in data.get("results", []):
            sev = severity_map.get(issue.get("issue_severity", "").upper(), "low")
            conf = issue.get("issue_confidence", "")
            if sev == "low" and conf != "HIGH":
                continue  # skip noisy low-confidence lows
            key = f"{issue.get('test_id')}:{issue.get('filename')}"
            if key in seen:
                continue
            seen.add(key)
            rel = Path(issue.get("filename", "")).name
            findings.append({
                "severity": sev, "category": "static_analysis",
                "message": f"{issue.get('test_name', 'bandit')}: {issue.get('issue_text', '')} ({rel}:{issue.get('line_number', '?')})",
            })

        metrics = data.get("metrics", {}).get("_totals", {})
        if metrics.get("SEVERITY.HIGH", 0) > 0:
            recommendations.append("Fix high-severity bandit issues before promotion (see security findings).")

    def _scan_dependencies(self, project_dir: str, findings: list[dict], recommendations: list[str]) -> None:
        """Run pip-audit against requirements.txt if present."""
        req_file = Path(project_dir) / "requirements.txt"
        if not req_file.exists():
            return
        try:
            result = subprocess.run(
                ["python", "-m", "pip_audit", "--format=json", "-r", str(req_file), "--progress-spinner=off"],
                capture_output=True, text=True, timeout=120,
            )
            data = json.loads(result.stdout or "{}")
        except (FileNotFoundError, subprocess.TimeoutExpired, json.JSONDecodeError) as e:
            logger.warning("pip-audit unavailable or failed: %s", e)
            return

        for dep in data.get("dependencies", []):
            for vuln in dep.get("vulns", []):
                fix = vuln.get("fix_versions", [])
                fix_str = f" (fix: {', '.join(fix[:2])})" if fix else ""
                findings.append({
                    "severity": "high", "category": "dependency_vuln",
                    "message": f"{dep['name']}=={dep.get('version', '?')} — {vuln.get('id', 'CVE')}{fix_str}",
                })
        if any(f["category"] == "dependency_vuln" for f in findings):
            recommendations.append("Update vulnerable dependencies to the fixed versions listed in security findings.")

    def _check_dockerfile_security(self, project_dir: str, findings: list[dict], recommendations: list[str]) -> None:
        """Parse Dockerfile for common security issues."""
        df = Path(project_dir) / "Dockerfile"
        if not df.exists():
            return
        try:
            lines = df.read_text(encoding="utf-8").splitlines()
        except Exception:
            return

        has_user = False
        has_healthcheck = False
        for line in lines:
            stripped = line.strip()
            upper = stripped.upper()

            if upper.startswith("USER ") and not upper.startswith("USER ROOT"):
                has_user = True
            if upper.startswith("USER ROOT"):
                findings.append({"severity": "high", "category": "docker_security",
                    "message": "Dockerfile sets USER root explicitly"})
            if upper.startswith("HEALTHCHECK"):
                has_healthcheck = True
            if re.match(r'^ADD\s+https?://', stripped, re.I):
                findings.append({"severity": "medium", "category": "docker_security",
                    "message": "ADD used with URL — use COPY + RUN curl for better layer control"})
            if re.match(r'^ENV\s+.*(PASSWORD|SECRET|API_KEY|TOKEN)\s*=\s*\S', stripped, re.I):
                findings.append({"severity": "critical", "category": "docker_security",
                    "message": f"Secret baked into image via ENV: {stripped[:80]}"})
            if 'chmod 777' in stripped or 'chmod -R 777' in stripped:
                findings.append({"severity": "high", "category": "docker_security",
                    "message": "chmod 777 in Dockerfile — avoid world-writable permissions"})

        if not has_user:
            findings.append({"severity": "high", "category": "docker_security",
                "message": "No USER directive — container runs as root by default"})
            recommendations.append("Add 'RUN useradd -r appuser && USER appuser' to your Dockerfile.")
        if not has_healthcheck:
            recommendations.append("Add a HEALTHCHECK instruction to your Dockerfile.")

    def _check_permissions(self, project_dir: str, findings: list[dict], recommendations: list[str]) -> None:
        """Check for dangerous file permissions and misplaced sensitive files."""
        root = Path(project_dir)
        if not root.is_dir():
            return
        world_writable_found = False
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            if any(part in self._SKIP_DIRS for part in path.parts):
                continue
            try:
                mode = path.stat().st_mode
            except Exception:
                continue
            # World-writable
            if mode & stat.S_IWOTH:
                if not world_writable_found:
                    findings.append({"severity": "high", "category": "permissions",
                        "message": f"World-writable file: {path.relative_to(root)}"})
                    world_writable_found = True  # report once, don't flood
            # Sensitive filename checks
            name_lower = path.name.lower()
            if name_lower in ("id_rsa", "id_ed25519", "id_ecdsa", "id_dsa"):
                findings.append({"severity": "critical", "category": "permissions",
                    "message": f"SSH private key in project tree: {path.relative_to(root)}"})
        if world_writable_found:
            recommendations.append("Remove world-writable (chmod o-w) permissions from project files.")

    def _llm_security_review(
        self,
        description: str,
        files_created: list[str],
        project_dir: str,
        findings: list[dict],
        recommendations: list[str],
    ) -> None:
        """LLM-based security review for issues static tools miss."""
        # Sample up to 3 Python files for review
        root = Path(project_dir)
        samples: list[str] = []
        for fp in root.rglob("*.py"):
            if any(p in self._SKIP_DIRS for p in fp.parts):
                continue
            try:
                content = fp.read_text(encoding="utf-8", errors="ignore")
                if content.strip():
                    samples.append(f"# {fp.relative_to(root)}\n{content[:800]}")
            except Exception:
                continue
            if len(samples) >= 3:
                break

        if not samples:
            return

        prompt = (
            f"Security audit for a Forge-built project.\n\n"
            f"Description: {description}\n"
            f"Files created: {', '.join(files_created[:10])}\n\n"
            f"Code samples:\n\n{''.join(samples)}\n\n"
            "Identify security issues not caught by static analysis: "
            "logic flaws, insecure auth patterns, missing input validation, "
            "SSRF, path traversal, injection risks, over-privileged operations.\n\n"
            'Return JSON: {"findings": [{"severity": "critical|high|medium|low", '
            '"category": str, "message": str}], '
            '"recommendations": [str]}. '
            "Only include real issues. Empty arrays are fine. No markdown."
        )
        tech_stack_ctx = ""
        if self.owner_memory:
            ts = self.owner_memory.get_tech_stack()
            if ts:
                tech_stack_ctx = f"\n\nTech stack context:\n{ts[:800]}"
        try:
            raw = self.llm.complete(
                messages=[{"role": "user", "content": prompt + tech_stack_ctx}],
                system=(
                    "You are Sentinel, a security reviewer for the owner's Roderick agent ecosystem. "
                    "Return valid JSON only — no markdown, no preamble."
                ),
                name="sentinel_security_review",
            )
            data = json.loads(raw.strip())
            for f in data.get("findings", []):
                if all(k in f for k in ("severity", "category", "message")):
                    findings.append(f)
            recommendations.extend(data.get("recommendations", []))
        except Exception as e:
            logger.warning("LLM security review failed: %s", e)

    # ── Formatting ────────────────────────────────────────────────────────────

    def _format_health_report(self, task: Task, report: dict) -> str:
        lines = [
            f"<b>Sentinel Health Check — Task #{task.id}</b>",
            f"<b>Status:</b> {report.get('overall_status', 'unknown')}",
            f"<b>Summary:</b> {report.get('summary', '')[:500]}",
        ]
        risks = report.get("risks", [])
        if risks:
            lines.append("\n<b>Risks:</b>")
            for risk in risks[:5]:
                lines.append(f"  • {str(risk)[:140]}")
        actions = report.get("recommended_actions", [])
        if actions:
            lines.append("\n<b>Recommended actions:</b>")
            for action in actions[:5]:
                lines.append(f"  • {str(action)[:140]}")
        return "\n".join(lines)

    def _escalate_to_merlin_if_needed(self, task: Task, report: dict, reason: str) -> None:
        """Ask Merlin for deeper research when Sentinel sees patterns that need analysis."""
        risks = report.get("risks") or []
        warnings = report.get("warnings") or []
        findings = report.get("security_findings") or []
        recommended = report.get("recommended_actions") or report.get("hardening_recommendations") or []
        needs_attention = bool(report.get("needs_human_attention"))
        high_findings = [
            f for f in findings
            if str(f.get("severity", "")).lower() in {"critical", "high"}
        ]
        if not (risks or high_findings or needs_attention or len(warnings) >= 2):
            return

        summary = str(report.get("summary") or report.get("overall_status") or reason)[:500]
        focus = (
            f"Sentinel found a {reason} issue while processing task #{task.id}.\n\n"
            f"Summary: {summary}\n"
            f"Risks: {json.dumps(risks[:5], ensure_ascii=False)}\n"
            f"High findings: {json.dumps(high_findings[:5], ensure_ascii=False)}\n"
            f"Warnings: {json.dumps(warnings[:5], ensure_ascii=False)}\n"
            f"Recommended actions: {json.dumps(recommended[:5], ensure_ascii=False)}\n\n"
            "Research the root cause and recommend evidence-backed next steps for Sentinel and Roderick. "
            "If Forge should implement a fix, explain the bounded scope."
        )
        key = self._research_key(focus)
        open_statuses = {"pending", "approved", "in_progress"}
        for existing in list_tasks(self.db_path, to_agent="merlin", limit=80):
            if existing.task_type not in {"system_research", "performance_research", "agent_diagnostics"}:
                continue
            if existing.status in open_statuses and self._research_key(existing.description) == key:
                return
        critical_findings = [
            f for f in findings
            if str(f.get("severity", "")).lower() == "critical"
        ]
        inferred_priority, inferred_urgency, _ = infer_priority(
            focus,
            default_priority="critical" if critical_findings else "high" if high_findings else "normal",
            default_urgency="immediate" if critical_findings else "today",
        )
        merlin_task = enqueue_task(
            self.db_path,
            Task(
                to_agent="merlin",
                from_agent=AGENT_NAME,
                task_type="system_research",
                description=focus,
                priority=inferred_priority,
                urgency=inferred_urgency,
                domain="operations",
                approval_required=False,
            ),
        )
        emit_event(self.db_path, "sentinel_research_escalated", AGENT_NAME, {
            "task_id": task.id,
            "merlin_task_id": merlin_task.id,
            "reason": reason,
            "risk_count": len(risks),
            "high_finding_count": len(high_findings),
            "priority": inferred_priority,
            "urgency": inferred_urgency,
        })

    @staticmethod
    def _research_key(text: str) -> str:
        return re.sub(r"[^a-z0-9]+", " ", text.lower())[:240].strip()

    def _build_summary(
        self,
        tests_passed: bool,
        docker_build_passed: Optional[bool],
        smoke_passed: Optional[bool],
        warnings: list[str],
        security_passed: bool = True,
        security_critical: int = 0,
        security_high: int = 0,
    ) -> str:
        parts = []
        parts.append("tests: " + ("✓" if tests_passed else "✗"))
        if docker_build_passed is not None:
            parts.append("docker: " + ("✓" if docker_build_passed else "✗"))
        if smoke_passed is not None:
            parts.append("smoke: " + ("✓" if smoke_passed else "✗"))
        if not security_passed:
            parts.append(f"security: ✗ ({security_critical}C/{security_high}H)")
        else:
            parts.append("security: ✓")
        overall_passed = (
            tests_passed
            and docker_build_passed is not False
            and smoke_passed is not False
            and security_passed
        )
        status = "PASSED" if overall_passed else "FAILED"
        summary = f"{status} — {', '.join(parts)}"
        if warnings:
            summary += f" | {len(warnings)} warning(s)"
        return summary

    def _format_report(self, task: Task, report: dict) -> str:
        tests_ok = report.get("tests_passed") and report.get("scaffold_generated")
        sec_ok = report.get("security_passed", True)
        overall = tests_ok and sec_ok
        status = "✅ PASSED" if overall else "❌ FAILED"

        lines = [
            f"🛡 <b>Sentinel Report — Task #{task.id}</b>",
            f"<b>Status:</b> {status}",
            f"<b>Summary:</b> {report.get('summary', '')}",
        ]
        test_files = report.get("test_files", [])
        if test_files:
            lines.append(f"<b>Tests:</b> {len(test_files)} file(s) generated")

        # Security section
        sec_findings = report.get("security_findings", [])
        if sec_findings:
            crit = [f for f in sec_findings if f.get("severity") == "critical"]
            high = [f for f in sec_findings if f.get("severity") == "high"]
            med  = [f for f in sec_findings if f.get("severity") == "medium"]
            lines.append(
                f"\n<b>Security:</b> {len(crit)} critical · {len(high)} high · {len(med)} medium"
            )
            for f in (crit + high)[:4]:
                lines.append(f"  [{f['severity'].upper()}] {f['category']}: {f['message'][:90]}")
        elif report.get("security_passed") is True:
            lines.append("\n<b>Security:</b> ✓ No critical/high findings")

        recs = report.get("hardening_recommendations", [])
        if recs:
            lines.append("\n<b>Hardening recommendations:</b>")
            for r in recs[:3]:
                lines.append(f"  • {r[:100]}")

        warnings = report.get("warnings", [])
        if warnings:
            lines.append("\n<b>Warnings:</b>")
            for w in warnings[:3]:
                lines.append(f"  ⚠ {w[:120]}")
            if len(warnings) > 3:
                lines.append(f"  … and {len(warnings) - 3} more")
        return "\n".join(lines)

    def _format_approval_description(self, task: Task, report: dict) -> str:
        sec_findings = report.get("security_findings", [])
        med_low = [f for f in sec_findings if f.get("severity") in ("medium", "low")]
        sec_line = "✓ Clean" if not sec_findings else f"✓ {len(med_low)} medium/low finding(s) — no blockers"
        return (
            f"🛡 <b>Sentinel — Promotion Approval</b>\n\n"
            f"Task #{task.id}: {task.description[:100]}\n\n"
            f"<b>Validation:</b> {report.get('summary', '')}\n"
            f"<b>Tests:</b> {len(report.get('test_files', []))} file(s)\n"
            f"<b>Security:</b> {sec_line}\n\n"
            f"Approve to mark build as live."
        )
