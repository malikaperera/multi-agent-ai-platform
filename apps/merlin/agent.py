"""
Merlin — research agent.

Polls task queue for pending research tasks (one at a time).
Calls LLM with structured research prompt.
Writes findings to data/reports/merlin_{task_id}.json.
Notifies Roderick via callback when complete.
"""
import asyncio
import contextlib
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Coroutine, Optional

from shared.db.agents import emit_heartbeat, update_agent_status
from shared.db.context import format_task_summaries, get_recent_task_summaries
from shared.db.connection import connect_sqlite
from shared.db.events import emit_event
from shared.db.improvements import advance_improvement, list_active_improvements, upsert_improvement, Improvement
from shared.db.tasks import enqueue_task, get_next_task, list_tasks, requeue_in_progress_tasks, touch_task, update_task_status
from shared.agent_learning import try_reflect_after_task
from shared.llm.anthropic_provider import AnthropicProvider
from shared.memory.founder import OwnerMemory
from shared.schemas.task import Task

logger = logging.getLogger(__name__)

AGENT_NAME = "merlin"


class MerlinAgent:
    def __init__(
        self,
        llm: AnthropicProvider,
        db_path: str,
        data_dir: str,
        config: dict,
        owner_memory: Optional["OwnerMemory"] = None,
        diagnostic_llm: Optional[AnthropicProvider] = None,
        research_model: Optional[str] = None,
        diagnostic_model: Optional[str] = None,
    ):
        self.llm = llm
        self.research_llm = llm
        self.diagnostic_llm = diagnostic_llm or llm
        self.config = config
        self.research_model = research_model or getattr(llm, "model", "unknown")
        self.diagnostic_model = diagnostic_model or getattr(self.diagnostic_llm, "model", self.research_model)
        self.db_path = db_path
        self.reports_dir = Path(data_dir) / "reports"
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self.owner_memory = owner_memory
        self.prompt_template = config.get("merlin", {}).get(
            "research_prompt_template",
            "Research the following question thoroughly and return structured JSON findings.\n\nQuestion: {question}",
        )
        self.poll_interval = int(
            config.get("scheduler", {}).get("worker_poll_interval_seconds", 10)
        )
        proactive_cfg = config.get("merlin", {}).get("proactive_research", {})
        self.proactive_topics = proactive_cfg.get("topics", [])
        self.continuous_mode = bool(proactive_cfg.get("continuous_mode", False))
        self.max_concurrent_tasks = max(1, int(proactive_cfg.get("max_concurrent_tasks", 1)))
        self.keepalive_seconds = max(10, int(config.get("observability", {}).get("task_keepalive_seconds", 20)))
        self._notify: Optional[Callable[[str], Coroutine]] = None
        self._send_approval: Optional[Callable[[str, int, str, dict], Coroutine]] = None

    def set_notify(self, fn: Callable[[str], Coroutine]) -> None:
        """Inject the async callback used to send results to Telegram."""
        self._notify = fn

    def set_approval_sender(self, fn: Callable[[str, int, str, dict], Coroutine]) -> None:
        """Inject async approval sender used for Forge handoff requests."""
        self._send_approval = fn

    async def run(self) -> None:
        """Long-running poll loop with bounded concurrency for deep research."""
        logger.info(
            "Merlin worker started (poll interval %ds, concurrency=%d)",
            self.poll_interval,
            self.max_concurrent_tasks,
        )
        recovered = requeue_in_progress_tasks(self.db_path, AGENT_NAME)
        if recovered:
            logger.info("Merlin recovered %d abandoned in-progress task(s)", recovered)
        update_agent_status(self.db_path, AGENT_NAME, "idle")
        emit_heartbeat(
            self.db_path,
            AGENT_NAME,
            current_task_id=None,
            current_model=f"{self.research_model}|diagnostic:{self.diagnostic_model}",
        )
        active: set[asyncio.Task] = set()

        while True:
            try:
                if active:
                    done = {t for t in active if t.done()}
                    for finished in done:
                        active.remove(finished)
                        exc = finished.exception()
                        if exc:
                            logger.error("Merlin concurrent task failed: %s", exc, exc_info=exc)
                    if not active:
                        update_agent_status(self.db_path, AGENT_NAME, "idle", "Merlin research queue is clear")

                task = get_next_task(self.db_path, AGENT_NAME, "pending")
                if task and len(active) < self.max_concurrent_tasks:
                    update_task_status(self.db_path, task.id, "in_progress")
                    active.add(asyncio.create_task(self._process(task)))
                    if len(active) >= self.max_concurrent_tasks:
                        logger.info("Merlin concurrency full (%d/%d)", len(active), self.max_concurrent_tasks)
                elif not active:
                    await asyncio.sleep(self.poll_interval)
                else:
                    await asyncio.sleep(1)
            except Exception as e:
                logger.error("Merlin worker error: %s", e, exc_info=True)
                await asyncio.sleep(self.poll_interval)

    # ── Diagnostic task types ─────────────────────────────────────────────────
    _DIAGNOSTIC_TYPES = {"system_research", "performance_research", "agent_diagnostics"}

    async def _process(self, task: Task) -> None:
        logger.info("Merlin processing task #%d: %s", task.id, task.description[:80])
        update_agent_status(self.db_path, AGENT_NAME, "busy", f"Researching: {task.description[:60]}")
        active_llm = self._llm_for_task(task)
        emit_heartbeat(self.db_path, AGENT_NAME, current_task_id=task.id, current_model=active_llm.model)
        if task.status != "in_progress":
            update_task_status(self.db_path, task.id, "in_progress")

        try:
            if task.task_type in self._DIAGNOSTIC_TYPES:
                result = await self._run_with_keepalive(
                    task,
                    f"Investigating: {task.description[:60]}",
                    lambda: self._investigate(task),
                    active_llm.model,
                )
            else:
                result = await self._run_with_keepalive(
                    task,
                    f"Researching: {task.description[:60]}",
                    lambda: self._research(task),
                    active_llm.model,
                )
            report_path = self.reports_dir / f"merlin_{task.id}.json"
            report_path.write_text(
                json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            update_task_status(self.db_path, task.id, "completed", result)
            update_agent_status(self.db_path, AGENT_NAME, "idle", f"Completed task #{task.id}")
            emit_event(self.db_path, "research_complete", AGENT_NAME, {
                "task_id": task.id,
                "summary": result.get("summary", "")[:120],
            })

            # If diagnostic, advance the improvement record to 'proposed'
            if task.task_type in self._DIAGNOSTIC_TYPES:
                improvement = self._handle_investigation_result(task, result)
                self._emit_atlas_tasks(result)
                if result.get("forge_recommended") and improvement:
                    if self._should_escalate_immediately(result):
                        await self._route_urgent_forge_improvement(task, result, improvement)
                    else:
                        emit_event(self.db_path, "merlin_improvement_held_for_digest", AGENT_NAME, {
                            "merlin_task_id": task.id,
                            "improvement_id": improvement.id,
                            "summary": result.get("forge_description") or result.get("summary", ""),
                        })
                        logger.info(
                            "Merlin held improvement #%s for daily Forge consolidation instead of creating an immediate Forge task",
                            improvement.id,
                        )
            else:
                self._emit_atlas_tasks(result)
                await self._route_domain_artifact(task, result)
                if self._notify:
                    await self._notify(self._format_result(task, result))

            await self._reflect_after_task_best_effort(active_llm, task, result)
            self._enqueue_continuous_research(task, result)

        except Exception as e:
            logger.error("Merlin task #%d failed: %s", task.id, e, exc_info=True)
            update_task_status(self.db_path, task.id, "failed", {"error": str(e)})
            update_agent_status(self.db_path, AGENT_NAME, "idle", f"Task #{task.id} failed: {e}")

            if self._notify:
                await self._notify(
                    f"⚠️ <b>Merlin — Task #{task.id} failed</b>\n\n"
                    f"<i>{task.description[:80]}</i>\n\nError: {e}"
                )

        finally:
            emit_heartbeat(
                self.db_path,
                AGENT_NAME,
                current_task_id=None,
                current_model=f"{self.research_model}|diagnostic:{self.diagnostic_model}",
            )

    async def _reflect_after_task_best_effort(self, llm, task: Task, result: dict) -> None:
        """Run ecosystem learning without letting reflection trap Merlin's worker slot."""
        try:
            await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: try_reflect_after_task(
                        llm=llm,
                        db_path=self.db_path,
                        data_dir=str(self.reports_dir.parent),
                        agent_name=AGENT_NAME,
                        task=task,
                        result=result,
                        owner_context=self.owner_memory.get_context() if self.owner_memory else "",
                    ),
                ),
                timeout=90,
            )
        except asyncio.TimeoutError:
            logger.warning("Merlin reflection timed out for task #%s; continuing worker loop", task.id)
        except Exception as exc:
            logger.warning("Merlin reflection failed for task #%s: %s", task.id, exc)

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

    def _llm_for_task(self, task: Task):
        """Route Merlin work to the smallest model that can truthfully do the job."""
        if task.task_type in self._DIAGNOSTIC_TYPES:
            return self.diagnostic_llm
        return self.research_llm

    def _enqueue_continuous_research(self, task: Task, result: dict) -> None:
        """Keep Merlin studying fresh evidence without flooding the queue."""
        if not self.continuous_mode:
            return
        for status in ("pending", "in_progress", "approved"):
            if get_next_task(self.db_path, AGENT_NAME, status):
                return

        if self.proactive_topics:
            topic = self.proactive_topics[(task.id or 0) % len(self.proactive_topics)]
        else:
            topic = (
                "Continue studying recent system evidence and find the next "
                "evidence-backed improvement opportunity."
            )
        summary = result.get("summary") or result.get("recommended_next_step") or ""
        enqueue_task(
            self.db_path,
            Task(
                to_agent=AGENT_NAME,
                from_agent=AGENT_NAME,
                task_type="system_research",
                description=f"{topic}\n\nPrevious finding to build on: {summary[:240]}",
                priority="normal",
                urgency="today",
                domain="operations",
                approval_required=False,
            ),
        )
        logger.info("Merlin queued continuous system_research follow-up")

    def _build_research_system(self) -> str:
        """Build Merlin's system prompt with tech stack and prior research context."""
        parts = [
            "You are Merlin, a structured research agent working for the system owner.\n"
            "Always respond in English with valid JSON only - no markdown fences, no preamble. "
            "Do not use Chinese or any non-English prose unless the user explicitly asks for translation."
        ]
        if self.owner_memory:
            owner_ctx = self.owner_memory.get_context()
            if owner_ctx:
                parts.append(f"\n### Owner Context\n{owner_ctx}")
            tech_stack = self.owner_memory.get_tech_stack()
            if tech_stack:
                parts.append(f"\n### Current Tech Stack\n{tech_stack}")
        # Prior research topics — avoid duplication
        prior = get_recent_task_summaries(self.db_path, to_agent="merlin", limit=6)
        if prior:
            prior_text = format_task_summaries(prior)
            parts.append(
                f"\n### Previously Researched (avoid re-covering ground already done)\n{prior_text}"
            )
        return "\n".join(parts)

    def _research(self, task: Task) -> dict:
        owner_ctx = self.owner_memory.get_context() if self.owner_memory else ""
        prompt = self.prompt_template.format(
            question=task.description,
            owner_context=owner_ctx,
        )
        raw = self.research_llm.complete(
            messages=[{"role": "user", "content": prompt}],
            system=self._build_research_system(),
            name="merlin_research",
        )
        try:
            result = json.loads(raw.strip())
        except json.JSONDecodeError:
            # Wrap unstructured response
            result = {
                "summary": raw[:500],
                "findings": [raw],
                "options": [],
                "tradeoffs": "",
                "confidence": 0.5,
                "recommended_next_step": "Review the raw findings above.",
            }
        result["_task_id"] = task.id
        result["_researched_at"] = datetime.now(timezone.utc).isoformat()
        return self._ensure_english_result(result, "merlin_research_translate", self.research_llm)

    # ── System investigation ──────────────────────────────────────────────────

    def _collect_system_evidence(self, task: Task) -> dict:
        """Gather live evidence from DB, events, agent states, learning notes."""
        import sqlite3 as _sql
        evidence: dict = {}

        # Recent agent states
        try:
            conn = _sql.connect(self.db_path, timeout=10)
            conn.row_factory = _sql.Row
            agents = conn.execute(
                "SELECT name, status, last_heartbeat, last_error, last_message, current_task_id, current_model "
                "FROM agent_registry ORDER BY name"
            ).fetchall()
            evidence["agent_states"] = [dict(a) for a in agents]
        except Exception as e:
            evidence["agent_states"] = [{"error": str(e)}]
        finally:
            conn.close()

        # Recent failed / stuck tasks
        try:
            conn = _sql.connect(self.db_path, timeout=10)
            conn.row_factory = _sql.Row
            failed = conn.execute(
                "SELECT id, to_agent, task_type, description, status, result, created_at, updated_at "
                "FROM tasks WHERE status IN ('failed','in_progress') ORDER BY updated_at DESC LIMIT 20"
            ).fetchall()
            evidence["failed_or_stuck_tasks"] = [dict(t) for t in failed]
        except Exception:
            evidence["failed_or_stuck_tasks"] = []
        finally:
            conn.close()

        # Recent events
        try:
            conn = _sql.connect(self.db_path, timeout=10)
            conn.row_factory = _sql.Row
            evts = conn.execute(
                "SELECT event_type, agent, payload, created_at FROM events ORDER BY created_at DESC LIMIT 40"
            ).fetchall()
            evidence["recent_events"] = [
                {
                    "type": e["event_type"],
                    "agent": e["agent"],
                    "ts": e["created_at"],
                    "payload": (
                        e["payload"][:200]
                        if isinstance(e["payload"], str)
                        else json.dumps(e["payload"], ensure_ascii=False)[:200]
                        if e["payload"]
                        else ""
                    ),
                }
                for e in evts
            ]
        except Exception:
            evidence["recent_events"] = []
        finally:
            conn.close()

        # Learning notes (most recent 2000 chars per agent)
        try:
            learning_dir = self.reports_dir.parent / "agent_learning"
            notes = {}
            if learning_dir.exists():
                for f in sorted(learning_dir.glob("*.md")):
                    notes[f.stem] = f.read_text(encoding="utf-8", errors="ignore")[-2000:]
            evidence["learning_notes"] = notes
        except Exception:
            evidence["learning_notes"] = {}

        # Active improvements
        try:
            active = list_active_improvements(self.db_path)
            evidence["active_improvements"] = [
                {"id": i.id, "title": i.title, "status": i.status, "signal": i.origin_signal}
                for i in active
            ]
        except Exception:
            evidence["active_improvements"] = []

        evidence["task_type"] = task.task_type
        evidence["question"] = task.description
        evidence["improvement_id"] = task.payload.get("improvement_id")
        return evidence

    def _investigate(self, task: Task) -> dict:
        """Evidence-backed investigation returning structured report."""
        evidence = self._collect_system_evidence(task)
        owner_ctx = self.owner_memory.get_context() if self.owner_memory else ""

        prompt = (
            f"You are Merlin running a {task.task_type} investigation.\n\n"
            f"Question/focus: {task.description}\n\n"
            f"Evidence gathered:\n{json.dumps(evidence, indent=2, ensure_ascii=False)[:6000]}\n\n"
            f"Owner context: {owner_ctx[:400]}\n\n"
            "Produce a structured investigation report. "
            "Return valid JSON only with keys:\n"
            "- summary (str): 2-3 sentence executive summary\n"
            "- verified_facts (list of str): things confirmed by evidence\n"
            "- likely_causes (list of str): probable root causes\n"
            "- unknowns (list of str): what we cannot determine from available evidence\n"
            "- affected_components (list of str): files, agents, or subsystems implicated\n"
            "- recommended_actions (list of str): specific actionable steps\n"
            "- forge_recommended (bool): should Forge implement a fix?\n"
            "- forge_scope (str): if forge_recommended, what scope: small_patch | medium_change | large_refactor\n"
            "- forge_description (str): if forge_recommended, concise description of the change\n"
            "- risk_level (str): low | medium | high\n"
            "- priority (str): low | normal | high | critical\n"
            "- confidence (float 0.0-1.0): how confident you are in findings\n"
            "Use English only. No markdown, no preamble."
        )
        raw = self.diagnostic_llm.complete(
            messages=[{"role": "user", "content": prompt}],
            system=self._build_research_system(),
            name="merlin_investigation",
        )
        try:
            result = json.loads(raw.strip())
        except json.JSONDecodeError:
            result = {
                "summary": raw[:500],
                "verified_facts": [],
                "likely_causes": ["Unable to parse structured response"],
                "unknowns": ["Full structured findings not available"],
                "affected_components": [],
                "recommended_actions": ["Review raw Merlin output"],
                "forge_recommended": False,
                "forge_scope": "unknown",
                "forge_description": "",
                "risk_level": "unknown",
                "priority": "normal",
                "confidence": 0.3,
            }
        result["_task_id"] = task.id
        result["_investigated_at"] = datetime.now(timezone.utc).isoformat()
        result["_evidence_keys"] = list(evidence.keys())
        return self._ensure_english_result(result, "merlin_investigation_translate", self.diagnostic_llm)

    _CJK_RE = re.compile(r"[\u3400-\u9fff\u3040-\u30ff\uac00-\ud7af]")

    def _ensure_english_result(self, result: dict, name: str, llm: Optional[AnthropicProvider] = None) -> dict:
        """Retry once as a translation pass if the local model leaks non-English text."""
        try:
            serialized = json.dumps(result, ensure_ascii=False)
        except TypeError:
            return result
        if not self._CJK_RE.search(serialized):
            return result
        logger.warning("Merlin produced non-English text; retrying English-only normalization")
        try:
            raw = (llm or self.research_llm).complete(
                messages=[{"role": "user", "content": (
                    "Translate every human-readable string in this JSON object into clear English. "
                    "Preserve all keys and structure. Return valid JSON only.\n\n"
                    f"{serialized[:6000]}"
                )}],
                system=(
                    "You are Merlin's output normalizer. Return English-only valid JSON. "
                    "No Chinese, no markdown, no preamble."
                ),
                name=name,
            )
            translated = json.loads(raw.strip())
            if isinstance(translated, dict):
                return translated
        except Exception as exc:
            logger.warning("Merlin English normalization failed: %s", exc)
        result["_language_warning"] = "Non-English text detected; normalization failed."
        return result

    def _handle_investigation_result(self, task: Task, result: dict) -> Optional[Improvement]:
        """Update the improvement record after investigation completes."""
        imp_id = task.payload.get("improvement_id")
        if not imp_id:
            # Create a new improvement record from this investigation
            try:
                imp = upsert_improvement(self.db_path, Improvement(
                    title=result.get("summary", task.description)[:120],
                    description=task.description,
                    origin_agent=AGENT_NAME,
                    origin_signal="investigation",
                    status="proposed" if result.get("forge_recommended") else "investigating",
                    evidence={
                        "verified_facts": result.get("verified_facts", []),
                        "likely_causes": result.get("likely_causes", []),
                        "unknowns": result.get("unknowns", []),
                        "recommended_actions": result.get("recommended_actions", []),
                    },
                    merlin_task_id=task.id,
                    priority=result.get("priority", "normal"),
                    risk_level=result.get("risk_level", "unknown"),
                    affected_components=result.get("affected_components", []),
                    forge_recommended=bool(result.get("forge_recommended", False)),
                ))
                logger.info("Merlin created improvement record #%d from investigation", imp.id)
                return imp
            except Exception as e:
                logger.warning("Failed to create improvement record: %s", e)
                return None
        else:
            try:
                return advance_improvement(
                    self.db_path, int(imp_id),
                    "proposed" if result.get("forge_recommended") else "investigating",
                    evidence_update={
                        "verified_facts": result.get("verified_facts", []),
                        "likely_causes": result.get("likely_causes", []),
                        "unknowns": result.get("unknowns", []),
                        "recommended_actions": result.get("recommended_actions", []),
                    },
                    merlin_task_id=task.id,
                    priority=result.get("priority", "normal"),
                    risk_level=result.get("risk_level", "unknown"),
                    affected_components=result.get("affected_components", []),
                    forge_recommended=bool(result.get("forge_recommended", False)),
                )
            except Exception as e:
                logger.warning("Failed to advance improvement #%s: %s", imp_id, e)
        return None

    async def _route_domain_artifact(self, task: Task, result: dict) -> None:
        """Route creative/domain findings to Forge as reviewable dashboard artifacts."""
        if not self._is_domain_research(task, result):
            return
        if self._has_open_domain_artifact_task(task):
            return

        summary = result.get("summary") or result.get("recommended_next_step") or task.description
        findings = result.get("findings", []) or result.get("verified_facts", []) or []
        next_step = result.get("recommended_next_step") or ""
        finding_preview = "; ".join(str(item)[:160] for item in findings[:5])
        description = (
            "Create a readable Markdown research note from Merlin's findings. "
            "Store it in the managed Forge artifact workspace so it is visible in the dashboard. "
            "Do not modify source code or secrets. "
            "Create exactly one primary file named research_note.md unless a better safe Markdown name is obvious. "
            f"Topic: {task.description[:180]}\n\n"
            f"Merlin summary: {summary[:500]}\n\n"
            f"Key findings: {finding_preview[:900]}\n\n"
            f"Recommended next step: {next_step[:300]}"
        )
        forge_task = enqueue_task(
            self.db_path,
            Task(
                to_agent="forge",
                from_agent=AGENT_NAME,
                task_type="build",
                description=description,
                status="approved",
                priority="normal",
                urgency="this_week",
                domain="creative",
                payload={
                    "source": "merlin_domain_research",
                    "source_merlin_task_id": task.id,
                    "summary": summary,
                    "findings": findings[:12],
                    "recommended_next_step": next_step,
                    "artifact_intent": "dashboard_readable_note",
                    "approval_policy": "markdown_artifact_auto",
                    "suggested_files": ["research_note.md"],
                },
                approval_required=False,
            ),
        )
        emit_event(self.db_path, "domain_artifact_proposed", AGENT_NAME, {
            "merlin_task_id": task.id,
            "forge_task_id": forge_task.id,
            "summary": summary[:200],
        })
        logger.info("Merlin queued auto-approved Forge Markdown artifact task #%s", forge_task.id)

    def _should_escalate_immediately(self, result: dict) -> bool:
        """Bypass the daily digest only for urgent safety/security work."""
        priority = str(result.get("priority", "normal")).lower()
        risk = str(result.get("risk_level", "unknown")).lower()
        text = json.dumps(result, ensure_ascii=False).lower()
        security_related = any(
            keyword in text
            for keyword in ("security", "secret", "token", "credential", "vulnerability", "unsafe", "exposed")
        )
        outage_related = any(
            keyword in text
            for keyword in ("outage", "down", "service unavailable", "data loss", "database file", "unable to open database file")
        )
        return priority == "critical" or (priority == "high" and risk == "high" and security_related and outage_related)

    def _forge_bundle_key(self, result: dict) -> str:
        components = [
            re.sub(r"[^a-z0-9]+", "_", str(item).lower()).strip("_")
            for item in (result.get("affected_components") or [])[:3]
        ]
        cleaned = re.sub(r"[^a-z0-9 ]+", " ", str(result.get("forge_description") or result.get("summary") or ""))
        tokens = [
            token for token in cleaned.lower().split()
            if len(token) > 3 and token not in {"with", "from", "that", "this", "should", "system", "improvement", "urgent"}
        ][:6]
        parts = [part for part in components + tokens if part]
        return ":".join(parts[:8]) or "general_system_improvement"

    def _list_open_forge_bundles(self) -> list[Task]:
        return [
            task for task in list_tasks(self.db_path, to_agent="forge", limit=120)
            if task.task_type == "system_improvement"
            and task.status in {"pending", "approved", "plan_ready", "plan_approved", "in_progress", "awaiting_validation"}
        ]

    def _find_matching_forge_bundle(self, bucket_key: str, affected_components: list[str]) -> Optional[Task]:
        wanted = {str(item).lower() for item in affected_components if item}
        for task in self._list_open_forge_bundles():
            payload = task.payload or {}
            existing_key = str(payload.get("bucket_key") or "")
            if existing_key and existing_key == bucket_key:
                return task
            existing_components = {str(item).lower() for item in (payload.get("affected_components") or []) if item}
            if wanted and existing_components and wanted.intersection(existing_components):
                return task
        return None

    def _daily_forge_bundle_budget_reached(self) -> bool:
        cfg = self.config.get("merlin", {}).get("proactive_research", {})
        max_daily = max(1, int(cfg.get("max_daily_approval_bundles", 2)))
        conn = connect_sqlite(self.db_path, timeout=30, attempts=5, backoff_seconds=1.0)
        try:
            today = datetime.now(timezone.utc).date().isoformat()
            count = conn.execute(
                """SELECT COUNT(*)
                   FROM approval_requests ar
                   JOIN tasks t ON t.id = ar.task_id
                   WHERE ar.request_type='task_approval'
                     AND t.to_agent='forge'
                     AND t.task_type='system_improvement'
                     AND ar.created_at >= ?""",
                (f"{today}T00:00:00+00:00",),
            ).fetchone()[0]
            return count >= max_daily
        finally:
            conn.close()

    def _merge_into_existing_bundle(self, existing_task: Task, task: Task, result: dict, improvement: Improvement) -> None:
        payload = dict(existing_task.payload or {})
        improvement_ids = list(dict.fromkeys([*(payload.get("improvement_ids") or []), improvement.id]))
        merlin_task_ids = list(dict.fromkeys([*(payload.get("merlin_task_ids") or []), task.id]))
        verified = list(dict.fromkeys([*(payload.get("verified_facts") or []), *(result.get("verified_facts") or [])]))[:16]
        unknowns = list(dict.fromkeys([*(payload.get("unknowns") or []), *(result.get("unknowns") or [])]))[:10]
        actions = list(dict.fromkeys([*(payload.get("recommended_actions") or []), *(result.get("recommended_actions") or [])]))[:18]
        components = list(dict.fromkeys([*(payload.get("affected_components") or []), *(result.get("affected_components") or [])]))[:20]
        payload.update({
            "improvement_ids": improvement_ids,
            "merlin_task_ids": merlin_task_ids,
            "verified_facts": verified,
            "unknowns": unknowns,
            "recommended_actions": actions,
            "affected_components": components,
            "last_merged_merlin_task_id": task.id,
            "last_merged_improvement_id": improvement.id,
        })
        conn = connect_sqlite(self.db_path, timeout=30, attempts=5, backoff_seconds=1.0)
        try:
            conn.execute(
                "UPDATE tasks SET payload=?, updated_at=? WHERE id=?",
                (json.dumps(payload), datetime.now(timezone.utc).isoformat(), existing_task.id),
            )
            conn.commit()
        finally:
            conn.close()
        try:
            advance_improvement(
                self.db_path,
                improvement.id,
                "proposed",
                evidence_update={"merged_into_forge_task_id": existing_task.id},
                forge_task_id=existing_task.id,
            )
        except Exception as exc:
            logger.warning("Could not link improvement #%s to merged Forge task #%s: %s", improvement.id, existing_task.id, exc)
        emit_event(self.db_path, "merlin_improvement_merged_into_existing_forge_bundle", AGENT_NAME, {
            "merlin_task_id": task.id,
            "improvement_id": improvement.id,
            "forge_task_id": existing_task.id,
        })

    async def _route_urgent_forge_improvement(self, task: Task, result: dict, improvement: Improvement) -> None:
        """Create an immediate approval-gated Forge task for urgent security/safety findings."""
        from shared.db.approvals import create_approval
        from shared.schemas.approval import ApprovalRequest

        bucket_key = self._forge_bundle_key(result)
        matching = self._find_matching_forge_bundle(bucket_key, result.get("affected_components") or [])
        if matching:
            self._merge_into_existing_bundle(matching, task, result, improvement)
            logger.info(
                "Merlin merged urgent improvement #%s into existing Forge task #%s instead of creating a new approval",
                improvement.id,
                matching.id,
            )
            return

        if self._daily_forge_bundle_budget_reached():
            emit_event(self.db_path, "merlin_improvement_held_for_digest_budget", AGENT_NAME, {
                "merlin_task_id": task.id,
                "improvement_id": improvement.id,
                "bucket_key": bucket_key,
            })
            logger.info(
                "Merlin held urgent improvement #%s for digest because the daily Forge approval budget was already reached",
                improvement.id,
            )
            return

        description = (
            "URGENT Merlin escalation: security/safety system improvement.\n\n"
            f"Source Merlin task #{task.id}; improvement #{improvement.id}.\n\n"
            f"Summary: {(result.get('forge_description') or result.get('summary') or task.description)[:900]}\n\n"
            "Why now: Merlin marked this as high/critical security-related work, so it should not wait for the daily Forge digest.\n"
            "Approval still only lets Forge create a plan; implementation still requires plan approval and Sentinel validation."
        )
        forge_task = enqueue_task(
            self.db_path,
            Task(
                to_agent="forge",
                from_agent=AGENT_NAME,
                task_type="system_improvement",
                description=description,
                status="pending",
                priority="critical" if str(result.get("priority", "")).lower() == "critical" else "high",
                urgency="immediate",
                domain="security",
                payload={
                    "source": "merlin_urgent_escalation",
                    "bucket_key": bucket_key,
                    "improvement_ids": [improvement.id],
                    "merlin_task_ids": [task.id],
                    "improvement_id": improvement.id,
                    "merlin_task_id": task.id,
                    "verified_facts": result.get("verified_facts", [])[:8],
                    "likely_causes": result.get("likely_causes", [])[:6],
                    "unknowns": result.get("unknowns", [])[:5],
                    "affected_components": result.get("affected_components", [])[:10],
                    "recommended_actions": result.get("recommended_actions", [])[:10],
                    "risk_level": result.get("risk_level", "high"),
                },
                approval_required=True,
            ),
        )
        try:
            advance_improvement(
                self.db_path,
                improvement.id,
                "proposed",
                evidence_update={"urgent_forge_task_id": forge_task.id},
                forge_task_id=forge_task.id,
            )
        except Exception as exc:
            logger.warning("Could not link urgent Forge task #%s to improvement #%s: %s", forge_task.id, improvement.id, exc)

        approval_description = (
            f"Urgent Forge approval requested from Merlin finding #{improvement.id}.\n\n"
            f"Task #{forge_task.id}: {(result.get('summary') or description)[:900]}\n\n"
            "Approve to let Forge create a priority plan. Implementation still requires a second approval."
        )
        if self._send_approval:
            await self._send_approval(
                approval_description,
                forge_task.id,
                "task_approval",
                {"improvement_id": improvement.id, "source": "merlin_urgent_escalation"},
            )
        else:
            create_approval(
                self.db_path,
                ApprovalRequest(
                    request_type="task_approval",
                    description=approval_description,
                    task_id=forge_task.id,
                    payload={"improvement_id": improvement.id, "source": "merlin_urgent_escalation"},
                ),
            )
        emit_event(self.db_path, "merlin_urgent_forge_escalation_created", AGENT_NAME, {
            "merlin_task_id": task.id,
            "improvement_id": improvement.id,
            "forge_task_id": forge_task.id,
            "priority": forge_task.priority,
        })
        logger.warning(
            "Merlin escalated urgent improvement #%s to Forge task #%s immediately",
            improvement.id,
            forge_task.id,
        )

    def _is_domain_research(self, task: Task, result: dict) -> bool:
        explicit = result.get("domain_artifact_recommended")
        if explicit is not None:
            return bool(explicit)
        task_domain = str(task.domain or "").lower()
        task_type = str(task.task_type or "").lower()
        payload_text = json.dumps(task.payload or {}, ensure_ascii=False).lower()
        description = str(task.description or "").lower()
        if task_type in self._DIAGNOSTIC_TYPES:
            return False
        blocked_domains = {"operations", "security", "validation", "career", "business", "venture"}
        if task_domain in blocked_domains:
            return False
        blocked_keywords = (
            "sentinel",
            "health check",
            "validation issue",
            "system improvement",
            "database",
            "sqlite",
            "docker",
            "ollama",
            "api",
            "forge",
            "approval",
            "credential",
            "token",
            "security",
            "vulnerability",
        )
        if any(keyword in description or keyword in payload_text for keyword in blocked_keywords):
            return False
        domain_hint = task_domain in {"creative", "personal", "hobby"}
        payload_hint = any(
            marker in payload_text
            for marker in ("creative", "hobby", "personal_project", "artifact_intent")
        )
        return domain_hint or payload_hint

    def _has_open_domain_artifact_task(self, task: Task) -> bool:
        for existing in list_tasks(self.db_path, to_agent="forge", limit=50):
            if existing.status not in {"pending", "approved", "in_progress", "plan_ready", "plan_approved"}:
                continue
            if existing.payload.get("source") == "merlin_domain_research" and existing.payload.get("source_merlin_task_id") == task.id:
                return True
        return False

    def _format_investigation(self, task: Task, result: dict) -> str:
        summary = result.get("summary", "")
        facts = result.get("verified_facts", [])
        causes = result.get("likely_causes", [])
        unknowns = result.get("unknowns", [])
        actions = result.get("recommended_actions", [])
        forge = result.get("forge_recommended", False)
        priority = result.get("priority", "normal")
        conf = result.get("confidence", 0)

        lines = [
            f"🔬 <b>Merlin — Investigation Complete (Task #{task.id})</b>\n",
            f"<b>Focus:</b> {task.description[:100]}\n",
            f"<b>Summary:</b> {summary}\n",
        ]
        if facts:
            lines.append("<b>✓ Verified facts:</b>")
            for f in facts[:4]:
                lines.append(f"  • {str(f)[:120]}")
            lines.append("")
        if causes:
            lines.append("<b>⚠ Likely causes:</b>")
            for c in causes[:3]:
                lines.append(f"  • {str(c)[:120]}")
            lines.append("")
        if unknowns:
            lines.append("<b>? Unknowns:</b>")
            for u in unknowns[:2]:
                lines.append(f"  • {str(u)[:100]}")
            lines.append("")
        if actions:
            lines.append("<b>→ Recommended actions:</b>")
            for a in actions[:3]:
                lines.append(f"  • {str(a)[:120]}")
            lines.append("")
        if forge:
            scope = result.get("forge_scope", "")
            lines.append(f"<b>🔨 Forge recommended</b> [{scope}]")
        lines.append(f"\n<i>Priority: {priority} · Confidence: {int(conf * 100)}%</i>")
        return "\n".join(lines)

    def _emit_atlas_tasks(self, result: dict) -> None:
        """If research results include DevOps learning opportunities, queue skill_lesson tasks for Atlas."""
        learning_opportunities = result.get("learning_opportunities", [])
        if not learning_opportunities:
            return
        for topic in learning_opportunities[:3]:  # cap at 3 per research result
            if not isinstance(topic, str) or not topic.strip():
                continue
            try:
                enqueue_task(
                    self.db_path,
                    Task(
                        to_agent="atlas",
                        from_agent=AGENT_NAME,
                        task_type="skill_lesson",
                        description=f"Lesson on: {topic.strip()}",
                        payload={"topic": topic.strip()},
                    ),
                )
                logger.info("Merlin queued Atlas skill_lesson: %s", topic)
            except Exception as ex:
                logger.warning("Failed to queue Atlas task for '%s': %s", topic, ex)

    def _format_result(self, task: Task, result: dict) -> str:
        summary = result.get("summary", "No summary.")
        confidence = result.get("confidence", 0)
        next_step = result.get("recommended_next_step", "")
        findings = result.get("findings", [])
        options = result.get("options", [])

        lines = [
            f"🔍 <b>Merlin — Research Complete (Task #{task.id})</b>\n",
            f"<b>Question:</b> {task.description[:120]}\n",
            f"<b>Summary:</b> {summary}\n",
        ]

        if findings:
            lines.append("<b>Key findings:</b>")
            for f in findings[:5]:
                lines.append(f"• {str(f)[:120]}")
            lines.append("")

        if options:
            lines.append("<b>Options:</b>")
            for o in options[:3]:
                if isinstance(o, dict):
                    lines.append(f"• <b>{o.get('option', '')}:</b> {o.get('pros', '')} / {o.get('cons', '')}")
                else:
                    lines.append(f"• {o}")
            lines.append("")

        if next_step:
            lines.append(f"<b>Recommended next step:</b> {next_step}")

        lines.append(f"\n<i>Confidence: {int(confidence * 100)}%</i>")
        return "\n".join(lines)
