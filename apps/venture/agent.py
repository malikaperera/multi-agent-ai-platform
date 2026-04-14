"""
Venture — business opportunity research agent.

Polls task queue for opportunity_research tasks.
Calls LLM with owner context + opportunity prompt.
Returns structured opportunity report.
Evaluates capital against guardrails.
Appends opportunity to memory/opportunity_log.md.

Capital guardrails:
  <$100   : log freely, no approval
  $100-$500: flag with note
  $500-$3k : trigger approval_request
  >$3k    : strong approval checkpoint
"""
import asyncio
import contextlib
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Coroutine, Optional

from shared.db.agents import emit_heartbeat, record_agent_error, record_agent_success, update_agent_status
from shared.db.context import format_approval_decisions, get_recent_approval_decisions
from shared.db.events import emit_event
from shared.db.messages import send_agent_message
from shared.db.tasks import enqueue_task, get_next_task, list_tasks, requeue_in_progress_tasks, touch_task, update_task_status
from shared.agent_learning import try_reflect_after_task
from shared.llm.anthropic_provider import AnthropicProvider
from shared.memory.founder import OwnerMemory
from shared.schemas.task import Task

logger = logging.getLogger(__name__)

AGENT_NAME = "venture"


def _parse_aud_amount(value) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return 0.0
    cleaned = text.replace(",", "")
    match = re.search(r"-?\d+(?:\.\d+)?", cleaned)
    if not match:
        return 0.0
    try:
        return float(match.group(0))
    except ValueError:
        return 0.0


class VentureAgent:
    def __init__(
        self,
        llm: AnthropicProvider,
        db_path: str,
        data_dir: str,
        config: dict,
        owner_memory: OwnerMemory,
        routine_llm: Optional[AnthropicProvider] = None,
        deep_model: Optional[str] = None,
        routine_model: Optional[str] = None,
    ):
        self.llm = llm
        self.deep_llm = llm
        self.routine_llm = routine_llm or llm
        self.deep_model = deep_model or getattr(llm, "model", "unknown")
        self.routine_model = routine_model or getattr(self.routine_llm, "model", self.deep_model)
        self.db_path = db_path
        self.reports_dir = Path(data_dir) / "reports"
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self.owner_memory = owner_memory

        vcfg = config.get("venture", {})
        self.prompt_template = vcfg.get("research_prompt_template", "Research opportunities for: {question}\nOwner context:\n{owner_context}")
        guardrails = vcfg.get("capital_guardrails", {})
        self.threshold_free       = float(guardrails.get("free_threshold", 100))
        self.threshold_light      = float(guardrails.get("light_review_threshold", 500))
        self.threshold_approval   = float(guardrails.get("approval_threshold", 3000))
        self.currency             = str(guardrails.get("currency", ""))
        self.poll_interval = int(config.get("scheduler", {}).get("worker_poll_interval_seconds", 10))
        self.keepalive_seconds = max(10, int(config.get("observability", {}).get("task_keepalive_seconds", 20)))
        self.proactive_domains = vcfg.get("proactive_research_domains", [])
        self.continuous_mode = bool(vcfg.get("continuous_mode", False))

        self._notify: Optional[Callable[[str], Coroutine]] = None
        self._send_approval: Optional[Callable[[str, int], Coroutine]] = None

    def set_notify(self, fn: Callable[[str], Coroutine]) -> None:
        self._notify = fn

    def set_approval_sender(self, fn: Callable[[str, int, str, dict], Coroutine]) -> None:
        """fn(description, task_id, request_type, payload={})"""
        self._send_approval = fn

    async def run(self) -> None:
        logger.info("Venture worker started")
        recovered = requeue_in_progress_tasks(self.db_path, AGENT_NAME)
        if recovered:
            logger.info("Venture recovered %d abandoned in-progress task(s)", recovered)
        update_agent_status(self.db_path, AGENT_NAME, "idle")
        emit_heartbeat(self.db_path, AGENT_NAME, current_task_id=None, current_model=f"{self.routine_model}|deep:{self.deep_model}")

        while True:
            try:
                task = get_next_task(self.db_path, AGENT_NAME, "pending")
                if task:
                    await self._process(task)
                else:
                    await asyncio.sleep(self.poll_interval)
            except Exception as e:
                logger.error("Venture worker error: %s", e, exc_info=True)
                await asyncio.sleep(self.poll_interval)

    async def _process(self, task: Task) -> None:
        logger.info("Venture processing task #%d: %s", task.id, task.description[:80])
        update_agent_status(self.db_path, AGENT_NAME, "busy", f"Researching: {task.description[:60]}")
        active_llm = self._llm_for_task(task)
        emit_heartbeat(self.db_path, AGENT_NAME, current_task_id=task.id, current_model=active_llm.model)
        update_task_status(self.db_path, task.id, "in_progress")

        try:
            result = await self._run_with_keepalive(
                task,
                f"Researching: {task.description[:60]}",
                lambda: self._research(task),
                active_llm.model,
            )
            quality_issues = self._quality_issues(result)
            if quality_issues:
                self._reject_low_quality_result(task, result, quality_issues)
                return

            report_path = self.reports_dir / f"venture_{task.id}.json"
            report_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
            update_task_status(self.db_path, task.id, "completed", result)
            record_agent_success(self.db_path, AGENT_NAME, f"Opportunity researched: {result.get('opportunity_summary', '')[:60]}")
            emit_event(self.db_path, "opportunity_discovered", AGENT_NAME, {"task_id": task.id, "summary": result.get("opportunity_summary", "")})

            try:
                # Log to memory/opportunity_log.md
                self.owner_memory.append_opportunity(self._format_log_entry(result))

                # Capital guardrail check
                capital = _parse_aud_amount(result.get("capital_required", 0))
                await self._apply_guardrails(task, result, capital)
                await self._reflect_after_task_best_effort(active_llm, task, result)
                self._enqueue_continuous_research(task, result)
            except Exception as side_effect_error:
                logger.warning(
                    "Venture post-completion side effect failed for task #%d: %s",
                    task.id,
                    side_effect_error,
                    exc_info=True,
                )
                emit_event(
                    self.db_path,
                    "venture_post_completion_warning",
                    AGENT_NAME,
                    {"task_id": task.id, "error": str(side_effect_error)[:300]},
                )

        except Exception as e:
            logger.error("Venture task #%d failed: %s", task.id, e, exc_info=True)
            update_task_status(self.db_path, task.id, "failed", {"error": str(e)})
            record_agent_error(self.db_path, AGENT_NAME, str(e))
            if self._notify:
                await self._notify(f"⚠️ <b>Venture — Task #{task.id} failed</b>\n\nError: {e}")

        finally:
            update_agent_status(self.db_path, AGENT_NAME, "idle", f"Task #{task.id} finished")
            emit_heartbeat(self.db_path, AGENT_NAME, current_task_id=None, current_model=f"{self.routine_model}|deep:{self.deep_model}")

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

    async def _reflect_after_task_best_effort(self, llm, task: Task, result: dict) -> None:
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
                        owner_context=self.owner_memory.get_context(),
                    ),
                ),
                timeout=90,
            )
        except asyncio.TimeoutError:
            logger.warning("Venture reflection timed out for task #%s; continuing worker loop", task.id)
        except Exception as exc:
            logger.warning("Venture reflection failed for task #%s: %s", task.id, exc)

    def _llm_for_task(self, task: Task) -> AnthropicProvider:
        if task.priority in {"high", "critical"} or task.urgency == "immediate" or task.payload.get("deep_analysis"):
            return self.deep_llm
        return self.routine_llm

    def _enqueue_continuous_research(self, task: Task, result: dict) -> None:
        """Keep Venture looking for the next opportunity without queue flooding."""
        if not self.continuous_mode or task.task_type != "opportunity_research":
            return

        open_tasks = [
            t for t in list_tasks(self.db_path, to_agent=AGENT_NAME, limit=20)
            if t.status in {"pending", "in_progress", "approved"}
        ]
        if open_tasks:
            return

        if self.proactive_domains:
            domain = self.proactive_domains[(task.id or 0) % len(self.proactive_domains)]
        else:
            domain = (
                "Look for the next realistic opportunity based on recent founder "
                "context and agent learning."
            )
        summary = result.get("opportunity_summary") or result.get("summary") or ""
        enqueue_task(
            self.db_path,
            Task(
                to_agent=AGENT_NAME,
                from_agent=AGENT_NAME,
                task_type="opportunity_research",
                description=f"{domain}\n\nBuild on recent finding: {summary[:240]}",
                priority="normal",
                urgency="this_week",
                domain="business",
                approval_required=False,
            ),
        )
        logger.info("Venture queued continuous opportunity_research follow-up")

    def _build_venture_system(self) -> str:
        parts = [
            "You are Venture, a strategic execution-intelligence agent for the system owner. "
            "You are not a generic business-ideas scanner and you do not re-litigate strategy from scratch. "
            "Your job is to sharpen execution inside the ventures that are already chosen and loaded from context.",
            "Research keyword gaps, lead buyers, customer pain, pricing, route-to-cash details, competitive weaknesses, "
            "and demand signals for the active ventures. Stay focused on execution, not ideation.",
            "Always respond with valid JSON only. No markdown, no preamble. "
            "All reports must be concrete, execution-oriented, and tied to the active ventures."
        ]
        if self.owner_memory:
            ctx = self.owner_memory.get_context()
            if ctx:
                parts.append(f"\n### Owner Context\n{ctx}")
            capital = self.owner_memory.get_capital_state()
            if capital:
                parts.append(f"\n### Capital State & Guardrails\n{capital}")
            parts.append(
                "\n### Domain Boundary\n"
                "Keep each domain of the owner's life separate unless Roderick explicitly asks to combine them."
            )
        prior = get_recent_approval_decisions(self.db_path, limit=8)
        if prior:
            parts.append(f"\n### Recent Approval Decisions (calibrate risk assessment)\n{format_approval_decisions(prior)}")
        return "\n".join(parts)

    def _research(self, task: Task) -> dict:
        llm = self._llm_for_task(task)
        owner_context = self.owner_memory.get_context()
        prompt = self.prompt_template.format(
            question=task.description,
            owner_context=owner_context,
        )
        result = self._call_research(llm, prompt, "venture_research")
        result = self._normalize_result(result)
        for attempt in range(2):
            issues = self._quality_issues(result)
            if not issues:
                break
            result = self._normalize_result(
                self._call_research(
                    llm,
                    self._build_repair_prompt(task, result, issues),
                    f"venture_research_repair_{attempt + 1}",
                )
            )
        result["_task_id"] = task.id
        result["_researched_at"] = datetime.now(timezone.utc).isoformat()
        return result

    def _call_research(self, llm: AnthropicProvider, prompt: str, name: str) -> dict:
        raw = llm.complete(
            messages=[{"role": "user", "content": prompt}],
            system=self._build_venture_system(),
            name=name,
        )
        try:
            return json.loads(raw.strip())
        except json.JSONDecodeError:
            return {
                "opportunity_summary": "",
                "category": "other",
                "market_problem": raw[:300],
                "proposed_solution": "",
                "revenue_model": "",
                "capital_required": 0,
                "time_to_first_revenue": "",
                "revenue_potential": "",
                "competition": "",
                "automation_potential": "",
                "difficulty": "",
                "risk_level": "",
                "relevance_score": 0,
                "specific_action": "",
                "estimated_revenue_impact": "",
                "time_to_first_dollar": "",
                "confidence_level": "",
                "next_steps": [],
                "requires_capital_approval": False,
            }

    def _normalize_result(self, result: dict) -> dict:
        result = dict(result or {})
        aliases = {
            "opportunity_summary": ["opportunity", "opportunity_name", "name", "title"],
            "market_problem": ["problem", "problem_solved"],
            "proposed_solution": ["solution"],
            "revenue_model": ["recurring_revenue_model", "monetization"],
            "time_to_first_revenue": ["time_to_revenue"],
            "revenue_potential": ["recurring_revenue_potential", "revenue"],
            "competition": ["competition_analysis"],
            "automation_potential": ["automation", "automation_level", "automation_alignment"],
            "specific_action": ["recommended_action", "action_for_roderick", "action_for_forge", "specific_next_action"],
            "estimated_revenue_impact": ["revenue_impact", "impact", "estimated_impact"],
            "time_to_first_dollar": ["time_to_cash", "time_to_first_cash", "time_to_value"],
            "confidence_level": ["confidence", "confidence_score", "confidence_assessment"],
        }
        for canonical, keys in aliases.items():
            if self._is_blank(result.get(canonical)):
                for key in keys:
                    if not self._is_blank(result.get(key)):
                        result[canonical] = result[key]
                        break
        if "difficulty" not in result or self._is_blank(result.get("difficulty")):
            result["difficulty"] = "medium"
        if "risk_level" not in result or self._is_blank(result.get("risk_level")):
            result["risk_level"] = "medium"
        if "requires_capital_approval" not in result:
            capital = _parse_aud_amount(result.get("capital_required", 0))
            result["requires_capital_approval"] = capital > self.threshold_light
        try:
            result["relevance_score"] = int(float(result.get("relevance_score", 0) or 0))
        except (TypeError, ValueError):
            result["relevance_score"] = 0
        return result

    def _quality_issues(self, result: dict) -> list[str]:
        required = [
            "opportunity_summary",
            "category",
            "market_problem",
            "proposed_solution",
            "revenue_model",
            "capital_required",
            "time_to_first_revenue",
            "revenue_potential",
            "competition",
            "automation_potential",
            "difficulty",
            "risk_level",
            "relevance_score",
            "specific_action",
            "estimated_revenue_impact",
            "time_to_first_dollar",
            "confidence_level",
            "next_steps",
        ]
        issues: list[str] = []
        for key in required:
            value = result.get(key)
            if key == "capital_required":
                if value is None:
                    issues.append(f"{key} missing")
                continue
            if key == "relevance_score":
                try:
                    score = int(float(value))
                except (TypeError, ValueError):
                    issues.append("relevance_score must be a number from 1 to 10")
                    continue
                if score < 1 or score > 10:
                    issues.append("relevance_score must be between 1 and 10")
                continue
            if key == "next_steps":
                if not isinstance(value, list) or len([s for s in value if str(s).strip()]) < 3:
                    issues.append("next_steps needs at least 3 concrete steps")
                continue
            if self._is_blank(value):
                issues.append(f"{key} missing or vague")
        if self._unknown_count(result) > 0:
            issues.append("contains unknown/tbd/n/a placeholders")
        if len(str(result.get("market_problem", ""))) < 60:
            issues.append("market_problem lacks detail")
        if len(str(result.get("proposed_solution", ""))) < 60:
            issues.append("proposed_solution lacks detail")
        return issues

    def _is_blank(self, value) -> bool:
        if value is None:
            return True
        if isinstance(value, (int, float, bool)):
            return False
        text = str(value).strip().lower()
        return text in {"", "unknown", "unkown", "n/a", "na", "none", "tbd", "to be determined"}

    def _unknown_count(self, result: dict) -> int:
        raw = json.dumps(result, ensure_ascii=False).lower()
        return sum(raw.count(token) for token in ("unknown", "unkown", "n/a", " tbd", "to be determined"))

    def _build_repair_prompt(self, task: Task, result: dict, issues: list[str]) -> str:
        return (
            "Your previous Venture opportunity report is not adequate to send to the user.\n"
            "Do not use unknown, n/a, tbd, vague placeholders, or unsupported empty fields.\n"
            "If you cannot support an exact number, provide a bounded estimate and state the concrete basis.\n"
            "Return the same required JSON schema, fully populated.\n\n"
            f"Original research question/domain:\n{task.description}\n\n"
            f"Quality issues to fix:\n{json.dumps(issues, indent=2)}\n\n"
            f"Previous report:\n{json.dumps(result, indent=2, ensure_ascii=False)[:5000]}"
        )

    def _reject_low_quality_result(self, task: Task, result: dict, issues: list[str]) -> None:
        payload = {
            "error": "Venture quality gate rejected the report before user delivery.",
            "quality_issues": issues,
            "draft": result,
        }
        update_task_status(self.db_path, task.id, "failed", payload)
        record_agent_error(self.db_path, AGENT_NAME, "Quality gate rejected vague Venture report")
        emit_event(self.db_path, "venture_report_rejected", AGENT_NAME, {
            "task_id": task.id,
            "issues": issues,
        })
        send_agent_message(
            self.db_path,
            from_agent="roderick",
            to_agent=AGENT_NAME,
            message=(
                f"Task #{task.id} was not adequate to send to the user. "
                f"Fix these issues before surfacing Venture output: {', '.join(issues[:8])}"
            ),
            priority="high",
        )
        enqueue_task(
            self.db_path,
            Task(
                to_agent=AGENT_NAME,
                from_agent="roderick",
                task_type="opportunity_research",
                description=(
                    f"{task.description}\n\n"
                    "Previous Venture draft was rejected by Roderick's quality gate. "
                    "Research deeper and return a fully populated, concrete report with no unknown placeholders."
                ),
                priority="high",
                urgency=task.urgency,
                domain=task.domain,
                payload={"retry_of_task_id": task.id, "quality_issues": issues},
                approval_required=False,
            ),
        )

    async def _apply_guardrails(self, task: Task, result: dict, capital: float) -> None:
        summary = result.get("opportunity_summary", "Opportunity")
        risk = result.get("risk_level", "unknown")

        if capital > self.threshold_approval:
            # Strong approval checkpoint
            description = (
                f"⚠️ HIGH CAPITAL OPPORTUNITY: {summary}\n\n"
                f"Capital required: ${capital:,.0f}{' ' + self.currency if self.currency else ''} | Risk: {risk}\n\n"
                f"This exceeds the ${self.threshold_approval:,.0f} approval threshold. "
                f"Explicit approval required before pursuing."
            )
            if self._send_approval:
                await self._send_approval(description, task.id, "capital_approval")
            elif self._notify:
                await self._notify(f"🚨 <b>Capital Approval Required</b>\n\n{description}")

        elif capital > self.threshold_light:
            # Standard approval ($500–$3k)
            description = (
                f"💰 Opportunity: {summary}\n\n"
                f"Capital required: ${capital:,.0f}{' ' + self.currency if self.currency else ''} | Risk: {risk}\n\n"
                f"Approval required (exceeds ${self.threshold_light:,.0f} threshold)."
            )
            if self._send_approval:
                await self._send_approval(description, task.id, "capital_approval")
            elif self._notify:
                await self._notify(self._format_notification(task, result))

        else:
            # Below threshold — just notify
            if self._notify:
                flag = f" <i>(light review: ${capital:,.0f})</i>" if capital > self.threshold_free else ""
                await self._notify(self._format_notification(task, result) + flag)

    def _format_notification(self, task: Task, result: dict) -> str:
        summary = result.get("opportunity_summary", "Unknown")
        category = result.get("category", "other")
        capital = _parse_aud_amount(result.get("capital_required", 0))
        time_to_rev = result.get("time_to_first_revenue", "unknown")
        revenue = result.get("revenue_potential", "unknown")
        risk = result.get("risk_level", "unknown")
        next_steps = result.get("next_steps", [])
        problem = result.get("market_problem", "")

        lines = [
            f"💡 <b>Venture — Opportunity Found (Task #{task.id})</b>\n",
            f"<b>{summary}</b> [{category}]\n",
            f"<b>Problem:</b> {problem[:120]}\n",
            f"<b>Capital:</b> ${capital:,.0f}{' ' + self.currency if self.currency else ''}",
            f"<b>Time to revenue:</b> {time_to_rev}",
            f"<b>Revenue potential:</b> {revenue}",
            f"<b>Risk:</b> {risk}\n",
        ]
        if next_steps:
            lines.append("<b>Next steps:</b>")
            for s in next_steps[:3]:
                lines.append(f"  • {s}")
        return "\n".join(lines)

    def _format_log_entry(self, result: dict) -> str:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        summary = str(result.get("opportunity_summary", "Unknown"))
        category = str(result.get("category", "other"))
        capital = _parse_aud_amount(result.get("capital_required", 0))
        time_to_rev = str(result.get("time_to_first_revenue", "unknown"))
        revenue = str(result.get("revenue_potential", "unknown"))
        risk = str(result.get("risk_level", "unknown"))
        problem = str(result.get("market_problem", ""))
        solution = str(result.get("proposed_solution", ""))
        next_steps = result.get("next_steps", [])
        if not isinstance(next_steps, list):
            next_steps = [str(next_steps)]
        next_steps_str = "\n".join(f"- {str(s)}" for s in next_steps[:3])

        return (
            f"\n## [{date}] Opportunity: {summary}\n"
            f"**Category:** {category}\n"
            f"**Capital required:** ${capital:,.0f}{' ' + self.currency if self.currency else ''}\n"
            f"**Time to first revenue:** {time_to_rev}\n"
            f"**Revenue potential:** {revenue}\n"
            f"**Risk level:** {risk}\n"
            f"**Market problem:** {problem[:200]}\n"
            f"**Proposed solution:** {solution[:200]}\n"
            f"**Next steps:**\n{next_steps_str}\n"
            f"**Status:** surfaced\n"
        )
