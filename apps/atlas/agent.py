"""
Atlas — personalised skill tutor and learning coach agent.

Delivers structured lessons on any configured subject area, tracks skill
progression, and uses the owner's real system as the learning environment.
Curriculum and focus topics are configured via config/roderick.json.

Primary interface: dashboard.
Secondary interface: dedicated Telegram chat (ATLAS_TELEGRAM_CHAT_ID).

Presence-aware:
  At PC  → lessons delivered via dashboard; Telegram reminder only
  Away   → full lesson content via Telegram

Skill states (progression):
  unknown → introduced → learning → practiced → project_used → interview_ready
"""
import asyncio
import json
import logging
import os
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable, Coroutine, Optional
from urllib.parse import quote_plus

from shared.db.agents import emit_heartbeat, record_agent_error, record_agent_success, update_agent_status
from shared.db.events import emit_event
from shared.db.tasks import get_next_task, update_task_status
from shared.agent_learning import try_reflect_after_task
from shared.llm.anthropic_provider import AnthropicProvider
from shared.memory.founder import OwnerMemory
from shared.schemas.task import Task

logger = logging.getLogger(__name__)

AGENT_NAME = "atlas"

SKILL_STATES = ["unknown", "introduced", "learning", "practiced", "project_used", "interview_ready"]

_STATE_EMOJI = {
    "unknown": "⬜",
    "introduced": "🟦",
    "learning": "🟨",
    "practiced": "🟧",
    "project_used": "🟩",
    "interview_ready": "⭐",
}


class AtlasAgent:
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
        self.atlas_dir = Path(data_dir) / "atlas"
        self.atlas_dir.mkdir(parents=True, exist_ok=True)
        self.lessons_dir = self.atlas_dir / "lessons"
        self.lessons_dir.mkdir(exist_ok=True)
        self.owner_memory = owner_memory

        acfg = config.get("atlas", {})
        self.prompt_template = acfg.get("lesson_prompt_template", "Create a practical lesson on: {topic}\n{owner_context}")
        self.curriculum_focus = acfg.get("curriculum_focus", ["Docker", "CI/CD", "Observability"])
        self.poll_interval = int(config.get("scheduler", {}).get("worker_poll_interval_seconds", 10))

        # Atlas Telegram chat — defaults to main chat if not set
        self.atlas_chat_id: Optional[int] = None  # Set from env in main.py

        self._notify: Optional[Callable[[str], Coroutine]] = None
        self._presence_fn: Optional[Callable[[], str]] = None

    def set_notify(self, fn: Callable[[str], Coroutine]) -> None:
        """Send a message to Atlas's dedicated Telegram chat."""
        self._notify = fn

    def set_presence(self, fn: Callable[[], str]) -> None:
        """Inject presence mode getter."""
        self._presence_fn = fn

    def _presence(self) -> str:
        return self._presence_fn() if self._presence_fn else "away"

    # ── Skill management ──────────────────────────────────────────────────────

    @property
    def skills_path(self) -> Path:
        return self.atlas_dir / "skills.json"

    def load_skills(self) -> dict:
        if self.skills_path.exists():
            return json.loads(self.skills_path.read_text(encoding="utf-8"))
        # Bootstrap from curriculum focus
        return {topic: "unknown" for topic in self.curriculum_focus}

    def save_skills(self, skills: dict) -> None:
        self.skills_path.write_text(json.dumps(skills, indent=2), encoding="utf-8")

    def advance_skill(self, skill: str, to_state: str) -> None:
        skills = self.load_skills()
        current = skills.get(skill, "unknown")
        if to_state in SKILL_STATES:
            skills[skill] = to_state
            self.save_skills(skills)
            logger.info("Skill %s: %s → %s", skill, current, to_state)

    def get_skill_summary(self) -> str:
        skills = self.load_skills()
        if not skills:
            return "No skills tracked yet."
        lines = ["<b>📚 Atlas — Skills</b>\n"]
        for skill, state in sorted(skills.items()):
            emoji = _STATE_EMOJI.get(state, "⬜")
            lines.append(f"{emoji} {skill} <i>[{state}]</i>")
        return "\n".join(lines)

    # ── Lesson management ─────────────────────────────────────────────────────

    def today_lesson_path(self) -> Path:
        return self.lessons_dir / f"{date.today().isoformat()}.json"

    def get_today_lesson(self) -> Optional[dict]:
        p = self.today_lesson_path()
        if p.exists():
            return json.loads(p.read_text(encoding="utf-8"))
        return None

    def save_lesson(self, lesson: dict) -> None:
        self.today_lesson_path().write_text(
            json.dumps(lesson, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    # ── Worker loop ───────────────────────────────────────────────────────────

    @property
    def lesson_status_path(self) -> Path:
        return self.atlas_dir / "lesson_status.json"

    @property
    def learning_log_path(self) -> Path:
        return self.atlas_dir / "learning_log.json"

    def load_learning_log(self) -> list[dict]:
        if not self.learning_log_path.exists():
            return []
        try:
            data = json.loads(self.learning_log_path.read_text(encoding="utf-8"))
            return data if isinstance(data, list) else []
        except Exception:
            return []

    def save_learning_log(self, entries: list[dict]) -> None:
        self.learning_log_path.write_text(
            json.dumps(entries, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    def record_learning_entry(self, entry: dict) -> dict:
        entries = self.load_learning_log()
        entry.setdefault("id", f"atlas_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}")
        entry.setdefault("created_at", datetime.now(timezone.utc).isoformat())
        entries.insert(0, entry)
        self.save_learning_log(entries[:200])
        emit_event(self.db_path, "atlas_learning_recorded", AGENT_NAME, {
            "topic": entry.get("topic"),
            "type": entry.get("type"),
        })
        return entry

    def set_lesson_status(self, status: str, note: str = "") -> dict:
        data = {
            "date": date.today().isoformat(),
            "status": status,
            "note": note,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self.lesson_status_path.write_text(
            json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        return data

    def get_lesson_status(self) -> dict:
        if not self.lesson_status_path.exists():
            return {}
        return json.loads(self.lesson_status_path.read_text(encoding="utf-8"))

    async def ensure_today_lesson(self) -> Optional[dict]:
        lesson = self.get_today_lesson()
        if lesson:
            return lesson

        skills = self.load_skills()
        priority_order = ["unknown", "introduced", "learning", "practiced", "project_used"]
        candidates = [
            (SKILL_STATES.index(state), topic)
            for topic, state in skills.items()
            if state in priority_order
        ]
        if not candidates:
            return None

        _, topic = min(candidates)
        logger.info("Atlas generating lesson on demand: %s", topic)
        lesson = await asyncio.get_event_loop().run_in_executor(None, self._generate_lesson, topic)
        self.save_lesson(lesson)
        emit_event(self.db_path, "lesson_created", AGENT_NAME, {"topic": topic, "source": "atlas_chat"})
        return lesson

    def format_lesson_telegram(self, lesson: dict) -> str:
        return self._format_lesson_telegram(lesson)

    async def chat(self, user_message: str) -> str:
        lowered = user_message.lower()
        action_taken = "none"

        if any(word in lowered for word in ("postpone", "skip", "not today", "tomorrow", "later")):
            self.set_lesson_status("postponed", user_message)
            action_taken = "marked today's lesson as postponed"
        elif any(word in lowered for word in ("study", "learn", "ready", "start lesson", "resume")):
            self.set_lesson_status("studying", user_message)
            await self.ensure_today_lesson()
            action_taken = "marked today's lesson as studying"
        elif any(word in lowered for word in ("done", "finished", "completed")):
            self.set_lesson_status("completed", user_message)
            self._record_today_completion(user_message)
            action_taken = "marked today's lesson as completed"

        response = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: self.llm.complete(
                messages=[{"role": "user", "content": self._build_chat_prompt(user_message, action_taken)}],
                system=self._build_chat_system(),
                name="atlas_chat",
            ),
        )
        return response.strip() or "I'm here. Tell me what you want to work on next."

    async def run(self) -> None:
        logger.info("Atlas worker started")
        update_agent_status(self.db_path, AGENT_NAME, "idle")
        emit_heartbeat(self.db_path, AGENT_NAME, current_task_id=None, current_model=self.llm.model)
        self._ensure_skill_bootstrap()

        while True:
            try:
                task = get_next_task(self.db_path, AGENT_NAME, "pending")
                if task:
                    await self._process(task)
                else:
                    await asyncio.sleep(self.poll_interval)
            except Exception as e:
                logger.error("Atlas worker error: %s", e, exc_info=True)
                await asyncio.sleep(self.poll_interval)

    async def _process(self, task: Task) -> None:
        logger.info("Atlas processing task #%d: %s", task.id, task.task_type)
        update_agent_status(self.db_path, AGENT_NAME, "busy", task.description[:60])
        update_task_status(self.db_path, task.id, "in_progress")

        try:
            if task.task_type == "skill_lesson":
                await self._create_lesson_task(task)
            elif task.task_type == "skill_assessment":
                await self._assess_skill(task)
            elif task.task_type == "curriculum_update":
                await self._curriculum_update(task)
            else:
                update_task_status(self.db_path, task.id, "completed")

            record_agent_success(self.db_path, AGENT_NAME, f"Processed {task.task_type}")

        except Exception as e:
            logger.error("Atlas task #%d failed: %s", task.id, e, exc_info=True)
            update_task_status(self.db_path, task.id, "failed", {"error": str(e)})
            record_agent_error(self.db_path, AGENT_NAME, str(e))

    async def _create_lesson_task(self, task: Task) -> None:
        topic = task.payload.get("topic") or task.description
        lesson = await asyncio.get_event_loop().run_in_executor(None, self._generate_lesson, topic)
        self.save_lesson(lesson)
        update_task_status(self.db_path, task.id, "completed", lesson)
        emit_event(self.db_path, "lesson_created", AGENT_NAME, {"topic": topic, "task_id": task.id})
        if self._notify:
            mode = self._presence()
            if mode in ("away", "focus"):
                await self._notify(self._format_lesson_telegram(lesson))
            else:
                # At PC — brief reminder only; full lesson on dashboard
                await self._notify(
                    f"📚 <b>Atlas lesson ready:</b> {lesson.get('topic', topic)}\n"
                    f"<i>Open the dashboard to view it.</i>"
                )

        try_reflect_after_task(
            llm=self.llm,
            db_path=self.db_path,
            data_dir=str(self.atlas_dir.parent),
            agent_name=AGENT_NAME,
            task=task,
            result=lesson,
            owner_context=self.owner_memory.get_context(),
        )

    async def _assess_skill(self, task: Task) -> None:
        skill = task.payload.get("skill") or task.description
        current_state = self.load_skills().get(skill, "unknown")
        # Advance skill state if coming from a known project usage
        if task.payload.get("advance_from_project"):
            self.advance_skill(skill, "project_used")
        result = {"skill": skill, "state": current_state}
        update_task_status(self.db_path, task.id, "completed", result)
        try_reflect_after_task(
            llm=self.llm,
            db_path=self.db_path,
            data_dir=str(self.atlas_dir.parent),
            agent_name=AGENT_NAME,
            task=task,
            result=result,
            owner_context=self.owner_memory.get_context(),
        )

    async def _curriculum_update(self, task: Task) -> None:
        update = await asyncio.get_event_loop().run_in_executor(
            None, self._generate_curriculum_update, task
        )
        for skill in update.get("skills_to_add", [])[:10]:
            if isinstance(skill, str) and skill.strip():
                skills = self.load_skills()
                skills.setdefault(skill.strip(), "unknown")
                self.save_skills(skills)

        report_path = self.atlas_dir / f"curriculum_update_{date.today().isoformat()}.json"
        report_path.write_text(json.dumps(update, indent=2, ensure_ascii=False), encoding="utf-8")
        update_task_status(self.db_path, task.id, "completed", update)
        emit_event(self.db_path, "curriculum_updated", AGENT_NAME, {"task_id": task.id})

        if self._notify:
            await self._notify(self._format_curriculum_update(update))
        try_reflect_after_task(
            llm=self.llm,
            db_path=self.db_path,
            data_dir=str(self.atlas_dir.parent),
            agent_name=AGENT_NAME,
            task=task,
            result=update,
            owner_context=self.owner_memory.get_context(),
        )

    def _build_lesson_system(self) -> str:
        parts = [
            "You are Atlas, a personalised skill tutor for the system owner. "
            "Use the owner's REAL system (the Roderick ecosystem) as examples wherever possible. "
            "Include relevant credential pathways by producing search queries, shareable outcomes, "
            "and profile/certificate notes. Never claim a course or certificate is completed unless the owner records it. "
            "Do not invent certification prices, certification availability, course titles, or certificate status. "
            "No toy examples. Always respond with valid JSON only — no markdown, no preamble."
        ]
        if self.owner_memory:
            job_market = self.owner_memory.get_job_market_context()
            if job_market:
                parts.append(f"\n### Job Market Context (prioritise in-demand skills)\n{job_market}")
            tech_stack = self.owner_memory.get_tech_stack()
            if tech_stack:
                parts.append(f"\n### Current Tech Stack\n{tech_stack}")
        return "\n".join(parts)

    def _generate_lesson(self, topic: str) -> dict:
        owner_context = self.owner_memory.get_context()
        prompt = self.prompt_template.format(topic=topic, owner_context=owner_context)
        raw = self.llm.complete(
            messages=[{"role": "user", "content": prompt}],
            system=self._build_lesson_system(),
            name="atlas_lesson",
        )
        try:
            lesson = json.loads(raw.strip())
        except json.JSONDecodeError:
            lesson = {
                "topic": topic,
                "skill_state_target": "introduced",
                "summary": raw[:300],
                "key_concepts": [],
                "practical_exercise": "Review the raw content above.",
                "system_connection": "",
                "further_reading": [],
                "estimated_time_minutes": 30,
            }
        lesson["_generated_at"] = datetime.now(timezone.utc).isoformat()
        lesson["_date"] = date.today().isoformat()
        self._enrich_lesson_for_credentials(lesson, topic)

        # Update skill state to at least 'introduced'
        skills = self.load_skills()
        current = skills.get(topic, "unknown")
        if current == "unknown":
            self.advance_skill(topic, "introduced")

        return lesson

    def _enrich_lesson_for_credentials(self, lesson: dict, topic: str) -> None:
        query = str(
            lesson.get("linkedin_learning_search_query")
            or lesson.get("linkedin_query")
            or f"{self._safe_learning_query(topic)} practical"
        ).strip()
        query = self._safe_learning_query(query)
        if query:
            lesson["linkedin_learning_search_query"] = query
            lesson["linkedin_learning_search_url"] = f"https://www.linkedin.com/learning/search?keywords={quote_plus(query)}"
        lesson.setdefault(
            "shareable_outcome",
            f"Explain and demonstrate {self._safe_learning_query(topic)} using a real Roderick ecosystem example.",
        )
        lesson.setdefault(
            "portfolio_receipt",
            "After completing a relevant LinkedIn Learning course or learning path, save the certificate/share link in Atlas.",
        )
        lesson.setdefault(
            "recruiter_visibility",
            "LinkedIn Learning course/path completion can be added to your LinkedIn profile after completion; Atlas will track links you provide.",
        )

    def _safe_learning_query(self, text: str) -> str:
        cleaned = str(text or "").replace("$", "").replace("AUD", "")
        cleaned = re.sub(r"\([^)]*(?:cost|price|\d+\s*certification|certification)[^)]*\)", "", cleaned, flags=re.IGNORECASE)
        for phrase in ("Cost-Effective", "cost-effective"):
            cleaned = cleaned.replace(phrase, "")
        while "  " in cleaned:
            cleaned = cleaned.replace("  ", " ")
        return cleaned.strip(" -()")

    def _generate_curriculum_update(self, task: Task) -> dict:
        context = {
            "request": task.description,
            "current_skills": self.load_skills(),
            "today_lesson": self.get_today_lesson(),
            "study_status": self.get_lesson_status(),
            "owner_context": self.owner_memory.get_context(),
            "job_market_context": self.owner_memory.get_job_market_context(),
        }
        raw = self.llm.complete(
            messages=[{"role": "user", "content": json.dumps(context, ensure_ascii=False, indent=2)}],
            system=(
                "You are Atlas, the owner's personal tutor and learning coach. "
                "Review the current curriculum, skills, job-market context, and study state. "
                "Return valid JSON only with keys: summary, skills_to_add, skills_to_prioritize, "
                "skills_to_defer, study_plan_adjustments, next_lesson_topic, rationale. "
                "Use the owner's real Roderick ecosystem as the learning environment."
            ),
            name="atlas_curriculum_update",
        )
        try:
            update = json.loads(raw.strip())
        except json.JSONDecodeError:
            update = {
                "summary": raw[:500],
                "skills_to_add": [],
                "skills_to_prioritize": [],
                "skills_to_defer": [],
                "study_plan_adjustments": [],
                "next_lesson_topic": "",
                "rationale": "Unstructured Atlas response.",
            }
        update["_task_id"] = task.id
        update["_updated_at"] = datetime.now(timezone.utc).isoformat()
        return update

    def _format_curriculum_update(self, update: dict) -> str:
        lines = [
            "<b>Atlas curriculum updated.</b>",
            update.get("summary", "")[:400],
        ]
        priorities = update.get("skills_to_prioritize", [])
        if priorities:
            lines.append("\n<b>Priorities:</b>")
            for skill in priorities[:5]:
                lines.append(f"  • {skill}")
        next_topic = update.get("next_lesson_topic")
        if next_topic:
            lines.append(f"\n<b>Next lesson:</b> {next_topic}")
        return "\n".join(lines)

    def _build_chat_system(self) -> str:
        return (
            "You are Atlas, the owner's personal tutor and study companion inside the Roderick ecosystem. "
            "You are not a generic chatbot and not a command menu. Talk naturally, coach the owner, adapt the lesson plan, "
            "and use their real system as the learning environment. If they postpone, acknowledge and reduce pressure. "
            "If they want to study, help them begin with one concrete next step. If they are confused, explain gently. "
            "You may reference current skills, today's lesson, and study status from the provided context. "
            "Suggest relevant credential pathways where applicable: recommend searches, ask the owner to record certificate links after completion, "
            "and never claim a certificate exists until they record it. "
            "Keep replies concise enough for Telegram. Use Telegram HTML only: <b>, <i>, and <code>. "
            "Do not use Markdown."
        )

    def _build_chat_prompt(self, user_message: str, action_taken: str) -> str:
        lesson = self.get_today_lesson()
        lesson_summary = None
        if lesson:
            lesson_summary = {
                "topic": lesson.get("topic"),
                "summary": lesson.get("summary"),
                "key_concepts": lesson.get("key_concepts", [])[:5],
                "practical_exercise": lesson.get("practical_exercise"),
                "estimated_time_minutes": lesson.get("estimated_time_minutes"),
                "linkedin_learning_search_query": lesson.get("linkedin_learning_search_query"),
                "linkedin_learning_search_url": lesson.get("linkedin_learning_search_url"),
                "shareable_outcome": lesson.get("shareable_outcome"),
            }

        context = {
            "user_message": user_message,
            "action_taken": action_taken,
            "presence": self._presence(),
            "study_status": self.get_lesson_status(),
            "skills": self.load_skills(),
            "today_lesson": lesson_summary,
            "learning_log_recent": self.load_learning_log()[:8],
            "owner_context": self.owner_memory.get_context(),
        }
        return (
            "Respond to the owner's Atlas chat message using this context.\n"
            f"{json.dumps(context, ensure_ascii=False, indent=2)}"
        )

    def _format_lesson_telegram(self, lesson: dict) -> str:
        topic = lesson.get("topic", "Unknown")
        summary = lesson.get("summary", "")
        concepts = lesson.get("key_concepts", [])
        exercise = lesson.get("practical_exercise", "")
        connection = lesson.get("system_connection", "")
        linkedin_query = lesson.get("linkedin_learning_search_query", "")
        linkedin_url = lesson.get("linkedin_learning_search_url", "")
        outcome = lesson.get("shareable_outcome", "")
        minutes = lesson.get("estimated_time_minutes", 30)

        lines = [
            f"📚 <b>Atlas Lesson: {topic}</b>\n",
            f"{summary}\n",
        ]
        if concepts:
            lines.append("<b>Key concepts:</b>")
            for c in concepts[:4]:
                lines.append(f"  • {c}")
            lines.append("")
        if exercise:
            lines.append(f"<b>Exercise:</b> {exercise[:200]}")
        if connection:
            lines.append(f"\n<b>In your system:</b> {connection[:150]}")
        if linkedin_query:
            lines.append(f"\n<b>LinkedIn Learning:</b> Search <i>{linkedin_query[:120]}</i>")
        if outcome:
            lines.append(f"\n<b>Proof target:</b> {outcome[:180]}")
        if linkedin_url:
            lines.append(f"\n<b>Search:</b> {linkedin_url}")
        lines.append(f"\n<i>⏱ ~{minutes} minutes</i>")
        return "\n".join(lines)

    def _record_today_completion(self, note: str = "") -> None:
        lesson = self.get_today_lesson() or {}
        topic = lesson.get("topic") or "Atlas lesson"
        self.record_learning_entry({
            "type": "atlas_lesson_completion",
            "topic": topic,
            "status": "completed",
            "note": note,
            "linkedin_certificate_url": "",
            "linkedin_learning_search_url": lesson.get("linkedin_learning_search_url", ""),
            "shareable_outcome": lesson.get("shareable_outcome", ""),
        })

    def _ensure_skill_bootstrap(self) -> None:
        if not self.skills_path.exists():
            skills = {topic: "unknown" for topic in self.curriculum_focus}
            self.save_skills(skills)
            logger.info("Atlas skill registry bootstrapped (%d skills)", len(skills))

    # ── Daily lesson trigger (called by scheduler) ────────────────────────────

    async def deliver_daily_lesson(self) -> None:
        """Called by scheduler. Creates today's lesson if it doesn't exist yet."""
        status = self.get_lesson_status()
        if status.get("date") == date.today().isoformat() and status.get("status") == "postponed":
            logger.info("Atlas daily lesson skipped: postponed for today")
            return

        if self.get_today_lesson():
            return  # Already done today

        skills = self.load_skills()
        # Pick the topic with the lowest progression that is not interview_ready
        priority_order = ["unknown", "introduced", "learning", "practiced", "project_used"]
        candidates = [
            (SKILL_STATES.index(state), topic)
            for topic, state in skills.items()
            if state in priority_order
        ]
        if not candidates:
            return
        _, topic = min(candidates)

        logger.info("Atlas generating daily lesson: %s", topic)
        lesson = await asyncio.get_event_loop().run_in_executor(None, self._generate_lesson, topic)
        self.save_lesson(lesson)

        if self._notify:
            mode = self._presence()
            if mode in ("away", "focus"):
                await self._notify(self._format_lesson_telegram(lesson))
            else:
                await self._notify(
                    f"📚 <b>Today's Atlas lesson:</b> {lesson.get('topic', topic)}\n"
                    f"<i>View full lesson on the dashboard.</i>"
                )
