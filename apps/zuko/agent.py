"""
Zuko - job search and application agent.

Scrapes SEEK and LinkedIn for jobs matching the configured target role and location,
scores them against targeting config, generates tailored cover letters,
surfaces each job as an actionable Telegram card (Apply / Skip / Full Letter / Manual),
and drives Playwright-based form filling with pre-submission approval.

This is a supervised agent — it never submits anything without explicit approval.
"""

import asyncio
import contextlib
import json
import logging
import re
import sqlite3
from datetime import datetime, timezone
from html import escape as _h
from pathlib import Path
from typing import Callable, Coroutine, Optional

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup

from shared.agent_learning import try_reflect_after_task
from shared.db.agents import emit_heartbeat, record_agent_error, record_agent_success, update_agent_status
from shared.db.events import emit_event
from shared.db.messages import send_agent_message
from shared.db.tasks import get_next_task, requeue_in_progress_tasks, touch_task, update_task_status
from shared.llm.anthropic_provider import AnthropicProvider
from shared.memory.founder import OwnerMemory
from shared.schemas.task import Task

logger = logging.getLogger(__name__)

AGENT_NAME   = "zuko"
JOBS_PER_RUN = 5  # max new jobs to surface per scan

SOURCE_ICONS = {"seek": "🟢 Seek", "linkedin": "🔵 LinkedIn", "linkedin_feed": "📢 LinkedIn Feed"}
APPLY_ICONS  = {
    "seek_quick":    "⚡ Quick Apply",
    "linkedin_easy": "✨ Easy Apply",
    "company":       "🏢 Company Site",
    "manual":        "🔗 Manual",
    "email":         "📧 Email",
    "unknown":       "❓",
}

# ── Targeting config ───────────────────────────────────────────────────────────
_TARGETING_PATH = Path(__file__).parent / "config" / "job_targeting.json"
_MULTILEVEL_RE  = re.compile(
    r'\b(junior|mid|senior|graduate)\b.*?\b(junior|mid|senior|graduate)\b',
    re.IGNORECASE,
)

# ── CV summary for cover letters ───────────────────────────────────────────────
# Loaded from candidate_profile.json so personal info stays out of source control.
_PROFILE_PATH = Path(__file__).parent / "config" / "candidate_profile.json"

def _load_cv_context() -> tuple[str, str]:
    """Load CV summary and cover letter style from candidate_profile.json.
    Returns (cv_summary, cover_letter_style). Falls back to placeholders if file missing."""
    try:
        p = json.loads(_PROFILE_PATH.read_text(encoding="utf-8"))
        name         = p.get("full_name", "Candidate")
        phone        = p.get("phone_formatted", "")
        email        = p.get("email", "")
        github       = p.get("github_url", "").replace("https://", "")
        linkedin     = p.get("linkedin_url", "").replace("https://", "")
        cv_summary   = p.get("cv_summary", "(no CV summary — add cv_summary field to candidate_profile.json)")
        cover_style  = p.get("cover_letter_style", "Write a concise, professional cover letter in first person. 250-350 words.")
        sign_off     = p.get("sign_off", f"Kind regards,\n{name}")
        header = f"Name: {name}\nPhone: {phone} | Email: {email}\nGitHub: {github} | LinkedIn: {linkedin}"
        return f"{header}\n\n{cv_summary}", f"{cover_style}\nSign off: \"{sign_off}\""
    except FileNotFoundError:
        return (
            "(CV summary not configured — copy apps/zuko/config/candidate_profile.example.json "
            "to candidate_profile.json and fill in your details)",
            "Write a concise, professional cover letter in first person. 250-350 words.",
        )

CV_SUMMARY, COVER_LETTER_STYLE = _load_cv_context()


class ZukoAgent:
    def __init__(
        self,
        llm: AnthropicProvider,
        db_path: str,
        data_dir: str,
        config: dict,
        owner_memory: OwnerMemory,
    ):
        self.llm            = llm
        self.db_path        = db_path
        self.data_dir       = Path(data_dir)
        self.reports_dir    = self.data_dir / "reports"
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self.owner_memory = owner_memory
        self.config         = config
        self.poll_interval  = int(config.get("scheduler", {}).get("worker_poll_interval_seconds", 10))
        self.keepalive_seconds = max(10, int(config.get("observability", {}).get("task_keepalive_seconds", 20)))

        # Targeting defaults — lazy-loaded from job_targeting.json
        self.target_location = "Your City"

        # Applications DB — separate from the task/agent DB
        self._app_db_path = str(self.data_dir / "applications.db")
        self._init_app_db()

        # Telegram bot — set by main.py after construction
        self._bot:      Optional[Bot]      = None
        self._chat_id:  Optional[int]      = None
        self._notify:   Optional[Callable] = None

    # ── Setup ──────────────────────────────────────────────────────────────────

    def set_notify(self, fn: Callable[[str], Coroutine]) -> None:
        self._notify = fn

    def set_bot(self, bot: Bot, chat_id: int) -> None:
        """Called by main.py to give the agent a direct Telegram Bot handle."""
        self._bot     = bot
        self._chat_id = chat_id

    # ── Applications DB ────────────────────────────────────────────────────────

    def _init_app_db(self) -> None:
        conn = sqlite3.connect(self._app_db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS applications (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id        TEXT UNIQUE,
                title         TEXT,
                company       TEXT,
                location      TEXT,
                url           TEXT,
                salary        TEXT,
                description   TEXT,
                cover_letter  TEXT,
                source        TEXT DEFAULT 'unknown',
                apply_type    TEXT DEFAULT 'unknown',
                status        TEXT DEFAULT 'pending',
                contact_email TEXT DEFAULT '',
                contact_phone TEXT DEFAULT '',
                created_at    TEXT,
                updated_at    TEXT
            )
        """)
        for col, defn in [
            ("source",        "TEXT DEFAULT 'unknown'"),
            ("apply_type",    "TEXT DEFAULT 'unknown'"),
            ("contact_email", "TEXT DEFAULT ''"),
            ("contact_phone", "TEXT DEFAULT ''"),
        ]:
            try:
                conn.execute(f"ALTER TABLE applications ADD COLUMN {col} {defn}")
            except Exception:
                pass
        conn.commit()
        conn.close()

    def _job_save(self, job: dict) -> None:
        conn = sqlite3.connect(self._app_db_path)
        try:
            conn.execute("""
                INSERT OR IGNORE INTO applications
                (job_id,title,company,location,url,salary,description,cover_letter,
                 source,apply_type,status,contact_email,contact_phone,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                job["job_id"],
                job.get("title", job.get("role", "")),
                job.get("company", ""),
                job.get("location", ""),
                job.get("url", ""),
                job.get("salary", ""),
                job.get("description", ""),
                job.get("cover_letter", ""),
                job.get("source", "unknown"),
                job.get("apply_type", "unknown"),
                "pending",
                job.get("contact_email", ",".join(job.get("emails", []))),
                job.get("contact_phone", ",".join(job.get("phones", []))),
                datetime.now().isoformat(),
                datetime.now().isoformat(),
            ))
            conn.commit()
        finally:
            conn.close()

    def _job_update_status(self, job_id: str, status: str) -> None:
        conn = sqlite3.connect(self._app_db_path)
        conn.execute(
            "UPDATE applications SET status=?, updated_at=? WHERE job_id=?",
            (status, datetime.now().isoformat(), job_id),
        )
        conn.commit()
        conn.close()

    def _job_already_seen(self, job_id: str) -> bool:
        conn = sqlite3.connect(self._app_db_path)
        row = conn.execute("SELECT id FROM applications WHERE job_id=?", (job_id,)).fetchone()
        conn.close()
        return row is not None

    def _job_get(self, job_id: str) -> dict | None:
        conn = sqlite3.connect(self._app_db_path)
        row = conn.execute(
            "SELECT job_id,title,company,location,url,cover_letter,source,apply_type,"
            "description,contact_email,contact_phone,status "
            "FROM applications WHERE job_id=?",
            (job_id,),
        ).fetchone()
        conn.close()
        if row:
            return {
                "job_id": row[0], "title": row[1], "company": row[2],
                "location": row[3], "url": row[4], "cover_letter": row[5],
                "source": row[6], "apply_type": row[7], "description": row[8],
                "contact_email": row[9] or "", "contact_phone": row[10] or "",
                "status": row[11],
            }
        return None

    # ── Targeting / scoring ────────────────────────────────────────────────────

    def _load_targeting(self) -> dict:
        try:
            return json.loads(_TARGETING_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _score_job(self, job: dict) -> int:
        cfg     = self._load_targeting()
        scoring = cfg.get("scoring", {})
        title   = job.get("title", job.get("role", ""))
        desc    = job.get("description", job.get("teaser", ""))
        text    = (title + " " + desc).lower()

        excluded = cfg.get("excluded_keywords_in_title", [])
        is_multi = bool(_MULTILEVEL_RE.search(title))
        if not is_multi:
            for kw in excluded:
                if re.search(r'\b' + re.escape(kw.lower()) + r'\b', title.lower()):
                    return 0

        score = 0.0
        for tier, default_w in [("high_fit_keywords", 3), ("medium_fit_keywords", 1), ("low_fit_keywords", 0.5)]:
            tier_cfg = scoring.get(tier, {})
            w = tier_cfg.get("weight", default_w)
            for term in tier_cfg.get("terms", []):
                if term.lower() in text:
                    score += w

        for term in scoring.get("penalty_keywords", {}).get("terms", []):
            if term.lower() in text:
                if is_multi and term.lower() in ("junior", "senior", "graduate"):
                    continue
                score += scoring.get("penalty_keywords", {}).get("weight", -5)

        return min(10, max(0, int(score)))

    # ── Cover letter generation ────────────────────────────────────────────────

    def _generate_cover_letter(self, job: dict) -> str:
        """Blocking call — runs in executor. Uses the configured LLM."""
        title   = job.get("title", job.get("role", "Role"))
        company = job.get("company", "the company")
        loc     = job.get("location", self.target_location)
        desc    = job.get("description", job.get("teaser", ""))[:2500]

        candidate_name = json.loads(_PROFILE_PATH.read_text()).get("full_name", "the applicant") if _PROFILE_PATH.exists() else "the applicant"
        prompt = (
            f"You are {candidate_name} writing a job application cover letter in your own voice.\n"
            f"Write entirely in first person (I, my, me). Never third person.\n\n"
            f"YOUR CV:\n{CV_SUMMARY}\n\n"
            f"STYLE:\n{COVER_LETTER_STYLE}\n\n"
            f"JOB YOU ARE APPLYING FOR:\n"
            f"Title: {title}\nCompany: {company}\nLocation: {loc}\nDescription:\n{desc}\n\n"
            f"Write the cover letter now. Output ONLY the cover letter text, nothing else."
        )
        try:
            return self.llm.complete(
                messages=[{"role": "user", "content": prompt}],
                system="You are a professional cover letter writer. Output only the letter text.",
                name="zuko_cover_letter",
            )
        except Exception as exc:
            logger.warning("[Zuko] Cover letter generation failed: %s", exc)
            return (
                f"Dear Hiring Manager,\n\n"
                f"I am writing to express my strong interest in the {title} position at {company}. "
                f"I am confident my skills and experience make me a strong candidate for this role.\n\n"
                f"Kind regards,\n{CV_SUMMARY.splitlines()[0].replace('Name: ', '') if CV_SUMMARY.startswith('Name:') else 'The Applicant'}"
            )

    # ── Telegram messaging ─────────────────────────────────────────────────────

    def _build_job_message(self, job: dict, score: int, cover_letter: str) -> str:
        title    = job.get("title", job.get("role", "Role"))
        company  = job.get("company", "Unknown")
        location = job.get("location", self.target_location)
        salary   = job.get("salary", "")
        url      = job.get("url", "")
        source   = SOURCE_ICONS.get(job.get("source", ""), "")
        apply    = APPLY_ICONS.get(job.get("apply_type", ""), "")
        salary_line = f"\n💰 {_h(salary)}" if salary else ""
        cl_preview  = _h(cover_letter[:800]) + ("..." if len(cover_letter) > 800 else "")

        return (
            f"🆕 <b>New Job Match</b> (Score: {score}/10)\n"
            f"{source}  {apply}\n\n"
            f"<b>{_h(title)}</b>\n"
            f"🏢 {_h(company)}\n"
            f"📍 {_h(location)}"
            f"{salary_line}\n"
            f'🔗 <a href="{url}">View Job</a>\n\n'
            f"――――――――――――――――――\n"
            f"<b>Cover Letter:</b>\n\n"
            f"{cl_preview}"
        )

    def _make_job_keyboard(self, job_id: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🚀 Auto-Apply",  callback_data=f"apply:{job_id}"),
                InlineKeyboardButton("❌ Skip",         callback_data=f"skip:{job_id}"),
            ],
            [
                InlineKeyboardButton("📋 Full Letter",  callback_data=f"full:{job_id}"),
                InlineKeyboardButton("🔗 Manual Apply", callback_data=f"manual:{job_id}"),
            ],
        ])

    def _build_feed_message(self, post: dict) -> str:
        author  = _h(post.get("author", "Unknown"))
        company = _h(post.get("company", ""))
        text    = _h(post.get("text", post.get("description", ""))[:600])
        emails  = post.get("emails", [])
        phones  = post.get("phones", [])
        url     = post.get("url", "")

        contact = ""
        if emails:
            contact += "\n📧 " + " | ".join(_h(e) for e in emails)
        if phones:
            contact += "\n📞 " + " | ".join(_h(p) for p in phones)
        url_line = f'\n🔗 <a href="{url}">View Post</a>' if url else ""

        return (
            f"📢 <b>LinkedIn Feed — Job Post</b>\n"
            f"👤 <b>{author}</b>"
            + (f" @ {company}" if company else "")
            + contact
            + url_line
            + f"\n\n――――――――――――――――――\n{text}"
            + ("..." if len(post.get("text", post.get("description", ""))) > 600 else "")
        )

    def _make_feed_keyboard(self, job_id: str, has_email: bool) -> InlineKeyboardMarkup:
        if has_email:
            return InlineKeyboardMarkup([[
                InlineKeyboardButton("📧 Send Email", callback_data=f"email_apply:{job_id}"),
                InlineKeyboardButton("❌ Skip",        callback_data=f"email_skip:{job_id}"),
            ]])
        return InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Skip", callback_data=f"email_skip:{job_id}"),
        ]])

    async def _send_card(self, job: dict, score: int, cover_letter: str) -> None:
        """Send a single job card with action buttons to Telegram."""
        if not self._bot or not self._chat_id:
            if self._notify:
                await self._notify(self._build_job_message(job, score, cover_letter))
            return

        msg      = self._build_job_message(job, score, cover_letter)
        keyboard = self._make_job_keyboard(job["job_id"])
        try:
            await self._bot.send_message(
                chat_id=self._chat_id,
                text=msg,
                parse_mode="HTML",
                reply_markup=keyboard,
                disable_web_page_preview=True,
            )
            logger.info("[Zuko] Sent job card: %s @ %s", job.get("title", job.get("role")), job.get("company"))
        except Exception as exc:
            logger.error("[Zuko] Failed to send job card: %s", exc)
            # Fallback plain text
            try:
                plain = (
                    f"{job.get('title', job.get('role', ''))} @ {job.get('company', '')}\n"
                    f"{job.get('location', '')}\n{job.get('url', '')}"
                )
                await self._bot.send_message(
                    chat_id=self._chat_id,
                    text=plain,
                    reply_markup=keyboard,
                    disable_web_page_preview=True,
                )
            except Exception:
                pass

    async def _send_feed_card(self, post: dict) -> None:
        if not self._bot or not self._chat_id:
            return
        msg      = self._build_feed_message(post)
        keyboard = self._make_feed_keyboard(post["job_id"], bool(post.get("emails")))
        try:
            await self._bot.send_message(
                chat_id=self._chat_id,
                text=msg,
                parse_mode="HTML",
                reply_markup=keyboard,
                disable_web_page_preview=True,
            )
        except Exception as exc:
            logger.error("[Zuko] Failed to send feed card: %s", exc)

    # ── Main worker loop ───────────────────────────────────────────────────────

    async def run(self) -> None:
        logger.info("Zuko worker started")
        recovered = requeue_in_progress_tasks(self.db_path, AGENT_NAME)
        if recovered:
            logger.info("Zuko recovered %d abandoned task(s)", recovered)
        update_agent_status(self.db_path, AGENT_NAME, "idle")
        emit_heartbeat(self.db_path, AGENT_NAME, current_task_id=None, current_model=self.llm.model)

        while True:
            try:
                task = get_next_task(self.db_path, AGENT_NAME, "pending")
                if task:
                    await self._process(task)
                else:
                    await asyncio.sleep(self.poll_interval)
            except Exception as exc:
                logger.error("Zuko worker error: %s", exc, exc_info=True)
                await asyncio.sleep(self.poll_interval)

    async def _process(self, task: Task) -> None:
        logger.info("Zuko processing task #%d: %s", task.id, task.task_type)
        update_agent_status(self.db_path, AGENT_NAME, "busy", task.description[:60])
        emit_heartbeat(self.db_path, AGENT_NAME, current_task_id=task.id, current_model=self.llm.model)
        update_task_status(self.db_path, task.id, "in_progress")

        try:
            if task.task_type == "job_search":
                result = await self._run_job_search(task)
            elif task.task_type == "application_prep":
                result = await self._run_application_prep(task)
            else:
                result = {"note": f"Unknown task type: {task.task_type}"}

            report_path = self.reports_dir / f"zuko_{task.id}.json"
            report_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
            update_task_status(self.db_path, task.id, "completed", result)
            update_agent_status(self.db_path, AGENT_NAME, "idle", f"Completed task #{task.id}")
            record_agent_success(self.db_path, AGENT_NAME, f"Task #{task.id} done")
            emit_event(self.db_path, "zuko_task_complete", AGENT_NAME, {"task_id": task.id})

            try:
                if task.task_type == "job_search":
                    self._persist_job_search_memory(result)
                    self._feed_merlin(result)
                try_reflect_after_task(
                    llm=self.llm,
                    db_path=self.db_path,
                    data_dir=str(self.data_dir),
                    agent_name=AGENT_NAME,
                    task=task,
                    result=result,
                    owner_context=self.owner_memory.get_context(),
                )
            except Exception as exc:
                logger.warning("Zuko post-completion side effect failed for task #%d: %s", task.id, exc)

        except Exception as exc:
            logger.error("Zuko task #%d failed: %s", task.id, exc, exc_info=True)
            update_task_status(self.db_path, task.id, "failed", {"error": str(exc)})
            update_agent_status(self.db_path, AGENT_NAME, "idle")
            record_agent_error(self.db_path, AGENT_NAME, str(exc))
            if self._notify:
                await self._notify(f"⚠️ <b>Zuko — Task #{task.id} failed</b>\n\nError: {_h(str(exc)[:300])}")
        finally:
            emit_heartbeat(self.db_path, AGENT_NAME, current_task_id=None, current_model=self.llm.model)

    # ── Keepalive ─────────────────────────────────────────────────────────────

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

    # ── Job search ─────────────────────────────────────────────────────────────

    async def _run_job_search(self, task: Task) -> dict:
        """Scrape real jobs, score them, send per-job Telegram cards."""
        criteria = task.payload.get("criteria", task.description)

        # Playwright scrapers are async — run keepalive alongside them directly
        keepalive = asyncio.create_task(
            self._keepalive_loop(task.id, f"Scanning: {criteria[:50]}", self.llm.model)
        )
        try:
            jobs = await self._scrape_all(criteria)
        finally:
            keepalive.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await keepalive

        cfg       = self._load_targeting()
        threshold = cfg.get("scoring", {}).get("fit_thresholds", {}).get("skip_below", 1)
        sent      = 0
        new_jobs  = []

        for job in jobs:
            if self._job_already_seen(job["job_id"]):
                continue
            new_jobs.append(job)

        logger.info("[Zuko] %d new jobs from %d scraped", len(new_jobs), len(jobs))

        for job in new_jobs[:JOBS_PER_RUN]:
            score = self._score_job(job)
            if score < threshold:
                logger.info("[Zuko] Skipping low-score (%d): %s @ %s", score, job.get("title", ""), job.get("company", ""))
                continue

            # Fetch full description to improve cover letter quality
            source = job.get("source", "")
            if source == "seek" and not job.get("description"):
                try:
                    from apps.zuko.scraper import seek_get_job_detail
                    desc, apply_type = await seek_get_job_detail(job["url"])
                    job["description"] = desc
                    job["apply_type"]  = apply_type
                except Exception as exc:
                    logger.debug("[Zuko] Detail fetch failed for %s: %s", job["url"], exc)

            # Generate cover letter in executor
            try:
                cover_letter = await asyncio.get_event_loop().run_in_executor(
                    None, lambda j=job: self._generate_cover_letter(j)
                )
            except Exception as exc:
                logger.warning("[Zuko] Cover letter failed for %s: %s", job.get("job_id"), exc)
                cover_letter = f"Dear Hiring Manager,\n\nI am interested in the {job.get('title', 'role')} at {job.get('company', 'your company')}.\n\nKind regards,\nthe system owner"

            job["cover_letter"] = cover_letter
            self._job_save(job)
            await self._send_card(job, score, cover_letter)
            sent += 1

        # Handle LinkedIn feed posts differently
        feed_posts = [j for j in new_jobs if j.get("source") == "linkedin_feed"]
        for post in feed_posts[:3]:
            self._job_save(post)
            await self._send_feed_card(post)

        summary = f"Scanned: {len(jobs)} total, {len(new_jobs)} new, {sent} sent to Telegram"
        return {
            "search_summary": summary,
            "jobs_found": len(jobs),
            "new_jobs": len(new_jobs),
            "cards_sent": sent,
            "listings_of_interest": [
                {
                    "role": j.get("title", j.get("role", "")),
                    "company": j.get("company", ""),
                    "location": j.get("location", ""),
                    "source": j.get("source", ""),
                    "status": "surfaced",
                }
                for j in new_jobs[:8]
            ],
            "active_applications": [],
            "_task_id": task.id,
            "_searched_at": datetime.now(timezone.utc).isoformat(),
        }

    async def _scrape_all(self, criteria: str) -> list[dict]:
        """Run all scrapers and return combined deduped job list."""
        from apps.zuko.scraper import scrape_seek, scrape_linkedin, scrape_linkedin_feed
        cfg     = self._load_targeting()
        self.target_location = cfg.get("target_location", "Your City")
        roles   = cfg.get("target_roles", ["DevOps Engineer", "Systems Engineer"])

        # Pick 2-3 roles relevant to the criteria
        criteria_lower = criteria.lower()
        selected_roles = []
        for role in roles:
            if any(w in criteria_lower for w in role.lower().split()):
                selected_roles.append(role)
        if not selected_roles:
            selected_roles = roles[:3]
        selected_roles = selected_roles[:3]

        all_jobs: list[dict] = []
        seen_ids: set[str]   = set()

        for role in selected_roles:
            try:
                seek_jobs = await scrape_seek(role)
                for j in seek_jobs:
                    if j["job_id"] not in seen_ids:
                        seen_ids.add(j["job_id"])
                        all_jobs.append(j)
            except Exception as exc:
                logger.warning("[Zuko] SEEK scrape failed for '%s': %s", role, exc)

        for role in selected_roles[:2]:
            try:
                li_jobs = await scrape_linkedin(role)
                for j in li_jobs:
                    if j["job_id"] not in seen_ids:
                        seen_ids.add(j["job_id"])
                        all_jobs.append(j)
            except Exception as exc:
                logger.warning("[Zuko] LinkedIn scrape failed for '%s': %s", role, exc)

        if "linkedin" in criteria_lower or "feed" in criteria_lower:
            try:
                feed_posts = await scrape_linkedin_feed(max_posts=30)
                for p in feed_posts:
                    if p["job_id"] not in seen_ids:
                        seen_ids.add(p["job_id"])
                        all_jobs.append(p)
            except Exception as exc:
                logger.warning("[Zuko] LinkedIn feed scrape failed: %s", exc)

        return all_jobs

    # ── Application prep ───────────────────────────────────────────────────────

    async def _run_application_prep(self, task: Task) -> dict:
        """Generate cover letter and optionally drive Playwright apply flow."""
        job_id         = task.payload.get("job_id", "")
        cover_only     = task.payload.get("cover_letter_only", False)
        start_playwright = task.payload.get("start_playwright", False)

        job = self._job_get(job_id) if job_id else None
        if not job:
            # Build job dict from payload if not in DB
            job = task.payload.get("job", {})
            if not job:
                return {"error": "No job data found for application prep"}

        # Generate cover letter
        keepalive = asyncio.create_task(
            self._keepalive_loop(task.id, f"Drafting cover letter: {job.get('title', '')[:40]}", self.llm.model)
        )
        try:
            cover_letter = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self._generate_cover_letter(job)
            )
        finally:
            keepalive.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await keepalive

        job["cover_letter"] = cover_letter
        self._job_save(job)

        # Send cover letter to Telegram
        title   = job.get("title", job.get("role", "Role"))
        company = job.get("company", "Unknown")
        if self._bot and self._chat_id:
            try:
                await self._bot.send_message(
                    chat_id=self._chat_id,
                    text=(
                        f"✍️ <b>Cover Letter</b>\n"
                        f"<b>{_h(title)}</b> @ {_h(company)}\n\n"
                        f"{_h(cover_letter[:3000])}"
                    ),
                    parse_mode="HTML",
                )
            except Exception as exc:
                logger.error("[Zuko] Failed to send cover letter: %s", exc)
        elif self._notify:
            await self._notify(
                f"✍️ <b>Cover Letter</b>\n"
                f"<b>{_h(title)}</b> @ {_h(company)}\n\n"
                f"{_h(cover_letter[:2000])}"
            )

        result = {
            "role": title,
            "company": company,
            "cover_letter_draft": cover_letter,
            "_task_id": task.id,
            "_prepped_at": datetime.now(timezone.utc).isoformat(),
        }

        if cover_only or not start_playwright:
            return result

        # Playwright apply flow
        if not self._bot or not self._chat_id:
            logger.warning("[Zuko] No bot configured — cannot run Playwright apply flow")
            return result

        await self._notify_text(
            f"🌐 <b>Opening application form...</b>\n"
            f"<b>{_h(title)}</b> @ {_h(company)}\n\n"
            f"Filling form and preparing for your review."
        )

        from apps.zuko.browser import apply_to_job
        keepalive2 = asyncio.create_task(
            self._keepalive_loop(task.id, f"Playwright apply: {title[:40]}", self.llm.model)
        )
        try:
            apply_result = await apply_to_job(job, cover_letter, bot=self._bot, chat_id=self._chat_id)
        finally:
            keepalive2.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await keepalive2

        if apply_result.success:
            self._job_update_status(job.get("job_id", ""), "applied")
            msg = (
                f"✅ <b>Application Submitted!</b>\n\n"
                f"<b>{_h(title)}</b> @ {_h(company)}\n\n"
                f"{_h(apply_result.message)}"
            )
        else:
            self._job_update_status(job.get("job_id", ""), "apply_failed")
            msg = (
                f"⚠️ <b>Application not submitted</b>\n\n"
                f"<b>{_h(title)}</b> @ {_h(company)}\n\n"
                f"{_h(apply_result.message)}\n\n"
                f'Apply manually: <a href="{job.get("url", "")}">Open Job</a>'
            )

        try:
            await self._bot.send_message(
                chat_id=self._chat_id,
                text=msg,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
            if apply_result.screenshot_path and Path(apply_result.screenshot_path).exists():
                with open(apply_result.screenshot_path, "rb") as f:
                    await self._bot.send_photo(
                        self._chat_id, f,
                        caption=f"Screenshot: {title} @ {company}",
                    )
        except Exception as exc:
            logger.error("[Zuko] Failed to send apply result: %s", exc)

        result["playwright_result"] = {
            "success": apply_result.success,
            "message": apply_result.message,
            "fields_filled": apply_result.fields_filled,
        }
        return result

    async def _notify_text(self, text: str) -> None:
        if self._bot and self._chat_id:
            try:
                await self._bot.send_message(chat_id=self._chat_id, text=text, parse_mode="HTML")
            except Exception:
                pass
        elif self._notify:
            await self._notify(text)

    # ── Memory / reporting ─────────────────────────────────────────────────────

    def _persist_job_search_memory(self, result: dict) -> None:
        if not self.owner_memory:
            return
        try:
            listings = result.get("listings_of_interest", [])
            if listings:
                rows = [
                    f"- **{l.get('role', '')}** @ **{l.get('company', '')}** [{l.get('source', '')}]"
                    for l in listings[:8]
                ]
                self.owner_memory.update_job_market("Recent Listings of Interest", "\n".join(rows))
        except Exception as exc:
            logger.warning("Zuko could not update job market memory: %s", exc)

    def _feed_merlin(self, result: dict) -> None:
        listings = result.get("listings_of_interest", [])
        if not listings:
            return
        top  = listings[0]
        line = f"Top job signal: {top.get('role', '')} at {top.get('company', '')} [{top.get('source', '')}]"
        try:
            send_agent_message(self.db_path, AGENT_NAME, "merlin", line, priority="normal")
        except Exception as exc:
            logger.warning("Zuko Merlin handoff failed: %s", exc)
