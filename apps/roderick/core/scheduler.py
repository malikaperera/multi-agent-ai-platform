"""
APScheduler-based scheduler for Roderick.
Runs inside the same asyncio event loop as the Telegram bot.

Active jobs:
  - daily_briefing        : 08:00 local time every day (configurable timezone)
  - atlas_daily_lesson    : 09:00 local time every day (configurable)
  - merlin_forge_digest   : once daily - consolidate Merlin findings into one Forge request
  - forge_watchdog        : periodically detect and recover stale Forge work
  - reminder_check        : every N minutes
  - weekly_planning       : Sunday 19:00 local time (when enabled in config)
  - venture_proactive     : every 6h - cycle through proactive_research_domains
  - zuko_proactive        : configurable job-market scans
"""
import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Callable, Coroutine, Optional

if TYPE_CHECKING:
    from shared.memory.founder import OwnerMemory
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler

logger = logging.getLogger(__name__)


class RoderickScheduler:
    def __init__(
        self,
        config: dict,
        briefing_fn: Callable[[], Coroutine],
        send_fn: Callable[[str], Coroutine],
        atlas_daily_lesson_fn: Optional[Callable[[], Coroutine]] = None,
        ecosystem_council_fn: Optional[Callable[[], Coroutine]] = None,
        owner_memory: Optional["OwnerMemory"] = None,
        db_path: Optional[str] = None,
    ):
        self.config = config["scheduler"]
        self.atlas_config = config.get("atlas", {})
        self.venture_config = config.get("venture", {})
        self.merlin_config = config.get("merlin", {})
        self.forge_config = config.get("forge", {})
        self.sentinel_config = config.get("sentinel", {})
        self.zuko_config = config.get("zuko", {})
        self.observability_config = config.get("observability", {})
        self.briefing_fn = briefing_fn
        self.send_fn = send_fn
        self.atlas_daily_lesson_fn = atlas_daily_lesson_fn
        self.ecosystem_council_fn = ecosystem_council_fn
        self.owner_memory = owner_memory
        self.db_path = db_path
        self.tz = ZoneInfo(self.config.get("timezone", "UTC"))
        self._scheduler = AsyncIOScheduler(timezone=self.tz)
        self._venture_domain_index = 0
        self._merlin_topic_index = 0
        self._zuko_scan_index = 0

    def start(self) -> None:
        self._schedule_daily_briefing()
        self._schedule_reminder_check()
        if self.atlas_daily_lesson_fn:
            self._schedule_atlas_lesson()
        if self.config.get("weekly_planning", {}).get("enabled", False):
            self._schedule_weekly_planning()
        if self.db_path and self.venture_config.get("proactive_research_domains"):
            self._schedule_venture_proactive()
        if self.db_path and self.merlin_config.get("proactive_research", {}).get("enabled", False):
            self._schedule_merlin_proactive()
        if self.db_path and self.zuko_config.get("proactive_scans", {}).get("enabled", False):
            self._schedule_zuko_proactive()
        if self.ecosystem_council_fn and self.config.get("ecosystem_council", {}).get("enabled", False):
            self._schedule_ecosystem_council()
        if self.config.get("approval_digest", {}).get("enabled", False) and self.db_path:
            self._schedule_approval_digest()
        if self.db_path and self.merlin_config.get("proactive_research", {}).get("forge_digest_enabled", True):
            self._schedule_merlin_forge_digest()
        if self.db_path and self.forge_config.get("watchdog", {}).get("enabled", True):
            self._schedule_forge_watchdog()
        if self.db_path and self.observability_config.get("agent_watchdog_enabled", True):
            self._schedule_agent_watchdog()
        self._scheduler.start()
        if self.db_path and self.venture_config.get("proactive_research_domains") and self.venture_config.get("enqueue_on_startup", True):
            asyncio.create_task(self._run_venture_proactive())
        if self.db_path and self.merlin_config.get("proactive_research", {}).get("enabled", False) and self.merlin_config.get("proactive_research", {}).get("enqueue_on_startup", True):
            asyncio.create_task(self._run_merlin_proactive())
        if self.db_path and self.zuko_config.get("proactive_scans", {}).get("enabled", False) and self.zuko_config.get("proactive_scans", {}).get("enqueue_on_startup", True):
            asyncio.create_task(self._run_zuko_proactive())
        logger.info(
            "Scheduler started (tz=%s, daily_briefing=%s, reminder_check=%smin)",
            self.config.get("timezone"),
            self.config.get("daily_briefing"),
            self.config.get("reminder_check_interval_minutes"),
        )

    def stop(self) -> None:
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)

    def _schedule_daily_briefing(self) -> None:
        time_str = self.config.get("daily_briefing", "08:00")
        h, m = time_str.split(":")
        self._scheduler.add_job(
            self._run_daily_briefing,
            trigger="cron",
            hour=int(h),
            minute=int(m),
            id="daily_briefing",
            replace_existing=True,
        )
        logger.info("Daily briefing scheduled at %s %s", time_str, self.config.get("timezone"))

    def _schedule_atlas_lesson(self) -> None:
        time_str = self.atlas_config.get(
            "daily_lesson_time",
            self.config.get("atlas_daily_lesson", "09:00"),
        )
        h, m = time_str.split(":")
        self._scheduler.add_job(
            self._run_atlas_lesson,
            trigger="cron",
            hour=int(h),
            minute=int(m),
            id="atlas_daily_lesson",
            replace_existing=True,
        )
        logger.info("Atlas daily lesson scheduled at %s %s", time_str, self.config.get("timezone"))

    def _schedule_reminder_check(self) -> None:
        interval = int(self.config.get("reminder_check_interval_minutes", 15))
        self._scheduler.add_job(
            self._run_reminder_check,
            trigger="interval",
            minutes=interval,
            id="reminder_check",
            replace_existing=True,
        )

    def _schedule_weekly_planning(self) -> None:
        wp = self.config.get("weekly_planning", {})
        time_str = wp.get("time", "19:00")
        day = self._normalize_day(wp.get("day_of_week", wp.get("day", "sun")))
        h, m = time_str.split(":")
        self._scheduler.add_job(
            self._run_weekly_planning,
            trigger="cron",
            day_of_week=day,
            hour=int(h),
            minute=int(m),
            id="weekly_planning",
            replace_existing=True,
        )
        logger.info("Weekly planning scheduled at %s %s on %s", time_str, self.config.get("timezone"), day)

    async def _run_daily_briefing(self) -> None:
        logger.info("Running scheduled daily briefing")
        try:
            text = await self.briefing_fn()
            await self.send_fn(text)
        except Exception as e:
            logger.error("Daily briefing failed: %s", e, exc_info=True)

    async def _run_atlas_lesson(self) -> None:
        logger.info("Running Atlas daily lesson delivery")
        try:
            await self.atlas_daily_lesson_fn()
        except Exception as e:
            logger.error("Atlas daily lesson failed: %s", e, exc_info=True)

    async def _run_reminder_check(self) -> None:
        if not self.db_path:
            return
        from shared.db.reminders import get_due_reminders, mark_done

        now_utc = datetime.now(timezone.utc).isoformat()
        logger.debug("Reminder check tick at %s", now_utc)
        try:
            due = get_due_reminders(self.db_path, as_of=now_utc)
        except Exception as e:
            logger.error("Reminder check DB error: %s", e)
            return

        for reminder in due:
            category = f" [{reminder.category}]" if reminder.category else ""
            text = f"⏰ <b>Reminder{category}</b>\n{reminder.text}"
            try:
                await self.send_fn(text)
                mark_done(self.db_path, reminder.id)
                logger.info("Sent and marked done reminder id=%s", reminder.id)
            except Exception as e:
                logger.error("Failed to send reminder id=%s: %s", reminder.id, e)

    async def _run_weekly_planning(self) -> None:
        logger.info("Running weekly planning digest")
        try:
            text = await self.briefing_fn()
            opp_section = ""
            if self.owner_memory:
                recent = self.owner_memory.get_recent_opportunities(days=7)
                if recent:
                    lines = ["\n\n<b>ðŸ’¡ Opportunities this week:</b>"]
                    for opp in recent[:5]:
                        cap = f” — ${opp['capital']:,.0f}” if opp.get(“capital”) else “”
                        risk = f" [{opp['risk']}]" if opp.get("risk") else ""
                        lines.append(f"  â€¢ {opp['title'][:70]}{cap}{risk}")
                    opp_section = "\n".join(lines)
            await self.send_fn(f"ðŸ“… <b>Weekly Planning</b>\n\n{text}{opp_section}")
        except Exception as e:
            logger.error("Weekly planning failed: %s", e, exc_info=True)

    def _schedule_venture_proactive(self) -> None:
        self._scheduler.add_job(
            self._run_venture_proactive,
            trigger="interval",
            hours=6,
            id="venture_proactive",
            replace_existing=True,
        )
        logger.info("Venture proactive research scheduled every 6h")

    def _schedule_merlin_proactive(self) -> None:
        cfg = self.merlin_config.get("proactive_research", {})
        hours = max(1, int(cfg.get("interval_hours", 2)))
        self._scheduler.add_job(
            self._run_merlin_proactive,
            trigger="interval",
            hours=hours,
            id="merlin_proactive",
            replace_existing=True,
        )
        logger.info("Merlin proactive research scheduled every %sh", hours)

    def _schedule_zuko_proactive(self) -> None:
        cfg = self.zuko_config.get("proactive_scans", {})
        hours = max(1, int(cfg.get("interval_hours", 3)))
        self._scheduler.add_job(
            self._run_zuko_proactive,
            trigger="interval",
            hours=hours,
            id="zuko_proactive",
            replace_existing=True,
        )
        logger.info("Zuko proactive LinkedIn scan scheduled every %sh", hours)

    def _schedule_ecosystem_council(self) -> None:
        council = self.config.get("ecosystem_council", {})
        hours = int(council.get("interval_hours", 12))
        self._scheduler.add_job(
            self._run_ecosystem_council,
            trigger="interval",
            hours=hours,
            id="ecosystem_council",
            replace_existing=True,
        )
        logger.info("Ecosystem council scheduled every %sh", hours)

    def _schedule_approval_digest(self) -> None:
        digest = self.config.get("approval_digest", {})
        times = set(digest.get("first_week_times", ["09:00", "14:00", "20:00"]))
        times.update(digest.get("normal_times", ["18:00"]))
        for time_str in sorted(times):
            h, m = time_str.split(":")
            self._scheduler.add_job(
                self._run_approval_digest,
                trigger="cron",
                hour=int(h),
                minute=int(m),
                id=f"approval_digest_{h}_{m}",
                replace_existing=True,
            )
        logger.info(
            "Approval digest scheduled: first week %s; normal %s",
            ", ".join(digest.get("first_week_times", ["09:00", "14:00", "20:00"])),
            ", ".join(digest.get("normal_times", ["18:00"])),
        )

    def _schedule_merlin_forge_digest(self) -> None:
        cfg = self.merlin_config.get("proactive_research", {})
        time_str = cfg.get("forge_digest_time", "16:30")
        h, m = time_str.split(":")
        self._scheduler.add_job(
            self._run_merlin_forge_digest,
            trigger="cron",
            hour=int(h),
            minute=int(m),
            id="merlin_forge_digest",
            replace_existing=True,
        )
        logger.info("Merlin Forge consolidation digest scheduled at %s", time_str)

    def _schedule_forge_watchdog(self) -> None:
        cfg = self.forge_config.get("watchdog", {})
        minutes = max(5, int(cfg.get("interval_minutes", 10)))
        self._scheduler.add_job(
            self._run_forge_watchdog,
            trigger="interval",
            minutes=minutes,
            id="forge_watchdog",
            replace_existing=True,
        )
        logger.info("Forge watchdog scheduled every %smin", minutes)

    def _schedule_agent_watchdog(self) -> None:
        cfg = self.config.get("agent_watchdog", {})
        minutes = max(5, int(cfg.get("interval_minutes", 10)))
        self._scheduler.add_job(
            self._run_agent_watchdog,
            trigger="interval",
            minutes=minutes,
            id="agent_watchdog",
            replace_existing=True,
        )
        logger.info("Agent watchdog scheduled every %smin", minutes)

    async def _run_venture_proactive(self) -> None:
        from shared.db.tasks import enqueue_task, list_tasks
        from shared.schemas.task import Task

        domains = self.venture_config.get("proactive_research_domains", [])
        if not domains:
            return
        max_open = int(self.venture_config.get("max_open_tasks", 2))
        open_tasks = [
            task for task in list_tasks(self.db_path, to_agent="venture", limit=200)
            if task.status in {"pending", "in_progress", "approved"}
        ]
        if len(open_tasks) >= max_open:
            logger.info("Skipping Venture proactive research: %s open Venture tasks", len(open_tasks))
            return

        domain = domains[self._venture_domain_index % len(domains)]
        self._venture_domain_index += 1
        if self._has_similar_recent_task("venture", "opportunity_research", domain, window_hours=12):
            logger.info("Skipping Venture proactive research: similar recent task already exists for '%s'", domain)
            return

        logger.info("Venture proactive research: %s", domain)
        try:
            enqueue_task(
                self.db_path,
                Task(
                    to_agent="venture",
                    task_type="opportunity_research",
                    description=domain,
                    approval_required=False,
                ),
            )
        except Exception as e:
            logger.error("Failed to enqueue venture proactive task: %s", e)

    async def _run_merlin_proactive(self) -> None:
        from shared.db.tasks import enqueue_task, list_tasks
        from shared.schemas.task import Task

        cfg = self.merlin_config.get("proactive_research", {})
        topics = cfg.get("topics", [])
        if not topics:
            return

        max_open = int(cfg.get("max_open_tasks", 2))
        open_tasks = [
            task for task in list_tasks(self.db_path, to_agent="merlin", limit=200)
            if task.status in {"pending", "in_progress", "approved"}
        ]
        if len(open_tasks) >= max_open:
            logger.info("Skipping Merlin proactive research: %s open Merlin tasks", len(open_tasks))
            return

        topic = topics[self._merlin_topic_index % len(topics)]
        self._merlin_topic_index += 1
        if self._has_similar_recent_task("merlin", "system_research", topic, window_hours=12):
            logger.info("Skipping Merlin proactive research: similar recent task already exists for '%s'", topic[:100])
            return
        logger.info("Merlin proactive research: %s", topic[:100])
        try:
            enqueue_task(
                self.db_path,
                Task(
                    to_agent="merlin",
                    from_agent="scheduler",
                    task_type="system_research",
                    description=topic,
                    priority="normal",
                    urgency="today",
                    domain="operations",
                    approval_required=False,
                ),
            )
        except Exception as e:
            logger.error("Failed to enqueue Merlin proactive task: %s", e)

    async def _run_zuko_proactive(self) -> None:
        from shared.db.tasks import enqueue_task, list_tasks
        from shared.schemas.task import Task

        cfg = self.zuko_config.get("proactive_scans", {})
        topics = cfg.get("scan_topics", [])
        if not topics:
            return

        max_open = int(cfg.get("max_open_tasks", 1))
        open_tasks = [
            task for task in list_tasks(self.db_path, to_agent="zuko", limit=200)
            if task.status in {"pending", "in_progress", "approved"}
        ]
        if len(open_tasks) >= max_open:
            logger.info("Skipping Zuko proactive scan: %s open Zuko tasks", len(open_tasks))
            return

        topic = topics[self._zuko_scan_index % len(topics)]
        self._zuko_scan_index += 1
        if self._has_similar_recent_task("zuko", "job_search", topic, window_hours=8):
            logger.info("Skipping Zuko proactive scan: similar recent task already exists for '%s'", topic[:100])
            return
        logger.info("Zuko proactive scan: %s", topic[:100])
        try:
            enqueue_task(
                self.db_path,
                Task(
                    to_agent="zuko",
                    from_agent="scheduler",
                    task_type="job_search",
                    description=topic,
                    priority="normal",
                    urgency="today",
                    domain="career",
                    approval_required=False,
                    payload={"source_preference": "linkedin_posts_first", "scan_mode": "proactive"},
                ),
            )
        except Exception as e:
            logger.error("Failed to enqueue Zuko proactive scan: %s", e)

    async def _run_ecosystem_council(self) -> None:
        logger.info("Running ecosystem council")
        try:
            text = await self.ecosystem_council_fn()
            await self.send_fn(text)
        except Exception as e:
            logger.error("Ecosystem council failed: %s", e, exc_info=True)

    async def _run_approval_digest(self) -> None:
        digest = self.config.get("approval_digest", {})
        if not self._approval_digest_due_now(digest):
            return
        try:
            from shared.db.approvals import list_pending_approvals

            pending = list_pending_approvals(self.db_path)
            if not pending:
                logger.info("Approval digest skipped: no pending approvals")
                return

            grouped: dict[str, list] = {}
            for approval in pending:
                grouped.setdefault(approval.request_type, []).append(approval)

            lines = [
                "<b>Approval Digest</b>",
                "",
                f"{len(pending)} pending approval request(s). Review them in the dashboard Approvals tab.",
                "",
            ]
            for request_type, approvals in sorted(grouped.items()):
                lines.append(f"<b>{request_type.replace('_', ' ').title()}</b>")
                for approval in approvals[:8]:
                    desc = " ".join(approval.description.split())
                    lines.append(f"- #{approval.id} / task #{approval.task_id}: {desc[:180]}")
                if len(approvals) > 8:
                    lines.append(f"- ...and {len(approvals) - 8} more")
                lines.append("")

            lines.append("Merlin can keep researching; Roderick will batch routine Forge proposals instead of interrupting immediately.")
            await self.send_fn("\n".join(lines))
            logger.info("Sent approval digest with %d pending approvals", len(pending))
        except Exception as e:
            logger.error("Approval digest failed: %s", e, exc_info=True)

    async def _run_forge_watchdog(self) -> None:
        """Coordinate Roderick + Sentinel when Forge work appears stale."""
        if not self.db_path:
            return
        try:
            from shared.db.events import emit_event
            from shared.db.tasks import enqueue_task, list_tasks, update_task_status
            from shared.schemas.task import Task

            cfg = self.forge_config.get("watchdog", {})
            stuck_minutes = max(10, int(cfg.get("stuck_minutes", 45)))
            requeue_minutes = max(stuck_minutes, int(cfg.get("auto_requeue_minutes", 90)))
            max_requeues = max(0, int(cfg.get("max_auto_requeues", 1)))
            now = datetime.now(timezone.utc)

            forge_tasks = [
                task for task in list_tasks(self.db_path, to_agent="forge", limit=80)
                if task.status in {"in_progress", "awaiting_validation"}
            ]
            sentinel_open = [
                task for task in list_tasks(self.db_path, to_agent="sentinel", limit=80)
                if task.status in {"pending", "in_progress", "approved"}
            ]
            for task in forge_tasks:
                updated = self._parse_utc(task.updated_at or task.created_at)
                if not updated:
                    continue
                age_min = int((now - updated).total_seconds() / 60)
                if age_min < stuck_minutes:
                    continue

                emit_event(self.db_path, "forge_watchdog_stuck_detected", "roderick", {
                    "forge_task_id": task.id,
                    "status": task.status,
                    "age_minutes": age_min,
                    "threshold_minutes": stuck_minutes,
                })

                has_sentinel_check = any(
                    st.payload.get("source") == "forge_watchdog"
                    and st.payload.get("forge_task_id") == task.id
                    for st in sentinel_open
                )
                if not has_sentinel_check:
                    sentinel_task = enqueue_task(
                        self.db_path,
                        Task(
                            from_agent="roderick",
                            to_agent="sentinel",
                            task_type="health_check",
                            description=(
                                f"Forge watchdog: inspect task #{task.id}, status {task.status}, "
                                f"stale for about {age_min} minutes. Decide whether it is safe to requeue, "
                                "cancel, or leave running."
                            ),
                            status="pending",
                            priority="high",
                            urgency="today",
                            domain="operations",
                            payload={
                                "source": "forge_watchdog",
                                "forge_task_id": task.id,
                                "forge_status": task.status,
                                "age_minutes": age_min,
                            },
                        ),
                    )
                    sentinel_open.append(sentinel_task)
                    emit_event(self.db_path, "forge_watchdog_sentinel_check_queued", "roderick", {
                        "forge_task_id": task.id,
                        "sentinel_task_id": sentinel_task.id,
                        "age_minutes": age_min,
                    })
                    logger.warning(
                        "Forge watchdog queued Sentinel task #%s for stale Forge task #%s (%s min)",
                        sentinel_task.id,
                        task.id,
                        age_min,
                    )

                requeues = int(task.payload.get("watchdog_requeues", 0) or 0)
                if task.status == "in_progress" and age_min >= requeue_minutes and requeues < max_requeues:
                    new_payload = dict(task.payload)
                    new_payload["watchdog_requeues"] = requeues + 1
                    new_payload["last_watchdog_requeue_at"] = now.isoformat()
                    new_payload["last_watchdog_requeue_reason"] = (
                        f"Forge task stale for {age_min} minutes; Roderick requeued once and asked Sentinel to inspect."
                    )
                    update_task_status(
                        self.db_path,
                        task.id,
                        "pending",
                        {
                            "previous_status": task.status,
                            "watchdog_requeued": True,
                            "watchdog_requeues": requeues + 1,
                            "reason": new_payload["last_watchdog_requeue_reason"],
                            "payload": new_payload,
                        },
                    )
                    # update_task_status stores result, not payload; also persist the payload change directly.
                    self._update_task_payload(task.id, new_payload)
                    emit_event(self.db_path, "forge_watchdog_task_requeued", "roderick", {
                        "forge_task_id": task.id,
                        "age_minutes": age_min,
                        "watchdog_requeues": requeues + 1,
                    })
                    logger.warning("Forge watchdog requeued stale Forge task #%s after %s min", task.id, age_min)
                    if self.send_fn:
                        await self.send_fn(
                            "<b>Forge watchdog intervened.</b>\n\n"
                            f"Task #{task.id} was stale for about {age_min} minutes. "
                            "Roderick requeued it once and asked Sentinel to inspect before further action."
                        )
        except Exception as e:
            logger.error("Forge watchdog failed: %s", e, exc_info=True)

    async def _run_agent_watchdog(self) -> None:
        """Recover stale non-Forge agent tasks once periodic keepalives stop moving."""
        if not self.db_path:
            return
        try:
            from shared.db.events import emit_event
            from shared.db.tasks import list_tasks, update_task_status

            cfg = self.config.get("agent_watchdog", {})
            minutes_by_agent = cfg.get(
                "stale_minutes_by_agent",
                {
                    "merlin": 30,
                    "venture": 30,
                    "zuko": 30,
                    "sentinel": 30,
                    "atlas": 30,
                },
            )
            max_requeues = max(0, int(cfg.get("max_auto_requeues", 1)))
            now = datetime.now(timezone.utc)

            for agent_name, stale_minutes in minutes_by_agent.items():
                open_tasks = [
                    task for task in list_tasks(self.db_path, to_agent=agent_name, limit=80)
                    if task.status == "in_progress"
                ]
                for task in open_tasks:
                    updated = self._parse_utc(task.updated_at or task.created_at)
                    if not updated:
                        continue
                    age_min = int((now - updated).total_seconds() / 60)
                    if age_min < int(stale_minutes):
                        continue

                    requeues = int(task.payload.get("watchdog_requeues", 0) or 0)
                    if requeues >= max_requeues:
                        continue

                    new_payload = dict(task.payload)
                    new_payload["watchdog_requeues"] = requeues + 1
                    new_payload["last_watchdog_requeue_at"] = now.isoformat()
                    new_payload["last_watchdog_requeue_reason"] = (
                        f"{agent_name} task stale for {age_min} minutes with no keepalive; requeued by agent watchdog."
                    )
                    update_task_status(
                        self.db_path,
                        task.id,
                        "pending",
                        {
                            "previous_status": task.status,
                            "watchdog_requeued": True,
                            "watchdog_requeues": requeues + 1,
                            "reason": new_payload["last_watchdog_requeue_reason"],
                            "payload": new_payload,
                        },
                    )
                    self._update_task_payload(task.id, new_payload)
                    emit_event(self.db_path, "agent_watchdog_task_requeued", "roderick", {
                        "task_id": task.id,
                        "agent_name": agent_name,
                        "age_minutes": age_min,
                        "watchdog_requeues": requeues + 1,
                    })
                    logger.warning(
                        "Agent watchdog requeued stale %s task #%s after %s min",
                        agent_name,
                        task.id,
                        age_min,
                    )
        except Exception as e:
            logger.error("Agent watchdog failed: %s", e, exc_info=True)

    async def _run_merlin_forge_digest(self) -> None:
        """Create at most one daily Forge proposal from Merlin's accumulated findings."""
        try:
            from shared.db.approvals import create_approval
            from shared.db.events import emit_event
            from shared.db.improvements import advance_improvement, list_improvements
            from shared.db.tasks import enqueue_task, list_tasks
            from shared.schemas.approval import ApprovalRequest
            from shared.schemas.task import Task

            open_consolidated = [
                task for task in list_tasks(self.db_path, to_agent="forge", limit=80)
                if task.task_type == "system_improvement"
                and task.status in {"pending", "approved", "plan_ready", "plan_approved", "in_progress", "awaiting_validation"}
                and task.payload.get("source") in {"merlin_daily_consolidation", "merlin_urgent_escalation"}
            ]
            if open_consolidated:
                logger.info(
                    "Merlin Forge digest skipped: Merlin-linked Forge task #%s is still %s",
                    open_consolidated[0].id,
                    open_consolidated[0].status,
                )
                return

            cfg = self.merlin_config.get("proactive_research", {})
            max_items = max(1, int(cfg.get("forge_digest_max_items", 8)))
            candidates = [
                imp for imp in list_improvements(self.db_path, status="proposed", limit=80)
                if imp.origin_agent == "merlin" and imp.forge_recommended and not imp.forge_task_id
            ][:max_items]
            if not candidates:
                logger.info("Merlin Forge digest skipped: no unlinked Merlin improvements")
                return

            priority_rank = {"critical": 3, "high": 2, "normal": 1, "low": 0}
            priority = max((imp.priority for imp in candidates), key=lambda p: priority_rank.get(p, 1))
            description_lines = [
                "Daily consolidated Merlin system-improvement package.",
                "",
                "Forge should inspect these related Merlin findings and produce one coherent plan instead of many small overlapping changes.",
                "",
                "Included improvement candidates:",
            ]
            for imp in candidates:
                description_lines.append(f"- Improvement #{imp.id}: {imp.title[:180]}")
            description_lines.extend([
                "",
                "Preserve approval boundaries: this is only the first approval to let Forge create a plan; implementation still requires plan approval and Sentinel validation.",
            ])

            payload = {
                "source": "merlin_daily_consolidation",
                "improvement_ids": [imp.id for imp in candidates],
                "merlin_task_ids": [imp.merlin_task_id for imp in candidates if imp.merlin_task_id],
                "verified_facts": [
                    fact
                    for imp in candidates
                    for fact in (imp.evidence.get("verified_facts", []) if isinstance(imp.evidence, dict) else [])
                ][:12],
                "unknowns": [
                    unknown
                    for imp in candidates
                    for unknown in (imp.evidence.get("unknowns", []) if isinstance(imp.evidence, dict) else [])
                ][:8],
                "affected_components": sorted({component for imp in candidates for component in imp.affected_components})[:15],
                "recommended_actions": [
                    action
                    for imp in candidates
                    for action in (imp.evidence.get("recommended_actions", []) if isinstance(imp.evidence, dict) else [])
                ][:15],
                "risk_level": "high" if any(imp.risk_level == "high" for imp in candidates) else "medium",
                "forge_scope": "consolidated_daily_batch",
            }
            forge_task = enqueue_task(
                self.db_path,
                Task(
                    to_agent="forge",
                    from_agent="roderick",
                    task_type="system_improvement",
                    description="\n".join(description_lines),
                    status="pending",
                    priority=priority,
                    urgency="today" if priority in {"high", "critical"} else "this_week",
                    domain="operations",
                    payload=payload,
                    approval_required=True,
                ),
            )
            for imp in candidates:
                advance_improvement(
                    self.db_path,
                    imp.id,
                    "proposed",
                    evidence_update={"daily_consolidated_forge_task_id": forge_task.id},
                    forge_task_id=forge_task.id,
                )
            create_approval(
                self.db_path,
                ApprovalRequest(
                    request_type="task_approval",
                    description=(
                        f"Merlin daily consolidated Forge request: task #{forge_task.id}\n\n"
                        f"This bundles {len(candidates)} Merlin improvement candidate(s) into one Forge planning task. "
                        "Approve to let Forge create a single coherent plan. Implementation still requires plan approval."
                    ),
                    task_id=forge_task.id,
                    payload={"improvement_ids": [imp.id for imp in candidates]},
                ),
            )
            emit_event(self.db_path, "merlin_daily_forge_digest_created", "roderick", {
                "forge_task_id": forge_task.id,
                "improvement_ids": [imp.id for imp in candidates],
            })
            await self.send_fn(
                "<b>Merlin daily Forge package ready.</b>\n\n"
                f"Bundled {len(candidates)} Merlin finding(s) into Forge task #{forge_task.id}. "
                "Review it from the dashboard Approvals tab."
            )
            logger.info("Merlin Forge digest created consolidated task #%s from %d improvements", forge_task.id, len(candidates))
        except Exception as e:
            logger.error("Merlin Forge digest failed: %s", e, exc_info=True)

    def _approval_digest_due_now(self, digest: dict) -> bool:
        now = datetime.now(self.tz)
        now_hm = now.strftime("%H:%M")
        start_raw = digest.get("first_week_start_date")
        days = int(digest.get("first_week_days", 7))
        first_week = False
        if start_raw:
            try:
                start = datetime.fromisoformat(start_raw).date()
                first_week = now.date() < start + timedelta(days=days)
            except ValueError:
                first_week = True
        allowed = digest.get("first_week_times" if first_week else "normal_times", ["18:00"])
        return now_hm in set(allowed)

    def _parse_utc(self, value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            return None

    def _has_similar_recent_task(self, agent: str, task_type: str, description: str, *, window_hours: int) -> bool:
        from shared.db.tasks import list_tasks

        recent = list_tasks(self.db_path, to_agent=agent, limit=40)
        normalized = " ".join((description or "").lower().split())
        now = datetime.now(timezone.utc)
        for task in recent:
            if task.task_type != task_type:
                continue
            task_desc = " ".join((task.description or "").lower().split())
            if task_desc != normalized:
                continue
            created = self._parse_utc(task.created_at or task.updated_at)
            if not created:
                continue
            age_hours = (now - created).total_seconds() / 3600
            if age_hours <= window_hours and task.status in {
                "pending", "approved", "plan_ready", "plan_approved", "in_progress", "awaiting_validation", "completed"
            }:
                return True
        return False

    def _update_task_payload(self, task_id: int, payload: dict) -> None:
        import sqlite3

        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("UPDATE tasks SET payload=? WHERE id=?", (json.dumps(payload), task_id))
            conn.commit()
        finally:
            conn.close()

    def _normalize_day(self, day: str) -> str:
        aliases = {
            "monday": "mon",
            "tuesday": "tue",
            "wednesday": "wed",
            "thursday": "thu",
            "friday": "fri",
            "saturday": "sat",
            "sunday": "sun",
        }
        day = str(day).strip().lower()
        return aliases.get(day, day)
