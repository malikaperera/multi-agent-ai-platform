"""
Zuko standalone entrypoint.

Runs as its own container. Polls the shared SQLite task queue for job_search
and application_prep tasks. Uses a dedicated Telegram bot for job cards and
approval flows (requires ZUKO_TELEGRAM_BOT_TOKEN and ZUKO_TELEGRAM_CHAT_ID).
"""
import asyncio
import json
import logging
import os
import sys
from html import escape as html_escape
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from telegram import Bot

from apps.zuko.agent import ZukoAgent
from apps.zuko.bot import build_zuko_application
from shared.db.agents import get_agent
from shared.db.schema import get_db_path, init_db, seed_db_if_needed
from shared.db.tasks import enqueue_task
from shared.llm.factory import build_llm
from shared.memory.founder import OwnerMemory
from shared.utils.config import agents_config_path, load_config
from shared.utils.logging_setup import setup_logging

logger = logging.getLogger(__name__)

_DEFAULT_MODEL = "qwen3:14b"


def _zuko_model() -> str:
    try:
        data = json.loads(Path(str(agents_config_path())).read_text(encoding="utf-8"))
        for a in data.get("agents", []):
            if a["name"] == "zuko":
                return a.get("model_used", _DEFAULT_MODEL)
    except Exception:
        pass
    return _DEFAULT_MODEL


def _cv_path() -> str:
    """Resolve CV path from ZUKO_CV_PATH env or candidate_profile.json."""
    env_path = os.environ.get("ZUKO_CV_PATH", "").strip()
    if env_path and Path(env_path).exists():
        return env_path
    try:
        profile_path = Path(__file__).parent / "config" / "candidate_profile.json"
        profile = json.loads(profile_path.read_text(encoding="utf-8"))
        cv = profile.get("cv_path", "")
        if cv and Path(cv).exists():
            return cv
    except Exception:
        pass
    return ""


async def main() -> None:
    config = load_config()
    setup_logging(config["data_dir"])
    logger.info("Starting Zuko standalone…")

    db_path = get_db_path(config)
    seed_db_if_needed(db_path, os.environ.get("DB_SEED_PATH"))
    init_db(db_path)
    logger.info("DB ready: %s", db_path)

    llm = build_llm(config, model=_zuko_model())
    owner_memory = OwnerMemory(config.get("memory_dir", "memory"))

    zuko_bot_token = os.environ.get("ZUKO_TELEGRAM_BOT_TOKEN", "").strip()
    zuko_chat_id_raw = os.environ.get("ZUKO_TELEGRAM_CHAT_ID", "").strip()
    zuko_chat_id = int(zuko_chat_id_raw) if zuko_chat_id_raw else None
    dedicated_chat_enabled = bool(zuko_bot_token and zuko_chat_id is not None)
    config["zuko_dedicated_chat_enabled"] = dedicated_chat_enabled

    cv_path = _cv_path()

    # Build the agent
    zuko = ZukoAgent(llm, db_path, config["data_dir"], config, owner_memory)

    # Give the agent a direct Bot handle for job cards + photos
    if dedicated_chat_enabled:
        bot = Bot(token=zuko_bot_token)
        zuko.set_bot(bot, zuko_chat_id)
    else:
        logger.warning(
            "Zuko dedicated chat disabled: set ZUKO_TELEGRAM_BOT_TOKEN and ZUKO_TELEGRAM_CHAT_ID. "
            "Job cards will not be sent."
        )

    # Fallback text-only notify (for non-job-card messages)
    async def send_fn(text: str) -> None:
        if not dedicated_chat_enabled:
            return
        await bot.send_message(chat_id=zuko_chat_id, text=text, parse_mode="HTML")

    zuko.set_notify(send_fn)

    # Build and start the Zuko chat Application (handles commands + callbacks)
    zuko_chat_app = None
    if dedicated_chat_enabled:

        def _last_report_summary() -> str:
            reports_dir = Path(config["data_dir"]) / "reports"
            latest = sorted(reports_dir.glob("zuko_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
            if not latest:
                return ""
            try:
                data = json.loads(latest[0].read_text(encoding="utf-8"))
            except Exception as exc:
                return f"Last Zuko report could not be read: {exc}"
            lines = ["<b>Last Zuko report</b>"]
            summary = data.get("search_summary") or data.get("summary") or ""
            if summary:
                lines += ["", html_escape(str(summary)[:500])]
            listings = data.get("listings_of_interest") or []
            if listings:
                lines.append("\n<b>Shortlist</b>")
                for item in listings[:5]:
                    role    = html_escape(str(item.get("role", "")))
                    company = html_escape(str(item.get("company", "")))
                    source  = html_escape(str(item.get("source", "")))
                    lines.append(f"• {role} @ {company} [{source}]")
            return "\n".join(lines)[:1600] or str(data)[:800]

        zuko_chat_app = build_zuko_application(
            zuko_bot_token,
            zuko_chat_id,
            enqueue_task=enqueue_task,
            db_path=db_path,
            get_agent=get_agent,
            get_last_report_summary=_last_report_summary,
            get_job=zuko._job_get,
            update_status=zuko._job_update_status,
            cv_path=cv_path,
        )

        async def _start_zuko_chat() -> None:
            try:
                await zuko_chat_app.initialize()
                await zuko_chat_app.start()
                await zuko_chat_app.updater.start_polling(
                    allowed_updates=["message", "callback_query"]
                )
                logger.info("Zuko chat polling started (message + callback_query)")
            except Exception as exc:
                logger.error("Zuko chat polling failed: %s", exc, exc_info=True)

        asyncio.create_task(_start_zuko_chat())
    else:
        logger.warning("Zuko chat app not started — no dedicated bot configured.")

    logger.info("Zuko online — polling for tasks…")
    await zuko.run()


if __name__ == "__main__":
    asyncio.run(main())
