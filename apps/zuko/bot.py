"""
Dedicated Telegram chat handlers for Zuko.

Handles:
  - Commands: /start, /status, /scan, /quietscan, /last, /setup
  - Job action callbacks: apply, skip, full, manual
  - Pre-submission review callbacks: preapprove, prereject
  - LinkedIn feed callbacks: email_apply, email_skip, email_confirm, email_cancel
"""
import asyncio
import logging
from html import escape as _h
from pathlib import Path

from telegram import Update
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from apps.roderick.bot.formatter import escape, split_message
from shared.schemas.task import Task

logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _reply_html(update: Update, text: str) -> None:
    for chunk in split_message(text):
        try:
            await update.message.reply_text(chunk, parse_mode="HTML")
        except BadRequest as exc:
            if "parse" not in str(exc).lower():
                raise
            await update.message.reply_text(escape(chunk), parse_mode="HTML")


def _authorized(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    zuko_chat_id = context.bot_data.get("zuko_chat_id")
    if update.effective_chat and update.effective_chat.id == zuko_chat_id:
        return True
    logger.warning(
        "Unauthorized Zuko chat message from chat_id=%s",
        update.effective_chat.id if update.effective_chat else None,
    )
    return False


def _enqueue_scan(context: ContextTypes.DEFAULT_TYPE, criteria: str, *, notify_user: bool = True) -> int:
    enqueue_task = context.bot_data["enqueue_task"]
    db_path      = context.bot_data["db_path"]
    task = enqueue_task(
        db_path,
        Task(
            from_agent="zuko_chat",
            to_agent="zuko",
            task_type="job_search",
            description=criteria,
            status="pending",
            priority="high" if notify_user else "normal",
            urgency="today",
            domain="career",
            payload={"criteria": criteria, "notify_user": notify_user, "source": "zuko_chat"},
            approval_required=False,
        ),
    )
    return task.id


# ── Command handlers ──────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    await _reply_html(
        update,
        "<b>Zuko online.</b>\n\n"
        "I scan job boards for roles matching your configured targets, generate cover letters, "
        "and drive applications with your approval.\n\n"
        "<b>Commands:</b>\n"
        "/scan &lt;criteria&gt; — run a scan and surface jobs here\n"
        "/quietscan &lt;criteria&gt; — scan without immediate delivery\n"
        "/status — current Zuko state\n"
        "/last — show last report summary\n"
        "/setup — instructions to set up SEEK + LinkedIn sessions",
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    get_agent = context.bot_data["get_agent"]
    agent = get_agent(context.bot_data["db_path"], "zuko")
    if not agent:
        await _reply_html(update, "Zuko is not registered yet.")
        return
    status = (
        "<b>Zuko status</b>\n"
        f"State: {escape(agent.status)}\n"
        f"Model: {escape(agent.current_model or agent.model_used or 'unknown')}\n"
        f"Last success: {escape(agent.last_success or 'unknown')}\n"
        f"Last error: {escape(agent.last_error or 'none')}\n"
        f"Current task: {escape(str(agent.current_task_id or 'none'))}"
    )
    await _reply_html(update, status)


async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    criteria = " ".join(context.args).strip()
    if not criteria:
        await _reply_html(update, "Usage: <code>/scan DevOps engineer [location]</code>")
        return
    task_id = _enqueue_scan(context, criteria, notify_user=True)
    await _reply_html(update, f"<b>Queued.</b> Scan task #{task_id} — results will appear here as job cards.")


async def cmd_quietscan(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    criteria = " ".join(context.args).strip()
    if not criteria:
        await _reply_html(update, "Usage: <code>/quietscan DevOps engineer [location]</code>")
        return
    task_id = _enqueue_scan(context, criteria, notify_user=False)
    await _reply_html(update, f"<b>Queued quietly.</b> Scan task #{task_id} is in the dashboard.")


async def cmd_last(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    get_last = context.bot_data["get_last_report_summary"]
    summary = get_last()
    await _reply_html(update, summary or "No Zuko report available yet.")


async def cmd_setup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    await _reply_html(
        update,
        "<b>Session Setup</b>\n\n"
        "Zuko needs saved browser sessions to scrape SEEK and LinkedIn.\n\n"
        "On the host machine, run:\n"
        "<code>python apps/zuko/setup_sessions.py</code>\n\n"
        "This opens browsers for you to log in manually. Sessions are saved to "
        "<code>data/sessions/</code> and reused on future scans.\n\n"
        "Only needs to be done once (or when sessions expire).",
    )


# ── Message handler ───────────────────────────────────────────────────────────

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    text = (update.message.text or "").strip()
    if not text:
        return
    lowered = text.lower()
    criteria = text
    if lowered.startswith("scan "):
        criteria = text[5:].strip()
    elif lowered.startswith("/scan "):
        criteria = text[6:].strip()
    if not criteria:
        await _reply_html(update, "Try: <code>scan DevOps engineer</code>")
        return
    task_id = _enqueue_scan(context, criteria, notify_user=True)
    await _reply_html(
        update,
        f"<b>Queued.</b> Scan task #{task_id}:\n<code>{escape(criteria)}</code>",
    )


# ── Callback handler ──────────────────────────────────────────────────────────

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    try:
        await query.answer()
    except Exception:
        pass  # query too old after restart — safe to ignore

    data   = query.data or ""
    parts  = data.split(":", 1)
    action = parts[0]
    ref_id = parts[1] if len(parts) > 1 else ""

    db_path      = context.bot_data["db_path"]
    enqueue_task = context.bot_data["enqueue_task"]
    get_job      = context.bot_data["get_job"]
    update_status = context.bot_data["update_status"]

    # ── Pre-submission approval ───────────────────────────────────────────────
    if action == "preapprove":
        from apps.zuko.modules.approval_gate import resolve
        resolve(ref_id, "approve")
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await query.message.reply_text(
            f"✅ Approved — submitting application for <b>{_h(ref_id)}</b>...",
            parse_mode="HTML",
        )
        return

    if action == "prereject":
        from apps.zuko.modules.approval_gate import resolve
        resolve(ref_id, "reject")
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        update_status(ref_id, "skipped")
        await query.message.reply_text(
            f"❌ Rejected — application for <b>{_h(ref_id)}</b> cancelled.",
            parse_mode="HTML",
        )
        return

    # ── Feed post actions ─────────────────────────────────────────────────────
    if action == "email_skip":
        update_status(ref_id, "skipped")
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await query.message.reply_text("❌ Feed post skipped.")
        return

    if action == "email_apply":
        post = get_job(ref_id)
        if not post:
            await query.message.reply_text("⚠️ Post not found.")
            return
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await context.bot.send_message(
            context.bot_data["zuko_chat_id"],
            f"📧 Generating cover letter for <b>{_h(post.get('title', ''))}</b>...",
            parse_mode="HTML",
        )
        enqueue_task(db_path, Task(
            from_agent="zuko_chat",
            to_agent="zuko",
            task_type="application_prep",
            description=f"Email cover letter: {post.get('title', 'LinkedIn post')} at {post.get('company', '')}",
            status="pending",
            priority="high",
            urgency="today",
            domain="career",
            payload={"job_id": ref_id, "cover_letter_only": True, "notify_user": True},
            approval_required=False,
        ))
        return

    if action in ("email_confirm", "email_cancel"):
        # Handled elsewhere if gmail_sender is wired in; skip gracefully
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await query.message.reply_text(
            "✅ Email action noted." if action == "email_confirm" else "❌ Email cancelled."
        )
        return

    # ── Standard job card actions ─────────────────────────────────────────────
    job = get_job(ref_id)

    if not job:
        await query.message.reply_text("⚠️ Job not found in database.")
        return

    title   = _h(job.get("title", job.get("role", "Role")))
    company = _h(job.get("company", "Unknown"))

    if action == "skip":
        update_status(ref_id, "skipped")
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await query.message.reply_text(f"❌ Skipped: {title} @ {company}")

    elif action == "full":
        cl = job.get("cover_letter", "")
        if not cl:
            await query.message.reply_text("No cover letter saved for this job.")
            return
        for i in range(0, len(cl), 3800):
            await context.bot.send_message(
                context.bot_data["zuko_chat_id"],
                f"📋 <b>Full Cover Letter — {title} @ {company}</b>\n\n{_h(cl[i:i+3800])}",
                parse_mode="HTML",
            )

    elif action == "manual":
        update_status(ref_id, "manual_review")
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        msg = (
            f"🔗 <b>Manual Apply</b>\n\n"
            f"<b>{title}</b> @ {company}\n"
            f'Apply here: <a href="{job.get("url", "")}">Open Job</a>\n\n'
            f"Cover letter is saved — tap <b>📋 Full Letter</b> on the original card to retrieve it."
        )
        await context.bot.send_message(
            context.bot_data["zuko_chat_id"],
            msg,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        # Send CV if configured
        cv_path = context.bot_data.get("cv_path", "")
        if cv_path and Path(cv_path).exists():
            with open(cv_path, "rb") as f:
                await context.bot.send_document(
                    context.bot_data["zuko_chat_id"],
                    f,
                    filename=Path(cv_path).name,
                    caption=f"📎 CV for {title} @ {company}",
                )

    elif action == "apply":
        update_status(ref_id, "applying")
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        from apps.roderick.bot.formatter import escape as esc
        await context.bot.send_message(
            context.bot_data["zuko_chat_id"],
            f"🤖 <b>Starting application...</b>\n\n"
            f"<b>{title}</b> @ {company}\n\n"
            f"I'll fill the form and send you a <b>pre-submission review</b> before submitting anything.",
            parse_mode="HTML",
        )
        enqueue_task(db_path, Task(
            from_agent="zuko_chat",
            to_agent="zuko",
            task_type="application_prep",
            description=f"Apply: {job.get('title', '')} at {job.get('company', '')}",
            status="pending",
            priority="high",
            urgency="immediate",
            domain="career",
            payload={
                "job_id": ref_id,
                "start_playwright": True,
                "notify_user": True,
            },
            approval_required=False,
        ))

    else:
        await query.message.reply_text(f"Unknown action: {action}")


# ── Application builder ───────────────────────────────────────────────────────

def build_zuko_application(
    token: str,
    zuko_chat_id: int,
    *,
    enqueue_task,
    db_path: str,
    get_agent,
    get_last_report_summary,
    get_job,
    update_status,
    cv_path: str = "",
) -> Application:
    app = Application.builder().token(token).build()
    app.bot_data["zuko_chat_id"]         = zuko_chat_id
    app.bot_data["enqueue_task"]         = enqueue_task
    app.bot_data["db_path"]              = db_path
    app.bot_data["get_agent"]            = get_agent
    app.bot_data["get_last_report_summary"] = get_last_report_summary
    app.bot_data["get_job"]              = get_job
    app.bot_data["update_status"]        = update_status
    app.bot_data["cv_path"]              = cv_path

    app.add_handler(CommandHandler("start",      cmd_start))
    app.add_handler(CommandHandler("status",     cmd_status))
    app.add_handler(CommandHandler("scan",       cmd_scan))
    app.add_handler(CommandHandler("dashscan",   cmd_scan))
    app.add_handler(CommandHandler("dash",       cmd_scan))
    app.add_handler(CommandHandler("quietscan",  cmd_quietscan))
    app.add_handler(CommandHandler("last",       cmd_last))
    app.add_handler(CommandHandler("setup",      cmd_setup))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    return app
