"""
Dedicated Telegram chat handlers for Operator.

Gives Operator its own private chat for initiative tracking, pending decisions,
execution updates, and business ops requests. All messages use HTML parse_mode.

Auth: only responds to OPERATOR_TELEGRAM_CHAT_ID.

Commands:
  /start       — overview and command list
  /status      — current Operator agent state
  /initiatives — list active initiatives from memory/initiatives.md
  /pending     — pending approvals and blocked tasks for Operator
  /blockers    — list current blockers across all initiatives
  /next        — list the most immediate next actions
"""
import logging

from telegram import Update
from telegram.error import BadRequest
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

from apps.roderick.bot.formatter import escape, split_message
from shared.schemas.task import Task

logger = logging.getLogger(__name__)


async def _reply_html(update: Update, text: str) -> None:
    for chunk in split_message(text):
        try:
            await update.message.reply_text(chunk, parse_mode="HTML")
        except BadRequest as exc:
            if "parse" not in str(exc).lower():
                raise
            await update.message.reply_text(escape(chunk), parse_mode="HTML")


def _authorized(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    operator_chat_id = context.bot_data["operator_chat_id"]
    if update.effective_chat and update.effective_chat.id == operator_chat_id:
        return True
    logger.warning(
        "Unauthorized Operator chat message from chat_id=%s",
        update.effective_chat.id if update.effective_chat else None,
    )
    return False


def _enqueue_operator_task(
    context: ContextTypes.DEFAULT_TYPE,
    task_type: str,
    description: str,
    *,
    initiative: str = "",
    notify_user: bool = True,
) -> int:
    enqueue_task = context.bot_data["enqueue_task"]
    db_path = context.bot_data["db_path"]
    task = enqueue_task(
        db_path,
        Task(
            from_agent="operator_chat",
            to_agent="operator",
            task_type=task_type,
            description=description,
            status="pending",
            priority="high" if notify_user else "normal",
            urgency="today",
            domain="business",
            payload={
                "initiative": initiative,
                "notify_user": notify_user,
                "source": "operator_chat",
            },
            approval_required=False,
        ),
    )
    return task.id


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    await _reply_html(
        update,
        "<b>Operator online.</b>\n\n"
        "I manage business initiatives and coordinate execution for the configured business ventures.\n\n"
        "<b>Commands:</b>\n"
        "/status — current Operator state\n"
        "/initiatives — list active initiatives\n"
        "/pending — pending decisions and blocked tasks\n"
        "/blockers — current blockers across all initiatives\n"
        "/next — most immediate next actions\n\n"
        "<i>Send any message to queue an initiative_execution task.</i>",
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    get_agent = context.bot_data["get_agent"]
    db_path = context.bot_data["db_path"]
    agent = get_agent(db_path, "operator")
    if not agent:
        await _reply_html(update, "Operator is not registered yet.")
        return
    lines = [
        "<b>Operator status</b>",
        f"State: {escape(agent.status)}",
        f"Model: {escape(agent.current_model or agent.model_used or 'unknown')}",
        f"Last success: {escape(agent.last_success or 'none')}",
        f"Last error: {escape(agent.last_error or 'none')}",
        f"Current task: {escape(str(agent.current_task_id or 'none'))}",
    ]
    await _reply_html(update, "\n".join(lines))


async def cmd_initiatives(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    get_initiatives_summary = context.bot_data.get("get_initiatives_summary")
    if get_initiatives_summary:
        summary = get_initiatives_summary()
    else:
        summary = "Initiatives summary not available — check memory/initiatives.md directly."
    await _reply_html(update, summary)


async def cmd_pending(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    get_pending_summary = context.bot_data.get("get_pending_summary")
    if get_pending_summary:
        summary = get_pending_summary()
    else:
        task_id = _enqueue_operator_task(
            context,
            "execution_followup",
            "List all pending decisions and blocked items across all active initiatives.",
            notify_user=True,
        )
        summary = f"Queued follow-up scan (Task #{task_id}) — result will arrive here."
    await _reply_html(update, summary)


async def cmd_blockers(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    task_id = _enqueue_operator_task(
        context,
        "execution_followup",
        "List and prioritise all current blockers across all active initiatives.",
        notify_user=True,
    )
    await _reply_html(update, f"<b>Queued.</b>\nOperator blocker scan task #{task_id} will report back here.")


async def cmd_next(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    task_id = _enqueue_operator_task(
        context,
        "milestone_review",
        "What are the most immediate next actions across all active initiatives? Prioritise by urgency and cash flow impact.",
        notify_user=True,
    )
    await _reply_html(update, f"<b>Queued.</b>\nOperator next-actions task #{task_id} will report back here.")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    text = (update.message.text or "").strip()
    if not text:
        return
    task_id = _enqueue_operator_task(
        context,
        "initiative_execution",
        text,
        notify_user=True,
    )
    await _reply_html(
        update,
        f"<b>Queued.</b>\n"
        f"Operator execution task #{task_id}:\n<code>{escape(text[:200])}</code>",
    )


def build_operator_application(
    token: str,
    operator_chat_id: int,
    *,
    enqueue_task,
    db_path: str,
    get_agent,
    get_initiatives_summary=None,
    get_pending_summary=None,
) -> Application:
    app = Application.builder().token(token).build()
    app.bot_data["operator_chat_id"] = operator_chat_id
    app.bot_data["enqueue_task"] = enqueue_task
    app.bot_data["db_path"] = db_path
    app.bot_data["get_agent"] = get_agent
    app.bot_data["get_initiatives_summary"] = get_initiatives_summary
    app.bot_data["get_pending_summary"] = get_pending_summary

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("initiatives", cmd_initiatives))
    app.add_handler(CommandHandler("pending", cmd_pending))
    app.add_handler(CommandHandler("blockers", cmd_blockers))
    app.add_handler(CommandHandler("next", cmd_next))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    return app
