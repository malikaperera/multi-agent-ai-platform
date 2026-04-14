"""
Telegram bot handlers for Roderick.
"""
import logging

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

from apps.roderick.bot.approvals import handle_approval_callback, is_approval_callback
from apps.roderick.bot.formatter import escape, split_message

logger = logging.getLogger(__name__)


async def _reply_html(message, text: str) -> None:
    for chunk in split_message(text):
        try:
            await message.reply_text(chunk, parse_mode="HTML")
        except BadRequest as e:
            if "parse" not in str(e).lower():
                raise
            await message.reply_text(escape(chunk), parse_mode="HTML")


def _authorized(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    authorized_id = context.bot_data["config"]["authorized_chat_id"]
    return update.effective_chat.id == authorized_id


# â”€â”€ Commands â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    await update.message.reply_text(
        "<b>Roderick online.</b>\n\n"
        "I'm your personal assistant and agent orchestrator.\n\n"
        "<b>What I do:</b>\n"
        "â€¢ Life admin â€” reminders, notes, follow-ups\n"
        “â€¢ Work admin â€” task tracking, project notes, follow-ups\n”
        "â€¢ Research â€” delegate to Merlin for structured findings\n"
        "â€¢ Build â€” delegate to Forge (requires your approval)\n"
        "â€¢ Agent oversight â€” status of Merlin, Forge, Zuko\n"
        "â€¢ Morning briefings\n\n"
        "<b>Commands:</b> /brief  /clear  /agents  /pending\n\n"
        "Just talk to me.",
        parse_mode="HTML",
    )


async def cmd_clear(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    context.bot_data["orchestrator"].clear_history()
    await update.message.reply_text("Conversation history cleared.")


async def cmd_brief(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    response = await context.bot_data["orchestrator"].morning_briefing_text()
    await _reply_html(update.message, response)


async def cmd_agents(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    summary = context.bot_data["orchestrator"].registry.get_status_summary()
    await update.message.reply_text(summary, parse_mode="HTML")


async def cmd_pending(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    from shared.db.approvals import list_pending_approvals
    pending = list_pending_approvals(context.bot_data["db_path"])
    if not pending:
        await update.message.reply_text("No pending approvals.")
        return
    lines = [f"â€¢ [{a.request_type}] {a.description[:80]}" for a in pending]
    await update.message.reply_text(
        f"<b>{len(pending)} pending approval(s):</b>\n" + "\n".join(lines),
        parse_mode="HTML",
    )


async def cmd_council(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    response = await context.bot_data["orchestrator"].propose_ecosystem_improvement(
        context.bot,
        update.effective_chat.id,
    )
    await _reply_html(update.message, response)


# â”€â”€ Free-text message â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

_HANDLER_TIMEOUT = 35  # seconds before we give a truthful "still working" reply


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        logger.warning("Unauthorized message from chat_id=%s", update.effective_chat.id)
        return

    user_message = update.message.text
    logger.info("Message received: %s", user_message[:120])

    ack = await update.message.reply_text(
        "Received. I’m routing this now and will confirm the task/receipt shortly.",
        parse_mode=None,
    )

    async def _run_and_reply() -> None:
        import asyncio as _asyncio
        handle_task = _asyncio.create_task(
            context.bot_data["orchestrator"].handle(
                user_message,
                context.bot,
                update.effective_chat.id,
            )
        )
        try:
            done, _pending = await _asyncio.wait({handle_task}, timeout=_HANDLER_TIMEOUT)
            if not done:
                logger.warning("handle_message still running after %ds", _HANDLER_TIMEOUT)
                try:
                    await ack.edit_text(
                        "<b>Still working.</b> I do not have a final receipt yet. "
                        "The request is still running in Roderick; I’ll send the result when it finishes.",
                        parse_mode="HTML",
                    )
                except Exception:
                    pass
                response = await handle_task
            else:
                response = handle_task.result()
            logger.info("Sending response (%d chars)", len(response))
            try:
                await ack.delete()
            except Exception:
                pass
            await _reply_html(update.message, response)
        except Exception as e:
            logger.error("Error handling message: %s", e, exc_info=True)
            try:
                await ack.delete()
            except Exception:
                pass
            await update.message.reply_text(f"⚠️ Error: {escape(str(e))}", parse_mode="HTML")

    import asyncio as _asyncio
    _asyncio.create_task(_run_and_reply())


# â”€â”€ Approval callbacks â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as exc:
        if "query is too old" not in str(exc).lower() and "query id is invalid" not in str(exc).lower():
            raise
        logger.info("Ignoring stale callback acknowledgement: %s", exc)

    if not is_approval_callback(query.data):
        return

    outcome = await handle_approval_callback(
        db_path=context.bot_data["db_path"],
        bot=context.bot,
        chat_id=query.message.chat_id,
        callback_data=query.data,
        message_id=query.message.message_id,
    )
    logger.info("Approval callback %s â†’ %s", query.data[:40], outcome)


# â”€â”€ Application factory â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def build_application(config: dict, orchestrator, db_path: str) -> Application:
    app = Application.builder().token(config["telegram_bot_token"]).build()

    app.bot_data["orchestrator"] = orchestrator
    app.bot_data["db_path"] = db_path
    app.bot_data["config"] = config

    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("clear",   cmd_clear))
    app.add_handler(CommandHandler("brief",   cmd_brief))
    app.add_handler(CommandHandler("agents",  cmd_agents))
    app.add_handler(CommandHandler("pending", cmd_pending))
    app.add_handler(CommandHandler("council", cmd_council))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    return app

