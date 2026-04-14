"""
Dedicated Telegram chat handlers for Atlas.

This is intentionally small: Atlas remains a worker agent, and this adapter only
lets the owner steer study state from the Atlas bot/chat.
"""
import logging

from telegram import Update
from telegram.error import BadRequest
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

from apps.roderick.bot.formatter import escape, split_message

logger = logging.getLogger(__name__)


async def _reply_html(update: Update, text: str) -> None:
    for chunk in split_message(text):
        try:
            await update.message.reply_text(chunk, parse_mode="HTML")
        except BadRequest as e:
            if "parse" not in str(e).lower():
                raise
            await update.message.reply_text(escape(chunk), parse_mode="HTML")


def _authorized(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    atlas_chat_id = context.bot_data["atlas_chat_id"]
    if update.effective_chat and update.effective_chat.id == atlas_chat_id:
        return True
    logger.warning(
        "Unauthorized Atlas chat message from chat_id=%s",
        update.effective_chat.id if update.effective_chat else None,
    )
    return False


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    await _reply_html(
        update,
        "<b>Atlas online.</b>\n\n"
        "Tell me when you want to study or when you need to postpone.\n\n"
        "<b>Commands:</b>\n"
        "/study - start or resume today's lesson\n"
        "/today - show today's lesson\n"
        "/postpone - postpone today's lesson\n"
        "/skills - show skill states\n"
        "/status - show today's study status",
    )


async def cmd_skills(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    await _reply_html(update, context.bot_data["atlas"].get_skill_summary())


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    status = context.bot_data["atlas"].get_lesson_status()
    if not status:
        await _reply_html(update, "No Atlas study status is set for today yet.")
        return
    await _reply_html(
        update,
        "<b>Atlas study status</b>\n"
        f"Date: {escape(status.get('date', 'unknown'))}\n"
        f"Status: {escape(status.get('status', 'unknown'))}\n"
        f"Note: {escape(status.get('note', ''))}",
    )


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    atlas = context.bot_data["atlas"]
    lesson = atlas.get_today_lesson()
    if not lesson:
        await _reply_html(update, "No lesson exists for today yet. Send /study and I will create one.")
        return
    await _reply_html(update, atlas.format_lesson_telegram(lesson))


async def cmd_study(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    atlas = context.bot_data["atlas"]
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    lesson = await atlas.ensure_today_lesson()
    if not lesson:
        await _reply_html(update, "I don't have a study topic queued right now.")
        return
    atlas.set_lesson_status("studying", "Started from Atlas chat.")
    await _reply_html(update, atlas.format_lesson_telegram(lesson))


async def cmd_postpone(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    note = " ".join(context.args).strip() if context.args else "Postponed from Atlas chat."
    context.bot_data["atlas"].set_lesson_status("postponed", note)
    await _reply_html(update, "<b>Atlas lesson postponed for today.</b>\nI'll hold the routine lesson.")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _authorized(update, context):
        return
    text = (update.message.text or "").strip()
    if not text:
        return

    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    try:
        response = await context.bot_data["atlas"].chat(text)
        await _reply_html(update, response)
    except Exception as e:
        logger.error("Atlas chat failed: %s", e, exc_info=True)
        await _reply_html(update, f"Atlas hit an error: {escape(e)}")


def build_atlas_application(token: str, atlas_chat_id: int, atlas) -> Application:
    app = Application.builder().token(token).build()
    app.bot_data["atlas"] = atlas
    app.bot_data["atlas_chat_id"] = atlas_chat_id

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("study", cmd_study))
    app.add_handler(CommandHandler("today", cmd_today))
    app.add_handler(CommandHandler("postpone", cmd_postpone))
    app.add_handler(CommandHandler("skills", cmd_skills))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    return app
