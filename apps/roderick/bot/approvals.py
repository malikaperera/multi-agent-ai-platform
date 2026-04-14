"""Approval request flows with Telegram inline keyboards."""
import json
import logging
from typing import Optional

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup

from shared.db.approvals import (
    create_approval,
    get_approval_by_callback,
    resolve_approval,
    set_telegram_message_id,
)
from shared.db.tasks import get_task, update_task_status
from shared.schemas.approval import ApprovalRequest

logger = logging.getLogger(__name__)

CALLBACK_PREFIX = "rod_appr_"


def is_approval_callback(callback_data: str) -> bool:
    return callback_data.startswith(CALLBACK_PREFIX)


def _keyboard(base: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Approve", callback_data=f"{base}:approve"),
            InlineKeyboardButton("Reject", callback_data=f"{base}:reject"),
        ],
        [
            InlineKeyboardButton("Defer", callback_data=f"{base}:defer"),
            InlineKeyboardButton("Ask details", callback_data=f"{base}:ask"),
        ],
    ])


async def send_approval_request(
    db_path: str,
    bot: Bot,
    chat_id: int,
    description: str,
    task_id: Optional[int] = None,
    request_type: str = "task_approval",
    payload: Optional[dict] = None,
) -> ApprovalRequest:
    """Create approval record, send Telegram message with keyboard, and return it."""
    approval = create_approval(
        db_path,
        ApprovalRequest(
            request_type=request_type,
            description=description,
            task_id=task_id,
            payload=payload or {},
        ),
    )

    titles = {
        "task_approval": "Build Request",
        "plan_approval": "Plan Review",
        "sentinel_approval": "Validation Complete",
        "capital_approval": "Capital Decision",
    }
    title = titles.get(request_type, "Approval Request")

    msg = await bot.send_message(
        chat_id=chat_id,
        text=(
            f"<b>{title}</b>\n\n"
            f"{description}\n\n"
            f"<i>Choose an action:</i>"
        ),
        parse_mode="HTML",
        reply_markup=_keyboard(approval.callback_data),
    )

    set_telegram_message_id(db_path, approval.id, msg.message_id)
    approval.telegram_message_id = msg.message_id
    return approval


async def handle_approval_callback(
    db_path: str,
    bot: Bot,
    chat_id: int,
    callback_data: str,
    message_id: int,
) -> str:
    try:
        base, action = callback_data.rsplit(":", 1)
    except ValueError:
        return "malformed callback"

    approval = get_approval_by_callback(db_path, base)
    if not approval:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="This request has already been resolved.",
        )
        return "already resolved"

    if action == "approve":
        resolve_approval(db_path, approval.id, "approved")
        await _on_approve(db_path, bot, chat_id, approval)
        outcome = "approved"
    elif action == "reject":
        resolve_approval(db_path, approval.id, "rejected")
        await _on_reject(db_path, bot, chat_id, approval)
        outcome = "rejected"
    elif action == "defer":
        resolve_approval(db_path, approval.id, "deferred")
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=f"<b>Deferred.</b>\n\n{approval.description}\n\n<i>Re-send the request when ready.</i>",
            parse_mode="HTML",
        )
        outcome = "deferred"
    elif action == "ask":
        await _send_details(db_path, bot, chat_id, approval)
        return "ask details sent"
    else:
        return f"unknown action: {action}"

    logger.info("Approval #%d %s (task_id=%s)", approval.id, outcome, approval.task_id)
    return outcome


async def _on_approve(
    db_path: str,
    bot: Bot,
    chat_id: int,
    approval: ApprovalRequest,
) -> None:
    if approval.task_id is None:
        await _edit_approval_message(bot, chat_id, approval, "Approved.", approval.description)
        return

    if approval.request_type == "task_approval":
        update_task_status(db_path, approval.task_id, "approved")
        _advance_improvement_from_approval(db_path, approval, "approved")
        await _edit_approval_message(
            bot,
            chat_id,
            approval,
            "Approved - Forge is planning.",
            f"{approval.description}\n\n<i>You will receive the plan for a second review before any files are created.</i>",
        )
    elif approval.request_type == "plan_approval":
        update_task_status(db_path, approval.task_id, "plan_approved")
        _advance_improvement_from_approval(db_path, approval, "implementing")
        await _edit_approval_message(
            bot,
            chat_id,
            approval,
            "Plan approved - Forge will implement.",
            approval.description,
        )
    elif approval.request_type == "sentinel_approval":
        forge_task_id = (approval.payload or {}).get("forge_task_id") or approval.task_id
        if forge_task_id:
            update_task_status(db_path, forge_task_id, "live")
            _advance_improvement_from_approval(db_path, approval, "complete")
        update_task_status(db_path, approval.task_id, "completed")
        await _edit_approval_message(
            bot,
            chat_id,
            approval,
            "Build promoted to live.",
            approval.description,
        )
    elif approval.request_type == "capital_approval":
        await _edit_approval_message(
            bot,
            chat_id,
            approval,
            "Capital opportunity approved for pursuit.",
            approval.description,
        )


async def _on_reject(
    db_path: str,
    bot: Bot,
    chat_id: int,
    approval: ApprovalRequest,
) -> None:
    if approval.task_id:
        if approval.request_type == "sentinel_approval":
            forge_task_id = (approval.payload or {}).get("forge_task_id") or approval.task_id
            if forge_task_id:
                update_task_status(db_path, forge_task_id, "rolled_back")
                _advance_improvement_from_approval(db_path, approval, "rolled_back")
            update_task_status(db_path, approval.task_id, "rolled_back")
        else:
            update_task_status(db_path, approval.task_id, "rejected")
            _advance_improvement_from_approval(db_path, approval, "rejected")
    await _edit_approval_message(bot, chat_id, approval, "Rejected.", approval.description)


async def _send_details(
    db_path: str,
    bot: Bot,
    chat_id: int,
    approval: ApprovalRequest,
) -> None:
    task = get_task(db_path, approval.task_id) if approval.task_id else None
    detail = ""
    if task and task.result:
        detail = f"\n\n<pre>{json.dumps(task.result, indent=2)[:800]}</pre>"
    await bot.send_message(
        chat_id=chat_id,
        text=(
            f"<b>Details for this request:</b>\n\n"
            f"{approval.description}"
            f"{detail}\n\n"
            f"<i>The approval is still pending. Use the buttons above to decide.</i>"
        ),
        parse_mode="HTML",
    )


async def _edit_approval_message(
    bot: Bot,
    chat_id: int,
    approval: ApprovalRequest,
    title: str,
    body: str,
) -> None:
    await bot.edit_message_text(
        chat_id=chat_id,
        message_id=approval.telegram_message_id,
        text=f"<b>{title}</b>\n\n{body}",
        parse_mode="HTML",
    )


def _advance_improvement_from_approval(db_path: str, approval: ApprovalRequest, status: str) -> None:
    improvement_id = (approval.payload or {}).get("improvement_id")
    if not improvement_id:
        return
    try:
        from shared.db.improvements import advance_improvement
        advance_improvement(db_path, int(improvement_id), status)
    except Exception as e:
        logger.warning("Could not advance improvement %s to %s: %s", improvement_id, status, e)
