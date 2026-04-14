"""
ApprovalGate — asyncio-based pre-submission approval for Zuko.

Flow:
  1. browser.py fills a form and calls request_approval()
  2. Gate sends Telegram pre-submission summary and waits (up to timeout)
  3. User taps APPROVE or REJECT in Telegram
  4. bot.py callback calls resolve() with the decision
  5. apply_to_job() receives decision and submits or aborts
"""
import asyncio
import logging
from pathlib import Path

log = logging.getLogger(__name__)

# Global registry: job_id -> {"event": asyncio.Event, "decision": str | None}
_registry: dict[str, dict] = {}


def register(job_id: str) -> asyncio.Event:
    """Create an approval slot. Returns the event to wait on."""
    event = asyncio.Event()
    _registry[job_id] = {"event": event, "decision": None}
    return event


def resolve(job_id: str, decision: str) -> None:
    """Called by bot.py when user taps APPROVE or REJECT."""
    entry = _registry.get(job_id)
    if entry:
        entry["decision"] = decision
        entry["event"].set()
        log.info("[Gate] Resolved %s → %s", job_id, decision)
    else:
        log.warning("[Gate] resolve() called for unknown job_id=%s", job_id)


def get_decision(job_id: str) -> str | None:
    entry = _registry.get(job_id)
    return entry["decision"] if entry else None


def cleanup(job_id: str) -> None:
    _registry.pop(job_id, None)


async def request_approval(
    bot,
    chat_id: int,
    job: dict,
    screenshot_path: str | None,
    safe_fields: list[str],
    confirm_fields: list[str],
    unknown_fields: list[str],
    timeout_seconds: int = 300,
) -> str:
    """
    Send pre-submission review to Telegram and wait for APPROVE/REJECT.
    Returns: 'approve' | 'reject' | 'timeout'
    """
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    job_id = job["job_id"]
    event = register(job_id)

    safe_txt    = "\n".join(f"  ✅ {f}" for f in safe_fields)    or "  (none)"
    confirm_txt = "\n".join(f"  ⚠️ {f}" for f in confirm_fields) or "  (none)"
    unknown_txt = "\n".join(f"  ❓ {f}" for f in unknown_fields) or "  (none)"
    submit_ready = "YES ✅" if not unknown_fields else "NO — unknown fields present ⚠️"

    msg = (
        f"📋 <b>Pre-Submission Review</b>\n\n"
        f"<b>Company:</b> {job.get('company', '')}\n"
        f"<b>Role:</b> {job.get('title', job.get('role', ''))}\n"
        f"<b>Source:</b> {job.get('source', '').upper()}\n"
        f"<b>Apply type:</b> {job.get('apply_type', '')}\n"
        f"<b>Job URL:</b> {job.get('url', '')}\n\n"
        f"<b>Fields Auto-Filled:</b>\n{safe_txt}\n\n"
        f"<b>Needs Your Confirmation:</b>\n{confirm_txt}\n\n"
        f"<b>Unknown / Unanswered:</b>\n{unknown_txt}\n\n"
        f"<b>Submit Ready:</b> {submit_ready}\n\n"
        f"⏱ You have {timeout_seconds // 60} minutes to respond."
    )

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ APPROVE & SUBMIT", callback_data=f"preapprove:{job_id}"),
            InlineKeyboardButton("❌ REJECT",           callback_data=f"prereject:{job_id}"),
        ],
    ])

    try:
        await bot.send_message(
            chat_id=chat_id,
            text=msg,
            parse_mode="HTML",
            reply_markup=keyboard,
            disable_web_page_preview=True,
        )
        if screenshot_path and Path(screenshot_path).exists():
            with open(screenshot_path, "rb") as f:
                await bot.send_photo(chat_id, f, caption="📸 Form preview")
    except Exception as exc:
        log.error("[Gate] Failed to send review: %s", exc)

    try:
        await asyncio.wait_for(event.wait(), timeout=timeout_seconds)
        decision = get_decision(job_id) or "reject"
    except asyncio.TimeoutError:
        log.warning("[Gate] Approval timed out for %s", job_id)
        try:
            await bot.send_message(
                chat_id,
                f"⏱ Approval timed out for <b>{job.get('title', job.get('role', ''))}</b>. Application cancelled.",
                parse_mode="HTML",
            )
        except Exception:
            pass
        decision = "timeout"
    finally:
        cleanup(job_id)

    return decision
