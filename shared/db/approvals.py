import json
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Optional

from shared.db.connection import connect_sqlite
from shared.schemas.approval import ApprovalRequest


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_approval(db_path: str, approval: ApprovalRequest) -> ApprovalRequest:
    """Persist approval request and return it with id + callback_data set."""
    callback_data = approval.callback_data or f"rod_appr_{uuid.uuid4().hex[:12]}"
    conn = connect_sqlite(db_path, timeout=30, attempts=5, backoff_seconds=1.0)
    try:
        now = _now()
        cur = conn.execute(
            """INSERT INTO approval_requests
               (task_id, request_type, description, payload, status,
                telegram_message_id, callback_data, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (approval.task_id, approval.request_type, approval.description,
             json.dumps(approval.payload), approval.status,
             approval.telegram_message_id, callback_data, now),
        )
        conn.commit()
        approval.id = cur.lastrowid
        approval.callback_data = callback_data
        approval.created_at = now
        return approval
    finally:
        conn.close()


def resolve_approval(db_path: str, approval_id: int, status: str) -> None:
    """Set status to approved | rejected | deferred."""
    conn = connect_sqlite(db_path, timeout=30, attempts=5, backoff_seconds=1.0)
    try:
        conn.execute(
            "UPDATE approval_requests SET status=?, resolved_at=? WHERE id=?",
            (status, _now(), approval_id),
        )
        conn.commit()
    finally:
        conn.close()


def get_approval_by_callback(db_path: str, callback_data: str) -> Optional[ApprovalRequest]:
    """Look up a pending approval by its base callback_data key."""
    conn = connect_sqlite(db_path, timeout=30, attempts=5, backoff_seconds=1.0)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM approval_requests WHERE callback_data=? AND status='pending'",
            (callback_data,),
        ).fetchone()
        return _row(row) if row else None
    finally:
        conn.close()


def set_telegram_message_id(db_path: str, approval_id: int, message_id: int) -> None:
    conn = connect_sqlite(db_path, timeout=30, attempts=5, backoff_seconds=1.0)
    try:
        conn.execute(
            "UPDATE approval_requests SET telegram_message_id=? WHERE id=?",
            (message_id, approval_id),
        )
        conn.commit()
    finally:
        conn.close()


def list_pending_approvals(db_path: str) -> list[ApprovalRequest]:
    conn = connect_sqlite(db_path, timeout=30, attempts=5, backoff_seconds=1.0)
    conn.row_factory = sqlite3.Row
    try:
        return [_row(r) for r in conn.execute(
            "SELECT * FROM approval_requests WHERE status='pending' ORDER BY created_at ASC"
        ).fetchall()]
    finally:
        conn.close()


def _row(r) -> ApprovalRequest:
    return ApprovalRequest(
        id=r["id"],
        task_id=r["task_id"],
        request_type=r["request_type"],
        description=r["description"],
        payload=json.loads(r["payload"] or "{}"),
        status=r["status"],
        telegram_message_id=r["telegram_message_id"],
        callback_data=r["callback_data"],
        created_at=r["created_at"],
        resolved_at=r["resolved_at"],
    )
