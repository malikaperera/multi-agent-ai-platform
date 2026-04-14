import sqlite3
from datetime import datetime, timezone
from typing import Optional

from shared.db.connection import connect_sqlite
from shared.schemas.reminder import Reminder


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def save_reminder(db_path: str, reminder: Reminder) -> Reminder:
    conn = connect_sqlite(db_path, timeout=30, attempts=5, backoff_seconds=1.0)
    try:
        now = _now()
        cur = conn.execute(
            "INSERT INTO reminders (text, due, category, done, recurring, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (reminder.text, reminder.due, reminder.category,
             int(reminder.done), reminder.recurring, now),
        )
        conn.commit()
        reminder.id = cur.lastrowid
        reminder.created_at = now
        return reminder
    finally:
        conn.close()


def list_reminders(
    db_path: str,
    category: Optional[str] = None,
    done: Optional[bool] = None,
    limit: int = 20,
) -> list[Reminder]:
    conn = connect_sqlite(db_path, timeout=30, attempts=5, backoff_seconds=1.0)
    conn.row_factory = sqlite3.Row
    try:
        query = "SELECT * FROM reminders WHERE 1=1"
        params: list = []
        if category:
            query += " AND category=?"
            params.append(category)
        if done is not None:
            query += " AND done=?"
            params.append(int(done))
        query += " ORDER BY COALESCE(due, created_at) ASC LIMIT ?"
        params.append(limit)
        return [_row(r) for r in conn.execute(query, params).fetchall()]
    finally:
        conn.close()


def mark_done(db_path: str, reminder_id: int) -> None:
    conn = connect_sqlite(db_path, timeout=30, attempts=5, backoff_seconds=1.0)
    try:
        conn.execute("UPDATE reminders SET done=1 WHERE id=?", (reminder_id,))
        conn.commit()
    finally:
        conn.close()


def get_due_reminders(db_path: str, as_of: str) -> list[Reminder]:
    """Return undone reminders whose due timestamp <= as_of (ISO string, UTC)."""
    conn = connect_sqlite(db_path, timeout=30, attempts=5, backoff_seconds=1.0)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT * FROM reminders WHERE done=0 AND due IS NOT NULL AND due <= ?"
            " ORDER BY due ASC",
            (as_of,),
        ).fetchall()
        return [_row(r) for r in rows]
    finally:
        conn.close()


def _row(r) -> Reminder:
    return Reminder(
        id=r["id"],
        text=r["text"],
        due=r["due"],
        category=r["category"],
        done=bool(r["done"]),
        recurring=r["recurring"],
        created_at=r["created_at"],
    )
