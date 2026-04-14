import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from shared.db.connection import connect_sqlite


@dataclass
class AgentMessage:
    id: int | None = None
    from_agent: str = ""
    to_agent: str = ""
    message: str = ""
    priority: str = "normal"
    read: bool = False
    created_at: str | None = None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return connect_sqlite(db_path, timeout=30, attempts=5, backoff_seconds=1.0)


def send_agent_message(
    db_path: str,
    from_agent: str,
    to_agent: str,
    message: str,
    priority: str = "normal",
) -> AgentMessage:
    conn = _connect(db_path)
    try:
        now = _now()
        cur = conn.execute(
            """INSERT INTO agent_messages
               (from_agent, to_agent, message, priority, read, created_at)
               VALUES (?, ?, ?, ?, 0, ?)""",
            (from_agent, to_agent, message, priority, now),
        )
        conn.commit()
        return AgentMessage(
            id=cur.lastrowid,
            from_agent=from_agent,
            to_agent=to_agent,
            message=message,
            priority=priority,
            read=False,
            created_at=now,
        )
    finally:
        conn.close()


def get_unread_messages(db_path: str, to_agent: str, limit: int = 20) -> list[AgentMessage]:
    conn = _connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """SELECT * FROM agent_messages
               WHERE to_agent=? AND read=0
               ORDER BY
                 CASE priority WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'normal' THEN 2 ELSE 3 END,
                 created_at ASC
               LIMIT ?""",
            (to_agent, limit),
        ).fetchall()
        return [_row(r) for r in rows]
    finally:
        conn.close()


def mark_message_read(db_path: str, message_id: int) -> None:
    conn = _connect(db_path)
    try:
        conn.execute("UPDATE agent_messages SET read=1 WHERE id=?", (message_id,))
        conn.commit()
    finally:
        conn.close()


def _row(row: sqlite3.Row) -> AgentMessage:
    return AgentMessage(
        id=row["id"],
        from_agent=row["from_agent"],
        to_agent=row["to_agent"],
        message=row["message"],
        priority=row["priority"],
        read=bool(row["read"]),
        created_at=row["created_at"],
    )
