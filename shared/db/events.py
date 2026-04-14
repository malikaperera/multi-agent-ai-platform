"""
Structured event log. Agents emit events here.
n8n (Phase 7) or a relay process consumes unprocessed events.
"""
import json
import sqlite3
from datetime import datetime, timezone

from shared.db.connection import connect_sqlite


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit_event(db_path: str, event_type: str, agent: str, payload: dict = None) -> None:
    """Append an event. Fire-and-forget — failures are logged, not raised."""
    conn = connect_sqlite(db_path, timeout=30, attempts=5, backoff_seconds=1.0)
    try:
        conn.execute(
            "INSERT INTO events (event_type, agent, payload, created_at) VALUES (?, ?, ?, ?)",
            (event_type, agent, json.dumps(payload or {}), _now()),
        )
        conn.commit()
    except Exception:
        pass  # Events are best-effort; never crash an agent on emit failure
    finally:
        conn.close()


def get_unprocessed_events(db_path: str, limit: int = 50) -> list[dict]:
    conn = connect_sqlite(db_path, timeout=30, attempts=5, backoff_seconds=1.0)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT * FROM events WHERE processed=0 ORDER BY created_at ASC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def mark_processed(db_path: str, event_ids: list[int]) -> None:
    if not event_ids:
        return
    conn = connect_sqlite(db_path, timeout=30, attempts=5, backoff_seconds=1.0)
    try:
        placeholders = ",".join(["?"] * len(event_ids))
        conn.execute(
            f"UPDATE events SET processed=1 WHERE id IN ({placeholders})",
            event_ids,
        )
        conn.commit()
    finally:
        conn.close()
