import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from shared.schemas.task import Task
from shared.db.behavior import get_effective_policies
from shared.db.connection import connect_sqlite


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return connect_sqlite(db_path, timeout=30)


def enqueue_task(db_path: str, task: Task) -> Task:
    conn = _connect(db_path)
    try:
        now = _now()
        cur = conn.execute(
            """INSERT INTO tasks
               (from_agent, to_agent, task_type, description, status, priority, urgency, domain,
                payload, approval_required, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (task.from_agent, task.to_agent, task.task_type, task.description,
             task.status, task.priority, task.urgency, task.domain,
             json.dumps(task.payload), int(task.approval_required), now, now),
        )
        conn.commit()
        task.id = cur.lastrowid
        task.created_at = now
        task.updated_at = now
        return task
    finally:
        conn.close()


def get_next_task(db_path: str, to_agent: str, status: str = "pending") -> Optional[Task]:
    runtime_state = get_effective_policies(db_path, to_agent).get("runtime_state", "active").lower()
    if runtime_state in {"paused", "stopped"}:
        return None
    conn = _connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """SELECT * FROM tasks WHERE to_agent=? AND status=?
               ORDER BY
                 CASE priority WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'normal' THEN 2 ELSE 3 END,
                 CASE urgency  WHEN 'immediate' THEN 0 WHEN 'today' THEN 1 WHEN 'this_week' THEN 2 ELSE 3 END,
                 created_at ASC
               LIMIT 1""",
            (to_agent, status),
        ).fetchone()
        return _row(row) if row else None
    finally:
        conn.close()


def update_task_status(
    db_path: str,
    task_id: int,
    status: str,
    result: Optional[dict] = None,
) -> None:
    conn = _connect(db_path)
    try:
        now = _now()
        if result is not None:
            conn.execute(
                "UPDATE tasks SET status=?, result=?, updated_at=? WHERE id=?",
                (status, json.dumps(result), now, task_id),
            )
        else:
            conn.execute(
                "UPDATE tasks SET status=?, updated_at=? WHERE id=?",
                (status, now, task_id),
            )
        conn.commit()
    finally:
        conn.close()


def touch_task(db_path: str, task_id: int) -> None:
    """Refresh updated_at for a long-running task without changing its status/result."""
    conn = _connect(db_path)
    try:
        conn.execute(
            "UPDATE tasks SET updated_at=? WHERE id=?",
            (_now(), task_id),
        )
        conn.commit()
    finally:
        conn.close()


def requeue_in_progress_tasks(db_path: str, to_agent: str) -> int:
    """Recover tasks abandoned by a worker restart."""
    conn = _connect(db_path)
    try:
        now = _now()
        cur = conn.execute(
            "UPDATE tasks SET status='pending', updated_at=? WHERE to_agent=? AND status='in_progress'",
            (now, to_agent),
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def get_task(db_path: str, task_id: int) -> Optional[Task]:
    conn = _connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        return _row(row) if row else None
    finally:
        conn.close()


def list_tasks(
    db_path: str,
    to_agent: Optional[str] = None,
    status: Optional[str] = None,
    domain: Optional[str] = None,
    limit: int = 20,
) -> list[Task]:
    conn = _connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        query = "SELECT * FROM tasks WHERE 1=1"
        params: list = []
        if to_agent:
            query += " AND to_agent=?"
            params.append(to_agent)
        if status:
            query += " AND status=?"
            params.append(status)
        if domain:
            query += " AND domain=?"
            params.append(domain)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        return [_row(r) for r in conn.execute(query, params).fetchall()]
    finally:
        conn.close()


def _row(r) -> Task:
    result_raw = r["result"]
    return Task(
        id=r["id"],
        from_agent=r["from_agent"],
        to_agent=r["to_agent"],
        task_type=r["task_type"],
        description=r["description"],
        status=r["status"],
        priority=r["priority"],
        urgency=r["urgency"] if "urgency" in r.keys() else "this_week",
        domain=r["domain"]   if "domain"  in r.keys() else "operations",
        payload=json.loads(r["payload"] or "{}"),
        result=json.loads(result_raw) if result_raw else None,
        approval_required=bool(r["approval_required"]),
        created_at=r["created_at"],
        updated_at=r["updated_at"],
    )
