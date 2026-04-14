import json
import sqlite3
from datetime import datetime, timezone
from typing import Optional

from shared.db.connection import connect_sqlite
from shared.schemas.agent import AgentRecord


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def upsert_agent(db_path: str, agent: AgentRecord) -> None:
    """Insert or update an agent record (keyed on name)."""
    conn = connect_sqlite(db_path)
    try:
        conn.execute(
            """INSERT INTO agent_registry
               (name, display_name, purpose, status, autonomy_level, model_used,
                task_types_accepted, report_types_produced,
                last_run, last_heartbeat, last_success, last_error,
                last_message, last_report, config, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(name) DO UPDATE SET
                 display_name          = excluded.display_name,
                 purpose               = excluded.purpose,
                 autonomy_level        = excluded.autonomy_level,
                 model_used            = excluded.model_used,
                 task_types_accepted   = excluded.task_types_accepted,
                 report_types_produced = excluded.report_types_produced,
                 updated_at            = excluded.updated_at""",
            (agent.name, agent.display_name, agent.purpose,
             agent.status, agent.autonomy_level, agent.model_used,
             json.dumps(agent.task_types_accepted),
             json.dumps(agent.report_types_produced),
             agent.last_run, agent.last_heartbeat, agent.last_success, agent.last_error,
             agent.last_message, agent.last_report,
             json.dumps(agent.config), _now()),
        )
        conn.commit()
    finally:
        conn.close()


def update_agent_status(
    db_path: str,
    name: str,
    status: str,
    last_message: Optional[str] = None,
) -> None:
    conn = connect_sqlite(db_path)
    try:
        now = _now()
        if last_message:
            conn.execute(
                "UPDATE agent_registry SET status=?, last_message=?, last_run=?, last_heartbeat=?, updated_at=? WHERE name=?",
                (status, last_message, now, now, now, name),
            )
        else:
            conn.execute(
                "UPDATE agent_registry SET status=?, last_run=?, last_heartbeat=?, updated_at=? WHERE name=?",
                (status, now, now, now, name),
            )
        conn.commit()
    finally:
        conn.close()


def emit_heartbeat(
    db_path: str,
    name: str,
    current_task_id: int = None,
    current_model: str = None,
    state_confidence: float = 1.0,
) -> None:
    """Update heartbeat timestamp + optional introspection fields."""
    conn = connect_sqlite(db_path)
    try:
        now = _now()
        conn.execute(
            """UPDATE agent_registry
               SET last_heartbeat=?, current_task_id=?, current_model=COALESCE(?, current_model),
                   state_confidence=?, updated_at=?
               WHERE name=?""",
            (now, current_task_id, current_model, state_confidence, now, name),
        )
        conn.commit()
    finally:
        conn.close()


def record_agent_success(db_path: str, name: str, message: str = None) -> None:
    conn = connect_sqlite(db_path)
    try:
        now = _now()
        conn.execute(
            "UPDATE agent_registry SET last_success=?, last_error=NULL, last_heartbeat=?, status='idle', updated_at=? WHERE name=?",
            (message or now, now, now, name),
        )
        conn.commit()
    finally:
        conn.close()


def record_agent_error(db_path: str, name: str, error: str) -> None:
    conn = connect_sqlite(db_path)
    try:
        now = _now()
        conn.execute(
            "UPDATE agent_registry SET last_error=?, last_heartbeat=?, status='idle', updated_at=? WHERE name=?",
            (error[:500], now, now, name),
        )
        conn.commit()
    finally:
        conn.close()


def update_agent_report(db_path: str, name: str, report: str) -> None:
    conn = connect_sqlite(db_path)
    try:
        conn.execute(
            "UPDATE agent_registry SET last_report=?, updated_at=? WHERE name=?",
            (report, _now(), name),
        )
        conn.commit()
    finally:
        conn.close()


def get_all_agents(db_path: str) -> list[AgentRecord]:
    conn = connect_sqlite(db_path)
    conn.row_factory = sqlite3.Row
    try:
        return [_row(r) for r in conn.execute(
            "SELECT * FROM agent_registry ORDER BY name"
        ).fetchall()]
    finally:
        conn.close()


def get_agent(db_path: str, name: str) -> Optional[AgentRecord]:
    conn = connect_sqlite(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM agent_registry WHERE name=?", (name,)
        ).fetchone()
        return _row(row) if row else None
    finally:
        conn.close()


def _row(r) -> AgentRecord:
    keys = r.keys() if hasattr(r, "keys") else []
    return AgentRecord(
        id=r["id"],
        name=r["name"],
        display_name=r["display_name"],
        purpose=r["purpose"],
        status=r["status"],
        autonomy_level=r["autonomy_level"] if "autonomy_level" in keys else "supervised",
        model_used=r["model_used"]         if "model_used"     in keys else "claude-sonnet-4-6",
        task_types_accepted=json.loads(r["task_types_accepted"]   if "task_types_accepted"   in keys else "[]") or [],
        report_types_produced=json.loads(r["report_types_produced"] if "report_types_produced" in keys else "[]") or [],
        last_run=r["last_run"],
        last_heartbeat=r["last_heartbeat"] if "last_heartbeat" in keys else None,
        last_success=r["last_success"]     if "last_success"   in keys else None,
        last_error=r["last_error"]         if "last_error"     in keys else None,
        last_message=r["last_message"],
        last_report=r["last_report"],
        config=json.loads(r["config"] or "{}"),
        updated_at=r["updated_at"],
        current_task_id=r["current_task_id"] if "current_task_id" in keys else None,
        current_model=r["current_model"]     if "current_model"    in keys else None,
        state_confidence=r["state_confidence"] if "state_confidence" in keys else 1.0,
    )
