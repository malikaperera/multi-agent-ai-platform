"""
Improvement pipeline DB helpers.

Lifecycle:
  signal → investigating → proposed → approved → implementing
         → validating → complete | rejected | failed | rolled_back

Each row tracks one improvement candidate end-to-end.
"""
import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from shared.db.connection import connect_sqlite

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class Improvement:
    id: Optional[int] = None
    title: str = ""
    description: str = ""
    origin_agent: str = ""          # who spotted this
    origin_signal: str = ""         # what triggered it: slow_llm | stuck_task | agent_error | learning_gap | opportunity | manual
    status: str = "signal"          # signal | investigating | proposed | approved | implementing | validating | complete | rejected | failed | rolled_back
    evidence: dict = field(default_factory=dict)           # verified facts, likely causes, unknowns
    merlin_task_id: Optional[int] = None
    forge_task_id: Optional[int] = None
    sentinel_task_id: Optional[int] = None
    priority: str = "normal"        # low | normal | high | critical
    risk_level: str = "unknown"     # low | medium | high
    affected_components: list = field(default_factory=list)
    forge_recommended: bool = False
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


def upsert_improvement(db_path: str, imp: Improvement) -> Improvement:
    conn = connect_sqlite(db_path, timeout=30, attempts=5, backoff_seconds=1.0)
    conn.row_factory = sqlite3.Row
    try:
        now = _now()
        if imp.id is None:
            cur = conn.execute(
                """INSERT INTO improvements
                   (title, description, origin_agent, origin_signal, status,
                    evidence, merlin_task_id, forge_task_id, sentinel_task_id,
                    priority, risk_level, affected_components, forge_recommended,
                    created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    imp.title, imp.description, imp.origin_agent, imp.origin_signal,
                    imp.status, json.dumps(imp.evidence), imp.merlin_task_id,
                    imp.forge_task_id, imp.sentinel_task_id, imp.priority,
                    imp.risk_level, json.dumps(imp.affected_components),
                    int(imp.forge_recommended), now, now,
                ),
            )
            imp.id = cur.lastrowid
            imp.created_at = now
            imp.updated_at = now
        else:
            conn.execute(
                """UPDATE improvements SET
                   title=?, description=?, status=?, evidence=?,
                   merlin_task_id=?, forge_task_id=?, sentinel_task_id=?,
                   priority=?, risk_level=?, affected_components=?,
                   forge_recommended=?, updated_at=?
                   WHERE id=?""",
                (
                    imp.title, imp.description, imp.status, json.dumps(imp.evidence),
                    imp.merlin_task_id, imp.forge_task_id, imp.sentinel_task_id,
                    imp.priority, imp.risk_level, json.dumps(imp.affected_components),
                    int(imp.forge_recommended), now, imp.id,
                ),
            )
            imp.updated_at = now
        conn.commit()
    finally:
        conn.close()
    return imp


def get_improvement(db_path: str, imp_id: int) -> Optional[Improvement]:
    conn = connect_sqlite(db_path, timeout=30, attempts=5, backoff_seconds=1.0)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("SELECT * FROM improvements WHERE id=?", (imp_id,)).fetchone()
        return _row_to_imp(row) if row else None
    finally:
        conn.close()


def list_improvements(
    db_path: str,
    status: Optional[str] = None,
    limit: int = 50,
) -> list[Improvement]:
    conn = connect_sqlite(db_path, timeout=30, attempts=5, backoff_seconds=1.0)
    conn.row_factory = sqlite3.Row
    try:
        if status:
            rows = conn.execute(
                "SELECT * FROM improvements WHERE status=? ORDER BY created_at DESC LIMIT ?",
                (status, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM improvements ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [_row_to_imp(r) for r in rows]
    finally:
        conn.close()


def list_active_improvements(db_path: str) -> list[Improvement]:
    """Return improvements that are not yet complete/rejected/failed."""
    conn = connect_sqlite(db_path, timeout=30, attempts=5, backoff_seconds=1.0)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """SELECT * FROM improvements
               WHERE status NOT IN ('complete','rejected','failed','rolled_back')
               ORDER BY created_at DESC""",
        ).fetchall()
        return [_row_to_imp(r) for r in rows]
    finally:
        conn.close()


def advance_improvement(
    db_path: str,
    imp_id: int,
    new_status: str,
    evidence_update: Optional[dict] = None,
    **kwargs,
) -> Optional[Improvement]:
    imp = get_improvement(db_path, imp_id)
    if not imp:
        return None
    imp.status = new_status
    if evidence_update:
        imp.evidence.update(evidence_update)
    for k, v in kwargs.items():
        if hasattr(imp, k):
            setattr(imp, k, v)
    return upsert_improvement(db_path, imp)


def _row_to_imp(row: sqlite3.Row) -> Improvement:
    keys = row.keys()

    def _js(col, fallback):
        v = row[col] if col in keys else None
        if not v:
            return fallback
        try:
            return json.loads(v)
        except Exception:
            return fallback

    return Improvement(
        id=row["id"],
        title=row["title"],
        description=row["description"],
        origin_agent=row["origin_agent"],
        origin_signal=row["origin_signal"],
        status=row["status"],
        evidence=_js("evidence", {}),
        merlin_task_id=row["merlin_task_id"] if "merlin_task_id" in keys else None,
        forge_task_id=row["forge_task_id"] if "forge_task_id" in keys else None,
        sentinel_task_id=row["sentinel_task_id"] if "sentinel_task_id" in keys else None,
        priority=row["priority"],
        risk_level=row["risk_level"],
        affected_components=_js("affected_components", []),
        forge_recommended=bool(row["forge_recommended"] if "forge_recommended" in keys else 0),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )
