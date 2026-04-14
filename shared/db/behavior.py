"""
Agent behavior policy CRUD.

Policies are key-value overrides per agent, persisted in agent_behavior_policies.
Statuses: proposed → approved → applied | rejected | rolled_back
"""
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from shared.db.connection import connect_sqlite


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = connect_sqlite(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


@dataclass
class BehaviorPolicy:
    id: Optional[int] = None
    agent: str = ""
    policy_key: str = ""
    policy_value: str = ""
    description: str = ""
    status: str = "proposed"
    origin: str = "user"
    changed_by: str = "roderick"
    requires_approval: bool = False
    approved_by: Optional[str] = None
    applied_at: Optional[str] = None
    expires_at: Optional[str] = None
    audit_notes: str = ""
    created_at: str = ""
    updated_at: str = ""


def _row_to_policy(row: sqlite3.Row) -> BehaviorPolicy:
    return BehaviorPolicy(
        id=row["id"],
        agent=row["agent"],
        policy_key=row["policy_key"],
        policy_value=row["policy_value"],
        description=row["description"],
        status=row["status"],
        origin=row["origin"],
        changed_by=row["changed_by"],
        requires_approval=bool(row["requires_approval"]),
        approved_by=row["approved_by"],
        applied_at=row["applied_at"],
        expires_at=row["expires_at"],
        audit_notes=row["audit_notes"] or "",
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def upsert_policy(db_path: str, policy: BehaviorPolicy) -> BehaviorPolicy:
    """Insert or update a policy (upsert on agent + policy_key)."""
    now = _now()
    conn = _connect(db_path)
    try:
        conn.execute(
            """INSERT INTO agent_behavior_policies
               (agent, policy_key, policy_value, description, status, origin,
                changed_by, requires_approval, approved_by, applied_at,
                expires_at, audit_notes, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(agent, policy_key) DO UPDATE SET
                 policy_value=excluded.policy_value,
                 description=excluded.description,
                 status=excluded.status,
                 origin=excluded.origin,
                 changed_by=excluded.changed_by,
                 requires_approval=excluded.requires_approval,
                 expires_at=excluded.expires_at,
                 audit_notes=excluded.audit_notes,
                 updated_at=excluded.updated_at
            """,
            (
                policy.agent, policy.policy_key, policy.policy_value,
                policy.description, policy.status, policy.origin,
                policy.changed_by, int(policy.requires_approval),
                policy.approved_by, policy.applied_at,
                policy.expires_at, policy.audit_notes,
                policy.created_at or now, now,
            ),
        )
        conn.commit()
        row = conn.execute(
            "SELECT * FROM agent_behavior_policies WHERE agent=? AND policy_key=?",
            (policy.agent, policy.policy_key),
        ).fetchone()
        return _row_to_policy(row)
    finally:
        conn.close()


def get_policy(db_path: str, agent: str, policy_key: str) -> Optional[BehaviorPolicy]:
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM agent_behavior_policies WHERE agent=? AND policy_key=?",
            (agent, policy_key),
        ).fetchone()
        return _row_to_policy(row) if row else None
    finally:
        conn.close()


def get_effective_policies(db_path: str, agent: str) -> dict[str, str]:
    """Return {policy_key: policy_value} for all applied (active) policies for an agent."""
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """SELECT policy_key, policy_value FROM agent_behavior_policies
               WHERE agent=? AND status='applied'
               AND (expires_at IS NULL OR expires_at > ?)""",
            (agent, _now()),
        ).fetchall()
        return {r["policy_key"]: r["policy_value"] for r in rows}
    finally:
        conn.close()


def list_policies(
    db_path: str,
    agent: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50,
) -> list[BehaviorPolicy]:
    conn = _connect(db_path)
    try:
        q = "SELECT * FROM agent_behavior_policies WHERE 1=1"
        params: list = []
        if agent:
            q += " AND agent=?"
            params.append(agent)
        if status:
            q += " AND status=?"
            params.append(status)
        q += " ORDER BY updated_at DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(q, params).fetchall()
        return [_row_to_policy(r) for r in rows]
    finally:
        conn.close()


def apply_policy(db_path: str, agent: str, policy_key: str, approved_by: str = "user") -> Optional[BehaviorPolicy]:
    """Mark a proposed policy as applied."""
    now = _now()
    conn = _connect(db_path)
    try:
        conn.execute(
            """UPDATE agent_behavior_policies
               SET status='applied', approved_by=?, applied_at=?, updated_at=?
               WHERE agent=? AND policy_key=?""",
            (approved_by, now, now, agent, policy_key),
        )
        conn.commit()
        row = conn.execute(
            "SELECT * FROM agent_behavior_policies WHERE agent=? AND policy_key=?",
            (agent, policy_key),
        ).fetchone()
        return _row_to_policy(row) if row else None
    finally:
        conn.close()


def reject_policy(db_path: str, agent: str, policy_key: str) -> None:
    now = _now()
    conn = _connect(db_path)
    try:
        conn.execute(
            "UPDATE agent_behavior_policies SET status='rejected', updated_at=? WHERE agent=? AND policy_key=?",
            (now, agent, policy_key),
        )
        conn.commit()
    finally:
        conn.close()


def rollback_policy(db_path: str, agent: str, policy_key: str) -> None:
    now = _now()
    conn = _connect(db_path)
    try:
        conn.execute(
            "UPDATE agent_behavior_policies SET status='rolled_back', audit_notes='Rolled back by user', updated_at=? WHERE agent=? AND policy_key=?",
            (now, agent, policy_key),
        )
        conn.commit()
    finally:
        conn.close()
