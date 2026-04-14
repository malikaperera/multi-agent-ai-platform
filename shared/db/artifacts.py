"""Forge artifact registry helpers."""
import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from shared.db.connection import connect_sqlite

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class ForgeArtifact:
    task_id: int
    artifact_type: str
    path: str
    summary: str = ""
    artifact_root: str = ""
    relative_path: str = ""
    approval_state: str = "unknown"
    validation_state: str = "pending"
    metadata: dict = field(default_factory=dict)
    id: Optional[int] = None
    created_at: Optional[str] = None


def record_forge_artifact(db_path: str, artifact: ForgeArtifact) -> ForgeArtifact:
    conn = connect_sqlite(db_path, timeout=30, attempts=5, backoff_seconds=1.0)
    conn.row_factory = sqlite3.Row
    try:
        now = _now()
        cur = conn.execute(
            """INSERT INTO forge_artifacts
               (task_id, artifact_type, artifact_root, relative_path, path,
                summary, approval_state, validation_state, metadata, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                artifact.task_id,
                artifact.artifact_type,
                artifact.artifact_root,
                artifact.relative_path,
                artifact.path,
                artifact.summary,
                artifact.approval_state,
                artifact.validation_state,
                json.dumps(artifact.metadata or {}),
                now,
            ),
        )
        conn.commit()
        artifact.id = cur.lastrowid
        artifact.created_at = now
        return artifact
    finally:
        conn.close()


def list_forge_artifacts(
    db_path: str,
    task_id: Optional[int] = None,
    limit: int = 100,
) -> list[ForgeArtifact]:
    conn = connect_sqlite(db_path, timeout=30, attempts=5, backoff_seconds=1.0)
    conn.row_factory = sqlite3.Row
    try:
        if task_id is not None:
            rows = conn.execute(
                "SELECT * FROM forge_artifacts WHERE task_id=? ORDER BY created_at DESC LIMIT ?",
                (task_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM forge_artifacts ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [_row_to_artifact(row) for row in rows]
    finally:
        conn.close()


def get_forge_artifact(db_path: str, artifact_id: int) -> Optional[ForgeArtifact]:
    conn = connect_sqlite(db_path, timeout=30, attempts=5, backoff_seconds=1.0)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM forge_artifacts WHERE id=?",
            (artifact_id,),
        ).fetchone()
        return _row_to_artifact(row) if row else None
    finally:
        conn.close()


def update_forge_artifact_validation(
    db_path: str,
    task_id: int,
    validation_state: str,
) -> None:
    conn = connect_sqlite(db_path, timeout=30, attempts=5, backoff_seconds=1.0)
    try:
        conn.execute(
            "UPDATE forge_artifacts SET validation_state=? WHERE task_id=?",
            (validation_state, task_id),
        )
        conn.commit()
    finally:
        conn.close()


def _row_to_artifact(row: sqlite3.Row) -> ForgeArtifact:
    raw_meta = row["metadata"] or "{}"
    try:
        metadata = json.loads(raw_meta)
    except Exception:
        metadata = {}
    return ForgeArtifact(
        id=row["id"],
        task_id=row["task_id"],
        artifact_type=row["artifact_type"],
        artifact_root=row["artifact_root"],
        relative_path=row["relative_path"],
        path=row["path"],
        summary=row["summary"],
        approval_state=row["approval_state"],
        validation_state=row["validation_state"],
        metadata=metadata,
        created_at=row["created_at"],
    )
