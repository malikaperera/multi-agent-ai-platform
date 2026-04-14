"""
DB-based context helpers for agent prompt enrichment.

Provides compact, structured summaries of recent activity drawn from
the shared SQLite database — tasks, approvals, events — for injection
into agent system prompts and research contexts.
"""
import json
import sqlite3
from typing import Optional

from shared.db.connection import connect_sqlite

def get_recent_task_summaries(
    db_path: str,
    to_agent: Optional[str] = None,
    status: str = "completed",
    limit: int = 5,
) -> list[dict]:
    """
    Return compact summaries of recent tasks.
    Used by agents to understand what has already been done.
    """
    conn = connect_sqlite(db_path, timeout=30, attempts=5, backoff_seconds=1.0)
    conn.row_factory = sqlite3.Row
    try:
        query = """
            SELECT id, to_agent, from_agent, task_type, description, status, updated_at, result
            FROM tasks
            WHERE status = ?
        """
        params: list = [status]
        if to_agent:
            query += " AND to_agent = ?"
            params.append(to_agent)
        query += " ORDER BY updated_at DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()
        summaries = []
        for r in rows:
            result_preview = ""
            if r["result"]:
                try:
                    data = json.loads(r["result"])
                    if isinstance(data, dict):
                        result_preview = data.get("summary", data.get("search_summary", ""))[:120]
                except Exception:
                    pass
            summaries.append({
                "id": r["id"],
                "agent": r["to_agent"],
                "type": r["task_type"],
                "description": r["description"][:100],
                "status": r["status"],
                "updated_at": (r["updated_at"] or "")[:10],
                "result_preview": result_preview,
            })
        return summaries
    finally:
        conn.close()


def get_recent_approval_decisions(db_path: str, limit: int = 10) -> list[dict]:
    """
    Return recent approval decisions (approved/rejected).
    Used by Venture and Forge to understand what has been approved or rejected.
    """
    conn = connect_sqlite(db_path, timeout=30, attempts=5, backoff_seconds=1.0)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT ar.id, ar.request_type, ar.status, ar.description, ar.resolved_at, ar.payload
            FROM approval_requests ar
            WHERE ar.status IN ('approved', 'rejected', 'rolled_back')
            ORDER BY ar.resolved_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        decisions = []
        for r in rows:
            payload = {}
            if r["payload"]:
                try:
                    payload = json.loads(r["payload"])
                except Exception:
                    pass
            decisions.append({
                "id": r["id"],
                "type": r["request_type"],
                "decision": r["status"],
                "description": (r["description"] or "")[:120],
                "date": (r["resolved_at"] or "")[:10],
                "capital": payload.get("capital_required"),
            })
        return decisions
    finally:
        conn.close()


def format_task_summaries(summaries: list[dict]) -> str:
    """Format task summaries as compact text for prompt injection."""
    if not summaries:
        return "(no recent completed tasks)"
    lines = []
    for s in summaries:
        preview = f" — {s['result_preview']}" if s["result_preview"] else ""
        lines.append(f"[{s['updated_at']}] #{s['id']} {s['agent']}/{s['type']}: {s['description']}{preview}")
    return "\n".join(lines)


def format_approval_decisions(decisions: list[dict]) -> str:
    """Format approval decisions as compact text for prompt injection."""
    if not decisions:
        return "(no recent decisions)"
    lines = []
    for d in decisions:
        cap = f" (${d['capital']:,.0f})" if d.get("capital") else ""
        lines.append(f"[{d['date']}] {d['decision'].upper()} — {d['type']}: {d['description']}{cap}")
    return "\n".join(lines)
