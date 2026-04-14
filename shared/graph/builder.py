"""
Memory graph builder — assembles evidence nodes and edges from all data sources.
Every node and edge maps to a real DB row, file, or system event.
Called by GET /memory/graph in the API.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from shared.db.connection import connect_sqlite

# ── Helpers ───────────────────────────────────────────────────────────────────

def _now() -> datetime:
    return datetime.now(timezone.utc)


def _ts_str(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")


def _short(s: str, n: int = 60) -> str:
    s = str(s)
    return s[:n] + "…" if len(s) > n else s


def _summary_from_report(report: Any) -> str:
    if isinstance(report, dict):
        for key in ("summary", "opportunity_summary", "title", "recommendation", "error"):
            val = report.get(key)
            if val:
                return str(val)[:200]
        return json.dumps(report, ensure_ascii=True)[:200]
    return str(report)[:200]


def _parse_ts(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        ts = datetime.fromisoformat(s.rstrip("Z"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts
    except Exception:
        return None


def _evidence_level(row_exists: bool, inferred: bool = False) -> str:
    if not row_exists:
        return "unknown"
    if inferred:
        return "inferred"
    return "verified"


# ── Main builder ──────────────────────────────────────────────────────────────

def build_memory_graph(
    db_path: str,
    data_dir: str,
    window_hours: int = 24,
    agent_filter: Optional[str] = None,
    limit: int = 200,
) -> dict:
    """
    Returns { nodes, edges, stats }.
    All nodes are keyed by stable string IDs.
    Edges reference node IDs via source/target.
    """
    conn = connect_sqlite(db_path, timeout=10, attempts=5, backoff_seconds=1.0)
    conn.row_factory = sqlite3.Row
    nodes: dict[str, dict] = {}
    edges: list[dict] = []
    warnings: list[str] = []
    now = _now()
    cutoff = _ts_str(now - timedelta(hours=window_hours))
    data_path = Path(data_dir)

    try:
        # ── 1. Agent nodes (always included, verified) ────────────────────────
        for row in conn.execute("SELECT * FROM agent_registry").fetchall():
            name = row["name"]
            if agent_filter and name != agent_filter:
                continue
            nid = f"agent_{name}"
            nodes[nid] = _node(
                id=nid, label=row["display_name"], type="agent",
                agent=name, summary=row["purpose"] or "",
                timestamp=row["updated_at"], evidence="verified",
                source=f"agent_registry#{row['id']}", status=row["status"],
                meta={
                    "model": row["model_used"],
                    "autonomy": row["autonomy_level"],
                    "last_heartbeat": row["last_heartbeat"],
                    "last_message": row["last_message"] or "",
                    "current_model": row["current_model"] or "",
                }
            )

        # Add user node (always, inferred from context)
        if not agent_filter:
            nodes["user_operator"] = _node(
                id="user_operator", label="You", type="user",
                agent=None, summary="System operator - the human-in-the-loop",
                timestamp=_ts_str(now), evidence="inferred",
                source="system_context", status="active", meta={}
            )

        # ── 2. Recent tasks ───────────────────────────────────────────────────
        task_rows = conn.execute(
            """SELECT * FROM tasks
               WHERE updated_at >= ? OR status IN ('pending','in_progress','approved','plan_ready','plan_approved')
               ORDER BY updated_at DESC LIMIT ?""",
            (cutoff, min(limit, 150))
        ).fetchall()

        for t in task_rows:
            if agent_filter and t["to_agent"] != agent_filter and t["from_agent"] != agent_filter:
                continue
            nid = f"task_{t['id']}"
            nodes[nid] = _node(
                id=nid, label=f"#{t['id']} {_short(t['task_type'], 22)}",
                type="task", agent=t["to_agent"],
                summary=_short(t["description"], 140),
                timestamp=t["updated_at"], evidence="verified",
                source=f"tasks#{t['id']}", status=t["status"],
                meta={
                    "task_type": t["task_type"], "from_agent": t["from_agent"],
                    "to_agent": t["to_agent"], "priority": t["priority"],
                    "domain": t["domain"] or "",
                }
            )
            fa_nid = f"agent_{t['from_agent']}"
            ta_nid = f"agent_{t['to_agent']}"
            if fa_nid in nodes:
                edges.append(_edge(
                    id=f"e_fa_{t['id']}", source=fa_nid, target=nid,
                    label="delegated", type="delegated_to",
                    timestamp=t["created_at"], evidence="verified",
                    source_ref=f"tasks#{t['id']}"
                ))
            if ta_nid in nodes:
                edges.append(_edge(
                    id=f"e_ta_{t['id']}", source=nid, target=ta_nid,
                    label="assigned", type="assigned_to",
                    timestamp=t["created_at"], evidence="verified",
                    source_ref=f"tasks#{t['id']}"
                ))

        # ── 3. Approval requests ──────────────────────────────────────────────
        for row in conn.execute(
            "SELECT * FROM approval_requests WHERE created_at >= ? OR status='pending' ORDER BY created_at DESC LIMIT 25",
            (cutoff,)
        ).fetchall():
            nid = f"approval_{row['id']}"
            nodes[nid] = _node(
                id=nid, label=f"approval: {row['request_type']}",
                type="approval", agent=None,
                summary=_short(row["description"], 140),
                timestamp=row["created_at"], evidence="verified",
                source=f"approval_requests#{row['id']}",
                status=row["status"],
                meta={"request_type": row["request_type"]}
            )
            if row["task_id"]:
                task_nid = f"task_{row['task_id']}"
                if task_nid in nodes:
                    edges.append(_edge(
                        id=f"e_appr_{row['id']}", source=task_nid, target=nid,
                        label="requires approval", type="requires_approval",
                        timestamp=row["created_at"], evidence="verified",
                        source_ref=f"approval_requests#{row['id']}"
                    ))
            if row["status"] == "pending" and "user_operator" in nodes:
                edges.append(_edge(
                    id=f"e_appr_u_{row['id']}", source=nid, target="user_operator",
                    label="pending review", type="approved_by_user",
                    timestamp=row["created_at"], evidence="verified",
                    source_ref=f"approval_requests#{row['id']}"
                ))

        # ── 4. Behavior policies ──────────────────────────────────────────────
        for row in conn.execute(
            "SELECT * FROM agent_behavior_policies WHERE updated_at >= ? OR status IN ('proposed','applied') ORDER BY updated_at DESC LIMIT 30",
            (cutoff,)
        ).fetchall():
            if agent_filter and row["agent"] != agent_filter:
                continue
            nid = f"policy_{row['id']}"
            nodes[nid] = _node(
                id=nid, label=_short(row["policy_key"], 28),
                type="policy", agent=row["agent"],
                summary=f"{row['policy_key']} = {_short(row['policy_value'], 60)}\n{row['description']}",
                timestamp=row["updated_at"], evidence="verified",
                source=f"agent_behavior_policies#{row['id']}",
                status=row["status"],
                meta={"policy_value": row["policy_value"], "changed_by": row["changed_by"]}
            )
            agent_nid = f"agent_{row['agent']}"
            if agent_nid in nodes:
                edges.append(_edge(
                    id=f"e_pol_{row['id']}", source=agent_nid, target=nid,
                    label="policy", type="changed_policy",
                    timestamp=row["updated_at"], evidence="verified",
                    source_ref=f"agent_behavior_policies#{row['id']}"
                ))
            if row["changed_by"] and row["changed_by"] != row["agent"]:
                cb_nid = f"agent_{row['changed_by']}"
                if cb_nid in nodes:
                    edges.append(_edge(
                        id=f"e_pol_cb_{row['id']}", source=cb_nid, target=nid,
                        label="changed", type="changed_policy",
                        timestamp=row["updated_at"], evidence="verified",
                        source_ref=f"agent_behavior_policies#{row['id']}"
                    ))

        # ── 5. Forge artifacts ────────────────────────────────────────────────
        for row in conn.execute(
            "SELECT * FROM forge_artifacts WHERE created_at >= ? ORDER BY created_at DESC LIMIT 25",
            (cutoff,)
        ).fetchall():
            nid = f"artifact_{row['id']}"
            nodes[nid] = _node(
                id=nid, label=_short(row["artifact_type"], 24),
                type="artifact", agent="forge",
                summary=_short(row["summary"], 140),
                timestamp=row["created_at"], evidence="verified",
                source=f"forge_artifacts#{row['id']}",
                status=row["validation_state"],
                meta={
                    "path": row["path"], "artifact_type": row["artifact_type"],
                    "approval_state": row["approval_state"],
                    "validation_state": row["validation_state"],
                }
            )
            task_nid = f"task_{row['task_id']}"
            if task_nid in nodes:
                edges.append(_edge(
                    id=f"e_art_{row['id']}", source=task_nid, target=nid,
                    label="produced", type="built_by_forge",
                    timestamp=row["created_at"], evidence="verified",
                    source_ref=f"forge_artifacts#{row['id']}"
                ))
            # Also link artifact to Sentinel if validation_state != pending
            if row["validation_state"] in ("passed", "blocked", "failed"):
                sentinel_nid = "agent_sentinel"
                if sentinel_nid in nodes:
                    etype = "validated_by_sentinel" if row["validation_state"] == "passed" else "blocked_by_sentinel"
                    edges.append(_edge(
                        id=f"e_art_s_{row['id']}", source=nid, target=sentinel_nid,
                        label="validated", type=etype,
                        timestamp=row["created_at"], evidence="verified",
                        source_ref=f"forge_artifacts#{row['id']}"
                    ))

        # ── 6. System improvements ────────────────────────────────────────────
        try:
            for row in conn.execute(
                "SELECT * FROM improvements WHERE updated_at >= ? ORDER BY updated_at DESC LIMIT 20",
                (cutoff,)
            ).fetchall():
                if agent_filter and row["origin_agent"] != agent_filter:
                    pass  # still include, just filter edges below
                nid = f"improvement_{row['id']}"
                nodes[nid] = _node(
                    id=nid, label=_short(row["title"], 36),
                    type="improvement", agent=row["origin_agent"],
                    summary=_short(row["description"] or row["title"], 180),
                    timestamp=row["updated_at"], evidence="verified",
                    source=f"improvements#{row['id']}", status=row["status"],
                    meta={
                        "risk_level": row["risk_level"], "priority": row["priority"],
                        "merlin_task_id": row["merlin_task_id"],
                        "forge_task_id": row["forge_task_id"],
                        "sentinel_task_id": row["sentinel_task_id"],
                    }
                )
                origin_nid = f"agent_{row['origin_agent']}"
                if origin_nid in nodes:
                    edges.append(_edge(
                        id=f"e_imp_o_{row['id']}", source=origin_nid, target=nid,
                        label="signalled", type="created",
                        timestamp=row["created_at"], evidence="verified",
                        source_ref=f"improvements#{row['id']}"
                    ))
                for tkey, etype in [
                    ("merlin_task_id", "escalated_to_merlin"),
                    ("forge_task_id", "proposed_to_forge"),
                    ("sentinel_task_id", "validated_by_sentinel"),
                ]:
                    tid = row[tkey]
                    if tid:
                        t_nid = f"task_{tid}"
                        if t_nid in nodes:
                            edges.append(_edge(
                                id=f"e_imp_{tkey}_{row['id']}", source=nid, target=t_nid,
                                label=etype.replace("_", " "), type=etype,
                                timestamp=row["updated_at"], evidence="verified",
                                source_ref=f"improvements#{row['id']}"
                            ))
        except sqlite3.OperationalError:
            warnings.append("improvements table not available")

        # ── 7. Recent inter-agent messages ────────────────────────────────────
        for row in conn.execute(
            "SELECT * FROM agent_messages WHERE created_at >= ? ORDER BY created_at DESC LIMIT 30",
            (cutoff,)
        ).fetchall():
            if agent_filter and row["from_agent"] != agent_filter and row["to_agent"] != agent_filter:
                continue
            nid = f"message_{row['id']}"
            nodes[nid] = _node(
                id=nid, label=_short(row["message"], 32),
                type="message", agent=row["from_agent"],
                summary=row["message"][:200],
                timestamp=row["created_at"], evidence="verified",
                source=f"agent_messages#{row['id']}", status="sent",
                meta={
                    "from_agent": row["from_agent"], "to_agent": row["to_agent"],
                    "priority": row["priority"], "read": bool(row["read"]),
                }
            )
            src_nid = f"agent_{row['from_agent']}"
            dst_nid = f"agent_{row['to_agent']}"
            if src_nid in nodes:
                edges.append(_edge(
                    id=f"e_msg_{row['id']}", source=src_nid, target=nid,
                    label="sent", type="messaged",
                    timestamp=row["created_at"], evidence="verified",
                    source_ref=f"agent_messages#{row['id']}"
                ))
            if dst_nid in nodes:
                edges.append(_edge(
                    id=f"e_msg_r_{row['id']}", source=nid, target=dst_nid,
                    label="to", type="messaged",
                    timestamp=row["created_at"], evidence="verified",
                    source_ref=f"agent_messages#{row['id']}"
                ))

        # ── 8. Recent high-signal events ──────────────────────────────────────
        skipped_types = {"heartbeat", "task_poll", "presence_updated"}
        for row in conn.execute(
            """SELECT * FROM events WHERE created_at >= ?
               ORDER BY created_at DESC LIMIT 30""",
            (cutoff,)
        ).fetchall():
            if row["event_type"] in skipped_types:
                continue
            if agent_filter and row["agent"] != agent_filter:
                continue
            nid = f"event_{row['id']}"
            try:
                payload = json.loads(row["payload"] or "{}")
            except Exception:
                payload = {}
            nodes[nid] = _node(
                id=nid, label=_short(row["event_type"], 30),
                type="event", agent=row["agent"],
                summary=f"{row['event_type']}\n{json.dumps(payload, ensure_ascii=True)[:120]}",
                timestamp=row["created_at"], evidence="verified",
                source=f"events#{row['id']}", status="emitted",
                meta={"event_type": row["event_type"], "payload": payload}
            )
            agent_nid = f"agent_{row['agent']}"
            if agent_nid in nodes:
                edges.append(_edge(
                    id=f"e_evt_{row['id']}", source=agent_nid, target=nid,
                    label=_short(row["event_type"], 20), type="created",
                    timestamp=row["created_at"], evidence="verified",
                    source_ref=f"events#{row['id']}"
                ))

    finally:
        conn.close()

    # ── 9. Reports from filesystem ────────────────────────────────────────────
    reports_dir = data_path / "reports"
    if reports_dir.exists():
        cutoff_ts = (now - timedelta(hours=window_hours)).timestamp()
        for path in sorted(reports_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)[:50]:
            if path.stat().st_mtime < cutoff_ts:
                continue
            agent_name = path.stem.split("_")[0] if "_" in path.stem else "unknown"
            if agent_filter and agent_name != agent_filter:
                continue
            try:
                data = json.loads(path.read_text(encoding="utf-8", errors="replace"))
                summary = _summary_from_report(data)
            except Exception:
                summary = path.name
            ts = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(timespec="seconds")
            nid = f"report_{path.stem[:50]}"
            nodes[nid] = _node(
                id=nid, label=f"report: {path.stem[:28]}",
                type="report", agent=agent_name,
                summary=summary, timestamp=ts, evidence="verified",
                source=str(path), status="produced", meta={"path": str(path)}
            )
            agent_nid = f"agent_{agent_name}"
            if agent_nid in nodes:
                edges.append(_edge(
                    id=f"e_rpt_{path.stem[:40]}", source=agent_nid, target=nid,
                    label="produced", type="created",
                    timestamp=ts, evidence="verified", source_ref=str(path)
                ))

    # ── 10. Atlas skills (inferred — from skills.json) ────────────────────────
    if not agent_filter or agent_filter == "atlas":
        skills_path = data_path / "atlas" / "skills.json"
        if skills_path.exists():
            try:
                skills_data = json.loads(skills_path.read_text(encoding="utf-8"))
                atlas_nid = "agent_atlas"
                for skill_name, skill_status in list(skills_data.items())[:20]:
                    nid = f"skill_{skill_name[:30].replace(' ', '_')}"
                    nodes[nid] = _node(
                        id=nid, label=_short(skill_name, 22),
                        type="skill", agent="atlas",
                        summary=f"{skill_name} — {skill_status}",
                        timestamp=_ts_str(now), evidence="inferred",
                        source=str(skills_path), status=str(skill_status),
                        meta={"skill_name": skill_name, "state": skill_status}
                    )
                    if atlas_nid in nodes:
                        edges.append(_edge(
                            id=f"e_skill_{nid}", source=atlas_nid, target=nid,
                            label="tracks skill", type="routed_to_atlas",
                            timestamp=_ts_str(now), evidence="inferred",
                            source_ref=str(skills_path)
                        ))
            except Exception as exc:
                warnings.append(f"Atlas skills: {exc}")

    # ── 11. Atlas lessons (recent, verified) ──────────────────────────────────
    if not agent_filter or agent_filter == "atlas":
        lessons_dir = data_path / "atlas" / "lessons"
        if lessons_dir.exists():
            cutoff_ts = (now - timedelta(hours=window_hours)).timestamp()
            for lpath in sorted(lessons_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)[:10]:
                if lpath.stat().st_mtime < cutoff_ts and window_hours <= 48:
                    continue
                try:
                    lesson = json.loads(lpath.read_text(encoding="utf-8"))
                    topic = lesson.get("topic", lpath.stem)
                    summary = lesson.get("summary", "")
                    ts = datetime.fromtimestamp(lpath.stat().st_mtime, timezone.utc).isoformat(timespec="seconds")
                    nid = f"lesson_{lpath.stem[:40]}"
                    nodes[nid] = _node(
                        id=nid, label=_short(topic, 24),
                        type="lesson", agent="atlas",
                        summary=_short(summary or topic, 160),
                        timestamp=ts, evidence="verified",
                        source=str(lpath), status="produced",
                        meta={"topic": topic, "path": str(lpath)}
                    )
                    atlas_nid = "agent_atlas"
                    if atlas_nid in nodes:
                        edges.append(_edge(
                            id=f"e_lesson_{lpath.stem[:30]}", source=atlas_nid, target=nid,
                            label="taught", type="created",
                            timestamp=ts, evidence="verified", source_ref=str(lpath)
                        ))
                except Exception:
                    pass

    # ── 12. Agent learning notes (verified from filesystem) ──────────────────
    learning_dir = data_path / "agent_learning"
    if learning_dir.exists():
        for mdpath in sorted(learning_dir.glob("*.md"), key=lambda p: p.stat().st_mtime, reverse=True):
            agent_name = mdpath.stem
            if agent_filter and agent_name != agent_filter:
                continue
            if agent_name not in {"merlin", "forge", "sentinel", "atlas", "venture", "zuko", "roderick"}:
                continue
            try:
                content = mdpath.read_text(encoding="utf-8", errors="replace")
                # Take last learning entry (most recent)
                last_entry = content.strip().split("\n## ")[-1]
                summary = _short(last_entry.replace("\n", " "), 180)
                ts = datetime.fromtimestamp(mdpath.stat().st_mtime, timezone.utc).isoformat(timespec="seconds")
                nid = f"learning_{agent_name}"
                nodes[nid] = _node(
                    id=nid, label=f"{agent_name}: learning",
                    type="memory_note", agent=agent_name,
                    summary=summary, timestamp=ts, evidence="verified",
                    source=str(mdpath), status="active",
                    meta={"path": str(mdpath), "agent": agent_name}
                )
                agent_nid = f"agent_{agent_name}"
                if agent_nid in nodes:
                    edges.append(_edge(
                        id=f"e_learn_{agent_name}", source=agent_nid, target=nid,
                        label="learning note", type="created",
                        timestamp=ts, evidence="verified", source_ref=str(mdpath)
                    ))
            except Exception:
                pass

    # ── Post-processing ───────────────────────────────────────────────────────

    # Deduplicate edges by (source, target, type)
    seen: set[tuple] = set()
    unique_edges: list[dict] = []
    for e in edges:
        key = (e["source"], e["target"], e["type"])
        if key not in seen:
            seen.add(key)
            unique_edges.append(e)

    # Remove edges whose nodes were filtered out
    node_ids = set(nodes.keys())
    unique_edges = [e for e in unique_edges if e["source"] in node_ids and e["target"] in node_ids]

    # Prioritize nodes: agents/user first, then most recent
    agent_nodes = [n for n in nodes.values() if n["type"] in ("agent", "user")]
    other_nodes = sorted(
        [n for n in nodes.values() if n["type"] not in ("agent", "user")],
        key=lambda n: n.get("timestamp") or "", reverse=True
    )
    # Keep agents + top (limit - agents) others
    final_nodes = agent_nodes + other_nodes[: max(0, limit - len(agent_nodes))]
    final_ids = {n["id"] for n in final_nodes}
    final_edges = [e for e in unique_edges if e["source"] in final_ids and e["target"] in final_ids]

    # Stats
    type_counts: dict[str, int] = {}
    evidence_counts: dict[str, int] = {"verified": 0, "inferred": 0, "unknown": 0}
    for n in final_nodes:
        type_counts[n["type"]] = type_counts.get(n["type"], 0) + 1
        ev = n.get("evidence", "unknown")
        evidence_counts[ev] = evidence_counts.get(ev, 0) + 1

    return {
        "nodes": final_nodes,
        "edges": final_edges,
        "stats": {
            "node_count": len(final_nodes),
            "edge_count": len(final_edges),
            "verified_count": evidence_counts["verified"],
            "inferred_count": evidence_counts["inferred"],
            "unknown_count": evidence_counts["unknown"],
            "type_counts": type_counts,
            "window_hours": window_hours,
            "warnings": warnings,
            "generated_at": _ts_str(now),
        }
    }


# ── Builders ──────────────────────────────────────────────────────────────────

def _node(*, id: str, label: str, type: str, agent: Optional[str],
          summary: str, timestamp: str, evidence: str, source: str,
          status: str, meta: dict) -> dict:
    return {
        "id": id, "label": label, "type": type, "agent": agent,
        "summary": summary, "timestamp": timestamp, "evidence": evidence,
        "source": source, "status": status, "meta": meta,
    }


def _edge(*, id: str, source: str, target: str, label: str, type: str,
          timestamp: str, evidence: str, source_ref: str) -> dict:
    return {
        "id": id, "source": source, "target": target, "label": label,
        "type": type, "timestamp": timestamp, "evidence": evidence,
        "source_ref": source_ref,
    }
